<?php

namespace App\Command\Fetch;

use App\Dataset\Commit\DataFrameFactory\CommitDetailsFactory;
use App\Dataset\PullRequests\PullRequests;
use App\Factory\GitHub\GenericUrlFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Join\Expression;
use Flow\ETL\Row;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\ProgressIndicator;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Stopwatch\Stopwatch;

use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{data_frame, lit, ref};

#[AsCommand(
    name: 'fetch:commits',
    description: 'Fetch github commits to data warehouse',
)]
class CommitsCommand extends Command
{
    public function __construct(
        private readonly string $token,
        private readonly string $warehousePath,
    ) {
        if ('' === $token) {
            throw new \InvalidArgumentException('GitHub API Token must be provided.');
        }

        if (!file_exists($this->warehousePath)) {
            \mkdir($this->warehousePath, recursive: true);
        }

        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('org', InputArgument::REQUIRED)
            ->addArgument('repository', InputArgument::REQUIRED)
            ->addOption('after_date', null, InputArgument::OPTIONAL, 'Fetch commits created after given date', '-5 day')
            ->addOption('before_date', null, InputArgument::OPTIONAL, 'Fetch commits created before given date', 'now')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start($this->getName());
        $io = new SymfonyStyle($input, $output);

        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');
        try {
            $afterDate = new \DateTimeImmutable($input->getOption('after_date'), new \DateTimeZone('UTC'));
            $afterDate->setTime(0, 0, 0);
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: '.$input->getOption('after_date'));

            return Command::FAILURE;
        }

        try {
            $beforeDate = new \DateTimeImmutable($input->getOption('before_date'), new \DateTimeZone('UTC'));
            $beforeDate->setTime(0, 0, 0);
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: '.$input->getOption('before_date'));

            return Command::FAILURE;
        }

        $io->note("Fetching commits from {$org}/{$repository} for previously fetched pull requests created between {$afterDate->format(\DATE_ATOM)} and {$beforeDate->format(\DATE_ATOM)}");

        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        $pullRequests = new PullRequests($org, $repository, $this->warehousePath);

        $progressIndicator = new ProgressIndicator($output);
        $progressIndicator->start('Fetching commits...');

        foreach ($pullRequests->between($afterDate, $beforeDate) as $prRow) {
            data_frame()
                ->read(
                    new PsrHttpClientDynamicExtractor(
                        $client,
                        new GenericUrlFactory($this->token, $prRow->valueOf('commits_url'))
                    )
                )
                // Extract response
                ->withEntry('unpacked', ref('response_body')->jsonDecode())
                ->select('unpacked')
                // Extract data as rows & columns
                ->withEntry('data', ref('unpacked')->expand())
                ->withEntry('data', ref('data')->unpack())
                ->renameAll('data.', '')
                ->drop('unpacked', 'data')
                // add pr number
                ->withEntry('date_utc', lit($prRow->valueOf('date_utc')))
                ->withEntry('pr', lit($prRow->valueOf('number')))
                ->transform(new CallbackRowTransformer(
                    function (Row $row) use ($progressIndicator) {
                        $progressIndicator->setMessage('Processing: '.$row->valueOf('date_utc'));

                        return $row;
                    }
                ))
                ->mode(SaveMode::Overwrite)
                ->joinEach(
                    new CommitDetailsFactory($this->token),
                    Expression::on(['sha' => 'sha'], 'details_')
                )
                ->partitionBy(ref('date_utc'), ref('pr'))
                ->write(to_json(rtrim($this->warehousePath, '/')."/repo/{$org}/{$repository}/commit"))
                // Execute
                ->run();
        }
        $progressIndicator->finish('Commits fetched!');

        $stopwatch->stop($this->getName());

        $io->success('Done in '.\number_format($stopwatch->getEvent($this->getName())->getDuration() / 1000, 2).'s');

        return Command::SUCCESS;
    }
}
