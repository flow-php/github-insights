<?php

namespace App\Command\Fetch;

use App\DataWarehouse\Paths;
use App\Factory\GitHub\PullRequestsFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Filesystem\SaveMode;
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
    name: 'fetch:pr',
    aliases: ['fetch:pull-requests', 'fetch:prs'],
    description: 'Fetch GitHub pull requests to data warehouse',
)]
class PullRequestsCommand extends Command
{
    public function __construct(
        private readonly string $token,
        private readonly Paths $paths
    ) {
        if ('' === $token) {
            throw new \InvalidArgumentException('GitHub API Token must be provided.');
        }

        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('org', InputArgument::REQUIRED)
            ->addArgument('repository', InputArgument::REQUIRED)
            ->addOption('after_date', null, InputArgument::OPTIONAL, 'Fetch pull requests created after given date', '-5 days')
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
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: ' . $input->getOption('after_date'));

            return Command::FAILURE;
        }

        $io->note("Fetching pull requests from {$org}/{$repository} created after {$afterDate->format(\DATE_ATOM)}");

        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        $progressIndicator = new ProgressIndicator($output);
        $progressIndicator->start('Fetching pull requests...');
        data_frame()
            ->read(
                new PsrHttpClientDynamicExtractor(
                    $client,
                    new PullRequestsFactory($this->token, $org, $repository)
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
            // Unify key for partitioning
            ->withEntry('date_utc', ref('created_at')->toDateTime(\DATE_ATOM)->dateFormat())
            ->transform(new CallbackRowTransformer(
                function (Row $row) use ($progressIndicator) {
                    $progressIndicator->setMessage('Processing: ' . $row->valueOf('date_utc'));

                    return $row;
                }
            ))
            // stop fetching data after given date
            ->until(ref('created_at')->toDateTime(\DATE_ATOM)->greaterThanEqual(lit($afterDate)))
            // Save with overwrite, partition files per unified date
            ->mode(SaveMode::Overwrite)
            ->partitionBy(ref('date_utc'))
            ->write(to_json($this->paths->pullRequests($org, $repository, Paths\Layer::RAW)))
            // Execute
            ->run();
        $progressIndicator->finish('Pull requests fetched!');

        $stopwatch->stop($this->getName());

        $io->success('Done in ' . \number_format($stopwatch->getEvent($this->getName())->getDuration() / 1000, 2) . 's');

        return Command::SUCCESS;
    }
}
