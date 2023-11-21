<?php

namespace App\Command\PullRequest;

use App\Factory\GithubRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Stopwatch\Stopwatch;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

#[AsCommand(
    name: 'pr:fetch',
    description: 'Fetch github pull requests to data warehouse',
)]
class FetchCommand extends Command
{
    public function __construct(
        private readonly string $token,
        private readonly string $warehousePath
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
            ->addOption('after_date', null, InputArgument::OPTIONAL, 'Fetch pull requests created after given date', '-24 hours');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start($this->getName());
        $io = new SymfonyStyle($input, $output);

        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');
        try {
            $afterData = new \DateTimeImmutable($input->getOption('after_date'), new \DateTimeZone('UTC'));
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: '.$input->getOption('after_date'));

            return Command::FAILURE;
        }

        $io->note("Fetching pull requests from {$org}/{$repository} created after {$afterData->format(\DATE_ATOM)}");

        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        (new Flow())
            ->read(
                new PsrHttpClientDynamicExtractor(
                    $client,
                    new GithubRequestFactory($this->token, $org, $repository)
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
            ->select('created_at', 'user')
            ->withEntry('date_utc', ref('created_at')->toDateTime(\DATE_ATOM)->dateFormat())

            ->until(ref('created_at')->toDateTime(\DATE_ATOM)->greaterThanEqual(lit($afterData)))
            // Save with overwrite, partition files per unified date
            ->mode(SaveMode::Overwrite)
            ->partitionBy(ref('date_utc'))
            ->write(Parquet::to(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/pr"))

            // Execute
            ->run();

        $stopwatch->stop($this->getName());

        $io->success('Done in '.$stopwatch->getEvent($this->getName())->getDuration().'ms');

        return Command::SUCCESS;
    }
}
