<?php

namespace App\Command\Clean;

use App\DataMesh\Dataset\Schema\PullRequestSchemaProvider;
use App\DataMesh\Paths;
use Flow\ETL\Function\Between\Boundary;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Stopwatch\Stopwatch;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\{df, list_ref, lit, overwrite, ref, structure_ref, when};

#[AsCommand(
    name: 'clean:pull-requests',
    description: 'Read, clean and store pull requests at cleaned layer of data mesh',
    aliases: ['clean:prs', 'clean:pr', 'clean:pull-request'],
)]
final class PullRequestsCommand extends Command
{
    public function __construct(
        private readonly Paths $paths,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('org', InputArgument::REQUIRED)
            ->addArgument('repository', InputArgument::REQUIRED)
            ->addOption('after_date', null, InputArgument::OPTIONAL, 'Fetch commits created after given date', '-10 day')
            ->addOption('before_date', null, InputArgument::OPTIONAL, 'Fetch commits created before given date', 'now')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start($this->getName());
        $io = new SymfonyStyle($input, $output);
        $io->title('Cleaning pull requests data');

        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');
        try {
            $afterDate = new \DateTimeImmutable($input->getOption('after_date'), new \DateTimeZone('UTC'));
            $afterDate->setTime(0, 0, 0);
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: ' . $input->getOption('after_date'));

            return Command::FAILURE;
        }

        try {
            $beforeDate = new \DateTimeImmutable($input->getOption('before_date'), new \DateTimeZone('UTC'));
            $beforeDate->setTime(0, 0, 0);
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: ' . $input->getOption('before_date'));

            return Command::FAILURE;
        }

        $io->note("Cleaning pull requests from {$org}/{$repository} created after {$afterDate->format(\DATE_ATOM)} and before {$beforeDate->format(\DATE_ATOM)}");

        df()
            ->read(from_json($this->paths->pullRequests($org, $repository, Paths\Layer::RAW) . '/date_utc=*/*.json'))
            ->filterPartitions(
                ref('date_utc')->cast('date')->between(lit($afterDate), lit($beforeDate), Boundary::INCLUSIVE)
            )
            ->filter(ref('merged_at')->isNotNull())
            ->select('url', 'id', 'node_id', 'number', 'state', 'locked', 'title', 'user', 'body', 'date_utc', 'created_at', 'updated_at', 'closed_at', 'merged_at', 'labels')
            ->withEntry('user', structure_ref('user')->select('login', 'id', 'node_id', 'avatar_url', 'url', 'type', 'site_admin'))
            ->withEntry('labels', list_ref('labels')->select('id', 'node_id', 'name', 'color', 'default'))
            ->withEntry('created_at_utc', when(ref('created_at')->isNotNull(), ref('created_at')->cast('datetime'), lit(null)))
            ->withEntry('updated_at_utc', when(ref('updated_at')->isNotNull(), ref('updated_at')->cast('datetime'), lit(null)))
            ->withEntry('merged_at_utc', when(ref('merged_at')->isNotNull(), ref('merged_at')->cast('datetime'), lit(null)))
            ->withEntry('closed_at_utc', when(ref('closed_at')->isNotNull(), ref('closed_at')->cast('datetime'), lit(null)))
            ->withEntry('date_utc', ref('date_utc')->cast('date'))
            ->drop('created_at', 'merged_at', 'updated_at', 'closed_at')
            ->collect()
            ->validate($schema = (new PullRequestSchemaProvider())->clean())
            ->partitionBy(ref('date_utc'))
            ->mode(overwrite())
            ->write(to_parquet($this->paths->pullRequests($org, $repository, Paths\Layer::CLEAN) . '/pull-requests.parquet', schema: $schema))
            ->run();

        return Command::SUCCESS;
    }
}
