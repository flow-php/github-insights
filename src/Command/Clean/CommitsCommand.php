<?php

namespace App\Command\Clean;

use App\DataMesh\Dataset\Schema\CommitSchemaProvider;
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
use function Flow\ETL\DSL\{df, lit, overwrite, ref, structure_ref};

#[AsCommand(
    name: 'clean:commits',
    description: 'Read, clean and store commits at cleaned layer of data mesh',
)]
final class CommitsCommand extends Command
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
        $io->title('Cleaning commits data');

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

        df()
            ->read(from_json($this->paths->commit($org, $repository, Paths\Layer::RAW) . '/date_utc=*/pr=*/*'))
            ->filterPartitions(
                ref('date_utc')->cast('date')->between(lit($afterDate), lit($beforeDate), Boundary::INCLUSIVE)
            )
            ->select('sha', 'node_id', 'pr', 'details_stats', 'author', 'date_utc')
            ->withEntry('details_stats', structure_ref('details_stats')->select('total', 'additions', 'deletions'))
            ->withEntry('author', structure_ref('author')->select('login', 'id', 'node_id', 'avatar_url', 'type'))
            ->withEntry('date_utc', ref('date_utc')->cast('date'))
            // Remove commits from Bots
            ->filter(ref('author')->arrayGet('type')->equals(lit('User')))
            ->collect()
            ->validate($schema = (new CommitSchemaProvider())->clean())
            ->partitionBy(ref('date_utc'))
            ->mode(overwrite())
            ->write(to_parquet($this->paths->commit($org, $repository, Paths\Layer::CLEAN), schema: $schema))
            ->run();

        return Command::SUCCESS;
    }
}
