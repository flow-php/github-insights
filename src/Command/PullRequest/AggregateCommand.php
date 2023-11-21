<?php

declare(strict_types=1);

namespace App\Command\PullRequest;

use Flow\ETL\DSL\ChartJS;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Partition;
use Flow\ETL\Partition\CallableFilter;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

#[AsCommand(
    name: 'pr:aggregate',
    description: 'Aggregate pull requests data from data warehouse',
)]
final class AggregateCommand extends Command
{
    public function __construct(private readonly string $warehousePath)
    {
        if (!file_exists($this->warehousePath) || !is_dir($this->warehousePath)) {
            throw new \InvalidArgumentException('Data warehouse path does not exist or it\'s not a directory: '.$this->warehousePath);
        }

        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('org', InputArgument::REQUIRED)
            ->addArgument('repository', InputArgument::REQUIRED)
            ->addOption('year', 'y', InputArgument::OPTIONAL, 'Year to aggregate', date('Y'));
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');
        $year = $input->getOption('year');
        $yearStart = new \DateTimeImmutable($input->getOption('year').'-01-01 00:00:00');
        $yearEnd = new \DateTimeImmutable($input->getOption('year').'-12-31 23:59:59');

        $io->note("Aggregating contributions from {$org}/{$repository} for {$year} year");

        (new Flow())
            ->read(Parquet::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/pr/date_utc=*/*"))

            ->filterPartitions(
                new CallableFilter(
                    fn (Partition $partition) => new \DateTimeImmutable($partition->value) >= $yearStart && new \DateTimeImmutable($partition->value) <= $yearEnd
                )
            )
            // Select unique data
            ->withEntry('user', ref('user')->arrayGet('login'))
            ->select('date_utc', 'user')

            // Remove bots from report
            ->filter(ref('user')->notEquals(lit('dependabot[bot]')))
            ->filter(ref('user')->notEquals(lit('ghost')))

            ->groupBy(ref('date_utc'), ref('user'))
            ->aggregate(count(ref('user')))
            ->rename('user_count', 'contributions')

            // Save with overwrite
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
            ->write(
                ChartJS::chart(
                    ChartJS::bar(
                        ref('date_utc'),
                        [
                            ref('contributions'),
                        ]
                    ),
                    rtrim($this->warehousePath, '/') . "/{$org}/{$repository}/report/".$year.'/daily_contributions.html'
                )
            )
            // Execute
            ->run()
        ;

        return Command::SUCCESS;
    }
}
