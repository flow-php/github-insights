<?php

declare(strict_types=1);

namespace App\Command\PullRequest;

use Flow\ETL\DSL\ChartJS;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\To;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Partition;
use Flow\ETL\Partition\CallableFilter;
use Flow\ETL\Row;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use Symfony\Component\Stopwatch\Stopwatch;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\dens_rank;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;

#[AsCommand(
    name: 'pr:aggregate',
    description: 'Aggregate pull requests data from data warehouse',
)]
final class AggregateCommand extends Command
{
    public function __construct(
        private readonly string $warehousePath,
        private readonly string $templatesPath,
    ) {
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
        $stopwatch = new Stopwatch();
        $stopwatch->start($this->getName());

        $io = new SymfonyStyle($input, $output);

        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');
        $year = $input->getOption('year');
        $yearStart = new \DateTimeImmutable($input->getOption('year').'-01-01 00:00:00');
        $yearEnd = new \DateTimeImmutable($input->getOption('year').'-12-31 23:59:59');

        $io->note("Aggregating contributions from {$org}/{$repository} for {$year} year");

        // Find out top 10 contributors, those contributors will be displayed in the chart
        // remaining contributors are going to be grouped into "others" group
        (new Flow())
            ->read(Json::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/pr/date_utc=*/*"))
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
            ->drop("date_utc")
            ->groupBy(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->rename("contributions_sum", "contributions")
            ->sortBy(ref('contributions')->desc())
            // Calculate rank for each user based on contributions
            ->withEntry("rank", rank()->over(window()->orderBy(ref('contributions')->desc())))
            // Limit to top 10 contributors
            ->limit(10)
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/top_10_contributions.csv'))
            ->run();

        // Create daily contributions dataset
        (new Flow())
            ->read(Json::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/pr/date_utc=*/*"))
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
            ->sortBy(ref('date_utc')->desc(), ref("contributions")->desc())
            ->join(
                (new Flow())
                    ->read(CSV::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/top_10_contributions.csv'))
                    ->drop("contributions"),
                Expression::on(['user' => 'user'], "top_contributor_"),
            )
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
            ->run();

        // Create daily contributions chart with top 10 contributors and remaining contributors groupped into "other_contributions" group
        (new Flow())
            ->read(CSV::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
            ->collect()
            ->filter(ref("top_contributor_rank")->isNotNull())
            ->groupBy(ref('date_utc'))
            ->pivot(ref('user'))
            ->aggregate(sum(ref('contributions')))
            ->join(
                (new Flow())
                    ->read(CSV::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
                    ->collect()
                    ->filter(ref("top_contributor_rank")->isNull())
                    ->groupBy(ref('date_utc'))
                    ->aggregate(sum(ref('contributions')))
                    ->rename("contributions_sum", "contributions"),
                Expression::on(['date_utc' => 'date_utc'], 'other_'),
            )
            ->map(function (Row $row) : Row {
                return $row->map(fn (Row\Entry $e) => $e->value() === null ? Entry::int($e->name(), 0) : $e);
            })
            ->collectRefs($users = refs())
            ->write(
                ChartJS::to_file(
                    ChartJS::bar(ref('date_utc'), $users->without('date_utc'))
                        ->setOptions([
                            'scales' => [
                                'x' => ['stacked' => true],
                                'y' => ['stacked' => true],
                            ]
                        ]),
                    rtrim($this->warehousePath, '/') . "/{$org}/{$repository}/report/".$year.'/daily_contributions.chart.json',
                    rtrim($this->templatesPath, '/') . "/flow/chart/chartjs.json"
                )
            )
            ->run();

        $stopwatch->stop($this->getName());

        $io->success('Done in '. \number_format($stopwatch->getEvent($this->getName())->getDuration() / 1000, 2) .'s');

        return Command::SUCCESS;
    }
}
