<?php

declare(strict_types=1);

namespace App\Command\Aggregate;

use App\Dataset\Contributions\Transformations\Contributions;
use Flow\ETL\DSL\ChartJS;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\Json;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\Join\Expression;
use Flow\ETL\Row;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Stopwatch\Stopwatch;

use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;

#[AsCommand(
    name: 'aggregate:contributions',
    description: 'Aggregate contributions from PRs and Commits stored in data warehouse',
)]
final class ContributionsCommand extends Command
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
            ->addOption('year', 'y', InputArgument::OPTIONAL, 'Year to aggregate', (int) date('Y'));
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start($this->getName());

        $io = new SymfonyStyle($input, $output);

        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');
        $year = (int) $input->getOption('year');

        $io->note("Aggregating contributions to {$org}/{$repository} for {$year} year");

        // Create a list of top contributors
        (new Flow())
            ->read(Json::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/pr/date_utc=*/*"))
            ->transform(new Contributions($year, $org, $repository, $this->warehousePath))
            ->groupBy(ref('user_login'))
            ->aggregate(
                sum(ref('contribution_changes_total')),
                sum(ref('contribution_changes_additions')),
                sum(ref('contribution_changes_deletions')),
                first(ref('user_avatar')),
            )
            ->rename('contribution_changes_total_sum', 'contribution_changes_total')
            ->rename('contribution_changes_additions_sum', 'contribution_changes_additions')
            ->rename('contribution_changes_deletions_sum', 'contribution_changes_deletions')
            ->withEntry('rank', rank()->over(window()->orderBy(ref('contribution_changes_total')->desc())))
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/top_contributors.csv'))
            ->run();

        // Create daily contributions dataset merged with ranks from top contributors
        (new Flow())
            ->read(Json::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/pr/date_utc=*/*"))
            ->transform(new Contributions($year, $org, $repository, $this->warehousePath))
            ->groupBy(ref('date_utc'), ref('user_login'))
            ->aggregate(
                sum(ref('contribution_changes_total')),
                sum(ref('contribution_changes_additions')),
                sum(ref('contribution_changes_deletions')),
                first(ref('user_avatar')),
            )
            ->rename('contribution_changes_total_sum', 'contribution_changes_total')
            ->rename('contribution_changes_additions_sum', 'contribution_changes_additions')
            ->rename('contribution_changes_deletions_sum', 'contribution_changes_deletions')
            ->sortBy(ref('date_utc')->asc(), ref('contribution_changes_total')->desc())
            ->join(
                (new Flow())
                    ->read(CSV::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/top_contributors.csv'))
                    ->select('user_login', 'rank'),
                Expression::on(['user_login' => 'user_login'], 'top_contributor_'),
            )
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
            ->run();

        // Create daily contributions chart with top 10 contributors and remaining contributors grouped into "other_contributions" group
        (new Flow())
            ->read(CSV::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
            ->collect()
            ->select('date_utc', 'user_login', 'contribution_changes_total', 'top_contributor_rank')
            ->filter(ref('top_contributor_rank')->lessThanEqual(lit(10)))
            ->groupBy(ref('date_utc'))
            ->pivot(ref('user_login'))
            ->aggregate(sum(ref('contribution_changes_total')))
            ->join(
                (new Flow())
                    ->read(CSV::from(rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.csv'))
                    ->collect()
                    ->filter(ref('top_contributor_rank')->greaterThan(lit(10)))
                    ->groupBy(ref('date_utc'))
                    ->aggregate(sum(ref('contribution_changes_total')))
                    ->rename('contribution_changes_total_sum', 'contributions'),
                Expression::on(['date_utc' => 'date_utc'], 'other_'),
            )
            ->map(function (Row $row): Row {
                return $row->map(fn (Row\Entry $e) => null === $e->value() ? Entry::int($e->name(), 0) : $e);
            })
            ->sortBy(ref('date_utc')->asc())
            ->collectRefs($users = refs())
            ->write(
                ChartJS::to_file(
                    ChartJS::bar(ref('date_utc'), $users->without('date_utc'))
                        ->setOptions([
                            'scales' => [
                                'x' => ['stacked' => true],
                                'y' => ['stacked' => true],
                            ],
                        ]),
                    rtrim($this->warehousePath, '/')."/{$org}/{$repository}/report/".$year.'/daily_contributions.chart.json',
                    rtrim($this->templatesPath, '/').'/flow/chart/chartjs.json'
                )
            )
            ->run();

        $stopwatch->stop($this->getName());

        $io->success('Done in '.\number_format($stopwatch->getEvent($this->getName())->getDuration() / 1000, 2).'s');

        return Command::SUCCESS;
    }
}