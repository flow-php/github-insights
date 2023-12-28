<?php

namespace App\Command\Clean;

use App\DataWarehouse\Paths;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Loader\StreamLoader\Output;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Stopwatch\Stopwatch;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{all, data_frame, lit, ref, to_output};

#[AsCommand(
    name: 'clean:pull-requests',
    description: 'Fetch github commits to data warehouse',
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

        // @Todo: Finish moving pull requests from json to more reliable parquet files.
        data_frame()
            ->read(from_json($this->paths->pullRequests($org, $repository, Paths\Layer::RAW) . '/date_utc=*/*'))
            ->filterPartitions(
                ref('date_utc')->cast('date')->between(lit($afterDate), lit($beforeDate), Boundary::INCLUSIVE)
            )
            ->collect()
            ->write(to_output(false, Output::schema))
            ->run();

        return Command::SUCCESS;
    }
}
