<?php

namespace App\Command\Fetch;

use App\DataMesh\Paths;
use App\Factory\GitHub\UserPublicEventsFactory;
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
use function Flow\ETL\DSL\{data_frame, ref};

#[AsCommand(
    name: 'fetch:user:events:public',
    description: 'Fetch github user public events',
)]
class UserPublicEventsCommand extends Command
{
    public function __construct(
        private readonly string $token,
        private readonly Paths $paths,
    ) {
        if ('' === $token) {
            throw new \InvalidArgumentException('GitHub API Token must be provided.');
        }

        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('username', InputArgument::REQUIRED)
            ->addOption('after_date', null, InputArgument::OPTIONAL, 'Fetch commits created after given date', '-5 day')
            ->addOption('before_date', null, InputArgument::OPTIONAL, 'Fetch commits created before given date', 'now')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start($this->getName());
        $io = new SymfonyStyle($input, $output);

        $username = $input->getArgument('username');
        try {
            $afterDate = new \DateTimeImmutable($input->getOption('after_date'), new \DateTimeZone('UTC'));
            $afterDate->setTime(0, 0, 0);
        } catch (\Exception $e) {
            $io->error('Invalid date format, can\'t create DateTimeImmutable instance from: ' . $input->getOption('after_date'));

            return Command::FAILURE;
        }

        $io->note("Fetching public events from {$username} created after {$afterDate->format(\DATE_ATOM)}");

        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        $progressIndicator = new ProgressIndicator($output);
        $progressIndicator->start('Fetching user public events...');
        data_frame()
            ->read(
                new PsrHttpClientDynamicExtractor(
                    $client,
                    new UserPublicEventsFactory($this->token, $username)
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
            // Save with overwrite, partition files per unified date
            ->mode(SaveMode::Overwrite)
            ->partitionBy(ref('date_utc'))
            ->write(to_json($this->paths->userEvents($username, Paths\Layer::RAW)))
            // Execute
            ->run();
        $progressIndicator->finish('User public events fetched!');

        $stopwatch->stop($this->getName());

        $io->success('Done in ' . \number_format($stopwatch->getEvent($this->getName())->getDuration() / 1000, 2) . 's');

        return Command::SUCCESS;
    }
}
