<?php

namespace App\Dataset\Contributions\Transformations;

use App\Dataset\Contributions\DataFrameFactory\ContributionsFactory;
use Flow\ETL\Join\Expression;
use Flow\ETL\Partition\CallableFilter;
use Flow\ETL\{DataFrame, Partition, Transformation};

use function Flow\ETL\DSL\{lit, not, ref};

final class Contributions implements Transformation
{
    public function __construct(
        private readonly int $year,
        private readonly string $org,
        private readonly string $repository,
        private readonly string $warehousePath,
    ) {
    }

    public function transform(DataFrame $dataFrame): DataFrame
    {
        $yearStart = new \DateTimeImmutable($this->year.'-01-01 00:00:00');
        $yearEnd = new \DateTimeImmutable($this->year.'-12-31 23:59:59');

        return $dataFrame->filterPartitions(
            new CallableFilter(
                fn (Partition $partition) => new \DateTimeImmutable($partition->value) >= $yearStart && new \DateTimeImmutable($partition->value) <= $yearEnd
            )
        )
            ->filter(ref('merged_at')->isNotNull())
            ->withEntry('user_login', ref('user')->arrayGet('login'))
            ->withEntry('user_avatar', ref('user')->arrayGet('avatar_url'))
            ->select('number', 'date_utc', 'user_login', 'user_avatar')
            ->batchSize(10)
            ->joinEach(
                new ContributionsFactory($this->org, $this->repository, $this->warehousePath),
                Expression::on(['number' => 'pr'], 'contribution_'),
            )
            ->filter(not(ref('user_login')->isIn(lit(['dependabot[bot]']))))
            // keep only contributions with total changes lower than 10_000 lines of code
            // this is to filter out PRs with generated code and other noise like adding/removing/changing fixture files
            ->filter(ref('contribution_changes_total')->lessThanEqual(lit(10_000)));
    }
}
