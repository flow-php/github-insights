<?php

namespace App\DataMesh\Dataset\Contributions\DataFrameFactory;

use App\DataMesh\Paths;
use Flow\ETL\{DataFrame, DataFrameFactory, Function\Between\Boundary, Rows};

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{df, from_array, lit, ref, sum};

final class ContributionsFactory implements DataFrameFactory
{
    public function __construct(
        private readonly string $org,
        private readonly string $repository,
        private readonly Paths $paths,
    ) {
    }

    public function from(Rows $rows): DataFrame
    {
        if (!\count($rows)) {
            return df()->read(from_array([]));
        }

        $start = new \DateTimeImmutable($rows->sortBy(ref('date_utc')->asc())->first()->valueOf('date_utc'));
        $end = new \DateTimeImmutable($rows->sortBy(ref('date_utc')->asc())->reverse()->first()->valueOf('date_utc'));

        return df()
            ->read(from_json($this->paths->commit($this->org, $this->repository, Paths\Layer::RAW) . '/date_utc=*/pr=*/*'))
            ->filterPartitions(
                ref('date_utc')->cast('date')->between(lit($start), lit($end), Boundary::INCLUSIVE)
            )
            ->select('pr', 'details_stats')
            ->withEntry('changes_total', ref('details_stats')->arrayGet('total'))
            ->withEntry('changes_additions', ref('details_stats')->arrayGet('additions'))
            ->withEntry('changes_deletions', ref('details_stats')->arrayGet('deletions'))
            ->drop('details_stats')
            ->groupBy(ref('pr'))
            ->aggregate(
                sum(ref('changes_total')),
                sum(ref('changes_additions')),
                sum(ref('changes_deletions')),
            )
            ->rename('changes_total_sum', 'changes_total')
            ->rename('changes_additions_sum', 'changes_additions')
            ->rename('changes_deletions_sum', 'changes_deletions');
    }

    public function __serialize(): array
    {
        return [];
    }

    public function __unserialize(array $data): void
    {
    }
}
