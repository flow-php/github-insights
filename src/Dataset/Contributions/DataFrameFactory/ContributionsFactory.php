<?php

namespace App\Dataset\Contributions\DataFrameFactory;

use Flow\ETL\DSL\{From, Json};
use Flow\ETL\Partition\CallableFilter;
use Flow\ETL\{DataFrame, DataFrameFactory, Flow, Partition, Rows};

use function Flow\ETL\DSL\{ref, sum};

final class ContributionsFactory implements DataFrameFactory
{
    public function __construct(
        private readonly string $org,
        private readonly string $repository,
        private readonly string $warehousePath,
    ) {
    }

    public function from(Rows $rows): DataFrame
    {
        if (!\count($rows)) {
            return (new Flow())->read(From::array([]));
        }

        $start = new \DateTimeImmutable($rows->sortBy(ref('date_utc')->asc())->first()->valueOf('date_utc'));
        $end = new \DateTimeImmutable($rows->sortBy(ref('date_utc')->asc())->reverse()->first()->valueOf('date_utc'));

        return (new Flow())
            ->read(Json::from(rtrim($this->warehousePath, '/')."/{$this->org}/{$this->repository}/commit/date_utc=*/pr=*/*"))
            ->filterPartitions(
                new CallableFilter(
                    function (Partition $partition) use ($start, $end): bool {
                        if ('date_utc' !== $partition->name) {
                            return true;
                        }

                        return new \DateTimeImmutable($partition->value) >= $start && new \DateTimeImmutable($partition->value) <= $end;
                    }
                )
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
