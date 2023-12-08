<?php

namespace App\Dataset\PullRequests;

use Flow\ETL\Partition\CallableFilter;
use Flow\ETL\{Flow, Partition};

use function Flow\ETL\Adapter\JSON\from_json;

final class PullRequests
{
    public function __construct(
        private readonly string $org,
        private readonly string $repository,
        private readonly string $warehousePath,
    ) {
    }

    public function between(\DateTimeInterface $afterDate, \DateTimeInterface $beforeDate): \Generator
    {
        return (new Flow())
            ->read(from_json(rtrim($this->warehousePath, '/')."/repo/{$this->org}/{$this->repository}/pr/date_utc=*/*"))
            ->filterPartitions(
                new CallableFilter(
                    fn (Partition $partition) => new \DateTimeImmutable($partition->value) >= $afterDate && new \DateTimeImmutable($partition->value) <= $beforeDate
                )
            )
            ->getEach();
    }
}
