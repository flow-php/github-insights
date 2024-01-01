<?php

namespace App\DataMesh\Dataset\PullRequests;

use App\DataMesh\Paths;
use Flow\ETL\{Function\Between\Boundary};

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{df, lit, ref};

final class PullRequests
{
    public function __construct(
        private readonly string $org,
        private readonly string $repository,
        private readonly Paths $paths,
    ) {
    }

    public function between(\DateTimeInterface $afterDate, \DateTimeInterface $beforeDate): \Generator
    {
        return df()
            ->read(from_json($this->paths->pullRequests($this->org, $this->repository, Paths\Layer::RAW) . '/date_utc=*/*'))
            ->filterPartitions(
                ref('date_utc')->cast('date')->between(lit($afterDate), lit($beforeDate), Boundary::INCLUSIVE)
            )
            ->getEach();
    }
}
