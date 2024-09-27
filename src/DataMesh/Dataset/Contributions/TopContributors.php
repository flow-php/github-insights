<?php

namespace App\DataMesh\Dataset\Contributions;

use App\DataMesh\Paths;
use Flow\ETL\Exception\InvalidArgumentException;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{df, lit, ref};

final class TopContributors
{
    public function __construct(
        private readonly string $org,
        private readonly string $repository,
        private readonly Paths $paths,
    ) {
    }

    /**
     * @return array{
     *     user_login: string,
     *     user_avatar: string,
     *     contribution_changes_total: int,
     *     contribution_changes_additions: int,
     *     contribution_changes_deletions: int,
     *     rank: int
     * }
     *
     * @throws InvalidArgumentException
     */
    public function contributor(int $year, string $login): array
    {
        $rows = df()
            ->read(from_csv($this->paths->report($this->org, $this->repository, $year, 'top_contributors.csv', Paths\Layer::RAW)))
            ->filter(ref('user_login')->lower()->equals(lit(\mb_strtolower($login))))
            ->fetch(1);

        if (0 === count($rows)) {
            throw new \RuntimeException("Contributor {$login} not found");
        }

        return $rows[0]->toArray();
    }
}
