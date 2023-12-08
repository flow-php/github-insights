<?php

namespace App\Dataset\Contributions;

use Flow\ETL\DSL\CSV;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Flow;

use function Flow\ETL\DSL\{lit, ref};

final class TopContributors
{
    public function __construct(
        private readonly string $org,
        private readonly string $repository,
        private readonly string $warehousePath,
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
        $rows = (new Flow())
            ->read(CSV::from($this->warehousePath."/repo/{$this->org}/{$this->repository}/report/".$year.'/top_contributors.csv'))
            ->filter(ref('user_login')->lower()->equals(lit(\mb_strtolower($login))))
            ->fetch(1);

        if (0 === count($rows)) {
            throw new \RuntimeException("Contributor {$login} not found");
        }

        return $rows[0]->toArray();
    }
}
