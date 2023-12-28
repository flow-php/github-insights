<?php

namespace App\DataWarehouse;

use App\DataWarehouse\Paths\Layer;

final class Paths
{
    public function __construct(private readonly string $warehousePath)
    {
        if (!file_exists($this->warehousePath)) {
            \mkdir($this->warehousePath, recursive: true);
        }
    }

    public function organizations(Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org']);
    }

    public function repositories(string $org, Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo']);
    }

    public function pullRequests(string $org, string $repo, Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'pr']);
    }

    public function commit(string $org, string $repo, Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'commit']);
    }

    public function reports(string $org, string $repo, Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'report']);
    }

    public function report(string $org, string $repo, int $year, string $report, Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'report', $year, $report]);
    }

    public function userEvents(string $username, Layer $layer): string
    {
        return \rtrim($this->warehousePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'user', $username, 'events']);
    }
}
