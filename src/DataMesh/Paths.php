<?php

namespace App\DataMesh;

use App\DataMesh\Paths\Layer;

final class Paths
{
    public function __construct(private readonly string $dataMeshPath)
    {
        if (!file_exists($this->dataMeshPath)) {
            \mkdir($this->dataMeshPath, recursive: true);
        }
    }

    public function organizations(Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org']);
    }

    public function repositories(string $org, Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo']);
    }

    public function pullRequests(string $org, string $repo, Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'pr']);
    }

    public function commit(string $org, string $repo, Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'commit']);
    }

    public function reports(string $org, string $repo, Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'report']);
    }

    public function report(string $org, string $repo, int $year, string $report, Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'org', $org, 'repo', $repo, 'report', $year, $report]);
    }

    public function userEvents(string $username, Layer $layer): string
    {
        return \rtrim($this->dataMeshPath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR
            . \implode(DIRECTORY_SEPARATOR, [$layer->value, 'user', $username, 'events']);
    }
}
