<?php

declare(strict_types=1);

namespace App\Factory\GitHub;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message;

final class PullRequestsFactory implements NextRequestFactory
{
    public function __construct(
        private readonly string $token,
        private readonly string $org,
        private readonly string $repository,
        private readonly string $state = 'closed',
        private readonly Psr17Factory $factory = new Psr17Factory()
    ) {
    }

    public function create(Message\ResponseInterface $previousResponse = null): ?Message\RequestInterface
    {
        if ($previousResponse instanceof Message\ResponseInterface) {
            if (false === $previousResponse->hasHeader('link')) {
                return null;
            }

            $paginationLinkHeaders = $this->parsePaginationLinkHeader($previousResponse->getHeaderLine('link'));

            if (isset($paginationLinkHeaders['next'])) {
                return $this->factory->createRequest('GET', $paginationLinkHeaders['next'])
                    ->withHeader('Accept', 'application/vnd.github+json')
                    ->withHeader('Authorization', 'Bearer ' . $this->token)
                    ->withHeader('X-GitHub-Api-Version', '2022-11-28')
                    ->withHeader('User-Agent', 'flow-gh-api-fetch');
            }

            return null;
        }

        return $this->factory
            ->createRequest('GET', "https://api.github.com/repos/{$this->org}/{$this->repository}/pulls?state=$this->state&per_page=100&page=1")
            ->withHeader('Accept', 'application/vnd.github+json')
            ->withHeader('Authorization', 'Bearer ' . $this->token)
            ->withHeader('X-GitHub-Api-Version', '2022-11-28')
            ->withHeader('User-Agent', 'flow-gh-api-fetch');
    }

    private function parsePaginationLinkHeader(string $headerLink): array
    {
        $availableLinks = [];
        $links = explode(',', $headerLink);

        foreach ($links as $link) {
            if (preg_match('/<(.*)>;\srel=\\"(.*)\\"/', $link, $matches)) {
                $availableLinks[$matches[2]] = $matches[1];
            }
        }

        return $availableLinks;
    }
}
