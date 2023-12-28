<?php

namespace App\DataWarehouse\Dataset\Commit\DataFrameFactory;

use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Flow\ETL\{DataFrame, DataFrameFactory, Rows};
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;

use function Flow\ETL\DSL\{df, ref};

final class CommitDetailsFactory implements DataFrameFactory
{
    public function __construct(
        private readonly string $token
    ) {
    }

    public function from(Rows $rows): DataFrame
    {
        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        $commitRequests = [];

        foreach ($rows->reduceToArray('url') as $url) {
            $commitRequests[] = $factory->createRequest('GET', $url)
                ->withHeader('Accept', 'application/vnd.github+json')
                ->withHeader('Authorization', 'Bearer ' . $this->token)
                ->withHeader('X-GitHub-Api-Version', '2022-11-28')
                ->withHeader('User-Agent', 'flow-gh-api-fetch');
        }

        return df()
            ->read(
                new PsrHttpClientStaticExtractor(
                    $client,
                    $commitRequests
                )
            )
            // Extract response
            ->withEntry('body', ref('response_body')->jsonDecode())
            ->select('body')
            // Extract data as rows & columns
            ->withEntry('data', ref('body')->unpack())
            ->renameAll('data.', '')
            ->drop('body', 'data')
            ->select('sha', 'stats', 'files');
    }

    public function __serialize(): array
    {
        return [
            'token' => $this->token,
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->token = $data['token'];
    }
}
