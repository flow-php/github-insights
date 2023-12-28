<?php

namespace App\Controller;

use App\DataWarehouse\Paths;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\{BinaryFileResponse, Response, ResponseHeaderBag};
use Symfony\Component\Routing\Annotation\Route;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{df, lit, local_files, not, ref};

class RepoController extends AbstractController
{
    public function __construct(private readonly Paths $paths)
    {
    }

    #[Route('/repo/{org}/{repo}', name: 'app_repo')]
    public function repo(string $org, string $repo): Response
    {
        return $this->render('repo/index.html.twig', [
            'org' => $org,
            'repo' => $repo,
            'years' => df()
                ->read(local_files($this->paths->reports($org, $repo, Paths\Layer::RAW)))
                ->filter(ref('is_dir')->equals(lit(true)))
                ->filter(not(ref('file_name')->isIn(lit(['.', '..']))))
                ->select('file_name')
                ->collect()
                ->fetch()
                ->reduceToArray(ref('file_name')),
        ]);
    }

    #[Route('/repo/{org}/{repo}/{year}', name: 'app_repo_year')]
    public function chart(string $org, string $repo, int $year): Response
    {
        $chartConfigPath = $this->paths->report($org, $repo, $year, 'daily_contributions.chart.json', Paths\Layer::RAW);

        if (!\file_exists($chartConfigPath)) {
            throw $this->createNotFoundException('The report does not exist');
        }

        $topContributors = df()
            ->read(from_csv($this->paths->report($org, $repo, $year, 'top_contributors.csv', Paths\Layer::RAW)))
            ->limit(10)
            ->getEachAsArray();

        return $this->render('repo/year.html.twig', [
            'org' => $org,
            'repo' => $repo,
            'year' => $year,
            'chart_config' => \file_get_contents($chartConfigPath),
            'top_contributors' => $topContributors,
        ]);
    }

    #[Route('/repo/{org}/{repo}/report/{year}', name: 'app_repo_report')]
    public function report(string $org, string $repo, int $year): Response
    {
        $report = $this->paths->report($org, $repo, $year, 'daily_contributions.csv', Paths\Layer::RAW);

        if (!\file_exists($report)) {
            throw $this->createNotFoundException('The report does not exist');
        }

        $response = new BinaryFileResponse($report);
        $response->headers->set('Content-Type', 'text/csv');
        $response->setContentDisposition(
            ResponseHeaderBag::DISPOSITION_ATTACHMENT,
            $org . '_' . $repo . '_daily_contributions_' . $year . '.csv'
        );

        return $response;
    }
}
