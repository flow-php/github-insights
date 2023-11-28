<?php

namespace App\Controller;

use Flow\ETL\DSL\CSV;
use Flow\ETL\Flow;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\ResponseHeaderBag;
use Symfony\Component\Routing\Annotation\Route;

class RepoController extends AbstractController
{
    #[Route('/repo/{org}/{repo}', name: 'app_repo')]
    public function repo(string $org, string $repo): Response
    {
        $reportsPath = $this->getParameter('data.warehouse.dir').'/'.$org.'/'.$repo.'/report';

        $years = [];
        if (\file_exists($reportsPath)) {
            foreach (\scandir($reportsPath) as $file) {
                if (\in_array($file, ['.', '..'], true)) {
                    continue;
                }

                $years[] = $file;
            }
        }

        return $this->render('repo/index.html.twig', [
            'org' => $org,
            'repo' => $repo,
            'years' => $years,
        ]);
    }

    #[Route('/repo/{org}/{repo}/{year}', name: 'app_repo_year')]
    public function chart(string $org, string $repo, int $year): Response
    {
        $chartConfigPath = $this->getParameter('data.warehouse.dir').'/'.$org.'/'.$repo.'/report/'.$year.'/daily_contributions.chart.json';

        if (!\file_exists($chartConfigPath)) {
            throw $this->createNotFoundException('The report does not exist');
        }

        $topContributors = (new Flow())
            ->read(CSV::from($this->getParameter('data.warehouse.dir').'/'.$org.'/'.$repo.'/report/'.$year.'/top_contributors.csv'))
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
        $report = $this->getParameter('data.warehouse.dir').'/'.$org.'/'.$repo.'/report/'.$year.'/daily_contributions.csv';

        if (!\file_exists($report)) {
            throw $this->createNotFoundException('The report does not exist');
        }

        $response = new BinaryFileResponse($report);
        $response->headers->set('Content-Type', 'text/csv');
        $response->setContentDisposition(
            ResponseHeaderBag::DISPOSITION_ATTACHMENT,
            $org.'_'.$repo.'_daily_contributions_'.$year.'.csv'
        );

        return $response;
    }
}
