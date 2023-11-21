<?php

namespace App\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\ResponseHeaderBag;
use Symfony\Component\Routing\Annotation\Route;

class PRController extends AbstractController
{
    #[Route('/pr/{org}/{repo}', name: 'app_pr')]
    public function index(string $org, string $repo): Response
    {
        $reportsPath = $this->getParameter('data.warehouse.dir') . '/' . $org . '/' . $repo . "/report";

        $reports = [];
        if (\file_exists($reportsPath)) {
            foreach (\scandir($reportsPath) as $file) {
                if (\in_array($file, ['.', '..'], true)) {
                    continue;
                }

                $reports[] = $file;
            }
        }

        return $this->render('pr/index.html.twig', [
            'org' => $org,
            'repo' => $repo,
            'reports' => $reports,
        ]);
    }

    #[Route('/pr/{org}/{repo}/report/{year}', name: 'app_pr_report')]
    public function report(string $org, string $repo, int $year) : Response
    {
        $report = $this->getParameter('data.warehouse.dir') . '/' . $org . '/' . $repo . "/report/" . $year . "/daily_contributions.csv";

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
