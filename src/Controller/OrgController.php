<?php

namespace App\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

class OrgController extends AbstractController
{
    #[Route('/org/{org}', name: 'app_org')]
    public function index(string $org): Response
    {
        $repos = [];
        foreach (\scandir($this->getParameter('data.warehouse.dir').'/'.$org) as $file) {
            if (\in_array($file, ['.', '..'], true)) {
                continue;
            }

            $repos[] = $file;
        }

        return $this->render('org/index.html.twig', [
            'org' => $org,
            'repos' => $repos,
        ]);
    }
}
