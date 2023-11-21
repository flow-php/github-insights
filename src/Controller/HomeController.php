<?php

namespace App\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

class HomeController extends AbstractController
{
    #[Route('/', name: 'app_home')]
    public function index(): Response
    {
        $orgs = [];
        foreach (\scandir($this->getParameter('data.warehouse.dir')) as $file) {
            if (\in_array($file, ['.', '..'], true)) {
                continue;
            }

            $orgs[] = $file;
        }

        return $this->render('home/index.html.twig', [
            'orgs' => $orgs,
        ]);
    }
}
