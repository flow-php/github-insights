<?php

namespace App\Controller;

use App\DataWarehouse\Paths;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

use function Flow\ETL\DSL\{df, lit, local_files, not, ref};

class HomeController extends AbstractController
{
    public function __construct(private readonly Paths $paths)
    {
    }

    #[Route('/', name: 'app_home')]
    public function index(): Response
    {
        return $this->render('home/index.html.twig', [
            'orgs' => df()
                ->read(local_files($this->paths->organizations(Paths\Layer::RAW)))
                ->filter(ref('is_dir')->equals(lit(true)))
                ->filter(not(ref('file_name')->isIn(lit(['.', '..']))))
                ->select('file_name')
                ->collect()
                ->fetch()
                ->reduceToArray(ref('file_name')),
        ]);
    }
}
