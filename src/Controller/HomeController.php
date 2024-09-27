<?php

namespace App\Controller;

use App\DataMesh\Paths;
use Flow\Filesystem\Path\Filter\KeepAll;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

use function Flow\ETL\DSL\{df, files, lit, not, ref};

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
                ->read(files($this->paths->organizations(Paths\Layer::RAW) . '/*')->withPathFilter(new KeepAll()))
                ->filter(ref('is_dir')->equals(lit(true)))
                ->filter(not(ref('file_name')->isIn(lit(['.', '..']))))
                ->select('file_name')
                ->collect()
                ->fetch()
                ->reduceToArray(ref('file_name')),
        ]);
    }
}
