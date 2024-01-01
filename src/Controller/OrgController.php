<?php

namespace App\Controller;

use App\DataMesh\Paths;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

use function Flow\ETL\DSL\{df, lit, local_files, not, ref};

class OrgController extends AbstractController
{
    public function __construct(private readonly Paths $paths)
    {
    }

    #[Route('/org/{org}', name: 'app_org')]
    public function index(string $org): Response
    {
        return $this->render('org/index.html.twig', [
            'org' => $org,
            'repos' => df()
                ->read(local_files($this->paths->repositories($org, Paths\Layer::RAW)))
                ->filter(ref('is_dir')->equals(lit(true)))
                ->filter(not(ref('file_name')->isIn(lit(['.', '..']))))
                ->select('file_name')
                ->collect()
                ->fetch()
                ->reduceToArray(ref('file_name')),
        ]);
    }
}
