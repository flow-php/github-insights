<?php

namespace App\Controller;

use App\Dataset\Contributions\TopContributors;
use Flow\ETL\DSL\ChartJS;
use Flow\ETL\DSL\CSV;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Flow;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

class ContributorController extends AbstractController
{
    #[Route('/contributor/{org}/{repo}/{year}/{login}', name: 'app_contributor')]
    public function contributor(string $org, string $repo, int $year, string $login): Response
    {
        try {
            $contributor = (new TopContributors($org, $repo, $this->getParameter('data.warehouse.dir')))
                ->contributor($year, $login);
        } catch (InvalidArgumentException $e) {
            throw $this->createNotFoundException("Contributor {$login} not found", $e->getCode(), $e);
        }

        $chartData = [];
        (new Flow())
            ->read(CSV::from($this->getParameter('data.warehouse.dir')."/{$org}/{$repo}/report/".$year.'/daily_contributions.csv'))
            ->filter(ref('user_login')->equals(lit($login)))
            ->select('date_utc', 'user_login', 'contribution_changes_total')
            ->write(
                ChartJS::to_var(
                    ChartJS::line(
                        ref('date_utc'),
                        refs(ref('contribution_changes_total'))
                    )->setDatasetOptions(
                        ref('contribution_changes_total'),
                        [
                            'label' => $login.' - total changes',
                        ]
                    ),
                    $chartData
                )
            )
            ->run();

        return $this->render('contributor/index.html.twig', [
            'org' => $org,
            'repo' => $repo,
            'year' => $year,
            'login' => $login,
            'chart_data' => $chartData,
            'contributor' => $contributor,
        ]);
    }
}
