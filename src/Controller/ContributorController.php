<?php

namespace App\Controller;

use App\DataWarehouse\Dataset\Contributions\TopContributors;
use App\DataWarehouse\Paths;
use Flow\ETL\Exception\InvalidArgumentException;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

use function Flow\ETL\Adapter\ChartJS\{line_chart, to_chartjs_var};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{df, lit, ref, refs};

class ContributorController extends AbstractController
{
    public function __construct(private readonly Paths $paths)
    {
    }

    #[Route('/contributor/{org}/{repo}/{year}/{login}', name: 'app_contributor')]
    public function contributor(string $org, string $repo, int $year, string $login): Response
    {
        $login = \mb_strtolower($login);

        try {
            $contributor = (new TopContributors($org, $repo, $this->paths))
                ->contributor($year, $login);
        } catch (InvalidArgumentException $e) {
            throw $this->createNotFoundException("Contributor {$login} not found", $e->getCode(), $e);
        }

        $chartData = [];
        df()
            ->read(from_csv($this->paths->report($org, $repo, $year, 'daily_contributions.csv', Paths\Layer::RAW)))
            ->filter(ref('user_login')->equals(lit($login)))
            ->select('date_utc', 'user_login', 'contribution_changes_total')
            ->write(
                to_chartjs_var(
                    line_chart(
                        ref('date_utc'),
                        refs(ref('contribution_changes_total'))
                    )->setDatasetOptions(
                        ref('contribution_changes_total'),
                        [
                            'label' => $login . ' - total changes',
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
