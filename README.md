# GitHub Insights 

This is a simple demo app that uses the GitHub API and Flow PHP to fetch, aggregate, and display GitHub Insights. 
The current version of this app is based on [flow-gh-api](https://github.com/stloyd/flow-gh-api).

## Installation

```console
composer insatll
```

This app requires only one secret, `GITHUB_TOKEN` it can be generated through [GitHub UI](https://github.com/settings/tokens).
Once you generate a token, put it into .env.local file in the root of the project.

## How it works 

The goal of this app is to read data from GitHub API, store it in a local Data Warehouse, and then aggregate in order 
to prepare reports. 

GH Pull Requests/Commits are fetched from GitHub API and stored in the local Data Warehouse as parquet files partitioned by date
when PR was created.

This can be done by running: 

```console
bin/gh fetch:pull-requests flow-php flow --after_date="2023-01-01"
bin/gh fetch:commits flow-php flow --after_date="2023-01-01"
```

Please be aware that we must first fetch pull requests since commits are taken from PR's.

Once data is stored in a local Data Warehouse, it can be aggregated and displayed in the form of a report. 

```console
bin/gh aggregate:contributions flow-php flow --year=2023
```

This will generate yearly report for a given org/repository and year. 

```console
var/data/mesh/dev/raw/org/flow-php/flow/repo/report/2023/daily_contributions.chart.json
var/data/mesh/dev/raw/org/flow-php/flow/repo/report/2023/daily_contributions.csv
var/data/mesh/dev/raw/org/flow-php/flow/repo/report/2023/top_contributors.csv
```

Example of Daily Contributions Report:

```csv
date_utc,user_login,user_avatar,contribution_changes_total,contribution_changes_additions,contribution_changes_deletions,top_contributor_rank
2023-01-05,norberttech,https://avatars.githubusercontent.com/u/1921950?v=4,497,212,285,1
2023-01-19,norberttech,https://avatars.githubusercontent.com/u/1921950?v=4,742,337,405,1
2023-01-30,ghost,https://avatars.githubusercontent.com/u/10137?v=4,1439,1257,182,3
2023-01-31,norberttech,https://avatars.githubusercontent.com/u/1921950?v=4,229,175,54,1
2023-02-12,stloyd,https://avatars.githubusercontent.com/u/67402?v=4,723,601,122,2
2023-02-14,norberttech,https://avatars.githubusercontent.com/u/1921950?v=4,84,78,6,1
2023-02-25,stloyd,https://avatars.githubusercontent.com/u/67402?v=4,19,8,11,2
2023-02-26,stloyd,https://avatars.githubusercontent.com/u/67402?v=4,58,42,16,2
2023-03-05,stloyd,https://avatars.githubusercontent.com/u/67402?v=4,757,517,240,2
2023-03-13,norberttech,https://avatars.githubusercontent.com/u/1921950?v=4,684,574,110,1
```

Example of Top Contributors Report:

```csv
user_login,user_avatar,contribution_changes_total,contribution_changes_additions,contribution_changes_deletions,rank
norberttech,https://avatars.githubusercontent.com/u/1921950?v=4,79460,47671,31789,1
stloyd,https://avatars.githubusercontent.com/u/67402?v=4,39442,23154,16288,2
ghost,https://avatars.githubusercontent.com/u/10137?v=4,2118,1843,275,3
owsiakl,https://avatars.githubusercontent.com/u/9623965?v=4,1201,482,719,4
szepeviktor,https://avatars.githubusercontent.com/u/952007?v=4,150,75,75,5
flavioheleno,https://avatars.githubusercontent.com/u/471860?v=4,20,18,2,6
scyzoryck,https://avatars.githubusercontent.com/u/8014727?v=4,12,9,3,7
```
