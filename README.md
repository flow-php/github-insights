# GitHub Insights 

This is a simple demo app that uses the GitHub API and Flow PHP to fetch, aggregate, and display GitHub Insights. 
Current version of this app is based on [flow-gh-api](https://github.com/stloyd/flow-gh-api).

## Installation

```bash
composer install
```

This app requires only one secret, `GITHUB_TOKEN` it can be generated through [GitHub UI](https://github.com/settings/tokens).
Once you generate a token, put it into the `.env.local` file at the root of the project.

## How it works 

The goal of this app is to read data from GitHub API, store it in a local Data Warehouse, and then aggregate it in order 
to prepare reports. 

GH Pull Requests are fetched from GitHub API and stored in the local Data Warehouse as parquet files partitioned by date
when PR was created.

This can be done by running: 

```bash
bin/gh pr:fetch flow-php flow --after_date="2023-01-01"
```

Once data is stored in a local Data Warehouse, it can be aggregated and displayed in the form of a report. 

```bash
bin/gh pr:aggregate flow-php flow --year=2023
```

This will generate yearly reports for a given org/repository and year. 

```shell
var/data/warehouse/dev/flow-php/flow/report/2023/daily_contributions.chart.json
var/data/warehouse/dev/flow-php/flow/report/2023/daily_contributions.csv
var/data/warehouse/dev/flow-php/flow/report/2023/top_10_contributions.csv
```

Example of Daily Contributions Report:

```csv
date_utc,user,contributions
2022-07-24,norberttech,3
2022-07-26,norberttech,1
2022-07-27,norberttech,1
2022-07-28,norberttech,1
2022-07-31,norberttech,1
2022-08-01,norberttech,1
2022-08-02,norberttech,1
2022-08-05,norberttech,1
2022-08-06,norberttech,1
2022-08-07,norberttech,3
2022-09-12,norberttech,2
2022-09-25,norberttech,1
2022-09-29,stloyd,1
2022-09-30,stloyd,1
2022-10-01,norberttech,2
2022-10-01,drupol,1
2022-10-03,norberttech,2
```
