# GitHub Insights 

This is a simple demo app that uses the GitHub API and Flow PHP to fetch, aggregate and display GitHub Insights. 
Current version of this app is based on [flow-gh-api](https://github.com/stloyd/flow-gh-api).

## Installation

```shell
composer insatll
```

This app requires only one secret, `GITHUB_TOKEN` it can be generated through [GitHub UI](https://github.com/settings/tokens).
Once you generate token, put it into .env.local file in the root of the project.

## How it works 

The goal of this app is to read data from GitHub API, store it in local Data Warehouse and then aggregate in order 
to prepare reports. 

GH Pull Requests are fetched from GitHub API and stored in local Data Warehouse as parquet files partitioned by date
when PR was created.

This can be done by running: 

```shell
bin/gh pr:fetch symfony symfony --after_date="2019-01-01"
```

Once data is stored in local Data Warehouse, it can be aggregated and displayed in a form of a report. 

```shell
bin/gh pr:aggregate symfony symfony --year=2021
bin/gh pr:aggregate symfony symfony --year=2022
bin/gh pr:aggregate symfony symfony --year=2023
```

This will generate yearly report for given org/repository and year. 

```
var/data/warehouse/dev/flow-php/flow/report/2021/daily_contributions.csv
var/data/warehouse/dev/flow-php/flow/report/2022/daily_contributions.csv
var/data/warehouse/dev/flow-php/flow/report/2023/daily_contributions.csv
```

```CSV
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