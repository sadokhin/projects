{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='(date)',
      incremental_strategy='append'
    )
}}

with prem as (
    select
        toStartOfHour(timestamp) as date,
        host,
        left(ticker,7) as book,
        side,
        right(ticker,1) as lifetime,
        size,
        quantile(0.25)(algo_prem) as algo_prem_perc_25,
        quantile(0.50)(algo_prem) as algo_prem_perc_50,
        quantile(0.75)(algo_prem) as algo_prem_perc_75,
        quantile(0.9)(algo_prem) as algo_prem_perc_90,
        avg(algo_prem) as algo_prem_avg,
        count() as observations
    from {{ ref('algo_premiums') }}
    where 
        toStartOfHour(timestamp) = toStartOfHour(now()) - toIntervalHour(1)
        and host in ('rxt1.qb.loc:8201','cbs1.qb.loc:8200')
    group by 1,2,3,4,5,6
)

select * from prem