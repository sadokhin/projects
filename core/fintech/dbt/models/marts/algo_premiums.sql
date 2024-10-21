{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='(timestamp)',
      incremental_strategy='append'
    )
}}

--CTE with reference (market) prices 
with m_prices as (
    select
        timestamp,
        toDateTime(timestamp) as ts_seconds,
        left(ticker,7) as tick,
        side,
        price,
        round(size_quote,0) as size,
        left(host,4) as pricer
    from 
        {{ source('raw_external', 'otc_market_prices') }}
    where
        toStartOfHour(timestamp) = toStartOfHour(now()) - toIntervalHour(1)
        and right(ticker,1) = 'q'
),

--CTE with trading prices from OTC
c_prices as (
    select
        timestamp,
        toDateTime(timestamp) as ts_seconds,
        ticker,
        side,
        price,
        round(size_quote,0) as size,
        left(host,4) as pricer,
        host
    from 
        {{ source('raw_external', 'otc_market_prices') }}
    where
        toStartOfHour(timestamp) = toStartOfHour(now()) - toIntervalHour(1)
        and right(ticker,1) = '3'
),

--union two types of prices
main as (
    select
        c_prices.*,
        multiIf(
            side='B', (toFloat64(c_prices.price) / toFloat64(m_prices.price) - 1) * 10000,
            side='S', (1 - toFloat64(c_prices.price) / toFloat64(m_prices.price)) * 10000,
            0
        ) AS algo_prem,
        toUnixTimestamp64Micro(c_prices.timestamp) - toUnixTimestamp64Micro(m_prices.timestamp) as time_diff,
        rank() over (partition by c_prices.pricer, c_prices.ticker, c_prices.side, c_prices.timestamp order by abs(time_diff)) as time_rank
    from m_prices
    inner join c_prices
    on m_prices.pricer = c_prices.pricer 
        and m_prices.side = c_prices.side 
        and m_prices.tick = left(c_prices.ticker,7)
        and m_prices.ts_seconds = c_prices.ts_seconds 
        and m_prices.size = c_prices.size
    where 
        time_diff between -12000 and 12000
        and algo_prem > 0
)

--final select
select
    c_prices.timestamp as timestamp,
    host,
    ticker,
    c_prices.side as side,
    toUInt32(c_prices.size) as size,
    algo_prem
from main 
where 
    time_rank = 1