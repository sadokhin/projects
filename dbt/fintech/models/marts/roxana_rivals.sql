{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='(timestamp)',
      unique_key='tx_hash',
      incremental_strategy='append'
    )
}}

with dex_with_config as (
    select
        *
    from {{ ref('stg_dex_data') }}
    where 
        toStartOfHour(date_time) = toStartOfHour(now()) - toIntervalHour(1)
),

-- get price for eth,btc,bnb from arb ops
coins_price as (
    select 
        time,
        avg(price_eth_usd) as price_eth_usd
    from (
        select 
            toStartOfSecond(timestamp) as time,
            avg(price_eth_usd) as price_eth_usd
        from {{ source('raw_ch_qb', 'chappie_arb_ops') }} as a
        where 
            toStartOfHour(timestamp) = toStartOfHour(now()) - toIntervalHour(1)
            and is_canceled = false
            and a.price_eth_usd != 0
        group by 1

        union all

        select 
            toStartOfSecond(timestamp) as time,
            avg(price_eth_usd) as price_eth_usd
        from {{ source('raw_ch_qb', 'eth_tokyo_chappie_arb_ops') }} as a
        where 
            toStartOfHour(timestamp) = toStartOfHour(now()) - toIntervalHour(1)
            and is_canceled = false
            and a.price_eth_usd != 0
        group by 1
    )
    group by 1
),

eth_price as (
    select
        tx_hash,
        price_eth_usd,
        row_number() over (partition by tx_hash order by abs(timestamp_diff('s',dex_with_config.date_time,coins_price.time))) as price_rn
    from dex_with_config
    left join coins_price
    on toStartOfHour(dex_with_config.date_time) = toStartOfHour(coins_price.time)
),

txs_data as (
    select
        tx_hash,
        bribe,
        gas_used,
        gas_price,
        gas_price * gas_used as base_fee,
        miner,
        from,
        to,
        num_logs
    from {{ source('raw_external', 'txs_data') }} as txs
    where 
        toStartOfHour(block_timestamp) = toStartOfHour(now()) - toIntervalHour(1)
),

-- union cex and dex data + join detected algotrading strategies
dex_cex as (
    select 
        dex.* EXCEPT (from,to,strategy),
        txs_data.bribe as bribe,
        txs_data.miner as miner,
        txs_data.gas_used as gas_used,
        txs_data.gas_price * pow(10,18) as gas_price,
        txs_data.base_fee as base_fee,
        txs_data.num_logs as logs,
        txs_data.from as from,
        txs_data.to as to,
        dex.size_quote / dex.size_base as dex_price,
        case
            when dex.tradingIsEnabled = 0 and dex.otc_ticker != '' then 'trading disabled'
            when size_match in ('more', 'less') then concat(size_match, ' size')
            when chain = 'ETH' 
                    and lower(miner) not in ('0xf2f5c73fa04406b1995e397b55c24ab1f3ea726c',
                    '0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5',
                    '0x690b9a9e9aa1c9db991c7721a92d351db4fac990',
                    '0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97',
                    '0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5',
                    '0x1f9090aae28b8a3dceadf281b0f12828e676c326',
                    '0xc15d2ba418631fc6c90f61fe4dd9a49229bd514f') then 'not_fb_block'
            when dex.swaps > 1 then 'more_than_1swap'
            else ''
        end as flag,
        If(swaps = 1 and length(unique_tokens) = 2 and length(tokens_in) = 1 and length(intermediate_tokens) = 0 and length(tokens_out) = 1 and logs > 8, 'unknown',strategy) as strategy
    from dex_with_config as dex
    left join txs_data
    on txs_data.tx_hash = dex.tx_hash
)

-- main select
select
    date_time as timestamp,
    tx_hash,
    from,
    to,
    chain,
    pool_id,
    block as block_number,
    miner,
    pair,
    pair_type,
    dex_side,
    symbol_base,
    symbol_quote,
    size_quote,
    size_base,
    amountUSD,
    bribe * eth_price.price_eth_usd as bribe_usd,
    gas_used,
    gas_price,
    dex_price,
    flag,
    gas_price / pow(10,18) * gas_used * eth_price.price_eth_usd as base_fee,
    internal_dex_name,
    0 * amountUSD / 10000 + bribe_usd + base_fee as gross0bps,
    amountUSD / size_quote as quote_price_usd,
    multiIf(
        bribe_usd + base_fee > 0 and dex_side = 'B' and symbol_quote in ('USDC','USDT','DAI'), dex_price + gross0bps / size_base,
        bribe_usd + base_fee > 0 and dex_side = 'S' and symbol_quote in ('USDC','USDT','DAI'), dex_price - gross0bps / size_base,
        bribe_usd + base_fee > 0 and dex_side = 'S' and symbol_quote in ('WBTC','WETH'), dex_price - gross0bps / quote_price_usd / size_quote,
        bribe_usd + base_fee > 0 and dex_side = 'B' and symbol_quote in ('WBTC','WETH'), dex_price + gross0bps / quote_price_usd / size_quote,
        0
    ) as rival_cex_price_0bps,
    strategy,
    unique_tokens,
    intermediate_tokens,
    tokens_in,
    tokens_out,
    amounts_in,
    amounts_out,
    logs,
    our_pools,
    swaps
from dex_cex
left join eth_price
on dex_cex.tx_hash = eth_price.tx_hash and price_rn = 1
where
    our_pools
    or strategy in ('graph','dex-dex')