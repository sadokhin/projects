{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='(surr_key,date_hour)',
      unique_key='surr_key',
      incremental_strategy='delete+insert'
    )
}}

with dex_volume as (
    select
        toStartOfHour(timestamp) as date,
        chain,
        pair_type,
        multiIf(
                strategy = 'graph','between',
                flag = 'less size', 'less',
                flag = 'more size', 'more',
                'between'
        ) as size_match,
        if(strategy = 'graph',true,false) as is_graph,
        sum(amountUSD) as amountUSD
    from {{ ref('roxana_rivals') }}
    where
        --toDate(timestamp) >= '2024-10-15' 
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        and our_pools
        and flag != 'trading disabled'
    group by 1,2,3,4,5
),

graph_ops as (
    select
        x,
        symbols_token_in.symbol as token_in,
        symbols_token_out.symbol as token_out
    from {{ source('raw_ch_qb', 'chappie_graph_swaps') }} as swap_ops 
    left join {{ source('raw_external', 'token_addresses') }} as symbols_token_in
    on lower(swap_ops.token_in) = lower(symbols_token_in.address)
    left join {{ source('raw_external', 'token_addresses') }} as symbols_token_out
    on lower(swap_ops.token_out) = lower(symbols_token_out.address)
    where
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        --toDate(timestamp) >= '2024-10-15'
    group by 1,2,3
),

graph_path as (
    select 
        t1.x as x,
        max(concat(t2.token_in, ' -> ', t1.token_in, ' -> ', t1.token_out)) as path
    from 
        graph_ops as t1
    left join graph_ops as t2 
    on t1.token_in = t2.token_out and t1.x = t2.x
    where t2.x != ''
    group by 1
),

arb_ops as (
    select
        timestamp,
        chain,
        hash,
        block_number,
        size_usd,
        gross_profit,
        mined,
        pair,
        pair_type,
        host,
        graph_path.path as path
    from {{ ref('stg_arb_ops') }} as arbs 
    left join graph_path
    on arbs.x = graph_path.x
    where 
        --toDate(timestamp) >= '2024-10-15'
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        and tx_rank = 1

),

fb_blocks as (
    select
        block_number,
        max(if(lower(miner) = lower(from) 
            or lower(miner) in ('0xf2f5c73fa04406b1995e397b55c24ab1f3ea726c',
                                '0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5',
                                '0x690b9a9e9aa1c9db991c7721a92d351db4fac990',
                                '0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97',
                                '0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5',
                                '0x1f9090aae28b8a3dceadf281b0f12828e676c326',
                                '0xc15d2ba418631fc6c90f61fe4dd9a49229bd514f'),
            1,0)) as fb_block
    from {{ source('raw_external', 'txs_data') }}
    where
        --toDate(block_timestamp) >= '2024-10-15'
        toStartOfHour(block_timestamp) >= toStartOfHour(now() - toIntervalHour(12))
    group by 1
),

tradable_metrics as (
    select
        toStartOfHour(timestamp) as date,
        pair_type,
        host,
        chain,
        if(arb_ops.path = '', arb_ops.pair, arb_ops.path) as path,
        if(arb_ops.path = '', False, True) as is_graph,
        if(mined, 'mined','mempool') as trade_strategy,
        'between' as size_match,
        sum(size_usd) as arb_volume,
        sumIf(size_usd, fb_blocks.fb_block = 1 or fb_blocks.block_number = 0) as tradable_volume,
        count(hash) as ops_count
    from arb_ops
    left join fb_blocks
    on fb_blocks.block_number - 1 = arb_ops.block_number
    group by 1,2,3,4,5,6,7,8
),

traded_metrics as (
    select
        toStartOfHour(timestamp) as date,
        pair_type,
        host,
        chain,
        path,
        is_graph,
        trade_strategy,
        'between' as size_match,
        sumIf(gross_profit, not tx_failed) as gross_profit,
        sumIf(gross_profit_with_slippage, not tx_failed) as gross_profit_with_slippage,
        sumIf(gross_profit_with_hedge_prices, not tx_failed) as gross_profit_with_hedge_prices,
        sumIf(algo_fee, not tx_failed) as algo_fee,
        sumIf(roxana_deals.base_fee, tx_failed) as base_fee_failed,
        sumIf(roxana_deals.base_fee, not tx_failed) as base_fee,
        sumIf(bribe, not tx_failed) as bribe,
        sumIf(size_usd, not tx_failed) as traded_volume,
        countIf(tx_hash, not tx_failed) as deals,
        count(tx_hash) as txs
    from {{ ref('roxana_deals') }} as roxana_deals
    where
        --toDate(timestamp) >= '2024-10-15'
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
    group by 1,2,3,4,5,6,7,8
)

select 
    greatest(dex_volume.date,tradable_metrics.date,traded_metrics.date) as date_hour,
    greatest(dex_volume.pair_type,tradable_metrics.pair_type,traded_metrics.pair_type) as pair,
    greatest(ifNull(dex_volume.chain,''),ifNull(tradable_metrics.chain,''),ifNull(traded_metrics.chain,'')) as blockchain,
    greatest(ifNull(dex_volume.size_match,''),ifNull(tradable_metrics.size_match,''),ifNull(traded_metrics.size_match,'')) as dex_size,
    greatest(tradable_metrics.host,traded_metrics.host) as instance,
    greatest(tradable_metrics.trade_strategy,traded_metrics.trade_strategy) as strategy,
    greatest(tradable_metrics.path,traded_metrics.path) as trade_path,
    greatest(dex_volume.is_graph,tradable_metrics.is_graph,traded_metrics.is_graph) as graph_mark,
    {{ dbt_utils.generate_surrogate_key(['date_hour','pair','blockchain','dex_size','instance','trade_path','graph_mark','strategy']) }} as surr_key,
    max(ifNull(dex_volume.amountUSD, 0.00)) as dex_volume,
    sum(ifNull(tradable_metrics.arb_volume, 0.00)) as arb_volume,
    multiIf(
        blockchain in ('BSC','Arbitrum'), sum(ifNull(tradable_metrics.arb_volume, 0.00)),
        sum(ifNull(tradable_metrics.tradable_volume, 0.00))
    ) as tradable_volume,
    sum(tradable_metrics.ops_count) as ops_count,
    sum(ifNull(traded_metrics.traded_volume, 0.00)) as traded_volume,
    sum(ifNull(traded_metrics.gross_profit, 0.00)) as gross_profit,
    sum(traded_metrics.algo_fee) as algo_fee,
    sum(traded_metrics.base_fee) as base_fee,
    sum(traded_metrics.bribe) as bribe,
    sum(traded_metrics.deals) as deals,
    sum(traded_metrics.base_fee_failed) as base_fee_failed,
    sum(traded_metrics.txs) as txs,
    sum(traded_metrics.gross_profit_with_slippage) as gross_profit_with_slippage,
    sum(traded_metrics.gross_profit_with_hedge_prices) as gross_profit_with_hedge_prices
from dex_volume 
full outer join tradable_metrics
on dex_volume.date = tradable_metrics.date and dex_volume.pair_type = tradable_metrics.pair_type and dex_volume.size_match = tradable_metrics.size_match and dex_volume.chain = tradable_metrics.chain and dex_volume.is_graph = tradable_metrics.is_graph
full outer join traded_metrics
on dex_volume.date = traded_metrics.date and dex_volume.pair_type = traded_metrics.pair_type and dex_volume.size_match = traded_metrics.size_match and dex_volume.chain = traded_metrics.chain and traded_metrics.host = tradable_metrics.host and traded_metrics.is_graph = tradable_metrics.is_graph and traded_metrics.path = tradable_metrics.path and traded_metrics.trade_strategy = tradable_metrics.trade_strategy
group by 1,2,3,4,5,6,7,8