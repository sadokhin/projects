
{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      unique_key='tx_hash',
      incremental_strategy='delete+insert'
    )
}}

with fees as (
    select 
        user_comment, 
		maxIf(if(concat(currency_base,currency_quote) in ('WBTCETH','DAIETH','USDTETH','USDCETH','USDCWBTC','USDTWBTC','ETHNEXO','ETHRLC','ETHAAVE','ETHMATIC','ETHAPE','ETHLDO'),1/toFloat64(price),toFloat64(price)), user_deal_id like '%-r-3%' or user_deal_id like '%-r-1%') as hedge_cex_price,
		sumIf(toFloat64(price) * size, user_deal_id like '%_mb%' ) as bribe,
		sumIf(toFloat64(price) * size, user_deal_id like '%_mf%' ) as base_fee
    from (
        select distinct
            user_comment,
            currency_quote,
            currency_base,
            price,
            size,
            user_deal_id
        from {{ source('raw_ch_prod', 'otc_data') }}
        where
            (user_deal_id like '%_mb%' 
            or user_deal_id like '%_mf%'
            or user_deal_id like '%-r-3%'
            or user_deal_id like '%-r-1%')
            and toStartOfHour(ts) >= toStartOfHour(now() - toIntervalHour(12))
            --and toDate(ts) >= '2024-10-15'
    )
    group by 1
),

deals as (
    select distinct 
        hash,
        quote,
        base,
        quote_amount,
        base_amount
    from {{ source('raw_ch_prod', 'contract_data') }}
    where 
        pair!='fees'
        and pair != 'gross_profit'
        and toStartOfHour(block_timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        --and toDate(block_timestamp) >= '2024-10-15'
),

dex_data as (
    select 
        tx_hash,        
        block_number,
        pool_id,
        miner as miner,
        multiIf(
            tokens_out[1] in ('USDC','USDT','DAI'),abs(amounts_out[1]),
            tokens_in[1] in ('USDC','USDT','DAI'),abs(amounts_in[1]),
            tokens_out[1] in ('WBTC','WETH','WBNB'),abs(amounts_out[1]),
            tokens_in[1] in ('WBTC','WETH','WBNB'),abs(amounts_in[1]),
            abs(amounts_out[1])
        ) as size_quote,
        multiIf(
            tokens_out[1] in ('USDC','USDT','DAI'),abs(amounts_in[1]),
            tokens_in[1] in ('USDC','USDT','DAI'),abs(amounts_out[1]),
            tokens_out[1] in ('WBTC','WETH','WBNB'),abs(amounts_in[1]),
            tokens_in[1] in ('WBTC','WETH','WBNB'),abs(amounts_out[1]),
            abs(amounts_in[1])
        ) as size_base,
        if(strategy = 'graph',size_quote / size_base, dex_price) as dex_price,
        dex_side
    from {{ ref('roxana_rivals') }}
    where 
        tx_hash in (select hash from deals)
),

dex as (
    select
        tx_hash,
        max(block_number) as block_number,
        arrayDistinct(groupArray(lower(pool_id))) as pool_ids,
        max(miner) as miner,
        max(dex_price) as dex_price,
        max(dex_side) as dex_side
    from dex_data
    group by 1
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
        hash,
        block_number,
        timestamp,
        is_dex_buying,
        symbol_quote,
        pair,
        pair_type,
        mined,
        if(host = 'prod-qb-rox-eth-chappie-02', 'GIGACHAPPIE', setup) as setup,
        chain,
        host,
        price_dex,
        price_cex,
        price_btc_usd,
        price_eth_usd,
        size,
        size_usd,
        gross_profit,
        bribe_usd,
        graph_path.path as path
    from {{ ref('stg_arb_ops') }} as arbs 
    left join graph_path on arbs.x = graph_path.x
    where
        hash in (select hash from deals) 
        or hash in (select user_comment from fees)
),

chappie_blocks as (
    select
        host,
        block_number,
        min(timestamp) as block_timestamp
    from {{ ref('stg_chappie_block_received') }}
    where 
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        --toDate(timestamp) >= '2024-10-15'
    group by 1,2
),

roxana_deals as (
    select 
        if(deals.hash = '', True, False) as tx_failed,
        greatest(deals.hash, fees.user_comment) as tx_hash,
        arb_ops.timestamp as timestamp,
        arb_ops.pair as pair,
        arb_ops.pair_type as pair_type,
        arb_ops.setup as setup,
        arb_ops.host as host,
        arb_ops.chain as chain,
        if(arb_ops.path = '', arb_ops.pair, arb_ops.path) as path,
        if(arb_ops.path = '', False, True) as is_graph,
        timestamp_diff('s',chappie_blocks.block_timestamp,arb_ops.timestamp) as block_diff_sec,
        if(arb_ops.mined, 'mined','mempool') as trade_strategy,
        arb_ops.gross_profit as gross_profit,
        arb_ops.size_usd as size_usd, 
        case
            when deals.quote in ('USDT','USDC') then quote_amount*0.0002
            when deals.quote ='WBTC' then quote_amount*0.0002*arb_ops.price_btc_usd
            when deals.quote ='WETH' then quote_amount*0.0002*arb_ops.price_eth_usd
            when deals.base = 'WETH' then base_amount*0.0002*arb_ops.price_eth_usd
            else 0.0
        end as algo_fee,
        fees.base_fee as base_fee,
        fees.bribe as bribe,
        fees.hedge_cex_price as hedge_cex_price,
        dex.dex_price as dex_price_with_slippage,
        dex.dex_side as dex_side,
        dex.block_number as block_number,
        dex.miner as miner,
        dex.pool_ids as pool_ids,
        arb_ops.price_dex as dex_price,
        arb_ops.price_cex as cex_price,
        (dex_price_with_slippage-dex_price)/dex_price * 10000 as slippage,
        case 
            when arb_ops.is_dex_buying = 0 and arb_ops.symbol_quote not in ['WBTC','BTC','ETH'] then (toFloat64(dex_price_with_slippage) - toFloat64(arb_ops.price_cex)) * toFloat64(arb_ops.size) / toFloat64(dex_price_with_slippage)
            when arb_ops.is_dex_buying = 1 and arb_ops.symbol_quote not in ['WBTC','BTC','ETH'] then (toFloat64(arb_ops.price_cex) - toFloat64(dex_price_with_slippage)) * toFloat64(arb_ops.size)
            when arb_ops.is_dex_buying = 0 and arb_ops.symbol_quote in ['WBTC','BTC'] then (toFloat64(dex_price_with_slippage) - toFloat64(arb_ops.price_cex)) * toFloat64(arb_ops.size) / toFloat64(dex_price_with_slippage) * toFloat64(arb_ops.price_btc_usd)
            when arb_ops.is_dex_buying = 1 and arb_ops.symbol_quote in ['WBTC','BTC'] then (toFloat64(arb_ops.price_cex) - toFloat64(dex_price_with_slippage)) * toFloat64(arb_ops.size) * toFloat64(price_btc_usd)
            when arb_ops.is_dex_buying = 0 and arb_ops.symbol_quote ='ETH' then (toFloat64(dex_price_with_slippage) - toFloat64(arb_ops.price_cex)) * toFloat64(arb_ops.size) / toFloat64(dex_price_with_slippage) * toFloat64(arb_ops.price_eth_usd)
            when arb_ops.is_dex_buying = 1 and arb_ops.symbol_quote ='ETH' then (toFloat64(arb_ops.price_cex) - toFloat64(dex_price_with_slippage)) * toFloat64(arb_ops.size) * toFloat64(arb_ops.price_eth_usd)
        end as gross_profit_with_slippage,
        case 
            when arb_ops.is_dex_buying = 0 and arb_ops.symbol_quote not in ['WBTC','BTC','ETH'] then (toFloat64(dex_price_with_slippage) - toFloat64(hedge_cex_price)) * toFloat64(arb_ops.size) / toFloat64(dex_price_with_slippage)
            when arb_ops.is_dex_buying = 1 and arb_ops.symbol_quote not in ['WBTC','BTC','ETH'] then (toFloat64(hedge_cex_price) - toFloat64(dex_price_with_slippage)) * toFloat64(arb_ops.size)
            when arb_ops.is_dex_buying = 0 and arb_ops.symbol_quote in ['WBTC','BTC'] then (toFloat64(dex_price_with_slippage) - toFloat64(hedge_cex_price)) * toFloat64(arb_ops.size) / toFloat64(dex_price_with_slippage) * toFloat64(arb_ops.price_btc_usd)
            when arb_ops.is_dex_buying = 1 and arb_ops.symbol_quote in ['WBTC','BTC'] then (toFloat64(hedge_cex_price) - toFloat64(dex_price_with_slippage)) * toFloat64(arb_ops.size) * toFloat64(price_btc_usd)
            when arb_ops.is_dex_buying = 0 and arb_ops.symbol_quote ='ETH' then (toFloat64(dex_price_with_slippage) - toFloat64(hedge_cex_price)) * toFloat64(arb_ops.size) / toFloat64(dex_price_with_slippage) * toFloat64(arb_ops.price_eth_usd)
            when arb_ops.is_dex_buying = 1 and arb_ops.symbol_quote ='ETH' then (toFloat64(hedge_cex_price) - toFloat64(dex_price_with_slippage)) * toFloat64(arb_ops.size) * toFloat64(arb_ops.price_eth_usd)
        end as gross_profit_with_hedge_prices,
        row_number() over (partition by tx_hash order by arb_ops.bribe_usd desc) as rn
    from deals
    left join dex
    on deals.hash = dex.tx_hash
    full outer join fees
    on deals.hash = fees.user_comment
    left join arb_ops
    on deals.hash = arb_ops.hash or fees.user_comment = arb_ops.hash
    left join chappie_blocks
    on arb_ops.block_number = chappie_blocks.block_number and arb_ops.host = chappie_blocks.host
)

select
    tx_failed,
    tx_hash,
    timestamp,
    pair,
    pair_type,
    setup,
    chain,
    block_diff_sec,
    trade_strategy,
    gross_profit,
    size_usd, 
    algo_fee,
    base_fee,
    bribe,
    dex_price_with_slippage,
    dex_side,
    dex_price,
    cex_price,
    slippage,
    if(isNull(gross_profit_with_slippage) or abs(slippage)>100 or dex_price_with_slippage = 0,gross_profit,gross_profit_with_slippage) as gross_profit_with_slippage,
    block_number,
    miner,
    hedge_cex_price,
    if(isNull(gross_profit_with_hedge_prices) or abs(slippage)>100 or hedge_cex_price = 0,gross_profit,gross_profit_with_hedge_prices) as gross_profit_with_hedge_prices,
    path,
    is_graph,
    host,
    pool_ids
from roxana_deals
where 
    rn = 1
    and chain != ''