{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='(date_time)',
      unique_key='tx_hash',
      incremental_strategy='append'
    )
}}

with dex as (
    select 
        tx_hash,
        amount0Out - amount0In as amount0,
        amount1Out - amount1In as amount1,
        if(amount0 < 0, abs(amount0), abs(amount1)) AS amountIn,
        if(amount0 < 0, abs(amount1), abs(amount0)) AS amountOut,
        if(amount0 < 0, token1, token0) AS symbolOut,
        if(amount0 < 0, token0, token1) AS symbolIn,
        amountUSD,
        lower(from) as from,
        lower(to) as to,
        block_timestamp as date_time,
        lower(pool_id) as pool_id,
        dex,
        block,
        amount1In,
        amount0In,
        amount1Out,
        amount0Out,
        count() over (partition by tx_hash) as swaps
    from {{ source('raw_external', 'dex_data_swaps') }}
    where 
        toStartOfHour(block_timestamp) = toStartOfHour(now()) - toIntervalHour(1)
        and (dex like '%ARB%' or dex like '%ETH%')
),

-- Fetch transfers to define all tokens and amounts in transaction
token_amounts as (
    select 
        tx_hash,
        symbolIn,
        sum(amountIn) as amount
    from (
        select 
            tx_hash, symbolIn, amountIn
        from dex
        
        union all
        
        select
            tx_hash, symbolOut, -amountOut
        from dex
    )
    group by 1,2
),

token_arrays as (
    select
        tx_hash,
        arrayDistinct(groupArray(symbolIn)) as unique_tokens,
        groupArrayIf(symbolIn, amount = 0) as intermediate_tokens,
        groupArrayIf(symbolIn, amount > 0) AS tokens_in,
        groupArrayIf(symbolIn, amount < 0) AS tokens_out,
        groupArrayIf(amount, amount > 0) AS amounts_in,
        groupArrayIf(amount, amount < 0) AS amounts_out
    from token_amounts
    group by 1
),

--detect 'sandwich' txs
sandwich as (
    select
        dex.tx_hash as tx_hash
    from dex
    left join dex as sandwich
    on dex.pool_id = sandwich.pool_id
        and dex.to = sandwich.to
        and dex.block = sandwich.block
    where 
        dex.tx_hash != sandwich.tx_hash
        and dex.dex like '%ETH%'
        and (
            round(abs(dex.amountIn),1) = round(abs(sandwich.amountOut),1) or
            round(abs(dex.amountOut),1) = round(abs(sandwich.amountIn),1) or
            lower(dex.from) in (lower('0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80'),lower('0x1f2f10d1c40777ae1da742455c65828ff36df387'))
        )
    group by 1
),

-- get our robot chappie config
trading_config as (
    select 
        otcTicker,
        lower(pairAddress) as pairAddress,
        feeTier,
        max(tradingIsEnabled) as tradingIsEnabled,
        max(maxOrderSizeInQuote) as maxOrderSizeInQuote,
        min(minOrderSizeInQuote) as minOrderSizeInQuote
    from {{ source('raw_external', 'vault_trading_config') }}
    group by 1,2,3
),

-- join our robot config to dex swaps
dex_with_config as (
    select 
        dex.*,
        trading_config.otcTicker as otc_ticker,
        trading_config.tradingIsEnabled,
        lower(concat(replace(replace(dex.dex, 'ARB','_'),'ETH','_'),trading_config.feeTier)) as internal_dex_name,
        multiIf(
            dex.dex like '%ETH%', 'ETH',
            dex.dex like '%BSC%', 'BSC',
            dex.dex like '%ARB%', 'Arbitrum',
            ''
        ) as chain,
        multiIf(
            symbolOut in ('USDC','USDT','DAI'),symbolOut,
            symbolIn in ('USDC','USDT','DAI'),symbolIn,
            symbolOut in ('WBTC'),symbolOut,
            symbolIn in ('WBTC'),symbolIn,
            symbolOut in ('WETH'),symbolOut,
            symbolIn in ('WETH'),symbolIn,
            symbolOut
        ) as symbol_quote,
        multiIf(
            symbolOut in ('USDC','USDT','DAI'),symbolIn,
            symbolIn in ('USDC','USDT','DAI'),symbolOut,
            symbolOut in ('WBTC'),symbolIn,
            symbolIn in ('WBTC'),symbolOut,
            symbolOut in ('WETH'),symbolIn,
            symbolIn in ('WETH'),symbolOut,
            symbolIn
        ) as symbol_base,
        multiIf(
            symbolOut in ('USDC','USDT','DAI'),abs(amountOut),
            symbolIn in ('USDC','USDT','DAI'),abs(amountIn),
            symbolOut in ('WBTC'),abs(amountOut),
            symbolIn in ('WBTC'),abs(amountIn),
            symbolOut in ('WETH'),abs(amountOut),
            symbolIn in ('WETH'),abs(amountIn),
            abs(amountOut)
        ) as size_quote,
        multiIf(
            symbolOut in ('USDC','USDT','DAI'),abs(amountIn),
            symbolIn in ('USDC','USDT','DAI'),abs(amountOut),
            symbolOut in ('WBTC'),abs(amountIn),
            symbolIn in ('WBTC'),abs(amountOut),
            symbolOut in ('WETH'),abs(amountIn),
            symbolIn in ('WETH'),abs(amountOut),
            abs(amountIn)
        ) as size_base,
        concat(symbol_base,symbol_quote) as pair,
        if(concat(dex.symbolOut,dex.symbolIn) = pair, 'B', 'S') as dex_side,
        if(pair in ('ARBWETH','WETHUSDC','WETHUSDT','WETHDAI','WETHWBTC','WBTCUSDT','WBTCUSDC','WBTCDAI','WBNBUSDT','WBNBUSDC','WBNBETH'), 'Liquid', 'Shit') as pair_type,
        multiIf(
            size_quote < minOrderSizeInQuote and trading_config.pairAddress != '','less',
            size_quote > maxOrderSizeInQuote and trading_config.pairAddress != '','more',
            size_quote <= maxOrderSizeInQuote and size_quote >= minOrderSizeInQuote and trading_config.pairAddress != '','between',
            ''
        ) as size_match,
        if(trading_config.pairAddress = '',false,true) as our_pools
    from dex 
    left join trading_config
    on dex.pool_id = trading_config.pairAddress
)

select 
    dex.tx_hash as tx_hash,
    dex.amountUSD as amountUSD,
    dex.from as from,
    dex.to as to,
    dex.date_time as date_time,
    dex.pool_id as pool_id,
    dex.block as block,
    dex.swaps as swaps,
    dex.otc_ticker as otc_ticker,
    dex.tradingIsEnabled as tradingIsEnabled,
    dex.internal_dex_name as internal_dex_name,
    dex.chain as chain,
    dex.symbol_quote as symbol_quote,
    dex.symbol_base as symbol_base,
    dex.size_quote as size_quote,
    dex.size_base as size_base,
    dex.pair as pair,
    dex.dex_side as dex_side,
    dex.pair_type as pair_type,
    dex.size_match as size_match,
    dex.our_pools as our_pools,
    token_arrays.unique_tokens as unique_tokens,
    token_arrays.intermediate_tokens as intermediate_tokens,
    token_arrays.tokens_in as tokens_in,
    token_arrays.tokens_out as tokens_out,
    token_arrays.amounts_in as amounts_in,
    token_arrays.amounts_out as amounts_out,
    multiIf(
        sandwich.tx_hash != '', 'sandwich',
        swaps = 1 and length(unique_tokens) = 2 and length(tokens_in) = 1 and length(intermediate_tokens) = 0 and length(tokens_out) = 1, 'dex-cex',
        swaps = 2 and length(unique_tokens) = 3 and length(tokens_in) = 1 and length(intermediate_tokens) = 1 and length(tokens_out) = 1, 'graph',
        swaps > 1 and length(unique_tokens) in (2,3,4) and length(tokens_in) in (1,2) and length(intermediate_tokens) = 0 and length(tokens_out) in (1,2,3), 'dex-cex',
        swaps in (3,4) and length(unique_tokens) in (3,4) and length(tokens_in) = 1 and length(intermediate_tokens) = 1 and length(tokens_out) in (1,2), 'graph',
        swaps >= 2 and length(unique_tokens) >= 3 and length(tokens_in) = 1 and length(intermediate_tokens) = length(unique_tokens) - 2 and length(tokens_out) = 1, 'graph',
        swaps > 1 and length(unique_tokens) >= 2 and length(tokens_in) = 0 and length(intermediate_tokens) > 0 and length(tokens_out) = 1, 'dex-dex',
        swaps = 4 and length(unique_tokens) = 2 and length(tokens_in) = 1 and length(intermediate_tokens) = 1 and length(tokens_out) = 0, 'graph',
        'unknown'
    ) as strategy
from dex_with_config as dex
left join sandwich 
on dex.tx_hash = sandwich.tx_hash
left join token_arrays 
on token_arrays.tx_hash = dex.tx_hash