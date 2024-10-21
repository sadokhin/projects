{{ config(materialized='view') }}

with dex as (
    select 
        tx_hash,
        from,
        to,
        toFloat64(amountUSD) as amountUSD,
        block_timestamp as date_time,
        lower(pool_id) as pool_id,
        token0,
        token1,
        replace(dex,'ETH','') as dex,
        block,
        amount1In,
        amount0In,
        amount1Out,
        amount0Out,
        concat(token0, token1) AS pair_first_var,
        concat(token1, token0) AS pair_second_var,
        amount0Out - amount0In as amount0,
        amount1Out - amount1In as amount1,
        if(amount0 < 0, abs(amount0), abs(amount1)) AS amountIn,
        if(amount0 < 0, abs(amount1), abs(amount0)) AS amountOut,
        if(amount0 < 0, token1, token0) AS symbolOut,
        if(amount0 < 0, token0, token1) AS symbolIn
    from {{ source('raw_external', 'dex_data_swaps') }}
    where 
        toDate(block_timestamp) >= toDate(now()) - 7
),


trading_config as (
    select 
        pair,
        otcTicker,
        lower(pairAddress) as pairAddress,
        feeTier,
        max(tradingIsEnabled) as tradingIsEnabled,
        min(minPremiumBps) as minPremiumBps,
        max(host) as host,
        max(maxOrderSizeInQuote) as maxOrderSizeInQuote,
        min(minOrderSizeInQuote) as minOrderSizeInQuote
    from {{ source('raw_external', 'vault_trading_config') }} 
    group by 1,2,3,4

)

select 
    dex.*,
    trading_config.pair as pair,
    trading_config.otcTicker as otc_ticker,
    trading_config.tradingIsEnabled,
    trading_config.pairAddress,
    trading_config.minPremiumBps,
    concat(dex.dex,trading_config.feeTier) as internal_dex_name,
    if(concat(dex.symbolOut,dex.symbolIn) = trading_config.pair, 'B', 'S') as dex_side,
    if(concat(dex.symbolOut,dex.symbolIn) = trading_config.pair, symbolOut, symbolIn) as symbol_base,
    if(concat(dex.symbolOut,dex.symbolIn) = trading_config.pair, symbolIn, symbolOut) as symbol_quote,
    multiIf(
        trading_config.host like '%eth%', 'ETH',
        trading_config.host like '%bsc%', 'BSC',
        trading_config.host like '%arbitrum%', 'Arbitrum',
        null
    ) as chain,
    if(trading_config.pair in ('ARBWETH','WETHUSDC','WETHUSDT','WETHDAI','WETHWBTC','WBTCUSDT','WBTCUSDC','WBTCDAI','WBNBUSDT','WBNBUSDC','WBNBETH'), 'Liquid', 'Shit') as pair_type,
    multiIf(
        token0 in ('USDC','USDT','DAI'),abs(amount0),
        token1 in ('USDC','USDT','DAI'),abs(amount1),
        token0 in ('WBTC','WETH','WBNB'),abs(amount0),
        token1 in ('WBTC','WETH','WBNB'),abs(amount1),
        null
    ) as size_pair,
    multiIf(
        token0 in ('USDC','USDT','DAI'),abs(amount1),
        token1 in ('USDC','USDT','DAI'),abs(amount0),
        token0 in ('WBTC','WETH','WBNB'),abs(amount1),
        token1 in ('WBTC','WETH','WBNB'),abs(amount0),
        null
    ) as size_base,
    multiIf(
        size_pair < minOrderSizeInQuote and trading_config.host != '','less',
        size_pair > maxOrderSizeInQuote and trading_config.host != '','more',
        size_pair <= maxOrderSizeInQuote and size_pair >= minOrderSizeInQuote and trading_config.host != '','between',
        null
    ) as size_match
from dex 
left join trading_config
on 
    (dex.pool_id = trading_config.pairAddress and dex.pair_first_var = trading_config.pair) 
    or (dex.pool_id = trading_config.pairAddress and dex.pair_second_var = trading_config.pair)
