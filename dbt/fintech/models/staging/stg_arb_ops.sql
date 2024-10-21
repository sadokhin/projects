{{ config(materialized='view') }}

with eth_arb_ops_prod2 as (
    select distinct
        'FSN' as setup,
        'ETH' as chain,
        host,
        timestamp,
        hash,
        block_number,
        concat(symbol_base, symbol_quote) as pair,
        source_dex,
        profit_usd,
        price_btc_usd,
        price_eth_usd,
        mined,
        x,
        price_cex,
        is_canceled,
        is_dex_buying,
        symbol_quote,
        symbol_base,
        price_dex,
        size,
        gas_used,
        gas_price,
        bribe
    from {{ source('raw_ch_prod2', 'chappie_arb_ops') }}
    where
        hash != 'dryRun'
        and is_canceled = False
        and toDate(timestamp) >= toDate(now()) - 7
),

eth_arb_ops_prod3 as (
    select distinct
        'LND' as setup,
        'ETH' as chain,
        host,
        timestamp,
        hash,
        block_number,
        concat(symbol_base, symbol_quote) as pair,
        source_dex,
        profit_usd,
        price_btc_usd,
        price_eth_usd,
        mined,
        x,
        price_cex,
        is_canceled,
        is_dex_buying,
        symbol_quote,
        symbol_base,
        price_dex,
        size,
        gas_used,
        gas_price,
        bribe
    from {{ source('raw_ch_prod3', 'chappie_arb_ops') }}
    where
        hash != 'dryRun'
        and is_canceled = False
        and toDate(timestamp) >= toDate(now()) - 7
),

eth_arb_ops_qb as (
    select distinct
        'QB' as setup,
        'ETH' as chain,
        host,
        timestamp,
        hash,
        block_number,
        concat(symbol_base, symbol_quote) as pair,
        source_dex,
        profit_usd,
        price_btc_usd,
        price_eth_usd,
        mined,
        x,
        price_cex,
        is_canceled,
        is_dex_buying,
        symbol_quote,
        symbol_base,
        price_dex,
        size,
        gas_used,
        gas_price,
        bribe
    from {{ source('raw_ch_qb', 'chappie_arb_ops') }}
    where
        hash != 'dryRun'
        and is_canceled = False
        and toDate(timestamp) >= toDate(now()) - 7
),

bsc_arb_ops_prod2 as (
    select distinct
        'FSN' as setup,
        'BSC' as chain,
        host,
        timestamp,
        hash,
        block_number,
        concat(symbol_base, symbol_quote) as pair,
        source_dex,
        profit_usd,
        price_btc_usd,
        price_eth_usd,
        mined,
        x,
        price_cex,
        is_canceled,
        is_dex_buying,
        symbol_quote,
        symbol_base,
        price_dex,
        size,
        gas_used,
        gas_price,
        bribe
    from {{ source('raw_ch_prod2', 'bsc_chappie_arb_ops') }}
    where
        hash != 'dryRun'
        and is_canceled = False
        and toDate(timestamp) >= toDate(now()) - 7
),

arbitrum_arb_ops_qb as (
    select distinct
        'QB' as setup,
        'Arbitrum' as chain,
        host,
        timestamp,
        hash,
        block_number,
        concat(symbol_base, symbol_quote) as pair,
        if(source_dex = '','uniswap_V3_500', source_dex) as source_dex,
        profit_usd,
        price_btc_usd,
        price_eth_usd,
        mined,
        x,
        price_cex,
        is_canceled,
        is_dex_buying,
        symbol_quote,
        symbol_base,
        price_dex,
        size,
        gas_used,
        gas_price,
        bribe
    from {{ source('raw_ch_qb', 'arbitrum_chappie_arb_ops') }}
    where
        hash != 'dryRun'
        and is_canceled = False
        and toDate(timestamp) >= toDate(now()) - 7
),

eth_tokyo_arb_ops_qb as (
    select distinct
        'TOKYO' as setup,
        'ETH' as chain,
        host,
        timestamp,
        hash,
        block_number,
        concat(symbol_base, symbol_quote) as pair,
        source_dex,
        profit_usd,
        price_btc_usd,
        price_eth_usd,
        mined,
        x,
        price_cex,
        is_canceled,
        is_dex_buying,
        symbol_quote,
        symbol_base,
        price_dex,
        size,
        gas_used,
        gas_price,
        bribe
    from {{ source('raw_ch_qb', 'eth_tokyo_chappie_arb_ops') }}
    where
        hash != 'dryRun'
        and is_canceled = False
        and toDate(timestamp) >= toDate(now()) - 7
),

all_arb_ops as (
    select * from eth_arb_ops_prod2

    union all

    select * from eth_arb_ops_prod3

    union all 

    select * from eth_arb_ops_qb

    union all 

    select * from bsc_arb_ops_prod2

    union all 

    select * from arbitrum_arb_ops_qb

    union all 
    
    select * from eth_tokyo_arb_ops_qb
),

arb_ops as (
    select 
        *,
        case 
            when is_dex_buying = 0 and symbol_quote not in ['WBTC','BTC','ETH'] then toFloat64(size)
            when is_dex_buying = 1 and symbol_quote not in ['WBTC','BTC','ETH'] then toFloat64(size) * toFloat64(price_cex)
            when is_dex_buying = 0 and symbol_quote in ['WBTC','BTC'] then toFloat64(size) * toFloat64(price_btc_usd)
            when is_dex_buying = 1 and symbol_quote in ['WBTC','BTC'] then toFloat64(price_cex) * toFloat64(size) * toFloat64(price_btc_usd)
            when is_dex_buying = 0 and symbol_quote ='ETH' then toFloat64(size) * toFloat64(price_eth_usd)
            when is_dex_buying = 1 and symbol_quote ='ETH' then toFloat64(price_cex) * toFloat64(size) * toFloat64(price_eth_usd)
        end as size_usd,
        case 
            when is_dex_buying = 0 and symbol_quote not in ['WBTC','BTC','ETH'] then (toFloat64(price_dex) - toFloat64(price_cex)) * toFloat64(size) / toFloat64(price_dex)
            when is_dex_buying = 1 and symbol_quote not in ['WBTC','BTC','ETH'] then (toFloat64(price_cex) - toFloat64(price_dex)) * toFloat64(size)
            when is_dex_buying = 0 and symbol_quote in ['WBTC','BTC'] then (toFloat64(price_dex) - toFloat64(price_cex)) * toFloat64(size) / toFloat64(price_dex) * toFloat64(price_btc_usd)
            when is_dex_buying = 1 and symbol_quote in ['WBTC','BTC'] then (toFloat64(price_cex) - toFloat64(price_dex)) * toFloat64(size) * toFloat64(price_btc_usd)
            when is_dex_buying = 0 and symbol_quote ='ETH' then (toFloat64(price_dex) - toFloat64(price_cex)) * toFloat64(size) / toFloat64(price_dex) * toFloat64(price_eth_usd)
            when is_dex_buying = 1 and symbol_quote ='ETH' then (toFloat64(price_cex) - toFloat64(price_dex)) * toFloat64(size) * toFloat64(price_eth_usd)
        end as gross_profit,
        if(pair in ('WETHWBTC','BTCUSDC','WBTCUSDC','WBTCUSDT','BTCUSDT','BTCDAI','ETHUSDT','ETHBTC','ETHWBTC','ETHUSDC','ETHDAI','BNBUSDT','BNBUSDC'),'Liquid','Shit') as pair_type,
        gas_used * gas_price * pow(10, -18) * price_eth_usd as base_fee_usd,
        bribe * pow(10, -18) * price_eth_usd as bribe_usd,
        row_number() over (partition by block_number,pair,is_dex_buying,source_dex order by bribe_usd desc) as tx_rank
    from all_arb_ops
    

)

select * from arb_ops