
{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      unique_key='surr_key',
      incremental_strategy='delete+insert'
    )
}}

with our_deals as (
    select 
        tx_hash as hash,
        tx_failed
    from {{ ref('roxana_deals') }}
    where 
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        --toDate(timestamp) >= '2024-10-15'
    group by 1,2
),

graph_ops as (
    select
        x,
        symbols_token_in.symbol as token_in,
        symbols_token_out.symbol as token_out,
        pool_id
    from {{ source('raw_ch_qb', 'chappie_graph_swaps') }} as swap_ops 
    left join {{ source('raw_external', 'token_addresses') }} as symbols_token_in
    on lower(swap_ops.token_in) = lower(symbols_token_in.address)
    left join {{ source('raw_external', 'token_addresses') }} as symbols_token_out
    on lower(swap_ops.token_out) = lower(symbols_token_out.address)
    where
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        --toDate(timestamp) >= '2024-10-15'
    group by 1,2,3,4
),

graph_path as (
    select 
        t1.x as x,
        max(arrayDistinct(array(t1.pool_id,t2.pool_id))) as pool_ids,
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
        x,
        is_dex_buying,
        block_number,
        size_usd,
        gross_profit,
        pair_type,
        replace(pair, 'ETH','WETH') as pair,
        lower(replace(source_dex,'_','')) as source_dex,
        if(is_dex_buying = 0, replace(symbol_base, 'ETH','WETH'), replace(symbol_quote, 'ETH','WETH')) as treasury_symbol,
        if(is_dex_buying = 0, size / price_dex, size * price_dex) as size,
        setup,
        host,
        mined,
        profit_usd,
        price_cex,
        price_dex,
        price_eth_usd,
        base_fee_usd as base_fee,
        bribe_usd as bribe,
        graph_path.path as path,
        graph_path.pool_ids as pool_ids,
        if(graph_path.path != '', 'graph','direct') as arb_strategy
    from {{ ref('stg_arb_ops') }} as arbs 
    left join graph_path
    on arbs.x = graph_path.x
    where 
        --toDate(timestamp) >= '2024-10-15'
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        and chain in ('ETH','Arbitrum')
        and setup != 'LND'
        and host != 'prod-qb-rox-eth-chappie-02'
),

rivals as (
    select 
        tx_hash,
        lower(replace(replace(internal_dex_name,'_',''),'ARB','')) as internal_dex_name,
        block_number,
        pair,
        size_quote,
        bribe_usd as bribe,
        base_fee,
        dex_price,
        amountUSD,
        flag,
        gross0bps,
        quote_price_usd,
        rival_cex_price_0bps,
        strategy,
        row_number() over (partition by internal_dex_name,block_number,pair order by amountUSD desc) as rn
    from {{ ref('roxana_rivals') }}
    where 
        --toDate(timestamp) >= '2024-10-15'
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
        and chain in ('ETH','Arbitrum')
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
                                '0x1f9090aae28b8a3dceadf281b0f12828e676c326'),
            1,0)) as fb_block
    from {{ source('raw_external', 'txs_data') }}
    where 
        --toDate(block_timestamp)>= '2024-10-15'
        toStartOfHour(block_timestamp) >= toStartOfHour(now() - toIntervalHour(12))
    group by 1
),

swarm_blocks as (
    select
        block_number,
        if(toDate(last_tx_at) = '1970-01-01',1,0) as private_block,
        last_tx_at + toIntervalSecond(1) as last_tx_at
    from {{ source('raw_ch_52', 'swarm') }}
),

main as (
    select
        swarm_blocks.last_tx_at as last_tx_in_swarm_ts,
        arb_ops.*,
        rivals.*,
        case
            when our_deals.tx_failed then 'our_failed_deal'
            when our_deals.hash != '' then 'our_deal'
            when arb_ops.bribe + arb_ops.base_fee < rivals.bribe + rivals.base_fee and rivals.flag = 'more_than_1swap' then 'more_than_1swap'
            when rivals.flag not in ('','no price', 'less size', 'our_deal','backrun','more_than_1swap') then rivals.flag
            when chain = 'ETH' and fb_blocks.fb_block = 0 and fb_blocks.block_number != 0 then 'not_fb_block'
            when chain = 'ETH' and swarm_blocks.private_block = 1 then 'private_block'
            when (arb_ops.is_dex_buying = 0 and arb_ops.price_cex > rival_cex_price_0bps and rivals.tx_hash != '')
                 or (arb_ops.is_dex_buying = 1 and arb_ops.price_cex < rival_cex_price_0bps and rivals.tx_hash != '') then 'not_our_market'
            when arb_ops.timestamp > swarm_blocks.last_tx_at and toDate(swarm_blocks.last_tx_at) != '1970-01-01' then 'late_in_block'
            when chain = 'ETH' and (fb_blocks.block_number = 0 or rivals.tx_hash != '' and rivals.bribe + rivals.base_fee = 0) then 'not_txs_data'
            when arb_ops.bribe + arb_ops.base_fee < rivals.bribe + rivals.base_fee 
                 and (arb_ops.is_dex_buying = 0 and arb_ops.price_dex < rivals.dex_price or arb_ops.is_dex_buying = 1 and arb_ops.price_dex > rivals.dex_price) then 'bad_dex_price'
            when rivals.bribe > arb_ops.bribe and arb_ops.base_fee > rivals.base_fee and arb_ops.base_fee - rivals.base_fee > rivals.bribe - arb_ops.bribe then 'overpaid_base_fee'
            when rivals.amountUSD > arb_ops.size_usd + 10_000 and rivals.bribe > arb_ops.bribe
                 or arb_ops.size_usd > rivals.amountUSD and rivals.bribe + rivals.base_fee > arb_ops.bribe + arb_ops.base_fee then 'bad_cex_price'
            when arb_ops.timestamp + toIntervalSecond(7) < swarm_blocks.last_tx_at and toDate(swarm_blocks.last_tx_at) != '1970-01-01' and arb_ops.bribe < rivals.bribe then 'start_of_the_block'
            when arb_ops.bribe + arb_ops.base_fee < rivals.bribe + rivals.base_fee
                 and arb_ops.gross_profit - rivals.bribe - rivals.base_fee >= arb_ops.size_usd * 2 / 10000 then 'not_optimal_bribe'
            when rivals.tx_hash = '' then 'no rival'
            else 'on_research'
        end as arb_ops_flag
    from arb_ops
    left join fb_blocks
    on fb_blocks.block_number = arb_ops.block_number + 1
    left join swarm_blocks
    on swarm_blocks.block_number = arb_ops.block_number + 1
    left join our_deals
    on arb_ops.hash = our_deals.hash
    left join rivals 
    on arb_ops.block_number = rivals.block_number - 1 and arb_ops.source_dex = rivals.internal_dex_name and arb_ops.pair = rivals.pair and rivals.rn = 1
),

arb_ops_with_rivals as (
    select
        last_tx_in_swarm_ts,
        arb_ops.timestamp as timestamp,
        arb_ops.chain as chain,
        arb_ops.hash as hash,
        arb_ops.x as x,
        arb_ops.is_dex_buying as is_dex_buying,
        arb_ops.block_number as block_number,
        arb_ops.size_usd as size_usd,
        arb_ops.gross_profit as gross_profit,
        arb_ops.pair_type as pair_type,
        arb_ops.pair as pair,
        lower(replace(arb_ops.source_dex,'_','')) as source_dex,
        arb_ops.setup as setup,
        arb_ops.host as host,
        arb_ops.mined as mined,
        arb_ops.profit_usd as profit_usd,
        arb_ops.price_cex as price_cex,
        arb_ops.price_dex as price_dex,
        arb_ops.bribe as bribe_usd,
        arb_ops.price_eth_usd as price_eth_usd,
        arb_ops.base_fee as base_fee,
        arb_ops.size as treasury_size,
        arb_ops.treasury_symbol as treasury_symbol,
        arb_ops.path as path,
        arb_ops.pool_ids as pool_ids,
        arb_ops.arb_strategy as arb_strategy,
        rivals.tx_hash as rival_hash,
        rivals.bribe as rival_bribe_usd,
        rivals.base_fee as rival_base_fee,
        rivals.dex_price as rival_dex_price,
        rivals.gross0bps as gross0bps,
        rivals.rival_cex_price_0bps as rival_cex_price_0bps,
        rivals.quote_price_usd as quote_price_usd,
        rivals.amountUSD as rival_size_usd,
        rivals.flag as rival_flag,
        rivals.strategy as rival_strategy,
        arb_ops_flag,
        row_number() over (partition by block_number,pair,is_dex_buying,source_dex order by bribe_usd desc) as arb_rn
    from main
),

mined_arb_ops as (
    select
        *,
        row_number() over (partition by hash order by bribe_usd desc) as tx_rn
    from arb_ops_with_rivals
    where arb_ops_flag = 'our_deal'
),

not_late_in_block_arb_ops as (
    select
        *,
        row_number() over (partition by block_number,pair,is_dex_buying,source_dex order by bribe_usd desc) as not_late_rn
    from arb_ops_with_rivals
    where arb_ops_flag != 'late_in_block'
),

finalize_ops as (
    select
        case
            when mined_arb_ops.x != '' then mined_arb_ops.last_tx_in_swarm_ts
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.last_tx_in_swarm_ts
            else arb_ops_with_rivals.last_tx_in_swarm_ts
        end as last_tx_in_swarm_ts,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.timestamp
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.timestamp
            else arb_ops_with_rivals.timestamp
        end as timestamp,
        coalesce(nullif(mined_arb_ops.chain,''),nullif(not_late_in_block_arb_ops.chain,''),nullif(arb_ops_with_rivals.chain,'')) as chain,
        coalesce(nullif(mined_arb_ops.hash,''),nullif(not_late_in_block_arb_ops.hash,''),nullif(arb_ops_with_rivals.hash,'')) as hash,
        coalesce(nullif(mined_arb_ops.x,''),nullif(not_late_in_block_arb_ops.x,''),nullif(arb_ops_with_rivals.x,'')) as x,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.is_dex_buying
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.is_dex_buying
            else arb_ops_with_rivals.is_dex_buying
        end as is_dex_buying,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.block_number
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.block_number
            else arb_ops_with_rivals.block_number
        end as block_number,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.size_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.size_usd
            else arb_ops_with_rivals.size_usd
        end as size_usd,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.gross_profit
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.gross_profit
            else arb_ops_with_rivals.gross_profit
        end as gross_profit,
        coalesce(nullif(mined_arb_ops.pair_type,''),nullif(not_late_in_block_arb_ops.pair_type,''),nullif(arb_ops_with_rivals.pair_type,'')) as pair_type,
        coalesce(nullif(mined_arb_ops.pair,''),nullif(not_late_in_block_arb_ops.pair,''),nullif(arb_ops_with_rivals.pair,'')) as pair,
        coalesce(nullif(mined_arb_ops.source_dex,''),nullif(not_late_in_block_arb_ops.source_dex,''),nullif(arb_ops_with_rivals.source_dex,'')) as source_dex,
        coalesce(nullif(mined_arb_ops.setup,''),nullif(not_late_in_block_arb_ops.setup,''),nullif(arb_ops_with_rivals.setup,'')) as setup,
        coalesce(nullif(mined_arb_ops.host,''),nullif(not_late_in_block_arb_ops.host,''),nullif(arb_ops_with_rivals.host,'')) as host,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.profit_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.profit_usd
            else arb_ops_with_rivals.profit_usd
        end as profit_usd,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.price_cex
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.price_cex
            else arb_ops_with_rivals.price_cex
        end as price_cex,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.price_dex
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.price_dex
            else arb_ops_with_rivals.price_dex
        end as price_dex,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.bribe_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.bribe_usd
            else arb_ops_with_rivals.bribe_usd
        end as bribe_usd,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.price_eth_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.price_eth_usd
            else arb_ops_with_rivals.price_eth_usd
        end as price_eth_usd,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.base_fee
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.base_fee
            else arb_ops_with_rivals.base_fee
        end as base_fee,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.treasury_size
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.treasury_size
            else arb_ops_with_rivals.treasury_size
        end as treasury_size,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.treasury_symbol
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.treasury_symbol
            else arb_ops_with_rivals.treasury_symbol
        end as treasury_symbol,
        coalesce(nullif(mined_arb_ops.rival_hash,''),nullif(not_late_in_block_arb_ops.rival_hash,''),nullif(arb_ops_with_rivals.rival_hash,'')) as rival_hash,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.rival_bribe_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.rival_bribe_usd
            else arb_ops_with_rivals.rival_bribe_usd
        end as rival_bribe_usd,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.rival_base_fee
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.rival_base_fee
            else arb_ops_with_rivals.rival_base_fee
        end as rival_base_fee,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.rival_dex_price
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.rival_dex_price
            else arb_ops_with_rivals.rival_dex_price
        end as rival_dex_price,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.gross0bps
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.gross0bps
            else arb_ops_with_rivals.gross0bps
        end as gross0bps,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.rival_cex_price_0bps
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.rival_cex_price_0bps
            else arb_ops_with_rivals.rival_cex_price_0bps
        end as rival_cex_price_0bps,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.quote_price_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.quote_price_usd
            else arb_ops_with_rivals.quote_price_usd
        end as quote_price_usd,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.rival_size_usd
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.rival_size_usd
            else arb_ops_with_rivals.rival_size_usd
        end as rival_size_usd,
        coalesce(nullif(mined_arb_ops.rival_flag,''),nullif(not_late_in_block_arb_ops.rival_flag,''),nullif(arb_ops_with_rivals.rival_flag,'')) as rival_flag,
        coalesce(nullif(mined_arb_ops.arb_ops_flag,''),nullif(not_late_in_block_arb_ops.arb_ops_flag,''),nullif(arb_ops_with_rivals.arb_ops_flag,'')) as arb_ops_flag,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.mined
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.mined
            else arb_ops_with_rivals.mined
        end as mined_strategy,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.path
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.path
            else arb_ops_with_rivals.path
        end as path,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.pool_ids
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.pool_ids
            else arb_ops_with_rivals.pool_ids
        end as pool_ids,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.arb_strategy
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.arb_strategy
            else arb_ops_with_rivals.arb_strategy
        end as arb_strategy,
        case
            when mined_arb_ops.x != '' then mined_arb_ops.rival_strategy
            when not_late_in_block_arb_ops.x != '' then not_late_in_block_arb_ops.rival_strategy
            else arb_ops_with_rivals.rival_strategy
        end as rival_strategy
    from arb_ops_with_rivals
    left join mined_arb_ops
    on arb_ops_with_rivals.block_number = mined_arb_ops.block_number
        and arb_ops_with_rivals.pair = mined_arb_ops.pair
        and arb_ops_with_rivals.is_dex_buying = mined_arb_ops.is_dex_buying
        and arb_ops_with_rivals.source_dex = mined_arb_ops.source_dex
        and mined_arb_ops.tx_rn = 1
    left join not_late_in_block_arb_ops
    on arb_ops_with_rivals.block_number = not_late_in_block_arb_ops.block_number
        and arb_ops_with_rivals.pair = not_late_in_block_arb_ops.pair
        and arb_ops_with_rivals.is_dex_buying = not_late_in_block_arb_ops.is_dex_buying
        and arb_ops_with_rivals.source_dex = not_late_in_block_arb_ops.source_dex
        and arb_ops_with_rivals.arb_ops_flag = 'late_in_block'
        and not_late_rn = 1
    where arb_ops_with_rivals.arb_rn = 1
),

arb_ops_main as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['block_number','pair','is_dex_buying','source_dex']) }} as surr_key
    from finalize_ops
),

balances as (
    select 
        arrayEnumerate(token_symbols) as indexes, 
        timestamp,
        token_symbols, 
        token_amounts,
        block,
        multiIf(
            protocol in ('uni','uni-fw'), 'ETH',
            protocol in ('arbitrum','arbitrum-fw'), 'Arbitrum',
            null
        ) as chain
    from 
        {{ source('raw_ch_prod', 'bi_historical') }} 
    where 
        --toDate(timestamp) >= '2024-10-15'
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(13))
        and protocol in ('uni','arbitrum','uni-fw','arbitrum-fw')
),

treasury as (
    select
        timestamp,
        block,
        chain,
        if(arrayElement(token_symbols, index) = 'ETH', 'WETH',arrayElement(token_symbols, index)) as token,
        sum(arrayElement(token_amounts, index)) as treasury
    from balances
    array join 
        indexes as index
    group by 1,2,3,4
),

arb_ops_treasury as (
    select 
        arb_ops_main.*,
        treasury.treasury as treasury_balance,
        row_number() over (partition by surr_key order by timestamp_diff('s', treasury.timestamp,arb_ops_main.timestamp)) as treasury_rn
    from arb_ops_main
    left join treasury
    on arb_ops_main.treasury_symbol = treasury.token and arb_ops_main.chain = treasury.chain 
    where 
        (arb_ops_main.timestamp > treasury.timestamp or treasury.token = '')
        and (treasury_balance != 0 or treasury.token = '')

),

treasury_reserve as (
    select 
        * EXCEPT treasury_rn,
        sum(treasury_size) over (partition by block_number, treasury_symbol order by timestamp) as treasury_reserve
    from arb_ops_treasury where treasury_rn = 1
)

select 
    * EXCEPT (path,pool_ids,arb_strategy,rival_strategy),
    if(treasury_reserve > treasury_balance and treasury_balance != 0, 'treasury_failed','treasury_available') as treasury_mark,
    path,pool_ids,arb_strategy,rival_strategy
from treasury_reserve