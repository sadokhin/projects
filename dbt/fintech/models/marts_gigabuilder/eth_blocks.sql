{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      unique_key='block_number',
      incremental_strategy='delete+insert'
    )
}}

with giga_blocks as (
    select
        block_number,
        set_at,
        tx_hash,
        eth_validator_reward as giga_validator_reward,
        row_number() over (partition by block_number order by eth_validator_reward desc) as rn
    from {{ source('raw_data_aqua', 'lnk_block_txs') }}
    where 
        --toDate(set_at) >= '2024-10-15'
        toStartOfHour(set_at) >= toStartOfHour(now() - toIntervalHour(8))
),

giga_sets_txs as (
    select
        block_number,
        set_at,
        giga_validator_reward,
        arrayJoin(tx_hash) as hash
    from giga_blocks
    where 
        rn = 1
),

giga_txs as (
    select
        tx_at,
        tx_hash,
        flow
    from {{ source('raw_data_aqua', 'txs_pool_w_flow') }}
    where 
        --toDate(tx_at) >= '2024-10-15'
        toStartOfHour(tx_at) >= toStartOfHour(now() - toIntervalHour(8))
),

swarm_txs as (
    select
        hash
    from {{ source('raw_ch_52', 'swarm_txs') }}
    where
        --toDate(received_at) >= '2024-10-15'
        toStartOfHour(received_at) >= toStartOfHour(now() - toIntervalHour(8))
    group by 1
),

roxana_txs as (
    select
        hash,
        if(host = 'prod-qb-rox-eth-chappie-02', 'gigachappie','main_flow') as roxana_flow,
        max(bribe) / pow(10,18) as bribe
    from {{ source('raw_ch_qb', 'chappie_arb_ops') }}
    where 
        --toDate(timestamp) >= '2024-10-15'
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(8))
    group by 1,2
    
    union all 
    
    select
        hash,
        'main_flow' as roxana_flow,
        max(bribe) / pow(10,18) as bribe
    from {{ source('raw_ch_qb', 'eth_tokyo_chappie_arb_ops') }}
    where
        --toDate(timestamp) = '2024-10-15' 
        toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(8))
    group by 1
),

mined_blocks as (
    select
        block_timestamp,
        eth_txs.block_number as block_number,
        lower(miner) as miner,
        max(if(lower(miner) = lower(from) or lower(from) in (lower('0x0AFfB0a96FBefAa97dCe488DfD97512346cf3Ab8'),lower('0xa83114a443da1cecefc50368531cace9f37fcccb'),lower('0x9FC3da866e7DF3a1c57adE1a97c9f00a70f010c8'),lower('0x2FA24F71e5f685C60e08f675dA6B3bA856D4cF7c'),lower('0xC6CeF345d50D1956af04Ffd8E82b05B6498Eb1Eb')), to, '0x0000000000000000000000000000000000000000')) as validator,
        countIf(distinct eth_txs.tx_hash,lower(to) in (lower('0xef97b8a6cbb72feeccf5bc5e897078e9e53ee0a4'),lower('0x817648d73Fd85c802cfdE8a29Eba9F68b783cA60'),lower('0x94edcc255890d0e62ea09c6f6c7b6121429a1db2'),lower('0x5e29a6a526b65035f6c1bbd39456ede06a858265'))) as roxana_txs,
        countIf(distinct eth_txs.tx_hash,lower(from) = lower('0x963737c550e70ffe4d59464542a28604edb2ef9a')) as hostel_txs,
        count(distinct eth_txs.tx_hash) as txs,
        max(if(lower(miner) = lower(from) or lower(from) in (lower('0x0AFfB0a96FBefAa97dCe488DfD97512346cf3Ab8'),lower('0xa83114a443da1cecefc50368531cace9f37fcccb'),lower('0x9FC3da866e7DF3a1c57adE1a97c9f00a70f010c8'),lower('0x2FA24F71e5f685C60e08f675dA6B3bA856D4cF7c'),lower('0xC6CeF345d50D1956af04Ffd8E82b05B6498Eb1Eb')), value, 0)) as validator_reward,
        max(if(lower(miner) = lower(from) or lower(from) in (lower('0x0AFfB0a96FBefAa97dCe488DfD97512346cf3Ab8'),lower('0xa83114a443da1cecefc50368531cace9f37fcccb'),lower('0x9FC3da866e7DF3a1c57adE1a97c9f00a70f010c8'),lower('0x2FA24F71e5f685C60e08f675dA6B3bA856D4cF7c'),lower('0xC6CeF345d50D1956af04Ffd8E82b05B6498Eb1Eb')), gas_price * gas_used, 0)) as fees_for_validator_reward,
        sum(bribe) as bribe,
        sum(gas_price * gas_used) as txs_fees,
        max(base_fee) * sum(gas_used) as burnt_fees,
        countIf(distinct eth_txs.tx_hash, giga_sets_txs.hash != '') as giga_seen_from_set_txs,
        countIf(distinct eth_txs.tx_hash, giga_txs.tx_hash != '') as giga_seen_txs,
        countIf(distinct eth_txs.tx_hash, swarm_txs.hash = '') as private_txs,
        sumIf(distinct eth_txs.gas_price * eth_txs.gas_used + eth_txs.bribe - eth_txs.base_fee * eth_txs.gas_used, swarm_txs.hash = '') as private_txs_reward
    from {{ source('raw_external', 'txs_data') }} as eth_txs
    left join giga_sets_txs
        on eth_txs.tx_hash = giga_sets_txs.hash
    left join giga_txs
        on eth_txs.tx_hash = giga_txs.tx_hash
    left join swarm_txs
        on eth_txs.tx_hash = swarm_txs.hash
    where
        --toDate(block_timestamp) = '2024-10-15'
        toStartOfHour(block_timestamp) >= toStartOfHour(now() - toIntervalHour(8))
    group by 1,2,3
),

giga_intime_block as (
    select  
        giga_blocks.block_number as block_number, 
        max(giga_validator_reward) as giga_intime_validator_reward
    from giga_blocks
    inner join mined_blocks
        on giga_blocks.block_number = mined_blocks.block_number
    where 
        block_timestamp >= set_at
    group by 1
),

eth_price as (
    select
        floor(block_number / 1000) as blocks,
        avg(price_eth_usd) as price
    from {{ ref('stg_arb_ops') }}
    where 
        blocks in (select floor(block_number / 1000) from mined_blocks group by 1)
    group by 1
),

roxana_deals as (
    select
        block_number,
        sum(gross_profit_with_hedge_prices) - sum(base_fee) - sum(bribe) as net,
        sum(size_usd) as traded_volume
    from {{ ref('roxana_deals') }}
    where 
        block_number in (select block_number from mined_blocks)
        and not tx_failed
        and chain = 'ETH'
    group by 1
),

giga_block_info as (
    select
        giga_sets_txs.block_number as block_number,
        max(giga_sets_txs.set_at) as set_at,
        max(giga_sets_txs.giga_validator_reward) as giga_bid_validator_reward,
        max(giga_intime_block.giga_intime_validator_reward) as giga_intime_bid_validator_reward,
        count(distinct giga_sets_txs.hash) as giga_bid_txs,
        countIf(distinct giga_sets_txs.hash, giga_txs.flow = 1) as giga_bid_private_txs,
        countIf(distinct giga_sets_txs.hash, roxana_txs.roxana_flow = 'main_flow') as giga_bid_roxana_txs,
        countIf(distinct giga_sets_txs.hash, roxana_txs.roxana_flow = 'gigachappie') as giga_bid_gigachappie_txs,
        sumIf(distinct roxana_txs.bribe, roxana_txs.roxana_flow = 'main_flow') as giga_bid_roxana_bribe,
        sumIf(distinct roxana_txs.bribe, roxana_txs.roxana_flow = 'gigachappie') as giga_bid_gigachappie_bribe
    from giga_sets_txs
    left join giga_txs
    on giga_sets_txs.hash = giga_txs.tx_hash
    left join roxana_txs
    on giga_sets_txs.hash = roxana_txs.hash
    left join giga_intime_block
    on giga_sets_txs.block_number = giga_intime_block.block_number
    group by 1
),

main as (
    select 
        block_timestamp,
        mined_blocks.block_number as block_number,
        miner,
        roxana_txs,
        hostel_txs,
        txs,
        validator_reward,
        bribe,
        txs_fees,
        burnt_fees,
        eth_price.price as price_eth_usd,
        roxana_deals.net as roxana_net_profit,
        roxana_deals.traded_volume as roxana_traded_volume,
        validator,
        fees_for_validator_reward,
        if(validator_reward = 0 and lower(miner) not in (lower('0x1f9090aae28b8a3dceadf281b0f12828e676c326'),lower('0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5'),lower('0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97')), true,false) as is_public_block,
        private_txs,
        private_txs_reward,
        giga_seen_from_set_txs,
        giga_seen_txs,
        set_at as giga_bid_timestamp,
        giga_bid_validator_reward,
        giga_intime_bid_validator_reward,
        giga_bid_txs,
        giga_bid_private_txs,
        giga_bid_roxana_txs,
        giga_bid_gigachappie_txs,
        giga_bid_roxana_bribe,
        giga_bid_gigachappie_bribe,
        multiIf(
            lower(miner) = lower('0xC15d2Ba418631FC6c90f61fe4dD9A49229BD514F'), 'our_block',
            giga_block_info.block_number = 0, 'not_build',
            giga_intime_bid_validator_reward = 0, 'late_bid',
            giga_intime_bid_validator_reward < validator_reward or is_public_block and giga_intime_bid_validator_reward < bribe + txs_fees - burnt_fees, 'lower_reward',
            is_public_block and giga_intime_bid_validator_reward > bribe + txs_fees - burnt_fees, 'rejected_bid',
            'on_research'
        ) as flag
    from mined_blocks
    left join giga_block_info
    on mined_blocks.block_number = giga_block_info.block_number
    left join eth_price
    on floor(mined_blocks.block_number / 1000) = eth_price.blocks
    left join roxana_deals
    on mined_blocks.block_number = roxana_deals.block_number
)

select * from main 