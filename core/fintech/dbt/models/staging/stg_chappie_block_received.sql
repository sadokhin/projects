{{ config(materialized='view') }}

with eth_chappie_block_received_prod2 as (
    select
        'FSN' as setup,
        'ETH' as chain,
        host,
        block_number,
        min(timestamp) as timestamp
    from {{ source('raw_ch_prod2', 'prod_chappie_new_block_received') }}
    group by 1,2,3,4
),

eth_chappie_block_received_prod3 as (
    select
        'LND' as setup,
        'ETH' as chain,
        host,
        block_number,
        min(timestamp) as timestamp
    from {{ source('raw_ch_prod3', 'prod_chappie_new_block_received') }}
    group by 1,2,3,4
),

eth_chappie_block_received_qb as (
    select
        'QB' as setup,
        'ETH' as chain,
        host,
        block_number,
        min(timestamp) as timestamp
    from {{ source('raw_ch_qb', 'prod_chappie_new_block_received') }}
    group by 1,2,3,4
),

bsc_chappie_block_received_prod2 as (
    select
        'FSN' as setup,
        'BSC' as chain,
        host,
        block_number,
        min(timestamp) as timestamp
    from {{ source('raw_ch_prod2', 'bsc_prod_chappie_new_block_received') }}
    group by 1,2,3,4
),

arbitrum_chappie_block_received_qb as (
    select
        'QB' as setup,
        'Arbitrum' as chain,
        host,
        block_number,
        min(timestamp) as timestamp
    from {{ source('raw_ch_qb', 'arbitrum_prod_chappie_new_block_received') }}
    group by 1,2,3,4
),

eth_chappie_block_received_tokyo as (
    select
        'TOKYO' as setup,
        'ETH' as chain,
        host,
        block_number,
        min(timestamp) as timestamp
    from {{ source('raw_ch_qb', 'eth_tokyo_prod_chappie_new_block_received') }}
    group by 1,2,3,4
)

select * from eth_chappie_block_received_prod2

union all 

select * from eth_chappie_block_received_prod3

union all 

select * from eth_chappie_block_received_qb

union all 

select * from bsc_chappie_block_received_prod2

union all 

select * from arbitrum_chappie_block_received_qb

union all 

select * from eth_chappie_block_received_tokyo

