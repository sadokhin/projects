{{ 
    config (
        order_by='(block_timestamp)', 
        engine='MergeTree()', 
        materialized='incremental', 
        unique_key='tx_hash', 
        incremental_strategy='append'
    ) 
}}

with liquidations as (
    select 
        *,
        'AAVEv2' as protocol,
        'ETH' as chain
    from {{ source('raw_ch_prod', 'taffy_canonical_liquidations_aave_chainlink') }}

    union all

    select 
        *,
        'AAVEv3' as protocol,
        'ETH' as chain
    from {{ source('raw_ch_prod', 'taffy_canonical_liquidations_aave_v3_chainlink') }}

    union all

    select 
        *,
        'COMP' as protocol,
        'ETH' as chain
    from {{ source('raw_ch_prod', 'taffy_canonical_liquidations_chainlink') }}

    union all

    select 
        *,
        'MAKER' as protocol,
        'ETH' as chain
    from {{ source('raw_ch_prod', 'taffy_canonical_liquidations_maker_chainlink') }}

    union all

    select 
        *,
        'AAVEv2' as protocol,
        'AVAX' as chain
    from {{ source('raw_ch_prod', 'canonical_aave_liquidation_cost_v2_avalanches') }}

    union all

    select 
        *,
        'AAVEv3' as protocol,
        'AVAX' as chain
    from {{ source('raw_ch_prod', 'canonical_aave_liquidation_cost_v3_avalanches') }}

    union all

    select 
        *,
        'VENUS' as protocol,
        'BSC' as chain
    from {{ source('raw_ch_prod', 'canonical_venus_liquidation_cost_v2_binances') }}
   
)

select 
    * EXCEPT block_height,
    '' as bug,
    '' as comment,
    block_height as block_number
from liquidations

{% if is_incremental() %}

-- this filter will only be applied on an incremental run
where block_timestamp > (select max(block_timestamp) from {{this}})

{% endif %}