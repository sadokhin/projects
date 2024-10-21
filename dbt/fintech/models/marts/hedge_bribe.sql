{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='bribe_processed_at',
      unique_key='tx_hash',
      incremental_strategy='delete+insert'
    )
}}

with hedge as (
    select 
        *
    from {{ ref('stg_successful_hedge') }}
    where body like '%_mb-0%'
    and toStartOfHour(start) >= toStartOfHour(now() - toIntervalHour(12))

),

events as (
    select
        time_app,
        setup,
        tx_hash,
        block_height,
        instance,
        tag,
        event,
        time_delta,
        row_number() over (partition by tx_hash,setup,tag,event order by time_app) as rnk
    from {{ ref('stg_orbit_trade_events') }}
    where 
        event = 'FoundManagerTx'
        and toStartOfHour(time_app) >= toStartOfHour(now() - toIntervalHour(12))
),

arb_ops as (
    select 
        host,
        hash
    from {{ ref('stg_arb_ops') }}
    where toStartOfHour(timestamp) >= toStartOfHour(now() - toIntervalHour(12))
    group by 1,2
),

main as (
    select
        events.tx_hash as tx_hash,
        events.block_height as block_height,
        events.instance as orbit_instance,
        hedge.setup as setup,
        arb_ops.host as chappie_instance,
        events.time_delta as manager_found,
        events.time_app as manager_found_at,
        hedge.start as bribe_processed_at,
        hedge.finish as answer_received_at,
        (toUnixTimestamp64Micro(hedge.start) - toUnixTimestamp64Micro(events.time_app)) / 1000 as bribe_processed,
        (toUnixTimestamp64Micro(hedge.finish) - toUnixTimestamp64Micro(hedge.start)) / 1000 as answer_received,
        hedge.chain as chain
    from events
    inner join hedge
    on events.tx_hash = hedge.tx_hash
    left join arb_ops
    on events.tx_hash = arb_ops.hash
    where rnk = 1
)

select
    *
from main
where bribe_processed > 0 
