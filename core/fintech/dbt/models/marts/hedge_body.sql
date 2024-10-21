{{
    config (
      materialized='incremental',
      engine='MergeTree()',
      order_by='hedge_start',
      unique_key='tx_hash',
      incremental_strategy='delete+insert'
    )
}}

with hedge as (
    select 
        *,
        parseDateTime64BestEffortOrNull(JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.price_timestamp')) as price_ts,
        parseDateTime64BestEffortOrNull(JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.exchanged_at')) as arb_ops_ts,
        JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.ticker_index') as ticker,
        JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.pricer') as pricer,
        JSON_VALUE(JSON_VALUE(post, '$.additional_info'),'$.pricer_version') as pricer_version
    from {{ ref('stg_successful_hedge') }}
    where 
        body like '%-r-3%'
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
        value,
        row_number() over (partition by tx_hash, block_height, setup,tag,event order by time_app) as rnk
    from {{ ref('stg_orbit_trade_events') }}
    where 
        tag in ('SubscribeHeaders','Realtime','Chappie','ChappieMrk','ChappieRfq','ChappieRfqMrk')
        and toStartOfHour(time_app) >= toStartOfHour(now() - toIntervalHour(12))
),

mrk_rfk_hedge as (
    select 
        user_comment,
        if(user_deal_id like '%back%', 'REORG',type) as custom_type, 
        row_number() over (partition by user_comment order by ts desc) as rn
    from {{ source('raw_ch_prod', 'otc_data') }}
    where 
        (user_deal_id like '%-r-3%' OR user_deal_id like '%-r1back%')
        and toStartOfHour(ts) >= toStartOfHour(now() - toIntervalHour(12))
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
        tx_events.tx_hash as tx_hash,
        tx_events.block_height as block_height,
        tx_events.instance as orbit_instance,
        tx_events.setup as setup,
        hedge.setup as hedge_setup,
        hedge.chain as chain,
        (toUnixTimestamp64Micro(hedge.finish) - toUnixTimestamp64Micro(hedge.start)) / 1000 as answer_received,
        hedge.start as hedge_start,
        case when tx_events.event = 'HttpStart' then tx_events.time_app end as tx_sent_at,
        date_diff('u',tx_sent_at,hedge_start) as d_diff,
        case when tx_events.event = 'HttpStart' then tx_events.time_delta end as tx_sent,
        mrk_rfk_hedge.custom_type as tx_type,
        arb_ops.host as chappie_instance,
        row_number() over (partition by tx_events.tx_hash order by abs(date_diff('u',tx_sent_at,hedge_start))) as rn_tx_sent,
        max(case when tx_events.event = 'FoundTrade' and tx_events.rnk = 1 then tx_events.time_delta else 0 end) as trade_found,
        max(case when tx_events.event = 'SendOtc' and tx_events.rnk = 1 then tx_events.time_delta else 0 end) as hedge_courier_received,
        max(case when block_events.event = 'ParseHash' and block_events.rnk = 1 then block_events.time_delta else 0 end) as parsed_start,
        max(case when block_events.event = 'NetworkDelay' and block_events.rnk = 1 then block_events.value else 0 end) as log_recieved,
        max(case when block_events.event = 'ParseDone' and block_events.rnk = 1 then block_events.time_delta else 0 end) as parsed_finished
    from events as tx_events
    inner join hedge
    on tx_events.tx_hash = hedge.tx_hash
    left join events as block_events
    on tx_events.block_height = block_events.block_height and tx_events.instance = block_events.instance
    left join mrk_rfk_hedge
    on tx_events.tx_hash = mrk_rfk_hedge.user_comment and rn = 1
    left join arb_ops
    on tx_events.tx_hash = arb_ops.hash
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select
    main.tx_hash as tx_hash,
    main.block_height as block_height,
    main.orbit_instance as orbit_instance,
    main.hedge_setup as setup,
    main.answer_received as answer_received,
    main.hedge_start as hedge_start,
    main.tx_sent_at as tx_sent_at,
    main.tx_sent as tx_sent,
    main.tx_type as tx_type,
    main.chappie_instance as chappie_instance,
    main_metrics.trade_found as trade_found,
    main_metrics.hedge_courier_received as hedge_courier_received,
    main_metrics.parsed_start as parsed_start,
    main_metrics.log_recieved as log_recieved,
    main_metrics.parsed_finished as parsed_finished,
    main.chain as chain
from main
left join main as main_metrics
on main.tx_hash = main_metrics.tx_hash and main.block_height = main_metrics.block_height and main.orbit_instance = main_metrics.orbit_instance
where
    main.rn_tx_sent = 1
    and isNull(main_metrics.tx_sent_at)
