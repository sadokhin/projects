{{ config(materialized='view') }}

with trade_event_prod2_eth as (
    select
        'FSN' as setup,
        tag,
        event,
        instance,
        block_height,
        tx_hash, 
        time_delta*1000 as time_delta,
        time_app,
        value*1000 as value
    from raw_ch_prod2.ptc_metric_records
    where
        toDate(time_app) >= toDate(now()) - 7
        and event in ('FoundManagerTx','FoundTrade', 'HttpStart','SendOtc','ParseHash', 'ParseDone','NetworkDelay')
),

trade_event_prod3_eth as (
    select
        'LND' as setup,
        tag,
        event,
        instance,
        block_height,
        tx_hash, 
        time_delta*1000 as time_delta,
        time_app,
        value*1000 as value
    from raw_ch_prod3.ptc_metric_records
    where
        toDate(time_app) >= toDate(now()) - 7
        and event in ('FoundManagerTx','FoundTrade', 'HttpStart','SendOtc','ParseHash', 'ParseDone','NetworkDelay')
),

trade_event_qb_eth as (
    select
        'QB' as setup,
        tag,
        event,
        instance,
        block_height,
        tx_hash, 
        time_delta*1000 as time_delta,
        time_app,
        value*1000 as value
    from raw_ch_qb.ptc_metric_records
    where
        toDate(time_app) >= toDate(now()) - 7
        and event in ('FoundManagerTx','FoundTrade', 'HttpStart','SendOtc','ParseHash', 'ParseDone','NetworkDelay')
),

trade_event_prod2_bsc as (
    select
        'FSN' as setup,
        tag,
        event,
        instance,
        block_height,
        tx_hash, 
        time_delta*1000 as time_delta,
        time_app,
        value*1000 as value
    from raw_ch_prod2.bsc_ptc_metric_records
    where
        toDate(time_app) >= toDate(now()) - 7
        and event in ('FoundManagerTx','FoundTrade', 'HttpStart','SendOtc','ParseHash', 'ParseDone','NetworkDelay')
),

trade_event_qb_arbitrum as (
    select
        'QB' as setup,
        tag,
        event,
        instance,
        block_height,
        tx_hash, 
        time_delta*1000 as time_delta,
        time_app,
        value*1000 as value
    from raw_ch_qb.arbitrum_ptc_metric_records
    where
        toDate(time_app) >= toDate(now()) - 7
        and event in ('FoundManagerTx','FoundTrade', 'HttpStart','SendOtc','ParseHash', 'ParseDone','NetworkDelay')
)

select * from trade_event_prod2_eth

union all 

select * from trade_event_prod3_eth

union all 

select * from trade_event_qb_eth

union all 

select * from trade_event_prod2_bsc

union all 

select * from trade_event_qb_arbitrum