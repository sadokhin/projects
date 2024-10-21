{{ config(materialized='view') }}

with successful_hedg_prod2_eth as (
    select
        'PROD2/FSN' as setup,
        'ETH' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_prod2.http_dumps 
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
),

successful_hedg_prod3_eth as (
    select
        'PROD3/LND' as setup,
        'ETH' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_prod3.http_dumps 
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
),

successful_hedg_qb_eth as (
    select
        'QB/LND' as setup,
        'ETH' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_qb.http_dumps 
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
),

successful_hedg_prod2_bsc as (
    select
        'PROD2/FSN' as setup,
        'BSC' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_prod2.bsc_http_dumps 
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
),

successful_hedg_qb_arbitrum as (
    select
        'QB/LND' as setup,
        'Arbitrum' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_qb.arbitrum_http_dumps 
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
),

successful_hedg_qb_eth_tokyo as (
    select
        'QB/TOKYO' as setup,
        'ETH' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_qb.eth_tokyo_http_dumps 
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
),

successful_hedg_qb_eth_graph as (
    select
        'QB/GRAPH' as setup,
        'ETH' as chain,
        instance,
        tx_hash,
        body,
        start,
        finish,
        post
    from raw_ch_qb.eth_graph_http_dumps
    where 
        toDate(start) >= toDate(now()) - 7
        and status = 200
        and (body like '%-r-3%' or body like '%_mf-0%' or body like '%_mb-0%') 
        and body like '%"success":true%'
        and JSON_VALUE(body, '$.result[0].type') = ''
)

select * from successful_hedg_prod2_eth

union all 

select * from successful_hedg_prod3_eth

union all 

select * from successful_hedg_qb_eth

union all 

select * from successful_hedg_prod2_bsc

union all 

select * from successful_hedg_qb_arbitrum

union all 

select * from successful_hedg_qb_eth_tokyo

