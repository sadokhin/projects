{{ config(materialized='table') }}

WITH calls AS ( --all outbound calls to clients
  SELECT distinct
    CAST(id as STRING) as id,
    --DATE(DATETIME_ADD(TIMESTAMP_SECONDS(created_at), INTERVAL 3 HOUR)) as created_at,
    created_at,
    created_by,
    entity_id,
    CASE WHEN params_duration > 0 THEN 'successful_call' ELSE 'unsuccessful_call' END as event_name,
    row_number() over (partition by entity_id,created_by order by created_at) as rn
  FROM intermark-analytics-prod.dbt_staging.stg_amocrm_calls
),

lead_contact as ( --contact and lead ids mapping
  SELECT id, CAST(lead_id as INT64) as lead_id
  FROM (
    SELECT id,CF_ID_1225251 as client_language, SPLIT(lead_ids, ',') AS lead_id
    FROM `intermark-analytics-prod.raw_data_amocrm.contacts`
  ), UNNEST(lead_id) AS lead_id
),

gross_leads as (
  SELECT
    CAST(entity_id as STRING) as id,
    created_at,
    CAST(responsible_after as INT64) as responsible_after,
    entity_id,
    'get_lead' as event_name,
    row_number() over (partition by entity_id,responsible_after order by created_at) as rn
  FROM `intermark-analytics-prod.raw_data_amocrm.responsible_changed_history`
),

messages AS (
  SELECT distinct
    CAST(talk_id as STRING) as talk_id,
    --DATE(DATETIME_ADD(TIMESTAMP_SECONDS(messages.created_at), INTERVAL 3 HOUR)) as created_at,
    messages.created_at,
    leads.responsible_user_id,
    messages.entity_id,
    'wa_dialog' as event_name
  FROM intermark-analytics-prod.dbt_staging.stg_amocrm_messages messages
  LEFT JOIN intermark-analytics-prod.dbt_staging.stg_amocrm_leads leads
  ON messages.entity_id = leads.id
  WHERE DATE(DATETIME_ADD(TIMESTAMP_SECONDS(messages.created_at), INTERVAL 3 HOUR)) >= claimed_at 
  AND (qualification_at IS NOT NULL AND qualification_at <= DATE(DATETIME_ADD(TIMESTAMP_SECONDS(messages.created_at), INTERVAL 3 HOUR))
      OR qualification_at IS NULL)
),

email as ( -- all tasks with email activity
  SELECT
    CAST(id as STRING) as id,
    --DATE(DATETIME_ADD(TIMESTAMP_SECONDS(created_at), INTERVAL 3 HOUR)) as created_at,
    created_at,
    responsible_user_id,
    entity_id,
    'out_email' as event_name
  FROM intermark-analytics-prod.dbt_staging.stg_amocrm_emails
), 

status_changes as ( -- all tasks with email activity
  SELECT
    distinct
    CAST(id as STRING) as id,
    --DATE(DATETIME_ADD(TIMESTAMP_SECONDS(created_at), INTERVAL 3 HOUR)) as created_at,
    created_at,
    created_by,
    entity_id,
    CASE
      WHEN status_after = 142 THEN 'qual_stage'
      WHEN status_after = 143 THEN 'not_converted_stage'
      WHEN status_after = 60847938 THEN 'claimed'
      ELSE 'status_changed' 
    END as event_name
  FROM `intermark-analytics-prod.raw_data_amocrm.lead_status_history`
  WHERE CONCAT(status_after,status_before) != '6084793446170721'
),

users AS (
  SELECT
    id,
    name,
    group_id
  FROM intermark-analytics-prod.dbt_staging.stg_amocrm_users
),

call_qc as (
  SELECT
    lead as lead_id,
    date,
    caller,
    CAST(REGEXP_REPLACE(`Greeting`,"-",'0') as FLOAT64) as greeting,
    CAST(REGEXP_REPLACE(`Primary qualification`,"-",'0') as FLOAT64) as pr_qual,
    CAST(REGEXP_REPLACE(`Additional qualification`,"-",'0') as FLOAT64) as add_qual,
    CAST(REGEXP_REPLACE(`Invitation to UM`,"-",'0') as FLOAT64) as inv_to_um,
    CAST(REGEXP_REPLACE(`Goodbye`,"-",'0') as FLOAT64) as goodbye,
    CAST(REGEXP_REPLACE(`Speech`,"-",'0') as FLOAT64) as speech,
    CAST(REGEXP_REPLACE(`Emotional tone and politeness`,"-",'0') as FLOAT64) as tone,
    ARRAY_LENGTH(regexp_extract_all(CONCAT(`Greeting`,`Primary qualification`,`Additional qualification`,`Invitation to UM`,`Goodbye`,`Speech`,`Emotional tone and politeness`), "-")) as missings
  FROM `intermark-analytics-prod.raw_data.excel_calls_report`
),

call_score as (
  SELECT
    caller,
    date,
    lead_id,
    greeting,
    pr_qual,
    add_qual,
    inv_to_um,
    goodbye,
    speech,
    tone,
    greeting+pr_qual+add_qual+inv_to_um+goodbye+speech+tone as score,
    7 - missings as target_score
  FROM call_qc
),

main AS (
    SELECT * FROM calls

    UNION ALL

    SELECT * FROM gross_leads

    UNION ALL

    SELECT *, 0 as rn FROM messages

    UNION ALL

    SELECT *, 0 as rn FROM email

    UNION ALL

    SELECT *, 0 as rn FROM status_changes
),

final as (

SELECT
    DATE(DATETIME_ADD(TIMESTAMP_SECONDS(main.created_at), INTERVAL 3 HOUR)) as created_date,
    main.created_at as created_at_ts,
    entity_id,
    created_by as responsible_id,
    users.name as user_name,
    leads.paid_unpaid as paid_unpaid,
    leads.lead_type as lead_type,
    leads.specific_marketing_type as specific_marketing_type,
    leads.activity_type as activity_type,
    leads.specific_activity as specific_activity,
    leads.lead_language as lead_language,
    leads.product_type as product_type,
    leads.mkt_source as mkt_source,
    CASE WHEN event_name in ('successful_call', 'unsuccessful_call') THEN entity_id ELSE lead_contact.id END as contact_id,
    CASE WHEN event_name = 'successful_call' THEN 1 ELSE 0 END as successful_call,
    CASE WHEN event_name = 'unsuccessful_call' THEN 1 ELSE 0 END as unsuccessful_call,
    CASE WHEN event_name = 'wa_dialog' THEN 1 ELSE 0 END as wa_dialog,
    CASE WHEN event_name = 'out_email' THEN 1 ELSE 0 END as out_email,
    CASE WHEN event_name = 'status_changed' THEN 1 ELSE 0 END as status_changed,
    CASE WHEN event_name = 'get_lead' THEN 1 ELSE 0 END as gross_lead,
    CASE WHEN event_name = 'successful_call' AND rn = 1 THEN 1 ELSE 0 END as net_lead,
    CASE WHEN event_name = 'claimed' THEN 1 ELSE 0 END as claimed,
    CASE WHEN event_name = 'qual_stage' THEN 1 ELSE 0 END as qual_stage,
    CASE WHEN event_name = 'not_converted_stage' THEN 1 ELSE 0 END as not_converted_stage,
    0 as score,
    0 as target_score
FROM main
LEFT JOIN users ON main.created_by = users.id
LEFT JOIN intermark-analytics-prod.dbt_staging.stg_amocrm_leads leads
ON main.entity_id = leads.id and main.entity_id != 0 and event_name not in ('successful_call', 'unsuccessful_call')
LEFT JOIN lead_contact
ON main.entity_id = lead_contact.lead_id and event_name not in ('successful_call', 'unsuccessful_call')
WHERE 
  DATE(DATETIME_ADD(TIMESTAMP_SECONDS(main.created_at), INTERVAL 3 HOUR)) >= "2024-01-01"
  AND users.group_id = 461094
  --AND (leads.pipeline_id = 7305870 OR leads.pipeline_id IS NULL)

UNION ALL

SELECT 
  PARSE_DATE('%d.%m.%Y', date),
  0,
  lead_id,
  0,
  caller,
  leads.paid_unpaid as paid_unpaid,
  leads.lead_type as lead_type,
  leads.specific_marketing_type as specific_marketing_type,
  leads.activity_type as activity_type,
  leads.specific_activity as specific_activity,
  leads.lead_language as lead_language,
  leads.product_type as product_type,
  leads.mkt_source as mkt_source,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  score,
  target_score
FROM call_score
LEFT JOIN intermark-analytics-prod.dbt_staging.stg_amocrm_leads leads
ON call_score.lead_id = leads.id
),

metrics as (
  select
    distinct
    final.*,
    case when successful_call = 1 or unsuccessful_call = 1 then concat('https://mdkb.amocrm.ru/contacts/detail/',final.entity_id,'/') else concat('https://mdkb.amocrm.ru/leads/detail/',final.entity_id,'/') end as link_to_amo,
    case when claimed = 1 then (final.created_at_ts - gross_leads.created_at) / 60 else null end as claimed_seconds,
    case when qual_stage = 1 then (final.created_at_ts - gross_leads.created_at) / 3600 else null end as qual_seconds,
    case when net_lead = 1 then (final.created_at_ts - gross_leads.created_at) / 3600 else null end as net_seconds,
    case when qual_stage = 1 then (final.created_at_ts - calls.created_at) / 3600 else null end as net_qual_seconds,

  from final
  left join gross_leads
  on final.entity_id = gross_leads.entity_id 
    and gross_leads.rn = 1 
    and (final.qual_stage = 1 or final.claimed = 1 or final.net_lead = 1) 
    and final.responsible_id = gross_leads.responsible_after 
    and gross_leads.created_at < final.created_at_ts
  left join calls 
  on final.contact_id = calls.entity_id 
    and final.responsible_id = calls.created_by 
    and calls.rn = 1
    and calls.created_at < final.created_at_ts
    and (final.qual_stage = 1) 
)

select 
  created_date,
  entity_id,
  responsible_id,
  user_name,
  contact_id,
  link_to_amo,
  paid_unpaid,
  lead_type,
  specific_marketing_type,
  activity_type,
  specific_activity,
  lead_language,
  product_type,
  mkt_source,
  sum(case when claimed_seconds <= 60 then 1 else 0 end) as claimed_target,
  max(claimed_seconds) as claimed_seconds,
  max(qual_seconds) as qual_seconds,
  max(net_seconds) as net_seconds,
  max(net_qual_seconds) as net_qual_seconds,
  sum(successful_call) as successful_call,
  sum(unsuccessful_call) as unsuccessful_call,
  sum(wa_dialog) as wa_dialog,
  sum(out_email) as out_email,
  sum(status_changed) as status_changed,
  sum(gross_lead) as gross_lead,
  sum(net_lead) as net_lead,
  sum(claimed) as claimed,
  sum(qual_stage) as qual_stage,
  sum(not_converted_stage) as not_converted_stage,
  max(score) as score,
  max(target_score) as target_score
from metrics
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
