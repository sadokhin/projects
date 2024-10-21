{{ config(materialized='table') }}

WITH calls AS ( --all outbound calls to clients
  SELECT
    *
  FROM {{ ref('stg_amocrm_calls') }}
),

lead_contact as ( --contact and lead ids mapping
  SELECT id, client_language, CAST(lead_id as INT64) as lead_id
  FROM (
    SELECT id,CF_ID_1225251 as client_language, SPLIT(lead_ids, ',') AS lead_id
    FROM `intermark-analytics-prod.raw_data_amocrm.contacts`
  ), UNNEST(lead_id) AS lead_id
),

lead_calls as ( --all calls joined to leads
  SELECT distinct
    lead_id,
    created_at
  FROM lead_contact
  LEFT JOIN calls
  ON lead_contact.id = calls.entity_id
),

net_leads as ( -- leads with successful calls and incoming WA messages
  -- successful calls
  SELECT distinct
    lead_id,
    created_at
  FROM lead_contact
  INNER JOIN (SELECT * FROM calls WHERE params_duration > 0) as calls
  ON lead_contact.id = calls.entity_id

  UNION ALL

  -- incoming WA messages
  SELECT distinct
    entity_id,
    created_at
  FROM `intermark-analytics-prod.raw_data_amocrm.event_messages`
  WHERE 
    type = 'incoming_chat_message'
    AND entity_type = 'lead'
),

schedule_tasks as ( --all scheduled tasks with calls
  SELECT
    entity_id,
    complete_till,
    created_at,
    ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at) AS RN
  FROM `intermark-analytics-prod.raw_data_amocrm.tasks`
  WHERE
    entity_type = 'leads'
    AND REGEXP_CONTAINS(text, "Schedule a callback.")
    AND REGEXP_CONTAINS(text, "Make primary qualification.")
),

email as ( -- all tasks with email activity
  SELECT
    entity_id,
    created_at,
  FROM {{ ref('stg_amocrm_emails') }}
),

call_qc as (
  SELECT
    lead as lead_id,
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

leads as ( --joined all tables above as preparation for data mart
SELECT
  leads.id,
  leads.new_at_ts,
  leads.custom_new_at,
  leads.claimed_at_ts,
  leads.qualification_at_ts,  
  schedule_tasks.complete_till as scheduled_call_at,
  CASE WHEN net_leads.lead_id IS NOT NULL OR status_id = 142 THEN 1 ELSE 0 END as net_lead,
  CASE WHEN status_id = 142 THEN 1 ELSE 0 END as qualified_lead,
  CASE WHEN status_id = 143 THEN 1 ELSE 0 END as not_converted_lead,
  CASE 
    WHEN status_id = 142 THEN "Primary qualification"
    WHEN status_id = 143 THEN "Closed and not converted"
    WHEN net_leads.lead_id IS NOT NULL THEN "Net"
    ELSE "New / In progress"
  END as status,
  users.name as responsible,
  leads.request_country,
  leads.product_type,
  leads.mkt_source,
  CASE WHEN lead_contact.client_language IS NULL THEN "(unknown)" ELSE lead_contact.client_language END as request_language,
  CASE
    WHEN product_type = "Investments" AND client_language = "English" THEN "Investments EN"
    WHEN product_type = "Investments" AND client_language = "Russian" THEN "Investments RU"
    WHEN product_type = "Immigration" AND client_language = "English" THEN "Immigration EN"
    WHEN product_type = "Immigration" AND client_language = "Russian" THEN "Immigration RU"
    ELSE "(unknown)"
  END as product_language,
  schedule_tasks.created_at as ft_scheduled_call_at,
  MAX(call_score.score) as call_score,
  MAX(call_score.target_score) as target_call_score,
  MIN(lead_calls.created_at) as ft_call_at,
  MIN(email.created_at) as ft_email_at,
  MIN(net_leads.created_at) as net_lead_at,
  MIN(DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(net_leads.created_at), INTERVAL 3 HOUR))) as net_lead_date_at
FROM {{ ref('stg_amocrm_leads') }} leads
LEFT JOIN {{ ref('stg_amocrm_users') }} users
ON leads.responsible_user_id = users.id
LEFT JOIN `intermark-analytics-prod.raw_data_amocrm.amocrm_stages` stages
ON leads.status_id = stages.id AND leads.pipeline_id = stages.pipeline_id
LEFT JOIN `intermark-analytics-prod.raw_data_amocrm.amocrm_pipelines` pipelines
ON leads.pipeline_id = pipelines.id
LEFT JOIN net_leads
ON leads.id = net_leads.lead_id and (net_leads.created_at > leads.new_at_ts AND new_at_ts IS NOT NULL)
LEFT JOIN schedule_tasks
ON leads.id = schedule_tasks.entity_id AND RN = 1 AND schedule_tasks.complete_till > leads.claimed_at_ts AND schedule_tasks.complete_till - leads.claimed_at_ts < 86400
LEFT JOIN lead_calls
ON leads.id = lead_calls.lead_id AND lead_calls.created_at > (claimed_at_ts - 60)
LEFT JOIN email
ON leads.id = email.entity_id AND email.created_at > claimed_at_ts
LEFT JOIN call_score
ON leads.id = call_score.lead_id
LEFT JOIN (
  SELECT
    lead_id,
    MAX(client_language) as client_language
  FROM lead_contact
  GROUP BY 1
) as lead_contact ON leads.id = lead_contact.lead_id
WHERE
  leads.pipeline_id = 7305870
  AND REGEXP_EXTRACT(lower(reason_for_refusal),"duplicate|test") IS NULL
  AND DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(new_at_ts), INTERVAL 3 HOUR)) >= "2024-01-01"
  AND users.group_id = 461094
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

),

main as ( --count time of first touch by caller
  SELECT
    leads.id,
    CONCAT("https://mdkb.amocrm.ru/leads/detail/",id,'/') as link_to_lead,
    DATE(DATETIME_ADD(TIMESTAMP_SECONDS(new_at_ts), INTERVAL 3 HOUR)) as new_at,
    CASE 
      WHEN scheduled_call_at IS NULL THEN custom_new_at 
      ELSE DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(scheduled_call_at), INTERVAL 3 HOUR)) 
    END AS custom_new_at,
    custom_new_at as custom_new_wo_task_at,
    DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(claimed_at_ts), INTERVAL 3 HOUR)) as claimed_at,
    DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(qualification_at_ts), INTERVAL 3 HOUR)) as qualification_at,
    net_lead,
    qualified_lead,
    not_converted_lead,
    status,
    product_language,
    mkt_source,
    responsible,
    request_country,
    request_language,
    product_type,
    net_lead_date_at,
    ft_call_at,
    ft_email_at,
    target_call_score,
    call_score,
    DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(scheduled_call_at), INTERVAL 3 HOUR)) as scheduled_call_at,
    LEAST(
      COALESCE(ft_call_at,scheduled_call_at,ft_email_at),
      COALESCE(scheduled_call_at,ft_call_at,ft_email_at),
      COALESCE(ft_email_at,scheduled_call_at,ft_call_at)
    ) as ft_at
  FROM leads
)

-- main select for data mart creation
SELECT
  id,
  CONCAT("https://mdkb.amocrm.ru/leads/detail/",id,'/') as link_to_lead,
  DATE(new_at) as new_at,
  custom_new_at,
  custom_new_wo_task_at,
  claimed_at,
  DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(ft_at), INTERVAL 3 HOUR)) as ft_at,
  DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(ft_call_at), INTERVAL 3 HOUR)) as ft_call_at,
  scheduled_call_at,
  DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(ft_email_at), INTERVAL 3 HOUR)) as ft_email_at,
  qualification_at,
  net_lead,
  qualified_lead,
  not_converted_lead,
  status,
  product_language,
  responsible,
  request_country,
  request_language,
  product_type,
  mkt_source,
  net_lead_date_at,
  target_call_score,
  call_score,
  DATE_DIFF(qualification_at,custom_new_at,HOUR) as new_qualification_time,
  CASE WHEN DATE_DIFF(qualification_at,custom_new_at,HOUR) < 72 THEN 1 ELSE 0 END as new_qual_target,
  DATE_DIFF(net_lead_date_at,custom_new_at,HOUR) as first_touch_new,
  CASE WHEN DATE_DIFF(net_lead_date_at,custom_new_at,HOUR) < 48 THEN 1 ELSE 0 END as first_touch_new_target,
  DATE_DIFF(qualification_at,net_lead_date_at,HOUR) as net_qualification_time,
  CASE WHEN DATE_DIFF(net_lead_date_at,qualification_at,HOUR) < 24 THEN 1 ELSE 0 END as net_qual_target,
  DATE_DIFF(claimed_at,custom_new_wo_task_at,MINUTE) as claimed_time, 
  CASE WHEN DATE_DIFF(claimed_at,custom_new_wo_task_at,MINUTE) < 60 THEN 1 ELSE 0 END as claimed_15min_share,
  ABS(DATE_DIFF(DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(ft_at), INTERVAL 3 HOUR)),claimed_at,MINUTE)) as first_touch_claimed,
  CASE WHEN ABS(DATE_DIFF(DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(ft_at), INTERVAL 3 HOUR)),claimed_at,MINUTE)) < 5 THEN 1 ELSE 0 END as first_touch_claimed_share,
FROM main
