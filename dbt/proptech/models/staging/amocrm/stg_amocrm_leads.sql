{{ config(materialized='view') }}

with leads as (
    SELECT
      id,
      DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(created_at), INTERVAL 3 HOUR)) as created_at,
      DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235651), INTERVAL 3 HOUR)) as new_at,
      CASE 
        WHEN EXTRACT(HOUR FROM DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235651), INTERVAL 3 HOUR))) < 10 THEN DATETIME(DATE(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235651), INTERVAL 3 HOUR)),TIME(10,0,0))
        WHEN EXTRACT(HOUR FROM DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235651), INTERVAL 3 HOUR))) > 20 THEN DATETIME(DATE(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235651), INTERVAL 1 DAY)),TIME(10,0,0))
        ELSE DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235651), INTERVAL 3 HOUR))
      END as custom_new_at,
      DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235653), INTERVAL 3 HOUR)) as claimed_at,
      DATETIME(DATETIME_ADD(TIMESTAMP_SECONDS(CF_ID_1235661), INTERVAL 3 HOUR)) as qualification_at,
      CF_ID_1235661 as qualification_at_ts,
      CF_ID_1235653 as claimed_at_ts,
      CF_ID_1235651 as new_at_ts,
      CF_ID_1218693 as request_country,
      CF_ID_1227213 as reason_for_refusal,
      CF_ID_1235205 as paid_unpaid,
      CF_ID_1237555 as lead_type,
      CF_ID_1236019 as specific_marketing_type,
      CF_ID_1237569 as activity_type,
      CF_ID_1236021 as specific_activity,
      CF_ID_1235209 as lead_language,
      CASE WHEN CF_ID_1236015 IS NULL THEN "(unknown)" ELSE CF_ID_1236015 END as product_type,
      CF_ID_1221953 as lead_source,
      CASE WHEN CF_ID_1236011 IS NULL THEN "(unknown)" ELSE CF_ID_1236011 END as mkt_source,
      status_id,
      responsible_user_id,
      pipeline_id
    FROM `intermark-analytics-prod.raw_data_amocrm.leads`
)

select * from leads
