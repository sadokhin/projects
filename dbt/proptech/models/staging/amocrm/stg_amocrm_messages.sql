{{ config(materialized='view') }}

WITH messages AS (
  SELECT distinct
    id,
    entity_id,
    talk_id,
    created_at
  FROM `intermark-analytics-prod.raw_data_amocrm.event_messages`
  WHERE entity_type = 'lead'
)

select * from messages