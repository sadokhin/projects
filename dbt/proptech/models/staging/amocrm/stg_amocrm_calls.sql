{{ config(materialized='view') }}

WITH calls AS ( --all outbound calls to clients
  SELECT distinct
    id,
    entity_id,
    created_at,
    created_by,
    CAST(SPLIT(params_duration, r".")[0] AS INT64) as params_duration
  FROM `intermark-analytics-prod.raw_data_amocrm.note_calls`
  WHERE 
    entity_type = "contacts"
    AND note_type = "call_out"
)

select * from calls