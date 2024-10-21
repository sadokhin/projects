{{ config(materialized='view') }}

WITH users AS (
  SELECT distinct
    id,
    name,
    CAST(rights_group_id as INT64) as group_id
  FROM `intermark-analytics-prod.raw_data_amocrm.users`
)

select * from users