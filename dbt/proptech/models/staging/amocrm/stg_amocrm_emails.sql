{{ config(materialized='view') }}

WITH email as ( -- all tasks with email activity
  SELECT
    id,
    entity_id,
    responsible_user_id,
    created_at
  FROM `intermark-analytics-prod.raw_data_amocrm.tasks`
  WHERE
    entity_type = 'leads'
    AND result_text = "Email sent."
)

select * from email