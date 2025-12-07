{{ config(materialized='table') }}

WITH source AS (
    SELECT
        data:"COMPLAINT_catego ry"::string      AS complaint_category,
        data:"Unnamed: 0"::number               AS record_id,
        data:"agent ID"::number                 AS agent_id,
        data:"call ID"::string                  AS call_id,
        data:"callLogsGenerationDate"::date     AS call_logs_generation_date,
        data:"call_end_time"::timestamp         AS call_end_time,
        data:"call_start_time"::timestamp       AS call_start_time,
        data:"customeR iD"::string              AS customer_id,
        data:"resolutionstatus"::string         AS resolution_status,
        data:"source_file"::string              AS source_file,
        data:"ingestion_timestamp"::timestamp   AS ingestion_timestamp,
        data:"load_timestamp"::timestamp        AS load_timestamp
    FROM {{ source('core_telecoms_raw', 'call_center_logs_raw') }}
)

SELECT * FROM source
