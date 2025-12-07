{{ config(materialized='table') }}

WITH source AS (
    SELECT
        data:"request_id"::string                         AS request_id,
        data:"customeR iD"::string                        AS customer_id,
        data:"agent ID"::number                           AS agent_id,

        INITCAP(data:"COMPLAINT_catego ry"::string)       AS complaint_category,

        -- SAFE DATE & TIMESTAMP CASTS
        TRY_TO_TIMESTAMP(NULLIF(data:"request_date"::string, ''))          AS request_date,
        TRY_TO_TIMESTAMP(NULLIF(data:"resolution_date"::string, ''))       AS resolution_date,
        TRY_TO_DATE(NULLIF(data:"webFormGenerationDate"::string, ''))      AS webform_generation_date,

        data:"resolutionstatus"::string                   AS resolution_status,
        data:"source_file"::string                        AS source_file,

        -- ingestion timestamp (raw sometimes numeric)
        TRY_TO_TIMESTAMP_NTZ(data:"ingestion_timestamp"::string)     AS ingestion_timestamp,

        TRY_TO_TIMESTAMP_NTZ(NULLIF(data:"load_timestamp"::string, '')) AS load_timestamp

    FROM {{ source('core_telecoms_raw', 'website_complaints_raw') }}
)

SELECT * FROM source
