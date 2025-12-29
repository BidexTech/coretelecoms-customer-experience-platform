-- {{ config(materialized='table') }}

-- WITH source AS (
--     SELECT
--         data:"complaint_id"::string                         AS complaint_id,
--         data:"customeR iD"::string                          AS customer_id,
--         data:"agent ID"::number                             AS agent_id,
--         INITCAP(data:"COMPLAINT_catego ry"::string)         AS complaint_category,
--         data:"media_channel"::string                        AS media_channel,
--         data:"request_date"::timestamp                      AS request_date,
--         data:"resolution_date"::timestamp                   AS resolution_date,
--         data:"resolutionstatus"::string                     AS resolution_status,
--         data:"MediaComplaintGenerationDate"::date           AS complaint_generation_date,
--         data:"source_file"::string                          AS source_file,
--         data:"ingestion_timestamp"::timestamp               AS ingestion_timestamp,
--         data:"load_timestamp"::timestamp                    AS load_timestamp
--     FROM {{ source('core_telecoms_raw', 'social_media_raw') }}
-- )

-- SELECT * FROM source

{{ config(materialized='table') }}

WITH source AS (
    SELECT
        data:"complaint_id"::string AS complaint_id,
        data:"customeR iD"::string AS customer_id,
        data:"agent ID"::number AS agent_id,
        data:"media_channel"::string AS media_channel,
        data:"resolutionstatus"::string AS resolution_status,

        -- SAFE TIMESTAMP CASTS
        data:"source_file"::string AS source_file,
        INITCAP(data:"COMPLAINT_catego ry"::string) AS complaint_category,
        TRY_TO_TIMESTAMP(NULLIF(data:"request_date"::string, ''))
            AS request_date,

        TRY_TO_TIMESTAMP(NULLIF(data:"resolution_date"::string, ''))
            AS resolution_date,
        TRY_TO_DATE(NULLIF(data:"MediaComplaintGenerationDate"::string, ''))
            AS complaint_generation_date,

        -- ingestion can be number or string
        TRY_TO_TIMESTAMP_NTZ(data:"ingestion_timestamp"::string)
            AS ingestion_timestamp,
        TRY_TO_TIMESTAMP_NTZ(NULLIF(data:"load_timestamp"::string, ''))
            AS load_timestamp
    FROM {{ source('core_telecoms_raw', 'social_media_raw') }}
)

SELECT * FROM source
