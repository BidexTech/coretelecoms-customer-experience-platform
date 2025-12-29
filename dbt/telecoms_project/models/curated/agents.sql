{{ config(materialized='table') }}

WITH source AS (
    SELECT
        data:"iD"::number AS id,
        data:"experience"::string AS experience,
        data:"state"::string AS state,
        data:"ingestion_timestamp"::timestamp AS ingestion_timestamp,
        data:"load_timestamp"::timestamp AS load_timestamp,
        INITCAP(data:"NamE"::string) AS name
    FROM {{ source('core_telecoms_raw', 'agents_raw') }}
)

SELECT * FROM source
