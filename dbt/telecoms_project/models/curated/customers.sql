{{ config(materialized='table') }}

WITH source AS (
    SELECT
        data:"customer_id"::string                      AS customer_id,
        data:"name"::string                             AS name,
        data:"email"::string                            AS email,
        data:"Gender"::string                           AS gender,
        data:"DATE of biRTH"::date                      AS date_of_birth,
        data:"signup_date"::date                        AS signup_date,
        data:"address"::string                          AS address,
        data:"source_file"::string                      AS source_file,
        data:"ingestion_timestamp"::number              AS ingestion_timestamp,
        data:"load_timestamp"::timestamp                AS load_timestamp
    FROM {{ source('core_telecoms_raw', 'customers_raw') }}
)

SELECT * FROM source
