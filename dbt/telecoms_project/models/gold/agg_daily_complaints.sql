{{ config(materialized='table') }}

SELECT
    request_day,
    channel,
    COUNT(*) AS total_complaints,
    COUNT_IF(resolved_flag) AS total_resolved,
    AVG(resolution_hours) AS avg_resolution_hours,
    COUNT_IF(NOT resolved_flag) AS total_unresolved
FROM {{ ref('fct_all_complaints') }}
GROUP BY
    request_day,
    channel
