{{ config(materialized='table') }}

SELECT
    agent_id,
    agent_name,
    agent_experience,
    agent_state,
    COUNT(*) AS total_complaints,
    COUNT_IF(resolved_flag) AS total_resolved,
    AVG(resolution_hours) AS avg_resolution_hours,
    COUNT_IF(resolution_hours <= 24) AS cases_resolved_within_24h
FROM {{ ref('fct_all_complaints') }}
GROUP BY
    agent_id,
    agent_name,
    agent_experience,
    agent_state
