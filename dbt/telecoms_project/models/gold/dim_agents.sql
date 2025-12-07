{{ config(materialized='table') }}

SELECT
    id AS agent_id,
    INITCAP(name) AS agent_name,
    experience,
    state
FROM {{ ref('agents') }}
