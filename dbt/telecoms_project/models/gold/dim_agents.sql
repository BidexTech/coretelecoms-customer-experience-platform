{{ config(materialized='table') }}

SELECT
    id AS agent_id,
    experience,
    state,
    INITCAP(name) AS agent_name
FROM {{ ref('agents') }}
