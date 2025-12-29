{{ config(materialized='table') }}

-- GOLD FACT TABLE: Unified complaints + joins

WITH call_center AS (
    SELECT
        'call_center' AS channel,
        call_id AS complaint_id,
        customer_id,
        agent_id,
        complaint_category,
        call_start_time AS request_date,
        call_end_time AS resolution_date,
        resolution_status
    FROM {{ ref('call_center_logs') }}
),

social_media AS (
    SELECT
        'social_media' AS channel,
        complaint_id,
        customer_id,
        agent_id,
        complaint_category,
        request_date AS request_date,
        resolution_date AS resolution_date,
        resolution_status
    FROM {{ ref('social_media') }}
),

website AS (
    SELECT
        'website' AS channel,
        request_id AS complaint_id,
        customer_id,
        agent_id,
        complaint_category,
        request_date AS request_date,
        resolution_date AS resolution_date,
        resolution_status
    FROM {{ ref('website_complaint_forms') }}
),

-- Union everything
unioned AS (
    SELECT * FROM call_center
    UNION ALL
    SELECT * FROM social_media
    UNION ALL
    SELECT * FROM website
),

-- Join with dimension tables
joined AS (
    SELECT
        u.*,
        c.customer_name,
        c.email AS customer_email,
        c.gender AS customer_gender,
        c.address AS customer_address,
        c.signup_date,
        c.date_of_birth,
        c.customer_segment,          -- new segment field
        a.name AS agent_name,
        a.experience AS agent_experience,
        a.state AS agent_state,
        -- Derived metrics
        coalesce (resolution_date IS NOT NULL, FALSE)
            AS resolved_flag,
        datediff('hour', request_date, resolution_date) AS resolution_hours,
        date_trunc('day', request_date) AS request_day
    FROM unioned AS u
    LEFT JOIN {{ ref('dim_customers') }} AS c
        ON u.customer_id = c.customer_id
    LEFT JOIN {{ ref('agents') }} AS a
        ON u.agent_id = a.id
)

SELECT * FROM joined
