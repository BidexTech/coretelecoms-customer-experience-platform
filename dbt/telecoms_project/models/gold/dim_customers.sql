{{ config(materialized='table') }}

WITH base AS (
    SELECT
        customer_id,
        gender,
        address,
        date_of_birth,
        signup_date,
        INITCAP(name) AS customer_name,
        LOWER(email) AS email,
        -- Calculate derived fields
        DATE_PART('year', CURRENT_DATE)
        - DATE_PART('year', date_of_birth) AS age,
        DATEDIFF('day', signup_date, CURRENT_DATE) AS days_since_signup
    FROM {{ ref('customers') }}
),

segmented AS (
    SELECT
        customer_id,
        customer_name,
        email,
        gender,
        address,
        date_of_birth,
        signup_date,
        age,
        days_since_signup,
        -- Simple segmentation logic
        CASE
            WHEN age < 25 THEN 'Youth'
            WHEN age BETWEEN 25 AND 44 THEN 'Adult'
            WHEN age BETWEEN 45 AND 64 THEN 'Middle-Aged'
            ELSE 'Senior'
        END AS age_segment,
        CASE
            WHEN days_since_signup < 180 THEN 'New'
            WHEN days_since_signup BETWEEN 180 AND 1095 THEN 'Regular'
            ELSE 'Loyal'
        END AS tenure_segment,
        CONCAT(
            CASE
                WHEN age < 25 THEN 'Youth'
                WHEN age BETWEEN 25 AND 44 THEN 'Adult'
                WHEN age BETWEEN 45 AND 64 THEN 'Middle-Aged'
                ELSE 'Senior'
            END,
            '_',
            CASE
                WHEN days_since_signup < 180 THEN 'New'
                WHEN days_since_signup BETWEEN 180 AND 1095 THEN 'Regular'
                ELSE 'Loyal'
            END
        ) AS customer_segment
    FROM base
)

SELECT * FROM segmented
