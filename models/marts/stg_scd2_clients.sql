{{
    config(
        materialized='table',
        sort=['customer_cd', 'valid_from_date']
    )
}}

with all_sources as (
    SELECT
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        cast(coalesce(is_deleted, 0) as integer) as is_deleted_flag
    FROM {{ source('clients_raw', 'correct_clients_t0') }}

    UNION ALL

    SELECT
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        cast(coalesce(is_deleted, 0) as integer) as is_deleted_flag
    FROM {{ source('clients_raw', 'correct_clients_t1') }}
),

change_detection AS (
    SELECT
        *,
        
        {{ dbt_utils.generate_surrogate_key(['name', 'email', 'city', 'is_deleted_flag']) }} AS scd_hash
        
    FROM all_sources
),

distinct_changes AS (
    SELECT
        *,

        LAG(scd_hash, 1) OVER (
            PARTITION BY customer_cd
            ORDER BY last_update ASC
        ) AS previous_scd_hash

    FROM change_detection
),

ranked_changes AS (
    SELECT
        *,
        
        LEAD(last_update, 1) OVER (
            PARTITION BY customer_cd
            ORDER BY last_update ASC
        ) AS valid_to_next_record
        
    FROM distinct_changes
    
    WHERE previous_scd_hash IS NULL OR scd_hash != previous_scd_hash
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_cd', 'last_update', 'scd_hash']) }} AS client_history_surrogate_key,
        
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update AS valid_from_date,
        
        COALESCE(valid_to_next_record, CAST('9999-12-31' AS DATE)) AS valid_to_date,
        
        CASE 
            WHEN valid_to_next_record IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current_flag,

        is_deleted_flag
        
    FROM ranked_changes
)

SELECT
    *
<<<<<<< HEAD
FROM final
=======
FROM final
>>>>>>> 87149f039f227de008431110a0deea99342af2d3
