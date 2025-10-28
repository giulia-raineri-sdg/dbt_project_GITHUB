{{
    config(
        materialized='table',
        unique_key='PRODUCT_CD'
    )
}}

WITH t1_active AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['PRODUCT_CD']) }} AS PRODUCT_SK,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        0 AS is_deleted
    FROM {{ source('products_raw', 'correct_products_t1') }}
),

t0_deleted AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['t0.PRODUCT_CD']) }} AS PRODUCT_SK,
        t0.PRODUCT_CD,
        t0.MODEL_NAME,
        t0.BRAND,
        t0.CATEGORY,
        t0.LIST_PRICE,
        t0.COLOR,
        1 AS is_deleted
    FROM {{ source('products_raw', 'correct_products_t0') }} t0
    LEFT JOIN t1_active t1
        ON t0.PRODUCT_CD = t1.PRODUCT_CD
    WHERE t1.PRODUCT_CD IS NULL
)

SELECT * FROM t1_active
UNION ALL
SELECT * FROM t0_deleted