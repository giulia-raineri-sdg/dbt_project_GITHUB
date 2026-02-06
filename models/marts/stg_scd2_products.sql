{{
    config(
        materialized='table',
        unique_key='PRODUCT_CD'
    )
}}

{% set scd_change_columns = [
    'MODEL_NAME',
    'BRAND',
    'CATEGORY',
    'LIST_PRICE',
    'COLOR'
] %}

WITH t1_active AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['PRODUCT_CD']) }} AS PRODUCT_SK,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS dbt_valid_from,
        NULL::TIMESTAMP_NTZ AS dbt_valid_to
    FROM {{ source('products_raw', 'correct_products_t1') }}
),

source_t0 AS (
    SELECT
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        CAST(LIST_PRICE AS NUMERIC(10, 2)) AS LIST_PRICE,
        COLOR
    FROM {{ source('products_raw', 'correct_products_t0') }}
),

-- Identifica i prodotti in T0 che NON sono cambiati in T1
unchanged_products AS (
    SELECT
        t0.PRODUCT_CD
    FROM source_t0 t0
    INNER JOIN t1_active t1
        ON t0.PRODUCT_CD = t1.PRODUCT_CD
    WHERE NOT (
        {% for col in scd_change_columns %}
        t0.{{ col }} != t1.{{ col }}
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
    )
),

all_previous_records AS (
    {% if is_incremental() %}
    -- Esecuzione 2+: Leggi lo storico completo dalla tabella SCD2 ({{ this }})
    SELECT 
        PRODUCT_SK,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        is_current,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ this }}
    {% else %}
    -- Prima Esecuzione: Leggi T0 come stato iniziale attivo
    SELECT
        {{ dbt_utils.generate_surrogate_key(['PRODUCT_CD']) }} AS PRODUCT_SK,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        TRUE AS is_current,
        '1900-01-01 00:00:00.000'::TIMESTAMP_NTZ AS dbt_valid_from,
        NULL::TIMESTAMP_NTZ AS dbt_valid_to
    FROM source_t0
    {% endif %}
),

current_records_to_check AS (
    SELECT *
    FROM all_previous_records
    WHERE is_current = TRUE
),

expired_records AS (
    SELECT
        pcr.PRODUCT_SK,
        pcr.PRODUCT_CD,
        pcr.MODEL_NAME,
        pcr.BRAND,
        pcr.CATEGORY,
        pcr.LIST_PRICE,
        pcr.COLOR,
        FALSE AS is_current,
        pcr.dbt_valid_from,
        DATEADD(MILLISECOND, -1, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS dbt_valid_to 
    FROM current_records_to_check pcr
    LEFT JOIN t1_active t1
        ON pcr.PRODUCT_CD = t1.PRODUCT_CD
    WHERE 
        t1.PRODUCT_CD IS NULL 
        OR 
        (
            t1.PRODUCT_CD IS NOT NULL AND
            (
                {% for col in scd_change_columns %}
                pcr.{{ col }} != t1.{{ col }}
                {% if not loop.last %} OR {% endif %}
                {% endfor %}
            )
        )
),

unchanged_records AS (
    SELECT
        pcr.PRODUCT_SK,
        pcr.PRODUCT_CD,
        pcr.MODEL_NAME,
        pcr.BRAND,
        pcr.CATEGORY,
        pcr.LIST_PRICE,
        pcr.COLOR,
        TRUE AS is_current,
        pcr.dbt_valid_from,
        pcr.dbt_valid_to
    FROM current_records_to_check pcr
    LEFT JOIN t1_active t1
        ON pcr.PRODUCT_CD = t1.PRODUCT_CD
    WHERE
        t1.PRODUCT_CD IS NOT NULL
        AND NOT (
            {% for col in scd_change_columns %}
            pcr.{{ col }} != t1.{{ col }}
            {% if not loop.last %} OR {% endif %}
            {% endfor %}
        )
)

-- 1. Nuove versioni (Inclusi i nuovi inserimenti)
SELECT
    PRODUCT_SK,
    PRODUCT_CD,
    MODEL_NAME,
    BRAND,
    CATEGORY,
    LIST_PRICE,
    COLOR,
    is_current,
    dbt_valid_from,
    dbt_valid_to
FROM t1_active
WHERE PRODUCT_CD NOT IN (SELECT PRODUCT_CD FROM unchanged_products)

UNION ALL

-- 2. Record scaduti (modificati o cancellati)
SELECT
    PRODUCT_SK,
    PRODUCT_CD,
    MODEL_NAME,
    BRAND,
    CATEGORY,
    LIST_PRICE,
    COLOR,
    is_current,
    dbt_valid_from,
    dbt_valid_to
FROM expired_records

UNION ALL

-- 3. Record inalterati (mantengono la data di inizio T0)
SELECT
    PRODUCT_SK,
    PRODUCT_CD,
    MODEL_NAME,
    BRAND,
    CATEGORY,
    LIST_PRICE,
    COLOR,
    is_current,
    dbt_valid_from,
    dbt_valid_to
FROM unchanged_records

UNION ALL

-- 4. Storia precedente (già scaduta prima di questa run)
SELECT
    PRODUCT_SK,
    PRODUCT_CD,
    MODEL_NAME,
    BRAND,
    CATEGORY,
    LIST_PRICE,
    COLOR,
    is_current,
    dbt_valid_from,
    dbt_valid_to
FROM all_previous_records
WHERE is_current = FALSE