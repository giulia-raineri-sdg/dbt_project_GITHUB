{{ 
    config(
        materialized = 'incremental',
        unique_key = 'product_sk',
        incremental_strategy = 'merge'
    ) 
}}

with 

source_data as (
    select
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        coalesce(trim(IS_DELETED), '0') as IS_DELETED,
        
        current_timestamp() as dbt_load_ts, 
        
        {{ dbt_utils.generate_surrogate_key([
            'PRODUCT_CD', 
            'MODEL_NAME', 
            'BRAND', 
            'CATEGORY', 
            'LIST_PRICE', 
            'COLOR', 
            'IS_DELETED'
        ]) }} as product_sk
        
    from 
    {% if is_incremental() %}
        {{ source('products_raw', 'correct_products_t1') }} 
    {% else %}
        {{ source('products_raw', 'correct_products_t0') }} 
    {% endif %}
),

{% if is_incremental() %}

changed_records_source as (
    select
        s.*
    from source_data s
    left join {{ this }} t
        on s.product_sk = t.product_sk
    where t.product_sk is null
),

current_records_to_close as (
    select
        t.*,
        s.dbt_load_ts as new_valid_from 
    from {{ this }} t
    join changed_records_source s
        on t.PRODUCT_CD = s.PRODUCT_CD 
    where t.is_current = true
),

rows_to_update as (
    select
        product_sk,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        IS_DELETED,
        dbt_updated_at,
        valid_from,
        new_valid_from as valid_to,
        false as is_current
    from current_records_to_close
),

{% endif %}

rows_to_insert as (
    select
        product_sk,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        IS_DELETED,
        dbt_load_ts as dbt_updated_at,
        dbt_load_ts as valid_from,
        cast('2999-12-31' as date) as valid_to,
        true as is_current
    from 
    {% if is_incremental() %}
        changed_records_source
    {% else %}
        source_data
    {% endif %}
),

unioned_data as (
    select * from rows_to_insert
    
    {% if is_incremental() %}
    union all
    select * from rows_to_update
    {% endif %}
),


-- CTE finale per ordinare i campi
final_reordered as (
    select
        product_sk,
        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        LIST_PRICE,
        COLOR,
        IS_DELETED,
        dbt_updated_at,
        valid_from,
        valid_to,
        is_current

    from unioned_data
)

select * from final_reordered