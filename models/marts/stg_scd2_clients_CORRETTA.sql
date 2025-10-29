{{ 
    config(
        materialized = 'incremental',
        unique_key = 'customer_sk', 
        incremental_strategy = 'merge'
    ) 
}}

with 

source_data as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['customer_cd','last_update']) }} as customer_sk
    from 
    {% if is_incremental() %}
        {{ source('clients_raw', 'correct_clients_t1') }}
    {% else %}
        {{ source('clients_raw', 'correct_clients_t0') }}
    {% endif %}
),

{% if is_incremental() %}

changed_records_source as (
    select
        s.*
    from source_data s
    left join {{ this }} t
        on s.customer_sk = t.customer_sk
    where t.customer_sk is null
),

current_records_to_close as (
    select
        t.*,
        s.last_update as new_last_update 
    from {{ this }} t
    join changed_records_source s
        on t.customer_cd = s.customer_cd
    where t.is_current = true
),

rows_to_update as (
    select
        customer_sk,
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        is_deleted,
        dbt_updated_at,
        valid_from,
        new_last_update as valid_to,
        false as is_current
    from current_records_to_close
),

{% endif %}

rows_to_insert as (
    select
        customer_sk,
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        is_deleted,
        current_timestamp() as dbt_updated_at,
        last_update as valid_from,
        cast('2999-12-31' as date) as valid_to,
        
        true as is_current
    from 
    {% if is_incremental() %}
        changed_records_source
    {% else %}
        source_data
    {% endif %}
),

-- Uniamo le righe da inserire e quelle da aggiornare
unioned_data as (
    select * from rows_to_insert
    
    {% if is_incremental() %}
    union all
    select * from rows_to_update
    {% endif %}
),


-- CTE PER AVERE ORDINE NEI CAMPI
final_reordered as (
    select
        customer_sk,
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        is_deleted,
        dbt_updated_at,
        valid_from,
        valid_to,
        is_current

    from unioned_data
)

select * from final_reordered