{{
    config(
        materialized='incremental',
        unique_key='customer_cd',
        on_schema_change='fail'
    )
}}

with all_sources as (

    -- Unione T0
    select
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        cast(coalesce(is_deleted, 0) as integer) as is_deleted_flag,
        't0' as data_tag
    from {{ source('clients_raw', 'correct_clients_t0') }}

    union all

    -- Unione T1
    select
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        cast(coalesce(is_deleted, 0) as integer) as is_deleted_flag,
        't1' as data_tag
    from {{ source('clients_raw', 'correct_clients_t1') }}

),

latest_records as (

    select
        *,
        -- Trova il record con la data di aggiornamento più recente (rank 1)
        row_number() over (
            partition by customer_cd
            order by last_update desc, data_tag desc 
        ) as rn
    from all_sources
    
    {% if is_incremental() %}
      where last_update >= (select max(last_update) from {{ this }})
    {% endif %}

)

select
    -- Chiave surrogata
    {{ dbt_utils.generate_surrogate_key(['customer_cd']) }} as client_surrogate_key,
    
    customer_cd,
    name,
    email,
    city,
    member_since,
    last_update,
    is_deleted_flag

from latest_records
where rn = 1
