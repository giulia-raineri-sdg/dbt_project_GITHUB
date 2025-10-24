{{
  config(
    materialized='incremental',
    unique_key=['nome', 'cognome'],
    incremental_strategy='merge'
  )
}}

-- 1. SOURCE CTE: Ottiene i dati in ingresso (t0 o t1)
WITH source_data AS (
    SELECT
        nome,
        cognome,
        dipartimento,
        nome_progetto AS nuovo_progetto_t1,
        CURRENT_TIMESTAMP() AS data_caricamento_record
    FROM
        {% if is_incremental() %}
        -- Esecuzione t1: Dati più recenti
        {{ source('progetti_source', 'progetti_t1') }}
        {% else %}
        -- Esecuzione t0: Dati iniziali (full refresh)
        {{ source('progetti_source', 'progetti_t0') }}
        {% endif %}
)

-- 2. TARGET CTE (CONDITIONAL): Ottiene i dati esistenti in DIM_PROGETTI_SCD3
{% if is_incremental() %}
, 
target_data AS (
    SELECT
        nome,
        cognome,
        progetto_attuale AS vecchio_progetto_t0,
        progetto_precedente
    FROM {{ this }}
)
{% endif %}

-- 3. FINAL SELECT: Applica la logica SCD 3
SELECT
    sd.nome,
    sd.cognome,
    sd.dipartimento,
    sd.data_caricamento_record,

    sd.nuovo_progetto_t1 AS progetto_attuale,

    CASE
        -- CASO A: Full Refresh (t0) o Nuovo record (INSERT) -> progetto_precedente = progetto_attuale
        WHEN NOT {{ is_incremental() }} THEN sd.nuovo_progetto_t1
        WHEN {% if is_incremental() %} td.vecchio_progetto_t0 IS NULL THEN sd.nuovo_progetto_t1
        {% else %} 1=2 THEN sd.nuovo_progetto_t1 
        {% endif %}

        -- CASO B: Il progetto è cambiato (UPDATE) -> Il vecchio 'progetto_attuale' (t0) diventa il 'progetto_precedente'
        {% if is_incremental() %}
        WHEN sd.nuovo_progetto_t1 <> td.vecchio_progetto_t0 THEN td.vecchio_progetto_t0

        -- CASO C: Nessun cambiamento -> Manteniamo il vecchio valore di 'progetto_precedente' dal target
        ELSE td.progetto_precedente
        {% else %}
        ELSE sd.nuovo_progetto_t1
        {% endif %}
    END AS progetto_precedente

FROM source_data sd

{% if is_incremental() %}
-- LEFT JOIN ESEGUITA SOLO IN MODALITÀ INCREMENTALE
LEFT JOIN target_data td
    ON sd.nome = td.nome AND sd.cognome = td.cognome
{% endif %}