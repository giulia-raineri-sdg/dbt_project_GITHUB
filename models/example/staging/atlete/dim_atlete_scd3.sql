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
        ruolo,
        squadra AS nuova_squadra_t1,
        CURRENT_TIMESTAMP() AS data_caricamento_record
    FROM
        {% if is_incremental() %}
        -- Esecuzione t1: Dati più recenti
        {{ source('atlete_source', 'atlete_t1') }}
        {% else %}
        -- Esecuzione t0: Dati iniziali (full refresh)
        {{ source('atlete_source', 'atlete_t0') }}
        {% endif %}
)

-- 2. TARGET CTE (CONDITIONAL): Ottiene i dati esistenti in DIM_ATLETE_SCD3
{% if is_incremental() %}
, 
target_data AS (
    SELECT
        nome,
        cognome,
        squadra_attuale AS vecchia_squadra_t0,
        squadra_precedente
    FROM {{ this }}
)
{% endif %}

-- 3. FINAL SELECT: Applica la logica SCD 3
SELECT
    sd.nome,
    sd.cognome,
    sd.ruolo,
    sd.data_caricamento_record,

    -- [squadra_attuale] È sempre il valore del set di dati più recente (t1)
    sd.nuova_squadra_t1 AS squadra_attuale,

    -- [squadra_precedente] Logica SCD Tipo 3
    CASE
        -- CASO A: Full Refresh (t0) o Record Nuovi (INSERT) -> squadra_precedente = squadra_attuale
        WHEN NOT {{ is_incremental() }} THEN sd.nuova_squadra_t1
        WHEN {% if is_incremental() %} td.vecchia_squadra_t0 IS NULL THEN sd.nuova_squadra_t1
        {% else %} 1=2 THEN sd.nuova_squadra_t1 -- Clausola per full-refresh, mai eseguita
        {% endif %}

        -- CASO B: La squadra è cambiata (UPDATE) -> La vecchia 'squadra_attuale' diventa la 'squadra_precedente'
        {% if is_incremental() %}
        WHEN sd.nuova_squadra_t1 <> td.vecchia_squadra_t0 THEN td.vecchia_squadra_t0

        -- CASO C: Nessun cambiamento -> Manteniamo il vecchio valore di 'squadra_precedente' dal target
        ELSE td.squadra_precedente
        {% else %}
        -- Clausola per full-refresh, mai eseguita
        ELSE sd.nuova_squadra_t1
        {% endif %}
    END AS squadra_precedente

FROM source_data sd

{% if is_incremental() %}
-- LEFT JOIN ESEGUITA SOLO IN MODALITÀ INCREMENTALE
LEFT JOIN target_data td
    ON sd.nome = td.nome AND sd.cognome = td.cognome
{% endif %}