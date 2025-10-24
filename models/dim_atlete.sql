{{
  config(
    materialized='incremental',
    unique_key=['nome', 'cognome'],
    incremental_strategy='merge'
  )
}}

-- Logica:
-- SE è la prima esecuzione (non incrementale), carica t0 (lo stato iniziale).
-- ALTRIMENTI (è un'esecuzione incrementale), carica t1 (gli aggiornamenti).

SELECT
    nome,
    cognome,
    ruolo,
    squadra,
    CURRENT_TIMESTAMP() AS data_caricamento_record

FROM
    {% if is_incremental() %}
    -- Esecuzione 2 (Simulazione t1): Carica gli aggiornamenti dal set di dati più recente
    {{ source('atlete_source', 'atlete_t1') }}
    
    {% else %}
    -- Esecuzione 1 (Simulazione t0): Carica lo stato iniziale
    {{ source('atlete_source', 'atlete_t0') }}
    
    {% endif %}