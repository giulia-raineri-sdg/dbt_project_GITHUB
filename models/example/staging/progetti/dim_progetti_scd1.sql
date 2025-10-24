{{
  config(
    materialized='incremental',
    unique_key=['nome', 'cognome'],
    incremental_strategy='merge'
  )
}}

SELECT
    nome,
    cognome,
    dipartimento,
    nome_progetto,
    CURRENT_TIMESTAMP() AS data_ultima_modifica

FROM
    {% if is_incremental() %}
    -- Esecuzione 2 (t1): La sorgente è il file aggiornato
    {{ source('progetti_source', 'progetti_t1') }}
    {% else %}
    -- Esecuzione 1 (t0): La sorgente è il file iniziale
    {{ source('progetti_source', 'progetti_t0') }}
    {% endif %}