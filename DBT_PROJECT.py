# Attiva environment virtuale
.\Scripts\Activate.ps1
cd dbt_project_github
# Esegui dbt
dbt run
dbt debug
dbt seed #caricare file csv creati in precedenza

# Esegui comandi Git
git add . # Aggiungi tutte le modifiche
git commit -m #"Aggiornamento dbt models e dati" # Crea un commit con un messaggio
git push # Invia le modifiche al repository remoto

# Creato il modello e la sorgente per atlete
dbt run --models dim_atlete --full-refresh
dbt run --models dim_atlete_scd3 --full-refresh

# Creato il modello e la sorgente per progetti SCD1
dbt run --models dim_progetti_scd1 --full-refresh
dbt run --models dim_progetti_scd1

# Creato il modello e la sorgente per progetti SCD3
dbt run --models dim_progetti_scd3 --full-refresh
dbt run --models dim_progetti_scd3