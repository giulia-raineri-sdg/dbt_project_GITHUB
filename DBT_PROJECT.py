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
