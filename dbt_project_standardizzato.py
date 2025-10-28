.\Scripts\Activate.ps1
# Create a new branch for the implementation of the standardized DBT project
git checkout -b feature/1-implementazione-es-standardizzato
git push -u origin feature/1-implementazione-es-standardizzato

git branch

# Commit changes to the standardized DBT project
git add .
git commit -m "Implementazione del progetto DBT standardizzato"
git push 

# Caricamento dei dati di esempio
dbt seed 


dbt run --select scd1_clients --full-refresh