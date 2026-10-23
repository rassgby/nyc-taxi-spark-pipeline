# Projet : nyc-taxi-spark-pipeline

## Objectif
Pipeline big data PySpark sur les courses de taxis jaunes de New York (données NYC TLC, Parquet mensuels, ~35-40 M de lignes/an). Projet portfolio pour viser des postes data/IA qui codent. Le dépôt doit être propre et reproductible, destiné à être lu par des recruteurs.

## Contraintes
- Exécution cible : Google Colab (Spark en mode local, ~12 Go de RAM). Le code doit aussi tourner en local.
- Le README doit dire honnêtement que Spark tourne en mode local.
- Ne jamais committer les données (data/, *.parquet, *.csv sont dans .gitignore).
- Commits petits, messages clairs en français.
- Code en modules dans src/, notebooks dans notebooks/ pour l'exploration seulement.

## Architecture (medallion)
- Bronze : Parquet bruts téléchargés dans data/bronze/
- Silver : données nettoyées, enrichies (jointure zones en broadcast), Parquet partitionné par year/month dans data/silver/trips
- Gold : agrégats (par zone, heure, jour) pour l'analyse

## Déjà fait (sur un seul mois : janvier 2024)
- Setup PySpark, ingestion, exploration (distance <= 0, passagers, prix moyen par heure)
- Règles de qualité avec rapport de rejets (durée 1-180 min, distance 0-100, prix > 0, passagers 1-6, date dans le mois, vitesse < 80 mph)
- Colonnes dérivées : duration_min, pickup_hour, pickup_dow, pickup_date, speed_mph
- Jointure avec taxi_zone_lookup.csv (départ et arrivée) via broadcast
- Écriture silver partitionnée par year/month

## À faire
1. Transformer le code silver en modules réutilisables dans src/ (data.py, silver.py, quality.py) + tests pytest
2. Boucle sur 12 mois (2024) avec schéma explicite (les schémas varient selon les mois)
3. Couche gold + analyses Spark SQL (window functions : heures de pointe, zones rentables, pourboires)
4. MLlib : prédire la durée de course ou le pourboire (Pipeline, VectorAssembler, GBT/RandomForest, validation croisée)
5. Benchmark Spark vs DuckDB vs Polars vs pandas (temps et mémoire)
6. Dashboard Streamlit ou graphiques exportés
7. README final avec chiffres réels (volume, % de rejets par règle, temps de traitement, métriques du modèle, captures)

## Structure cible

```
├── README.md
├── CLAUDE.md
├── requirements.txt
├── .gitignore
├── notebooks/   # exploration (Colab)
├── src/         # data.py, quality.py, silver.py, gold.py, train.py
├── tests/
└── app/         # Streamlit
```

## Style de travail
- Explique brièvement ce que tu fais et pourquoi : je suis en apprentissage et je dois pouvoir réexpliquer chaque choix en entretien.
- Avant de modifier plusieurs fichiers, résume le plan.
- Lance les tests avant de proposer un commit.
