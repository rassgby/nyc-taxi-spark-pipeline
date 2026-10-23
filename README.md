# nyc-taxi-spark-pipeline

Pipeline big data PySpark sur les courses de taxis jaunes de New York
([NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), fichiers Parquet mensuels,
~35-40 millions de courses par an).

## Objectif

Construire un pipeline de données reproductible, de l'ingestion brute jusqu'à l'analyse et à un
modèle de machine learning, en appliquant des règles de qualité mesurables (taux de rejet par règle).

## Architecture (medallion)

```
data/bronze/  Parquet bruts téléchargés depuis la TLC
      │
      ▼   colonnes dérivées + règles de qualité + jointure zones (broadcast)
data/silver/trips/  Parquet nettoyé, partitionné par year/month
      │
      ▼   agrégats (par zone, heure, jour)
data/gold/  tables d'analyse
```

| Couche | Contenu | Module |
|---|---|---|
| Bronze | Données brutes, non modifiées | `src/data.py` *(à venir)* |
| Silver | Nettoyé, enrichi, partitionné | `src/silver.py`, `src/quality.py` |
| Gold | Agrégats pour l'analyse | `src/gold.py` *(à venir)* |

### Règles de qualité

| Règle | Condition |
|---|---|
| `duree_1_180_min` | durée entre 1 et 180 minutes |
| `distance_0_100` | distance > 0 et <= 100 miles |
| `prix_positif` | `total_amount` > 0 |
| `passagers_1_6` | 1 à 6 passagers |
| `date_dans_le_mois` | date de prise en charge dans le mois traité |
| `vitesse_sous_80_mph` | vitesse moyenne < 80 mph |

Le rapport de rejets (`rejection_report`) donne le nombre et le % de lignes rejetées par règle.

## Note honnête sur Spark

Spark tourne ici en **mode local** (une seule machine, `master=local[*]`), pas sur un cluster.
Le projet démontre l'API et les bonnes pratiques Spark (schémas explicites, broadcast join,
partitionnement, Spark SQL, MLlib), mais **pas** une exécution distribuée sur plusieurs nœuds.
Les temps mesurés dépendent de la machine (Colab, ~12 Go de RAM).

## Lancer le projet

### Sur Google Colab (cible principale)

Java est déjà installé sur Colab.

```python
!git clone https://github.com/TON_PSEUDO/nyc-taxi-spark-pipeline.git
%cd nyc-taxi-spark-pipeline
!pip install -q -r requirements.txt
!python -m pytest -q
```

Mettre à jour le code après un nouveau commit : `!git pull`.

### En local

Prérequis : Python 3.10+ et un JDK **17 ou 21** (les versions plus récentes ne sont pas garanties
avec PySpark).

```bash
git clone https://github.com/TON_PSEUDO/nyc-taxi-spark-pipeline.git
cd nyc-taxi-spark-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
```

## Structure

```
├── README.md
├── CLAUDE.md
├── requirements.txt
├── notebooks/   # exploration (Colab)
├── src/         # quality.py, silver.py (data.py, gold.py, train.py à venir)
└── tests/
```

## Résultats

*À remplir : volume traité, % de rejets par règle, temps de traitement, métriques du modèle,
benchmark Spark / DuckDB / Polars / pandas, captures.*
