"""Couche silver : nettoyage, colonnes dérivées, enrichissement par les zones, écriture.

Ordre du pipeline (voir `build_silver`) :
    bronze -> colonnes dérivées -> règles de qualité -> filtre -> jointure zones -> écriture
Les colonnes dérivées viennent AVANT les règles car deux règles (durée, vitesse)
portent sur des colonnes calculées.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.quality import apply_rules, get_rules, keep_valid, rejection_report

PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Ajoute duration_min, pickup_hour, pickup_dow, pickup_date, speed_mph, year, month.

    - pickup_dow suit la convention Spark `dayofweek` : 1 = dimanche ... 7 = samedi.
    - speed_mph vaut NULL si la durée est <= 0 (évite une division par zéro ;
      ces lignes sont de toute façon rejetées par la règle de durée).
    - year / month servent uniquement au partitionnement de l'écriture.
    """
    duration_min = (F.unix_timestamp(DROPOFF) - F.unix_timestamp(PICKUP)) / 60.0
    return (
        df.withColumn("duration_min", duration_min)
        .withColumn("pickup_hour", F.hour(PICKUP))
        .withColumn("pickup_dow", F.dayofweek(PICKUP))
        .withColumn("pickup_date", F.to_date(PICKUP))
        .withColumn(
            "speed_mph",
            F.when(
                F.col("duration_min") > 0,
                F.col("trip_distance") / (F.col("duration_min") / 60.0),
            ),
        )
        .withColumn("year", F.year(PICKUP))
        .withColumn("month", F.month(PICKUP))
    )


def join_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Ajoute arrondissement et nom de zone au départ (pu_) et à l'arrivée (do_).

    `zones` est minuscule (~265 lignes) : on la diffuse (broadcast) à tous les
    exécuteurs, ce qui évite de mélanger (shuffle) les 40 M de courses.
    Jointure `left` : une course dont la zone est inconnue n'est pas perdue.
    """
    zones = F.broadcast(zones.select("LocationID", "Borough", "Zone"))
    pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )
    do = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone"),
    )
    return trips.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")


def write_silver(df: DataFrame, path: str) -> None:
    """Écrit en Parquet partitionné par year/month.

    Mode `dynamic` : on n'écrase que les partitions présentes dans `df`. Ainsi,
    retraiter un seul mois ne supprime pas les 11 autres.
    """
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("overwrite").partitionBy("year", "month").parquet(path)


def build_silver(
    spark: SparkSession,
    bronze_path: str,
    zones_path: str,
    silver_path: str,
    year: int,
    month: int,
) -> DataFrame:
    """Construit le silver d'un mois et renvoie le rapport de rejets."""
    trips = add_derived_columns(spark.read.parquet(bronze_path))
    rules = get_rules(year, month)

    report = rejection_report(trips, rules)
    clean = keep_valid(apply_rules(trips, rules), rules)

    zones = spark.read.option("header", True).csv(zones_path)
    zones = zones.withColumn("LocationID", F.col("LocationID").cast("int"))
    write_silver(join_zones(clean, zones), silver_path)
    return report
