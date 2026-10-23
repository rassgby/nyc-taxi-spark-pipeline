"""Règles de qualité des courses de taxis et rapport de rejets.

Principe : chaque règle est une expression Spark booléenne (True = ligne valide).
Les règles sont stockées dans un dictionnaire nom -> Column, ce qui permet de
les appliquer toutes en un seul passage et de compter les rejets par règle
sans écrire de code répétitif.

Les règles de durée et de vitesse supposent que `add_derived_columns`
(src/silver.py) a déjà créé `duration_min` et `speed_mph`.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PICKUP = "tpep_pickup_datetime"


def _ok(condition: Column) -> Column:
    """Une condition NULL (donnée manquante) est traitée comme un échec.

    En Spark, `NULL < 80` vaut NULL, et `filter(NULL)` retire la ligne : on
    rend ce comportement explicite pour que le rapport compte bien ces lignes.
    """
    return F.coalesce(condition, F.lit(False))


def get_rules(year: int, month: int) -> dict[str, Column]:
    """Règles de qualité pour un mois donné (le mois attendu sert à la règle de date)."""
    return {
        "duree_1_180_min": _ok(F.col("duration_min").between(1, 180)),
        "distance_0_100": _ok((F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100)),
        "prix_positif": _ok(F.col("total_amount") > 0),
        "passagers_1_6": _ok(F.col("passenger_count").between(1, 6)),
        "date_dans_le_mois": _ok(
            (F.year(PICKUP) == year) & (F.month(PICKUP) == month)
        ),
        "vitesse_sous_80_mph": _ok(F.col("speed_mph") < 80),
    }


def ok_col(rule_name: str) -> str:
    """Nom de la colonne booléenne associée à une règle."""
    return f"ok_{rule_name}"


def apply_rules(df: DataFrame, rules: dict[str, Column]) -> DataFrame:
    """Ajoute une colonne `ok_<règle>` par règle, et `is_valid` (toutes les règles)."""
    for name, condition in rules.items():
        df = df.withColumn(ok_col(name), condition)
    all_ok = F.lit(True)
    for name in rules:
        all_ok = all_ok & F.col(ok_col(name))
    return df.withColumn("is_valid", all_ok)


def keep_valid(df: DataFrame, rules: dict[str, Column]) -> DataFrame:
    """Garde les lignes valides et retire les colonnes techniques `ok_*` / `is_valid`."""
    to_drop = [ok_col(n) for n in rules] + ["is_valid"]
    return df.filter(F.col("is_valid")).drop(*to_drop)


def rejection_report(df: DataFrame, rules: dict[str, Column]) -> DataFrame:
    """Rapport de rejets : une ligne par règle + une ligne « toutes règles ».

    Un seul `agg` => un seul passage sur les données (important sur 40 M de lignes).
    Une ligne peut violer plusieurs règles : les % par règle ne s'additionnent donc
    pas, d'où la ligne `TOTAL_REJETEES` (lignes qui violent au moins une règle).
    """
    checked = apply_rules(df, rules)
    aggs = [F.count(F.lit(1)).alias("total")]
    aggs += [
        F.sum(F.when(~F.col(ok_col(n)), 1).otherwise(0)).alias(n) for n in rules
    ]
    aggs.append(F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("TOTAL_REJETEES"))
    row = checked.agg(*aggs).first()

    total = row["total"]
    names = list(rules) + ["TOTAL_REJETEES"]
    data = [
        (n, int(row[n] or 0), round(100.0 * (row[n] or 0) / total, 3) if total else 0.0)
        for n in names
    ]
    return df.sparkSession.createDataFrame(data, ["regle", "lignes_rejetees", "pct_du_total"])
