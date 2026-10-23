from datetime import datetime, timedelta

import pytest
from pyspark.sql.types import DoubleType, LongType, StructField, StructType, TimestampType

from src.quality import apply_rules, get_rules, keep_valid, ok_col, rejection_report
from src.silver import add_derived_columns

YEAR, MONTH = 2024, 1
COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "total_amount",
    "passenger_count",
]


def make_trip(**overrides):
    """Une course valide (15 min, 3 miles, 12 mph) ; `overrides` casse un champ."""
    pickup = overrides.pop("pickup", datetime(2024, 1, 15, 10, 0))
    duration = overrides.pop("duration", timedelta(minutes=15))
    trip = {
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + duration,
        "trip_distance": 3.0,
        "total_amount": 18.5,
        "passenger_count": 1,
    }
    trip.update(overrides)
    return tuple(trip[c] for c in COLUMNS)


SCHEMA = StructType(
    [
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("trip_distance", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("passenger_count", LongType()),
    ]
)


def checked(spark, trips):
    # Schéma explicite : Spark ne peut pas deviner le type d'une colonne ne contenant que None
    df = add_derived_columns(spark.createDataFrame(trips, SCHEMA))
    return df, apply_rules(df, get_rules(YEAR, MONTH))


def failed_rules(spark, trip):
    """Noms des règles violées par une seule course."""
    _, out = checked(spark, [trip])
    row = out.first()
    return {n for n in get_rules(YEAR, MONTH) if not row[ok_col(n)]}


def test_course_valide_ne_viole_aucune_regle(spark):
    assert failed_rules(spark, make_trip()) == set()


@pytest.mark.parametrize(
    "overrides, regle",
    [
        # 30 s à 0.1 mile => 12 mph : seule la durée est en cause
        ({"duration": timedelta(seconds=30), "trip_distance": 0.1}, "duree_1_180_min"),
        ({"duration": timedelta(minutes=200)}, "duree_1_180_min"),
        ({"trip_distance": 0.0}, "distance_0_100"),
        ({"trip_distance": 150.0}, "distance_0_100"),
        ({"total_amount": 0.0}, "prix_positif"),
        ({"total_amount": -5.0}, "prix_positif"),
        ({"passenger_count": 0}, "passagers_1_6"),
        ({"passenger_count": 7}, "passagers_1_6"),
        ({"passenger_count": None}, "passagers_1_6"),
        # Pickup en décembre 2023 alors qu'on traite janvier 2024
        ({"pickup": datetime(2023, 12, 31, 23, 50)}, "date_dans_le_mois"),
        # 90 miles en 60 min = 90 mph : distance OK (<= 100) mais vitesse trop haute
        (
            {"trip_distance": 90.0, "duration": timedelta(minutes=60)},
            "vitesse_sous_80_mph",
        ),
    ],
)
def test_course_invalide_viole_la_bonne_regle(spark, overrides, regle):
    assert regle in failed_rules(spark, make_trip(**overrides))


def test_keep_valid_retire_les_rejets_et_les_colonnes_techniques(spark):
    trips = [make_trip(), make_trip(trip_distance=0.0), make_trip(passenger_count=9)]
    df, out = checked(spark, trips)
    clean = keep_valid(out, get_rules(YEAR, MONTH))

    assert clean.count() == 1
    assert "is_valid" not in clean.columns
    assert not any(c.startswith("ok_") for c in clean.columns)


def test_rapport_de_rejets_compte_par_regle(spark):
    trips = [
        make_trip(),
        make_trip(),
        make_trip(trip_distance=0.0),
        make_trip(total_amount=0.0),
        make_trip(total_amount=-1.0),
    ]
    df, _ = checked(spark, trips)
    report = {
        r["regle"]: r["lignes_rejetees"]
        for r in rejection_report(df, get_rules(YEAR, MONTH)).collect()
    }

    assert report["distance_0_100"] == 1
    assert report["prix_positif"] == 2
    assert report["TOTAL_REJETEES"] == 3
