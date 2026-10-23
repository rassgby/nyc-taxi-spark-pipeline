import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Session Spark locale partagée par tous les tests (démarrer Spark coûte cher)."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")  # petits DataFrames : 1 partition suffit
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
