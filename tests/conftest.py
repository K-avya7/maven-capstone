import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Initializes a local PySpark session for transformation testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("maven-market-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )