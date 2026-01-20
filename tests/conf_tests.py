import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_fixture():
    # Setup a local Spark instance for testing
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Testing_Suite") \
        .getOrCreate()
    yield spark
    spark.stop()