import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

from src.comparers.row_comparer import RowLevelComparer
from src.comparers.element_comparer import ElementwiseComparer

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("TestSparkComparers").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def spark_df_pair(spark_session):
    """Create two small Spark DataFrames for testing."""
    data1 = [
        Row(id=1, name="Alice", age=30),
        Row(id=2, name="Bob", age=25)
    ]
    data2 = [
        Row(id=2, name="Bob", age=26),
        Row(id=3, name="Charlie", age=35)
    ]
    df1 = spark_session.createDataFrame(data1)
    df2 = spark_session.createDataFrame(data2)
    return df1, df2

def test_row_level_comparer(spark_df_pair):
    df1, df2 = spark_df_pair
    comparer = RowLevelComparer()
    diff_df1, diff_df2 = comparer.compare(df1, df2)
    # df1 subtract df2 => should contain row (id=1)
    # df2 subtract df1 => should contain row (id=3)
    results1 = diff_df1.collect()
    results2 = diff_df2.collect()
    assert len(results1) == 1
    assert results1[0]["id"] == 1
    assert len(results2) == 1
    assert results2[0]["id"] == 3

def test_elementwise_comparer(spark_df_pair):
    df1, df2 = spark_df_pair
    comparer = ElementwiseComparer(key="id")
    joined_df = comparer.compare(df1, df2)
    results = joined_df.collect()
    # The only common key is id=2
    assert len(results) == 1
    row = results[0]
    # Expect name_comparison => "Match"
    assert row["name_comparison"] == "Match"
    # Expect age_comparison => "Mismatch" because age=25 vs age=26
    assert row["age_comparison"] == "Mismatch"