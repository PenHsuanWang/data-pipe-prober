import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.data_reader import DataReader
import json  # needed for the json fixture below

@pytest.fixture
def sample_csv(tmp_path):
    """Create a small CSV file for testing local reads."""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("col1,col2\n1,2\n3,4\n")
    return str(csv_path)

@pytest.fixture
def sample_json(tmp_path):
    """Create a small JSON file for testing local reads."""
    json_path = tmp_path / "sample.json"
    json_content = [
        {"col1": 1, "col2": 2},
        {"col1": 3, "col2": 4}
    ]
    json_path.write_text(json.dumps(json_content))
    return str(json_path)

def test_data_reader_local_csv(sample_csv):
    """Test reading a local CSV with pandas."""
    reader = DataReader(
        spark=None,
        source_path=sample_csv,
        source_type="local",
        fmt="csv",
        header=True
    )
    df = reader.read()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["col1", "col2"]

def test_data_reader_local_json(sample_json):
    """Test reading a local JSON with pandas."""
    reader = DataReader(
        spark=None,
        source_path=sample_json,
        source_type="local",
        fmt="json",
        header=False
    )
    df = reader.read()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["col1", "col2"]

@patch("pyspark.sql.SparkSession")
def test_data_reader_spark_s3(mock_spark, sample_csv):
    """
    Test reading from S3 by mocking Spark. We won't actually call S3,
    but we confirm we use spark.read.format(...).load().
    """
    spark_session_mock = MagicMock()
    mock_spark.builder.appName.return_value.getOrCreate.return_value = spark_session_mock

    reader = DataReader(
        spark=spark_session_mock,
        source_path="s3://fake-bucket/fake-path",
        source_type="s3",
        fmt="csv",
        header=True,
        options={"someOption": "someValue"}
    )
    df_spark = reader.read()
    # Assert the initial call on format() is made with "csv"
    spark_session_mock.read.format.assert_called_with("csv")
    # Assert that option("header", True) is called on the object returned by format()
    spark_session_mock.read.format.return_value.option.assert_called_with("header", True)
    # Since our code chains the calls, .options() is called on the return value of .option()
    spark_session_mock.read.format.return_value.option.return_value.options.assert_called_with(someOption="someValue")
    # And the subsequent .load() call is on the return value of .options()
    spark_session_mock.read.format.return_value.option.return_value.options.return_value.load.assert_called_with("s3://fake-bucket/fake-path")
    # Note: df_spark is a mock, so we verify calls rather than data

@patch("pyspark.sql.SparkSession")
def test_data_reader_snowflake_dbtable(mock_spark):
    """
    Test reading from Snowflake using dbtable (no 'query' in options).
    """
    spark_session_mock = MagicMock()
    mock_spark.builder.appName.return_value.getOrCreate.return_value = spark_session_mock

    reader = DataReader(
        spark=spark_session_mock,
        source_path="MYSCHEMA.MYTABLE",
        source_type="snowflake",
        fmt="snowflake",
        header=False,
        options={
            "sfUser": "test_user",
            "sfPassword": "test_password",
            "sfURL": "test_account.snowflakecomputing.com"
        }
    )
    df_spark = reader.read()
    spark_session_mock.read.format.assert_called_with("snowflake")
    spark_session_mock.read.format.return_value.options.assert_called_with(
        sfUser="test_user",
        sfPassword="test_password",
        sfURL="test_account.snowflakecomputing.com"
    )
    # For the Snowflake dbtable case, .option("dbtable", ...) is chained on the return value of options()
    spark_session_mock.read.format.return_value.options.return_value.option.assert_called_with("dbtable", "MYSCHEMA.MYTABLE")
    spark_session_mock.read.format.return_value.options.return_value.option.return_value.load.assert_called_once()

@patch("pyspark.sql.SparkSession")
def test_data_reader_snowflake_query(mock_spark):
    """
    Test reading from Snowflake using a custom query.
    """
    spark_session_mock = MagicMock()
    mock_spark.builder.appName.return_value.getOrCreate.return_value = spark_session_mock

    reader = DataReader(
        spark=spark_session_mock,
        source_path="IGNORED_TABLE_NAME",
        source_type="snowflake",
        fmt="snowflake",
        header=False,
        options={
            "sfUser": "test_user",
            "sfPassword": "test_password",
            "sfURL": "test_account.snowflakecomputing.com",
            "query": "SELECT * FROM MYTABLE WHERE ID > 100"
        }
    )
    df_spark = reader.read()
    spark_session_mock.read.format.assert_called_with("snowflake")
    spark_session_mock.read.format.return_value.options.assert_called_with(
        sfUser="test_user",
        sfPassword="test_password",
        sfURL="test_account.snowflakecomputing.com",
        query="SELECT * FROM MYTABLE WHERE ID > 100"
    )
    # In the custom query case, no additional .option() should be called
    spark_session_mock.read.format.return_value.options.return_value.option.assert_not_called()
    spark_session_mock.read.format.return_value.options.return_value.load.assert_called_once()