import pandas as pd
from pyspark.sql import SparkSession


class DataReader:
    """
    Reads data from a specified source.

    Uses pandas for local files and Spark for non-local sources.

    :param spark: SparkSession instance or None for local files.
    :param source_path: File path or table name.
    :param source_type: Type of source (e.g., "local", "s3", "azure", "snowflake").
    :param fmt: Data format (e.g., "csv", "json", "snowflake").
    :param header: Flag indicating if the file has a header row.
    :param options: Additional options as a dictionary (for Spark or Snowflake credentials, etc.).
    """

    def __init__(self, spark, source_path: str, source_type: str = "local",
                 fmt: str = "csv", header: bool = False, options: dict = None):
        self.spark = spark
        self.source_path = source_path
        self.source_type = source_type.lower()
        self.fmt = fmt.lower()
        self.header = header
        self.options = options or {}

    def read(self):
        """
        Read data from the specified source.

        - For local sources, returns a pandas DataFrame.
        - For non-local sources, returns a Spark DataFrame.

        :returns: A pandas DataFrame or Spark DataFrame.
        :raises ValueError: If the format or source type is unsupported.
        """
        # 1. Local file read via pandas
        if self.source_type in ["local", "file"]:
            if self.fmt == "csv":
                return pd.read_csv(self.source_path, header=0 if self.header else None)
            elif self.fmt == "json":
                return pd.read_json(self.source_path)
            else:
                raise ValueError(f"Unsupported format for local files: {self.fmt}")

        # 2. Spark-based reads for external/cloud sources
        else:
            if self.source_type in ["s3", "azure", "abfs", "wasb"]:
                return (self.spark.read.format(self.fmt)
                        .option("header", self.header)
                        .options(**self.options)
                        .load(self.source_path))

            elif self.source_type == "snowflake":
                # Snowflake specifics:
                # The user can pass "query" OR "dbtable" in self.options.
                reader = self.spark.read.format("snowflake").options(**self.options)
                if "query" in self.options:
                    # Use a custom SQL query if provided:
                    reader = reader.option("query", self.options["query"])
                else:
                    # Otherwise assume we want to read from a table:
                    # self.source_path might be a table name or fully-qualified name
                    reader = reader.option("dbtable", self.source_path)
                return reader.load()

            else:
                raise ValueError(f"Unsupported source type: {self.source_type}")