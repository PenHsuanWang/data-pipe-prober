from pyspark.sql import SparkSession

class DataReader:
    def __init__(self, spark: SparkSession, source_path: str, source_type: str = "local",
                 fmt: str = "csv", header: bool = False, options: dict = None):
        """
        :param spark: SparkSession instance.
        :param source_path: File path or table name.
        :param source_type: Type of source (local, s3, azure, snowflake, etc.).
        :param fmt: Data format (csv, json, etc.).
        :param header: Boolean flag indicating if the file has a header row.
        :param options: Additional options as a dictionary (e.g., credentials, endpoints).
        """
        self.spark = spark
        self.source_path = source_path
        self.source_type = source_type.lower()
        self.fmt = fmt
        self.header = header
        self.options = options or {}

    def read(self):
        if self.source_type in ["local", "file"]:
            # Reading local files
            return self.spark.read.format(self.fmt).option("header", self.header).load(self.source_path)
        elif self.source_type == "s3":
            # Reading from S3 (ensure AWS configurations are set up)
            return self.spark.read.format(self.fmt).option("header", self.header).options(**self.options).load(self.source_path)
        elif self.source_type in ["azure", "abfs", "wasb"]:
            # Reading from Azure Blob Storage (e.g., using abfss://)
            return self.spark.read.format(self.fmt).option("header", self.header).options(**self.options).load(self.source_path)
        elif self.source_type == "snowflake":
            # Reading from Snowflake using the Spark Snowflake connector
            return self.spark.read.format("snowflake").options(**self.options).option("dbtable", self.source_path).load()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")
