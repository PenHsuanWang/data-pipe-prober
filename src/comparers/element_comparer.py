from pyspark.sql.functions import col, when, lit
from .base_comparer import DataComparer


class ElementwiseComparer(DataComparer):
    def __init__(self, key: str, custom_condition: str = None):
        """
        :param key: Unique key column used to join the DataFrames.
        :param custom_condition: Optional SQL filter condition to apply to the joined result.
        """
        self.key = key
        self.custom_condition = custom_condition

    def compare(self, df1, df2):
        df1_alias = df1.alias("df1")
        df2_alias = df2.alias("df2")
        joined_df = df1_alias.join(df2_alias, on=self.key, how="inner")
        columns_to_compare = [c for c in df1.columns if c != self.key]
        for column in columns_to_compare:
            joined_df = joined_df.withColumn(
                f"{column}_comparison",
                when(col(f"df1.{column}") == col(f"df2.{column}"), lit("Match"))
                .otherwise(lit("Mismatch"))
            )
        if self.custom_condition:
            joined_df = joined_df.filter(self.custom_condition)
        return joined_df
