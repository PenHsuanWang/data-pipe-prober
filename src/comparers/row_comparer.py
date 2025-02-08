from pyspark.sql.functions import col, when, lit
from .base_comparer import DataComparer


class RowLevelComparer(DataComparer):
    def __init__(self, custom_condition: str = None, key: str = None):
        """
        :param custom_condition: Optional SQL filter condition to apply to the difference result.
        :param key: Optional key column for row-level comparison. If provided (or if both DataFrames contain "id"),
                    rows are compared based on the key.
        """
        self.custom_condition = custom_condition
        self.key = key

    def compare(self, df1, df2):
        key = self.key
        # If no key is provided but both DataFrames have an "id" column, use "id" as the key.
        if key is None and "id" in df1.columns and "id" in df2.columns:
            key = "id"
        if key is not None:
            # Use leftanti joins on the key to return rows in one DataFrame whose key is missing in the other.
            diff_df1 = df1.join(df2.select(key).dropDuplicates(), on=key, how="leftanti")
            diff_df2 = df2.join(df1.select(key).dropDuplicates(), on=key, how="leftanti")
        else:
            diff_df1 = df1.subtract(df2)
            diff_df2 = df2.subtract(df1)
        if self.custom_condition:
            diff_df1 = diff_df1.filter(self.custom_condition)
            diff_df2 = diff_df2.filter(self.custom_condition)
        return diff_df1, diff_df2