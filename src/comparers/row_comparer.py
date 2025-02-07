from pyspark.sql.functions import col, when, lit
from .base_comparer import DataComparer


class RowLevelComparer(DataComparer):
    def __init__(self, custom_condition: str = None):
        """
        :param custom_condition: Optional SQL filter condition to apply to the difference result.
        """
        self.custom_condition = custom_condition

    def compare(self, df1, df2):
        diff_df1 = df1.subtract(df2)
        diff_df2 = df2.subtract(df1)
        if self.custom_condition:
            diff_df1 = diff_df1.filter(self.custom_condition)
            diff_df2 = diff_df2.filter(self.custom_condition)
        return diff_df1, diff_df2
