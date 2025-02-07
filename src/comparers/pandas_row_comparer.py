import pandas as pd
from src.comparers.base_comparer import DataComparer

class PandasRowLevelComparer(DataComparer):
    def __init__(self, custom_condition: str = None):
        self.custom_condition = custom_condition

    def compare(self, df1: pd.DataFrame, df2: pd.DataFrame):
        # Perform a row-level comparison by merging with indicator
        merged = df1.merge(df2, how='outer', indicator=True)
        diff_df1 = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
        diff_df2 = merged[merged['_merge'] == 'right_only'].drop(columns=['_merge'])
        if self.custom_condition:
            diff_df1 = diff_df1.query(self.custom_condition)
            diff_df2 = diff_df2.query(self.custom_condition)
        return diff_df1, diff_df2
