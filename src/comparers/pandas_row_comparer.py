import pandas as pd
from src.comparers.base_comparer import DataComparer

class PandasRowLevelComparer(DataComparer):
    def __init__(self, custom_condition: str = None, key: str = None):
        self.custom_condition = custom_condition
        self.key = key

    def compare(self, df1: pd.DataFrame, df2: pd.DataFrame):
        key = self.key
        # If no key is provided but both DataFrames contain "id", use "id" as the key.
        if key is None and "id" in df1.columns and "id" in df2.columns:
            key = "id"
        if key is not None:
            # Compare based solely on the key.
            diff_df1 = df1[~df1[key].isin(df2[key])]
            diff_df2 = df2[~df2[key].isin(df1[key])]
        else:
            merged = df1.merge(df2, how='outer', indicator=True)
            diff_df1 = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            diff_df2 = merged[merged['_merge'] == 'right_only'].drop(columns=['_merge'])
        if self.custom_condition:
            diff_df1 = diff_df1.query(self.custom_condition)
            diff_df2 = diff_df2.query(self.custom_condition)
        return diff_df1, diff_df2