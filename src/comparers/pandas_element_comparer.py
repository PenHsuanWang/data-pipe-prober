import pandas as pd


class PandasElementwiseComparer:
    def __init__(self, key: str, custom_condition: str = None):
        self.key = key
        self.custom_condition = custom_condition

    def compare(self, df1: pd.DataFrame, df2: pd.DataFrame):
        # Merge the two dataframes on the key
        merged = pd.merge(df1, df2, on=self.key, suffixes=('_df1', '_df2'))
        # For each non-key column, create a comparison column indicating "Match" or "Mismatch"
        for col in df1.columns:
            if col == self.key:
                continue
            merged[f"{col}_comparison"] = merged.apply(
                lambda row: "Match" if row[f"{col}_df1"] == row[f"{col}_df2"] else "Mismatch", axis=1
            )
        if self.custom_condition:
            merged = merged.query(self.custom_condition)
        return merged
