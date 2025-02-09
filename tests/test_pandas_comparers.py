import pytest
import pandas as pd
from src.comparers.pandas_row_comparer import PandasRowLevelComparer
from src.comparers.pandas_element_comparer import PandasElementwiseComparer

@pytest.fixture
def df_pair():
    df1 = pd.DataFrame([
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob",   "age": 25},
    ])
    df2 = pd.DataFrame([
        {"id": 2, "name": "Bob",     "age": 26},
        {"id": 3, "name": "Charlie", "age": 35}
    ])
    return df1, df2

def test_pandas_row_level(df_pair):
    df1, df2 = df_pair
    comparer = PandasRowLevelComparer()
    diff_df1, diff_df2 = comparer.compare(df1, df2)
    # Expect ID=1 in diff_df1, ID=3 in diff_df2
    assert len(diff_df1) == 1
    assert diff_df1.iloc[0]["id"] == 1
    assert len(diff_df2) == 1
    assert diff_df2.iloc[0]["id"] == 3

def test_pandas_elementwise(df_pair):
    df1, df2 = df_pair
    comparer = PandasElementwiseComparer(key="id")
    merged_df = comparer.compare(df1, df2)
    # Only matching ID = 2
    assert len(merged_df) == 1
    row = merged_df.iloc[0]
    assert row["name_comparison"] == "Match"
    assert row["age_comparison"] == "Mismatch"  # 25 != 26