import pandas as pd
import pytest
from src.reports.pandas_row_report import PandasRowLevelReportGenerator


@pytest.fixture
def pandas_diff_non_empty():
    # Non-empty differences represented by Pandas DataFrames.
    df1 = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
    df2 = pd.DataFrame({'id': [3], 'value': ['c']})
    return df1, df2


@pytest.fixture
def pandas_diff_empty():
    # Empty DataFrames with defined columns.
    df1 = pd.DataFrame(columns=['id', 'value'])
    df2 = pd.DataFrame(columns=['id', 'value'])
    return df1, df2


@pytest.fixture
def pandas_diff_partial():
    # One DataFrame is empty while the other is not.
    df1 = pd.DataFrame({'id': [1], 'value': ['a']})
    df2 = pd.DataFrame(columns=['id', 'value'])
    return df1, df2


def test_pandas_row_report_non_empty(tmp_path, pandas_diff_non_empty):
    output_file = tmp_path / "pandas_row_report_non_empty.html"
    report = PandasRowLevelReportGenerator()
    report.generate(pandas_diff_non_empty, str(output_file))

    content = output_file.read_text()
    # Check for report title and table content.
    assert "Row-level Data Comparison Report" in content
    assert "<table" in content


def test_pandas_row_report_empty(tmp_path, pandas_diff_empty):
    output_file = tmp_path / "pandas_row_report_empty.html"
    report = PandasRowLevelReportGenerator()
    report.generate(pandas_diff_empty, str(output_file))

    content = output_file.read_text()
    # Both sections should indicate no differences.
    assert content.count("No differences found.") == 2


def test_pandas_row_report_partial(tmp_path, pandas_diff_partial):
    output_file = tmp_path / "pandas_row_report_partial.html"
    report = PandasRowLevelReportGenerator()
    report.generate(pandas_diff_partial, str(output_file))

    content = output_file.read_text()
    # Expect one table (for the non-empty DataFrame) and one "No differences found" message.
    assert content.count("No differences found.") == 1
    assert "<table" in content


def test_pandas_row_report_file_write_error(monkeypatch, tmp_path, pandas_diff_non_empty):
    output_file = tmp_path / "unwritable.html"

    def fake_open(*args, **kwargs):
        raise IOError("Simulated file write error")

    monkeypatch.setattr("builtins.open", fake_open)

    report = PandasRowLevelReportGenerator()
    with pytest.raises(IOError, match="Simulated file write error"):
        report.generate(pandas_diff_non_empty, str(output_file))
