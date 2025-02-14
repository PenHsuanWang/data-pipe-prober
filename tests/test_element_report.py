import pandas as pd
import pytest
from src.reports.element_report import ElementwiseReportGenerator


# Dummy wrapper to simulate a Spark DataFrame by providing a toPandas() method and columns property.
class DummySparkDFWrapper:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def toPandas(self):
        return self.df

    @property
    def columns(self):
        return self.df.columns


# Fixtures now return wrapped DataFrames
@pytest.fixture
def merged_with_mismatches_df():
    # Simulate a DataFrame for Spark element report where at least one column has mismatches.
    data = {
        'id': [1, 2, 3],
        'col1_comparison': ['Match', 'Mismatch', 'Match'],
        'col2_comparison': ['Mismatch', 'Match', 'Match'],
        # Convention for Spark: columns with dots to represent source columns.
        'df1.col1': [10, 20, 30],
        'df2.col1': [10, 25, 30],
        'df1.col2': ['a', 'b', 'c'],
        'df2.col2': ['x', 'b', 'c']
    }
    return pd.DataFrame(data)


@pytest.fixture
def merged_without_mismatches_df():
    # All rows have matching values.
    data = {
        'id': [1, 2],
        'col1_comparison': ['Match', 'Match'],
        'col2_comparison': ['Match', 'Match'],
        'df1.col1': [10, 20],
        'df2.col1': [10, 20],
        'df1.col2': ['a', 'b'],
        'df2.col2': ['a', 'b']
    }
    return pd.DataFrame(data)


@pytest.fixture
def merged_empty_df():
    # An empty DataFrame with expected columns.
    columns = ['id', 'col1_comparison', 'col2_comparison', 'df1.col1', 'df2.col1', 'df1.col2', 'df2.col2']
    return pd.DataFrame(columns=columns)


# Wrappers to simulate Spark DataFrames
@pytest.fixture
def merged_with_mismatches(merged_with_mismatches_df):
    return DummySparkDFWrapper(merged_with_mismatches_df)


@pytest.fixture
def merged_without_mismatches(merged_without_mismatches_df):
    return DummySparkDFWrapper(merged_without_mismatches_df)


@pytest.fixture
def merged_empty(merged_empty_df):
    return DummySparkDFWrapper(merged_empty_df)


def test_element_report_with_mismatches(tmp_path, merged_with_mismatches):
    output_file = tmp_path / "element_report_mismatches.html"
    report = ElementwiseReportGenerator()
    report.generate(merged_with_mismatches, str(output_file))

    content = output_file.read_text()
    # Verify report title, header, and at least one row with mismatches.
    assert "Element-wise Data Comparison Report" in content
    assert "Unique Key:" in content
    # Verify that at least one mismatch row is rendered (header + data rows).
    assert content.count("<tr>") > 1


def test_element_report_without_mismatches(tmp_path, merged_without_mismatches):
    output_file = tmp_path / "element_report_no_mismatches.html"
    report = ElementwiseReportGenerator()
    report.generate(merged_without_mismatches, str(output_file))

    content = output_file.read_text()
    # When there are no mismatches, only the header row should be present.
    assert content.count("<tr>") == 1


def test_element_report_empty(tmp_path, merged_empty):
    output_file = tmp_path / "element_report_empty.html"
    report = ElementwiseReportGenerator()
    report.generate(merged_empty, str(output_file))

    content = output_file.read_text()
    # Even with an empty DataFrame, the report should render headers.
    assert "Element-wise Data Comparison Report" in content
    # Only header row is present.
    assert content.count("<tr>") == 1


def test_element_report_file_write_error(monkeypatch, tmp_path, merged_with_mismatches):
    output_file = tmp_path / "unwritable.html"

    def fake_open(*args, **kwargs):
        raise IOError("Simulated file write error")

    monkeypatch.setattr("builtins.open", fake_open)

    report = ElementwiseReportGenerator()
    with pytest.raises(IOError, match="Simulated file write error"):
        report.generate(merged_with_mismatches, str(output_file))
