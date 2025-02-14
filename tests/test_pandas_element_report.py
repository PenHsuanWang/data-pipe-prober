import pandas as pd
import pytest
from src.reports.pandas_element_report import PandasElementwiseReportGenerator


@pytest.fixture
def merged_with_mismatches():
    # DataFrame for Pandas element report with mismatches.
    data = {
        'id': [1, 2, 3],
        'col1_comparison': ['Match', 'Mismatch', 'Match'],
        'col2_comparison': ['Mismatch', 'Match', 'Match'],
        # Naming convention: <col>_df1 and <col>_df2 for Pandas reports.
        'col1_df1': [10, 20, 30],
        'col1_df2': [10, 25, 30],
        'col2_df1': ['a', 'b', 'c'],
        'col2_df2': ['x', 'b', 'c']
    }
    return pd.DataFrame(data)


@pytest.fixture
def merged_without_mismatches():
    data = {
        'id': [1, 2],
        'col1_comparison': ['Match', 'Match'],
        'col2_comparison': ['Match', 'Match'],
        'col1_df1': [10, 20],
        'col1_df2': [10, 20],
        'col2_df1': ['a', 'b'],
        'col2_df2': ['a', 'b']
    }
    return pd.DataFrame(data)


@pytest.fixture
def merged_empty():
    # An empty DataFrame with the expected columns.
    columns = ['id', 'col1_comparison', 'col2_comparison', 'col1_df1', 'col1_df2', 'col2_df1', 'col2_df2']
    return pd.DataFrame(columns=columns)


def test_pandas_element_report_with_mismatches(tmp_path, merged_with_mismatches):
    output_file = tmp_path / "pandas_element_report_mismatches.html"
    report = PandasElementwiseReportGenerator()
    report.generate(merged_with_mismatches, str(output_file))

    content = output_file.read_text()
    # Verify the title, key header, and that mismatch details are present.
    assert "Element-wise Data Comparison Report" in content
    assert "Unique Key:" in content
    assert "df1:" in content and "df2:" in content
    # There should be at least one mismatch row.
    assert content.count("<tr>") > 1


def test_pandas_element_report_without_mismatches(tmp_path, merged_without_mismatches):
    output_file = tmp_path / "pandas_element_report_no_mismatches.html"
    report = PandasElementwiseReportGenerator()
    report.generate(merged_without_mismatches, str(output_file))

    content = output_file.read_text()
    # Only header row should be present (only one <tr> tag).
    assert content.count("<tr>") == 1


def test_pandas_element_report_empty(tmp_path, merged_empty):
    output_file = tmp_path / "pandas_element_report_empty.html"
    report = PandasElementwiseReportGenerator()
    report.generate(merged_empty, str(output_file))

    content = output_file.read_text()
    # Even if empty, headers should be present.
    assert "Element-wise Data Comparison Report" in content
    assert content.count("<tr>") == 1


def test_pandas_element_report_file_write_error(monkeypatch, tmp_path, merged_with_mismatches):
    output_file = tmp_path / "unwritable.html"

    def fake_open(*args, **kwargs):
        raise IOError("Simulated file write error")

    monkeypatch.setattr("builtins.open", fake_open)

    report = PandasElementwiseReportGenerator()
    with pytest.raises(IOError, match="Simulated file write error"):
        report.generate(merged_with_mismatches, str(output_file))
