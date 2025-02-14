import os
import pandas as pd
import pytest
from src.reports.row_report import RowLevelReportGenerator


# DummySparkDF simulates minimal Spark DataFrame behavior.
class DummySparkDF:
    def __init__(self, data):
        # data is a list of dictionaries representing rows.
        self.data = data

    def count(self):
        return len(self.data)

    def toPandas(self):
        return pd.DataFrame(self.data)


@pytest.fixture
def spark_diff_non_empty():
    # Test with non-empty differences for both sources.
    data1 = [{'id': 1, 'value': 'a'}, {'id': 2, 'value': 'b'}]
    data2 = [{'id': 3, 'value': 'c'}]
    return DummySparkDF(data1), DummySparkDF(data2)


@pytest.fixture
def spark_diff_empty():
    # Test when both diff data sources are empty.
    return DummySparkDF([]), DummySparkDF([])


@pytest.fixture
def spark_diff_partial():
    # Test when one diff is empty and the other is non-empty.
    data1 = [{'id': 1, 'value': 'a'}]
    data2 = []  # empty
    return DummySparkDF(data1), DummySparkDF(data2)


def test_row_report_non_empty(tmp_path, spark_diff_non_empty):
    output_file = tmp_path / "row_report_non_empty.html"
    report = RowLevelReportGenerator()
    report.generate(spark_diff_non_empty, str(output_file))

    content = output_file.read_text()
    # Verify HTML structure and expected headings.
    assert content.startswith("<html>")
    assert "Row-level Data Comparison Report" in content
    assert "Rows in source1 but not in source2" in content
    assert "Rows in source2 but not in source1" in content
    # Expect tables for both diff sets.
    assert "<table" in content


def test_row_report_empty(tmp_path, spark_diff_empty):
    output_file = tmp_path / "row_report_empty.html"
    report = RowLevelReportGenerator()
    report.generate(spark_diff_empty, str(output_file))

    content = output_file.read_text()
    # Should show "No differences found" for both sections.
    assert "No differences found." in content
    # HTML structure check.
    assert content.startswith("<html>")
    assert content.endswith("</html>")


def test_row_report_partial(tmp_path, spark_diff_partial):
    output_file = tmp_path / "row_report_partial.html"
    report = RowLevelReportGenerator()
    report.generate(spark_diff_partial, str(output_file))

    content = output_file.read_text()
    # One section should show table (non-empty) and the other "No differences found."
    assert "No differences found." in content
    assert "<table" in content


def test_row_report_file_write_error(monkeypatch, tmp_path, spark_diff_non_empty):
    output_file = tmp_path / "unwritable.html"

    # Monkeypatch open to simulate a file write error.
    def fake_open(*args, **kwargs):
        raise IOError("Simulated file write error")

    monkeypatch.setattr("builtins.open", fake_open)

    report = RowLevelReportGenerator()
    with pytest.raises(IOError, match="Simulated file write error"):
        report.generate(spark_diff_non_empty, str(output_file))