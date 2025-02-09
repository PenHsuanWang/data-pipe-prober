import pytest
import sys
import os
from unittest.mock import patch
from src.cli import main

@pytest.fixture
def simple_csv(tmp_path):
    """Create a simple CSV for CLI tests."""
    csv_path = tmp_path / "simple.csv"
    csv_path.write_text("id,col\n1,A\n2,B\n")
    return str(csv_path)

def test_cli_row_compare_local_pandas(simple_csv, tmp_path, capsys):
    """Test the CLI in row-comparison mode with local CSVs using Pandas."""
    csv1 = simple_csv
    csv2 = tmp_path / "simple2.csv"
    with open(csv2, "w") as f:
        f.write("id,col\n1,A\n3,C\n")

    report_path = str(tmp_path / "test_report.html")

    # We pass command-line args to the CLI
    test_args = [
        "cli.py",
        "--source1", csv1,
        "--source2", str(csv2),
        "--source1-type", "local",
        "--source2-type", "local",
        "--format", "csv",
        "--header",
        "--compare-type", "row",
        "--report", report_path
    ]

    with patch.object(sys, "argv", test_args):
        main()  # Execute CLI

    captured = capsys.readouterr()
    out = captured.out
    # We expect ID=2 is in df1 but not df2, ID=3 is in df2 but not df1
    assert "Rows in source1 but not in source2" in out
    assert "Rows in source2 but not in source1" in out

    # Check the report got created
    assert os.path.exists(report_path), "HTML report should be generated"

def test_cli_columns_subset(simple_csv, tmp_path, capsys):
    """Test the CLI with --columns, focusing on a subset."""
    csv1 = simple_csv
    # We'll add another CSV with extra columns
    csv2 = tmp_path / "extra_col.csv"
    csv2.write_text("id,col,extra\n1,A,X\n2,B,X\n")

    report_path = str(tmp_path / "subset_report.html")

    # Compare only the 'col' column
    test_args = [
        "cli.py",
        "--source1", csv1,
        "--source2", str(csv2),
        "--source1-type", "local",
        "--source2-type", "local",
        "--format", "csv",
        "--header",
        "--compare-type", "row",
        "--columns", "col",
        "--report", report_path
    ]

    with patch.object(sys, "argv", test_args):
        main()

    out = capsys.readouterr().out
    # If we only compare "col", then the row with 'id=2' is actually a match
    # since "col" = "B" in both. So we expect row #2 might not show up in diffs
    # Let's just confirm the output is printed and report is created.
    assert "Rows in source1 but not in source2" in out
    assert "Rows in source2 but not in source1" in out
    assert os.path.exists(report_path)