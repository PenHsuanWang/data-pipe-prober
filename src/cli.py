#!/usr/bin/env python
"""
CLI entry point for Data-Pipe-Prober.
"""

import argparse
import json
import pandas as pd
from src.data_reader import DataReader

# Import Spark-based implementations (if needed)
from src.comparers.row_comparer import RowLevelComparer as SparkRowComparer
from src.comparers.element_comparer import ElementwiseComparer as SparkElementComparer
from src.reports.row_report import RowLevelReportGenerator as SparkRowReportGenerator
from src.reports.element_report import ElementwiseReportGenerator as SparkElementReportGenerator

# Import Pandas-based implementations
from src.comparers.pandas_row_comparer import PandasRowLevelComparer
from src.comparers.pandas_element_comparer import PandasElementwiseComparer
from src.reports.pandas_row_report import PandasRowLevelReportGenerator
from src.reports.pandas_element_report import PandasElementwiseReportGenerator

class DataCompareCLI:
    def __init__(self):
        self.args = self._parse_args()
        # Instantiate Spark only if at least one source is non-local.
        self.spark = None
        if self.args.source1_type not in ["local", "file"] or self.args.source2_type not in ["local", "file"]:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName("DataPipeProber").getOrCreate()

    def _parse_args(self):
        parser = argparse.ArgumentParser(
            description="Data-Pipe-Prober: A CLI tool for comparing data sources"
        )
        parser.add_argument("--source1", required=True, help="Path or table name for first data source")
        parser.add_argument("--source2", required=True, help="Path or table name for second data source")
        parser.add_argument("--source1-type", default="local", help="Source type for source1: local, s3, azure, snowflake")
        parser.add_argument("--source2-type", default="local", help="Source type for source2: local, s3, azure, snowflake")
        parser.add_argument("--source1-options", default="{}", help="JSON string of options for source1 (e.g., credentials)")
        parser.add_argument("--source2-options", default="{}", help="JSON string of options for source2 (e.g., credentials)")
        parser.add_argument("--format", default="csv", help="Data format (csv, json, etc.)")
        parser.add_argument("--header", action="store_true", help="Flag indicating if files have a header row")
        parser.add_argument("--compare-type", choices=["row", "element"], default="row", help="Comparison type: row or element")
        parser.add_argument("--key", help="Unique key column for element-wise comparison (required for element)")
        parser.add_argument("--custom-condition", help="Optional SQL filter condition to apply after comparison")
        parser.add_argument("--report", help="Path for the HTML report output (optional)")
        return parser.parse_args()

    def run(self):
        try:
            source1_options = json.loads(self.args.source1_options)
            source2_options = json.loads(self.args.source2_options)
        except json.JSONDecodeError as e:
            print("Error parsing JSON options:", e)
            if self.spark:
                self.spark.stop()
            return

        reader1 = DataReader(
            self.spark,
            source_path=self.args.source1,
            source_type=self.args.source1_type,
            fmt=self.args.format,
            header=self.args.header,
            options=source1_options
        )
        reader2 = DataReader(
            self.spark,
            source_path=self.args.source2,
            source_type=self.args.source2_type,
            fmt=self.args.format,
            header=self.args.header,
            options=source2_options
        )
        df1 = reader1.read()
        df2 = reader2.read()

        if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
            # Use Pandas implementations
            if self.args.compare_type == "row":
                comparer = PandasRowLevelComparer(custom_condition=self.args.custom_condition)
                diff_result = comparer.compare(df1, df2)
                print("Rows in source1 but not in source2:")
                print(diff_result[0])
                print("Rows in source2 but not in source1:")
                print(diff_result[1])
                if self.args.report:
                    report_gen = PandasRowLevelReportGenerator()
                    report_gen.generate(diff_result, self.args.report)
            else:
                if not self.args.key:
                    print("Error: Unique key (--key) is required for element-wise comparison")
                    if self.spark:
                        self.spark.stop()
                    return
                comparer = PandasElementwiseComparer(key=self.args.key, custom_condition=self.args.custom_condition)
                merged_df = comparer.compare(df1, df2)
                print("Element-wise comparison results:")
                print(merged_df)
                if self.args.report:
                    report_gen = PandasElementwiseReportGenerator()
                    report_gen.generate(merged_df, self.args.report)
        else:
            # Use Spark implementations
            if self.args.compare_type == "row":
                comparer = SparkRowComparer(custom_condition=self.args.custom_condition)
                diff_result = comparer.compare(df1, df2)
                print("Rows in source1 but not in source2:")
                diff_result[0].show(truncate=False)
                print("Rows in source2 but not in source1:")
                diff_result[1].show(truncate=False)
                if self.args.report:
                    report_gen = SparkRowReportGenerator()
                    report_gen.generate(diff_result, self.args.report)
            else:
                if not self.args.key:
                    print("Error: Unique key (--key) is required for element-wise comparison")
                    if self.spark:
                        self.spark.stop()
                    return
                comparer = SparkElementComparer(key=self.args.key, custom_condition=self.args.custom_condition)
                joined_df = comparer.compare(df1, df2)
                print("Element-wise comparison results:")
                joined_df.show(truncate=False)
                if self.args.report:
                    report_gen = SparkElementReportGenerator()
                    report_gen.generate(joined_df, self.args.report)

        if self.spark:
            self.spark.stop()

def main():
    cli = DataCompareCLI()
    cli.run()

if __name__ == "__main__":
    main()
