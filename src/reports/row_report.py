from .base_report import ReportGenerator


class RowLevelReportGenerator(ReportGenerator):
    def generate(self, diff_result, output_path: str):
        diff_df1, diff_df2 = diff_result
        pdf1 = diff_df1.toPandas() if diff_df1.count() > 0 else None
        pdf2 = diff_df2.toPandas() if diff_df2.count() > 0 else None

        html = "<html><head><title>Row-level Data Comparison Report</title>"
        html += "<style>table { border-collapse: collapse; } th, td { border: 1px solid black; padding: 5px; }</style>"
        html += "</head><body><h1>Row-level Data Comparison Report</h1>"

        html += "<h2>Rows in source1 but not in source2</h2>"
        if pdf1 is not None and not pdf1.empty:
            html += pdf1.to_html(index=False)
        else:
            html += "<p>No differences found.</p>"

        html += "<h2>Rows in source2 but not in source1</h2>"
        if pdf2 is not None and not pdf2.empty:
            html += pdf2.to_html(index=False)
        else:
            html += "<p>No differences found.</p>"

        html += "</body></html>"

        with open(output_path, "w") as f:
            f.write(html)
        print(f"Row-level report generated at: {output_path}")
