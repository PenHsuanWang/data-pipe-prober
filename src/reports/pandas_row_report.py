class PandasRowLevelReportGenerator:
    def generate(self, diff_result, output_path: str):
        diff_df1, diff_df2 = diff_result
        html = "<html><head><title>Row-level Data Comparison Report</title>"
        html += "<style>table { border-collapse: collapse; } th, td { border: 1px solid black; padding: 5px; }</style>"
        html += "</head><body><h1>Row-level Data Comparison Report</h1>"
        html += "<h2>Rows in source1 but not in source2</h2>"
        if not diff_df1.empty:
            html += diff_df1.to_html(index=False)
        else:
            html += "<p>No differences found.</p>"
        html += "<h2>Rows in source2 but not in source1</h2>"
        if not diff_df2.empty:
            html += diff_df2.to_html(index=False)
        else:
            html += "<p>No differences found.</p>"
        html += "</body></html>"
        with open(output_path, "w") as f:
            f.write(html)
        print(f"Row-level report generated at: {output_path}")
