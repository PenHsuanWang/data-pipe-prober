from .base_report import ReportGenerator


class ElementwiseReportGenerator(ReportGenerator):
    def generate(self, joined_df, output_path: str):
        pdf = joined_df.toPandas()
        comparison_cols = [c for c in pdf.columns if c.endswith("_comparison")]
        key = joined_df.columns[0]  # Assume first column is the key for display

        html = "<html><head><title>Element-wise Data Comparison Report</title>"
        html += "<style>table { border-collapse: collapse; } th, td { border: 1px solid black; padding: 5px; }</style>"
        html += "</head><body><h1>Element-wise Data Comparison Report</h1>"
        html += f"<h2>Unique Key: {key}</h2>"
        html += "<table><tr>"
        html += f"<th>{key}</th>"
        for comp in comparison_cols:
            col_name = comp.replace("_comparison", "")
            html += f"<th>{col_name}</th>"
        html += "</tr>"
        for _, row in pdf.iterrows():
            if any(row[comp] == "Mismatch" for comp in comparison_cols):
                html += "<tr>"
                html += f"<td>{row[key]}</td>"
                for comp in comparison_cols:
                    col_name = comp.replace("_comparison", "")
                    if row[comp] == "Mismatch":
                        val1 = row.get(f"df1.{col_name}", "N/A")
                        val2 = row.get(f"df2.{col_name}", "N/A")
                        html += f"<td>df1: {val1}<br/>df2: {val2}</td>"
                    else:
                        html += "<td>Match</td>"
                html += "</tr>"
        html += "</table></body></html>"

        with open(output_path, "w") as f:
            f.write(html)
        print(f"Element-wise report generated at: {output_path}")
