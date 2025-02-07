class PandasElementwiseReportGenerator:
    def generate(self, merged_df, output_path: str):
        comparison_cols = [col for col in merged_df.columns if col.endswith("_comparison")]
        key = merged_df.columns[0]  # assuming the first column is the key
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
        for _, row in merged_df.iterrows():
            if any(row[comp] == "Mismatch" for comp in comparison_cols):
                html += "<tr>"
                html += f"<td>{row[key]}</td>"
                for comp in comparison_cols:
                    col_name = comp.replace("_comparison", "")
                    if row[comp] == "Mismatch":
                        val1 = row.get(f"{col_name}_df1", "N/A")
                        val2 = row.get(f"{col_name}_df2", "N/A")
                        html += f"<td>df1: {val1}<br/>df2: {val2}</td>"
                    else:
                        html += "<td>Match</td>"
                html += "</tr>"
        html += "</table></body></html>"
        with open(output_path, "w") as f:
            f.write(html)
        print(f"Element-wise report generated at: {output_path}")
