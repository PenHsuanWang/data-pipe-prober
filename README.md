# Data-Pipe-Prober (Developer-Focused)

**Data-Pipe-Prober** is a Python-based tool for comparing datasets across different stages of a data pipeline, using either **pandas** or **Spark**. It provides row-level and element-wise comparisons, an optional column subset filter, and custom filtering conditions. This README details the **architecture**, **installation** steps, **usage** patterns, and **extension** points from a developer’s perspective.

---

## Table of Contents

1. [Features](#features)  
2. [Project Structure](#project-structure)  
3. [Installation](#installation)  
4. [Usage and Examples](#usage-and-examples)  
5. [Core Components](#core-components)  
    - [CLI](#cli)  
    - [DataReader](#datareader)  
    - [Comparers](#comparers)  
    - [Reports](#reports)  
    - [Utilities](#utilities)  
6. [Extending the Project](#extending-the-project)  
    - [Adding a New Data Source](#adding-a-new-data-source)  
    - [Adding a New Comparer](#adding-a-new-comparer)  
7. [Testing](#testing)  
8. [Developer Notes & Best Practices](#developer-notes--best-practices)

---

## 1. Features

- **Multi-Source Data Support**  
  - Local files (CSV, JSON) read via **pandas**  
  - Cloud-based or large data (S3, Azure, Snowflake, etc.) read via **Spark**  
  - Unified CLI experience across different data storage systems

- **Comparison Modes**  
  - **Row-level**: Identify new or missing rows between two dataframes  
  - **Element-wise**: Match rows by a key column, then label each field as “Match” or “Mismatch”

- **Subset Columns**  
  - Compare only a **subset** of columns via `--columns`, so new or irrelevant columns don’t cause unwanted mismatches

- **Custom Filtering**  
  - Use `--custom-condition` to filter the comparison results (e.g. focus only on status == "FAIL")

- **Human-Readable Reports**  
  - Optional HTML output that highlights differences or mismatched columns

---

## 2. Project Structure

```
.
├── src/
│   ├── cli.py                      # Main CLI entry point
│   ├── data_reader.py              # Reads data from local/Spark-based sources
│   ├── utils.py                    # Misc utility functions
│   ├── comparers/
│   │   ├── base_comparer.py        # Abstract class for comparers
│   │   ├── row_comparer.py         # Spark row-level comparer
│   │   ├── element_comparer.py     # Spark element-wise comparer
│   │   ├── pandas_row_comparer.py  # Pandas row-level comparer
│   │   └── pandas_element_comparer.py # Pandas element-wise comparer
│   └── reports/
│       ├── base_report.py          # Abstract class for reporters
│       ├── row_report.py           # Spark row-level HTML report
│       ├── element_report.py       # Spark element-wise HTML report
│       ├── pandas_row_report.py    # Pandas row-level HTML report
│       └── pandas_element_report.py# Pandas element-wise HTML report
├── tests/
│   ├── test_cli.py
│   ├── test_data_reader.py
│   ├── test_comparers.py
│   └── test_pandas_comparers.py
└── README.md                       # This file
```

---

## 3. Installation

1. **Clone** the repository:
   ```bash
   git clone https://github.com/<org>/data-pipe-prober.git
   cd data-pipe-prober
   ```
2. **Python Environment**:  
   - Recommended: create a virtual environment:
     ```bash
     python -m venv venv
     source venv/bin/activate
     ```
3. **Install Dependencies**:
   - If you’re only using **pandas** (local data), you can run:
     ```bash
     pip install pandas
     ```
   - If you need **Spark** for large or cloud data:
     ```bash
     pip install pyspark
     ```
   - For Snowflake, ensure you have the [Spark Snowflake connector](https://docs.snowflake.com/en/user-guide/spark-connector) on your classpath or installed via Spark’s `--packages` argument.  
   - (Optional) For testing:
     ```bash
     pip install pytest
     ```

---

## 4. Usage and Examples

The main entry point is `cli.py`. You can run it either with `python src/cli.py` or `spark-submit src/cli.py` if you’re using Spark and want to distribute the job across a cluster.

### Basic Row-Level Comparison (Local CSV)

```bash
python src/cli.py \
  --source1 data_stage1.csv \
  --source2 data_stage2.csv \
  --source1-type local \
  --source2-type local \
  --format csv \
  --header \
  --compare-type row \
  --report row_comparison.html
```

### Comparing Subset of Columns

```bash
python src/cli.py \
  --source1 local_before.json \
  --source2 local_after.json \
  --source1-type local \
  --source2-type local \
  --format json \
  --compare-type row \
  --columns "record_id, name" \
  --report subset_diff_report.html
```

---

## 5. Core Components

### 5.1 CLI

- **`src/cli.py`** defines **`DataCompareCLI`**, which:
  - **Parses arguments** using `argparse`.  
  - **Instantiates Spark** if needed.  
  - **Reads data** from two sources using `DataReader`.  
  - **Subsets columns** (if `--columns` is provided).  
  - Chooses either **Spark** or **Pandas** comparers based on the DataFrame type.  
  - Optionally **generates** an HTML report.

### 5.2 DataReader

- **`src/data_reader.py`**  
  - Responsible for returning a **pandas.DataFrame** (local) or **Spark** DataFrame (cloud).  
  - Checks `source_type` to decide reading strategy.  
  - If `source_type == "snowflake"`, it uses either `"dbtable"` or `"query"` options to load from Snowflake.

### 5.3 Comparers

- Each comparer implements a `compare(df1, df2)` method.  
- **RowLevelComparer**:
  - For Pandas: merges with `indicator=True` and identifies “left_only”/“right_only”.  
  - For Spark: uses `df1.subtract(df2)` and `df2.subtract(df1)`.  
- **ElementwiseComparer**:
  - For Pandas: merges on `--key` and adds `_comparison` columns.  
  - For Spark: joins on `--key` and similarly creates `_comparison` columns.  
- Optionally applies a **custom condition** or filter after computing differences.

### 5.4 Reports

- **`src/reports/`**  
  - Generates **HTML** outputs.  
  - Row-level reports list unique rows from each source.  
  - Element-wise reports display mismatches for each column.  
  - Applies basic styling to produce a clear, shareable HTML file.

### 5.5 Utilities

- **`src/utils.py`**  
  - Currently houses helper functions, like `parse_json_options`, or any custom logic you want to share across modules.

---

## 6. Extending the Project

### 6.1 Adding a New Data Source

1. **Update `DataReader`**  
   - If it’s a Spark-compatible source, add a new branch (e.g. `"deltalake"` or `"bigquery"`) in `read()`.  
   - If it’s local or purely Python-based, read it via Pandas.  
2. **Update CLI** (Optional)  
   - If you want a custom source type, pass `--sourceX-type <new_source_type>` in the CLI.  
   - The CLI will call `DataReader` with that type.

### 6.2 Adding a New Comparer

1. **Create a new** `MyCustomComparer(DataComparer)` in `src/comparers/`.  
2. **Implement** `compare(df1, df2)` logic.  
3. **(Optional)** Add a new argument to the CLI if you want to enable it via `--compare-type my_custom_compare`.  
   - Or let it remain an internal tool you call programmatically.

---

## 7. Testing

- **Pytest** is recommended.  
- Typical structure is in `tests/`:
  - `test_data_reader.py` checks reading from local CSV/JSON, plus mock-based tests for Spark or Snowflake.  
  - `test_comparers.py` tests the Spark comparers with small in-memory data.  
  - `test_pandas_comparers.py` tests Pandas comparers with sample DataFrames.  
  - `test_cli.py` does minimal end-to-end tests by mocking command-line args.  
- Run tests:
  ```bash
  pytest tests
  ```
