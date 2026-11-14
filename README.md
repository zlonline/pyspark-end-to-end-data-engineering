# PySpark End-to-End Data Engineering Pipeline

## 1. Project Overview

This project demonstrates an end-to-end data engineering pipeline built with Python and Apache Spark.

The pipeline generates synthetic sales data, processes it through Bronze, Silver, and Gold layers, performs data quality validation, and produces analytics-ready Parquet datasets.

It also includes Spark performance benchmarking and result consistency testing.

## 2. Architecture

**Data Generation → Bronze → Silver → Transformation & Join → Gold Analytics → Validation**

The project follows a Medallion Architecture:

- **Bronze:** Raw synthetic Orders, Customers, and Products datasets.
- **Silver:** Cleaned and validated datasets with standardized schemas.
- **Gold:** Customer Sales, Product Sales, and Daily Sales analytics.

## 3. Technology Stack

- Python
- Apache Spark / PySpark
- Spark SQL
- Parquet with Snappy compression
- pytest
- Git / GitHub

## 4. Data Model

The project uses three primary source datasets:

| Dataset | Description |
|---|---|
| Orders | Synthetic order transactions |
| Customers | Customer reference data |
| Products | Product reference data |

The transformed Sales Detail dataset is used to create three Gold analytics datasets:

| Gold Dataset | Purpose |
|---|---|
| Customer Sales | Customer-level revenue and rankings |
| Product Sales | Product-level sales and quantities |
| Daily Sales | Daily revenue, order counts, and running totals |

## 5. Pipeline Stages

1. Generate synthetic source datasets using `spark.range()`.
2. Write raw datasets to Bronze Parquet.
3. Validate schemas and clean invalid or duplicate records.
4. Write cleaned datasets to Silver Parquet.
5. Transform completed orders and join dimension datasets.
6. Aggregate sales into Customer, Product, and Daily analytics.
7. Write Gold datasets using Parquet and Snappy compression.
8. Validate record counts, revenue totals, and output consistency.
9. Optionally benchmark baseline and optimized pipelines.

## 6. Installation

Create a Python virtual environment and install dependencies.

```powershell
python -m venv .venv

.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

A compatible Java runtime is also required for Apache Spark.

## 7. Running the Pipeline

Run the complete pipeline:

```powershell
.\.venv\Scripts\python.exe -m week12.main
```

Run the pipeline with performance benchmarking:

```powershell
.\.venv\Scripts\python.exe -m week12.main --benchmark
```

Run the automated tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q week12/tests/
```

## 8. Data Quality Validation

The project includes checks for:

- Required output datasets.
- Non-empty Gold datasets.
- Revenue reconciliation between Sales Detail and Gold.
- Order count reconciliation.
- Baseline and optimized output consistency.

Result consistency is tested using bidirectional `exceptAll()` comparisons.

## 9. Performance Optimization

The project evaluates several Spark optimization techniques:

- Broadcast Hash Join versus Sort Merge Join.
- Shuffle partition configuration.
- DataFrame caching.
- Parquet column pruning.
- Predicate pushdown.
- Partition pruning.
- Output partition tuning.

Performance comparisons include execution time, shuffle metrics, output file counts, and output sizes.

## 10. Benchmark Results

Benchmark measurements are stored in:

`week12/results/performance_results.md`

Only measured results should be reported. No performance improvement is assumed before benchmarking.

## 11. Project Structure

```text
week12/
├── config.py
├── generate_data.py
├── ingestion.py
├── cleaning.py
├── validation.py
├── transform.py
├── aggregation.py
├── performance_test.py
├── main.py
├── tests/
└── results/
```

## 12. Future Improvements

- Azure Data Lake Storage Gen2 integration.
- Azure Data Factory orchestration.
- Azure Databricks execution.
- Delta Lake support.
- Incremental data processing.
- Automated CI/CD testing.
- Improved data quality monitoring.

## 13. Project Purpose

This project demonstrates practical data engineering skills in distributed data processing, ETL development, performance optimization, data quality validation, and pipeline orchestration.