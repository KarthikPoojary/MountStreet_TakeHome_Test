# Mount Street Data Engineer Take-Home

## Overview
This repository contains the complete data engineering solution for the Mount Street Issue Management dataset. It demonstrates an end-to-end pipeline including:
1.  **Ingestion (Bronze):** Unpacking complex nested JSON.
2.  **Transformation (Gold):** Building a Kimball Star Schema with data quality checks.
3.  **Reporting:** Power BI semantic model design.
4.  **Architecture:** Production design for Azure Fabric automation.

## Project Structure

| Task | File / Path | Description |
| :--- | :--- | :--- |
| **1. Unpack Data** | [`unpack_data/unpack_data.py`](./unpack_data/unpack_data.py) | PySpark script to flatten JSON arrays and normalize nested objects. |
| **ER Diagram** | [`unpack_data/ERD.png`](./unpack_data/ERD.png) | Entity-Relationship Diagram of the source data model. |
| **2. Create Marts (Logic)** | [`mart_table/create_marts.py`](./mart_table/create_marts.py) | **Main Execution Script.** Transforms raw data into Fact/Dim tables and handles data quality (timestamp regex, encoding fixes). |
| **2. Create Marts (SQL)** | [`mart_table/create_marts.sql`](./mart_table/create_marts.sql) | SQL version of the transformation logic for documentation and readability. |
| **3. Power BI** | [`powerBI/`](./powerBI/) | Contains the Semantic Model design and dashboard deliverables. |
| **4. Automation** | [`automation&Fabric_pipeline/Architecture_Design.md`](./automation&Fabric_pipeline/Architecture_Design.md) | Technical design doc covering Fabric pipelines, schema drift (EAV pattern), and orchestration strategies. |
| **Output Data** | `output/marts/` | Generated CSV files ready for Power BI ingestion. |
| **Source Data** | `data/` | Original anonymised JSON dataset. |

## Key Engineering Decisions

### 1. Robust Data Quality
* **Mixed Timestamps:** Implemented a robust `CASE WHEN...RLIKE` strategy in PySpark to reliably parse mixed ISO 8601 and Epoch Millisecond timestamps found in the source.
* **Encoding Fixes:** Applied sanitation logic to `Title` fields to handle special characters (Em Dashes) that cause encoding issues in downstream reporting tools.

### 2. Schema Drift Strategy
* **Vertical Decomposition:** Utilized an **Entity-Attribute-Value (EAV)** pattern for `CustomAttributeData`. This allows new, unknown keys to appear in the source without breaking the pipeline schema or requiring DDL changes.

### 3. Dimensional Modeling
* **Bridge Tables:** Modeled `Owners` and `Contributors` using a Bridge Table approach (`bridge_Issue_People`) to accurately handle Many-to-Many relationships and avoid row duplication in the Fact table.

## How to Run

**Prerequisites:** Python 3.10+, Java 11/17, PySpark.

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the Mart Generation:**
    This script reads the data, applies transformations, and outputs the final CSVs to `output/marts`.
    ```bash
    python mart_table/create_marts.py
    ```

---
*Generated for Mount Street Data Engineering Assessment.*