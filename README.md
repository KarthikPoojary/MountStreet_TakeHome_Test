# Mount Street Data Engineering Assessment

## 1. Executive Summary
This repository contains an end-to-end data engineering solution designed to transform raw, semi-structured issue logs into a high-performance analytical data model.

The solution moves beyond simple JSON flattening by implementing **Dimensional Modeling (Star Schema)** and **Process Mining** techniques to extract critical operational KPIs (MTTI, MTTR, Workload Balance) that were latent in the source data.

**Key Technical Features:**
* **Schema Drift Handling:** Implemented an Entity-Attribute-Value (EAV) pattern for `CustomAttributes` to ensure the pipeline never breaks when upstream systems add new fields.
* **Process Mining:** leveraged the nested `events` array to reconstruct the full lifecycle of every ticket, calculating precise "Days to Resolve" and "Current Status" timestamps.
* **Robust Data Quality:** Developed a universal timestamp parser to handle mixed formats (ISO 8601 vs. Epoch Milliseconds) and character encoding issues found in the source.

---

## 2. Project Structure

| Component | Path | Description |
| :--- | :--- | :--- |
| **1. Bronze Layer (Ingestion)** | [`unpack_data/unpack_data.py`](./unpack_data/unpack_data.py) | PySpark script that "explodes" nested JSON arrays (`owners`, `tags`, `events`, `attachments`) into standalone Parquet tables. |
| **2. Gold Layer (Logic)** | [`mart_table/create_marts.py`](./mart_table/create_marts.py) | **Main Transformation Engine.** Uses Spark SQL to build the Star Schema, handle M:N relationships via bridge tables, and calculate performance metrics. |
| **Documentation** | [`automation&Fabric_pipeline/Architecture_Design.md`](./automation&Fabric_pipeline/Architecture_Design.md) | Production architecture design covering Azure Fabric pipelines, incremental loading strategies, and disaster recovery. |
| **Power BI Model** | [`powerBI/`](./powerBI/) | Semantic model design and dashboard instructions. |
| **Output Data** | `output/marts/` | The final "Gold" datasets (CSVs) ready for Power BI ingestion. |
| **Source Data** | `data/` | Original anonymised JSON dataset. |

---

## 3. Solution Details by Task

### Task 1: Unpack Data (The Bronze Layer)
* **Challenge:** The source was a deeply nested JSON with arrays for `owners`, `contributors`, `departments`, `tags`, and `attachments`.
* **Solution:** I implemented a PySpark ingestion script that vertically decomposes these arrays into standalone tables (Third Normal Form).
* **Schema Drift Strategy:** For the dynamic `CustomAttributeData` field, I avoided hard-coding columns. Instead, I exploded it into a Key-Value pair table (`src_CustomAttributes`). This guarantees the pipeline remains resilient to upstream schema changes.

### Task 2: Create Mart Tables (The Gold Layer)
* **Challenge:** The business needs to analyze "Time to Resolve" and "Workload," but the raw data lacked explicit "Status" or "ResolutionDate" columns.
* **Solution:** I treated the `events` array as a transaction log. By ranking these events by timestamp, I derived:
    * **Current Status:** The latest event type (e.g., 'closed', 'mitigated').
    * **Resolution Date:** The exact timestamp of the 'closed' event.
    * **MTTI / MTTR:** Calculated lags between Occurrence, Identification, and Resolution.
* **Modeling Strategy:** I chose a **Kimball Star Schema** to optimize for Power BI performance:
    * **Fact:** `fct_Issues` (Metrics).
    * **Dimensions:** `dim_Person` (Consolidated view of Owners, Contributors, and Modifiers).
    * **Bridges:** `bridge_Issue_People` to correctly model the Many-to-Many relationship between Issues and People.

### Task 3: Power BI (Analytics)
* The data model supports a "Command Center" dashboard focusing on:
    * **Velocity:** Average Days to Identify (MTTI) & Resolve (MTTR).
    * **Risk:** "Stale" and "Orphaned" ticket flags.
    * **Volume:** Backlog trends over time using a standard Date Dimension.

### Task 4: Automation Architecture
* The architecture document outlines a production deployment using **Azure Fabric**.
* **Ingestion:** Event-Grid triggered pipelines for real-time file processing.
* **Processing:** Idempotent PySpark notebooks using Delta Lake `MERGE` commands to handle duplicates and updates safely.

---

## 4. How to Run

**Prerequisites:** Python 3.10+, Java 11/17 (for PySpark).

**1. Environment Setup:**

# Install required libraries
pip install -r requirements.txt

**2. Run Ingestion (Bronze Layer):** This resets the output/source_tables directory and repopulates it with clean Parquet files.
python unpack_data/unpack_data.py

**3. Run Transformation (Gold Layer):** This reads the Bronze Parquet files, applies the business logic, and outputs the final Analytical Marts (CSV) to output/marts.
python mart_table/create_marts.py

**4. View Results:** Navigate to output/marts/ to inspect the generated CSV files (fct_Issues.csv, dim_Person.csv, etc.).

```bash