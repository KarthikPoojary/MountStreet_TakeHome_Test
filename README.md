# Mount Street Data Engineering Assessment

## 1. Executive Summary
This repository contains an end-to-end data engineering solution designed to transform raw, semi-structured issue logs into a high-performance analytical data model.

The solution moves beyond simple JSON flattening by implementing **Dimensional Modeling (Star Schema)** and **Process Mining** techniques to extract critical operational KPIs (MTTI, MTTR, Workload Balance) that were latent in the source data.

**Key Technical Features:**
* **Schema Drift Handling:** Implemented an Entity-Attribute-Value (EAV) pattern for `CustomAttributes` to ensure the pipeline never breaks when upstream systems add new fields.
* **Hybrid State Engine:** Prioritizes explicit system status where available, while using the nested `events` log to reconstruct the historical timeline and "Resolution Date".
* **Robust Data Quality:** Developed a universal timestamp parser and "Business Time" logic to handle mixed formats and retrospective data entry (fixing negative duration issues).

---

## 2. Project Structure

| Component | Path | Description |
| :--- | :--- | :--- |
| **1. Bronze Layer (Ingestion)** | [`unpack_data/unpack_data.py`](./unpack_data/unpack_data.py) | PySpark script that "explodes" nested JSON arrays (`owners`, `tags`, `events`, `attachments`) into standalone Parquet tables. |
| **2. Gold Layer (Logic)** | [`mart_table/create_marts.py`](./mart_table/create_marts.py) | **Main Transformation Engine.** Uses Spark SQL to build the Star Schema, handle M:N relationships via bridge tables, and calculate performance metrics. |
| **Documentation** | [`docs/Architecture_Design.md`](./docs/Architecture_Design.md) | Production architecture design covering Azure Fabric pipelines, incremental loading strategies, and disaster recovery. |
| **ER Diagram** | [`docs/ERD.md`](./docs/ERD.md) | Entity Relationship Diagram of the transformed data model. |
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
* **Challenge:** The raw data presented three analytical challenges:
    1.  **Conflicting Statuses:** An explicit status field existed alongside an event history log.
    2.  **Static Data:** Using `current_date()` on historical test data made all issues look "Critical (>30 Days)".
    3.  **Data Entry Lag:** `CreatedAt` (System Time) was often later than `ResolutionDate` (Real World Time), creating negative KPI values.
* **Solution:**
    * **Hybrid Status Logic:** I implemented a coalesce strategy that respects the Source System Status (e.g., "on_hold") as the truth, falling back to the Event Log only when necessary.
    * **Relative Time Travel:** I implemented a `SnapshotDate` logic (Max Date in Dataset) to calculate "Age" relative to the data itself, ensuring the dashboard looks realistic regardless of when the script is run.
    * **Business-Centric KPIs:** Anchored MTTR and Age calculations to `IdentifiedDate` (Business Time) rather than `CreatedAt` (System Time) to eliminate negative duration artifacts.

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

### 1. Environment Setup:**
* Install required libraries
```bash 
pip install -r requirements.txt
```

### 2. Run Ingestion (Bronze Layer): 
* This resets the output/source_tables directory and repopulates it with clean Parquet files.
```bash 
python unpack_data/unpack_data.py
```

### 3. Run Transformation (Gold Layer):
* This reads the Bronze Parquet files, applies the business logic, and outputs the final Analytical Marts (CSV) to output/marts.
```bash 
python mart_table/create_marts.py
```

### 4. View Results:
* Navigate to output/marts/ to inspect the generated CSV files (fct_Issues.csv, dim_Person.csv, etc.).



##### Submitted for the Mount Street Data Engineering Assessment.
