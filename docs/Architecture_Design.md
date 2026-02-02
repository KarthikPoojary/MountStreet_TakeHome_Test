# Automation & Fabric Pipeline Architecture

## Design Rationale & Assumptions

### A. Core Design Philosophy (The "Why")
* **Compute Engine (PySpark vs. Pandas):**
    * **Decision:** I chose **PySpark** over Pandas.
    * **Reasoning:** Pandas fails to scale beyond memory limits. PySpark ensures this exact pipeline architecture works identically whether the input is 5MB or 500GB.
* **Data Modeling (Star Schema):**
    * **Decision:** I transformed the nested JSON into a **Star Schema** (Fact/Dim) rather than a flat wide table.
    * **Reasoning:** Power BI is highly optimized for Star Schemas. A wide flat table would lead to massive data duplication (repeating User/Department details for every issue) and slower query performance on large datasets.
* **Schema Drift Strategy (EAV Pattern):**
    * **Decision:** I modeled `CustomAttributeData` using an Entity-Attribute-Value (EAV) pattern (`fct_Issue_Attributes`) instead of dynamic columns.
    * **Reasoning:** If we altered the table schema (DDL) for every new key, the pipeline would be brittle and prone to locking. EAV allows infinite new attributes without breaking the pipeline.

### B. Key Assumptions
1.  **Timezone Consistency:** The source data had mixed formats (ISO strings and Epoch MS). I have assumed all timestamps represent **UTC** to ensure accurate duration calculations (e.g., `IssueAgeDays`).
2.  **User Uniqueness:** I assumed that `UserId` is the stable GUID across the system. A single person might appear as an Owner on one issue and a Contributor on another; `dim_Person` was created to consolidate these roles into a single profile.
3.  **Data Volume:** I assumed this data represents a "Snapshot" or a "Batch" of recent issues. The architecture is designed to handle incremental loads (appending new issues) via the `IssueId` key.

---

## 1. Ingestion Architecture (Bronze Layer)

We follow the **Medallion Architecture** (Bronze → Silver → Gold) within Microsoft Fabric / Azure.

### Scenario A: Data via API (REST)
**Pattern:** Scheduled Polling (or Webhook).
* **Tool:** Azure Data Factory (ADF) / Fabric Data Pipeline.
* **Workflow:**
    1.  **Orchestration:** Pipeline runs on a schedule (e.g., every 15 mins).
    2.  **Activity:** `Copy Data` activity calls the REST API.
    3.  **Pagination:** Implements a loop to handle pagination tokens until all pages are fetched.
    4.  **Watermarking:** Tracks `last_modified_timestamp` in a control table. Only requests data `> Watermark` to minimize payload.
    5.  **Landing:** Saves raw JSON to ADLS Gen2 `Landing/API/{YYYYMMDD}/{HHMM}.json`.

### Scenario B: Data in Blob Storage
**Pattern:** Event-Driven (Real-Time).
* **Tool:** Azure Event Grid + Fabric Pipeline (Storage Event Trigger).
* **Workflow:**
    1.  **Event:** A file lands in the ADLS `Inbox` container.
    2.  **Trigger:** **Azure Event Grid** detects the `BlobCreated` event.
    3.  **Action:** Fires a Fabric Pipeline immediately, passing `@triggerBody().folderPath` and `@triggerBody().fileName` as parameters.
    4.  **Advantage:** Zero latency; processing starts the second the file arrives.

---

## 2. Handling Schema Drift & Data Quality (Silver Layer)

### A. Mixed Timestamp Normalization
* **Observation:** Source data mixes ISO 8601 strings (`2024-06-02T...`) with Epoch Milliseconds (`1717580119124`).
* **Solution:** Robust conditional parsing in PySpark.
    ```sql
    CASE
        -- Detect 13-digit Epoch (Millisecond) timestamps via Regex
        WHEN CreatedAt RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(CreatedAt AS LONG))
        -- Fallback to standard ISO parser
        ELSE to_timestamp(CreatedAt)
    END
    ```
    * **Impact:** Prevents pipeline failure on format changes; standardizes everything to `TimestampType` (UTC).

### B. Dynamic Custom Attributes
* **Observation:** New keys (e.g., `175928_text`) appear without warning.
* **Solution:** **Vertical Decomposition**.
    * Instead of `ALTER TABLE ADD COLUMN`, we explode the JSON map into a vertical table: `fct_Issue_Attributes` (`IssueKey`, `AttributeKey`, `Value`).
    * **Impact:** Zero maintenance required when the upstream app adds new fields; Power BI simply sees new rows.

### C. Validation & Bad Records ("Quarantine & Continue")
* **Policy:** The pipeline must **never fail** on a single bad row.
* **Mechanism:**
    * **Valid rows:** Write to `Silver_Issues`.
    * **Invalid rows:** Write to `_Quarantine` table (Parquet) with `ErrorReason`, `OriginalPayload`, and `IngestTimestamp`.
* **Assertions (Great Expectations):**
    * `IssueId` must not be NULL.
    * `Type` must be one of: `['incident', 'issue', 'risk_event']`.

---

## 3. Orchestration, Scheduling & Recovery

### Triggering
* **Dimensions (Slowly Changing):** Scheduled nightly (e.g., `dim_Person` updates from HR system).
* **Facts (Transactions):** Event-based triggers (File Arrival) for near-real-time operational reporting.

### Idempotency (Duplicate Prevention)
* **Challenge:** If a file is re-processed by accident, we must not double-count issues.
* **Solution:** Use **Delta Lake `MERGE`** (Upsert).
    * **Logic:** Match on `IssueId`.
        * `WHEN MATCHED THEN UPDATE` (Update status/details).
        * `WHEN NOT MATCHED THEN INSERT`.
    * **Result:** Re-running the pipeline 10 times results in the same final state (Idempotent).

### Dependency Management
* **Tool:** Fabric Pipeline / ADF DAGs.
* **Flow:**
    
    * `Bronze_Notebook` (Ingest) → *On Success* → `Silver_Notebook` (Clean/Normalize) → *On Success* → `Gold_Notebook` (Marts).
* **Failure:** Any failure triggers an `Alert Activity` (Teams/Email) to the Data Engineering support queue.