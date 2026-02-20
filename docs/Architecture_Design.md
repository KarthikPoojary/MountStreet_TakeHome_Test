# Automation & Fabric Pipeline Architecture

This document outlines the production architecture for deploying the Mount Street Data Engineering solution within **Microsoft Fabric**.

---

## 1. Architecture & Approach

### Design Philosophy
The pipeline architecture is designed to be **modular, resilient, and secure**. We leverage Microsoft Fabric's unified capacity to handle both batch-based historical loads (API) and real-time event-driven ingestion (Blob), ensuring a "Medallion Architecture" flow (Raw $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold) regardless of the source origin.

---

### Scenario 1a: REST API Integration (The "Pull" Pattern)
**Use Case:** High-latency, periodic ingestion (e.g., pulling hourly audit logs from a SaaS provider).

* **1. Orchestration Engine:**
    * We use **Fabric Data Factory Pipelines** as the orchestrator. Unlike a simple script, this provides built-in retries, monitoring, and credential management.

* **2. State Management (Watermarking):**
    * *Problem:* Fetching the full history every hour is inefficient and costly.
    * *Solution:* We implement a **Watermark Pattern**. A control table (or file) stores the `Last_Success_Timestamp`.
    * *Execution:* The pipeline reads this timestamp and passes it to the API request (e.g., `GET /issues?updated_after=2024-01-01T12:00:00`).

* **3. Pagination & Throttling:**
    * APIs rarely return all data in one call. We use a `Copy Data` activity inside a `Until` or `ForEach` loop to handle pagination (traversing `next_page_token` or offsets) until the full batch is retrieved.
    * We respect HTTP 429 (Too Many Requests) headers by configuring the connector's "Retry Interval" to back off automatically.

* **4. Security:**
    * API Keys and Bearer Tokens are stored in **Azure Key Vault**. The Fabric Workspace accesses them via **Managed Identity**, ensuring zero-trust security.

* **5. Ingestion Sink:**
    * Data is landed strictly **"As-Is"** (Raw JSON) into the `Raw` zone of OneLake. We do not attempt to clean the data during extraction to ensure we have an immutable audit trail of the source.

### Scenario 1b: Blob Storage Integration (The "Push" Pattern)
**Use Case:** Low-latency, reactive processing (e.g., a system dumps a log file, and we need to see it in the dashboard immediately).

* **1. The Trigger (Event Grid):**
    * Polling storage every minute is wasteful ("Empty Cycles"). Instead, we use an **Event-Driven Architecture**.
    * We configure an **Azure Event Grid** subscription on the Storage Account. It listens specifically for `Microsoft.Storage.BlobCreated` events.

* **2. Event Routing:**
    * When a file lands (e.g., `issue_log_2025.json`), Event Grid fires a signal to the Fabric Pipeline, passing the precise `folderPath` and `fileName` in the metadata payload.

* **3. Burst Handling (Concurrency):**
    * *Risk:* If 1,000 files arrive simultaneously, triggering 1,000 parallel pipelines could exhaust our Spark Capacity.
    * *Solution:* We configure the Fabric Pipeline **Concurrency Control** (e.g., Max 10 concurrent runs). Additional events are queued and processed as capacity becomes available, preventing a "Thundering Herd" outage.

* **4. Incremental Processing:**
    * The pipeline passes the filename parameter directly to the `unpack_data` Notebook.
    * The Notebook processes **only that specific file**, effectively creating a micro-batch stream. This is significantly faster and cheaper than scanning the entire data lake for new files.

---

## 2. Schema Drift & Data Quality Strategy

### A. Handling Mixed Timestamps
* **Challenge:** The source data arrives with inconsistent time formats that would break standard casting:
    1.  **ISO 8601 with Offset:** `2024-01-01T10:00:00+05:00`
    2.  **Zulu Time:** `2024-01-01T10:00:00Z`
    3.  **Date-Only:** `2024-01-01` (Implies midnight)
    4.  **Epoch Milliseconds:** `1714567890123`
* **Solution (Universal Normalization):**
    * I will implement a **Polymorphic Coalesce Strategy** in PySpark to normalize all inputs into a standard `TimestampType (UTC)`.
    * **Logic:**
        ```sql
        CASE 
            -- 1. Handle Epoch Milliseconds (Numeric string check)
            WHEN timestamp_col RLIKE '^[0-9]{13}$' 
                THEN timestamp_millis(CAST(timestamp_col AS LONG))
            
            -- 2. Handle Date-Only (Auto-defaults to midnight UTC)
            WHEN length(timestamp_col) = 10 AND timestamp_col RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN to_timestamp(timestamp_col, 'yyyy-MM-dd')

            -- 3. Handle ISO 8601 (Offsets & Zulu)
            -- Spark's to_timestamp() automatically parses ISO patterns and normalizes to session timezone (UTC)
            ELSE to_timestamp(timestamp_col) 
        END
        ```
    * **Outcome:** Regardless of whether the input is `1714...` or `2024...T...+05:00`, the downstream system receives a unified **UTC Timestamp**, ensuring that KPIs like "Time to Resolve" are calculated accurately without timezone skew.

### B. Schema Drift (Custom Attributes)
* **Challenge:** Upstream systems add new keys to `CustomAttributeData` without warning.
* **Solution:** **Entity-Attribute-Value (EAV) Modeling**.
* **Policy:** We do **not** use `ALTER TABLE` to add columns for every new key.
* **Implementation:** We explode the JSON map into a vertical table (`fct_Issue_Attributes`) with `AttributeKey` and `AttributeValue` columns.
    * *Result:* The pipeline never fails on new attributes; they simply appear as new rows in the EAV table, instantly available for analysis.

### C. Validation & Assertions
We enforce expectations using **Great Expectations** or PySpark assertions at the Silver boundary:
1.  **Uniqueness:** `IssueId` must be unique.
2.  **Referential Integrity:** In the Gold layer, `ModifiedBy_UserId` must exist in the `dim_Person` list (handled via our Union logic).
3.  **Mandatory Fields:** `CreatedAt` and `Title` cannot be Null.

### D. Bad Record Policy
* **Strategy:** **Quarantine & Continue** (Do not Fail-Fast).
* **Mechanism:** Use Spark's `badRecordsPath` or `PERMISSIVE` mode with a `_corrupt_record` column.
* **Workflow:**
    1.  Valid rows are written to Bronze.
    2.  Malformed JSON rows are redirected to a `_quarantine/` folder in OneLake.
    3.  An automated alert triggers a notification for engineers to inspect the quarantine folder, preventing pipeline blockage.

---

## 3. Orchestration, Scheduling & Recovery

### A. Triggering Strategy
* **Batches (API):** Scheduled Triggers (Time-Window). Good for retrospective reporting.
* **Files (Blob):** Storage Event Triggers (Event Grid). Essential for "Near Real-Time" visibility of critical risk events.

### B. Idempotency & Retries
* **Idempotency:**
    * The PySpark notebooks are designed to be **Idempotent**.
    * **Bronze:** Overwrites the partition for that specific file batch.
    * **Gold:** Uses **Delta Lake MERGE** (Upsert) logic based on `IssueId`. If the pipeline runs twice, it simply updates the existing record with the same values, creating no duplicates.
* **Retries:**
    * Fabric Pipeline Activities are configured with a **Retry Policy** (Count: 3, Interval: 30s) to handle transient network blips (e.g., OneLake throttle or API timeout).

### C. Dependency Management
* We use the **Fabric Orchestration Pipeline** to enforce order:
    1.  **Activity 1:** `Ingest_Notebook` (Unpack Data).
    2.  **Dependency:** "On Success" link.
    3.  **Activity 2:** `Mart_Notebook` (Create Marts).
    4.  **Activity 3:** `Semantic_Model_Refresh` (Power BI).
* **Failure Handling:** If Activity 1 fails, Activity 2 is skipped, and an `Outlook/Teams Notification` activity is triggered to alert the Ops team.