# Automation & Fabric Pipeline Architecture

This document outlines the production architecture for deploying the Mount Street Data Engineering solution within **Microsoft Fabric**. It addresses the specific requirements regarding API vs. Blob ingestion, Data Quality, and Orchestration.

---

## 1. Architecture & Approach

### Scenario 1a: Data via API (Pull Pattern)
* **Objective:** Periodically fetch issue logs from an external REST API.
* **Fabric Tool:** **Data Factory Pipelines** (Copy Activity).
* **Workflow:**
    1.  **Trigger:** Schedule-based (e.g., Hourly or Daily at 01:00 UTC).
    2.  **Activity:** `Copy Data` activity using the REST Connector.
    3.  **Pagination:** Implement a loop (Until/ForEach) to handle pagination (cursor or offset-based) to retrieve the full dataset.
    4.  **Security:** Store API Keys/Tokens in **Azure Key Vault**, accessed via Fabric Managed Identity.
    5.  **Sink:** Land raw JSON response into OneLake `Raw` zone (Bronze).

### Scenario 1b: Data in Blob Storage (Push/Event Pattern)
* **Objective:** Process files immediately as they are uploaded to a Storage Account.
* **Fabric Tool:** **Event-Driven Pipelines** (Storage Events).
* **Workflow:**
    1.  **Trigger:** Azure Event Grid listens for `Microsoft.Storage.BlobCreated`.
    2.  **Action:** The event triggers a Fabric Pipeline execution, passing the `@triggerBody().fileName` as a parameter.
    3.  **Processing:** The pipeline calls the `unpack_data` Notebook, passing the specific file path to process only the new data (Incremental Load).
    4.  **Sink:** Write processed Parquet to OneLake `Bronze` zone.

---

## 2. Schema Drift & Data Quality Strategy

### A. Handling Mixed Timestamps
* **Challenge:** Source contains ISO 8601 (`2024-01-01T...`) and Epoch Milliseconds (`1714...`).
* **Solution (Implemented in PySpark):**
    * I utilize a **Conditional Parsing Logic** within the Silver transformation.
    * **Logic:**
        ```python
        CASE
            WHEN timestamp_col RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(timestamp_col AS LONG))
            ELSE to_timestamp(timestamp_col)
        END
        ```
    * **Standardization:** All timestamps are cast to **UTC** immediately upon ingestion to ensure consistent duration calculations (MTTI/MTTR) downstream.

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