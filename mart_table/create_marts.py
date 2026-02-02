import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================================
# 0. SETUP & CONFIGURATION
# ==========================================
spark = SparkSession.builder \
    .appName("MountStreet_Gold_Marts") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Define Paths
current_dir = os.path.dirname(os.path.abspath(__file__))
source_path = os.path.join(current_dir, "../output/source_tables")
output_path = os.path.join(current_dir, "../output/marts")

# Reset Output Directory
if os.path.exists(output_path):
    shutil.rmtree(output_path)

print(f"Reading Bronze Data from: {source_path}")
print(f"Writing Gold Marts to:    {output_path}")

# ==========================================
# 1. LOAD BRONZE TABLES (Dynamic Loading)
# ==========================================
# We load all parquet files from the source and register them as SQL Views
tables = [
    "src_Issues", "src_Owners", "src_Contributors", 
    "src_Departments", "src_Tags", "src_Events", "src_CustomAttributes"
]

for table in tables:
    path = os.path.join(source_path, table)
    if os.path.exists(path):
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)
        print(f"Loaded View: {table}")
    else:
        print(f"WARNING: Source table {table} not found at {path}")

# ==========================================
# 2. CREATE DIMENSIONS (SQL)
# ==========================================

# 2.1 DIM_PERSON
# Logic: Consolidate all users (Owners, Contributors, Modifiers, Event Actors) into one profile.
print("\nBuilding: dim_Person...")
dim_person_sql = """
WITH All_Users AS (
    SELECT UserId, Name, Email, 'Owner' as RoleSource FROM src_Owners
    UNION ALL
    SELECT UserId, Name, Email, 'Contributor' as RoleSource FROM src_Contributors
    UNION ALL
    SELECT ModifiedBy_UserId, ModifiedBy_Name, ModifiedBy_Email, 'Modifier' FROM src_Issues WHERE ModifiedBy_UserId IS NOT NULL
    UNION ALL
    SELECT TriggeredByUserId, TriggeredByName, NULL, 'EventActor' FROM src_Events WHERE TriggeredByUserId IS NOT NULL
)
SELECT 
    row_number() OVER (ORDER BY UserId) as PersonKey,
    UserId as SourceUserId,
    COALESCE(MAX(Name), 'Unknown') as Name,
    COALESCE(MAX(Email), 'Unknown') as Email,
    -- Comma-separated list of roles they have appeared in
    concat_ws(',', collect_set(RoleSource)) as KnownRoles
FROM All_Users
WHERE UserId IS NOT NULL
GROUP BY UserId
"""
dim_person = spark.sql(dim_person_sql)
dim_person.createOrReplaceTempView("dim_Person")


# ==========================================
# 3. CREATE FACT TABLES (SQL)
# ==========================================

# 3.1 BRIDGE TABLES (Many-to-Many Resolution)
print("Building: bridge_Issue_People...")
spark.sql("""
    SELECT DISTINCT
        i.IssueId as IssueKey, -- Using Natural Key for simplicity in this demo
        p.PersonKey,
        'Owner' as RoleType
    FROM src_Owners o
    JOIN dim_Person p ON o.UserId = p.SourceUserId
    JOIN src_Issues i ON o.IssueId = i.IssueId
    
    UNION ALL
    
    SELECT DISTINCT
        i.IssueId,
        p.PersonKey,
        'Contributor' as RoleType
    FROM src_Contributors c
    JOIN dim_Person p ON c.UserId = p.SourceUserId
    JOIN src_Issues i ON c.IssueId = i.IssueId
""").createOrReplaceTempView("bridge_Issue_People")

print("Building: bridge_Issue_Departments...")
spark.sql("""
    SELECT DISTINCT
        IssueId as IssueKey,
        DepartmentTypeId,
        DepartmentName
    FROM src_Departments
""").createOrReplaceTempView("bridge_Issue_Departments")

# 3.2 FACT ISSUES (The Master Table)
print("Building: fct_Issues...")
fct_issues_sql = """
WITH 
-- 1. Determine "Today" based on the data (Relative Snapshot)
Ref_Date AS (
    SELECT MAX(
        CASE 
            WHEN CreatedAt RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(CreatedAt AS LONG))
            -- Handle Date-Only (yyyy-MM-dd) explicitly
            WHEN length(CreatedAt) = 10 THEN to_timestamp(CreatedAt, 'yyyy-MM-dd')
            ELSE to_timestamp(CreatedAt) 
        END
    ) as SnapshotDate
    FROM src_Issues
),
Raw_Events AS (
    SELECT 
        IssueId, 
        EventName, 
        CASE 
            WHEN EventTimestamp RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(EventTimestamp AS LONG)) 
            WHEN length(EventTimestamp) = 10 THEN to_timestamp(EventTimestamp, 'yyyy-MM-dd')
            ELSE to_timestamp(EventTimestamp) 
        END as TrueTimestamp
    FROM src_Events
),
Event_Derived_Info AS (
    SELECT 
        IssueId,
        MAX(CASE WHEN EventName = 'closed' THEN TrueTimestamp END) as ResolutionTimestamp
    FROM Raw_Events
    GROUP BY IssueId
),
Owner_Stats AS (
    SELECT IssueId, COUNT(*) as OwnerCount 
    FROM src_Owners 
    GROUP BY IssueId
),
Normalized_Issues AS (
    SELECT 
        i.IssueId,
        regexp_replace(i.Title, '[—–]', '-') as Title,
        i.Type,
        i.IsExternalIssue,
        i.ImpactsCustomer,
        i.Details,
        i.ModifiedBy_UserId,
        
        -- DATES: Robust normalization for Mixed Formats
        CASE 
            WHEN i.CreatedAt RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(i.CreatedAt AS LONG)) 
            WHEN length(i.CreatedAt) = 10 THEN to_timestamp(i.CreatedAt, 'yyyy-MM-dd')
            ELSE to_timestamp(i.CreatedAt) 
        END as Norm_CreatedAt,
        
        CASE 
            WHEN i.DateOccurred RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(i.DateOccurred AS LONG)) 
            WHEN length(i.DateOccurred) = 10 THEN to_timestamp(i.DateOccurred, 'yyyy-MM-dd')
            ELSE to_timestamp(i.DateOccurred) 
        END as Norm_DateOccurred,
        
        CASE 
            WHEN i.DateIdentified RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(i.DateIdentified AS LONG)) 
            WHEN length(i.DateIdentified) = 10 THEN to_timestamp(i.DateIdentified, 'yyyy-MM-dd')
            ELSE to_timestamp(i.DateIdentified) 
        END as Norm_DateIdentified,
        
        -- STATUS (Priority: Explicit Source > Event Derived)
        COALESCE(i.SourceStatus, 'raised') as CurrentStatus,

        e.ResolutionTimestamp as Norm_ResolutionDate,
        COALESCE(os.OwnerCount, 0) as OwnerCount,
        
        r.SnapshotDate
        
    FROM src_Issues i
    CROSS JOIN Ref_Date r
    LEFT JOIN Event_Derived_Info e ON i.IssueId = e.IssueId
    LEFT JOIN Owner_Stats os ON i.IssueId = os.IssueId
)
SELECT 
    row_number() OVER (ORDER BY i.IssueId) as IssueKey,
    i.IssueId as NaturalKey,
    p.PersonKey as ModifiedByPersonKey,
    
    CAST(i.Norm_CreatedAt AS DATE) as CreatedDate,
    CAST(i.Norm_DateOccurred AS DATE) as OccurredDate,
    CAST(i.Norm_DateIdentified AS DATE) as IdentifiedDate,
    CAST(i.Norm_ResolutionDate AS DATE) as ResolutionDate,
    
    COALESCE(CAST(date_format(i.Norm_CreatedAt, 'yyyyMMdd') AS INT), 19000101) as CreatedDateKey,

    i.Title,
    i.Type,
    i.CurrentStatus, 
    i.IsExternalIssue,
    i.ImpactsCustomer,
    i.OwnerCount,
    
    -- KPI CALCULATIONS (Sanitized)
    GREATEST(0, datediff(i.Norm_DateIdentified, i.Norm_DateOccurred)) as DaysToIdentify,
    GREATEST(0, datediff(i.Norm_ResolutionDate, i.Norm_DateIdentified)) as DaysToResolve,
    GREATEST(0, datediff(i.SnapshotDate, i.Norm_DateIdentified)) as IssueAgeDays,
    
    CASE WHEN i.CurrentStatus = 'closed' THEN false ELSE true END as IsOpen,
    CASE WHEN i.OwnerCount = 0 THEN true ELSE false END as IsOrphaned,
    
    -- DYNAMIC BUCKETS
    CASE 
        WHEN i.CurrentStatus = 'closed' THEN 'Closed'
        WHEN datediff(i.SnapshotDate, i.Norm_DateIdentified) <= 7 THEN 'New (<7 Days)'
        WHEN datediff(i.SnapshotDate, i.Norm_DateIdentified) <= 30 THEN 'Stale (8-30 Days)'
        ELSE 'Critical (>30 Days)'
    END as AgeBucket

FROM Normalized_Issues i
LEFT JOIN dim_Person p ON i.ModifiedBy_UserId = p.SourceUserId
"""
fct_issues = spark.sql(fct_issues_sql)
fct_issues.createOrReplaceTempView("fct_Issues")

# 3.3 ATTRIBUTES FACT (Schema Drift Handling)
print("Building: fct_Issue_Attributes...")
spark.sql("""
    SELECT 
        IssueId as IssueKey,
        AttributeKey,
        AttributeValue
    FROM src_CustomAttributes
""").createOrReplaceTempView("fct_Issue_Attributes")


# ==========================================
# 4. EXPORT TO CSV (Power BI Ready)
# ==========================================
def export_mart(df, name):
    target = os.path.join(output_path, name)
    print(f"Exporting: {name} -> {target}")
    # Coalesce(1) ensures a single CSV file output per table (simpler for Power BI import)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(target)

export_mart(dim_person, "dim_Person")
export_mart(fct_issues, "fct_Issues")
export_mart(spark.table("bridge_Issue_People"), "bridge_Issue_People")
export_mart(spark.table("bridge_Issue_Departments"), "bridge_Issue_Departments")
export_mart(spark.table("fct_Issue_Attributes"), "fct_Issue_Attributes")

print("\n--- Mart Generation Complete ---")
spark.stop()    