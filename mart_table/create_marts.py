import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, first, coalesce, lit, current_date, datediff, date_format

# 1. SETUP
spark = SparkSession.builder.appName("MountStreet_Marts").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") 

current_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(current_dir, "../data/anonymised_issue_data_no_comments.json")

# 2. LOAD & REGISTER SOURCE TABLES 
raw_df = spark.read.option("multiline", "true").json(json_path)
df_root = raw_df.select(explode(col("issues")).alias("issue"))

# --- CORRECTION HERE ---
# We map 'RaisedAtTimestamp' to 'CreatedAt' so downstream SQL works
df_issues = df_root.select(
    col("issue.Id").alias("IssueId"),
    col("issue.Title"),
    col("issue.Type"),
    col("issue.RaisedAtTimestamp").alias("CreatedAt"), # <--- FIXED
    col("issue.RaisedAtTimestamp"),
    col("issue.DateIdentified"),
    col("issue.DateOccurred"),
    col("issue.Details"),
    col("issue.ImpactsCustomer"),
    col("issue.IsExternalIssue"),
    col("issue.modifiedByUser.Id").alias("ModifiedBy_UserId"),
    col("issue.modifiedByUser.FriendlyName").alias("ModifiedBy_Name"),
    col("issue.modifiedByUser.Email").alias("ModifiedBy_Email")
)
df_issues.createOrReplaceTempView("src_Issues")

# B. src_Owners
df_owners = df_root.select(col("issue.Id").alias("IssueId"), explode(col("issue.owners")).alias("child")) \
    .select("IssueId", col("child.user.Id").alias("UserId"), col("child.user.FriendlyName").alias("Name"), col("child.user.Email").alias("Email"))
df_owners.createOrReplaceTempView("src_Owners")

# C. src_Contributors
df_contributors = df_root.select(col("issue.Id").alias("IssueId"), explode(col("issue.contributors")).alias("child")) \
    .select("IssueId", col("child.user.Id").alias("UserId"), col("child.user.FriendlyName").alias("Name"), col("child.user.Email").alias("Email"))
df_contributors.createOrReplaceTempView("src_Contributors")

# D. src_Departments (Handle nested type)
df_departments = df_root.select(col("issue.Id").alias("IssueId"), explode(col("issue.departments")).alias("child")) \
    .select("IssueId", col("child.type.DepartmentTypeId").alias("DepartmentTypeId"), col("child.type.Name").alias("DepartmentName"))
df_departments.createOrReplaceTempView("src_Departments")

# E. src_Tags
df_tags = df_root.select(col("issue.Id").alias("IssueId"), explode(col("issue.tags")).alias("child")) \
    .select("IssueId", col("child").alias("Value"))
df_tags.createOrReplaceTempView("src_Tags")

# F. src_CustomAttributes (Normalized)
# We cast to STRING to avoid the "Map Types" error we fixed earlier
custom_data_schema = df_root.select("issue.CustomAttributeData.*").schema
map_parts = [f"'{f.name}', CAST(issue.CustomAttributeData.`{f.name}` AS STRING)" for f in custom_data_schema]
map_expr = f"map({','.join(map_parts)})"
df_custom = df_root.select(col("issue.Id").alias("IssueId"), explode(expr(map_expr)).alias("AttributeKey", "AttributeValue")).filter("AttributeValue IS NOT NULL")
df_custom.createOrReplaceTempView("src_CustomAttributes")


print("--- Source Tables Registered in SQL Memory ---")

# =================================================================
# 3. RUN MART SQL (The Gold Phase)
# =================================================================

# 3.1 DIM_PERSON
print("\nCreating Mart: dim_Person...")
dim_person_sql = """
SELECT 
    row_number() OVER (ORDER BY Email) as PersonKey,
    UserId as SourceUserId,
    Name,
    Email,
    first(Source) as PrimaryRoleSource
FROM (
    SELECT UserId, Name, Email, 'Owner' as Source FROM src_Owners
    UNION ALL
    SELECT UserId, Name, Email, 'Contributor' as Source FROM src_Contributors
    UNION ALL
    SELECT ModifiedBy_UserId as UserId, ModifiedBy_Name as Name, ModifiedBy_Email as Email, 'Modifier' as Source 
    FROM src_Issues WHERE ModifiedBy_UserId IS NOT NULL
) all_users
GROUP BY UserId, Name, Email
"""
dim_person = spark.sql(dim_person_sql)
dim_person.show(5, truncate=False)
dim_person.createOrReplaceTempView("dim_Person")


# 3.2 FCT_ISSUES
print("\nCreating Mart: fct_Issues...")

# ROBUST DATE PARSING LOGIC:
# The 'CreatedAt' (RaisedAtTimestamp) column contains mixed formats:
# 1. ISO strings ("2024-06-02T...")
# 2. Epoch strings ("1717580119124")
# We use a CASE WHEN or COALESCE strategy to handle both.

fct_issues_sql = """
WITH Normalized_Issues AS (
    SELECT 
        IssueId,
        Title,
        Type,
        IsExternalIssue,
        ModifiedBy_UserId,
        
        -- Normalization Logic for CreatedAt (RaisedAtTimestamp)
        CASE 
            -- If it looks like a big number (Epoch MS), cast to Long then Timestamp
            WHEN CreatedAt RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(CreatedAt AS LONG))
            -- Otherwise, let Spark try standard parsing
            ELSE to_timestamp(CreatedAt) 
        END as Norm_CreatedAt,
        
        -- Normalization Logic for DateOccurred
        CASE 
            WHEN DateOccurred RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(DateOccurred AS LONG))
            ELSE to_timestamp(DateOccurred) 
        END as Norm_DateOccurred,
        
        -- Normalization Logic for DateIdentified
        CASE 
            WHEN DateIdentified RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(DateIdentified AS LONG))
            ELSE to_timestamp(DateIdentified) 
        END as Norm_DateIdentified
        
    FROM src_Issues
)
SELECT 
    row_number() OVER (ORDER BY IssueId) as IssueKey,
    IssueId as NaturalKey,
    
    -- Date Keys (Now using the Normalized columns)
    COALESCE(CAST(date_format(Norm_CreatedAt, 'yyyyMMdd') AS INT), 19000101) as CreatedDateKey,
    COALESCE(CAST(date_format(Norm_DateOccurred, 'yyyyMMdd') AS INT), 19000101) as OccurredDateKey,
    
    -- Person FK
    p.PersonKey as ModifiedByPersonKey,
    
    -- Metrics
    Title,
    Type,
    IsExternalIssue,
    datediff(current_date(), Norm_CreatedAt) as IssueAgeDays

FROM Normalized_Issues i
LEFT JOIN dim_Person p ON i.ModifiedBy_UserId = p.SourceUserId
"""

fct_issues = spark.sql(fct_issues_sql)
fct_issues.show(5, truncate=False)
fct_issues.createOrReplaceTempView("fct_Issues")


# 3.3 BRIDGE: ISSUE_PEOPLE
print("\nCreating Mart: bridge_Issue_People...")
bridge_people_sql = """
SELECT 
    f.IssueKey,
    p.PersonKey,
    'Owner' as RoleType
FROM src_Owners s
JOIN fct_Issues f ON s.IssueId = f.NaturalKey
JOIN dim_Person p ON s.UserId = p.SourceUserId
"""
bridge_people = spark.sql(bridge_people_sql)
bridge_people.show(5)

# 3.4 BRIDGE: ISSUE_DEPARTMENTS
print("\nCreating Mart: bridge_Issue_Departments...")
bridge_dept_sql = """
SELECT 
    f.IssueKey,
    s.DepartmentTypeId,
    s.DepartmentName
FROM src_Departments s
JOIN fct_Issues f ON s.IssueId = f.NaturalKey
"""
bridge_dept = spark.sql(bridge_dept_sql)
bridge_dept.show(5)

print("\n--- Mart Creation Successful ---")
spark.stop()