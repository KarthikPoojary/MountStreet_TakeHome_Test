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

print(f"Total Source Records: {df_root.count()}")

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

fct_issues_sql = """
WITH Normalized_Issues AS (
    SELECT 
        IssueId,
        -- CLEANING FIX: Replace fancy dashes
        regexp_replace(Title, '[—–]', '-') as Title,
        Type,
        IsExternalIssue,
        ImpactsCustomer,
        Details,
        ModifiedBy_UserId,
        CreatedAt,
        DateOccurred,
        DateIdentified,
        
        -- ROBUST DATE PARSING LOGIC:
        CASE 
            WHEN CreatedAt RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(CreatedAt AS LONG))
            ELSE to_timestamp(CreatedAt) 
        END as Norm_CreatedAt,
        
        CASE 
            WHEN DateOccurred RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(DateOccurred AS LONG))
            ELSE to_timestamp(DateOccurred) 
        END as Norm_DateOccurred,

        CASE 
            WHEN DateIdentified RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(DateIdentified AS LONG))
            ELSE to_timestamp(DateIdentified) 
        END as Norm_DateIdentified
        
    FROM src_Issues
)
SELECT 
    row_number() OVER (ORDER BY i.IssueId) as IssueKey,
    i.IssueId as NaturalKey,
    
    -- Date Keys
    COALESCE(CAST(date_format(i.Norm_CreatedAt, 'yyyyMMdd') AS INT), 19000101) as CreatedDateKey,
    COALESCE(CAST(date_format(i.Norm_DateOccurred, 'yyyyMMdd') AS INT), 19000101) as OccurredDateKey,
    COALESCE(CAST(date_format(i.Norm_DateIdentified, 'yyyyMMdd') AS INT), 19000101) as IdentifiedDateKey,
    
    -- Person FK
    p.PersonKey as ModifiedByPersonKey,
    
    -- Metrics
    i.Title,
    i.Type,
    i.IsExternalIssue,
    i.ImpactsCustomer,
    datediff(current_date(), i.Norm_CreatedAt) as IssueAgeDays

FROM Normalized_Issues i
-- FIX: Changed 'p.SourceSystemId' to 'p.SourceUserId' to match dim_Person definition
LEFT JOIN dim_Person p ON i.ModifiedBy_UserId = p.SourceUserId
"""

fct_issues = spark.sql(fct_issues_sql)
fct_issues.show(5, truncate=False)
fct_issues.createOrReplaceTempView("fct_Issues")


# 3.3 BRIDGE: ISSUE_PEOPLE
print("\nCreating Mart: bridge_Issue_People...")
bridge_people_sql = """
SELECT * FROM (
    SELECT 
        f.IssueKey,
        p.PersonKey,
        'Owner' as RoleType
    FROM src_Owners s
    JOIN fct_Issues f ON s.IssueId = f.NaturalKey
    JOIN dim_Person p ON s.UserId = p.SourceUserId
    
    UNION ALL
    
    SELECT 
        f.IssueKey,
        p.PersonKey,
        'Contributor' as RoleType
    FROM src_Contributors s
    JOIN fct_Issues f ON s.IssueId = f.NaturalKey
    JOIN dim_Person p ON s.UserId = p.SourceUserId
) 
ORDER BY IssueKey, RoleType 
"""
bridge_people = spark.sql(bridge_people_sql)
bridge_people.show(10) # Show 10 rows to verify

# QUICK DEBUG: Print counts to prove they exist
print("\n--- DEBUG: Role Counts ---")
bridge_people.groupBy("RoleType").count().show()

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


# =================================================================
# 4. EXPORT DATA FOR POWER BI
# =================================================================
print("\nExporting Marts to CSV for Power BI...")

# Define output path
output_path = os.path.join(current_dir, "../powerBI/marts")

def write_to_csv(df, folder_name):
    target = f"{output_path}/{folder_name}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(target)
    print(f"Saved: {folder_name}")

# Write the tables we created
write_to_csv(dim_person, "dim_Person")
write_to_csv(fct_issues, "fct_Issues")
write_to_csv(bridge_people, "bridge_Issue_People")
write_to_csv(bridge_dept, "bridge_Issue_Departments")

print(f"\nSUCCESS: Data exported to {output_path}")

spark.stop()