import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr

spark = SparkSession.builder \
    .appName("MountStreet_Unpack") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(current_dir, "../data/anonymised_issue_data_no_comments.json")

print(f"Reading data from: {json_path}")

raw_df = spark.read.option("multiline", "true").json(json_path)

df_root = raw_df.select(explode(col("issues")).alias("issue"))
df_root.cache()

print(f"Total Issues Loaded: {df_root.count()}")


# ==========================================
# 1. EXTRACT: src_Issues (Fact Grain)
# ==========================================

print("\nProcessing: src_Issues...")

df_issues = df_root.select(
    col("issue.Id").alias("IssueId"),
    col("issue.Title"),
    col("issue.Type"),
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

df_issues.show(3)

# ==========================================
# 2. TRANSFORM: Flatten Standard Arrays
# ==========================================

def flatten_array(df, array_col_name, fields_to_extract, table_name):
    print(f"\nProcessing: {table_name}...")
    
    exploded = df.select(
        col("issue.Id").alias("IssueId"),
        explode(col(f"issue.{array_col_name}")).alias("child")
    )
    
    if fields_to_extract:
        select_exprs = [col("IssueId")]
        for field_path, alias in fields_to_extract.items():
            select_exprs.append(col(f"child.{field_path}").alias(alias))
        result = exploded.select(*select_exprs)
    else:
        result = exploded.select(col("IssueId"), col("child").alias("Value"))
        
    result.show(3, truncate=False)
    return result

df_owners = flatten_array(df_root, "owners", 
    {"user.Id": "UserId", "user.FriendlyName": "Name", "user.Email": "Email"}, "src_Owners")

df_departments = flatten_array(df_root, "departments", 
    {"type.DepartmentTypeId": "DepartmentTypeId", "type.Name": "DepartmentName"}, "src_Departments")

df_contributors = flatten_array(df_root, "contributors", 
    {"user.Id": "UserId", "user.FriendlyName": "Name", "user.Email": "Email"}, "src_Contributors")

df_tags = flatten_array(df_root, "tags", None, "src_Tags")

df_attachments = flatten_array(df_root, "attachments", 
    {"file_name": "FileName", "source": "Url"}, "src_Attachments")

# ==========================================
# 3. TRANSFORM: Complex Array (Events)
# ==========================================

print("\nProcessing: src_Events...")

df_events = df_root.select(
    col("issue.Id").alias("IssueId"),
    explode(col("issue.events")).alias("event")
).select(
    col("IssueId"),
    col("event.event").alias("EventType"),
    col("event.timestamp").alias("Timestamp"),
    col("event.by.Id").alias("Actor_UserId"),
    col("event.by.FriendlyName").alias("Actor_Name"),
    col("event.by.Email").alias("Actor_Email")
)

df_events.show(3, truncate=False)

print("\nProcessing: src_CustomAttributes...")

# ==========================================
# 4. TRANSFORM: Normalize Custom Attributes (EAV)
# ==========================================

custom_data_schema = df_root.select("issue.CustomAttributeData.*").schema

map_expr_parts = []
for field in custom_data_schema:
    field_name = field.name
    map_expr_parts.append(f"'{field_name}'")
    map_expr_parts.append(f"CAST(issue.CustomAttributeData.`{field_name}` AS STRING)")

map_expr_string = f"map({','.join(map_expr_parts)})"

df_custom_attributes = df_root.select(
    col("issue.Id").alias("IssueId"),
    explode(expr(map_expr_string)).alias("AttributeKey", "AttributeValue")
).filter(col("AttributeValue").isNotNull()) 

df_custom_attributes.show(5, truncate=False)

print("\n--- Unpacking Complete ---")

spark.stop()