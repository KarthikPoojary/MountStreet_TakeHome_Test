import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr

# Initialize Spark
spark = SparkSession.builder \
    .appName("MountStreet_Unpack") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Paths
current_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(current_dir, "../data/anonymised_issue_data_no_comments.json")
output_base = os.path.join(current_dir, "../output/source_tables")

# Clean previous run
if os.path.exists(output_base):
    shutil.rmtree(output_base)

print(f"Reading data from: {json_path}")
print(f"Writing outputs to: {output_base}")

# Load Raw Data
raw_df = spark.read.option("multiline", "true").json(json_path)
df_root = raw_df.select(explode(col("issues")).alias("issue"))
df_root.cache()

print(f"Total Issues Loaded: {df_root.count()}")

# Helper to save tables
def save_table(df, name):
    output_path = os.path.join(output_base, name)
    print(f"\nSaving {name} to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)
    df.show(3, truncate=False)

# ==========================================
# 1. EXTRACT: src_Issues (Fact Grain)
# ==========================================
df_issues = df_root.select(
    col("issue.Id").alias("IssueId"),
    col("issue.Title"),
    col("issue.Type"),
    col("issue.RaisedAtTimestamp").alias("CreatedAt"),
    col("issue.DateIdentified"),
    col("issue.DateOccurred"),
    col("issue.Details"),
    col("issue.ImpactsCustomer"),
    col("issue.IsExternalIssue"),
    col("issue.issueStatus.Status").alias("SourceStatus"),
    col("issue.modifiedByUser.Id").alias("ModifiedBy_UserId"),
    col("issue.modifiedByUser.FriendlyName").alias("ModifiedBy_Name"),
    col("issue.modifiedByUser.Email").alias("ModifiedBy_Email")
)
save_table(df_issues, "src_Issues")

# ==========================================
# 2. TRANSFORM: Flatten Standard Arrays
# ==========================================
def flatten_array(df, array_col_name, fields_to_extract, table_name):
    print(f"\nProcessing: {table_name}...")
    
    # Check if column exists before processing to prevent errors
    if f"issue.{array_col_name}" not in df.columns and array_col_name not in df.select("issue.*").columns:
         print(f"Skipping {table_name}: Column not found")
         return None

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
        
    save_table(result, table_name)
    return result

# Owners
df_owners = flatten_array(df_root, "owners", 
    {"user.Id": "UserId", "user.FriendlyName": "Name", "user.Email": "Email"}, 
    "src_Owners")

# Contributors
df_contributors = flatten_array(df_root, "contributors", 
    {"user.Id": "UserId", "user.FriendlyName": "Name", "user.Email": "Email"}, 
    "src_Contributors")

# Departments
df_departments = flatten_array(df_root, "departments", 
    {"type.DepartmentTypeId": "DepartmentTypeId", "type.Name": "DepartmentName"}, 
    "src_Departments")

# Tags
df_tags = flatten_array(df_root, "tags", None, "src_Tags")


# ==========================================
# 3. TRANSFORM: Attachments
# ==========================================
print("\nProcessing: src_Attachments...")
# The schema is flat: {attachment_id, file_name, size_bytes, source}
df_attachments = df_root.select(
    col("issue.Id").alias("IssueId"),
    explode(col("issue.attachments")).alias("child")
).select(
    "IssueId",
    col("child.attachment_id").alias("AttachmentId"),
    col("child.file_name").alias("FileName"),
    col("child.size_bytes").alias("FileSize"),
    col("child.source").alias("Source")
)
save_table(df_attachments, "src_Attachments")


# ==========================================
# 4. TRANSFORM: Events
# ==========================================
print("\nProcessing: src_Events...")
df_events = df_root.select(
    col("issue.Id").alias("IssueId"),
    explode(col("issue.events")).alias("event_struct")
).select(
    "IssueId",
    col("event_struct.event").alias("EventName"),
    col("event_struct.timestamp").alias("EventTimestamp"),
    col("event_struct.by.Id").alias("TriggeredByUserId"),
    col("event_struct.by.FriendlyName").alias("TriggeredByName"),
    col("event_struct.carrier").alias("Carrier")
)
save_table(df_events, "src_Events")


# ==========================================
# 5. TRANSFORM: Custom Attributes (EAV)
# ==========================================
print("\nProcessing: src_CustomAttributes...")

# Dynamically map all fields in CustomAttributeData
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

save_table(df_custom_attributes, "src_CustomAttributes")

print("\n--- Unpacking Complete. Bronze Layer created in output/source_tables ---")
spark.stop()