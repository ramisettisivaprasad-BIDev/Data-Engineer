"""
Data Validation Pipeline using PySpark + Delta Lake
=====================================================
- Valid records   → saved as Delta table (clean zone)
- Invalid records → saved to audit/quarantine Delta table with error reasons
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime

# ─────────────────────────────────────────────
# 1. SPARK SESSION (Delta Lake enabled)
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("DataValidationPipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ─────────────────────────────────────────────
# 2. SAMPLE DATA  (replace with your DataFrame)
# ─────────────────────────────────────────────
schema = StructType([
    StructField("customer_id",  StringType(),  True),
    StructField("name",         StringType(),  True),
    StructField("email",        StringType(),  True),
    StructField("age",          IntegerType(), True),
    StructField("salary",       DoubleType(),  True),
    StructField("country",      StringType(),  True),
])

raw_data = [
    ("C001", "Alice",   "alice@example.com",  30, 55000.0, "US"),   # ✅ valid
    ("C002", "Bob",     "bob_at_example.com",  25, 48000.0, "UK"),   # ❌ bad email
    ("C003", None,      "carol@example.com",   28, 62000.0, "US"),   # ❌ null name
    ("C004", "Dave",    "dave@example.com",   -5, 71000.0,  "CA"),   # ❌ negative age
    ("C005", "Eve",     "eve@example.com",    35, -1000.0,  "AU"),   # ❌ negative salary
    ("C006", "Frank",   "frank@example.com",  40, 90000.0,  "XX"),   # ❌ invalid country
    ("C007", "Grace",   "grace@example.com",  29, 53000.0,  "IN"),   # ✅ valid
    (None,   "Heidi",   "heidi@example.com",  33, 67000.0,  "US"),   # ❌ null customer_id
    ("C009", "Ivan",    "ivan@example.com",   22, 39000.0,  "UK"),   # ✅ valid
    ("C010", "Judy",    "",                   45, 82000.0,  "US"),   # ❌ empty email
]

df = spark.createDataFrame(raw_data, schema=schema)

# ─────────────────────────────────────────────
# 3. DEFINE VALIDATION RULES
#    Each rule returns a boolean column (True = rule VIOLATED)
# ─────────────────────────────────────────────
VALID_COUNTRIES = ["US", "UK", "CA", "AU", "IN", "DE", "FR"]

validation_rules = {
    "missing_customer_id" : F.col("customer_id").isNull(),
    "missing_name"        : F.col("name").isNull() | (F.trim(F.col("name")) == ""),
    "invalid_email"       : ~F.col("email").rlike(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"),
    "invalid_age"         : (F.col("age").isNull()) | (F.col("age") < 0) | (F.col("age") > 120),
    "invalid_salary"      : (F.col("salary").isNull()) | (F.col("salary") < 0),
    "invalid_country"     : ~F.col("country").isin(VALID_COUNTRIES),
}

# ─────────────────────────────────────────────
# 4. APPLY RULES → tag each row with a list of errors
# ─────────────────────────────────────────────
df_checked = df

# Add one boolean flag column per rule
for rule_name, rule_expr in validation_rules.items():
    df_checked = df_checked.withColumn(rule_name, rule_expr)

# Build a single array column that collects all violated rule names
error_array_expr = F.array(
    *[F.when(F.col(rule), F.lit(rule)) for rule in validation_rules]
)

df_checked = df_checked.withColumn("_error_list_raw", error_array_expr)

# Filter out nulls from the array → only the violated rules remain
df_checked = df_checked.withColumn(
    "error_reasons",
    F.expr("filter(_error_list_raw, x -> x is not null)")
)

# A row is valid when the error array is empty
df_checked = df_checked.withColumn(
    "is_valid",
    F.size(F.col("error_reasons")) == 0
)

# ─────────────────────────────────────────────
# 5. SPLIT INTO VALID / INVALID
# ─────────────────────────────────────────────
base_cols = [f.name for f in schema.fields]   # original columns only

df_valid = (
    df_checked
    .filter(F.col("is_valid"))
    .select(base_cols)                         # keep only business columns
)

df_invalid = (
    df_checked
    .filter(~F.col("is_valid"))
    .select(
        *base_cols,
        F.col("error_reasons"),
        F.lit(datetime.now().isoformat()).cast(TimestampType()).alias("audit_timestamp"),
        F.lit("data_validation_pipeline").alias("pipeline_name"),
    )
)

# ─────────────────────────────────────────────
# 6. SAVE VALID RECORDS → DELTA TABLE
# ─────────────────────────────────────────────
VALID_PATH   = "/delta/clean/customers"
INVALID_PATH = "/delta/audit/customers_quarantine"

(
    df_valid.write
    .format("delta")
    .mode("append")              # use "overwrite" for full refresh
    .option("mergeSchema", "true")
    .save(VALID_PATH)
)
print(f"✅ Valid records written to Delta: {VALID_PATH}")

# ─────────────────────────────────────────────
# 7. SAVE INVALID RECORDS → AUDIT / QUARANTINE DELTA TABLE
# ─────────────────────────────────────────────
(
    df_invalid.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(INVALID_PATH)
)
print(f"⚠️  Invalid records written to audit Delta: {INVALID_PATH}")

# ─────────────────────────────────────────────
# 8. VALIDATION SUMMARY REPORT
# ─────────────────────────────────────────────
total_count   = df.count()
valid_count   = df_valid.count()
invalid_count = df_invalid.count()

print("\n" + "="*50)
print("         VALIDATION SUMMARY")
print("="*50)
print(f"  Total records   : {total_count}")
print(f"  Valid records   : {valid_count}")
print(f"  Invalid records : {invalid_count}")
print("="*50)

print("\n📋 Invalid records with reasons:")
df_invalid.select("customer_id", "name", "error_reasons").show(truncate=False)

# Error frequency breakdown
print("📊 Error frequency by rule:")
(
    df_invalid
    .select(F.explode(F.col("error_reasons")).alias("error_rule"))
    .groupBy("error_rule")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

# ─────────────────────────────────────────────
# 9. (OPTIONAL) REGISTER AS SQL TABLES
# ─────────────────────────────────────────────
spark.sql(f"CREATE TABLE IF NOT EXISTS clean.customers    USING DELTA LOCATION '{VALID_PATH}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS audit.customers_quarantine USING DELTA LOCATION '{INVALID_PATH}'")

print("🗂️  Tables registered in the metastore.")
print("   Query valid data  : SELECT * FROM clean.customers")
print("   Query audit data  : SELECT * FROM audit.customers_quarantine")
