# Databricks notebook source
# MAGIC %md
# MAGIC #Baby Names Lakehouse: Bronze to Gold with Databricks
# MAGIC

# COMMAND ----------


"""
Project: Baby Names Lakehouse with Databricks

Layer: Silver

Description:
This notebook reads raw baby names data from the Bronze Delta table,
cleans and standardizes the dataset, extracts the year from source filenames,
enforces schema rules, performs data quality checks, and writes the curated
data to a Silver Delta table.
"""
# importing required functions
from pyspark.sql.functions import (
    col,
    regexp_extract,
    trim,
    upper,
    current_timestamp
)
from pyspark.sql.types import IntegerType, StringType


# COMMAND ----------

# read from the bronze layer
bronze_df = spark.table("workspace.default.bronze_baby_names")

display(bronze_df)

# COMMAND ----------

# clean, rename, standardize, and extract year
silver_df = (
    bronze_df
    .select(
        trim(col("name")).cast(StringType()).alias("name"),  # trims spaces from names
        upper(trim(col("sex"))).cast(StringType()).alias("sex"), # standardizes sex values to uppercase
        col("count").cast(IntegerType()).alias("count"), # casts count as integer
        col("source_file").cast(StringType()).alias("source_file")
    )
    # extracts year from filenames like yob2007.txt.
    .withColumn(
        "year",
        regexp_extract(col("source_file"), r"yob(\d{4})\.txt", 1).cast(IntegerType()) 
    )
    # adds an ingestion timestamp
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    # reorders columns into a clean Silver schema
    .select(
        "year",
        "name",
        "sex",
        "count",
        "source_file",
        "ingestion_timestamp"
    )
)

display(silver_df)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# quality checks for null values
from pyspark.sql.functions import count, when

null_check_df = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in silver_df.columns
])

display(null_check_df)

# COMMAND ----------

# quality checks for gender values
display(
    silver_df
    .groupBy("sex")
    .count()
)

# COMMAND ----------

# keep only valid records
silver_valid_df = silver_df.filter(
    col("year").isNotNull() &
    col("name").isNotNull() &
    col("sex").isin("M", "F") &
    col("count").isNotNull() &
    (col("count") > 0)
)

display(silver_valid_df)

# COMMAND ----------

# COMMAND ----------

# Data quality: row-count validation before writing to Silver
bronze_count = bronze_df.count()
silver_total_count = silver_df.count()
silver_valid_count = silver_valid_df.count()
dropped_count = silver_total_count - silver_valid_count
dropped_pct = (dropped_count / silver_total_count * 100) if silver_total_count > 0 else 0

print(f"Bronze row count:              {bronze_count}")
print(f"Silver row count (pre-filter): {silver_total_count}")
print(f"Silver row count (valid):      {silver_valid_count}")
print(f"Rows dropped by validation:    {dropped_count} ({dropped_pct:.2f}%)")

# Fail the write if validation drops an unexpectedly high share of rows —
# catches silent data issues (e.g. a source file suddenly missing a column)
VALIDATION_THRESHOLD_PCT = 2.0
if dropped_pct > VALIDATION_THRESHOLD_PCT:
    raise ValueError(
        f"Data quality check failed: {dropped_pct:.2f}% of rows dropped during "
        f"Silver validation, exceeding the {VALIDATION_THRESHOLD_PCT}% threshold. "
        f"Investigate before writing to the Silver table."
    )

# COMMAND ----------

# save as silver delta table
(
    silver_valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.silver_baby_names")
)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- verify silver table
# MAGIC SELECT *
# MAGIC FROM workspace.default.silver_baby_names
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     sex,
# MAGIC     COUNT(*) AS record_count,
# MAGIC     SUM(count) AS total_births
# MAGIC FROM workspace.default.silver_baby_names
# MAGIC GROUP BY year, sex
# MAGIC ORDER BY year, sex;