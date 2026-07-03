# Databricks notebook source
# MAGIC %md
# MAGIC #Baby names

# COMMAND ----------

# to Check what storage is available
display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --to explore Volumes
# MAGIC SHOW CATALOGS

# COMMAND ----------

# same as above but in pyspark 
spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.baby_names;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/baby_names")

# COMMAND ----------


# Databricks notebook source

# Bronze Ingestion - Baby Names Lakehouse Project

from pyspark.sql.functions import col

# Define source path
source_path = "/Volumes/workspace/default/baby_names/*.txt"

# Read raw baby names text files
bronze_df = (
    spark.read
         .option("header", "false")
         .option("inferSchema", "true")
         .csv(source_path)
         .withColumn("source_file", col("_metadata.file_path"))
)

# Display sample data
display(bronze_df)



# COMMAND ----------

# Rename raw columns
bronze_df = bronze_df.select(
    col("_c0").alias("name"),
    col("_c1").alias("sex"),
    col("_c2").alias("count"),
    col("source_file")
)

# Display sample data
display(bronze_df)



# COMMAND ----------

# Check schema
bronze_df.printSchema()

# Save as Bronze Delta table
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_baby_names")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- verify the bronze table
# MAGIC SELECT *
# MAGIC FROM workspace.default.bronze_baby_names
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- row count
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM workspace.default.bronze_baby_names;
