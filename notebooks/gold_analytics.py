# Databricks notebook source
# MAGIC %md
# MAGIC #Baby Names Lakehouse: Bronze to Gold with Databricks

# COMMAND ----------

"""
Project: Baby Names Lakehouse with Databricks

Layer: Gold

Description:
Creates business-ready analytical datasets from the Silver layer,
supporting reporting, trend analysis, and data exploration.
"""

# read silver table
silver_df = spark.table("workspace.default.silver_baby_names")

display(silver_df)


# COMMAND ----------

# gold table 1: Births by Year
from pyspark.sql.functions import sum

births_by_year_df = (
    silver_df
    .groupBy("year")
    .agg(
        sum("count").alias("total_births")
    )
    .orderBy("year")
)

display(births_by_year_df)

# COMMAND ----------

# save the table 
(
    births_by_year_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_births_by_year")
)

# COMMAND ----------

# gold table 2: Births by Year and Gender
from pyspark.sql.functions import sum

births_by_gender_df = (
    silver_df
    .groupBy("year", "sex")
    .agg(
        sum("count").alias("total_births")
    )
    .orderBy("year", "sex")
)

display(births_by_gender_df)

# COMMAND ----------

# save the table
(
    births_by_gender_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_births_by_gender")
)

# COMMAND ----------

# gold table 3: Top 10 Names per Year
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("year").orderBy(desc("count"))

top_names_df = (
    silver_df
    .withColumn(
        "rank",
        row_number().over(window_spec)
    )
    .filter("rank <= 10")
)

display(top_names_df)

# COMMAND ----------

# save the table 
(
    top_names_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_top_names_by_year")
)

# COMMAND ----------

# gold table 4: Most Popular Names Across All Years
from pyspark.sql.functions import sum

popular_names_df = (
    silver_df
    .groupBy("name", "sex")
    .agg(
        sum("count").alias("total_births")
    )
    .orderBy("total_births", ascending=False)
)

display(popular_names_df)

# COMMAND ----------

# save the table 
(
    popular_names_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_popular_names")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verify gold tables
# MAGIC SHOW TABLES IN workspace.default;

# COMMAND ----------

display(
    spark.table("workspace.default.gold_births_by_year")
)


# COMMAND ----------

display(
    spark.table("workspace.default.gold_births_by_gender")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.gold_top_names_by_year
# MAGIC WHERE year = 2015
# MAGIC ORDER BY rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.default.gold_popular_names
# MAGIC LIMIT 20;
# MAGIC