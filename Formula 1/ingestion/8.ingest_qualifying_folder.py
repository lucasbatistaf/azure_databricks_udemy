# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest qualifying folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API
# MAGIC 1. reading multiple files in a folder

# COMMAND ----------

df_schema = StructType(fields=[
  StructField("qualifyId", IntegerType(), False),
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("constructorId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("q1", StringType(), True),
  StructField("q2", StringType(), True),
  StructField("q3", StringType(), True),
])

# COMMAND ----------

df = spark.read \
  .schema(df_schema) \
  .option("multiLine", True) \
  .json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. qualifyId to qualify_id
# MAGIC 1. raceId to race_id
# MAGIC 1. driverId to driver_id
# MAGIC 1. constructorId to constructor_id
# MAGIC 1. Add ingestion_date (current_timestamp)

# COMMAND ----------

renamed_df = df.withColumnRenamed("qualifyId", "qualify_id") \
               .withColumnRenamed("raceId", "race_id") \
               .withColumnRenamed("driverId", "driver_id") \
               .withColumnRenamed("constructorId", "constructor_id") \
               .withColumn("data_source", lit(v_data_source)) \
               .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(renamed_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to parquet file to processed container
# MAGIC

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_table(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/qualifying

# COMMAND ----------

dbutils.notebook.exit("Success")