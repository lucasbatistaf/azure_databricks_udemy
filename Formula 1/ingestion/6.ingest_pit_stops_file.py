# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pit_stops.json file

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
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader
# MAGIC 1. multiline json must be specified

# COMMAND ----------

df_schema = StructType(fields=[
  StructField("raceId", IntegerType(), False),
  StructField("driverId", IntegerType(), True),
  StructField("stop", StringType(), True),
  StructField("lap", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("duration", StringType(), True),
  StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

df = spark.read \
  .schema(df_schema) \
  .option("multiLine", True) \
  .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId to driver_id
# MAGIC 1. raceId to race_id
# MAGIC 1. Add ingestion_date (current_timestamp)

# COMMAND ----------

renamed_df = df.withColumnRenamed("driverId", "driver_id") \
               .withColumnRenamed("raceId", "race_id") \
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

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop"
merge_delta_table(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/pit_stops

# COMMAND ----------

dbutils.notebook.exit("Success")