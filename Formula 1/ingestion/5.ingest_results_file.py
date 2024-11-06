# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

df_schema = StructType(fields=[
  StructField("resultId", IntegerType(), False),
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("constructorId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("grid", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("positionText", StringType(), True),
  StructField("positionOrder", IntegerType(), True),
  StructField("points", FloatType(), True),
  StructField("laps", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("fastestLapSpeed", StringType(), True),
  StructField("statusId", IntegerType(), True),
])

# COMMAND ----------

df = spark.read.schema(df_schema) \
                       .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. resultId to resul_id
# MAGIC 1. raceId to race_id
# MAGIC 1. driverId to driver_id
# MAGIC 1. constructorId to constructor_id
# MAGIC 1. positionText to position_text
# MAGIC 1. positionOrder to position_order
# MAGIC 1. fastestLap to fastest_lap
# MAGIC 1. fastestLapTime to fastest_lap_time
# MAGIC 1. fastestLapSpeed to fastest_lap_speed
# MAGIC 1. Add ingestion_date (current_timestamp)

# COMMAND ----------

renamed_df = df.withColumnRenamed("resultId", "result_id") \
               .withColumnRenamed("raceId", "race_id") \
               .withColumnRenamed("driverId", "driver_id") \
               .withColumnRenamed("constructorId", "constructor_id") \
               .withColumnRenamed("positionText", "position_text") \
               .withColumnRenamed("positionOrder", "position_order") \
               .withColumnRenamed("fastestLap", "fastest_lap") \
               .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
               .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
               .withColumn("data_source", lit(v_data_source)) \
               .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

renamed_df = add_ingestion_date(renamed_df)  

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns
# MAGIC 1. statusId

# COMMAND ----------

final_df = renamed_df.drop(col("statusId"))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe (delete duplicates)

# COMMAND ----------

results_deduped_df = final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file to processed container
# MAGIC
# MAGIC 1. Partition by race_id

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------


# final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2
# MAGIC
# MAGIC 1. Last Column must be the partitionBy Column
# MAGIC 1. Set partitionBy parameter to Dynamic

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_table(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/results

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC