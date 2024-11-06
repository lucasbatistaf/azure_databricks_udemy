# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

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

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import col, lit, concat, to_timestamp

# COMMAND ----------

df_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

df = spark.read \
.option("header", True) \
.schema(df_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only the required columns and race_timestamp
# MAGIC
# MAGIC first example is the easiest one to apply, but you cant do operations with the selected columns
# MAGIC
# MAGIC second example is the best option, for when you need to change names or apply other operations

# COMMAND ----------

selected_df = df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

renamed_df = selected_df.withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("year", "race_year") \
                        .withColumnRenamed("circuitId", "circuit_id") \
                        .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

final_df = add_ingestion_date(renamed_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write the data to the database as parquet

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/races

# COMMAND ----------

dbutils.notebook.exit("Success")