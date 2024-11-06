# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

df_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

df = spark.read \
    .schema(df_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. driveId to driver_id
# MAGIC 1. driverRef to driver_ref
# MAGIC 1. Add ingestion_date
# MAGIC 1. Add name with concat of forename and surname

# COMMAND ----------

renamed_df = df.withColumnRenamed("driverId", "driver_id") \
               .withColumnRenamed("driverRef", "driver_ref") \
               .withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname"))) \
               .withColumn("data_source", lit(v_data_source)) \
               .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(renamed_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns
# MAGIC 1. name.forename
# MAGIC 1. name.surname (these two were overwriten by the column "name" created by concat)
# MAGIC 1. url

# COMMAND ----------

final_df = final_df.drop(col("url"))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file to processed container

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/drivers

# COMMAND ----------

dbutils.notebook.exit("Success")