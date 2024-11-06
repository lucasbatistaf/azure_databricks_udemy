# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------

df_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

df = spark.read \
.option("header", True) \
.schema(df_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - select only the required columns
# MAGIC
# MAGIC first example is the easiest one to apply, but you cant do operations with the selected columns
# MAGIC
# MAGIC second example is the best option, for when you need to change names or apply other operations

# COMMAND ----------

selected_df = df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

selected_df = df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

renamed_df = selected_df.withColumnRenamed("circuitId", "circuit_id") \
                        .withColumnRenamed("circuitRef", "circuit_ref") \
                        .withColumnRenamed("lat", "latitude") \
                        .withColumnRenamed("lng", "longitude") \
                        .withColumnRenamed("alt", "altitude") \
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion date to the database

# COMMAND ----------

final_df = add_ingestion_date(renamed_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the data to the database as parquet

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/circuits

# COMMAND ----------

dbutils.notebook.exit("Success")