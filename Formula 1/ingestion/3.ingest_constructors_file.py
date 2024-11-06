# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

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

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

df_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

df = spark.read \
    .schema(df_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

dropped_df = df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

renamed_df = dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                       .withColumnRenamed("constructorRef", "constructor_ref") \
                       .withColumn("data_source", lit(v_data_source)) \
                       .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(renamed_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl6510/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")