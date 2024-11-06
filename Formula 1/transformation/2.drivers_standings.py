# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

drivers_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("total_wins"))

# COMMAND ----------

display(drivers_standings_df.filter("race_year == 2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))

final_df = drivers_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name"
merge_delta_table(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')