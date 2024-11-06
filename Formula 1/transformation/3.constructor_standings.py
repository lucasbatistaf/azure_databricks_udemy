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

constructors_standings_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(constructors_standings_df, 'race_year')

# COMMAND ----------

display(constructors_standings_df)

# COMMAND ----------

constructors_grouped_df = constructors_standings_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("total_wins")) 

# COMMAND ----------

display(constructors_grouped_df)

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))
final_df = constructors_grouped_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_table(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')