# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using acess keys
# MAGIC 1. Set the Spark config fs.azure.account.keys
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1dl6510_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl6510.dfs.core.windows.net",
    formula1dl6510_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl6510.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl6510.dfs.core.windows.net/circuits.csv"))