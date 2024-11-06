# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Aplication / Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake.
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl6510.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl6510.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl6510.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl6510.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl6510.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl6510.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl6510.dfs.core.windows.net/circuits.csv"))