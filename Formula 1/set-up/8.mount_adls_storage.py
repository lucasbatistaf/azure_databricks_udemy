# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secret from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl6510-client-secret')

    # Set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Check if mount already exists in the DBFS folder, if exists unmount it
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    #display mounts
    display(dbutils.fs.mounts())



# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container

# COMMAND ----------

mount_adls('formula1dl6510', 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Processed Container

# COMMAND ----------

mount_adls('formula1dl6510', 'processed')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Presentation Container

# COMMAND ----------

mount_adls('formula1dl6510', 'presentation')

# COMMAND ----------

mount_adls('formula1dl6510', 'demo')