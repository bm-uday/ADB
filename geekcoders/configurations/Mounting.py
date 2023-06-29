# Databricks notebook source
dbutils.fs.mount( source = 'wasbs://source@udayblobsa.blob.core.windows.net', 
                 mount_point= '/mnt/source_blob', extra_configs ={'fs.azure.sas.source.udayblobsa.blob.core.windows.net':dbutils.secrets.get(scope="geekcoders-secret",key="sas")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/source_blob

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://raw@udaydatalakedevsink.blob.core.windows.net', 
                 mount_point= '/mnt/raw_datalake/', extra_configs ={'fs.azure.sas.raw.udaydatalakedevsink.blob.core.windows.net':dbutils.secrets.get(scope="geekcoders-secret",key="sas1sink")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/raw_datalake/
# MAGIC

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://cleansed@udaydatalakedevsink.blob.core.windows.net', 
                 mount_point= '/mnt/cleansed_datalake/', extra_configs ={'fs.azure.sas.cleansed.udaydatalakedevsink.blob.core.windows.net':dbutils.secrets.get(scope="geekcoders-secret",key="sas1sink")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/cleansed_datalake/

# COMMAND ----------

dbutils.fs.unmount('/mnt/cleansed_datalake/')

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://mart@udaydatalakedevsink.blob.core.windows.net', 
                 mount_point= '/mnt/mart_datalake/', extra_configs ={'fs.azure.sas.mart.udaydatalakedevsink.blob.core.windows.net':dbutils.secrets.get(scope="geekcoders-secret",key="sas1sink")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/mart_datalake/

# COMMAND ----------


