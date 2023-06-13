# Databricks notebook source
# MAGIC %run /geekcoders/utilities/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/Unique_carriers")\
    .load('/mnt/raw_datalake/UNIQUE_CARRIERS')

# COMMAND ----------

display(df)

# COMMAND ----------

#dbutils.fs.rm('/mnt/cleansed_datalake/plane',True)

# COMMAND ----------

df_base=df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
display(df_base)

# COMMAND ----------

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","dbfs/Filestore/tables/checkpointLocation/Unique_carriers")\
    .start("/mnt/cleansed_datalake/unique_carriers")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/unique_carriers')
df.dtypes

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/unique_carriers')
schema=pre_schema(df)

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/unique_carriers')
schema=pre_schema(df)
f_delta_cleansed_load('unique_carriers','/mnt/cleansed_datalake/unique_carriers',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.unique_carriers

# COMMAND ----------

f_delta_cleansed_load('unique_carriers','/mnt/cleansed_datalake/unique_carriers','cleansed_geekcoders')

# COMMAND ----------


