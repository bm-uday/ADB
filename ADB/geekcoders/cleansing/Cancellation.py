# Databricks notebook source
# MAGIC %run /geekcoders/utilities/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/cancellation")\
    .load('/mnt/raw_datalake/Cancellation')

# COMMAND ----------

display(df)

# COMMAND ----------

#dbutils.fs.rm('/mnt/cleansed_datalake/plane',True)

# COMMAND ----------

df_base=df.selectExpr(
   "replace(Code,'\"','') as code",
   "replace(Description,'\"','') as description",
   "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
display(df_base)

# COMMAND ----------

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","dbfs/Filestore/tables/checkpointLocation/Cancellation")\
    .start("/mnt/cleansed_datalake/cancellation")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/cancellation')
df.dtypes

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/cancellation')
schema=pre_schema(df)

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/cancellation')
schema=pre_schema(df)
f_delta_cleansed_load('cancellation','/mnt/cleansed_datalake/cancellation',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.cancellation

# COMMAND ----------

f_delta_cleansed_load('cancellation','/mnt/cleansed_datalake/cancellation','cleansed_geekcoders')

# COMMAND ----------


