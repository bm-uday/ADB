# Databricks notebook source
# MAGIC %run /geekcoders/utilities/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/airport")\
    .load('/mnt/raw_datalake/Airport')

# COMMAND ----------

display(df)

# COMMAND ----------

#dbutils.fs.rm('/mnt/cleansed_datalake/plane',True)

# COMMAND ----------

df_base=df.selectExpr(
    "Code as code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_part,'yyyy-MM-dd') as Date_Part"
)
display(df_base)

# COMMAND ----------

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","dbfs/Filestore/tables/checkpointLocation/Airport")\
    .start("/mnt/cleansed_datalake/airport")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/airport')
df.dtypes

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/airport')
schema=pre_schema(df)

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/airport')
schema=pre_schema(df)
f_delta_cleansed_load('airport','/mnt/cleansed_datalake/airport',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airport

# COMMAND ----------

f_delta_cleansed_load('airport','/mnt/cleansed_datalake/airport','cleansed_geekcoders')

# COMMAND ----------


