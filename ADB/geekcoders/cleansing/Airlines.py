# Databricks notebook source
# MAGIC %run /geekcoders/utilities/Utilities

# COMMAND ----------

from pyspark.sql.functions import explode

df=spark.read.json("/mnt/raw_datalake/airlines/")
df1=df.select(explode("response"),"Date_Part")
df_final=df1.select("col.*","Date_Part")


# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airlines")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/airlines').limit(1)
schema=pre_schema(df)
f_delta_cleansed_load('airlines','/mnt/cleansed_datalake/airlines',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airlines

# COMMAND ----------

f_delta_cleansed_load('airlines','/mnt/cleansed_datalake/airlines','cleansed_geekcoders')

# COMMAND ----------


