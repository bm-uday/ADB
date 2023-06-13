# Databricks notebook source
# MAGIC %run /geekcoders/utilities/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/Flight")\
    .load('/mnt/raw_datalake/flight')

# COMMAND ----------

display(df)

# COMMAND ----------

#dbutils.fs.rm('/mnt/cleansed_datalake/plane',True)

# COMMAND ----------

df_base=df.selectExpr(
   "Year as year","Month as month","DayofMonth as dayofmonth","DayofWeek as dayofweek","DepTime as deptime","CRSDepTime as crsdeptime","ArrTime as arrtime","CRSArrTime as crsarrtime","UniqueCarrier as uniquecarrier","FlightNum as flightnum","TailNum as tailnum","ActualElapsedTime as actualelapsedtime","CRSElapsedTime as crselapsedtime","AirTime as airtime","ArrDelay as airdelay","DepDelay as depdelay","Origin as origin","Dest as dest","Distance as distance","TaxiIn as taxiln","TaxiOut as taxiout","Cancelled as cancelled","CancellationCode as cancellationcode","Diverted as diverted","CarrierDelay as carrierdelay","WeatherDelay as weatherdelay","NASDelay as nasdelay","SecurityDelay as securitydelay","LateAircraftDelay as lateaircraftdelay",
    "to_date(Datepart,'yyyy-MM-dd') as Date_Part")
display(df_base)

# COMMAND ----------

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","dbfs/Filestore/tables/checkpointLocation/Flight")\
    .start("/mnt/cleansed_datalake/flight")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/flight')
df.dtypes

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/flight')
schema=pre_schema(df)

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/flight')
schema=pre_schema(df)
f_delta_cleansed_load('flight','/mnt/cleansed_datalake/flight',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.flight

# COMMAND ----------

f_delta_cleansed_load('flight','/mnt/cleansed_datalake/flight','cleansed_geekcoders')

# COMMAND ----------


