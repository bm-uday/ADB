# Databricks notebook source
# MAGIC %run /geekcoders/utilities/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/Flight")\
    .load('/mnt/raw_datalake/flight')

# COMMAND ----------

display(df)

# COMMAND ----------



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

from pyspark.sql.functions import concat_ws
df_base=df.selectExpr(
"to_date(concat_ws('-',Year,Month,DayoOfMonth),'yyyy-MM-dd') as date",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm') as deptime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm') as CRSDepTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm') as ArrTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm') as CRSArrTime",
"UniqueCarrier",
"cast(FlightNum as int) as FlightNum",
"cast(TailNum as int) as TailNum",
"cast(ActualElapsedTime as int) as ActualElapsedTime",
"cast(CRSElapsedTime as int) as CRSElapsedTime",
"cast(Airtime as int) as AirTime",
"cast(ArrDelay as int) as ArrDelay",
"cast(DepDelay as int) as DepDelay",
"Origin",
"dest",
"cast(distance as int) as Distance",
"cast(taxiIn as int) as TaxiIn",
"cast(TaxiOut as int) as TaxiOut",
"Cancelled",
"CancellationCode",
"cast(Diverted as int) as castDiverted",
"cast(CarrierDelay as int) as CarrierDelay",
"cast(WeatherDelay as int) as WeatherDelay",
"cast(NASDelay as int) as NASDelay",
"cast(SecurityDelay as int) as SecurityDelay",
"cast(LateAircraftDelay as int) as LateAircraftDelay",
"to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","dbfs/Filestore/tables/checkpointLocation/Flight")\
    .start("/mnt/cleansed_datalake/flight")

# COMMAND ----------


