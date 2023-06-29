-- Databricks notebook source
use mart_geekcoders

-- COMMAND ----------

select * from cleansed_geekcoders.flight

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

-- COMMAND ----------

select to_date(concat_ws('-',Year,Month,DayOfMonth),'yyyy-MM-dd') as date,airdelay,depdelay,origin,cancelled,cancellationcode,uniquecarrier,flightnum,tailnum,deptime
from cleansed_geekcoders.flight
where flightnum=2891

-- COMMAND ----------

select * from cleansed_geekcoders.flight

-- COMMAND ----------

desc cleansed_geekcoders.flight

-- COMMAND ----------

create table if not exists Reporting_Flight
(
  date date,
  arrdelay int,
  depdelay int,
  origin string,
  cancelled int,
  cancellationcode string,
  uniquecarrier string,
  flightnum int,
  tailnum string,
  deptime string
) using delta location 'mnt/mart_datalake/Reporting_Flight'


-- COMMAND ----------

insert overwrite Reporting_Flight
select to_date(concat_ws('-',Year,Month,DayOfMonth),'yyyy-MM-dd') as date,airdelay,depdelay,origin,cancelled,cancellationcode,uniquecarrier,flightnum,tailnum,deptime
from cleansed_geekcoders.flight

-- COMMAND ----------


