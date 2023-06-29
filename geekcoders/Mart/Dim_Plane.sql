-- Databricks notebook source
create database if not exists mart_geekcoders;

-- COMMAND ----------

use mart_geekcoders

-- COMMAND ----------

select * from cleansed_geekcoders.plane

-- COMMAND ----------

delete from cleansed_geekcoders.plane where date_part='2023-06-10'

-- COMMAND ----------

select * from cleansed_geekcoders.plane

-- COMMAND ----------

select count(tailid),count(distinct(tailid)) from cleansed_geekcoders.plane

-- COMMAND ----------

desc cleansed_geekcoders.plane

-- COMMAND ----------

create table if not exists Dim_Plane
(
  tailid string,
  type string,
  manufacturer string,
  issue_date date,
  model string,
  status string,
  aircraft_type string,
  engine_type string,
  year int
) using delta location '/mnt/mart_datalake/DIM_PLANE'


-- COMMAND ----------

insert overwrite Dim_Plane
select
tailid ,
  type ,
  manufacturer ,
  issue_date ,
  model ,
  status ,
  aircraft_type ,
  engine_type ,
  year 
  from cleansed_geekcoders.plane

-- COMMAND ----------

desc extended dim_plane

-- COMMAND ----------


