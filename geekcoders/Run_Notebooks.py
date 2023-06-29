# Databricks notebook source
dbutils.widgets.text("Layer_Name","")
Layer_Name=dbutils.widgets.get("Layer_Name")

# COMMAND ----------


Notebook_Path_Json={
  "Raw":["/geekcoders/configurations/sourcing/Raw_Plane"],
  "Cleansed":[
    "/geekcoders/cleansing/Airlines",
    "/geekcoders/cleansing/Airport",
    "/geekcoders/cleansing/Cancellation",
    "/geekcoders/cleansing/Flight",
    "/geekcoders/cleansing/Plane",
    "/geekcoders/cleansing/Unique_Carriers"
  ],
  "Data_Quality_Cleansed":["/geekcoders/Data_Quality_Checks/Data_Quality_Check"]
}

# COMMAND ----------

print(Notebook_Path_Json[Layer_Name])

# COMMAND ----------

for n in Notebook_Path_Json[Layer_Name]:
    dbutils.notebook.run(n,0)

# COMMAND ----------


