# Databricks notebook source
# MAGIC %sql
# MAGIC desc history cleansed_geekcoders.airlines

# COMMAND ----------

# MAGIC %py
# MAGIC spark.sql("""desc history cleansed_geekcoders.airlines""").createOrReplaceTempView("Table_count")

# COMMAND ----------

# MAGIC %sql
# MAGIC select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where operation='WRITE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where operation='WRITE' order by version desc limit 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history cleansed_geekcoders.plane

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history cleansed_geekcoders.plane

# COMMAND ----------

def f_count_check1(database,operation_type,table_name,number_diff):
    try:
        spark.sql(f"""desc history {database}.{table_name}""").createOrReplaceTempView("Table_count")
        count_current=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where trim(lower(operation))=lower('{operation_type}'))""")
        if(count_current.first() is None):
            final_count_current=0
        else:
            final_count_current=int(count_current.first().numOutputRows)
        count_previous=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where trim(lower(operation))=lower('{operation_type}') order by version desc limit 1)""")
        if(count_previous.first() is None):
            final_count_previous=0
        else:
            final_count_previous=int(count_previous.first().numOutputRows)
        if((final_count_current-final_count_previous)>number_diff):
            #print("Difference is huge in ",table_name)
            raise Exception("Difference is huge in ",table_name)
        else:
            pass
    except Exception as err:
        print("Error occured ",str(err))

# COMMAND ----------

def f_count_check(database,operation_type,table_name,number_diff):
        spark.sql(f"""desc history {database}.{table_name}""").createOrReplaceTempView("Table_count")
        count_current=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where trim(lower(operation))=lower('{operation_type}'))""")
        if(count_current.first() is None):
            final_count_current=0
        else:
            final_count_current=int(count_current.first().numOutputRows)
        count_previous=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where trim(lower(operation))=lower('{operation_type}') order by version desc limit 1)""")
        if(count_previous.first() is None):
            final_count_previous=0
        else:
            final_count_previous=int(count_previous.first().numOutputRows)
        if((final_count_current-final_count_previous)>number_diff):
            #print("Difference is huge in ",table_name)
            raise Exception("Difference is huge in ",table_name)
        else:
            pass

# COMMAND ----------

f_count_check('cleansed_geekcoders','STREAMING UPDATE','plane',100)

# COMMAND ----------

f_count_check('cleansed_geekcoders','write','airlines',100)

# COMMAND ----------


