# Databricks notebook source
def pre_schema(location):
    try:
        df=spark.read.format('delta').load(f'{location}').limit(1)
        schema=""
        for i in df.dtypes:
            schema=schema+i[0]+" "+i[1]+","
        return schema[0:-1]
    except Exception as err:
        print('Error occured ', str(err))

# COMMAND ----------

# MAGIC %py
# MAGIC def f_delta_cleansed_load(table_name,location,database):
# MAGIC     try:
# MAGIC
# MAGIC         schema=pre_schema(f'{location}')
# MAGIC         spark.sql(f"""DROP TABLE IF EXISTS {database}.{table_name}""")
# MAGIC         spark.sql(f"""
# MAGIC                   create table if not exists {database}.{table_name}
# MAGIC                   ({schema})
# MAGIC                   using delta
# MAGIC                   location '{location}'
# MAGIC                   """)
# MAGIC     except Exception as err:
# MAGIC         print('Error occured ',str(err))

# COMMAND ----------


