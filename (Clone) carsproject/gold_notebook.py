# Databricks notebook source
# MAGIC %md
# MAGIC # Create flag parameter

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a widget
# MAGIC

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0') # parameter

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Model

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@adlscarproject.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %sql
# MAGIC select Model_ID,modelcategory from parquet.`abfss://silver@adlscarproject.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC dataframe for the above table

# COMMAND ----------

df_src=spark.sql('''select distinct(Model_ID) as modelid,modelcategory from parquet.`abfss://silver@adlscarproject.dfs.core.windows.net/carsales`''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Create surrogate key and schema for model table

# COMMAND ----------

if spark.catalog.tableExists('dim_model'):
    df_sink=spark.sql('''select 1 as dim_model_key, Model_ID,modelcategory from parquet.`abfss://silver@adlscarproject.dfs.core.windows.net/carsales` ''')  # If table exists load all the data
else:
    df_sink=spark.sql('''select 1 as dim_model_key, Model_ID,modelcategory from parquet.`abfss://silver@adlscarproject.dfs.core.windows.net/carsales` where 1=0''') #Initial load

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC filtering new records and old records

# COMMAND ----------

df_filter=df_src.join(df_sink, df_src.modelid==df_sink.Model_ID, 'left').select(df_src.modelid,df_src.modelcategory,df_sink.dim_model_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(df_filter.dim_model_key.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC df_filter_new

# COMMAND ----------

df_filter_new=df_filter.filter(df_filter.dim_model_key.isNull()).select('modelid','modelcategory')

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC creating surrogate key(dim_key)
# MAGIC

# COMMAND ----------


