# Databricks notebook source
# MAGIC %md
# MAGIC # data reading

# COMMAND ----------

df=spark.read.format("parquet")\
    .option("inferSchema", "true")\
        .load("abfss://bronze@adlscarproject.dfs.core.windows.net/rawdata")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=df.withColumn("modelcategory",split(col('Model_ID'), '-') [0])

# COMMAND ----------

# MAGIC %md
# MAGIC typecast int to string

# COMMAND ----------

df.withColumn("Units_Sold",col('Units_Sold').cast(StringType()))

# COMMAND ----------

df=df.withColumn('Revenueperunit',col('Revenue')/col('Units_Sold'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Ad-hoc

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Totalunits'))\
    .sort('Year','Totalunits',ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # data writing to adls

# COMMAND ----------

df.write.format("parquet").mode("append").option("path", "abfss://silver@adlscarproject.dfs.core.windows.net/carsales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC reading the parquet file from silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@adlscarproject.dfs.core.windows.net/carsales`
