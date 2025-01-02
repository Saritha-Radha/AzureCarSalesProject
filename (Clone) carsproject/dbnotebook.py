# Databricks notebook source
# MAGIC %md
# MAGIC # create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC # create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold
