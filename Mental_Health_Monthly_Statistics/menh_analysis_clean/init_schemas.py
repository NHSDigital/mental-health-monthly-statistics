# Databricks notebook source
import os

current_env = os.environ.get('env')
assert current_env

db = dbutils.widgets.get("db")
assert db

params = { 'db' : db, }

print(params)

# COMMAND ----------

# DBTITLE 1,call to notebooks in schemas folder
dbutils.notebook.run('schemas/schemas_master',0, params)