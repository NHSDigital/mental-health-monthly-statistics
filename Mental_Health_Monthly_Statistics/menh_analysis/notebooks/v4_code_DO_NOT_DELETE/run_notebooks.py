# Databricks notebook source
 %md
 
 # this notebook is probably redundant in its current placement

# COMMAND ----------

# DBTITLE 1,This is the original cell for parameter creation 
import os

current_env = os.environ.get('env')
assert current_env

dbutils.widgets.text("db","","Target Database")
db = dbutils.widgets.get("db")
assert db

dbutils.widgets.text("db_source","", "Source database")
db_source = dbutils.widgets.get("db_source")
assert db_source

dbutils.widgets.text("reference_data","","Source Ref Database")
reference_data = dbutils.widgets.get("reference_data")

dbutils.widgets.text("month_id","", "UniqMonthID")
month_id = dbutils.widgets.get("month_id")

dbutils.widgets.text("rp_enddate","", "rp_enddate")
rp_enddate = dbutils.widgets.get("rp_enddate")

dbutils.widgets.text("rp_startdate","", "rp_startdate")
rp_startdate = dbutils.widgets.get("rp_startdate")

dbutils.widgets.text("status","", "Status")
status = dbutils.widgets.get("status")

params = {'rp_enddate' : str(rp_enddate), 'rp_startdate' : str(rp_startdate), 'month_id' : str(month_id), 'db_source' : str(db_source), 'db' : str(db), 'reference_data': str(reference_data), 'status': str(status)}

print(params)

# COMMAND ----------

dbutils.notebook.run('notebooks/run_notebooks_master', 0, params)