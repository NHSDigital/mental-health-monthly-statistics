# Databricks notebook source
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
status=dbutils.widgets.get("status")
print(status)
assert status
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source

# COMMAND ----------

# DBTITLE 1,Clean unformatted output table (in case there already is left-over data for this month/status in the table)
 %sql

 DELETE FROM $db_output.72hours_unrounded_stg
 WHERE UniqMonthID = '$month_id'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source';
 VACUUM $db_output.72hours_unrounded_stg RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
 %python

 import os

 spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='72hours_unrounded_stg'))
