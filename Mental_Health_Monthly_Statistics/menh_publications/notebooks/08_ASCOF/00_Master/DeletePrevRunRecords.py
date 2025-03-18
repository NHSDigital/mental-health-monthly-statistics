# Databricks notebook source
db_output = dbutils.widgets.get("db_output")

# COMMAND ----------

# DBTITLE 1,Clean unformatted output table (in case there already is left-over data for this month/status in the table)
 %sql

 DELETE FROM $db_output.ascof_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.ascof_unformatted RETAIN 8 HOURS

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
 %python

 import os

 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='ascof_unformatted'))