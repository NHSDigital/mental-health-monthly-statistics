# Databricks notebook source
# DBTITLE 1,Get widget variables
db_output = dbutils.widgets.get("db_output")
db_source  = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status = dbutils.widgets.get("status")

params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'status': status}

print(params)

# COMMAND ----------

# DBTITLE 1,Clean unformatted output table (in case there already is left-over data for this month/status in the table)
 %sql
 
 DELETE FROM $db_output.CAP_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.CAP_unformatted RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Aggregate CAP
dbutils.notebook.run("../02_Aggregate/4.CAP_agg", 0, params)

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
 %python
 
 import os
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CAP_unformatted'))