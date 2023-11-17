# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
db_source  = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")
status = dbutils.widgets.get("status")

params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'status': status}


# COMMAND ----------

# DBTITLE 1,Product-specific quarterly start date for AWT
# This doesn't work in the same way in menh_publications because the structure of the code run is different
# instead all instances of $rp_startdate are changed to $rp_startdate_quarterly in the code

# params["rp_startdate"] = rp_startdate_quarterly

# print(params)

# COMMAND ----------

# DBTITLE 1,Clean unformatted output table (in case there already is left-over data for this month/status in the table)
 %sql
 
 DELETE FROM $db_output.AWT_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.AWT_unformatted RETAIN 8 HOURS

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
 %python
 
 import os
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_unformatted'))