# Databricks notebook source
# DBTITLE 1,Get widget variables
db_output = dbutils.widgets.get("db_output")
db_source  = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status = dbutils.widgets.get("status")
rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")


params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'status': status}

print(params)

# COMMAND ----------

# DBTITLE 1,Calculate remaining dates from current month widget value
from datetime import datetime
from dateutil.relativedelta import relativedelta

params['month_id_m1'] = int(params['month_id']) - 2
params['month_id_m2'] = int(params['month_id']) - 1

params['rp_startdate_m1'] = rp_startdate_quarterly
params['rp_startdate_m2'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-1)).strftime('%Y-%m-%d')

params['rp_enddate_m1'] = (datetime.strptime(params['rp_enddate'], '%Y-%m-%d') + relativedelta(months=-2)).strftime('%Y-%m-%d')
params['rp_enddate_m2'] = (datetime.strptime(params['rp_enddate'], '%Y-%m-%d') + relativedelta(months=-1)).strftime('%Y-%m-%d')

print(params)

# COMMAND ----------

# DBTITLE 1,Clean unformatted output table (in case there already is left-over data for this month/status in the table)
 %sql

 DELETE FROM $db_output.CCGOIS_unformatted
 WHERE REPORTING_PERIOD = '$rp_startdate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.CCGOIS_unformatted RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Aggregate CCGOIS
dbutils.notebook.run("../02_Aggregate/9.CCGOIS_3-17", 0, params)

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
 %python

 import os

 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CCGOIS_unformatted'))