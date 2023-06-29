# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
status  = dbutils.widgets.get("status")

params = {'rp_startdate_1m': str(rp_startdate_1m),'rp_startdate_12m': str(rp_startdate_12m), 'rp_startdate_qtr': str(rp_startdate_qtr), 'rp_enddate': str(rp_enddate), 'start_month_id': start_month_id, 'end_month_id': end_month_id, 'db_output': db_output, 'db_source': db_source, 'status': status}
print(params)

# COMMAND ----------

dbutils.notebook.run('MHSDS OAPS - 3a OAP01', 0, params)
dbutils.notebook.run('MHSDS OAPS - 3b OAP02', 0, params)
dbutils.notebook.run('MHSDS OAPS - 3c OAP03', 0, params)
dbutils.notebook.run('MHSDS OAPS - 3d OAP04', 0, params)