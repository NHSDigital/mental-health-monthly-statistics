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
 
 DELETE FROM $db_output.Main_monthly_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.Main_monthly_unformatted RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Aggregate Main monthly
# 1. National
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/1.National/Inpatient measures", 0, params)
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/1.National/MHA measures", 0, params)
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/1.National/Outpatient-Other measures", 0, params)

# 2.CCG
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/2.CCG/Inpatient measures", 0, params)
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/2.CCG/MHA measures", 0, params)
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/2.CCG/Outpatient-Other measures", 0, params)

# 3.Provider
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/3.Provider/Inpatient measures", 0, params)
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/3.Provider/MHA measures", 0, params)
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/3.Provider/Outpatient-Other measures", 0, params)

# 4.LA-CASSR
dbutils.notebook.run("../02_Aggregate/1.Main_monthly_agg/4.LA-CASSR/Outpatient-Other measures", 0, params)

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
 %python
 
 import os
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Main_monthly_unformatted'))