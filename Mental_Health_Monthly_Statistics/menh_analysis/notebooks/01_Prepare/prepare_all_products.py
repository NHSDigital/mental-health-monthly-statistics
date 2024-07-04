# Databricks notebook source
# DBTITLE 1,Get widget variables
from datetime import datetime, date
db_output = dbutils.widgets.get("db_output")
db_source  = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")
status = dbutils.widgets.get("status")
reference_data = dbutils.widgets.get("reference_data")

params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'status': status, 'reference_data': reference_data, 'rp_startdate_quarterly': rp_startdate_quarterly}

rp_enddate = datetime.strptime(rp_enddate, '%Y-%m-%d')

fy_start = date(rp_enddate.year if rp_enddate.month > 3 else (rp_enddate.year - 1), 4, 1)
fy_end = date(fy_start.year + 1, 3, 31)

params['Financial_Yr_Start'] = str(fy_start)
params['Financial_Yr_End'] = str(fy_end)

print(params)


# COMMAND ----------

# DBTITLE 1,Generic preparation
dbutils.notebook.run("../01_Prepare/0.5_VersionChanges", 0, params)
dbutils.notebook.run("../01_Prepare/0.Generic_prep", 0, params)
dbutils.notebook.run("../01_Prepare/0.Insert_lookup_data", 0, params)

# COMMAND ----------

# DBTITLE 1,Product-specific preparation (monthly start date)
dbutils.notebook.run("../01_Prepare/1.Main_monthly_prep", 0, params)
dbutils.notebook.run("../01_Prepare/3.CYP_2nd_contact_prep", 0, params)
dbutils.notebook.run("../01_Prepare/4.CAP_prep", 0, params)
dbutils.notebook.run("../01_Prepare/5.CYP_monthly_prep", 0, params)


# COMMAND ----------

# added by  (taken from 8.FYFV_prep)

from datetime import datetime
from dateutil.relativedelta import relativedelta

params['month_id_1'] = int(params['month_id']) - 2
params['month_id_2'] = int(params['month_id']) - 1

params['rp_startdate_m1'] = rp_startdate_quarterly
params['rp_startdate_m2'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-1)).strftime('%Y-%m-%d')

params['rp_enddate_m1'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-1,days=-1)).strftime('%Y-%m-%d')
params['rp_enddate_m2'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(days=-1)).strftime('%Y-%m-%d')

print(params)

# COMMAND ----------

# DBTITLE 1,Product-specific preparation (quarterly start date)
#params["rp_startdate_quarterly"] = rp_startdate_quarterly

# FYFV_prep needs rp_startdate_quarterly (re-introduced above) - although only because of the way it's coded - not REALLY needed
# If you assign the value of rp_startdate_quarterly to the rp_startdate parameter then this REALLY messes up FYFV_prep - changes the bed days...
# I am therefore changing rp_startdate_quarterly to be rp_startdate_quarterly and keeping rp_startdate as rp_startdate.
# rp_stardate is set to rp_startdate_quarterly in AWT_agg for display and aggregation purposes - the prep code uses Month_id -1 or -2 when needed

# params["rp_startdate"] = rp_startdate_quarterly

print(params)

# EIP measures moved to menh_publications
# dbutils.notebook.run("../01_Prepare/2.AWT_prep", 0, params)
dbutils.notebook.run("../01_Prepare/8.FYFV_prep", 0, params)

# COMMAND ----------

