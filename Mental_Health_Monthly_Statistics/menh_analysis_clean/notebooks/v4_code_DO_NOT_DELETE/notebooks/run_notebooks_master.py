# Databricks notebook source
 %md
 
 # This notebook (and all those below it) needs to be reviewed for compatability with 'Performance OR Final' vs hardcoded 'Final'

# COMMAND ----------

# DBTITLE 1,Function used for Date Conversions (Do Not Remove)
import datetime
import math
from dateutil.relativedelta import relativedelta

def convert_date(inputdate:str):
    year,month,day = inputdate.split("-")
    inputdate = datetime.date(int(year),int(month),1)
    rp_enddate = inputdate + relativedelta(day=1, months=+1, days=-1)
    rp_startdate = inputdate + relativedelta(day=1)
    historic_date = datetime.date(1900, 4, 1)
    time_diff = relativedelta(rp_startdate, historic_date)
    uniquemonthid = time_diff.years * 12 + time_diff.months + 1
    rp_startdate_quarterly = rp_startdate + relativedelta(months=-2)
    
    return str(rp_startdate), str(rp_enddate), str(uniquemonthid), str(rp_startdate_quarterly)

# COMMAND ----------

# DBTITLE 1,Get Parameters
from datetime import datetime
from dateutil.relativedelta import relativedelta

db_source  = dbutils.widgets.get("db_source")
db_output = dbutils.widgets.get("db_output")
month_id = dbutils.widgets.get("month_id")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
$reference_data= dbutils.widgets.get("$reference_data")

print("Provided parameters: db_source: ",db_source," db_output: ", db_output, " month_id: ",month_id," re_startdate: ", rp_startdate," re_enddate: ", rp_enddate," status: ", status, "$reference_data: ", $reference_data)

# COMMAND ----------

# DBTITLE 1,Check if Custom or Not
# check if any input string parameters are empty then is_custom_run == FALSE else TRUE
is_custom_run = (len(month_id)>0 and len(rp_enddate)>0 and len(rp_startdate)>0 and len(status)>0 and len(db_output)>0 and len(db_source)>0)
print("Custom run: ", is_custom_run)

# COMMAND ----------

# DBTITLE 1,Execute Routines (for Custom or Non-Custom Runs)
# define parameters lists
params_provisional = {}
params_final = {}
final_names = ['Final','Performance']

# if is_custom_run is true initialise the params as such
if is_custom_run == True:
  params_custom = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'status': status, '$reference_data' : $reference_data}
  params_provisional = params_custom if status == "Provisional" else {}
  params_final = params_custom if status in final_names else {}
  
# if is_custom_run is FALSE initialise the params according to the status
elif is_custom_run == False:
  
 # Pull provisional (last month) from MHS000Header
  month_id_provisional = spark.sql("SELECT MAX(UniqMonthID) AS UniqMonthID FROM {db_source}.MHS000Header WHERE FileType = 'Primary'".format(db_source=db_source)).collect()[0]['UniqMonthID']
  start_date_provisional = spark.sql("SELECT DISTINCT ReportingPeriodStartDate FROM {db_source}.MHS000Header WHERE UniqMonthID = {month_id_provisional}".format(db_source=db_source,month_id_provisional=month_id_provisional)).collect()[0]['ReportingPeriodStartDate']
  end_date_provisional = spark.sql("SELECT DISTINCT ReportingPeriodEndDate FROM {db_source}.MHS000Header WHERE UniqMonthID = {month_id_provisional}".format(db_source=db_source,month_id_provisional=month_id_provisional)).collect()[0]['ReportingPeriodEndDate']

  # Pull final (second-last month)  from MHS000Header
  month_id = spark.sql("SELECT MAX(UniqMonthID) - 1 AS UniqMonthID FROM {db_source}.MHS000Header WHERE FileType = 'Primary'".format(db_source=db_source)).collect()[0]['UniqMonthID']
  start_date_final = spark.sql("SELECT DISTINCT ReportingPeriodStartDate FROM {db_source}.MHS000Header WHERE UniqMonthID = {month_id_final}".format(db_source=db_source,month_id_final=month_id)).collect()[0]['ReportingPeriodStartDate']
  end_date_final = spark.sql("SELECT DISTINCT ReportingPeriodEndDate FROM {db_source}.MHS000Header WHERE UniqMonthID = {month_id_final}".format(db_source=db_source,month_id_final=month_id)).collect()[0]['ReportingPeriodEndDate']

  # Provisional parameters
  params_provisional = {'rp_enddate' : str(end_date_provisional), 'rp_startdate' : str(start_date_provisional), 'month_id' : month_id_provisional, 'status' : "Provisional", 'db_output' : db_output, 'db_source' : db_source, '$reference_data' : $reference_data}
    
  # Final parameters
  params_final = {'rp_enddate' : str(end_date_final), 'rp_startdate' : str(start_date_final), 'month_id' : month_id, 'status' : "Final", 'db_output' : db_output, 'db_source' : db_source, '$reference_data' : $reference_data}

# check if selected month is a quarter 
if int(month_id) % 3 == 0:
  is_quarter = True
else:
  is_quarter = False
  
print('Is quarter:', is_quarter)

if params_provisional:
  params_provisional['rp_startdate_quarterly'] = (datetime.strptime(params_provisional['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-2)).strftime('%Y-%m-%d')
  assert datetime.strptime(params_provisional['rp_enddate'], '%Y-%m-%d')
  assert params_provisional['status'] in ('Performance', 'Provisional')
  
if params_final:
  params_final['rp_startdate_quarterly'] = (datetime.strptime(params_final['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-2)).strftime('%Y-%m-%d')
  assert datetime.strptime(params_final['rp_enddate'], '%Y-%m-%d')
  assert params_final['status'] in ('Final', 'Performance')
  
print('Provisional params:', params_provisional)
print('Final params:', params_final)

# COMMAND ----------

# DBTITLE 1,Provisional month prep
if params_provisional:
  dbutils.notebook.run("01_Prepare/prepare_all_products", 0, params_provisional)

# COMMAND ----------

# DBTITLE 1,Final month prep
if params_final:
  dbutils.notebook.run("01_Prepare/prepare_all_products", 0, params_final)

# COMMAND ----------

# DBTITLE 1,Provisional products
if params_provisional:
  dbutils.notebook.run("00_Master/1.Run_Main_monthly", 0, params_provisional)
  dbutils.notebook.run("00_Master/2.Run_AWT", 0, params_provisional)
  dbutils.notebook.run("00_Master/3.Run_CYP_2nd_contact", 0, params_provisional)
  dbutils.notebook.run("00_Master/4.Run_CAP", 0, params_provisional)
  dbutils.notebook.run("00_Master/5.Run_CYP_monthly", 0, params_provisional)

  dbutils.notebook.run("00_Master/7.Run_Ascof", 0, params_provisional)

# COMMAND ----------

# DBTITLE 1,Format and round provisional output
if params_provisional:
 
  dbutils.notebook.run("02_Aggregate/List_possible_metrics", 0, params_provisional)
  dbutils.notebook.run("02_Aggregate/Expand_output", 0, params_provisional)
  dbutils.notebook.run("02_Aggregate/Clean_formatted_tables", 0, params_provisional)
  dbutils.notebook.run("02_Aggregate/Cache_output", 0, params_provisional)
  dbutils.notebook.run("02_Aggregate/Round_output", 0, params_provisional)

# COMMAND ----------

# DBTITLE 1,Final products
print(is_quarter)
print(params_final)

if params_final:
  dbutils.notebook.run("00_Master/1.Run_Main_monthly", 0, params_final)
  dbutils.notebook.run("00_Master/2.Run_AWT", 0, params_final)
  dbutils.notebook.run("00_Master/3.Run_CYP_2nd_contact", 0, params_final)
  dbutils.notebook.run("00_Master/4.Run_CAP", 0, params_final)
  dbutils.notebook.run("00_Master/5.Run_CYP_monthly", 0, params_final)

  dbutils.notebook.run("00_Master/7.Run_Ascof", 0, params_final)
  if is_quarter:
    dbutils.notebook.run("00_Master/8.Run_FYFV", 0, params_final)
    dbutils.notebook.run("00_Master/9.Run_CCGOIS", 0, params_final)

# COMMAND ----------

# DBTITLE 1,Format and round final output
if params_final:
  dbutils.notebook.run("02_Aggregate/List_possible_metrics", 0, params_final)
  dbutils.notebook.run("02_Aggregate/Expand_output", 0, params_final)
  dbutils.notebook.run("02_Aggregate/Clean_formatted_tables", 0, params_final)
  dbutils.notebook.run("02_Aggregate/Cache_output", 0, params_final)
  dbutils.notebook.run("02_Aggregate/Round_output", 0, params_final)