# Databricks notebook source
 %md
 
 #Master Notebook for MHSDS Monthly Extract
 
 
 This note book will run the other note books to create the MHSDS Monthly Extract.
 The following stages will be run in order:
 - Prep
 - Aggregation
 - Extract

# COMMAND ----------

 %md
 ## Get the parameter values (dates, month_id)
 This gets the provisional and final month_ids and end_dates from the MHS000Header table, and then uses these to calculate the the start_dates. 
 
 There are two sets of start dates for both 1 month and 3 month products (one is one month before the end_date, the other is 3 months before the end date).
 
 These values are then stored in parameter dictionaries which are then used when the notebooks are called. 

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source  = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status = dbutils.widgets.get("status")

params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'status': status}

print(params)

# COMMAND ----------

# DBTITLE 1,don't need this - probably...
from datetime import datetime
import os

db = dbutils.widgets.get("db_output")
dbm = dbutils.widgets.get("db_source")

#Pull the month_id from MHS000Header
month_id_df = spark.sql("SELECT MAX (UniqMonthID) FROM {dbm}.MHS000Header".format(dbm=dbm))
month_id = month_id_df.collect()[0]['max(UniqMonthID)']

month_id_final = spark.sql("SELECT MAX (UniqMonthID) FROM {dbm}.MHS000Header WHERE FileType = 'Refresh'".format(dbm=dbm)).collect()[0]['max(UniqMonthID)']

#Pull the corresponding end_date from MHS000Header and extract the value
end_date_df = spark.sql("SELECT DISTINCT ReportingPeriodEndDate FROM {dbm}.MHS000Header WHERE UniqMonthID = {month_id}".format(dbm=dbm,month_id=month_id))
end_date = end_date_df.collect()[0]['ReportingPeriodEndDate']

end_date_final = spark.sql("SELECT DISTINCT ReportingPeriodEndDate FROM {dbm}.MHS000Header WHERE UniqMonthID = {month_id_final}".format(dbm=dbm,month_id_final=month_id_final)).collect()[0]['ReportingPeriodEndDate']

#Calculate the corresponding start_date
# for three month products, first need to extract end_date month:
end_month = end_date.timetuple()[1]
start_month_3 = end_month -2 # subtract two whole months and then set day to 1 to get the third month
start_date_3 = end_date.replace(day=1, month=start_month_3) 

end_month_final = end_date_final.timetuple()[1]
start_month_final_3 = end_month_final -2 # subtract two whole months and then set day to 1 to get the third month
start_date_final_3 = end_date_final.replace(day=1, month=start_month_final_3) 

# for one month products simply set the end_date "day" to 1, and you have the start_date:
start_date = end_date.replace(day=1)
start_date_final = end_date_final.replace(day=1)

# get current env 
current_env = os.environ.get('env', 'ref')

if not current_env:
  raise ValueError("environment variable is not available, aborting current execution")

# Create the list of parameters to be loaded 
status = "Provisional"
params = {'rp_enddate' : str(end_date), 'rp_startdate' : str(start_date), 'month_id' : str(month_id), 'status' : status, "current_env" : current_env, 'db' : db}
params_3 = {'rp_enddate' : str(end_date), 'rp_startdate' : str(start_date_3), 'month_id' : str(month_id), 'status' : status, "current_env" : current_env, 'db' : db}

status = "Final"
params_final = {'rp_enddate' : str(end_date_final), 'rp_startdate' : str(start_date_final), 'month_id' : str(month_id_final), 'status' : status, "current_env" : current_env, 'db' : db}
params_final_3 = {'rp_enddate' : str(end_date_final), 'rp_startdate' : str(start_date_final_3), 'month_id' : str(month_id_final), 'status' : status, "current_env" : current_env, 'db' : db}

# Debugging
params = {'current_env': 'ref', 'month_id': '1418', 'db': 'temp_menh_analysis1', 'dbm': '$db_source', 'status': 'Provisional', 'rp_enddate': '2018-05-31', 'rp_startdate': '2018-05-01'}

print(params)
print(params_3)
print(params_final)
print(params_final_3)


# COMMAND ----------

# DBTITLE 1,Clean unformatted output table (in case there already is left-over data for this month/status in the table)
 %sql
 
 DELETE FROM $db_output.LDA_Monthly_Output_Unrounded
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status';
 
 VACUUM $db_output.LDA_Monthly_Output_Unrounded RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Aggregate LDA
dbutils.notebook.run("../02_Aggregate/6.LDA", 0, params)

# COMMAND ----------

# DBTITLE 1,Optimize output table for performance
import os

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='LDA_Monthly_Output_Unrounded'))