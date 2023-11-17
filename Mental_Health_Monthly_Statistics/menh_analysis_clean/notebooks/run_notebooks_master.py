# Databricks notebook source
# dbutils.widgets.text("automatic_run","false","automatic_run")
# dbutils.widgets.text("adhoc_desc","testing","adhoc_desc")

# COMMAND ----------

# DBTITLE 1,Get Params
 %python
 import json
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 
 db_source = dbutils.widgets.get("db_source")
 db_output = dbutils.widgets.get("db_output")
 month_id = dbutils.widgets.get("month_id")
 status  = dbutils.widgets.get("status")
 rp_enddate  = dbutils.widgets.get("rp_enddate")
 rp_startdate  = dbutils.widgets.get("rp_startdate")
 $reference_data = dbutils.widgets.get("$reference_data")
 automatic_run  = dbutils.widgets.get("automatic_run")
 custom_run  = dbutils.widgets.get("custom_run")
 adhoc_desc = dbutils.widgets.get("adhoc_desc")
 
 startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")
 month_period = startdateasdate.strftime('%B')[:3] + '-' + rp_enddate[:4][-2:] 
 
 # Datatype conversion need to happen as they are being passed as strings
 automatic_run = True if(automatic_run == 'true') else False
 custom_run = True if(custom_run == 'true') else False
 
 params = {
     "db_source": db_source,
     "db_output": db_output,
     "month_id": month_id,
     "status": status,
     "rp_enddate": rp_enddate,
     "rp_startdate": rp_startdate,
     "$reference_data": $reference_data,
     "automatic_run": automatic_run,
     "custom_run": custom_run,
     "adhoc_desc": adhoc_desc,
     "MonthPeriod" : month_period,
     "rp_startdate_quarterly" : '',
     #IsQuarterly Report required
     "is_quarter" : False #Defaulting to false state 
 
 }
 
 
 if(status not in ['Final','Performance','Provisional','Adhoc'] or #Not a valid status
   startdateasdate >  datetime.now()):       # No future dates allowed
   raise Exception ('Not a valid values passed')
 
 
 print(json.dumps(params,
                   indent = 4,
                   sort_keys = True)); 
 
 print(f"Data type of custom_run value : {type(params['custom_run'])}")

# COMMAND ----------

# DBTITLE 1,Function used for Date Conversions (Do Not Remove) 
#GBT not actually used at the moment?

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
  
#not used at present but could be used as follows:

# newparams = convert_date(rp_startdate)
    
# newparams


# COMMAND ----------

 %python
 notebooks_list = [
   '01_Prepare/prepare_all_products',
   '00_Master/1.Run_Main_monthly',
   '00_Master/2.Run_AWT',
   '00_Master/3.Run_CYP_2nd_contact',
   '00_Master/4.Run_CAP',
   '00_Master/5.Run_CYP_monthly',
 #  '00_Master/7.Run_Ascof',
   
   '02_Aggregate/90.Pre_April_2021_Measures',
   '02_Aggregate/90a.Pre_April_2023_Measures'
 ]
   
 
 aggregate_steps = [
   '02_Aggregate/91.List_possible_metrics',
   '02_Aggregate/92.Expand_output',
   '02_Aggregate/93.Clean_formatted_tables',
   '02_Aggregate/94.Cache_output',
   '02_Aggregate/95.Round_output',
   '02_Aggregate/96.Update_raw_outputs_ICB'
 ]
 
 quarter_books = [  
 #   Quarterly notebooks that should run on quarter basis
   #'00_Master/9.Run_CCGOIS', - no longer needed (since CCGs ceased to exist in June 2022)
   '00_Master/8.Run_FYFV'
 ]
 
   
 '''This 02_Aggregate/90.Pre_April_2021_Measures notebook is being run on if condition for both provisional and performance (possible for custom run but that is ages ago though!) so removing it from the list based on condition monthid condition is which is there before'''
 
 if int(params['month_id']) <= 1452:
   print('We are keeping the notebook 02_Aggregate/90.Pre_April_2021_Measures')
 
 else:
   print('The following measures have been decommissioned for ' + str(month_id) + ': ACC02, CYP02, AMH02, AMH03, ACC53, AMH15, AMH04, AMH18, AMH05, AMH06, MHS02. So, we dont need to run 02_Aggregate/90.Pre_April_2021_Measures')
   if('02_Aggregate/90.Pre_April_2021_Measures' in notebooks_list):
     notebooks_list.remove('02_Aggregate/90.Pre_April_2021_Measures')
     
 
 '''This 02_Aggregate/90a.Pre_April_2023_Measures notebook is being run on if condition for both provisional and performance (possible for custom run but that is ages ago though!) so removing it from the list based on condition monthid condition is which is there before'''
 
 if int(params['month_id']) <= 1476:
   print('We are keeping the notebook 02_Aggregate/90a.Pre_April_2023_Measures')
 
 else:
   print('The following measures have been decommissioned for ' + str(month_id) + ': CCR70, CCR70a, CCR70b, CCR71, CCR71a, CCR71b, CCR72, CCR72a, CCR72b, CCR73, CCR73a, CCR73b. So, we dont need to run 02_Aggregate/90a.Pre_April_2023_Measures')
   if('02_Aggregate/90a.Pre_April_2023_Measures' in notebooks_list):
     notebooks_list.remove('02_Aggregate/90a.Pre_April_2023_Measures')
 
 
 
 
 print('Main Notebooks to run : ', *notebooks_list, sep = '\n')
 print('Quarter Notebooks : ', *quarter_books, sep = '\n')
 print('Aggregation Notebooks : ', *aggregate_steps, sep = '\n')

# COMMAND ----------

# DBTITLE 1,Need to know how many job loops should run
 %python
 jobs = [params['status']] # defaultly adding one one job as it is passed from the status. Example 'Provisional' for automatic run, sometimes 'Final' for custom run
 if(params['automatic_run']):
   jobs.append('Performance') # Only for automatic run we need to add this
   print('We are going for Automatic run now which runs First for Provisional and then Performance')
   
   

# COMMAND ----------

# DBTITLE 1,This function logs the Job run params to the audit_menh_analysis table
 
 %python
 
 # 01/03/2022: Create audit table
 def auditInsertLog(params, runStart, runEnd):
   try:
     # MARKS THE LOG WITH Auto FOR RUNs ON SCHEUDLED BASIS
     status_log = 'Auto ' + params['status'] if (params['automatic_run']) else params['status']
     spark.sql("INSERT INTO TABLE {db_output}.audit_menh_analysis VALUES('{month_id}','{status}','{rp_startdate}','{rp_enddate}','{db_source}',FROM_UNIXTIME({runStart}),FROM_UNIXTIME({runEnd}),'{adhoc_desc}')"
               .format(db_output =  params['db_output'],
                       month_id = params['month_id'],
                       status = status_log,
                       rp_startdate = params['rp_startdate'],
                       rp_enddate = params['rp_enddate'],
                       db_source = params['db_source'],
                       adhoc_desc = params['adhoc_desc'],
                       runStart = runStart,
                       runEnd = runEnd))
   except Exception as ex:
     print(ex, ' Failed to log to audit_menh_analysis') 

# COMMAND ----------

# DBTITLE 1,Block manages both custom and automatic run
 %python
 
 # this is the automated run code (where widget parameters NOT filled in)
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 import time
 import os
 import json
 print(f"Data type of custom_run value : {type(params['custom_run'])}")
 
 
 
 
 try:
 
   for run in jobs:
     now = datetime.now()
     runStart = datetime.timestamp(now)
         
     print(f'Started the job at: {datetime.time(now)}')
   ############################## Some manipulation to pass the performance parameters from the submission calendar #############################################
   # Assuming run made it upto here means, we have supplied performance parameters too. so we can access them now, if it is custom run those parameters are not introduced in the params section
   # As the provisional run completed with values rp_startdate, rp_enddate, monthId, rp_startdate_quarterly. Now we need to assign the same parameter keys with Provisional values as the same parameter names are being passed in the notebooks
 
     if(run == 'Performance'):
        
         #This if condition helps in tailoring the performance paramters during the auto run process
       if(params['automatic_run']):
         params['rp_startdate'] = dbutils.widgets.get("perf_rp_startdate")
         params['rp_enddate'] = dbutils.widgets.get("perf_rp_enddate")
         params['month_id'] = dbutils.widgets.get("perf_month_id")
         params['status'] = run
         params['MonthPeriod'] = datetime.strptime(params['rp_startdate'], "%Y-%m-%d").strftime('%B')[:3] + '-' + params['rp_enddate'][:4][-2:] 
          #check if selected month is a quarter. if quarter, we have notebooks that need quarterly start date to run the reports,  even if it is manual or auto run. This if condition need to be below the above automation condition so it can pick correct values of month id while doing auto run
       if int(params['month_id']) % 3 == 0:
         params['is_quarter'] = True
         params['rp_startdate_quarterly'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-2)).strftime('%Y-%m-%d')
     
       
       
     
     print("Params for {0} run are: {1}".format(run,
                                               json.dumps(params,
                                                         indent = 4)))
 
 
  
     for book in notebooks_list:
       dbutils.notebook.run(f"{book}", 0, params)
       print(f'{book} run complete\n')
       
     if (params['is_quarter'] and (params['status'] in ['Final','Performance','Adhoc'] )):
       # Need to run only if it is quarter month and status is any of mentioned statuses
       for book in quarter_books:
         dbutils.notebook.run(f"{book}", 0, params)
         print(f'{book} quarter book run complete\n')
 
     for book in aggregate_steps:
       dbutils.notebook.run(f"{book}", 0, params)
       print(f'{book} run complete\n')
 
     print(f'{run} completed.\n')
 
 
   # (advised) To Keep different notebooks rather than single note book with multiple if conditions on statuses and quarterly 
   # flag to ease the code writing and spotting based on statuses. 
   # NB, you can't run the report section out of this 'try' block as we are assigning the performance parameters during a second loop. (Old code used to call on the params rp_startdate, rp_enddate, month_id names so used the same names to call for performance)
   # This case should only run in prod, it can't work in ref and it hinders with job fail if not kept under IF condition
     if(os.environ.get('env') == 'prod'):
 #     if(os.environ.get('env') == 'ref'):
   #       dbutils.notebook.run("Publication_csvs", 0, params); # old one when reports are all in single book
   #       print(f'Publication_csvs run complete\n')
 
   #      Performance and Provisional (sometimes 'Final') reports, This report runs in all possible cases (both automatic and custom run so no need of if checks)
       dbutils.notebook.run("03_Extract/01_menh_analysis_csvs_perf_prov", 0, params);
       print(f'01_menh_analysis_csvs_perf_prov report run complete\n')
   #     Only Performance (Final)
       if(params['status'] in ['Performance', 'Final','Adhoc']):
         dbutils.notebook.run("03_Extract/02_menh_analysis_csvs_perf", 0, params);
         print(f'02_menh_analysis_csvs_perf report run complete\n')
         dbutils.notebook.run("03_Extract/03_LDA_Monthly_Outputs", 0, params);
         print(f'03_LDA_Monthly_Outputs report run complete\n')
 
         if(params['is_quarter']):
           dbutils.notebook.run("03_Extract/04_menh_analysis_csvs_quarter", 0, params); # running quarterly only in cases of performance status
           print(f'04_menh_analysis_csvs_quarter report run complete\n')
 
     print('Run complete after the for loop')
     now = datetime.now()
     runEnd = datetime.timestamp(now)
     print(f'Ended the job at: {datetime.time(now)}')
   #     calling the log functions if the job is success
     print('logging the successful run')
     auditInsertLog(params,runStart,runEnd)
 except Exception as ex:
   print(ex, ' Job run failed and logging to audit_menh_analysis.')
 #     calling the log function as the job failed with empty end date which will record as null in table
   auditInsertLog(params,runStart,'NULL')
   # using exception block escapes the show of'Failure' tag for Code Promotion notebooks even if there is an error in the notebooks.(Instead it shows as 'Success' even in case of errors and very misleading). So, need to raise the error here after logging to audit table as it is the entry point
   ## DO NOT USE TRY-EXCEPT BLOCKS ANYWHERE INSIDE THE CALLING NOTEBOOKS
   assert False

# COMMAND ----------

