-- Databricks notebook source
 %md

 # Bootstrap for the MHSDS Data Quality extract

 Please read and keep up-to-date the [Confluence documentation of this codebase](https://confluence.digital.nhs.uk/display/KH/Mental+Health+Data+Quality) !!!


-- COMMAND ----------

-- DBTITLE 1,Get Parameters
 %python
 import json
 from datetime import datetime
 from dateutil.relativedelta import relativedelta

 db_source = dbutils.widgets.get("dbm")
 db_output = dbutils.widgets.get("db_output")
 month_id = dbutils.widgets.get("month_id")
 status  = dbutils.widgets.get("status")
 rp_enddate  = dbutils.widgets.get("rp_enddate")
 rp_startdate  = dbutils.widgets.get("rp_startdate")
 reference_data  = dbutils.widgets.get("reference_data")
 automatic_run  = dbutils.widgets.get("automatic_run")
 custom_run  = dbutils.widgets.get("custom_run")
 MonthPeriod  = dbutils.widgets.get("MonthPeriod")

 automatic_run = True if(automatic_run == 'true') else False
 custom_run = True if(custom_run == 'true') else False
 params = {
     "db_source": db_source,
     "dbm" : db_source, # two notebooks are using this as source parameter key
     "db_output": db_output,
     "month_id": month_id,
     "status": status,
     "rp_enddate": rp_enddate,
     "rp_startdate": rp_startdate,
     "reference_data": reference_data,
     "automatic_run": automatic_run,
     "MonthPeriod": MonthPeriod, # used in DQMI_extracts notebooks
     "custom_run": custom_run
 }

 startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")

 if(status not in ['Final','Performance','Provisional']  #Not a valid status
   or startdateasdate >  datetime.now()):       # No future dates allowed
   raise Exception ('Invalid value passed')
   
 print(json.dumps(params,
                   indent = 4,
                   sort_keys = True)); 



 print(f"Data type of custom_run value : {type(params['custom_run'])}")

-- COMMAND ----------

-- DBTITLE 1,Check if Custom Run or NOT
 %python
 # # check if any input string parameters are empty then is_custom_run == FALSE else TRUE
 # is_custom_run = (len(month_id)>0 and len(rp_enddate)>0 and len(rp_startdate)>0 and len(status)>0 and len(db_output)>0 and len(db_source)>0)
 # print("Custom run: ", is_custom_run)

 # print('db_source:',db_source)
 # print('month_id:', month_id)

 notebook_list = [
   "00.common_objects/00_version_change_tables",
   "2.aggregate/truncate_inventory_by_month_id",
   "2.aggregate/integrity",
   "2.aggregate/validity",
   "2.aggregate/validity_gender",
   "2.aggregate/validity_CDQA",
   "2.aggregate/VODIM_aggregation",
   # keeping the notebook order as in the old code as the same books are called at three places so keeping them under one callable list

   "3.extract/3.3.coverage_csvs",
   "3.extract/3.4.vodim_csvs",
   "DQMI/DQMI_extracts"
   
 ]


 print('My list:', *notebook_list, sep='\n ')

-- COMMAND ----------

-- DBTITLE 1,Need to know how many job loops should run
 %python
 jobs = [params['status']] # defaultly adding one one job as it is passed from the status. Example 'Provisional' for automatic run, some times 'Final' for custom run

 #LW Update Apr 2024 - if up to and including October 2023 - we add a performance submission based on second window. Otherwise take the first month as the performance submission.
 if int(month_id) <= 1483:
   if(params['automatic_run']):
     jobs.append('Performance') # Only for automatic run we need to add this
     print('For pre October-2023 data - we will also run a performance outputs based on the second window. Afterwards performance is considered outputs based on the first window.')
   

-- COMMAND ----------

-- DBTITLE 1,This function logs the Job run params to the audit_menh_dq table


 %python
 # UKD 28/02/2022 BITC-3066 menh_dq: Populate audit table >>>

 def auditInsertLog(params, runStart, runEnd):
   try:
     # MARKS THE LOG WITH Auto FOR RUNs ON SCHEUDLED BASIS
     status_log = 'Auto ' + params['status'] if (params['automatic_run']) else params['status']
     spark.sql("INSERT INTO TABLE {db_output}.audit_menh_dq VALUES('{month_id}','{status}','{rp_startdate}','{rp_enddate}','{dbm}',FROM_UNIXTIME({runStart}),FROM_UNIXTIME({runEnd}))"
               .format(db_output = params['db_output'],
                       month_id = params['month_id'],
                       status = status_log,
                       rp_startdate = params['rp_startdate'],
                       rp_enddate = params['rp_enddate'],
                       dbm = params['dbm'],
                       runStart = runStart,
                       runEnd = runEnd))
   except Exception as ex:
     print(ex)


-- COMMAND ----------

-- DBTITLE 1,Block manages both custom and automatic run
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
   
   #LW update Apr 2024 - the above logic only applies for months up to and including October 2023

     if(params['automatic_run'] and run == 'Performance'  and int(month_id) <= 1483):
       params['rp_startdate'] = dbutils.widgets.get("perf_rp_startdate")
       params['rp_enddate'] = dbutils.widgets.get("perf_rp_enddate")
       params['month_id'] = dbutils.widgets.get("perf_month_id")
       params['status'] = run

       startdateasdate = datetime.strptime(params['rp_startdate'], "%Y-%m-%d")
       month_period = startdateasdate.strftime('%B')[:3] + '-' + params['rp_enddate'][:4][-2:]
       params['MonthPeriod'] = month_period

     print("Params for {0} run are: {1}".format(run,
                                               json.dumps(params,
                                                         indent = 4,
                                                         sort_keys = True)))


     for book in notebook_list:

       dbutils.notebook.run(f"{book}", 0, params)
       print(f'{book} run complete\n')

     print(f'{run} completed.\n')


   # (advised) To Keep different notebooks rather than single note book with multiple if conditions on statuses and quarterly 
   # flag to ease the code writing and spotting based on statuses. 
   # NB, you can't run the report section out of this 'try' block as we are assigning the peroformance parameters during a second loop. (Old code used to call on the params rp_startdate, rp_enddate, month_id names so used the same names to call for performance)
   # This case should only run in prod, it can't work in ref and it hinders with job fail if not kept under if condition
     if(os.environ.get('env') == 'prod'):
   #   if(os.environ.get('env') == 'ref'):
   #      Performance and Provisional (sometimes 'Final') reports, This report runs in all possible cases (both automatic and custom run so no need of if checks)
       dbutils.notebook.run("3.extract/000_monthly_dq_csvs", 0, params);
       print(f'monthly_dq_csvs report run complete\n')
     print('Run complete after the for loop')
     now = datetime.now()
     runEnd = datetime.timestamp(now)
     print(f'Ended the job at: {datetime.time(now)}')
     # UKD 28/02/2022 BITC-3066 menh_dq: Populate audit table 
   #     calling the log functions if the job is success
     print('logging the successful run')
     auditInsertLog(params,runStart,runEnd)

 except Exception as ex:
   print(ex, ' Job run failed and logged to audit_menh_dq.')
 #     calling the log function as the job failed with empty end date which will record as null in table
   auditInsertLog(params,runStart,'NULL')
   # using exception block escapes the show of'Failure' tag for Code Promotion notebooks even if there is an error in the notebooks.(Instead it shows as 'Success' even in case of errors and very misleading). So, need to raise the error here after logging to audit table as it is the entry point
   ## DONOT USE TRY-EXCEPt BLOCKS ANYWHERE INSIDE THE CALLING NOTEBOOKS
   assert False
