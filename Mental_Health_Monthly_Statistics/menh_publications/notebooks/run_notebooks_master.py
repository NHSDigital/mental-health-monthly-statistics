# Databricks notebook source
# dbutils.widgets.remove("automaticrun")

# COMMAND ----------

 %sql
 create widget text adhoc_desc default ""

# COMMAND ----------

 %python
 import json
 from datetime import datetime
 from dateutil.relativedelta import relativedelta
 # product = ''
 product = dbutils.widgets.get("product")

 db_output = dbutils.widgets.get("db_output")
 assert db_output

 db_source = dbutils.widgets.get("db_source")
 assert db_source

 rp_startdate = dbutils.widgets.get("rp_startdate")
 assert rp_startdate

 startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")

 # rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly") # needs to be defined for each run so that it changes value between Provisional and Performance in a standard run
 # assert rp_startdate_quarterly

 rp_enddate = dbutils.widgets.get("rp_enddate")
 assert rp_enddate

 month_id = dbutils.widgets.get("month_id")
 assert month_id

 status = dbutils.widgets.get("status")
 assert status

 reference_data = dbutils.widgets.get("reference_data")
 assert reference_data

 rp_startdate_quarterly = (startdateasdate + relativedelta(months=-2, days=0)).strftime("%Y-%m-%d")
 assert rp_startdate_quarterly

 rp_startdate_12m = (startdateasdate + relativedelta(months=-11, days=0)).strftime("%Y-%m-%d")
 assert rp_startdate_12m

 testrun = dbutils.widgets.get("testrun")

 # Financial_Yr_Start = dbutils.widgets.get("Financial_Yr_Start")
 # assert Financial_Yr_Start

 automatic_run  = dbutils.widgets.get("automatic_run")
 custom_run  = dbutils.widgets.get("custom_run")
 adhoc_desc  = dbutils.widgets.get("adhoc_desc")

 # Datatype conversion need to happen as they are being passed as strings
 automatic_run = True if(automatic_run == 'true') else False
 custom_run = True if(custom_run == 'true') else False
 # perf_month_id  = '' if (custom_run) else dbutils.widgets.get("perf_month_id") # these params have no significance in the custom run
 # perf_rp_startdate  = '' if (custom_run) else  dbutils.widgets.get("perf_rp_startdate")
 # perf_rp_enddate  = '' if (custom_run) else dbutils.widgets.get("perf_rp_enddate")

 if(status not in ['Final','Performance','Provisional','Adhoc'] or #Not a valid status
   startdateasdate >  datetime.now()):       # No future dates allowed
   raise Exception ('Not a valid values passed')

# COMMAND ----------

metadata_df = spark.sql(f"SELECT * FROM {db_output}.metadata")
display(metadata_df)

# COMMAND ----------


 %python

 params = {
     "db_source": db_source,
     "db_output": db_output,
     "month_id": month_id,
     "status": status,
     "rp_enddate": rp_enddate,
     "rp_startdate": rp_startdate,
     "reference_data": reference_data,
     "product": product,
     "automatic_run": automatic_run,
     "custom_run": custom_run,
     "adhoc_desc": adhoc_desc,
     "testrun": testrun,
     "rp_startdate_12m": rp_startdate_12m,
     "rp_startdate_quarterly": rp_startdate_quarterly
 }

 print(json.dumps(params,
                   indent = 4,
                   sort_keys = True)); 

 print(f"Data type of custom_run value : {type(params['custom_run'])}")

# COMMAND ----------

from pyspark.sql.functions import col,lit



group_by_seq =  metadata_df.select(col('seq'))\
                          .groupBy(col('seq'))\
                          .count()\
                          .where(col('count') > 1)\
                          .collect()

print(group_by_seq)

if group_by_seq:
  raise ValueError(f"multiple entries found for seq {group_by_seq}")
  
#check for ambigious metadata
group_by_products = metadata_df.select(col('product'),col('module'),col('stage'),col('notebook_path'))\
                                .groupBy(col('product'),col('module'),col('stage'),col('notebook_path'))\
                                .count()\
                                .where(col('count') > 1)\
                                .collect()

print(group_by_products)

if group_by_products:
  raise ValueError(f"multiple entries found product, module and stage columns")


# COMMAND ----------

 %md

 menh_publications has only one code block from old. As there is no separate block for automation using the existing one for both Performance and Provisional during automatic run and single run during the custom run

# COMMAND ----------

# DBTITLE 1,Need to know how many job loops should run
 %python
 jobs = [params['status']] # defaultly adding one one job as it is passed from the status. Example 'Provisional' for automatic run, some times 'Final' for custom run
 if(params['automatic_run']):
   jobs.append('Performance') # Only for automatic run we need to add this
   print('We are going for Automatic run now which runs First for Provisional and then Performance')
   
   

# COMMAND ----------

# DBTITLE 1,This function logs the Job run params to the audit_menh_publications table

 %python

 # menh_publications: Populate audit table
 def auditInsertLog(params, runStart, runEnd):
   # MARKS THE LOG WITH Auto FOR RUNs ON SCHEUDLED BASIS
   status_log = 'Auto ' + params['status'] if (params['automatic_run']) else params['status']
   try:
     spark.sql("INSERT INTO TABLE {db_output}.audit_menh_publications VALUES('{month_id}','{status}','{rp_startdate}','{rp_enddate}','{db_source}',FROM_UNIXTIME({runStart}),FROM_UNIXTIME({runEnd}),'{adhoc_desc}')"
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
     print(ex, ' failed to log to audit_menh_publications')

# COMMAND ----------

# DBTITLE 1,Month ID 1477 is used as the changeover month for ED as that represents the last month of 2022-23

 %python


 from datetime import datetime
 import time
 import os

 try:
   for run in jobs:

     now = datetime.now()
     runStart = datetime.timestamp(now)
     print(f'Started the job at: {datetime.time(now)}')
     if(params['automatic_run'] and run == 'Performance'):
       #Only for performance run we need reassign the variables as those key names are used in the subsequent notebooks.
       # We can get these values only for automatic run, so used dbutils here.
       params['rp_startdate'] = dbutils.widgets.get("perf_rp_startdate")
       params['rp_enddate'] = dbutils.widgets.get("perf_rp_enddate")
       params['month_id'] = dbutils.widgets.get("perf_month_id")
       params['status'] = run
     # COMMON PARAMETER ADHOC CALCULATIONS THAT APPLIES BOTH PERFORMANCE AND PROVISIONAL IN CUSTOM/AUTOMATIC RUN
     startdateasdate = datetime.strptime(params['rp_startdate'], "%Y-%m-%d")
     rp_startdate_quarterly = (startdateasdate + relativedelta(months=-2, days=0)).strftime("%Y-%m-%d")
     params['rp_startdate_quarterly'] = str(rp_startdate_quarterly)
     rp_startdate_12m = (startdateasdate + relativedelta(months=-11, days=0)).strftime("%Y-%m-%d")
     params['rp_startdate_12m'] = str(rp_startdate_12m)
     params['rp_startdate_run'] = str(rp_startdate_quarterly)
     
     # new start of financial year parameter required by CYP_Outcome_Measures 
     NowMonth = startdateasdate.month
     print('NowMonth: ',NowMonth)
     if NowMonth > 3:
       Financial_Yr_Start = datetime(startdateasdate.year,4,1).strftime("%Y-%m-%d")
       params['Financial_Yr_Start'] = str(Financial_Yr_Start)
     else:
       Financial_Yr_Start = datetime(startdateasdate.year-1,4,1).strftime("%Y-%m-%d")
       params['Financial_Yr_Start'] = str(Financial_Yr_Start)

     print("Params for {0} run are: {1}".format(run,
                                               json.dumps(params,
                                                         indent = 4)))
     ########################################################### always run common objects notebooks
     dbutils.notebook.run('00_common_objects/00_version_change_tables', 0, params)
     dbutils.notebook.run('00_common_objects/01_prep_common_objects', 0, params)
     dbutils.notebook.run('00_common_objects/02_load_common_ref_data', 0, params)
     dbutils.notebook.run('00_common_objects/03_load_population_ref_data', 0, params)

     ############################################################ DEFAULT RUN - RUN ALL
     if not product:
       print("processing default run")
       rows = metadata_df.select('product','notebook_path')\
                         .where(col('module') == lit('run_notebooks'))\
                         .sort(col("seq"))\
                         .collect()

       print(rows)
       for row in rows:
         run = 1
         row_product = row['product']
         if (row_product == 'CYP_ED_WaitingTimes_preFY2324' and month_id >= '9999'):
           run = 0
         elif ((row_product == 'CYP_ED_WaitingTimes' or row_product == 'CYP_ED_WaitingTimes_12m') and month_id < '9999'):
           run = 0
           
         if row_product == 'CYP_ED_WaitingTimes':
           params['ccg_table'] = db_output + ".MHS001_CCG_LATEST"
           params['provider_table'] = db_output + ".providers_between_rp_start_end_dates"
           params['rp_startdate_run'] = params['rp_startdate_quarterly']
         elif row_product == 'CYP_ED_WaitingTimes_12m':
           params['ccg_table'] = db_output + ".MHS001_CCG_LATEST_12m"
           params['provider_table'] = db_output + ".providers_between_rp_start_end_dates_12m"
           params['rp_startdate_run'] = params['rp_startdate_12m']
         else:
           params['ccg_table'] = ''
           params['provider_table'] = ''
           params['rp_startdate_run'] = ''
         
         if run == 1:
           path = row['notebook_path']
           print(path)
           dbutils.notebook.run(f'{path}', 0, params)
     else:
       print(f"processing {product} product")
       run_product = product
       if (product == 'CYP_ED_WaitingTimes' and month_id < '9999'):
         run_product = 'CYP_ED_WaitingTimes_preFY2324'
         
       if run_product == 'CYP_ED_WaitingTimes':
         run_product2 = run_product+"_12m"
         rows = metadata_df.select('product','notebook_path')\
                           .where((col('module') == lit('run_notebooks')) & ((col("product") == lit(run_product))|(col("product") == lit(run_product2))))\
                           .sort(col("seq"))\
                           .collect()
       else:
         rows = metadata_df.select('product','notebook_path')\
                           .where((col('module') == lit('run_notebooks')) & (col("product") == lit(run_product)))\
                           .sort(col("seq"))\
                           .collect()
       print(rows)
       if not rows:
         raise ValueError(f"invalid value {product} for product")

       for row in rows:
         row_product = row['product']
         path = row['notebook_path']
         
         if row_product == 'CYP_ED_WaitingTimes':
           params['ccg_table'] = db_output + ".MHS001_CCG_LATEST"
           params['provider_table'] = db_output + ".providers_between_rp_start_end_dates"
           params['rp_startdate_run'] = params['rp_startdate_quarterly']
         elif row_product == 'CYP_ED_WaitingTimes_12m':
           params['ccg_table'] = db_output + ".MHS001_CCG_LATEST_12m"
           params['provider_table'] = db_output + ".providers_between_rp_start_end_dates_12m"
           params['rp_startdate_run'] = params['rp_startdate_12m']
         else:
           params['ccg_table'] = ''
           params['provider_table'] = ''
           params['rp_startdate_run'] = ''

         dbutils.notebook.run(f'{path}', 0, params)
         print(f'{path} run now complete from for loop')


     ############################################################
     dbutils.notebook.run("91_List_possible_metrics", 0, params)
     dbutils.notebook.run("92_Expand_output", 0, params)

 #     Hardcoded mapping fixed by changes to creation of $db_output.RD_CCG_LATEST within notebooks/00_common_objects/02_load_common_ref_data
  

     dbutils.notebook.run("93_Cache_output", 0, params)

     dbutils.notebook.run("94_Round_output", 0, params)

     dbutils.notebook.run("96.Update_raw_outputs_ICB", 0, params)

     print('Ran few notebooks here')



   # (advised) To Keep different notebooks rather than single note book with multiple if conditions on statuses and quarterly 
   # flag to ease the code writing and spotting based on statuses. 
   # NB, you can't run the report section out of this 'try' block as we are assigning the performance parameters during a second loop. (Old code used to call on the params rp_startdate, rp_enddate, month_id names so used the same names to call for performance)

     if(os.environ.get('env') == 'prod'):
   #   if(os.environ.get('env') == 'ref'):
   #       dbutils.notebook.run("Publication_csvs", 0, params);
   #       print(f'Publication_csvs run complete\n')

   #      Performance and Provisional (sometimes 'Final') reports, This report runs in all possible cases (both automatic and custom run so no need of if checks)
       dbutils.notebook.run("99_Extract/menh_publications_perf_prov", 0, params);
       print(f'menh_publications_perf_prov report run complete\n')
       dbutils.notebook.run("99_Extract/menh_publications_perf", 0, params);
       print(f'menh_publications_perf report run complete\n')
     #     Only Performance (Final)
 #       if(params['status'] in ['Performance', 'Final', 'Adhoc']):
 #         dbutils.notebook.run("99_Extract/menh_publications_perf", 0, params); 
 #         print(f'menh_publications_perf report run complete\n')

     print('Run complete after the for loop')
     #   raise Exception ('Making the job fail')
     now = datetime.now()
     runEnd = datetime.timestamp(now)
     print(f'Ended the job at: {datetime.time(now)}')
     #  Populate audit table 
   #     calling the log functions if the job is success
     auditInsertLog(params,runStart,runEnd)
 except Exception as ex:
   print(ex, ' Job run failed and logged to audit_menh_publications.')
 #     calling the log function as the job failed with empty end date which will record as null in table
   auditInsertLog(params,runStart,'NULL')
   # using exception block escapes the show of'Failure' tag for Code Promotion notebooks even if there is an error in the notebooks.(Instead it shows as 'Success' even in case of errors and very misleading). So, need to raise the error here after logging to audit table as it is the entry point
   ## DONOT USE TRY-EXCEPt BLOCKS ANYWHERE INSIDE THE CALLING NOTEBOOKS
   assert False
