# Databricks notebook source
 %md

 # This code base has been updated to be compatible with Spark 3 and will work fine only on Spark3 clusters ( SPARK-3-DBR-10-4-DATA-MANAGERS-1). It might raise exception on Spark 2 clusters.

# COMMAND ----------

 %python
 import os

 '''
 In Prod if the scheduler runs the job, at that time only default params which were already configured will be passed. 'rp_startdate' and 'status' are manual entries for parameters during custom job run, these params are passed as widget entry or json values but won't be there during automatic run. So, we need to check presence of these params to determine if this is an Automatic run
 '''

 is_rp_start_avail = True
 is_status_avail = True

 # Toggle the comments if you want to simulate the run in Ref
 # if(os.environ.get('env') == 'ref'):
 if(os.environ.get('env') == 'prod'):
   try:
     dbutils.widgets.get("rp_startdate")
   except Exception as ex:
     is_rp_start_avail = False

   try:
     dbutils.widgets.get("status")
   except Exception as ex:
     is_status_avail = False
 print(f'rp_start parameter availability: {is_rp_start_avail} \nstatus parameter availability: {is_status_avail}')

 # It can  be auto run if both fields are not available as in automatic run (means those widgets are not present)
 auto_prov_check = False if (is_rp_start_avail and is_status_avail) else True
 print(f'Provisional check for automatic run: {auto_prov_check}')
   

# COMMAND ----------

# DBTITLE 1,Parameters checking and assigning
#User note copied (and amended) from menh_publications to remove the need to put in so many data inputs...

import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

dbutils.widgets.text("db", "menh_dq", "Target database")
db = dbutils.widgets.get("db")
assert db

# this is needed to enable run_notebooks to be run both from run_tests during promotion and directly run
# needs to use $mhsds_db for both but the parameter is fed in from run_tests as the original $mhsds_db :o(

# get the original parameter value (will work in all sitations)
try:
  db_source = dbutils.widgets.get("$mhsds_db")
except:
  print('$mhsds_db is not defined')

# get the new parameter value (will only work in direct run, and will overwrite value for $mhsds_db)
try:
  db_source = dbutils.widgets.get("$mhsds_db")
except:
  print('$mhsds_db is not defined')

try:
  db_source = dbutils.widgets.get("$mhsds_db")
except:
  print('$mhsds_db is not defined')  

dbutils.widgets.text("reference_data","reference_data","Reference Database")
reference_data = dbutils.widgets.get("reference_data")
assert reference_data



# Basic parameters that can be passed

params = {
  'dbm' : db_source, 
  'db_output' : db, 
  'reference_data': reference_data,
  'rp_enddate' : '', 
  'rp_startdate' : '', 
  'month_id' : '', 
  'status': '', 
  'MonthPeriod': '',
  'custom_run': False,  #Defaulting to false for job run
  'automatic_run' : False # Defaulting to false
}

'''
we have two boolean parameters because they are not complimentary:
Custom - true , means obviously we fed the rp_startdate and status to run
automatic - true, custom - false, means there is data available at source for provisional and there is no job ran recently.
automatic - false, custom - false, there is nothing to do, if we kept complimentary this case would be off

'''

print('Basic parameters: {}'.format(json.dumps(params,
                                              indent = 4)))
'''
Function to calculate month_id for the custom run, we can use the submission calendar too but its date range is limited and this function block is already there to calculate month id for custom run, so been using it.
'''

def calculateUniqMonthID(RPStartDate: datetime)-> int:
  date_for_month = RPStartDate
  print(f'Reporting period start date: {RPStartDate}')
  start_date = datetime(1900, 4, 1)
  time_diff = relativedelta(date_for_month, start_date)
  return time_diff.years * 12 + time_diff.months + 1



# COMMAND ----------

# DBTITLE 1,Parameter building for the job run
 %python
 from shared.constants import DS
 # from shared.submissions.calendars import mhsds_ytd, submission_calendar # This is V4 calendar
 from shared.submissions.calendars import mhsds_v6, submission_calendar
 from datetime import datetime, date

 # UKD 07/03/2022 BITC-3069 menh_dq: Determine standard run months>>>>>>>>

 if(auto_prov_check):

   # As the provisional check is passed, we need to get the month id, reporting periods for the provisional months of current date

 #   _mhsds_ytd_calendar = submission_calendar(DS.MHSDS_YTD, {}) #this is V4 call
   _mhsds_ytd_calendar = submission_calendar(DS.MHSDS_V6, {})
   

   today_date = datetime.today()
   print('Validity check for the date: {0}\n'.format( today_date))
   submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
       today_date, fake_historic_windows=False,
   )
   idx_current_report_month = len(submission_window.reporting_periods) - 1
   prov_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id

   #LW Updates - Apr -24 update print statement to reflect change to single submission window model
   if prov_month_id <= 1483:
       print(f"\
               length of period tuple from Submission Calendar: {len(submission_window.reporting_periods)}\n\
               Submission window opens: {submission_window.opens}\n\
               Submission window closes: {submission_window.closes}\n\
               Provisional period start: {submission_window.reporting_periods[idx_current_report_month].start}\n\
               Provisional period end: {submission_window.reporting_periods[idx_current_report_month].end}\n\
               Provisional period month id: {prov_month_id}\n\
               ")
   else:
       print(f"\
               length of period tuple from Submission Calendar: {len(submission_window.reporting_periods)}\n\
               Submission window opens: {submission_window.opens}\n\
               Submission window closes: {submission_window.closes}\n\
               Performance period start: {submission_window.reporting_periods[idx_current_report_month].start}\n\
               Performance period end: {submission_window.reporting_periods[idx_current_report_month].end}\n\
               Performance period month id: {prov_month_id}\n\
               ")
       
   
   #### CHECK THE RECENT MONTH ID OF SUCCESSFUL RUN IN audit_menh_dq TABLE
   audit_month_id = spark.sql("SELECT MAX(MONTH_ID) AS auditMonthId FROM {0}.audit_menh_dq WHERE RUN_END IS NOT NULL".format(params['db_output'])).collect()[0]['auditMonthId']; 
   print(f'Audit month ID from recent job runs: {audit_month_id}')
 #   audit_month_id = 1461
   source_month_id = spark.sql("SELECT MAX(UniqMonthId) AS sourceMonthId FROM {0}.mhs000header".format(params['dbm'])).collect()[0]['sourceMonthId']; 

 #   source_month_id = 1462
   print(f'Recent month id available at source database: {source_month_id}')
   ### CONDITION TO CHECK WHETHER THERE IS SUCCESSFUL RUN FOR PROVISIONAL MONTH AND DATA AVAILABLE FOR PROVISIONAL IN SOURCE DB
   if((prov_month_id > audit_month_id) and (prov_month_id == source_month_id )):
 #   if(True):
     params['automatic_run'] = True # YES, WE CAN RUN THE JOB THAT PICKS UP THE PROVISIONAL DATA
     params['rp_enddate'] = str(submission_window.reporting_periods[idx_current_report_month].end)
     params['rp_startdate'] = str(submission_window.reporting_periods[idx_current_report_month].start)
     params['month_id'] = prov_month_id
     startdateasdate = datetime.strptime(params['rp_startdate'], "%Y-%m-%d")
     month_period = startdateasdate.strftime('%B')[:3] + '-' + params['rp_enddate'][:4][-2:]
     params['MonthPeriod'] = month_period
     #LW Update Apr 24 - for months up to and including October 23 - provisional is the first reporting window and performance is the second reporting window
     if prov_month_id <= 1483:
       params['status'] = 'Provisional'  # Defaulting to Provisional run as it would be run first in the automatic block
       idx_perf_report_month = idx_current_report_month - 1 # reducing by one to get the performance month index
       perf_month_id = submission_window.reporting_periods[idx_perf_report_month].unique_month_id 
       perf_rp_startdate = submission_window.reporting_periods[idx_perf_report_month].start
       perf_rp_enddate = submission_window.reporting_periods[idx_perf_report_month].end
       params['perf_month_id'] = str(perf_month_id)
       params['perf_rp_startdate'] = str(perf_rp_startdate)
       params['perf_rp_enddate'] = str(perf_rp_enddate )
       # Not calculating month period as it is not found to be used anywhere in the calling notebooks
       print('Provisional parameters for Eligible automatic run: {}'.format(json.dumps(params,
                                                          indent = 4)))
     else:
       params['status'] = 'Performance'
       
     print('Performance parameters for Eligible Automatic run: {}'.format(json.dumps(params,
                                                                                    indent = 4)))
     
   else:
     params['automatic_run'] = False # We don't need to run the automatic job
     print('Provisional parameters for Failsafe Automatic run: {}'.format(json.dumps(params,
                                                                                    indent = 4)))
   
 else:
   # Few assertions need to done as rp_startdate and status are passed only in the custom job run
   rp_startdate = dbutils.widgets.get("rp_startdate")
   assert rp_startdate
   
   status = dbutils.widgets.get("status")
   assert status
   # Calculated few needed parameters to run the custom job
   startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")
   rp_enddate = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
   UniqMonthID = calculateUniqMonthID(startdateasdate)
   
   month_period = startdateasdate.strftime('%B')[:3] + '-' + rp_enddate[:4][-2:] # This value is used in the DQMI extracts books
   
   # assign the calculated parameters that are needed for the cutom job run 
   params['custom_run'] = True
   params['rp_enddate'] = rp_enddate
   params['rp_startdate'] = rp_startdate
   params['month_id'] = UniqMonthID
   params['MonthPeriod'] = month_period
   params['status'] = status
   print('Custom job parameters: {}'.format(json.dumps(params,
                                                       indent = 4)))
   
   
   
   

# COMMAND ----------

# User note added for alternative run with different source data

dbutils.widgets.text("alt_source_data","","alt_source_data")
alt_source_data = dbutils.widgets.get("alt_source_data")

print("alt_source_data: ",alt_source_data)

# User note this enables the main code base (or the v4 code base) to run on an alternative data source - such as Point In Time data or v4 (non-MSWM) data
# value of alt_source_data should be the name of the alternative database

# options for this parameter are:
# 'menh_primary_refresh' for v4 data
# 'menh_point_in_time' for point in time data


if len(alt_source_data) > 0:
  db_source = dbutils.widgets.get("alt_source_data")
  params['dbm'] = db_source
  print("new params: {}".format(json.dumps(params,
                                        indent = 4,
                                         sort_keys = True)))
else:
  print("original params: {}".format(json.dumps(params,
                                            indent = 4,
                                            sort_keys = True)))



# COMMAND ----------

# DBTITLE 1,Encapsulated block to log the runs
 %python

 from datetime import datetime
 import time

 print("Params here: {}".format(json.dumps(params,
                                       indent = 4,
                                        sort_keys = True)))


 #run the job only if one of the case is true
 if(params['automatic_run'] or params['custom_run']):
   print('Running the bootstrap notebook')
   dbutils.notebook.run("notebooks/bootstrap", 0, params)

   ############################################# run extracts
   
 #  the 2 notebooks below populate out of date metadata into tables - this is not used in publication and hasn't been updated PROPERLY since first written in v4.
 #  the data being entered into the tables is the same each month - it should change when there is a new version of MHSDS, but version specific metadata are maintained elsewhere
 #  the sources for these non-extracted extracts are updated to enable the code to work but the detailed descriptions generated here are out of date and shouldn't be used (and are not being used)
 #  the running of these 2 notebooks is not needed and is being commented out - BITC-3403

   # Parameter keys utilized in this note book are 'db_output', 
 #   dbutils.notebook.run("notebooks/3.extract/3.1.integrity_rules_csv", 0, params)
     # Parameter keys utilized in this note book are 'db_output', 
 #   dbutils.notebook.run("notebooks/3.extract/3.2.validity_rules_csv", 0, params)

 #   raise Exception ('Making the job fail')

 else:
   print(f'Neither Custom run nor Automatic run conditions are met and the job could not run')

# COMMAND ----------

# DBTITLE 1,Quick glance at the audit table for runs

 %python
 audit_table = spark.sql(f"SELECT * FROM {params['db_output']}.audit_menh_dq ORDER BY RUN_START DESC LIMIT 5")
 display(audit_table)