# Databricks notebook source
 %md

 # This code base has been updated to be compatible with Spark 3 and will work fine only on Spark3 clusters ( SPARK-3-DBR-10-4-DATA-MANAGERS-1). It might raise exception on Spark 2 clusters.

# COMMAND ----------

 %python
 import os
 import json
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 from shared.constants import DS
 # from shared.submissions.calendars import mhsds_ytd, submission_calendar # This is V4 calendar
 from shared.submissions.calendars import mhsds_v6, submission_calendar
 import time

# COMMAND ----------

 %python
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

# DBTITLE 1,Build functions used in notebook
'''
Function to calculate month id for the custom run, we can use the submission calendar too but its date range is limited and this function block is already been there to calculate month id for custom run, so been using it.
'''


# month_id calculation for custom runs
def calculateUniqMonthID(RPStartDate: datetime)-> int:
  date_for_month = RPStartDate
  print(f'Report period start date :{RPStartDate}')
  start_date = datetime(1900, 4, 1)
  time_diff = relativedelta(date_for_month, start_date)
  return time_diff.years * 12 + time_diff.months + 1



'''
Functions to check if the parameters of a manual run are valid
'''

def compare_monthid(calcmonthid,inputmonthid):
  #This function compares 2 input monthid's to see if the correct data is available. This is for Performance and Provisional data
  msg = ("""Data in {db_source} does not match input parameters for {status}: Parameter UniqMonthID = {calcmonthid}, Data UniqMonthID = {inputmonthid}.
  The run has been cancelled. Please correct widgets with valid values for the data present in {db_source} and rerun.""".format( calcmonthid=calcmonthid,
                                                                                                                                 db_source=params['db_source'],
                                                                                                                                 status=status,
                                                                                                                                 inputmonthid=inputmonthid))
  assert calcmonthid ==inputmonthid, msg 

def checkfinalstatus():
  #This function calculates the financial year of the input rp_startdate and checks to see if there is data for March of that year with filetype 2 in mhs000header. 
  if startdateasdate.month < 4:
    finaldate = (startdateasdate + relativedelta(months=5-startdateasdate.month))
  else:
    finaldate = (startdateasdate + relativedelta(months=17-startdateasdate.month))
#     finaldate here is always the 1st of May following the data month - this is the month when the March Refresh data is submitted (i.e. FileType = 2,  March Final = EOY)
  print('finaldate:',finaldate)
  
  maxyr = int(finaldate.year)
  minyr = maxyr-1
  financialyear = str(minyr) + '/' + str(maxyr)

  submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
      finaldate, fake_historic_windows=False,
  )
  
  
  idx_current_report_month = len(submission_window.reporting_periods)-1
  print('idx_current_report_month:',idx_current_report_month)
  final_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id
  final_month_id
  print('final_month_id:',final_month_id)

  final_file_type = spark.sql("SELECT MAX(FileType) AS MaxFileType FROM {db_source}.mhs000header WHERE UniqMonthID = {final_month_id}".format(db_source=params['db_source'],
                                                                                                                                              final_month_id=final_month_id)).collect()[0]['MaxFileType']
  
  print('final_file_type:',final_file_type)
  assert final_file_type==2, 'Final data not available in {db_source} for {financialyear}'.format(db_source=params['db_source'],financialyear=financialyear)

# COMMAND ----------

# DBTITLE 1,Parameters checking and assigning
db = dbutils.widgets.get("db")
assert db

# this is needed to enable run_notebooks to be run both from run_tests during promotion and directly run
# needs to use $mhsds for both but the parameter is fed in from run_tests as the original $mhsds :o(

# get the original parameter value (will work in all sitations)
try:
  db_source = dbutils.widgets.get("$mhsds")
except:
  print('$mhsds is not defined')

# get the new parameter value (will only work in direct run, and will overwrite value for $mhsds)
try:
  db_source = dbutils.widgets.get("$mhsds")
except:
  print('$mhsds is not defined')
  

# the above replaces this simpler situation!
# db_source = dbutils.widgets.get("$mhsds")
# new v5 source
# db_source = dbutils.widgets.get("$mhsds")
# assert db_source

$reference_data = dbutils.widgets.get("$reference_data")
assert $reference_data


product = dbutils.widgets.text("product", "", "product")
product = dbutils.widgets.get("product")


# Basic parameters that can be passed

params = {
  'db_source' : db_source, 
  'db_output' : db, 
  '$reference_data': $reference_data,
  'rp_enddate' : '', 
  'rp_startdate' : '', 
  'month_id' : '', 
  'status': '', 
  'rp_startdate_quarterly': '',
  'Financial_Yr_Start': '',
  'product' : product,
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
                                              indent = 4,
                                              sort_keys = True)))


# COMMAND ----------

# For alternative run of v4 code

#test running this with June 2018 as this appears to be the only month with any likely data for the FYFV measures

# dbutils.widgets.removeAll();

dbutils.widgets.text("alt_source_data","","alt_source_data")
alt_source_data = dbutils.widgets.get("alt_source_data")

print("alt_source_data: ",alt_source_data)

# COMMAND ----------

# For setting testrun

# dbutils.widgets.removeAll();

dbutils.widgets.text("testrun","","testrun")
testrun = dbutils.widgets.get("testrun")

print("testrun: ",testrun)
params['testrun'] = dbutils.widgets.get("testrun")

# COMMAND ----------

# To enable alternative source data


if len(alt_source_data)>0:
#   db_source = dbutils.widgets.get("alt_source_data") # original that needs to be replaced by the line below to handle changes due to automation
  params['db_source'] = dbutils.widgets.get("alt_source_data")
  print("new params: {}".format(json.dumps(params,
                                          indent = 4,
                                          sort_keys = True)))
# assert db_source

else:
  print("original params: {}".format(json.dumps(params,
                                                indent = 4,
                                                sort_keys = True)))

# COMMAND ----------

# DBTITLE 1,Parameters to build for run
 %python
 # Determine standard run months>>>>>>>>
 _mhsds_ytd_calendar = submission_calendar(DS.MHSDS_V6, {})


 today_date = datetime.today()
 submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
     today_date, fake_historic_windows=False,
 )
 idx_current_report_month = len(submission_window.reporting_periods) - 1

 if(auto_prov_check):

   # As the provisional check is passed, we need to get the month id, reporting periods for the provisional months of current date

   print('Validity check for the date: {0}\n'.format( today_date))

   prov_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id

   print(f"\
           length of period tuple from Submission Calendar: {len(submission_window.reporting_periods)}\n\
           Submission window opens: {submission_window.opens}\n\
           Submission window closes: {submission_window.closes}\n\
           Provisional period start: {submission_window.reporting_periods[idx_current_report_month].start}\n\
           Provisional period end: {submission_window.reporting_periods[idx_current_report_month].end}\n\
           Provisional period month id: {prov_month_id}\n\
           ")
   
   adhoc_desc = ""
   
   #### CHECK THE RECENT MONTH ID OF SUCCESSFUL RUN IN audit_menh_dq TABLE
   audit_month_id = spark.sql("SELECT MAX(MONTH_ID) AS auditMonthId FROM {0}.audit_menh_publications WHERE RUN_END IS NOT NULL".format(params['db_output'])).collect()[0]['auditMonthId']; 
   print(f'Audit month ID from recent job runs: {audit_month_id}')
 #   audit_month_id = 1461
   source_month_id = spark.sql("SELECT MAX(UniqMonthId) AS sourceMonthId FROM {0}.mhs000header".format(params['db_source'])).collect()[0]['sourceMonthId']; 

 #   source_month_id = 1462
   print(f'Recent month id available at source database: {source_month_id}')
   ### CONDITION TO CHECK WHETHER THERE IS SUCCESSFUL RUN FOR PROVISIONAL MONTH AND DATA AVAILABLE FOR PROVISIONAL IN SOURCE DB
   if((prov_month_id > audit_month_id) and (prov_month_id == source_month_id )):
 #   if(True):
     params['automatic_run'] = True # YES, WE CAN RUN THE JOB THAT PICKS UP THE PROVISIONAL DATA
     params['rp_enddate'] = str(submission_window.reporting_periods[idx_current_report_month].end)
     params['rp_startdate'] = str(submission_window.reporting_periods[idx_current_report_month].start)
     params['month_id'] = prov_month_id
     params['status'] = 'Provisional'  # Defaultig to Provisional run as it would be run first in the automatic block
     
     idx_perf_report_month = idx_current_report_month - 1 # reducing by one to get the performance month index
     perf_month_id = submission_window.reporting_periods[idx_perf_report_month].unique_month_id 
     perf_rp_startdate = submission_window.reporting_periods[idx_perf_report_month].start
     perf_rp_enddate = submission_window.reporting_periods[idx_perf_report_month].end
     params['perf_month_id'] = str(perf_month_id)
     params['perf_rp_startdate'] = str(perf_rp_startdate)
     params['perf_rp_enddate'] = str(perf_rp_enddate )
     params['adhoc_desc'] = adhoc_desc
     
     print('Provisional parameters for Eligible automatic run: {}'.format(json.dumps(params,
                                                                                    indent = 4,
                                                                                    sort_keys = True)))
     
   else:
     params['automatic_run'] = False # We don't need to run the automatic job
     print('Provisional parameters for Failsafe Automatic run: {}'.format(json.dumps(params,
                                                                                    indent = 4,
                                                                                    sort_keys = True)))
   
 else:
   # Few assertions need to done as rp_startdate and status are passed only in the custom job run
   rp_startdate = dbutils.widgets.get("rp_startdate")
   assert rp_startdate
   
   status = dbutils.widgets.get("status")
   assert status
   
   # added to be able to add a description for status = 'Adhoc' 

   dbutils.widgets.text("adhoc_desc","","adhoc_desc")
   if dbutils.widgets.get("status") == 'Adhoc':
     adhoc_desc = dbutils.widgets.get("adhoc_desc")
   else:
     adhoc_desc = ""

   print("adhoc_desc: ",adhoc_desc)
   
   # Calculated few needed parameters to run the custom job
   startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")
   rp_enddate = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
   UniqMonthID = calculateUniqMonthID(startdateasdate)
   
   if len(alt_source_data)>0: # check to see if the source database is the standard one or alt_source_data
     #If alt_source_data is populated check the header table for the maximum month id with file types 1 and 2 to calculate prov_month_id and perf_month_id
     prov_month_id = spark.sql("SELECT MAX(UniqMonthId) AS sourceMonthId FROM {0}.mhs000header WHERE FileType = '1'".format(params['db_source'])).collect()[0]['sourceMonthId']
     perf_month_id = spark.sql("SELECT MAX(UniqMonthId) AS sourceMonthId FROM {0}.mhs000header WHERE FileType = '2'".format(params['db_source'])).collect()[0]['sourceMonthId']
   else:
     #If alt_source_data is not populated check, use submission window info to get prov_month_id and perf_month_id
     prov_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id
     perf_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id-1
   print('prov_month_id:',prov_month_id)
   print('perf_month_id:',perf_month_id)
     
   #Compare calculated uniq month id's to see if the correct months are being run for the available data sources
   if status == "Performance":
     compare_monthid(UniqMonthID,perf_month_id)
   elif status == "Provisional":
     compare_monthid(UniqMonthID,prov_month_id)
   elif status == 'Final':
     checkfinalstatus()
   elif status == 'Adhoc':
     print('No time period tests needed for adhoc runs')
   else:
     print('Unknown status')
   
   df = spark.sql("SELECT * FROM {db_output}.{table} WHERE REPORTING_PERIOD_END = '{rp_enddate}' AND STATUS = '{status}'".format(db_output=db,
                                                                                                                                 table='All_products_formatted',
                                                                                                                                 rp_enddate=rp_enddate,
                                                                                                                                 status=status)).collect()
   
   #This code checks to see if there is data in the output tables for the month and status being run and prints a message to say whether data will be overwritten or not
   if len(df) == 0:
     print('Data for REPORTING_PERIOD_END "{rp_enddate}" and STATUS "{status}" not present in {db_output}.{table}'.format(db_output=db,
                                                                                                                          table='All_products_formatted',
                                                                                                                          rp_enddate=rp_enddate,
                                                                                                                          status=status))
   else:
     print('Data for REPORTING_PERIOD_END "{rp_enddate}" and STATUS "{status}" aleady present in {db_output}.{table} and will be overwritten.'.format( db_output=db,
                                                                                                                                                       table='All_products_formatted',
                                                                                                                                                       rp_enddate=rp_enddate,
                                                                                                                                                       status=status))
   
   
   # assign the calculated parameters that are needed for the cutom job run 
   params['custom_run'] = True
   params['rp_enddate'] = rp_enddate
   params['rp_startdate'] = rp_startdate
   params['month_id'] = UniqMonthID
   params['status'] = status
   params['adhoc_desc'] = adhoc_desc
   print('Custom job parameters: {}'.format(json.dumps(params,
                                                       indent = 4,
                                                      sort_keys = True)))
   
   

# COMMAND ----------

# DBTITLE 1,Encapsulated block to log the runs
 %python

 #run the job only if one of the case is true
 if(params['automatic_run'] or params['custom_run']):



   ###############
   # Please Calculate any common parameters in this block that can be used for both custom or automatic run
   # As we are not passing any values if automatic run turns to false, so those might raise exceptions if you are working on those passed values
   ###############



   print("Params passed to notebooks: {}".format(json.dumps(params,
                                                           indent = 4,
                                                            sort_keys = True)))


   print('Running the notebooks/run_notebooks_master notebook')

   dbutils.notebook.run("notebooks/run_notebooks_master", 0, params)


 else:
   print(f'Neither Custom run nor Automatic run conditions are met and the job could not run')


# COMMAND ----------

# DBTITLE 1,Quick glance at the audit table for runs

 %python
 audit_table = spark.sql(f"SELECT * FROM {params['db_output']}.audit_menh_publications ORDER BY RUN_START DESC LIMIT 5")
 display(audit_table)