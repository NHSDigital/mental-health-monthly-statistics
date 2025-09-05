# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

'''this code sets up the widgets (boxes) at the top of this worksheet to mimic the inputs added at runtime
the code does not need to run each time as once the widgets exist they will not be recreated.
Code is left here for easy copying to other notebooks!  or for cases where widgets have been created with wrong names or labels
dbutils.widgets.removeAll() above can be run to annihilate the existing widgets and then run these to create new ones'''

# dbutils.widgets.text("db", "menh_bbrb", "Target database")
# dbutils.widgets.text("mhsds_database", "testdata_menh_bbrb_mhsds_database", "Input database")
# dbutils.widgets.text("status", "Performance", "status")
# dbutils.widgets.text("reference_data", "reference_data", "reference_data")
# dbutils.widgets.text(name='rp_startdate', defaultValue='2021-10-01', label='Reporting period start date')
# dbutils.widgets.text("product","","product")

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

 # It can  be auto run if both fields are not available as in automatic run (means those widges are not present)
 auto_prov_check = False if (is_rp_start_avail and is_status_avail) else True
 print(f'Provisional check for automatic run: {auto_prov_check}')  

# COMMAND ----------

# DBTITLE 1,Checking and Assigning Parameters
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json


# the target database for a cp project is the same as the name of the cp project and the parameter is always named db
# here this is renamed to the standard db_output for familiarity/consistency with other projects
#dbutils.widgets.text("db", "", "Target database")
db_output = dbutils.widgets.get("db")
assert db_output

# the parameter name of any source database(s) for a cp project is the same as the database name, i.e. the mhsds_database parameter is named mhsds_database!
# here this is renamed to the standard db_source for familiarity/consistency with other projects
try:
  db_source = dbutils.widgets.get("mhsds_database")
except:
  print('mhsds_database is not defined')
  
try:
  db_source = dbutils.widgets.get("mhsds_database")
except:
  print('mhsds_database is not defined')

###dbutils.widgets.text("reference_data","reference_data","Source Ref Database")
reference_data = dbutils.widgets.get("reference_data")
assert reference_data

####
# here a parameter is defined and assigned no value - this makes it an optional parameter - it doesn't need to be added at run time because it will be added here with no value if it doesn't already exist.
# this avoids the following error which you'd see with the params above if you forgot to add them at runtime
# InputWidgetNotDefined: No input widget named product is defined
# a single product can be run by adding this to the parameter list at run time

dbutils.widgets.text("product","","product")

if dbutils.widgets.get("product") == '':
  product_name = "ALL"
else:
  product_name = dbutils.widgets.get("product")


# Basic parameters that can be passed

params = {
  'db_source' : db_source, 
  'db_output' : db_output, 
  'reference_data': reference_data,
  'rp_enddate' : '', 
  'rp_startdate' : '', 
  'month_id' : '', 
  'status': '', 
  'product' : product_name,
  'custom_run': False,  #Defaulting to false for job run
  'automatic_run' : False # Defaulting to false
}

'''
we have two boolean parameters becasue they are not complimentary:
Custom - true , means obviously we fed the rp_startdate and status to run
automatic - true, custom - false, means there is data available at source for provisional and there is no job ran recently.
automatic - false, custom - false, there is nothing to do, if we kept complimentary this case would be off

'''

print('Basic parameters: {}'.format(json.dumps(params,
                                              indent = 4,
                                              sort_keys = True)))
'''
Function to calculate month id for the custom run, we can use the submission calendar too but its date range is limited and this function block is already been there to calculate month id for custom run, so been using it.
'''


# month_id calculation for custom runs
def calculatemonthid(RPStartDate: datetime)-> int:
  date_for_month = RPStartDate
  print(f'Report period start date :{RPStartDate}')
  start_date = datetime(1900, 4, 1)
  time_diff = relativedelta(date_for_month, start_date)
  return time_diff.years * 12 + time_diff.months + 1

def compare_monthid(calcmonthid,inputmonthid):
  #This function compares 2 input monthid's to see if the correct data is available. This is for Performance and Provisional data
  msg = (f"""Data in {db_source} does not match input parameters for {status}: Parameter UniqMonthID = {calcmonthid}, Data UniqMonthID = {inputmonthid}.
  The run has been cancelled. Please correct widgets with valid values for the data present in {db_source} and rerun.""")
  
  assert calcmonthid == inputmonthid, msg 

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

  final_file_type = spark.sql(f"SELECT MAX(FileType) AS MaxFileType FROM {db_source}.mhs000header WHERE UniqMonthID = {final_month_id}").collect()[0]['MaxFileType']
  
  print('final_file_type:',final_file_type)
  
  assert final_file_type==2, f'Final data not available in {db_source} for {financialyear}'

# COMMAND ----------

# DBTITLE 1,Parameter building for the job run
from shared.constants import DS
# from shared.submissions.calendars import mhsds_ytd, submission_calendar # This is V4 calendar
from shared.submissions.calendars import mhsds_v6, submission_calendar
from datetime import datetime, date

#  Determine standard run months>>>>>>>>
_mhsds_ytd_calendar = submission_calendar(DS.MHSDS_V6, {})


today_date = datetime.today()
submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
    today_date, fake_historic_windows=False,
)
idx_current_report_month = len(submission_window.reporting_periods) - 1

if(auto_prov_check): ##use this line for prod

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
  
  #Define adhoc_desc as blank as automatic run will never be an adhoc run
  adhoc_desc = ""
  
  #### CHECK THE RECENT MONTH ID OF SUCCESSFUL RUN IN audit_menh_bbrb TABLE######  
  audit_month_id = spark.sql("SELECT MAX(MONTH_ID) AS auditMonthId FROM {0}.audit_menh_bbrb WHERE RUN_END IS NOT NULL".format(params['db_output'])).collect()[0]['auditMonthId']; 
  print(f'Audit month ID from recent job runs: {audit_month_id}')
#   audit_month_id = 1461
  source_month_id = spark.sql("SELECT MAX(UniqMonthId) AS sourceMonthId FROM {0}.mhs000header".format(db_source)).collect()[0]['sourceMonthId']; 

#   source_month_id = 1462
  print(f'Recent month id available at source database: {source_month_id}')
  
  ### CONDITION TO CHECK WHETHER THERE IS SUCCESSFUL RUN FOR PROVISIONAL MONTH AND DATA AVAILABLE FOR PROVISIONAL IN SOURCE DB  
  if((prov_month_id > audit_month_id) and (prov_month_id == source_month_id)):###use for prod
  ##if(True):
    params['automatic_run'] = True # YES, WE CAN RUN THE JOB THAT PICKS UP THE PROVISIONAL DATA
    params['rp_enddate'] = str(submission_window.reporting_periods[idx_current_report_month].end)
    params['rp_startdate'] = str(submission_window.reporting_periods[idx_current_report_month].start)
    params['month_id'] = prov_month_id
    startdateasdate = datetime.strptime(params['rp_startdate'], "%Y-%m-%d")##(need to check this line)
#     month_period = startdateasdate.strftime('%B')[:3] + '-' + params['rp_enddate'][:4][-2:] # can be removed if not found to be used in the calling notebooks
#     params['MonthPeriod'] = month_period   # moving this down to run_notebooks_master
    params['status'] = 'Provisional'  # Defaulting to Provisional run as it would be run first in the automatic block
    
    idx_perf_report_month = idx_current_report_month - 1 # reducing by one to get the performance month index
    perf_month_id = submission_window.reporting_periods[idx_perf_report_month].unique_month_id 
    perf_rp_startdate = submission_window.reporting_periods[idx_perf_report_month].start
    perf_rp_enddate = submission_window.reporting_periods[idx_perf_report_month].end
    params['perf_month_id'] = str(perf_month_id)
    params['perf_rp_startdate'] = str(perf_rp_startdate)
    params['perf_rp_enddate'] = str(perf_rp_enddate)
    params['adhoc_desc'] = adhoc_desc
    # Not calculating month period as it is not found to be used anywhere in the calling notebooks    
    

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
  
  #Check to see if status is Adhoc and if so get adhoc_desc from widget else set to blank
  dbutils.widgets.text("adhoc_desc","","adhoc_desc")
  if dbutils.widgets.get("status") == 'Adhoc':
    adhoc_desc = dbutils.widgets.get("adhoc_desc")
  else:
    adhoc_desc = ""

  print("adhoc_desc: ",adhoc_desc)
  
  # Calculated few needed parameters to run the custom job
  startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")
  rp_enddate = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
  UniqMonthID = calculatemonthid(startdateasdate)
  print("UniqMonthID:",UniqMonthID)
  
  month_period = startdateasdate.strftime('%B')[:3] + '-' + rp_enddate[:4][-2:] # This value is used in the DQMI extracts books
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
  
  table = "bbrb_final"
  
  check_these = ("Performance", "Provisional", "Final")
  
  if status in check_these:
  
    df = spark.sql(f"SELECT * FROM {db_output}.{table} WHERE REPORTING_PERIOD_END = '{rp_enddate}' AND STATUS = '{status}'").collect()
  
  #This code checks to see if there is data in the output tables for the month and status being run and prints a message to say whether data will be overwritten or not
    if len(df) == 0:
      print(f'Data for REPORTING_PERIOD_END "{rp_enddate}" and STATUS "{status}" not present in {db_output}.{table}')
    else:
      print(f'Data for REPORTING_PERIOD_END "{rp_enddate}" and STATUS "{status}" aleady present in {db_output}.{table} and will be overwritten.')
  
  # assign the calculated parameters that are needed for the custom job run 
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

# DBTITLE 1,Need to know how many job loops should run
jobs = [params['status']] # default adding one one job as it is passed from the status. Example 'Provisional' for automatic run, sometimes 'Final' for custom run
if(params['automatic_run']):
  jobs.append('Performance') # Only for automatic run we need to add this
  print('We are going for Automatic run now which runs First for Provisional and then Performance')

# COMMAND ----------

# DBTITLE 1,Populate Audit Table
# menh_bbrb: Populate audit table
def auditInsertLog(params, runStart, runEnd, notes):
  # MARKS THE LOG WITH Auto FOR RUNs ON SCHEUDLED BASIS
  status_log = 'Auto ' + params['status'] if (params['automatic_run']) else params['status']
  try:
    db_output =  params['db_output']
    month_id = params['month_id']
    status = status_log
    rp_startdate = params['rp_startdate']
    rp_enddate = params['rp_enddate']
    
    spark.sql(f"""
    INSERT INTO TABLE {db_output}.audit_menh_bbrb VALUES
    ('{month_id}','{status}','{rp_startdate}','{rp_enddate}','{db_source}',FROM_UNIXTIME({runStart}),FROM_UNIXTIME({runEnd}),'{notes}')
    """)
  except Exception as ex:
    print(ex, ' failed to log to audit_menh_bbrb')

# COMMAND ----------

# DBTITLE 1,Extracts of job
 %python

 from datetime import datetime
 import time

 try:
   if(params['automatic_run'] or params['custom_run']):
     for run in jobs:
       now = datetime.now()
       runStart = datetime.timestamp(now)
       print(f'Started the job at: {datetime.time(now)}')
       
       
 ############################## Some manipulation to pass the performance parameters from the submission calendar #############################################
 # Assuming run made it upto here means, we have supplied Provisional parameters too. so we can access them now, if it is custom run those parameters are not introduced in the params section
 # As the Primary run completed with values rp_startdate, rp_enddate, Unique_MonthID. Now we need to assign the same parameter keys with Refresh values as the same parameter names are being passed in the notebooks

       if(params['automatic_run'] and run == 'Performance'):
         params['rp_startdate'] = params['perf_rp_startdate'] 
         params['rp_enddate'] = params['perf_rp_enddate'] 
         params['month_id'] = params['perf_month_id'] 
         params['status'] = run
         
       DateTimeRunStart = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
       params['runTimestamp'] = DateTimeRunStart
       print("Params for {0} run are: {1}".format(run,
                                                 json.dumps(params,
                                                             indent = 4,
                                                           sort_keys = True)))
       notes = dbutils.notebook.run("./notebooks/run_notebooks_master", 0, params)
       dbutils.notebook.run("./notebooks/03_Extract/menh_bbrb_extract", 0, params)
       ##print(f'menh_bbrb_extract\n')
       print(f"output",params) 
         ####    
       now = datetime.now()
       runEnd = datetime.timestamp(now)
       print(f'Ended the job at: {datetime.time(now)}')
     #     calling the log functions if the job is success
       print('logging the successful run')
       auditInsertLog(params,runStart,runEnd, notes)
   else:
     print(f'Neither Custom run nor Automatic run conditions are met and the job could not run')
 except Exception as ex:
   print(ex, ' Job run failed and logging to audit_menh_bbrb')
 #     calling the log function as the job failed with empty end date which will record as null in table
   auditInsertLog(params,runStart,'NULL')
   # using exception block escapes the show of'Failure' tag for Code Promotion notebooks even if there is an error in the notebooks.(Instead it shows as 'Success' even in case of errors and very misleading). So, need to raise the error here after logging to audit table as it is the entry point
   ## DONOT USE TRY-EXCEPt BLOCKS ANYWHERE INSIDE THE CALLING NOTEBOOKS
   assert False

# COMMAND ----------

# DBTITLE 1,cp email functionality
if(os.environ.get('env') == 'prod'):
  from dsp.code_promotion.email_send import cp_email_send
  template_name = 'menh_bbrb_reports.json' # the template name must be the full name and include the .json extension
  recipients = "gillian.birk-telford@nhs.net|obiageli.chiwetalu1@nhs.net".split("|") # an iterable list of recipients to send the email to

  content_data = {
    "reporting_period": "menh_bbrb data is available for"

  } # a dictionary of variables to pass to the template

  try:
    cp_email_send(
        spark=spark,
        template_name=template_name,
        recipients=recipients,
        content_data=content_data,
    )
  except Exception as e:
    error(
        "The email sender failed.",
        template_name=template_name,
        recipients=recipients,
        content_data=content_data,
        error=str(e),
    )

# COMMAND ----------

# ###Parameter building for the job run#######
# %python
# from shared.constants import DS
# # from shared.submissions.calendars import mhsds_ytd, submission_calendar # This is V4 calendar
# from shared.submissions.calendars import mhsds_v5, submission_calendar
# from datetime import datetime, date

# # UKD 15/03/2022 BITC-3070 menh_dq: Determine standard run months>>>>>>>>

# if(auto_prov_check):

#   # As the provisional check is passed, we need to get the month id, reporting periods for the provisional months of current date

# #   _mhsds_ytd_calendar = submission_calendar(DS.MHSDS_YTD, {}) #this is V4 call
#   _mhsds_ytd_calendar = submission_calendar(DS.MHSDS_V5, {})


#   today_date = datetime.today()
#   print('Validity check for the date: {0}\n'.format( today_date))
#   submission_window = _mhsds_ytd_calendar.find_last_closed_submission_window(
#       today_date, fake_historic_windows=False,
#   )
#   idx_current_report_month = len(submission_window.reporting_periods) - 1
#   prov_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id

#   print(f"\
#           length of period tuple from Submission Calendar: {len(submission_window.reporting_periods)}\n\
#           Submission window opens: {submission_window.opens}\n\
#           Submission window closes: {submission_window.closes}\n\
#           Provisional period start: {submission_window.reporting_periods[idx_current_report_month].start}\n\
#           Provisional period end: {submission_window.reporting_periods[idx_current_report_month].end}\n\
#           Provisional period month id: {prov_month_id}\n\
#           ")

#   #### CHECK THE RECENT MONTH ID OF SUCCESSFUL RUN IN audit_menh_dq TABLE
#   audit_month_id = spark.sql("SELECT MAX(MONTH_ID) AS auditMonthId FROM {0}.audit_menh_bbrb WHERE RUN_END IS NOT NULL".format(params['db_output'])).collect()[0]['auditMonthId']; 
#   print(f'Audit month ID from recent job runs: {audit_month_id}')
# #   audit_month_id = 1461
#   source_month_id = spark.sql("SELECT MAX(UniqMonthId) AS sourceMonthId FROM {0}.mhs000header".format(params['db_source'])).collect()[0]['sourceMonthId']; 

# #   source_month_id = 1462
#   print(f'Recent month id available at source database: {source_month_id}')
#   ### CONDITION TO CHECK WHETHER THERE IS SUCCESSFUL RUN FOR PROVISIONAL MONTH AND DATA AVAILABLE FOR PROVISIONAL IN SOURCE DB
#   if((prov_month_id > audit_month_id) and (prov_month_id == source_month_id )):
# #   if(True):
#     params['automatic_run'] = True # YES, WE CAN RUN THE JOB THAT PICKS UP THE PROVISIONAL DATA
#     params['rp_enddate'] = str(submission_window.reporting_periods[idx_current_report_month].end)
#     params['rp_startdate'] = str(submission_window.reporting_periods[idx_current_report_month].start)
#     params['month_id'] = prov_month_id
#     params['status'] = 'Provisional'  # Defaultig to Provisional run as it would be run first in the automatic block

#     idx_perf_report_month = idx_current_report_month - 1 # reducing by one to get the performance month index
#     perf_month_id = submission_window.reporting_periods[idx_perf_report_month].unique_month_id 
#     perf_rp_startdate = submission_window.reporting_periods[idx_perf_report_month].start
#     perf_rp_enddate = submission_window.reporting_periods[idx_perf_report_month].end
#     params['perf_month_id'] = str(perf_month_id)
#     params['perf_rp_startdate'] = str(perf_rp_startdate)
#     params['perf_rp_enddate'] = str(perf_rp_enddate )




#     print('Provisional parameters for Eligible automatic run: {}'.format(json.dumps(params,
#                                                                                    indent = 4,
#                                                                                    sort_keys = True)))

#   else:
#     params['automatic_run'] = False # We don't need to run the automatic job
#     print('Provisional parameters for Failsafe Automatic run: {}'.format(json.dumps(params,
#                                                                                    indent = 4,
#                                                                                    sort_keys = True)))

# else:
#   # Few assertions need to done as rp_startdate and status are passed only in the custom job run
#   rp_startdate = dbutils.widgets.get("rp_startdate")
#   assert rp_startdate

#   status = dbutils.widgets.get("status")
#   assert status
#   # Calculated few needed parameters to run the custom job
#   startdateasdate = datetime.strptime(rp_startdate, "%Y-%m-%d")
#   rp_enddate = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
#   UniqMonthID = calculateUniqMonthID(startdateasdate)

#   # assign the calculated parameters that are needed for the cutom job run 
#   params['custom_run'] = True
#   params['rp_enddate'] = rp_enddate
#   params['rp_startdate'] = rp_startdate
#   params['month_id'] = UniqMonthID
#   params['status'] = status
#   print('Custom job parameters: {}'.format(json.dumps(params,
#                                                       indent = 4,
#                                                      sort_keys = True)))
  
  

# COMMAND ----------

# %python

# # this is the automated run code (where widget parameters NOT filled in)
# from datetime import datetime, date
# from dateutil.relativedelta import relativedelta
# import time
# import os
# import json
# #print(f"Data type of custom_run value : {type(params['custom_run'])}")

#   # (advised) To Keep different notebooks rather than single note book with multiple if conditions on statuses and quarterly 
#   # flag to ease the code writing and spotting based on statuses. 
#   # NB, you can't run the report section out of this 'try' block as we are assigning the performance parameters during a second loop. (Old code used to call on the params rp_startdate, rp_enddate, month_id names so used the same names to call for performance)
#   # This case should only run in prod, it can't work in ref and it hinders with job fail if not kept under IF condition
# ##if(os.environ.get('env') == 'prod'):
# #if(os.environ.get('env') == 'ref'):
#   #       dbutils.notebook.run("Publication_csvs", 0, params); # old one when reports are all in single book
#   #       print(f'Publication_csvs run complete\n')

#   #      Performance and Provisional (sometimes 'Final') reports, This report runs in all possible cases (both automatic and custom run so no need of if checks)

# ####
#   ####dbutils.notebook.run("03_Extract/menh_bbrb_extract", 0, params);
# dbutils.notebook.run("./notebooks/03_Extract/menh_bbrb_extract", 0, params )
# print(f'menh_bbrb_extract\n')
    
# #else:
#   #mh_run_params.as_dict()
#   #mh_run_params.as_dict()
#   #print(f'menh_bbrb_extract run complete\n')

#         #if(params['is_quarter']):
#           #dbutils.notebook.run("03_Extract/04_menh_analysis_csvs_quarter", 0, params); # running 