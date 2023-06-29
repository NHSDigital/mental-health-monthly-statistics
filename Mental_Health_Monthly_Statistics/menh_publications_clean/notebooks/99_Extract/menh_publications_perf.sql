-- Databricks notebook source
%python
import os

from dsp.code_promotion.s3_send import cp_s3_send
from dsp.code_promotion.mesh_send import cp_mesh_send
# dbutils.widgets.removeAll()

-- COMMAND ----------

-- DBTITLE 1,Create widgets
%python

rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output") 
db_source = dbutils.widgets.get("db_source")  
month_id = dbutils.widgets.get("month_id")



print(f'rp_startdate is {rp_startdate}; \
      rp_enddate is {rp_enddate}; \
      status is {status}; \
      db_output is {db_output}; \
      db_source is {db_source}; \
      month_id is {month_id}')


# import functions
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dsp.common.exports import create_csv_for_download


if len(status) == 5:
  shortstatus = status
else:
  shortstatus = status[:4]
YYYY = rp_startdate[:4]
Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

file_part_name = f"_{Mname}{shortstatus}_{YYYY}" # without csv extension

if db_source == "menh_point_in_time":
  file_part_name = f"_pit{file_part_name}"
  
print(f'Second part of file name: {file_part_name}')

#Prod mail box id
mailbox_to = 'X26HC004'
workflow_id = 'GNASH_MHSDS'

-- COMMAND ----------

-- DBTITLE 1,Fabricated param to try the code locally, Uncomment when necessary for testing
%python
# month_id = "1449"
# status = "Performance"
# rp_enddate =  "2020-12-31"
# rp_startdate = "2020-12-01"
# database =  "database"


# db_output = 'menh_publications' #dbutils.widgets.get("menh_publications") #----------
# db_source = '$db_source' #dbutils.widgets.get("$db_source") #-----------



# print(f'rp_startdate is {rp_startdate}; \
#       rp_enddate is {rp_enddate}; \
#       status is {status}; \
#       db_output is {db_output}; \
#       db_source is {db_source}; \
#       month_id is {month_id}')


# # import functions
# from datetime import datetime, date
# from dateutil.relativedelta import relativedelta
# from dsp.common.exports import create_csv_for_download


# shortstatus = status[:4]
# YYYY = rp_startdate[:4]
# Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

# file_part_name = f"_{Mname}_{YYYY}_{shortstatus}" # without csv extension
# print(f'Second part of file name: {file_part_name}')

-- COMMAND ----------

-- DBTITLE 1,Only Performance reports - ASCOF RAW for MHSDS Publication Team
%python

# Updated to reflect the required outputs for MH and Social Care publications

local_id = str(datetime.now().date()) +'-menh_publications' # Given date combo with project name
print(f'Second part of file name: {file_part_name}')

# RAW unrounded file
df_ascof_monthly_raw_csv = spark.sql("SELECT \
                                  REPORTING_PERIOD_START, \
                                  REPORTING_PERIOD_END, \
                                  STATUS, \
                                  BREAKDOWN, \
                                  PRIMARY_LEVEL, \
                                  PRIMARY_LEVEL_DESCRIPTION, \
                                  SECONDARY_LEVEL, \
                                  SECONDARY_LEVEL_DESCRIPTION, \
                                  THIRD_LEVEL, \
                                  METRIC, \
                                  METRIC_VALUE \
                              from {db_output}.ascof_unformatted \
                              WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                              AND STATUS = '{status}' \
                              AND SOURCE_DB = '{db_source}'  \
                              ORDER BY  \
                              REPORTING_PERIOD_START, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, THIRD_LEVEL, METRIC".format(db_output = db_output,
                                                                                                                                              rp_enddate = rp_enddate,
                                                                                                                                              status = status,
                                                                                                                                              db_source = db_source))

#to help with local testing and avoiding the commenting and uncommenting the code
if(os.environ.get('env') == 'prod'):
  ascof_monthly_raw_csv = f'Monthly_FYFV_File{file_part_name}_RAW.csv' 

  try:
#    request_id = cp_mesh_send(spark, df_ascof_monthly_raw_csv, mailbox_to, workflow_id, #ascof_monthly_raw_csv, local_id)
    request_id = cp_mesh_send(spark, df_ascof_monthly_raw_csv, mailbox_to, workflow_id, ascof_monthly_raw_csv, ascof_monthly_raw_csv) # replacing local_id with filename for new renaming)
    print(f"{ascof_monthly_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
else: 
  display(df_ascof_monthly_raw_csv)

-- COMMAND ----------

-- DBTITLE 1,Only Performance reports - ASCOF - rounded - for MHSDS Publication Team
%python

# Updated to reflect the required outputs for MH and Social Care publications

local_id = str(datetime.now().date()) +'-menh_publications' # Given date combo with project name
print(f'Second part of file name: {file_part_name}')

# RAW unrounded file
df_ascof_monthly_csv = spark.sql("SELECT \
                                  REPORTING_PERIOD_START, \
                                  REPORTING_PERIOD_END, \
                                  STATUS, \
                                  BREAKDOWN, \
                                  PRIMARY_LEVEL, \
                                  PRIMARY_LEVEL_DESCRIPTION, \
                                  SECONDARY_LEVEL, \
                                  SECONDARY_LEVEL_DESCRIPTION, \
                                  THIRD_LEVEL, \
                                  MEASURE_ID, \
                                  MEASURE_NAME, \
                                  MEASURE_VALUE \
                              from {db_output}.third_level_products_formatted \
                              WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                              AND STATUS = '{status}' \
                              AND SOURCE_DB = '{db_source}'  \
                              ORDER BY  \
                              REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(db_output = db_output,
                                                                                                                                              rp_enddate = rp_enddate,
                                                                                                                                              status = status,
                                                                                                                                              db_source = db_source))

#to help with local testing and avoiding the commenting and uncommenting the code
if(os.environ.get('env') == 'prod'):
  ascof_monthly_csv = f'Monthly_FYFV_File{file_part_name}.csv' 

  try:
 #   request_id = cp_mesh_send(spark, df_ascof_monthly_csv, mailbox_to, workflow_id, #ascof_monthly_csv, local_id)
    request_id = cp_mesh_send(spark, df_ascof_monthly_csv, mailbox_to, workflow_id, ascof_monthly_csv, ascof_monthly_csv)
    # replacing local_id with filename for new renaming)
    print(f"{ascof_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
else: 
  display(df_ascof_monthly_csv)

-- COMMAND ----------

-- DBTITLE 1,Only Performance reports - ASCOF - rounded - for Social Care
%python

# Updated to reflect the required outputs for MH and Social Care publications

local_id = str(datetime.now().date()) +'-menh_publications' # Given date combo with project name
print(f'Second part of file name: {file_part_name}')

# RAW unrounded file
df_ascof_monthly_csv = spark.sql("SELECT \
                                  REPORTING_PERIOD, \
                                  STATUS, \
                                  BREAKDOWN, \
                                  LEVEL_ONE, \
                                  LEVEL_ONE_DESCRIPTION, \
                                  LEVEL_TWO, \
                                  LEVEL_TWO_DESCRIPTION, \
                                  LEVEL_THREE, \
                                  METRIC, \
                                  METRIC_VALUE \
                              from global_temp.ascof_formatted \
                              ORDER BY  \
                              REPORTING_PERIOD, STATUS, BREAKDOWN, LEVEL_ONE, LEVEL_TWO, LEVEL_THREE, METRIC")

#to help with local testing and avoiding the commenting and uncommenting the code
if(os.environ.get('env') == 'prod'):
  ascof_monthly_csv = f'Monthly_ASCOF_File{file_part_name}.csv' 

  try:
#    request_id = cp_mesh_send(spark, df_ascof_monthly_csv, mailbox_to, workflow_id, #ascof_monthly_csv, local_id)
    request_id = cp_mesh_send(spark, df_ascof_monthly_csv, mailbox_to, workflow_id, ascof_monthly_csv, ascof_monthly_csv)
    print(f"{ascof_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
else: 
  display(df_ascof_monthly_csv)

-- COMMAND ----------

