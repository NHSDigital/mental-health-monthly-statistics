-- Databricks notebook source
%python
import os

from dsp.code_promotion.s3_send import cp_s3_send
from dsp.code_promotion.mesh_send import cp_mesh_send

-- COMMAND ----------

-- DBTITLE 1,Create widgets
%python

rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output") # 'menh_publications' #dbutils.widgets.get("menh_publications") #----------
month_id = dbutils.widgets.get("month_id")
db_source = dbutils.widgets.get("db_source")



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
# $reference_data =  "$reference_data"


# db_output1 = 'menh_publications' #dbutils.widgets.get("menh_publications") #----------
# menh_publications_source = '$mhsds_database' #dbutils.widgets.get("$mhsds_database") #-----------



# print(f'rp_startdate is {rp_startdate}; \
#       rp_enddate is {rp_enddate}; \
#       status is {status}; \
#       db_output1 is {db_output1}; \
#       menh_publications_source is {menh_publications_source}; \
#       month_id is {month_id}')


# # import functions
# from datetime import datetime, date
# from dateutil.relativedelta import relativedelta
# from dsp.common.exports import create_csv_for_download


# shortstatus = status[:4]
# YYYY = rp_startdate[:4]
# Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

# file_part_name = f"_{Mname}{shortstatus}_{YYYY}" # without csv extension
# print(f'Second part of file name: {file_part_name}')

-- COMMAND ----------

-- DBTITLE 1,Reports for Performance and Provisional
%python
local_id = str(datetime.now().date()) +'-menh_pub' # Confluence doesn't specify which id to pass as it says 'user specified id'. So given date combo with project name
print(f'Second part of file name: {file_part_name}')

##############################################################
#              Extract the CSV of most products (MHSDS Data)  ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
##############################################################
df_mhsds_monthly_csv = spark.sql("SELECT DISTINCT \
                                  REPORTING_PERIOD_START, \
                                  REPORTING_PERIOD_END, \
                                  STATUS, \
                                  BREAKDOWN, \
                                  PRIMARY_LEVEL, \
                                  PRIMARY_LEVEL_DESCRIPTION, \
                                  SECONDARY_LEVEL, \
                                  SECONDARY_LEVEL_DESCRIPTION, \
                                  MEASURE_ID, \
                                  MEASURE_NAME, \
                                  MEASURE_VALUE \
                              FROM {db_output}.All_products_formatted a \
                              WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                              AND STATUS = '{status}' \
                              And SOURCE_DB = '{db_source}' \
                              AND PRODUCT_NO NOT IN (6,8) \
                              ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(rp_enddate = rp_enddate,
                                                                                                                                                          status = status,
                                                                                                                                                          db_output = db_output,
                                                                                                                                                          db_source = db_source))

#to help with local testing and avoiding the commenting and uncommenting the code
if(os.environ.get('env') == 'prod'):
  mhsds_monthly_csv = f'MHSDS Data{file_part_name}_publications.csv' 
#   request_id = cp_s3_send(spark, df_mhsds_monthly_csv, mhsds_monthly_csv, local_id)
  try:
    request_id = cp_mesh_send(spark, df_mhsds_monthly_csv, mailbox_to, workflow_id, mhsds_monthly_csv, mhsds_monthly_csv)
    print(f"{mhsds_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
else:
  display(df_mhsds_monthly_csv)

##############################################################
#              Extract the CSV of most products - UNROUNDED ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
##############################################################
df_mhsds_monthly_raw_csv = spark.sql("SELECT DISTINCT \
                                      REPORTING_PERIOD_START, \
                                      REPORTING_PERIOD_END, \
                                      STATUS, \
                                      BREAKDOWN, \
                                      COALESCE(PRIMARY_LEVEL, \"UNKNOWN\") as PRIMARY_LEVEL, \
                                      PRIMARY_LEVEL_DESCRIPTION, \
                                      SECONDARY_LEVEL, \
                                      SECONDARY_LEVEL_DESCRIPTION, \
                                      METRIC as MEASURE_ID, \
                                      METRIC_VALUE as MEASURE_VALUE \
                                  FROM {db_output}.main_monthly_unformatted_new a \
                                  WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                                  AND STATUS = '{status}' \
                                  And SOURCE_DB = '{db_source}' \
                                  UNION ALL \
                                  SELECT DISTINCT \
                                      REPORTING_PERIOD_START, \
                                      REPORTING_PERIOD_END, \
                                      STATUS, \
                                      BREAKDOWN, \
                                      COALESCE(PRIMARY_LEVEL, \"UNKNOWN\") as PRIMARY_LEVEL, \
                                      PRIMARY_LEVEL_DESCRIPTION, \
                                      SECONDARY_LEVEL, \
                                      SECONDARY_LEVEL_DESCRIPTION, \
                                      METRIC as MEASURE_ID, \
                                     METRIC_VALUE as MEASURE_VALUE \
                                  FROM {db_output}.cyp_ed_wt_unformatted \
                                  WHERE  \
                                  REPORTING_PERIOD_END = '{rp_enddate}'  \
                                  AND STATUS = '{status}' \
                                  And SOURCE_DB = '{db_source}' \
                                   UNION ALL \
                                  SELECT DISTINCT \
                                      REPORTING_PERIOD_START, \
                                      REPORTING_PERIOD_END, \
                                      STATUS, \
                                      BREAKDOWN, \
                                      COALESCE(PRIMARY_LEVEL, \"UNKNOWN\") as PRIMARY_LEVEL, \
                                      PRIMARY_LEVEL_DESCRIPTION, \
                                      SECONDARY_LEVEL, \
                                      SECONDARY_LEVEL_DESCRIPTION, \
                                      METRIC as MEASURE_ID, \
                                      METRIC_VALUE as MEASURE_VALUE \
                                  FROM {db_output}.mha_monthly_unformatted \
                                  WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                                  AND STATUS = '{status}'  \
                                  AND SOURCE_DB = '{db_source}'  \
                                      UNION ALL \
                                  SELECT DISTINCT \
                                      REPORTING_PERIOD_START, \
                                      REPORTING_PERIOD_END, \
                                      STATUS, \
                                      BREAKDOWN, \
                                      COALESCE(LEVEL, \"UNKNOWN\") as PRIMARY_LEVEL, \
                                      LEVEL_DESCRIPTION, \
                                      SECONDARY_LEVEL, \
                                      SECONDARY_LEVEL_DESCRIPTION, \
                                      METRIC as MEASURE_ID, \
                                     METRIC_VALUE as MEASURE_VALUE \
                                  FROM {db_output}.awt_unformatted \
                                  WHERE  \
                                  REPORTING_PERIOD_END = '{rp_enddate}'  \
                                  AND STATUS = '{status}' \
                                  And SOURCE_DB = '{db_source}' \
                                  ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(rp_enddate = rp_enddate,
                                                                                                                                                          status = status,
                                                                                                                                                          db_output = db_output,
                                                                                                                                                          db_source = db_source))

#to help with local testing and avoiding the commenting and uncommenting the code
if(os.environ.get('env') == 'prod'):
  mhsds_monthly_raw_csv = f'MHSDS Data{file_part_name}_RAW_publications.csv' 
#   request_id = cp_s3_send(spark, df_mhsds_monthly_raw_csv, mhsds_monthly_raw_csv, local_id)
  try:
    request_id = cp_mesh_send(spark, df_mhsds_monthly_raw_csv, mailbox_to, workflow_id, mhsds_monthly_raw_csv,mhsds_monthly_raw_csv) # replacing local_id with filename for new renaming
    print(f"{mhsds_monthly_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
else:
  display(df_mhsds_monthly_raw_csv)