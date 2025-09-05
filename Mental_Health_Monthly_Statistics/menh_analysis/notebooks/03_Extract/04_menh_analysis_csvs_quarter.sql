-- Databricks notebook source
 %python
 import os


 # import functions
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 from dsp.common.exports import create_csv_for_download

 # from dsp.code_promotion.s3_send import cp_s3_send
 from dsp.code_promotion.mesh_send import cp_mesh_send
 # dbutils.widgets.removeAll()

-- COMMAND ----------

-- DBTITLE 1,Create widgets
 %python

 # MonthPeriod = dbutils.widgets.get("MonthPeriod") 
 db_output = dbutils.widgets.get("db_output")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 status = dbutils.widgets.get("status")
 db_source = dbutils.widgets.get("db_source") #dbutils.widgets.get("mhsds_database") #----------
 # db_output1 = 'menh_publications' #dbutils.widgets.get("menh_publications") #----------
 # menh_publications_source = 'mhsds_database' #dbutils.widgets.get("mhsds_database") #-----------
 month_id = dbutils.widgets.get("month_id")



 print(f'db_output is {db_output}; \
       rp_startdate is {rp_startdate}; \
       rp_enddate is {rp_enddate}; \
       status is {status}; \
       db_source is {db_source}; \
       month_id is {month_id}')


 if len(status) == 5: # this will spit out Final and Adhoc as they are
   shortstatus = status
 else:
   shortstatus = status[:4] # this will shorten Provisional and Performance to the first 4 letters
   
 YYYY = rp_startdate[:4]
 Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

 file_part_name = f"_{Mname}{shortstatus}_{YYYY}" # without csv extension

 if db_source == "menh_point_in_time":
   file_part_name = f"_pit_{Mname}{shortstatus}_{YYYY}"

 print(f'Second part of file name: {file_part_name}')

 #Prod mail box id
 mailbox_to = 'X26HC004'
 workflow_id = 'GNASH_MHSDS'
 # local_id = str(datetime.now().date()) +'-menh_analysis' # Confluence doesn't specify which id to pass as it says 'user specified id'. So given date combo with project name # updated to be filename for new LEAD_MESH renaming process


-- COMMAND ----------

-- DBTITLE 1,Fabricated param to try the code locally, Uncomment when necessary for testing
 %python
 # db_output =  "menh_analysis"
 # month_id = "1449"
 # status = "Performance"
 # rp_enddate =  "2020-12-31"
 # rp_startdate = "2020-12-01"
 # reference_data =  "reference_data"


 # db_source = "mhsds_database"

 # print(f'db_output is {db_output}; \
 #       rp_startdate is {rp_startdate}; \
 #       rp_enddate is {rp_enddate}; \
 #       status is {status}; \
 #       db_source is {db_source}; \
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

 %md

 - extracts below this point are only needed for quarters 

 - i.e. Performance/Final June, September, December and March

-- COMMAND ----------

-- DBTITLE 1,Quarterly Performance reports

 %python

 ##################################################################################################
 #####################    Extract the csv for FYFV

 ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
 ##################################################################################################
 df_fyfv_quarterly_csv = spark.sql("SELECT DISTINCT \
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
                               FROM {db_output}.All_products_formatted \
                               WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                               AND STATUS = '{status}' \
                               AND PRODUCT_NO = 8 \
                               And SOURCE_DB = '{db_source}' \
                               ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(db_output = db_output,
                                                                                                                                                           rp_enddate = rp_enddate,
                                                                                                                                                           status = status,
                                                                                                                                                           db_source = db_source))

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   fyfv_quarterly_csv = f'FYFV{file_part_name}.csv' 
   local_id = fyfv_quarterly_csv
 #   request_id = cp_s3_send(spark, df_fyfv_quarterly_csv, fyfv_quarterly_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_fyfv_quarterly_csv, mailbox_to, workflow_id, fyfv_quarterly_csv, local_id)
     print(f"{fyfv_quarterly_csv} file has been pushed to MESH with request id {request_id}. \n")
     print("df_fyfv_quarterly_csv rowcount", df_fyfv_quarterly_csv.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_fyfv_quarterly_csv)
   print("df_fyfv_quarterly_csv rowcount", df_fyfv_quarterly_csv.count())


 ##################################################################################################
 #####################    Extract the csv for FYFV - unformatted

 ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
 ##################################################################################################
 df_fyfv_quarterly_raw = spark.sql("SELECT DISTINCT \
                                   REPORTING_PERIOD_START, \
                                   REPORTING_PERIOD_END, \
                                   STATUS, \
                                   BREAKDOWN, \
                                   PRIMARY_LEVEL, \
                                   PRIMARY_LEVEL_DESCRIPTION, \
                                   SECONDARY_LEVEL, \
                                   SECONDARY_LEVEL_DESCRIPTION, \
                                   METRIC AS MEASURE_ID, \
                                   COALESCE(METRIC_VALUE,0) AS MEASURE_VALUE \
                               FROM {db_output}.FYFV_unformatted \
                               WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                               AND STATUS = '{status}' \
                               And SOURCE_DB = '{db_source}' \
                               ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(db_output = db_output,
                                                                                                                                                           rp_enddate = rp_enddate,
                                                                                                                                                           status = status,
                                                                                                                                                           db_source = db_source))

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   fyfv_quarterly_raw = f'FYFV{file_part_name}_RAW.csv' 
   local_id = fyfv_quarterly_raw
 #   request_id = cp_s3_send(spark, df_fyfv_quarterly_raw, fyfv_quarterly_raw, local_id)
   try:
     request_id = cp_mesh_send(spark, df_fyfv_quarterly_raw, mailbox_to, workflow_id, fyfv_quarterly_raw, local_id)
     print(f"{fyfv_quarterly_raw} file has been pushed to MESH with request id {request_id}. \n")
     print("df_fyfv_quarterly_raw rowcount", df_fyfv_quarterly_raw.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_fyfv_quarterly_raw)
   print("df_fyfv_quarterly_raw rowcount", df_fyfv_quarterly_raw.count())

 ##################################################################################################
 #####################    CCGOIS output (unrounded - for internal use)

 ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
 ##################################################################################################

 ##################################################################Removing CCGOIS completely so commenting out all of the code below

 #df_ccgois_quarterly_csv = spark.sql("SELECT \
 #                                    REPORTING_PERIOD, \
 #                                    STATUS, \
 #                                    CCG, \
 #                                    AgeRepPeriodEnd, \
 #                                    Gender, \
 #                                    CLUSTER, \
 #                                    SUPER_CLUSTER, \
 #                                    EMPLOYMENT_STATUS, \
 #                                    METRIC_VALUE \
 #                                    FROM {db_output}.CCGOIS_unformatted  \
 #                                    WHERE REPORTING_PERIOD = '{rp_startdate}' \
 #                                    AND STATUS = '{status}' \
 #                                    And SOURCE_DB = '{db_source}' \
 #                                    ORDER BY  \
 #                                    CCG, \
 #                                    CLUSTER, \
 #                                    SUPER_CLUSTER, \
 #                                    Gender, \
 #                                    EMPLOYMENT_STATUS, \
 #                                    AgeRepPeriodEnd".format(db_output = db_output,
 #                                                            rp_startdate = rp_startdate,
 #                                                            status = status,
 #                                                            db_source = db_source))

 #to help with local testing and avoiding the commenting and uncommenting the code
 #if(os.environ.get('env') == 'prod'):
 #  ccgois_quarterly_csv = f'CCGOIS{file_part_name}.csv' 
 #   request_id = cp_s3_send(spark, df_ccgois_quarterly_csv, ccgois_quarterly_csv, local_id)
 #  try:
 #    request_id = cp_mesh_send(spark, df_ccgois_quarterly_csv, mailbox_to, workflow_id, ccgois_quarterly_csv, local_id)
 #    print(f"{ccgois_quarterly_csv} file has been pushed to S3 with request id {request_id}. \n")
 #    print("df_ccgois_quarterly_csv rowcount", df_ccgois_quarterly_csv.count())
 #  except Exception as ex:
 #    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 #else:
 #   display(df_ccgois_quarterly_csv)
 #  print("df_ccgois_quarterly_csv rowcount", df_ccgois_quarterly_csv.count())