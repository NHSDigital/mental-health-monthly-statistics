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

 db_output = dbutils.widgets.get("db_output")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 status = dbutils.widgets.get("status")
 db_source = dbutils.widgets.get("db_source") 
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
   file_part_name = f"_pit_{file_part_name}"

 print(f'Second part of file name: {file_part_name}')

 #Prod mail box id
 mailbox_to = 'X26HC004'
 workflow_id = 'GNASH_MHSDS'

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

-- DBTITLE 1,Reports for Performance and Provisional
 %python
 # local_id = str(datetime.now().date()) +'-menh_analysis' # Confluence doesn't specify which id to pass as it says 'user specified id'. So given date combo with project name # updated to be filename for new LEAD_MESH renaming process

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
                               FROM {db_output}.All_products_formatted \
                               WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                               AND STATUS = '{status}' \
                               AND PRODUCT_NO <> 8 \
                               And SOURCE_DB = '{db_source}' \
                               ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(db_output = db_output,
                                                                                                                                                           rp_enddate = rp_enddate,
                                                                                                                                                           status = status,
                                                                                                                                                           db_source = db_source
 #                                                                                                                                                             db_output1 = db_output1,
 #                                                                                                                                                             menh_publications_source = menh_publications_source
                                                                                                                                                           ))

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_csv = f'MHSDS Data{file_part_name}_analysis.csv' 
   local_id = mhsds_monthly_csv
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_csv, mhsds_monthly_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_csv, mailbox_to, workflow_id, mhsds_monthly_csv, local_id)
     print(f"{mhsds_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
     print("df_mhsds_monthly_csv rowcount", df_mhsds_monthly_csv.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_mhsds_monthly_csv)
   print("df_mhsds_monthly_csv rowcount", df_mhsds_monthly_csv.count())

 ##############################################################
 #              Extract the CSV of most products - UNROUNDED ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
 ##############################################################
 df_mhsds_monthly_raw_csv = spark.sql("SELECT DISTINCT \
                                       REPORTING_PERIOD_START, \
                                       REPORTING_PERIOD_END, \
                                       STATUS, \
                                       BREAKDOWN, \
                                       PRIMARY_LEVEL, \
                                       PRIMARY_LEVEL_DESCRIPTION, \
                                       SECONDARY_LEVEL, \
                                       SECONDARY_LEVEL_DESCRIPTION, \
                                       METRIC as MEASURE_ID, \
                                       METRIC_VALUE as MEASURE_VALUE \
                                   FROM {db_output}.Main_monthly_unformatted \
                                   WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                                   AND STATUS = '{status}' \
                                   And SOURCE_DB = '{db_source}' \
                                   UNION ALL  \
                                   SELECT DISTINCT \
                                       REPORTING_PERIOD_START, \
                                       REPORTING_PERIOD_END, \
                                       STATUS, \
                                       BREAKDOWN, \
                                       LEVEL AS PRIMARY_LEVEL, \
                                       LEVEL_DESCRIPTION AS PRIMARY_LEVEL_DESCRIPTION, \
                                       COALESCE(SECONDARY_LEVEL,'NONE') AS SECONDARY_LEVEL, \
                                       COALESCE(SECONDARY_LEVEL_DESCRIPTION,'NONE') AS SECONDARY_LEVEL_DESCRIPTION, \
                                       METRIC as MEASURE_ID, \
                                       METRIC_VALUE as MEASURE_VALUE \
                                   FROM {db_output}.AWT_unformatted \
                                   WHERE  \
                                   REPORTING_PERIOD_END = '{rp_enddate}'  \
                                   AND STATUS = '{status}' \
                                   AND METRIC IS NOT NULL \
                                   And SOURCE_DB = '{db_source}' \
                                   UNION ALL \
                                   SELECT DISTINCT \
                                       REPORTING_PERIOD_START, \
                                       REPORTING_PERIOD_END, \
                                       STATUS, \
                                       BREAKDOWN, \
                                       PRIMARY_LEVEL, \
                                       PRIMARY_LEVEL_DESCRIPTION, \
                                       SECONDARY_LEVEL, \
                                       SECONDARY_LEVEL_DESCRIPTION, \
                                       METRIC as MEASURE_ID, \
                                      METRIC_VALUE as MEASURE_VALUE \
                                   FROM {db_output}.CYP_2nd_contact_unformatted \
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
                                       LEVEL AS PRIMARY_LEVEL, \
                                       LEVEL_DESCRIPTION AS PRIMARY_LEVEL_DESCRIPTION, \
                                       CLUSTER AS SECONDARY_LEVEL, \
                                       'NONE' AS SECONDARY_LEVEL_DESCRIPTION, \
                                       METRIC as MEASURE_ID, \
                                      METRIC_VALUE as MEASURE_VALUE \
                                   FROM {db_output}.CAP_unformatted \
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
                                       PRIMARY_LEVEL, \
                                       PRIMARY_LEVEL_DESCRIPTION, \
                                       SECONDARY_LEVEL, \
                                       SECONDARY_LEVEL_DESCRIPTION, \
                                       METRIC as MEASURE_ID, \
                                      METRIC_VALUE as MEASURE_VALUE \
                                   FROM {db_output}.CYP_monthly_unformatted \
                                   WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                                   AND STATUS = '{status}' \
                                   AND METRIC NOT IN ('MHS21a') \
                                   And SOURCE_DB = '{db_source}' \
                                   ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(db_output = db_output,
                                                                                                                                                           rp_enddate = rp_enddate,
                                                                                                                                                           status = status,
                                                                                                                                                           db_source = db_source
 #                                                                                                                                                             db_output1 = db_output1,
 #                                                                                                                                                             menh_publications_source = menh_publications_source
                                                                                                                                                           ))

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_raw_csv = f'MHSDS Data{file_part_name}_RAW_analysis.csv' 
   local_id = mhsds_monthly_raw_csv
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_raw_csv, mhsds_monthly_raw_csv, local_id)
   try: 
     request_id = cp_mesh_send(spark, df_mhsds_monthly_raw_csv, mailbox_to, workflow_id, mhsds_monthly_raw_csv, local_id)
     print(f"{mhsds_monthly_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
     print("df_mhsds_monthly_raw_csv rowcount", df_mhsds_monthly_raw_csv.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_mhsds_monthly_raw_csv)
   print("df_mhsds_monthly_raw_csv rowcount", df_mhsds_monthly_raw_csv.count())




 #     extract_location = sqlContext.sql(f"SELECT extract_s3_location FROM cp_data_out.s3_send_extract_id WHERE request_id = '{request_id}'").first()[0]
 #     extract_location
 #     extract_df = spark.read.csv(extract_location)


-- COMMAND ----------

 %py

 ##############################################################
 #              Extract the RAW results from measures in development for checking
 ##############################################################
 df_mhsds_exp_raw_csv = spark.sql("SELECT DISTINCT \
                                       REPORTING_PERIOD_START, \
                                       REPORTING_PERIOD_END, \
                                       STATUS, \
                                       BREAKDOWN, \
                                       PRIMARY_LEVEL, \
                                       PRIMARY_LEVEL_DESCRIPTION, \
                                       SECONDARY_LEVEL, \
                                       SECONDARY_LEVEL_DESCRIPTION, \
                                       METRIC as MEASURE_ID, \
                                       METRIC_VALUE as MEASURE_VALUE \
                                   FROM {db_output}.Main_monthly_unformatted_exp \
                                   WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
                                   AND STATUS = '{status}' \
                                   AND SOURCE_DB = '{db_source}' \
                                   ORDER BY BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID".format(db_output = db_output, rp_enddate = rp_enddate, status = status, db_source = db_source))

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   mhsds_exp_raw_csv = f'MHSDS Data{file_part_name}_RAW_exp.csv' 
   local_id = mhsds_exp_raw_csv
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_raw_csv, mhsds_monthly_raw_csv, local_id)
   try: 
     request_id = cp_mesh_send(spark, df_mhsds_exp_raw_csv, mailbox_to, workflow_id, mhsds_exp_raw_csv, local_id)
     print(f"{mhsds_exp_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
     print("df_mhsds_monthly_raw_csv rowcount", df_mhsds_exp_raw_csv.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_mhsds_monthly_raw_csv)
   print("df_mhsds_exp_raw_csv rowcount", df_mhsds_exp_raw_csv.count())


