-- Databricks notebook source
 %python
 import os

 # import functions
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 from dsp.common.exports import create_csv_for_download

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
 ##month_id = dbutils.widgets.get("end_month_id")



 print(f'db_output is {db_output}; \
       rp_startdate is {rp_startdate}; \
       rp_enddate is {rp_enddate}; \
       status is {status}; \
       db_source is {db_source}')
       ##month_id is {month_id}')


 if status == 'Final':
   shortstatus = status
 else:
   shortstatus = status[:4]
   
 YYYY = rp_startdate[:4]
 Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

 file_part_name = f"_{Mname}{shortstatus}_{YYYY}" # without csv extension
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


 # db_source = "$mhsds_db" 


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

-- DBTITLE 1,Reports for Performance and Provisional BBRB output
 %python
  
 print(f'Second part of file name: {file_part_name}')
  
 ##############################################################
 #              Extract the CSV of most products (MHSDS Data)  ## RAW##unrounded,unsp
 ##############################################################
 spark.sql(f"OPTIMIZE {db_output}.bbrb_final_raw")
 df_mhsds_monthly_raw_csv = spark.sql(f"""SELECT DISTINCT
                                       REPORTING_PERIOD_START,
                                       REPORTING_PERIOD_END,
                                       STATUS,
                                       BREAKDOWN,
                                       PRIMARY_LEVEL,
                                       PRIMARY_LEVEL_DESCRIPTION,
                                       SECONDARY_LEVEL,
                                       SECONDARY_LEVEL_DESCRIPTION,
                                       MEASURE_ID,
                                       MEASURE_NAME,
                                       MEASURE_VALUE    
                                       FROM {db_output}.bbrb_final_raw
                                       WHERE REPORTING_PERIOD_END = '{rp_enddate}'
                                       AND STATUS = '{status}'     
                                       AND SOURCE_DB = '{db_source}'
                                       AND MEASURE_VALUE != 0
                                       AND MEASURE_ID NOT LIKE "%OAP%"
                                       AND MEASURE_ID NOT LIKE "%MRS%"
                                       AND MEASURE_ID NOT LIKE "%MHSPOP%"
                                       AND MEASURE_ID NOT LIKE "%ADD%"
                                       AND MEASURE_ID NOT LIKE "%BED%"
                                       AND MEASURE_ID NOT LIKE "%DISCH%"
                                       ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID""")
  
 #to help with local testing and avoiding the commenting and uncommenting the code
  
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_raw_csv = f'MHSDS Data{file_part_name}_RAW_bbrb.csv' 
   local_id = mhsds_monthly_raw_csv #use filename here for new LEAD_MESH process
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_raw_csv, mhsds_monthly_raw_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_raw_csv, mailbox_to, workflow_id, mhsds_monthly_raw_csv, local_id)
     print(f"{mhsds_monthly_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
 else:
   display(df_mhsds_monthly_raw_csv)
  
 ##############################################################
 #              Extract the CSV of most products - NOT RAW# sup is bbrb final
 ##############################################################
 spark.sql(f"OPTIMIZE {db_output}.bbrb_final")
 df_mhsds_monthly_csv = spark.sql(f"""SELECT DISTINCT
                                   REPORTING_PERIOD_START,
                                   REPORTING_PERIOD_END,
                                   STATUS,
                                   BREAKDOWN,
                                   PRIMARY_LEVEL,
                                   PRIMARY_LEVEL_DESCRIPTION,
                                   SECONDARY_LEVEL,
                                   SECONDARY_LEVEL_DESCRIPTION,
                                   MEASURE_ID,
                                   MEASURE_NAME,
                                   MEASURE_VALUE    
                                   FROM {db_output}.bbrb_final
                                   WHERE REPORTING_PERIOD_END = '{rp_enddate}'
                                   AND STATUS = '{status}'  
                                   AND SOURCE_DB = '{db_source}'                                  
                                   AND MEASURE_ID NOT LIKE "%OAP%"
                                   AND MEASURE_ID NOT LIKE "%MRS%"
                                   AND MEASURE_ID NOT LIKE "%ADD%"
                                   AND MEASURE_ID NOT LIKE "%BED%"
                                   AND MEASURE_ID NOT LIKE "%DISCH%"
                                   ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID""")
  
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_csv = f'MHSDS Data{file_part_name}_bbrb.csv' 
   local_id = mhsds_monthly_csv #use filename here for new LEAD_MESH process
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_csv, mhsds_monthly_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_csv, mailbox_to, workflow_id, mhsds_monthly_csv, local_id)
     print(f"{mhsds_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
 else:
   display(df_mhsds_monthly_csv)

-- COMMAND ----------

-- DBTITLE 1,Reports for Performance and Provisional OAPs output
 %python
  
 print(f'Second part of file name: {file_part_name}')
  
 ##############################################################
 #              Extract the CSV of OAPs product (MHSDS Data)  ## RAW##unrounded,unsp
 ##############################################################
 spark.sql(f"OPTIMIZE {db_output}.bbrb_final_raw")
 df_mhsds_monthly_raw_csv = spark.sql(f"""SELECT DISTINCT
                                       REPORTING_PERIOD_START,
                                       REPORTING_PERIOD_END,
                                       STATUS,
                                       BREAKDOWN,
                                       PRIMARY_LEVEL,
                                       PRIMARY_LEVEL_DESCRIPTION,
                                       SECONDARY_LEVEL,
                                       SECONDARY_LEVEL_DESCRIPTION,
                                       MEASURE_ID,
                                       MEASURE_NAME,
                                       MEASURE_VALUE    
                                       FROM {db_output}.bbrb_final_raw
                                       WHERE REPORTING_PERIOD_END = '{rp_enddate}'
                                       AND STATUS = '{status}'     
                                       AND SOURCE_DB = '{db_source}'
                                       AND MEASURE_VALUE != 0
                                       AND MEASURE_ID LIKE "%OAP%"
                                       AND MEASURE_ID NOT LIKE "%MHSPOP%"
                                       ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID""")
  
 #to help with local testing and avoiding the commenting and uncommenting the code
  
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_raw_csv = f'MHSDS Data_OAPs{file_part_name}_RAW.csv' 
   local_id = mhsds_monthly_raw_csv #use filename here for new LEAD_MESH process
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_raw_csv, mhsds_monthly_raw_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_raw_csv, mailbox_to, workflow_id, mhsds_monthly_raw_csv, local_id)
     print(f"{mhsds_monthly_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
 else:
   display(df_mhsds_monthly_raw_csv)
  
 ##############################################################
 #              Extract the CSV of OAPs product (MHSDS Data)  ## RAW##unrounded,unsp
 ##############################################################
 spark.sql(f"OPTIMIZE {db_output}.bbrb_final")
 df_mhsds_monthly_csv = spark.sql(f"""SELECT DISTINCT
                                   REPORTING_PERIOD_START,
                                   REPORTING_PERIOD_END,
                                   STATUS,
                                   BREAKDOWN,
                                   PRIMARY_LEVEL,
                                   PRIMARY_LEVEL_DESCRIPTION,
                                   SECONDARY_LEVEL,
                                   SECONDARY_LEVEL_DESCRIPTION,
                                   MEASURE_ID,
                                   MEASURE_NAME,
                                   MEASURE_VALUE    
                                   FROM {db_output}.bbrb_final
                                   WHERE REPORTING_PERIOD_END = '{rp_enddate}'
                                   AND STATUS = '{status}'  
                                   AND SOURCE_DB = '{db_source}'                                  
                                   AND MEASURE_ID LIKE "%OAP%"
                                   ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID""")
  
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_csv = f'MHSDS Data_OAPs{file_part_name}.csv' 
   local_id = mhsds_monthly_csv #use filename here for new LEAD_MESH process
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_csv, mhsds_monthly_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_csv, mailbox_to, workflow_id, mhsds_monthly_csv, local_id)
     print(f"{mhsds_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
 else:
   display(df_mhsds_monthly_csv)

-- COMMAND ----------

-- DBTITLE 1,Reports for Performance and Provisional 4-week waits output
 %python
  
 print(f'Second part of file name: {file_part_name}')
  
 ##############################################################
 #              Extract the CSV of 4-week waits product (MHSDS Data)  ## RAW##unrounded,unsp
 ##############################################################
 spark.sql(f"OPTIMIZE {db_output}.bbrb_final_raw")
 df_mhsds_monthly_raw_csv = spark.sql(f"""SELECT DISTINCT
                                       REPORTING_PERIOD_START,
                                       REPORTING_PERIOD_END,
                                       STATUS,
                                       BREAKDOWN,
                                       PRIMARY_LEVEL,
                                       PRIMARY_LEVEL_DESCRIPTION,
                                       SECONDARY_LEVEL,
                                       SECONDARY_LEVEL_DESCRIPTION,
                                       MEASURE_ID,
                                       MEASURE_NAME,
                                       MEASURE_VALUE    
                                       FROM {db_output}.bbrb_final_raw
                                       WHERE REPORTING_PERIOD_END = '{rp_enddate}'
                                       AND STATUS = '{status}'     
                                       AND SOURCE_DB = '{db_source}'
                                       AND MEASURE_VALUE != 0
                                       AND MEASURE_ID LIKE "%MRS%"
                                       AND MEASURE_ID NOT LIKE "%MHSPOP%"
                                       ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID""")
  
 #to help with local testing and avoiding the commenting and uncommenting the code
  
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_raw_csv = f'MHSDS Data_4WW{file_part_name}_RAW.csv' 
   local_id = mhsds_monthly_raw_csv #use filename here for new LEAD_MESH process
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_raw_csv, mhsds_monthly_raw_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_raw_csv, mailbox_to, workflow_id, mhsds_monthly_raw_csv, local_id)
     print(f"{mhsds_monthly_raw_csv} file has been pushed to MESH with request id {request_id}. \n")
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
 else:
   display(df_mhsds_monthly_raw_csv)
  
 ##############################################################
 #              Extract the CSV of OAPs product (MHSDS Data)  ## RAW##unrounded,unsp
 ##############################################################
 spark.sql(f"OPTIMIZE {db_output}.bbrb_final")
 df_mhsds_monthly_csv = spark.sql(f"""SELECT DISTINCT
                                   REPORTING_PERIOD_START,
                                   REPORTING_PERIOD_END,
                                   STATUS,
                                   BREAKDOWN,
                                   PRIMARY_LEVEL,
                                   PRIMARY_LEVEL_DESCRIPTION,
                                   SECONDARY_LEVEL,
                                   SECONDARY_LEVEL_DESCRIPTION,
                                   MEASURE_ID,
                                   MEASURE_NAME,
                                   MEASURE_VALUE    
                                   FROM {db_output}.bbrb_final
                                   WHERE REPORTING_PERIOD_END = '{rp_enddate}'
                                   AND STATUS = '{status}'  
                                   AND SOURCE_DB = '{db_source}'                                  
                                   AND MEASURE_ID LIKE "%MRS%"
                                   ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID""")
  
 if(os.environ.get('env') == 'prod'):
   mhsds_monthly_csv = f'MHSDS Data_4WW{file_part_name}.csv' 
   local_id = mhsds_monthly_csv #use filename here for new LEAD_MESH process
 #   request_id = cp_s3_send(spark, df_mhsds_monthly_csv, mhsds_monthly_csv, local_id)
   try:
     request_id = cp_mesh_send(spark, df_mhsds_monthly_csv, mailbox_to, workflow_id, mhsds_monthly_csv, local_id)
     print(f"{mhsds_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path') 
 else:
   display(df_mhsds_monthly_csv)