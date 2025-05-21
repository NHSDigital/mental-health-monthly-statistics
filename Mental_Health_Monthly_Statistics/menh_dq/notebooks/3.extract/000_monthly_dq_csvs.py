# Databricks notebook source

 %python
 import os

 # from dsp.code_promotion.s3_send import cp_s3_send
 from dsp.code_promotion.mesh_send import cp_mesh_send


# COMMAND ----------

# DBTITLE 1,get params etc (python)
 %python

 db_output = dbutils.widgets.get("db_output")
 status = dbutils.widgets.get("status")
 month_id = dbutils.widgets.get("month_id")
 db_source = dbutils.widgets.get("db_source")
 rp_startdate = dbutils.widgets.get("rp_startdate")

 print(f'db_output is {db_output}; \
 status is {status}; \
 month_id is {month_id}; \
 rp_startdate is {rp_startdate}; \
 db_source is {db_source}')
 # month_id = '1462'
 # status = 'Performance'
 # db_source = 'testdata_menh_dq_$mhsds'
 # db_output = 'menh_dq'


 # for month_id < 1459 (this is to remove the new v5 tables that have been introduced mid year) be like "AND TABLE_NAME NOT IN (table_names)"
 if (int(month_id) < 1459):
     table_names = "'MHS013MHCurrencyModel',\
 'MHS302MHDropInContact',\
 'MHS505RestrictiveInterventInc',\
 'MHS515RestrictiveInterventType',\
 'MHS516PoliceAssistanceRequest',\
 'MHS517SMHExceptionalPackOfCare'"
     measures = "'MHS-DQM71','MHS-DQM72','MHS-DQM73','MHS-DQM74'"  #declare the measure to be excluded in single quotes ('') separated by comma(,).
 if ((int(month_id) >= 1459) & (int(month_id) <= 1488)) : 
 # for month_id = 1459 and over should be like "AND TABLE_NAME NOT IN (table_names)"
   table_names = "'MHS505RestrictiveIntervention'"
   measures = "''" #declare the measure to be excluded in single quotes ('') separated by comma(,).
 if (int(month_id) >= 1489):
   table_names = """'MHS013MHCurrencyModel', 'MHS107MedicationPrescription', 'MHS505RestrictiveIntervention', 'MHS504DelayedDischarge', 'MHS603ProvDiag', 'MHS801ClusterTool', 'MHS802ClusterAssess', 
     'MHS803CareCluster', 'MHS804FiveForensicPathways'"""
   measures = "''" #declare the measure to be excluded in single quotes ('') separated by comma(,).
   
 print(f'Tables not to be included for month id: {month_id} is/are: {table_names}')
 print(f'Measures not to be included for month id: {month_id} is/are: {measures}')  


 # import functions
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 from dsp.common.exports import create_csv_for_download


 if status == 'Final':
   shortstatus = status
 else:
   shortstatus = status[:4]
 YYYY = rp_startdate[:4]
 Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

 file_part_name = f"_{Mname}{shortstatus}_{YYYY}.csv"
 print(f'Second part of file name: {file_part_name}')

 #Prod mail box id
 mailbox_to = 'X26HC004'
 workflow_id = 'GNASH_MHSDS'

# COMMAND ----------

# DBTITLE 1,Performance Or Provisional Or Final category reports
 %python

 # local_id = str(datetime.now().date()) +'-menh_dq' # Confluence doesn't specify which id to pass as it says 'user specified id'. So given date combo with project name # updated to be filename for new LEAD_MESH renaming process


 if(status in ['Performance','Provisional','Final']):
 #######    DQ_coverage_MmmPxxx_YYYY - run for Provisional (Prov) & Performance (Perf)
 # -- 17/2/21 SH MONTH_ID removed from extract sent to analysts ; added coalesce on coverage_count as * assigned to bigint column leaves it as null - in /data_management_services/menh_dq/notebooks/3.extract/3.3.coverage_csvs
 # -- 26/01/22 User: added in exclusions for new tables in v5 in case we need to run historic months of dq outputs

   dq_coverage_monthly_csv = spark.sql("SELECT \
                                       REPORTING_PERIOD_START, \
                                       REPORTING_PERIOD_END, \
                                       STATUS, \
                                       ORGANISATION_CODE, \
                                       ORGANISATION_NAME, \
                                       TABLE_NAME, \
                                       coalesce(COVERAGE_COUNT, '*') AS COVERAGE_COUNT \
                                       FROM {db_output}.dq_coverage_monthly_csv \
                                       WHERE Month_id = '{month_id}' \
                                       AND status = '{status}' \
                                       AND SOURCE_DB = '{db_source}' \
                                       AND TABLE_NAME NOT IN ({table_names})".format(db_output = db_output,
                                                                                     month_id = month_id,
                                                                                     status = status,
                                                                                     db_source = db_source,
                                                                                     table_names = table_names))
   #to help with local testing and avoiding the commenting and uncommenting the code
   if(os.environ.get('env') == 'prod'):
     cov_month_csv_file = 'DQ_coverage' + file_part_name
     local_id = cov_month_csv_file
 #     request_id = cp_s3_send(spark, dq_coverage_monthly_csv, cov_month_csv_file, local_id)
     try:
       request_id = cp_mesh_send(spark, dq_coverage_monthly_csv, mailbox_to, workflow_id, cov_month_csv_file, local_id)
       print(f"{cov_month_csv_file} file has been pushed to MESH with request id {request_id}. \n")
       print("dq_coverage_monthly_csv rowcount", dq_coverage_monthly_csv.count())
     except Exception as ex:
       print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
   else:
     display(dq_coverage_monthly_csv)
     print("dq_coverage_monthly_csv rowcount", dq_coverage_monthly_csv.count())

 #     extract_location = sqlContext.sql(f"SELECT extract_s3_location FROM cp_data_out.s3_send_extract_id WHERE request_id = '{request_id}'").first()[0]
 #     extract_location
 #     extract_df = spark.read.csv(extract_location)

 ###### DQ_vodim_MmmPxxx_YYYY - run for Provisional (Prov) & Performance (Perf) SEPARATELY
   dq_vodim_monthly_csv = spark.sql("SELECT \
                                     Reporting_Period, \
                                     Status, \
                                     Reporting_Level, \
                                     Provider_Code, \
                                     Provider_Name, \
                                     DQ_Measure, \
                                     DQ_Measure_Name, \
                                     DQ_Result, \
                                     DQ_Dataset_Metric_ID, \
                                     Unit, \
                                     Value \
                                     FROM {db_output}.DQ_VODIM_monthly_csv \
                                     WHERE Month_Id = '{month_id}' \
                                     AND status = '{status}' \
                                     AND SOURCE_DB = '{db_source}'\
                                     AND DQ_Measure NOT IN ({measures})".format(db_output = db_output,
                                                                         month_id = month_id,
                                                                         status = status,
                                                                         db_source = db_source,
                                                                         measures = measures))
   #to help with local testing and avoiding the commenting and uncommenting the code
   if(os.environ.get('env') == 'prod'):
     vodim_month_csv_file = 'DQ_vodim' + file_part_name
     local_id = vodim_month_csv_file

 #     request_id = cp_s3_send(spark, dq_vodim_monthly_csv, vodim_month_csv_file, local_id)
     try:
       request_id = cp_mesh_send(spark, dq_vodim_monthly_csv, mailbox_to, workflow_id, vodim_month_csv_file, local_id)
       print(f"{vodim_month_csv_file} file has been pushed to MESH with request id {request_id}. \n")
       print("dq_vodim_monthly_csv rowcount", dq_vodim_monthly_csv.count())
     except Exception as ex:
       print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
   else:
     display(dq_vodim_monthly_csv)
     print("dq_vodim_monthly_csv rowcount", dq_vodim_monthly_csv.count())


# COMMAND ----------

# DBTITLE 1,Only Performance reports
 %python
 file_part_name = f"_{Mname}_{YYYY}.csv"
 print(f'Second part of file name: {file_part_name}')

 if(status in ['Performance','Final','Provisional']):
   # -- 26/01/22 User: added in exclusions for new tables in v5 in case we need to run historic months of dq outputs
   ########### DQ_coverage_monthly_PBI_Mmm_YYYY - just for Performance
   # -- 26/01/22 User: added in exclusions for new tables in v5 in case we need to run historic months of dq outputs
   dq_coverage_monthly_pbi = spark.sql("SELECT \
                                       Organisation_code , \
                                       Organisation_Name , \
                                       Table_Name , \
                                       Sum_Of_Period_name , \
                                       X, \
                                       Period_name , \
                                       period_end_date \
                                       FROM {db_output}.dq_coverage_monthly_pbi \
                                       WHERE \
                                       Month_Id = '{month_id}' \
                                       AND status = '{status}' \
                                       AND SOURCE_DB = '{db_source}' \
                                       AND TABLE_NAME NOT IN ({table_names})".format(db_output = db_output,
                                                                                      month_id = month_id,
                                                                                      status = status,
                                                                                      db_source = db_source,
                                                                                      table_names = table_names))
   #to help with local testing and avoiding the commenting and uncommenting the code
   if(os.environ.get('env') == 'prod'):
     cov_mon_pbi = 'DQ_coverage_monthly_PBI' + file_part_name
     local_id = cov_mon_pbi
 #     request_id = cp_s3_send(spark, dq_coverage_monthly_pbi, cov_mon_pbi, local_id)
     try:
       request_id = cp_mesh_send(spark, dq_coverage_monthly_pbi, mailbox_to, workflow_id, cov_mon_pbi, local_id)
       print(f"{cov_mon_pbi} file has been pushed to MESH with request id {request_id}. \n")
       print("dq_coverage_monthly_pbi rowcount", dq_coverage_monthly_pbi.count())
     except Exception as ex:
       print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
   else:
     display(dq_coverage_monthly_pbi)
     print("dq_coverage_monthly_pbi rowcount", dq_coverage_monthly_pbi.count())

   ######## DQ_vodim_monthly_PBI_Mmm_YYYY - just for Performance
   #used LEFT operator in where condition to retrieve only measure ID from DQ_Measure column, separated by ' '
   dq_vodim_monthly_pbi = spark.sql("SELECT \
                                     Reporting_Period AS Reporting_Period_Start, \
                                     Reporting_Level, \
                                     Provider_Code, \
                                     Provider_Name, \
                                     DQ_Measure, \
                                     DQ_Result, \
                                     DQ_Dataset_Metric_ID AS DQ_Dataset_MetricID, \
                                     coalesce(Count, '*') AS Count, \
                                     Percentage \
                                     FROM {db_output}.DQ_VODIM_monthly_pbi \
                                     WHERE Month_Id = '{month_id}' \
                                     AND status = '{status}' \
                                     AND SOURCE_DB = '{db_source}' \
                                     AND LEFT(DQ_Measure, position(' ', DQ_Measure) - 1) NOT IN ({measures})\
                                     ORDER BY Provider_Code, DQ_Measure, DQ_Dataset_Metric_ID".format(db_output = db_output,
                                                                                                        month_id = month_id,
                                                                                                        status = status,
                                                                                                        db_source = db_source,
                                                                                                        measures = measures ))
   #to help with local testing and avoiding the commenting and uncommenting the code
   if(os.environ.get('env') == 'prod'):
     vodim_mon_pbi = 'DQ_vodim_monthly_PBI' + file_part_name
     local_id = vodim_mon_pbi
 #     request_id = cp_s3_send(spark, dq_vodim_monthly_pbi, vodim_mon_pbi, local_id)
     try:
       request_id = cp_mesh_send(spark, dq_vodim_monthly_pbi, mailbox_to, workflow_id, vodim_mon_pbi, local_id)
       print(f"{vodim_mon_pbi} file has been pushed to MESH with request id {request_id}. \n")
       print("dq_vodim_monthly_pbi rowcount", dq_vodim_monthly_pbi.count())
     except Exception as ex:
       print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
   else:
     display(dq_vodim_monthly_pbi)
     print("dq_vodim_monthly_pbi rowcount", dq_vodim_monthly_pbi.count())
