-- Databricks notebook source
 %python
 import os

 # import functions
 from datetime import datetime, date
 from dateutil.relativedelta import relativedelta
 from dsp.common.exports import create_csv_for_download

 # from dsp.code_promotion.s3_send import cp_s3_send
 from dsp.code_promotion.mesh_send import cp_mesh_send

 # select distinct reporting_period_start,status from menh_analysis.all_products_formatted
 # where source_db like 'mhsds_database'
 # order by reporting_period_start desc
 # select distinct reporting_period_start,status from menh_analysis.all_products_formatted
 # where source_db like 'mhsds_database'
 # order by reporting_period_start desc

-- COMMAND ----------

-- # %py

-- # # databases where the tables to extract from are
-- # dbutils.widgets.text("db_output", "menh_analysis", "db_output");
-- # dbutils.widgets.text("db_source", "mhsds_database", "db_source");

-- # # parameters for non-standard runs
-- # dbutils.widgets.text("rp_startdate", "2020-12-01", "rp_startdate");
-- # dbutils.widgets.text("rp_enddate", "2020-12-31", "rp_enddate");
-- # dbutils.widgets.text("status", "Performance", "status");
-- # dbutils.widgets.text("MonthPeriod", "Dec-20", "MonthPeriod")

-- COMMAND ----------

-- DBTITLE 1,Create widgets
 %python

 MonthPeriod = dbutils.widgets.get("MonthPeriod")
 db_output = dbutils.widgets.get("db_output")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 status = dbutils.widgets.get("status")
 db_source = dbutils.widgets.get("db_source")


 print(f'MonthPeriod is {MonthPeriod}; \
       db_output is {db_output}; \
       rp_startdate is {rp_startdate}; \
       rp_enddate is {rp_enddate}; \
       status is {status}; \
       db_source is {db_source}')


 if len(status) == 5: # this will spit out Final and Adhoc as they are
   shortstatus = status
 else:
   shortstatus = status[:4] # this will shorten Provisional and Performance to the first 4 letters
   
 YYYY = rp_startdate[:4]
 Mname = datetime.strptime(rp_startdate, '%Y-%m-%d').strftime("%b")

 file_part_name = f"_{Mname}_{YYYY}_pbi_{shortstatus}.csv"

 if db_source == "menh_point_in_time":
   file_part_name = f"_pit_{file_part_name}"

 #Prod mail box id
 mailbox_to = 'X26HC004'
 workflow_id = 'GNASH_MHSDS'

-- COMMAND ----------

-- DBTITLE 1,CaP preparation
 %sql
  
 --this creates a temporary table that is extracted from in the next cell 
 --it's done this way to handle different values of blanks within the data: null, '*' and ''
 --if there is a better way of making this happen, feel free to update!

 CREATE OR REPLACE TEMPORARY VIEW CaP_ALL
 AS
 (
 select distinct '$MonthPeriod' as Reporting_Period,
                 a.BREAKDOWN,
                 a.Primary_Level as LEVEL,
                 a.Primary_Level_description as level_description,
                 a.secondary_level as CLUSTER,
                 (case when (b.measure_value = '*' or b.measure_value is null) then '' else b.measure_value end) as ACC02_People_on_CPA_at_the_end_of_the_Reporting_Period,
                 (case when (c.measure_value = '*' or c.measure_value is null) then '' else c.measure_value end) as ACC33_People_assigned_to_an_adult_MH_care_cluster_end_RP,
                 (case when (d.measure_value = '*' or d.measure_value is null) then '' else d.measure_value end) as ACC36_People_assigned_to_an_adult_MH_care_cluster_within_cluster_review_period_end_RP, 
                 (case when (e.measure_value = '*' or e.measure_value is null) then '' else e.measure_value end) as ACC37_Proportion_of_people_assigned_to_an_adult_MH_care_cluster_within_cluster_review_period_end_RP,
                 (case when (f.measure_value = '*' or f.measure_value is null) then '' else f.measure_value end) as ACC53_Proportion_of_people_at_the_end_of_the_Reporting_Period_who_are_on_CPA,
                 (case when (g.measure_value = '*' or g.measure_value is null) then '' else g.measure_value end) as ACC54_People_at_the_end_of_the_RP_in_settled_accommodation,              
                 (case when (h.measure_value = '*' or h.measure_value is null) then '' else h.measure_value end) as ACC62_Proportion_of_people_at_the_end_of_the_RP_in_settled_accommodation
                
 from $db_output.all_products_formatted a
 left join $db_output.all_products_formatted b
    on a.breakdown = b.breakdown
    and a.primary_level = b.primary_level
    and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
    and b.reporting_period_start = '$rp_startdate'
    and b.reporting_period_end = '$rp_enddate'
    and b.measure_id = 'ACC02'
    and b.status = '$status'
    and b.SOURCE_DB = '$db_source'
  
  left join $db_output.all_products_formatted c
    on a.breakdown = c.breakdown
    and a.primary_level = c.primary_level
    and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL
    and c.reporting_period_start = '$rp_startdate'
    and c.reporting_period_end = '$rp_enddate'
    and c.measure_id = 'ACC33'
    and c.status = '$status'
    and c.SOURCE_DB = '$db_source'
    
 left join $db_output.all_products_formatted d
    on a.breakdown = d.breakdown
    and a.primary_level = d.primary_level
    and a.SECONDARY_LEVEL = d.SECONDARY_LEVEL
    and d.reporting_period_start = '$rp_startdate'
    and d.reporting_period_end = '$rp_enddate'
    and d.measure_id = 'ACC36'
    and d.status = '$status'
    and d.SOURCE_DB = '$db_source'
    
 left join $db_output.all_products_formatted e
    on a.breakdown = e.breakdown
    and a.primary_level = e.primary_level
    and a.SECONDARY_LEVEL = e.SECONDARY_LEVEL
    and e.reporting_period_start = '$rp_startdate'
    and e.reporting_period_end = '$rp_enddate'
    and e.measure_id = 'ACC37'
    and e.status = '$status'
    and e.SOURCE_DB = '$db_source'
    
 left join $db_output.all_products_formatted f
    on a.breakdown = f.breakdown
    and a.primary_level = f.primary_level
    and a.SECONDARY_LEVEL = f.SECONDARY_LEVEL
    and f.reporting_period_start = '$rp_startdate'
    and f.reporting_period_end = '$rp_enddate'
    and f.measure_id = 'ACC53'
    and f.status = '$status'
    and f.SOURCE_DB = '$db_source'
  
 left join $db_output.all_products_formatted g
    on a.breakdown = g.breakdown
    and a.primary_level = g.primary_level
    and a.SECONDARY_LEVEL = g.SECONDARY_LEVEL
    and g.reporting_period_start = '$rp_startdate'
    and g.reporting_period_end = '$rp_enddate'
    and g.measure_id = 'ACC54'
    and g.status = '$status'
    and g.SOURCE_DB = '$db_source'
  
 left join $db_output.all_products_formatted h
    on a.breakdown = h.breakdown
    and a.primary_level = h.primary_level
    and a.SECONDARY_LEVEL = h.SECONDARY_LEVEL
    and h.reporting_period_start = '$rp_startdate'
    and h.reporting_period_end = '$rp_enddate'
    and h.measure_id = 'ACC62'
    and h.status = '$status'
    and h.SOURCE_DB = '$db_source'
     
  Where a.breakdown in ('England','Provider','CCG - GP Practice or Residence','Sub-ICB Location - GP Practice or Residence','Sub ICB - GP Practice or Residence')
    and a.measure_id in ('ACC02','ACC33','ACC36','ACC37','ACC53','ACC54','ACC62')
    and a.status = '$status'
    and a.reporting_period_start = '$rp_startdate'
    and a.reporting_period_end = '$rp_enddate'
    and a.SOURCE_DB = '$db_source'
  
 )

-- COMMAND ----------

-- DBTITLE 1,CaP output


 %python
 # local_id = str(datetime.now().date()) +'-menh_analysis' # Confluence doesn't specify which id to pass as it says 'user specified id'. So given date combo with project name # updated to be filename for new LEAD_MESH renaming process

 print(f'Second part of file name: {file_part_name}')


 #######    Monthly_CAP_Mmm_yyyy_pbi_Prf - run for Provisional (Prov)
 df_monthly_cap_pbi = spark.sql("SELECT * FROM CaP_ALL \
                                 WHERE not(ACC02_People_on_CPA_at_the_end_of_the_Reporting_Period = ''  \
                                 and ACC33_People_assigned_to_an_adult_MH_care_cluster_end_RP = ''  \
                                 and ACC36_People_assigned_to_an_adult_MH_care_cluster_within_cluster_review_period_end_RP = ''  \
                                 and ACC37_Proportion_of_people_assigned_to_an_adult_MH_care_cluster_within_cluster_review_period_end_RP = ''  \
                                 and ACC53_Proportion_of_people_at_the_end_of_the_Reporting_Period_who_are_on_CPA = ''  \
                                 and ACC54_People_at_the_end_of_the_RP_in_settled_accommodation = ''  \
                                 and ACC62_Proportion_of_people_at_the_end_of_the_RP_in_settled_accommodation = '') \
                                 ORDER BY BREAKDOWN, LEVEL, CLUSTER")

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   monthly_cap_pbi_file = 'Monthly_CAP' + file_part_name
   local_id = monthly_cap_pbi_file

   try:
     request_id = cp_mesh_send(spark, df_monthly_cap_pbi, mailbox_to, workflow_id, monthly_cap_pbi_file, local_id)
     print(f"{monthly_cap_pbi_file} file has been pushed to MESH with request id {request_id}. \n")
     print("df_monthly_cap_pbi rowcount", df_monthly_cap_pbi.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_monthly_cap_pbi)
   print("df_monthly_cap_pbi rowcount", df_monthly_cap_pbi.count())



-- COMMAND ----------

-- DBTITLE 1,7.CYP preparation
 %sql
  
 --this creates a temporary table that is extracted from in the next cell 
 --it's done this way to handle different values of blanks within the data: null, '*' and ''
 --if there is a better way of making this happen, feel free to update!
  
 CREATE OR REPLACE TEMPORARY VIEW CYP_ALL
 AS
 (
 select distinct '$MonthPeriod' as Reporting_Period,
                 a.BREAKDOWN as Breakdown_All,
                 (case
                   when upper(a.BREAKDOWN) LIKE '%ENGLAND%' then 'England'
                   when upper(a.BREAKDOWN) LIKE '%PROVIDER%' then 'Provider'
                   when upper(a.BREAKDOWN) LIKE '%CCG%' then 'CCG - GP Practice or Residence'
                 end) as BREAKDOWN,
                 a.PRIMARY_LEVEL,
                 a.PRIMARY_LEVEL_DESCRIPTION,
                 a.SECONDARY_LEVEL,
                 a.SECONDARY_LEVEL_DESCRIPTION,
                 (case when (b.measure_value = '*' or b.measure_value is null) then '' else b.measure_value end) as MHS30e_Attended_contacts_in_the_RP_0_to_18_by_consultation_medium,
                 (case when (c.measure_value = '*' or c.measure_value is null) then '' else c.measure_value end) as MHS32a_Referrals_starting_in_Reporting_Period_aged_0_to_18,
                 (case when (d.measure_value = '*' or d.measure_value is null) then '' else d.measure_value end) as MHS58a_Missed_care_contacts_in_the_Reporting_Period_aged_0_to_18_by_DNA_reason
 from $db_output.all_products_formatted a
 left join $db_output.all_products_formatted b
    on a.breakdown = b.breakdown
    and a.primary_level = b.primary_level
    and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
    and b.reporting_period_start = '$rp_startdate'
    and b.reporting_period_end = '$rp_enddate'
    and b.measure_id = 'MHS30e'
    and b.status = '$status'
    and b.SOURCE_DB = '$db_source'
       
 left join $db_output.all_products_formatted c
    on a.breakdown = c.breakdown
    and a.primary_level = c.primary_level
    and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL
    and c.reporting_period_start = '$rp_startdate'
    and c.reporting_period_end = '$rp_enddate'
    and c.measure_id = 'MHS32a'
    and c.status = '$status'
    and c.SOURCE_DB = '$db_source'
    
 left join $db_output.all_products_formatted d
    on a.breakdown = d.breakdown
    and a.primary_level = d.primary_level
    and a.SECONDARY_LEVEL = d.SECONDARY_LEVEL
    and d.reporting_period_start = '$rp_startdate'
    and d.reporting_period_end = '$rp_enddate'
    and d.measure_id = 'MHS58a'
    and d.status = '$status'
    and d.SOURCE_DB = '$db_source'
    
 where (a.breakdown like 'England%' or a.breakdown like 'Provider%' or a.breakdown like '%CCG%' or a.breakdown like '%Sub-ICB%' or a.breakdown like '%Sub ICB%')
 and a.measure_id in ('MHS30e','MHS32a','MHS58a')
 and a.status = '$status'
 and a.reporting_period_start = '$rp_startdate'
 and a.reporting_period_end = '$rp_enddate'
 and a.SOURCE_DB = '$db_source'

 )

-- COMMAND ----------

-- DBTITLE 1,CYP output
 %python
 print(f'Second part of file name: {file_part_name}')


 #######    Monthly_CAP_Mmm_yyyy_pbi_Prf - run for Provisional (Prov)
 df_monthly_cyp_pbi = spark.sql("SELECT * FROM CYP_ALL \
                                 WHERE not(MHS30e_Attended_contacts_in_the_RP_0_to_18_by_consultation_medium    = ''  \
                                 and MHS32a_Referrals_starting_in_Reporting_Period_aged_0_to_18 = ''  \
                                 and MHS58a_Missed_care_contacts_in_the_Reporting_Period_aged_0_to_18_by_DNA_reason = '')")

 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   monthly_cyp_pbi_file = 'Monthly_CYP' + file_part_name
   local_id = monthly_cyp_pbi_file

   try:
     request_id = cp_mesh_send(spark, df_monthly_cyp_pbi, mailbox_to, workflow_id, monthly_cyp_pbi_file, local_id)
     print(f"{monthly_cyp_pbi_file} file has been pushed to MESH with request id {request_id}. \n")
     print("df_monthly_cyp_pbi rowcount", df_monthly_cyp_pbi.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_monthly_cyp_pbi)
   print("df_monthly_cyp_pbi rowcount", df_monthly_cyp_pbi.count())


-- COMMAND ----------

-- DBTITLE 1,MHS / MHA Output
 %python

 print(f'Second part of file name: {file_part_name}')


 ########### Monthly_MHS_MHA_Mmm_yyyy_pbi_Prf - just for Performance
 df_mhs_mha_pbi = spark.sql("select distinct '{MonthPeriod}' as Reporting_Period, \
                                     a.BREAKDOWN, \
                                     a.PRIMARY_LEVEL, \
                                     a.PRIMARY_LEVEL_DESCRIPTION, \
                                     a.SECONDARY_LEVEL, \
                                     a.SECONDARY_LEVEL_DESCRIPTION, \
                                     concat(a.measure_id,' - ',a.measure_Name) as Measure_Name, \
                                     (case when a.measure_value = '*' then '' else a.measure_value end) as Value \
                                     from {db_output}.all_products_formatted a \
                                     where a.reporting_period_start = '{rp_startdate}' \
                                     and a.reporting_period_end = '{rp_enddate}' \
                                     and a.measure_id in ('AMH01','AMH21','AMH23','CYP01','CYP21','CYP23','MH01a','MH01b','MH01c','MHS08','MHS09','MHS10','MHS11') \
                                     and a.breakdown in ('England','Provider','CCG - GP Practice or Residence','Sub-ICB Location - GP Practice or Residence','Sub ICB - GP Practice or Residence') \
                                     and a.status = '{status}' \
                                     and a.SOURCE_DB = '{db_source}'".format(MonthPeriod = MonthPeriod,
                                                                                        db_output = db_output,
                                                                                        rp_startdate = rp_startdate,
                                                                                        rp_enddate = rp_enddate, 
                                                                                        status = status,
                                                                                        db_source = db_source))
 #to help with local testing and avoiding the commenting and uncommenting the code
 if(os.environ.get('env') == 'prod'):
   mhs_mha_pbi = 'Monthly_MHS_MHA' + file_part_name
   local_id = mhs_mha_pbi

   try:
     request_id = cp_mesh_send(spark, df_mhs_mha_pbi, mailbox_to, workflow_id, mhs_mha_pbi, local_id)
     print(f"{mhs_mha_pbi} file has been pushed to MESH with request id {request_id}. \n")
     print("df_mhs_mha_pbi rowcount", df_mhs_mha_pbi.count())
   except Exception as ex:
     print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 else:
 #   display(df_mhs_mha_pbi)
   print("df_mhs_mha_pbi rowcount", df_mhs_mha_pbi.count())



-- COMMAND ----------

-- DBTITLE 1,Only Performance reports

 %python

 #file_part_name = f"_{Mname}_{YYYY}_{shortstatus}_CPA.csv"
 #print(f'Second part of file name: {file_part_name}')
 #workflow_id = 'GNASH_MHSDS'


 ##################################################################################################
 #####################    Extract the CSV of ASCOF ## REMOVE THE COMMENTS FROM THE ACTUAL SQLS STATEMENT !!! OTHERWISE IT IS GIVING ERRORS
 #  we dont have few columns here! with available columns are:
 # REPORTING_PERIOD_START:string
 # REPORTING_PERIOD_END:string
 # STATUS:string
 # BREAKDOWN:string
 # PRIMARY_LEVEL:string
 # PRIMARY_LEVEL_DESCRIPTION:string
 # SECONDARY_LEVEL:string
 # SECONDARY_LEVEL_DESCRIPTION:string
 # METRIC:string
 # METRIC_VALUE:string
 # SOURCE_DB:string
 ##################################################################################################
 #df_ascof_monthly_csv = spark.sql("SELECT \
 #                                  REPORTING_PERIOD_START as REPORTING_PERIOD, \
 #                                  STATUS, \
 #                                  BREAKDOWN, \
 #                                  LEVEL_ONE, \
 #                                  LEVEL_ONE_DESCRIPTION, \
 #                                  LEVEL_TWO, \
 #                                  LEVEL_TWO_DESCRIPTION, \
 #                                  LEVEL_THREE, \
 #                                  METRIC, \
 #                                  METRIC_VALUE \
 #                              FROM {db_output}.Ascof_unformatted \
 #                              WHERE REPORTING_PERIOD_END = '{rp_enddate}'  \
 #                              AND STATUS = '{status}' \
 #                              And SOURCE_DB = '{db_source}' \
 #                              ORDER BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, LEVEL_ONE, LEVEL_TWO, LEVEL_THREE, METRIC".format(db_output = db_output,
 #                                                                                                                                                          rp_enddate = rp_enddate,
 #                                                                                                                                                          status = status,
 #                                                                                                                                                          db_source = db_source))
 #
 #to help with local testing and avoiding the commenting and uncommenting the code
 #if(os.environ.get('env') == 'prod'):
 #  ascof_monthly_csv = f'Monthly_ASCOF_File{file_part_name}' 
 #  local_id = ascof_monthly_csv
 #   request_id = cp_s3_send(spark, df_ascof_monthly_csv, ascof_monthly_csv, local_id)
 #  try:
 #    request_id = cp_mesh_send(spark, df_ascof_monthly_csv, mailbox_to, workflow_id, ascof_monthly_csv, local_id)
 #    print(f"{ascof_monthly_csv} file has been pushed to MESH with request id {request_id}. \n")
 #    print("df_ascof_monthly_csv rowcount", df_ascof_monthly_csv.count())
 #  except Exception as ex:
 #    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
 #else:
 #   display(df_ascof_monthly_csv)
 #  print("df_ascof_monthly_csv rowcount", df_ascof_monthly_csv.count())

 #     extract_location = sqlContext.sql(f"SELECT extract_s3_location FROM cp_data_out.s3_send_extract_id WHERE request_id = '{request_id}'").first()[0]
 #     extract_location
 #     extract_df = spark.read.csv(extract_location)


-- COMMAND ----------

