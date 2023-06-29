# Databricks notebook source
 %md
 #MHSDS Monthly - Additional Breakdowns - NO LONGER IN USE
 ##Master Notebook

# COMMAND ----------

 %md
 ##Please note as of March 2022 the name of the widgets has changed
 example in this cmd is for: January 2022 Performance, February 2022 Provisional - this code produces both outputs in a single run
 
 > ##### **Input database:**
 mh_v5_pre_clear
 > ##### Publication Month:
 202204
 > ##### Source Ref Database:
 dss_corporate
 
 > ##### Target database:
 Your database name

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("db_output", "", "Target database")
db_output = dbutils.widgets.get("db_output")
# assert db_output
 
dbutils.widgets.text("db_source", "", "Input database")
db_source = dbutils.widgets.get("db_source")
# assert db_source
 
dbutils.widgets.text("dss_corporate","","Source Ref Database")
dss_corporate = dbutils.widgets.get("dss_corporate")
# assert dss_corporate
 
dbutils.widgets.text("pub_month", "202108", "Publication Month")
pub_month = dbutils.widgets.get("pub_month")
# assert month_id
 
# dbutils.widgets.text("month_id", "1455", "Month ID")
# month_id = dbutils.widgets.get("month_id")
# # assert month_id
 
# dbutils.widgets.text("rp_startdate", "2021-06-01", "Reporting period start date")
# rp_startdate = dbutils.widgets.get("rp_startdate")
# # assert rp_startdate
 
# dbutils.widgets.text("rp_enddate", "2021-06-30", "Reporting period end date")
# rp_enddate = dbutils.widgets.get("rp_enddate")
# # assert rp_enddate
 
# dbutils.widgets.text("status", "Performance", "Status")
# status = dbutils.widgets.get("status")
# # assert status
 
 
# params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate': str(rp_enddate), 'rp_startdate': str(rp_startdate), 'month_id': month_id, 'status': status, 'dss_corporate': dss_corporate, 'mh_pre_pseudo_d1': db_source}
# params

# COMMAND ----------

#Calculate start dates for performance and provisional runs from Publication Month

pubyr = pub_month[:4]
pubmth = pub_month[4:]
print(pubyr)
print(pubmth)

if pubmth == '01':
  perfstartdate = str(int(pubyr)-1)+'-10-01'
  provstartdate = str(int(pubyr)-1)+'-11-01'
elif pubmth == '02':
  perfstartdate = str(int(pubyr)-1)+'-11-01'
  provstartdate = str(int(pubyr)-1)+'-12-01'
elif pubmth == '03':
  perfstartdate = str(int(pubyr)-1)+'-12-01'
  provstartdate = pubyr+'-01-01'
else:
  perfstartdate = pubyr+'-'+('00'+str(int(pubmth)-3))[1:]+'-01'
  provstartdate = pubyr+'-'+('00'+str(int(pubmth)-2))[1:]+'-01'  
  
print('Perf: '+perfstartdate)
print('Prov: '+provstartdate)

# COMMAND ----------

#SET UP FOR PERFORMANCE RUN
dbutils.widgets.text("startdate", perfstartdate, "startdate")
startdate = dbutils.widgets.get("startdate")
  
month_id = str(spark.sql("SELECT DISTINCT UniqMonthID FROM "+db_source+".MHS000Header WHERE ReportingPeriodStartDate = '"+startdate+"'").head()[0])
rp_startdate = str(spark.sql("SELECT DISTINCT ReportingPeriodStartDate FROM "+db_source+".MHS000Header WHERE ReportingPeriodStartDate = '"+startdate+"'").head()[0])
rp_enddate = str(spark.sql("SELECT DISTINCT ReportingPeriodEndDate FROM "+db_source+".MHS000Header WHERE ReportingPeriodStartDate = '"+startdate+"'").head()[0])

dbutils.widgets.text("status", "Performance", "Status")
status = dbutils.widgets.get("status")

dbutils.widgets.text("month_id", month_id, "Month ID")
dbutils.widgets.text("rp_startdate", rp_startdate, "Reporting period start date")
dbutils.widgets.text("rp_enddate", rp_enddate, "Reporting period end date")

params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate': str(rp_enddate), 'rp_startdate_1m': str(rp_startdate), 'end_month_id': month_id, 'status': status, 'dss_corporate': dss_corporate, 'mh_pre_pseudo_d1': db_source}
params

# COMMAND ----------

 %md
 ##Generic Preparation

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Generic Prep', 0, params)
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Ref Data Load', 0, params)

# COMMAND ----------

 %md
 ##Run Measures

# COMMAND ----------

#MHS23 based measures - MHS23d&e
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS23', 0, params)

#MHS27 based measures - MHS27?
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS27', 0, params)

#MHS29 based measures - MHS29d&e
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS29', 0, params)

#MHS30 based measures - MHS30f,g,h&i
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS30', 0, params)

#MHS32 based measures - MHS32c&d
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS32', 0, params)

#MHS57 based measures - MHS57b&c
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS57', 0, params)


# COMMAND ----------

 %md
 ##Extract Output Data

# COMMAND ----------

dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Outputs', 0, params)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_unsuppressed_perf;
 
 CREATE TABLE IF NOT EXISTS $db_output.output_unsuppressed_perf USING DELTA AS
 Select distinct * from $db_output.output_unsuppressed 
 order by MEASURE_ID, breakdown, primary_level, secondary_level;
 
 DROP TABLE IF EXISTS $db_output.output_suppressed_final_perf;
 
 CREATE TABLE IF NOT EXISTS $db_output.output_suppressed_final_perf USING DELTA AS
 Select distinct * from $db_output.output_suppressed_final_1 
 order by MEASURE_ID, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level;

# COMMAND ----------

#SET UP FOR PROVISIONAL RUN
#REMOVE PREVIOUS WIDGETS
dbutils.widgets.remove("startdate")
dbutils.widgets.remove("status")
dbutils.widgets.remove("month_id")
dbutils.widgets.remove("rp_startdate")
dbutils.widgets.remove("rp_enddate")

#SET UP NEW WIDGETS
dbutils.widgets.text("startdate", provstartdate, "startdate")
startdate = dbutils.widgets.get("startdate")
  
month_id = str(spark.sql("SELECT DISTINCT UniqMonthID FROM "+db_source+".MHS000Header WHERE ReportingPeriodStartDate = '"+startdate+"'").head()[0])
rp_startdate = str(spark.sql("SELECT DISTINCT ReportingPeriodStartDate FROM "+db_source+".MHS000Header WHERE ReportingPeriodStartDate = '"+startdate+"'").head()[0])
rp_enddate = str(spark.sql("SELECT DISTINCT ReportingPeriodEndDate FROM "+db_source+".MHS000Header WHERE ReportingPeriodStartDate = '"+startdate+"'").head()[0])

dbutils.widgets.text("status", "Provisional", "Status")
status = dbutils.widgets.get("status")

dbutils.widgets.text("month_id", month_id, "Month ID")
dbutils.widgets.text("rp_startdate", rp_startdate, "Reporting period start date")
dbutils.widgets.text("rp_enddate", rp_enddate, "Reporting period end date")

params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate': str(rp_enddate), 'rp_startdate_1m': str(rp_startdate), 'end_month_id': month_id, 'status': status, 'dss_corporate': dss_corporate, 'mh_pre_pseudo_d1': db_source}
params

# COMMAND ----------

 %md
 ##Generic Preparation

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Generic Prep', 0, params)
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Ref Data Load', 0, params)

# COMMAND ----------

 %md
 ##Run Measures

# COMMAND ----------

#MHS23 based measures - MHS23d&e
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS23', 0, params)

#MHS27 based measures - MHS27?
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS27', 0, params)

#MHS29 based measures - MHS29d&e
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS29', 0, params)

#MHS30 based measures - MHS30f,g,h&i
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS30', 0, params)

#MHS32 based measures - MHS32c&d
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS32', 0, params)

#MHS57 based measures - MHS57b&c
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS57', 0, params)


# COMMAND ----------

 %md
 ##Extract Output Data

# COMMAND ----------

dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Outputs', 0, params)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_unsuppressed_prov;
 
 CREATE TABLE IF NOT EXISTS $db_output.output_unsuppressed_prov USING DELTA AS
 Select distinct * from $db_output.output_unsuppressed 
 order by MEASURE_ID, breakdown, primary_level, secondary_level;
 
 DROP TABLE IF EXISTS $db_output.output_suppressed_final_prov;
 
 CREATE TABLE IF NOT EXISTS $db_output.output_suppressed_final_prov USING DELTA AS
 Select distinct * from $db_output.output_suppressed_final_1 
 order by MEASURE_ID, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level;

# COMMAND ----------

# DBTITLE 1,This output should be saved as Monthly_File_Additional_MMM_YYYY_Perf_RAW.csv
 %sql
 Select distinct * from $db_output.output_unsuppressed_perf 
 order by MEASURE_ID, breakdown, primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,This output should be saved as Monthly_File_Additional_MMM_YYYY_Perf.csv
 %sql
 Select distinct * from $db_output.output_suppressed_final_perf
 order by MEASURE_ID, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,This output should be saved as Monthly_File_Additional_MMM_YYYY_Prov_RAW.csv
 %sql
 Select distinct * from $db_output.output_unsuppressed_prov
 order by MEASURE_ID, breakdown, primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,This output should be saved as Monthly_File_Additional_MMM_YYYY_Prov.csv
 %sql
 Select distinct * from $db_output.output_suppressed_final_prov
 order by MEASURE_ID, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level

# COMMAND ----------

 %sql
 Select distinct * from $db_output.output_suppressed_final_prov
 order by MEASURE_ID, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level

# COMMAND ----------

#REMOVE PREVIOUS WIDGETS
dbutils.widgets.remove("startdate")
dbutils.widgets.remove("status")
dbutils.widgets.remove("month_id")
dbutils.widgets.remove("rp_startdate")
dbutils.widgets.remove("rp_enddate")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))