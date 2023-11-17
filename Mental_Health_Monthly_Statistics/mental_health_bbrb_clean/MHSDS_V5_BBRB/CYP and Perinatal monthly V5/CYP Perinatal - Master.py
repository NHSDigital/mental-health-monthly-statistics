# Databricks notebook source
 %md
 db_output: menh_analysis 23/06/22
 currently changed to personal output for testing
 1462 = jan22
 ##### Please note as of March 2022 the name of the widgets has changed 
 example below is for: 
 *January 2022*
 
 **db_output:**
 Your database name
 
 **db_source: **
 *$reference_data*
 
 **end_month_id (previously "month_id"):**
 this is the month you are reporting, eg 1462 (Jan 2022)
 
 **rp_enddate:**
 this is the end date of the reporting month, in this example will the '2022-01-31'
 
 **rp_startdate_12m:**
 this is the start date of the report (12 rolling month) so in this example will be '2021-02-01'
 
 **rp_startdate_1m (previously "rp_startdate"):**
 this is the start of the reporting month, in this example '2022-01-01'
 
 **rp_startdate_qtr (not previously used):**
 this is quarterly so its your "rp_startdate_1m" minus two months, so in this example its '2021-11-01'
 
 **start_month_id (not previoulsy used):**
 this is start of the reporting month so you would use the month from 'rp_startdate_12m' in this example will be '1451' (Feb2021)
 
 **status:**
 this depends on whether you're running 'Performance' or 'Provisional' so remember to update this

# COMMAND ----------

 %sql
 select distinct UniqMonthID, ReportingPeriodEndDate from $reference_data.mhs000header order by uniqmonthid desc

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]
# dbutils.widgets.dropdown("rp_startdate_1m", "2022-06-01", startchoices)
# dbutils.widgets.dropdown("rp_startdate_12m", "2021-07-01", startchoices)
# dbutils.widgets.dropdown("rp_startdate_qtr", "2022-04-01", startchoices)
# dbutils.widgets.dropdown("rp_enddate", "2022-06-30", endchoices)
# dbutils.widgets.dropdown("start_month_id", "1456", monthid)
# dbutils.widgets.dropdown("end_month_id", "1467", monthid)
# dbutils.widgets.text("status","Provisional")
# dbutils.widgets.text("db_output","")
# dbutils.widgets.text("db_source","$reference_data")
# dbutils.widgets.text("financial_year_start","2023-04-01")
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
financial_year_start = dbutils.widgets.get("financial_year_start")
params = {'rp_startdate_1m': str(rp_startdate_1m), 'rp_startdate_12m': str(rp_startdate_12m), 'rp_startdate_qtr': str(rp_startdate_qtr), 'rp_enddate': str(rp_enddate), 'start_month_id': start_month_id, 'end_month_id': end_month_id, 'db_output': db_output, 'db_source': db_source, 'status': status, 'financial_year_start': financial_year_start}

# COMMAND ----------

# DBTITLE 1,**if you have cloned this notebook, you will need to update the outputs link**
 %md 
 
 ## Outputs
 #### [Go to Unsuppressed/raw Output](https://db.core.data.digital.nhs.uk/#notebook/5608676/command/5711796)
 #### [Go to Suppressed/rounded Output](https://db.core.data.digital.nhs.uk/#notebook/5608676/command/5711848)

# COMMAND ----------

 %py
 spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

 %py
 
 dbutils.notebook.run('CYP_Perinatal_tables', 0, params)

# COMMAND ----------

 %py
 
 dbutils.notebook.run('CYP_Perinatal_ref', 0, params)

# COMMAND ----------

 %py
 
 dbutils.notebook.run('CYP Access v2', 0, params)

# COMMAND ----------

# DBTITLE 1,MHS109 *New metric*  start from 18/07/2022
 %py
 
 dbutils.notebook.run('AdultsMHS109', 0, params) 

# COMMAND ----------

 %py
 
 dbutils.notebook.run('CYP Outcomes', 0, params)

# COMMAND ----------

 %py
 
 dbutils.notebook.run('Perinatal final', 0, params)

# COMMAND ----------

# DBTITLE 1,Unrounded
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_peri_raw;
 CREATE TABLE $db_output.cyp_peri_raw USING DELTA AS 
 SELECT
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 MEASURE_ID,
 CASE
 WHEN MEASURE_ID = 'MHS91' THEN 'Number of people in contact with Specialist Perinatal Mental Health Community Services (12 month rolling)'
 WHEN MEASURE_ID = 'MHS109' THEN 'Number of people aged 18 to 24 supported through NHS funded mental health with at least one contact (12 month rolling)' -- new metric June2022, not to be used until confirmed
 WHEN MEASURE_ID = 'MHS95' THEN 'Number of CYP aged under 18 supported through NHS funded mental health with at least one contact (12 month rolling)'
 WHEN MEASURE_ID = 'MHS92' THEN 'Closed referrals for children and young people aged between 0 and 17 with 2 contacts and a paired score'
 WHEN MEASURE_ID = 'MHS93' THEN 'Closed referrals for children and young people aged between 0 and 17 with 2 contacts'
 WHEN MEASURE_ID = 'MHS94' THEN 'Percentage of closed referrals for children and young people aged between 0 and 17 with 2 contacts and a paired score'
 WHEN MEASURE_ID = 'MHS130' THEN 'Number of referrals for CYP aged under 18 supported through NHS funded mental health with a first contact in the RP (3 month rolling)'
 WHEN MEASURE_ID = 'MHS131' THEN 'Median waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health with a first contact in the RP (3 month rolling)'
 WHEN MEASURE_ID = 'MHS132' THEN '90th percentile waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health with a first contact in the RP (3 month rolling)'
 WHEN MEASURE_ID = 'MHS133' THEN 'Number of referrals for CYP aged under 18 supported through NHS funded mental health still waiting for a first contact and still waiting at the end of the RP'
 WHEN MEASURE_ID = 'MHS134' THEN 'Median waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health still waiting for a first contact and still waiting at the end of the RP'
 WHEN MEASURE_ID = 'MHS135' THEN '90th percentile waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health still waiting for a first contact and still waiting at the end of the RP'
 END AS MEASURE_NAME,
 MEASURE_VALUE
 FROM 
 $db_output.CYP_PERI_monthly
 --WHERE
 --BREAKDOWN = 'England'

# COMMAND ----------

# DBTITLE 1,Unrounded Output (download)
 %sql
 select * from $db_output.cyp_peri_raw 
 ORDER BY BREAKDOWN, MEASURE_ID

# COMMAND ----------

# DBTITLE 1,Rounded
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_peri_sup;
 CREATE TABLE $db_output.cyp_peri_sup USING DELTA AS 
 SELECT 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 MEASURE_ID,
 CASE 
 WHEN MEASURE_ID = 'MHS95' THEN 'Number of CYP aged under 18 supported through NHS funded mental health with at least one contact (12 month rolling)'
 WHEN MEASURE_ID = 'MHS109' THEN 'Number of people aged 18 to 24 supported through NHS funded mental health with at least one contact (12 month rolling)' -- new metric June2022 not to be used until confirmed
 WHEN MEASURE_ID = 'MHS91' THEN 'Perinatal Access showing the number of people in contact with Specialist Perinatal Mental Health Community Services'
 WHEN MEASURE_ID = 'MHS92' THEN 'Closed referrals for children and young people aged between 0 and 17 with 2 contacts where the length of referral was over 14 days and a paired score'
 WHEN MEASURE_ID = 'MHS93' THEN 'Closed referrals for children and young people aged between 0 and 17 with 2 contacts where the length of referral was over 14 days'
 WHEN MEASURE_ID = 'MHS94' THEN 'Percentage of closed referrals for children and young people aged between 0 and 17 with 2 contacts where the length of referral was over 14 days and a paired score'
 WHEN MEASURE_ID = 'MHS130' THEN 'Number of referrals for CYP aged under 18 supported through NHS funded mental health with a first contact in the RP (3 month rolling)'
 WHEN MEASURE_ID = 'MHS133' THEN 'Number of referrals for CYP aged under 18 supported through NHS funded mental health still waiting for a first contact and still waiting at the end of the RP'
 END AS MEASURE_NAME,
 -- MEASURE_VALUE, Removed to align with all Babbage MH Outputs
 CASE
   WHEN MEASURE_VALUE IS NULL THEN '*'
   WHEN MEASURE_VALUE = 0 THEN '*'
   ELSE MEASURE_VALUE
   END AS MEASURE_VALUE
 
 FROM
 
 (SELECT 
 a.REPORTING_PERIOD_START,
 a.REPORTING_PERIOD_END,
 a.STATUS,
 a.BREAKDOWN,
 a.PRIMARY_LEVEL,
 a.PRIMARY_LEVEL_DESCRIPTION,
 a.SECONDARY_LEVEL,
 a.SECONDARY_LEVEL_DESCRIPTION,
 a.MEASURE_ID,
 a.MEASURE_NAME,
 -- a.MEASURE_VALUE, Removed to align with all Babbage MH Outputs
 CASE 
                 WHEN A.PRIMARY_LEVEL = 'England' THEN a.MEASURE_VALUE
                 WHEN a.MEASURE_VALUE < 5 and a.MEASURE_ID <> 'MHS94' THEN 0 
                 WHEN a.MEASURE_ID = 'MHS94' and (b.MEASURE_VALUE < 5 or c.MEASURE_VALUE < 5) THEN 0
                 WHEN a.MEASURE_ID = 'MHS94' THEN ROUND(a.MEASURE_VALUE/1.0,0)*1
                 ELSE ROUND(a.MEASURE_VALUE/5.0,0)*5
                 END AS MEASURE_VALUE
 FROM 
 $db_output.CYP_PERI_monthly a
 LEFT JOIN $db_output.CYP_PERI_monthly b on a.measure_id = 'MHS94' and b.Measure_ID = 'MHS92' and a.Primary_Level = b.Primary_Level
 LEFT JOIN $db_output.CYP_PERI_monthly c on a.measure_id = 'MHS94' and c.Measure_ID = 'MHS93' and a.Primary_Level = c.Primary_Level
 WHERE
 a.MEASURE_ID NOT IN ('MHS131','MHS132','MHS134','MHS135')) 
 
 
 UNION ALL 
 
 SELECT 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 MEASURE_ID,
 CASE 
 WHEN MEASURE_ID = 'MHS131' THEN 'Median waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health with a first contact in the RP (3 month rolling)'
 WHEN MEASURE_ID = 'MHS132' THEN '90th percentile waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health with a first contact in the RP (3 month rolling)'
 WHEN MEASURE_ID = 'MHS134' THEN 'Median waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health still waiting for a first contact and still waiting at the end of the RP'
 WHEN MEASURE_ID = 'MHS135' THEN '90th percentile waiting time between referral start date and first contact in days for referrals for CYP aged under 18 supported through NHS funded mental health still waiting for a first contact and still waiting at the end of the RP'
 END AS MEASURE_NAME,
 -- MEASURE_VALUE, Removed to align with all Babbage MH Outputs
 CASE
   WHEN MEASURE_VALUE IS NULL THEN '*'
   WHEN MEASURE_VALUE = 0 THEN '*'
   ELSE MEASURE_VALUE
   END AS MEASURE_VALUE
 
 FROM
 
 (SELECT 
 a.REPORTING_PERIOD_START,
 a.REPORTING_PERIOD_END,
 a.STATUS,
 a.BREAKDOWN,
 a.PRIMARY_LEVEL,
 a.PRIMARY_LEVEL_DESCRIPTION,
 a.SECONDARY_LEVEL,
 a.SECONDARY_LEVEL_DESCRIPTION,
 a.MEASURE_ID,
 a.MEASURE_NAME,
 -- a.MEASURE_VALUE, Removed to align with all Babbage MH Outputs
 CASE 
                 WHEN A.PRIMARY_LEVEL = 'England' THEN a.MEASURE_VALUE
                 WHEN b.MEASURE_VALUE < 5 and b.MEASURE_ID = 'MHS130' THEN 0 
                 WHEN c.MEASURE_VALUE < 5 and c.MEASURE_ID = 'MHS133' THEN 0 
                 ELSE cast(cast(round(a.MEASURE_VALUE,0) as float) as int)
                 END AS MEASURE_VALUE
 FROM 
 $db_output.CYP_PERI_monthly a
 LEFT JOIN $db_output.CYP_PERI_monthly b on a.measure_id in ('MHS131','MHS132') and b.measure_id = 'MHS130' and b.Measure_ID = 'MHS92' and a.Primary_Level = b.Primary_Level
 LEFT JOIN $db_output.CYP_PERI_monthly c on a.measure_id in ('MHS134','MHS135') and c.measure_id = 'MHS133' and c.Measure_ID = 'MHS93' and a.Primary_Level = c.Primary_Level
 WHERE
 a.MEASURE_ID IN ('MHS131','MHS132','MHS134','MHS135')) 

# COMMAND ----------

# DBTITLE 1,Suppressed Output (download)
 %sql
 select * from $db_output.cyp_peri_sup
 ORDER BY 
 CASE
   WHEN BREAKDOWN = 'England' THEN 1
   ELSE 2
   END,
 BREAKDOWN,
 primary_level_description,
 MEASURE_ID

# COMMAND ----------

 %md 
 
 ## Back to Top
 #### [Back to TOP](https://db.core.data.digital.nhs.uk/#notebook/5608676/command/6686037)

# COMMAND ----------

# DBTITLE 1,Test for Automated Checks
 %py
 import json
 dbutils.notebook.exit(json.dumps({
   "status": "OK",
   "unsuppressed_table": "cyp_peri_raw",
   "suppressed_table": "cyp_peri_sup"
 }))