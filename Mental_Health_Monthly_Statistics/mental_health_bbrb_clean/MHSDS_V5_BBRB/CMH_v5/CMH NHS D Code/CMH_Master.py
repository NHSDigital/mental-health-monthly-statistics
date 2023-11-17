# Databricks notebook source
# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]
# end_month_id  = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]
# dbutils.widgets.dropdown("rp_startdate_12m", "2020-04-01", startchoices)
# dbutils.widgets.dropdown("rp_startdate_qtr", "2021-03-01", startchoices)
# dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
# dbutils.widgets.dropdown("start_month_id", "1441", monthid)
# dbutils.widgets.dropdown("end_month_id", "1452", monthid)
# dbutils.widgets.text("status","Provisional")
# dbutils.widgets.text("db_output","$user_id")
# dbutils.widgets.text("db_source","$mhsds_database")
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
params = {'rp_startdate_12m': str(rp_startdate_12m), 'rp_startdate_qtr': str(rp_startdate_qtr), 'rp_enddate': str(rp_enddate), 'start_month_id': start_month_id, 'end_month_id': end_month_id, 'db_output': db_output, 'db_source': db_source, 'status': status}
print(params)

# COMMAND ----------

# DBTITLE 1,Use this table to help pick parameters above using dropdown
 %sql
 -- end_month_id = uniqmonthid of the month you are reporting on
 -- start_month_id = end_month_id - 11
 -- rp_enddate = reportingperiodenddate of the end_month_id
 -- rp_startdate_12m = reportingperiodstartdate of the start_month_id
 -- rp_startdate_qtr = (quarterly only metric) reportingperiodstartdate of (end_month_id - 2)
 
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate from $db_source.mhs000header order by uniqmonthid desc

# COMMAND ----------

# DBTITLE 1,Create Sub-ICB/ICB/CCG/STP/Region Reference data and NHSE Pre-Processing Tables
dbutils.notebook.run('CMH_Prep/Create_STP_Region_Mapping', 0, params)
dbutils.notebook.run('CMH_Prep/NHSE_Pre_Processing_Tables', 0, params)

# COMMAND ----------

# DBTITLE 1,Create CMH Access Final Prep Tables
dbutils.notebook.run('CMH_Prep/CMH Access Reduced', 0, params)

# COMMAND ----------

# DBTITLE 1,Create CMH Admissions Final Prep Tables
dbutils.notebook.run('CMH_Prep/Acute_Admissions_with_or_without_contact', 0, params)

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.CMH_Admissions;
 REFRESH TABLE $db_output.CMH_Agg;
 REFRESH TABLE $db_output.CMH_AdmissionsNC_Monthly_Output

# COMMAND ----------

# DBTITLE 1,Create and Aggregate Tables for CMH Admissions and CMH Access
dbutils.notebook.run('CMH_Agg/CMH_Access_Agg', 0, params)
dbutils.notebook.run('CMH_Agg/CMH_Admissions_Agg', 0, params)

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.cmh_admissions_monthly;
 REFRESH TABLE $db_output.cmh_access_monthly

# COMMAND ----------

# DBTITLE 1,Unsuppressed Output
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_unsup;
 CREATE TABLE         $db_output.cmh_unsup USING DELTA AS
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
      WHEN MEASURE_ID = 'MHS106' THEN 'Adult and older adult acute admissions in the reporting period (this is based on a 3 month rolling RP)'
      WHEN MEASURE_ID = 'MHS106a' THEN 'Adult and older adult acute admissions for patients with contact in the prior year with mental health services, in the RP (this is based on a 3 month rolling RP)'
      WHEN MEASURE_ID = 'MHS106b' THEN 'Adult and older adult acute admissions for patients with no contact in the prior year with mental health services, in the RP (this is based on a 3 month rolling RP)'
      WHEN MEASURE_ID = 'MHS107a' THEN 'Percentage of adult and older adult acute admissions for patients with contact in the prior year with mental health services, in the RP (this is based on a 3 month rolling RP)'
      WHEN MEASURE_ID = 'MHS107b' THEN 'Percentage of adult and older adult acute admissions for patients with no contact in the prior year with mental health services, in the RP (this is based on a 3 month rolling RP)'
      END AS MEASURE_NAME,
 MEASURE_VALUE
 from $db_output.cmh_admissions_monthly --order by breakdown, measure_id, primary_level, primary_level_description
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
   WHEN MEASURE_ID = 'MHS108' THEN 'Number of people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts within the RP (this is based on a 12 month rolling RP)'
   WHEN MEASURE_ID = 'MHS124' THEN 'Number of referrals accessing community mental health services for adults and older adults with serious mental illness which received the second contact within the RP (this is based on a three month rolling RP)'
   WHEN MEASURE_ID = 'MHS125' THEN 'Median waiting time between referral and second contact for referrals accessing community mental health services for adults and older adults with serious mental illness which received the second contact within the RP (this is based on a three month rolling RP)'
   WHEN MEASURE_ID = 'MHS126' THEN '90th Percentile waiting time between referral and second contact for referrals accessing community mental health services for adults and older adults with serious mental illness which received the second contact within the RP (this is based on a three month rolling RP)'
   WHEN MEASURE_ID = 'MHS127' THEN 'Number of referrals accessing community mental health services for adults and older adults with serious mental illness still waiting for a second contact within the RP that were open at the end of the RP'
   WHEN MEASURE_ID = 'MHS128' THEN 'Median waiting time between referral and the end of the reporting period  for referrals accessing community mental health services for adults and older adults with serious mental illness still waiting for a second contact within the RP that were open at the end of the RP'
   WHEN MEASURE_ID = 'MHS129' THEN '90th Percentile waiting time between referral and the end of the reporting period for referrals accessing community mental health services for adults and older adults with serious mental illness still waiting for a second contact within the RP that were open at the end of the RP'
      END AS MEASURE_NAME,
 MEASURE_VALUE
 from $db_output.cmh_access_monthly order by breakdown, measure_id, primary_level, primary_level_description

# COMMAND ----------

# DBTITLE 1,Final Unsuppressed Output (Download)
 %sql
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
 MEASURE_NAME,
 MEASURE_VALUE
 FROM $db_output.cmh_unsup
 ORDER BY BREAKDOWN, MEASURE_ID, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION

# COMMAND ----------

# DBTITLE 1,Suppress Output
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_supp;
 CREATE TABLE         $db_output.cmh_supp USING DELTA AS
 ---England and Ethnicity breakdowns (no suppression)
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
 MEASURE_NAME,
 MEASURE_VALUE
 FROM $db_output.cmh_unsup
 WHERE BREAKDOWN IN ('England', 'England; Ethnicity (White British/Non-White British)')
 UNION ALL
 ---Sub-national counts suppression
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
 MEASURE_NAME,
 cast(case when cast(MEASURE_VALUE as float) < 5 then '9999999' else cast(cast(round(MEASURE_VALUE/5,0)*5 as float) as int) end as string) AS MEASURE_VALUE   
 FROM $db_output.cmh_unsup
 --added ICB, sub-ICB to breakdown list 8/9/22
 WHERE BREAKDOWN IN ('ICB of Residence', 'Sub ICB of Residence','CCG of Residence', 'Provider', 'STP', 'STP; Ethnicity (White British/Non-White British)', 'ICB of Residence; Ethnicity (White British/Non-White British)', 'Commissioning Region') 
                 AND MEASURE_ID NOT IN ('MHS107a', 'MHS107b','MHS125','MHS126','MHS128','MHS129')     
 UNION ALL
 ---Sub-national rates suppression
 SELECT
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
 case  when b.MEASURE_ID = 'MHS106a' and b.MEASURE_VALUE <5 then '9999999' 
 	  when c.MEASURE_ID = 'MHS106b' and c.MEASURE_VALUE <5 then '9999999' 
       else cast(cast(round(a.MEASURE_VALUE,0) as float) as int) end as MEASURE_VALUE 
 FROM $db_output.cmh_unsup a
 left join ( select * from $db_output.cmh_unsup where MEASURE_ID = 'MHS106a') b on a.BREAKDOWN = b.BREAKDOWN 
                                                     and a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
                                                     and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
 left join ( select * from $db_output.cmh_unsup where MEASURE_ID = 'MHS106b') c on a.BREAKDOWN = c.BREAKDOWN
                                                     and a.PRIMARY_LEVEL = c.PRIMARY_LEVEL
                                                     and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL
 WHERE a.BREAKDOWN IN ('ICB of Residence', 'Sub ICB of Residence','CCG of Residence', 'Provider', 'STP', 'STP; Ethnicity (White British/Non-White British)', 'ICB of Residence; Ethnicity (White British/Non-White British)', 'Commissioning Region') 
                   AND a.MEASURE_ID IN ('MHS107a', 'MHS107b')       
 
 UNION ALL
 ---Sub-national rates suppression
 SELECT
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
 CASE 
     WHEN b.MEASURE_ID = 'MHS124' AND b.MEASURE_VALUE <5 THEN '9999999' 
 	WHEN c.MEASURE_ID = 'MHS127' and c.MEASURE_VALUE <5 THEN '9999999' 
     ELSE cast(cast(round(a.MEASURE_VALUE,0) as float) as int)
     END as MEASURE_VALUE 
 FROM $db_output.cmh_unsup a
 left join ( select * from $db_output.cmh_unsup where MEASURE_ID = 'MHS124') b on a.BREAKDOWN = b.BREAKDOWN 
                                                     and a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
                                                     and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
                                                     and a.MEASURE_ID IN ('MHS125','MHS126') 
 left join ( select * from $db_output.cmh_unsup where MEASURE_ID = 'MHS127') c on a.BREAKDOWN = c.BREAKDOWN 
                                                     and a.PRIMARY_LEVEL = c.PRIMARY_LEVEL
                                                     and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL
                                                     and a.MEASURE_ID IN ('MHS128','MHS129')
 WHERE a.BREAKDOWN IN ('ICB of Residence', 'Sub ICB of Residence','CCG of Residence', 'Provider', 'STP', 'STP; Ethnicity (White British/Non-White British)', 'ICB of Residence; Ethnicity (White British/Non-White British)', 'Commissioning Region') 
                   AND a.MEASURE_ID IN ('MHS125','MHS126','MHS128','MHS129')  

# COMMAND ----------

# DBTITLE 1,Final Suppressed Output Table
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_supp_final;
 CREATE TABLE         $db_output.cmh_supp_final USING DELTA AS
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
 MEASURE_NAME,
 CASE WHEN MEASURE_VALUE = '9999999' THEN '*' ELSE MEASURE_VALUE END AS MEASURE_VALUE
 FROM $db_output.cmh_supp

# COMMAND ----------

# DBTITLE 1,Final Suppressed Output (Download)
 %sql
 select * from $db_output.cmh_supp_final
 where measure_value = '0'
 ORDER BY BREAKDOWN, MEASURE_ID, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "cmh_unsup",
  "suppressed_table": "cmh_supp_final"
}))