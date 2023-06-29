# Databricks notebook source
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
 CASE WHEN MEASURE_ID = 'MHS106' THEN 'Adult and older adult acute admissions in the reporting period (this is based on a 3 month rolling RP)'
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
 CASE WHEN MEASURE_ID = 'MHS108' THEN 'Number of people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts within the RP (this is based on a 12 month rolling RP)'
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
 WHERE BREAKDOWN IN ('England', 'Ethnicity (White British/Non-White British)')
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
 WHERE BREAKDOWN IN ('ICB', 'Sub ICB of Residence','CCG of Residence', 'Provider', 'STP', 'STP; Ethnicity (White British/Non-White British)', 'ICB; Ethnicity (White British/Non-White British)', 'Commissioning Region') 
                 AND MEASURE_ID NOT IN ('MHS107a', 'MHS107b')     
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
 WHERE a.BREAKDOWN IN ('ICB', 'Sub ICB of Residence','CCG of Residence', 'Provider', 'STP', 'STP; Ethnicity (White British/Non-White British)', 'ICB; Ethnicity (White British/Non-White British)', 'Commissioning Region') 
                   AND a.MEASURE_ID IN ('MHS107a', 'MHS107b')                                                   

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
 ORDER BY BREAKDOWN, MEASURE_ID, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "cmh_unsup",
  "suppressed_table": "cmh_supp_final"
}))