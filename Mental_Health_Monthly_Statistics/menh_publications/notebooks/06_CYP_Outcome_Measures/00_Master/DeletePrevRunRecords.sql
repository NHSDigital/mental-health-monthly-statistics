-- Databricks notebook source
-- DBTITLE 1,clear run records for $db_output.cyp_peri_monthly_unformatted
 %sql
 DELETE FROM $db_output.cyp_peri_monthly_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.cyp_peri_monthly_unformatted RETAIN 8 HOURS;

-- COMMAND ----------

 %sql
 Truncate table $db_output.closed_refs

-- COMMAND ----------

 %sql
 Truncate table $db_output.cont_final

-- COMMAND ----------

 %sql
 Truncate table $db_output.assessments



-- COMMAND ----------

 %sql
 Truncate table $db_output.ass_final

-- COMMAND ----------

 %sql
 Truncate table $db_output.cyp_outcomes

-- COMMAND ----------

 %sql
 Truncate table $db_output.cyp_outcomes_output

-- COMMAND ----------

 %sql
 Truncate table $db_output.MHS95_Main
