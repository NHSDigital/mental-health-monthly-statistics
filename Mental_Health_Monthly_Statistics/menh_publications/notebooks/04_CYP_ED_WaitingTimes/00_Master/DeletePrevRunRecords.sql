-- Databricks notebook source
-- DBTITLE 1,clear run records for $db_output.cyp_ed_wt_unformatted
 %sql
 DELETE FROM $db_output.cyp_ed_wt_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.cyp_ed_wt_unformatted RETAIN 8 HOURS;

-- COMMAND ----------

 %sql
 DELETE FROM $db_output.cyp_ed_wt_step4
 WHERE UniqMonthID = '$month_id'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.cyp_ed_wt_step4 RETAIN 8 HOURS;

-- COMMAND ----------

 %sql
 DELETE FROM $db_output.cyp_ed_wt_step6
 WHERE UniqMonthID = '$month_id'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.cyp_ed_wt_step6 RETAIN 8 HOURS;