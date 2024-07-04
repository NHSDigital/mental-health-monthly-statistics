-- Databricks notebook source
-- DBTITLE 1,clear run records for $db_output.mha_monthly_unformatted
 %sql
 DELETE FROM $db_output.mha_monthly_unformatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.mha_monthly_unformatted RETAIN 8 HOURS;

-- COMMAND ----------

TRUNCATE TABLE $db_output.DETENTIONS_monthly;

TRUNCATE TABLE $db_output.SHORT_TERM_ORDERS_MONTHLY;

TRUNCATE TABLE $db_output.CTO_MONTHLY;