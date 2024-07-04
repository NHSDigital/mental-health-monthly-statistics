# Databricks notebook source
# %sql
# DROP TABLE IF EXISTS $db_output.audit_menh_analysis

# COMMAND ----------

 %sql
 -- 01/03/2022: BITC-3061 (menh_analysis: Create audit table)
 CREATE TABLE IF NOT EXISTS $db_output.audit_menh_analysis (
   MONTH_ID int,
   STATUS string,
   REPORTING_PERIOD_START date,
   REPORTING_PERIOD_END date,
   SOURCE_DB string,
   RUN_START timestamp,
   RUN_END timestamp,
   ADHOC_DESC string
 ) USING DELTA PARTITIONED BY (MONTH_ID, STATUS)