-- Databricks notebook source
-- DBTITLE 1,New tables created for Quarterly dashboard
-- DROP TABLE IF EXISTS $db_output.FYFV_Dashboard_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.FYFV_Dashboard_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.FYFV_Dashboard_level_values;
CREATE TABLE IF NOT EXISTS $db_output.FYFV_Dashboard_level_values (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.FYFV_Dashboard_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.FYFV_Dashboard_metric_values (metric string, metric_name string) USING DELTA;


-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.CCGOIS_unformatted
(
  REPORTING_PERIOD string, 
  STATUS string, 
  CCG string, 
  AgeRepPeriodEnd int, 
  Gender string, 
  CLUSTER string, 
  SUPER_CLUSTER string, 
  EMPLOYMENT_STATUS string, 
  METRIC_VALUE float,
  SOURCE_DB string
) 
USING DELTA 
PARTITIONED BY (REPORTING_PERIOD, STATUS)

-- COMMAND ----------

