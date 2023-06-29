-- Databricks notebook source
--  %md
--  
--  The following tables are created in this notebook:
--  - Main_monthly_unformatted
--  - awt_unformatted
--  - CYP_2nd_contact_unformatted
--  - CAP_unformatted
--  - CYP_monthly_unformatted
--  - Ascof_unformatted
--  - all_products_cached

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_unformatted
(
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
    BREAKDOWN string,
    PRIMARY_LEVEL string,
    PRIMARY_LEVEL_DESCRIPTION string,
    SECONDARY_LEVEL string,
    SECONDARY_LEVEL_DESCRIPTION string,
    METRIC string,
    METRIC_VALUE float,
    SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.awt_unformatted
(
  REPORTING_PERIOD_START DATE,
  REPORTING_PERIOD_END DATE,
  STATUS STRING,
  BREAKDOWN STRING,
  LEVEL STRING,
  LEVEL_DESCRIPTION STRING,
  METRIC STRING,
  METRIC_VALUE FLOAT,
  SOURCE_DB string,
  SECONDARY_LEVEL string,
  SECONDARY_LEVEL_DESCRIPTION string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.CYP_2nd_contact_unformatted
(
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
    BREAKDOWN string,
    PRIMARY_LEVEL string,
    PRIMARY_LEVEL_DESCRIPTION string,
    SECONDARY_LEVEL string,
    SECONDARY_LEVEL_DESCRIPTION string,
    METRIC string,
    METRIC_VALUE float,
    SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.CAP_unformatted
(
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
    BREAKDOWN string,
    LEVEL string,
    LEVEL_DESCRIPTION string,
    CLUSTER string,
    METRIC string,
    METRIC_VALUE float,
    SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.CYP_monthly_unformatted
(
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
    BREAKDOWN string,
    PRIMARY_LEVEL string,
    PRIMARY_LEVEL_DESCRIPTION string,
    SECONDARY_LEVEL string,
    SECONDARY_LEVEL_DESCRIPTION string,
    METRIC string,
    METRIC_VALUE FLOAT,
    SOURCE_DB string
)
USING DELTA 
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.Ascof_unformatted
(
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
	BREAKDOWN STRING,
	LEVEL_ONE STRING,
	LEVEL_ONE_DESCRIPTION STRING,
	LEVEL_TWO STRING,
	LEVEL_TWO_DESCRIPTION STRING,
    LEVEL_THREE STRING,
    METRIC STRING,
	METRIC_VALUE FLOAT,
    SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS $db_output.all_products_cached (
  PRODUCT_NO INT, 
  REPORTING_PERIOD_START DATE, 
  REPORTING_PERIOD_END DATE, 
  STATUS STRING, 
  BREAKDOWN STRING, 
  PRIMARY_LEVEL STRING, 
  PRIMARY_LEVEL_DESCRIPTION STRING, 
  SECONDARY_LEVEL STRING, 
  SECONDARY_LEVEL_DESCRIPTION STRING, 
  METRIC STRING, 
  METRIC_NAME STRING, 
  METRIC_VALUE FLOAT,
  SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, PRODUCT_NO, STATUS)