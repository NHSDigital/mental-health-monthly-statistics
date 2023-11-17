-- Databricks notebook source
-- CREATE WIDGET TEXT MONTH_ID DEFAULT "1449";
-- CREATE WIDGET TEXT MSDS_15 DEFAULT "$mat15_database";
-- CREATE WIDGET TEXT MSDS_2 DEFAULT "mat_pre_clear";
-- CREATE WIDGET TEXT MHSDS DEFAULT "$mhsds_database";
-- CREATE WIDGET TEXT RP_STARTDATE DEFAULT "2020-01-01";
-- CREATE WIDGET TEXT RP_ENDDATE DEFAULT "2020-12-31";
-- CREATE WIDGET TEXT personal_db DEFAULT "$user_id";
-- CREATE WIDGET TEXT prev_months DEFAULT "12";

-- COMMAND ----------

%py

import os

dbutils.widgets.text("MONTH_ID", "$MONTH_ID", "MONTH_ID")
MONTH_ID = dbutils.widgets.get("MONTH_ID")

dbutils.widgets.text("MSDS_15", "$MSDS_15", "MSDS_15")
MSDS_15 = dbutils.widgets.get("MSDS_15")

dbutils.widgets.text("MSDS_2", "$MSDS_2", "MSDS_2")
MSDS_2 = dbutils.widgets.get("MSDS_2")

dbutils.widgets.text("MHSDS", "$MHSDS", "MHSDS")
MHSDS = dbutils.widgets.get("MHSDS")

dbutils.widgets.text("RP_STARTDATE", "$RP_STARTDATE", "RP_STARTDATE")
RP_STARTDATE = dbutils.widgets.get("RP_STARTDATE")

dbutils.widgets.text("RP_ENDDATE", "$RP_ENDDATE", "RP_ENDDATE")
RP_ENDDATE = dbutils.widgets.get("RP_ENDDATE")

dbutils.widgets.text("personal_db", "$personal_db", "personal_db")
personal_db = dbutils.widgets.get("personal_db")

dbutils.widgets.text("prev_months", "$prev_months", "prev_months")
prev_months = dbutils.widgets.get("prev_months")

params = {'MONTH_ID': str(MONTH_ID), 'MSDS_15': str(MSDS_15), 'MSDS_2': str(MSDS_2), 'MHSDS': str(MHSDS), 'RP_STARTDATE': str(RP_STARTDATE), 'RP_ENDDATE': str(RP_ENDDATE), 'personal_db': str(personal_db), 'prev_months': str(prev_months)}

print(params)

-- COMMAND ----------

%py

dbutils.notebook.run('Perinatal Table', 0, params)

-- COMMAND ----------

%py

dbutils.notebook.run('Perinatal Prep v2', 0, params)

-- COMMAND ----------

%py

dbutils.notebook.run('Perinatal Agg v2', 0, params)

-- COMMAND ----------

SELECT 
*
FROM 
$personal_db.Perinatal_Output

-- COMMAND ----------

SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
LEVEL,
LEVEL_DESCRIPTION,
LEVEL_2,
LEVEL_2_DESCRIPTION,
METRIC,
CASE 
  WHEN METRIC_VALUE IS NULL THEN "*"
  ELSE METRIC_VALUE
  END AS METRIC_VALUE
FROM 
$personal_db.Perinatal_Output