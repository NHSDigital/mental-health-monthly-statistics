-- Databricks notebook source
-- CREATE WIDGET TEXT MONTH_ID DEFAULT "1449";
-- CREATE WIDGET TEXT MSDS_15 DEFAULT "$mat_1.5";
-- CREATE WIDGET TEXT MSDS_2 DEFAULT "$maternity";
-- CREATE WIDGET TEXT MHSDS DEFAULT "$mhsds";
-- CREATE WIDGET TEXT RP_STARTDATE DEFAULT "2020-01-01";
-- CREATE WIDGET TEXT RP_ENDDATE DEFAULT "2020-12-31";
-- CREATE WIDGET TEXT personal_db DEFAULT "$personal_db";
-- CREATE WIDGET TEXT prev_months DEFAULT "12";
--CREATE WIDGET TEXT FY_START DEFAULT "2020-04-01";

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

 dbutils.widgets.text("FY_START", "$FY_START", "FY_START")
 FY_START = dbutils.widgets.get("FY_START")

 params = {'MONTH_ID': str(MONTH_ID), 'MSDS_15': str(MSDS_15), 'MSDS_2': str(MSDS_2), 'MHSDS': str(MHSDS), 'RP_STARTDATE': str(RP_STARTDATE), 'RP_ENDDATE': str(RP_ENDDATE), 'personal_db': str(personal_db), 'prev_months': str(prev_months), 'FY_START': str(FY_START)}

 print(params)

-- COMMAND ----------

 %py
 spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

-- COMMAND ----------

 %py

 dbutils.notebook.run('Perinatal_Table', 0, params)

-- COMMAND ----------

 %py

 dbutils.notebook.run('Perinatal_Prep_v4', 0, params)

-- COMMAND ----------

TRUNCATE TABLE $personal_db.Perinatal

-- COMMAND ----------

 %py

 dbutils.notebook.run('Table_1_measures_and_breakdowns', 0, params)

-- COMMAND ----------

 %py

 dbutils.notebook.run('Table_2_measures_and_breakdowns', 0, params)

-- COMMAND ----------

 %py

 dbutils.notebook.run('Table_3_measures_and_breakdowns', 0, params)

-- COMMAND ----------

 %py

 dbutils.notebook.run('Table_4_measures_and_breakdowns', 0, params)

-- COMMAND ----------

OPTIMIZE $personal_db.Perinatal

-- COMMAND ----------

-- DBTITLE 1,Unsuppressed data (Perinatal Qx_yy_yy Data_Unrounded)
SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
CASE 
  WHEN BREAKDOWN = 'CCG' THEN 'Sub ICB / CCG of GP Practice or Residence'
  WHEN BREAKDOWN = 'STP' THEN 'ICB / STP'
  ELSE BREAKDOWN
  END AS BREAKDOWN,
LEVEL,
LEVEL_DESCRIPTION,
LEVEL_2,
LEVEL_2_DESCRIPTION,
METRIC,
METRIC_VALUE
FROM $personal_db.Perinatal
WHERE LEVEL_DESCRIPTION NOT LIKE '%SUB-ICB REPORTING ENTITY%'

-- COMMAND ----------

 %py

 dbutils.notebook.run('Perinatal_output', 0, params)

-- COMMAND ----------

DROP TABLE IF EXISTS $personal_db.perinatal_sup;
CREATE TABLE         $personal_db.perinatal_sup AS
SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
CASE 
  WHEN BREAKDOWN = 'CCG' THEN 'Sub ICB / CCG of GP Practice or Residence'
  WHEN BREAKDOWN = 'STP' THEN 'ICB / STP'
  ELSE BREAKDOWN
  END AS BREAKDOWN,
LEVEL,
LEVEL_DESCRIPTION,
LEVEL_2,
LEVEL_2_DESCRIPTION,
METRIC AS METRIC_ID,
CASE 
WHEN METRIC = 'PMH23b' THEN 'Number of people aged 16 or over in the period between booking and twenty four months post pregnancy in the reporting period'
WHEN METRIC = 'PMH01b' THEN 'Number of people aged 16 or over in the period between booking and twelve months post pregnancy in the reporting period'
WHEN METRIC = 'PMH24b' THEN 'Number of people aged 16 or over in the period between booking and twenty four months post pregnancy with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH02b' THEN 'Number of people aged 16 or over in the period between booking and twelve months post pregnancy with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH25b' THEN 'Number of people aged 16 or over in the period between twelve and twenty four months post pregnancy who are continuing with services with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH26b' THEN 'Number of people aged 16 or over in the period between twelve and twenty four months post pregnancy who are new to services with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH06a' THEN 'Number of people aged 16 or over in the perinatal period in contact with specialist perinatal mental health services in the reporting period and time between booking and twelve months post pregnancy (either inpatient or community services)'
WHEN METRIC = 'PMH07a' THEN 'Number of people aged 16 or over in the perinatal period who have spent time in a Mother and Baby Unit in the reporting period and during the time between booking and twelve months post pregnancy'
WHEN METRIC = 'PMH27a' THEN 'Number of people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between booking and twenty four months post pregnancy'
WHEN METRIC = 'PMH08a' THEN 'Number of people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between booking and twelve months post pregnancy'
WHEN METRIC = 'PMH28a' THEN 'Number of people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between twelve and twenty four months post pregnancy who are continuing with services'
WHEN METRIC = 'PMH29a' THEN 'Number of people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between twelve and twenty four months post pregnancy who are new to services'
WHEN METRIC = 'PMH30a' THEN 'Number of maternities for people aged 16 or over in the period between booking and twenty four months post pregnancy in the reporting period'
WHEN METRIC = 'PMH18a' THEN 'Number of maternities for people aged 16 or over in the period between booking and twelve months post pregnancy in the reporting period'
WHEN METRIC = 'PMH31a' THEN 'Number of maternities for people aged 16 or over in the period between booking and twenty four months post pregnancy with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH19a' THEN 'Number of maternities for people aged 16 or over in the period between booking and twelve months post pregnancy with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH32a' THEN 'Number of maternities for people aged 16 or over in the period between twelve and twenty four months post pregnancy who are continuing with services with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH33a' THEN 'Number of maternities for people aged 16 or over in the period between twelve and twenty four months post pregnancy who are new to services with a mental health referral open in in the reporting period and during the perinatal period'
WHEN METRIC = 'PMH20a' THEN 'Number of maternities for people aged 16 or over in the perinatal period in contact with specialist perinatal mental health services in the reporting period and time between booking and twelve months post pregnancy (either inpatient or community services)'
WHEN METRIC = 'PMH21a' THEN 'Number of maternities for people aged 16 or over in the perinatal period who have spent time in a Mother and Baby Unit in the reporting period and during the time between booking and twelve months post pregnancy'
WHEN METRIC = 'PMH34a' THEN 'Number of maternities for people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between booking and twenty four months post pregnancy'
WHEN METRIC = 'PMH22a' THEN 'Number of maternities for people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between booking and twelve months post pregnancy'
WHEN METRIC = 'PMH35a' THEN 'Number of maternities for people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between twelve and twenty four months post pregnancy who are continuing with services'
WHEN METRIC = 'PMH36a' THEN 'Number of maternities for people aged 16 or over in the perinatal period in contact with specialist community based perinatal mental health services in the reporting period and during time between twelve and twenty four months post pregnancy who are new to services'
END AS METRIC_NAME,
CASE 
  WHEN (METRIC_VALUE IS NULL OR METRIC_VALUE = "*") THEN "*"
  ELSE CAST(METRIC_VALUE as INT)
  END AS METRIC_VALUE
FROM 
$personal_db.Perinatal_Output

-- COMMAND ----------

-- DBTITLE 1,Suppressed data (Perinatal Qx_yy_yy_Data)
select * from $personal_db.perinatal_sup WHERE LEVEL_DESCRIPTION NOT LIKE '%SUB-ICB REPORTING ENTITY%'

-- COMMAND ----------

 %py
 import json
 dbutils.notebook.exit(json.dumps({
   "status": "OK",
   "unsuppressed_table": "Perinatal",
   "suppressed_table": "perinatal_sup"
 }))

-- COMMAND ----------


select * from $personal_db.perinatal_sup
WHERE 
LEVEL = 'UNKNOWN' AND BREAKDOWN = 'Sub ICB / CCG of GP Practice or Residence'
ORDER BY METRIC_ID