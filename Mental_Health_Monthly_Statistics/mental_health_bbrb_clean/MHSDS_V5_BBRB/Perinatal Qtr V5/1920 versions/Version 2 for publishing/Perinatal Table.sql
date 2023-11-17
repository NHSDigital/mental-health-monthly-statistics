-- Databricks notebook source
DROP TABLE IF EXISTS $personal_db.Perinatal

-- COMMAND ----------

CREATE TABLE $personal_db.Perinatal
(REPORTING_PERIOD_START DATE,
 REPORTING_PERIOD_END DATE,
 STATUS STRING,
 BREAKDOWN STRING,
 LEVEL STRING,
 LEVEL_DESCRIPTION STRING, 
 LEVEL_2 STRING,
 LEVEL_2_Description STRING,
 METRIC STRING,
 METRIC_VALUE INT)

-- COMMAND ----------

DROP TABLE IF EXISTS $personal_db.Perinatal_Output

-- COMMAND ----------

CREATE TABLE $personal_db.Perinatal_Output
(REPORTING_PERIOD_START DATE,
 REPORTING_PERIOD_END DATE,
 STATUS STRING,
 BREAKDOWN STRING,
 LEVEL STRING,
 LEVEL_DESCRIPTION STRING, 
 LEVEL_2 STRING,
 LEVEL_2_Description STRING,
 METRIC STRING,
 METRIC_VALUE INT)

-- COMMAND ----------

DROP TABLE IF EXISTS $personal_db.PerinatalPeriod_DF

-- COMMAND ----------

CREATE TABLE $personal_db.PerinatalPeriod_DF
(Version STRING, 
Person_ID_Mother STRING, 
UniqPregID STRING,
StartDate DATE,
EndDate DATE, 
AgeAtBookingMother INT,
EthnicCategoryMother STRING,
EthnicCategoryMother_Description STRING,
IC_Rec_CCG STRING, 
NAME STRING,
rnk INT,
preg_rnk INT)

-- COMMAND ----------

DROP TABLE IF EXISTS $personal_db.MHSDSPerinatalPeriodMH_DF

-- COMMAND ----------

CREATE TABLE $personal_db.MHSDSPerinatalPeriodMH_DF
(Person_ID STRING,
Person_ID_Mother STRING,
UniqPregID STRING, 
StartDate DATE,
EndDate DATE, 
AgeAtBookingMother INT, 
EthnicCategoryMother STRING,  
EthnicCategoryMother_DESCRIPTION STRING,
IC_Rec_CCG STRING, 
NAME STRING,
rnk INT,
preg_rnk INT)