# Databricks notebook source
# DBTITLE 1,table to store integrity_rules METADATA
 %sql
 CREATE TABLE IF NOT EXISTS $db.integrity_rules(
   MeasureNumber STRING,
   MeasureName STRING,
   MeasureDescription STRING,
   DataItem STRING,
   Denominator STRING,
   Numerator STRING
 ) USING DELTA;

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db.validity_rules(
   MeasureNumber STRING,
   MeasureName STRING,
   MeasureDescription STRING,
   DataItem STRING,
   Denominator STRING,
   Valid STRING,
   Other STRING,
   Default STRING,
   Invalid STRING,
   Missing STRING
 ) USING DELTA;

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db.dq_coverage_monthly_csv(
   MONTH_ID INT,
   REPORTING_PERIOD_START STRING,
   REPORTING_PERIOD_END STRING,
   STATUS STRING,
   ORGANISATION_CODE STRING,
   ORGANISATION_NAME STRING,
   TABLE_NAME STRING,
   COVERAGE_COUNT BIGINT,
   SOURCE_DB string
 ) USING DELTA;

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db.dq_coverage_monthly_pbi(
   MONTH_ID INT,
   STATUS STRING,
   ORGANISATION_CODE STRING,
   ORGANISATION_NAME STRING,
   TABLE_NAME STRING,
   SUM_OF_PERIOD_NAME STRING,
   X STRING,
   PERIOD_NAME STRING,
   PERIOD_END_DATE STRING,
   SOURCE_DB string
 ) USING DELTA;

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db.DQ_VODIM_monthly_csv(
   Month_Id INT,
   Reporting_Period STRING,
   Status STRING,
   Reporting_Level STRING,
   Provider_Code STRING,
   Provider_Name STRING,
   DQ_Measure STRING,
   DQ_Measure_Name STRING,
   DQ_Result STRING,
   DQ_Dataset_Metric_ID STRING,
   Unit STRING,
   Value STRING,
   SOURCE_DB string
 ) USING DELTA;

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db.DQ_VODIM_monthly_pbi(
   Month_Id INT,
   Reporting_Period STRING,
   Status STRING,
   Reporting_Level STRING,
   Provider_Code STRING,
   Provider_Name STRING,
   DQ_Measure STRING,
   DQ_Result STRING,
   DQ_Dataset_Metric_ID STRING,
   Count INT,
   Percentage STRING,
   SOURCE_DB string
 ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,DQMI Coverage table
 %sql
 
 CREATE TABLE IF NOT EXISTS $db.DQMI_Coverage(
   Month_ID INT,
   Period STRING,
   Dataset STRING,
   Status STRING,
   Organisation STRING,
   OrganisationName STRING,
   ExpectedToSubmit INT,
   Submitted INT
 ) USING DELTA

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db.DQMI_Monthly_Data(
   Month_ID INT,
   Period STRING,
   Dataset STRING,
   Status STRING,
   DataProviderId STRING,
   DataProviderName STRING,
   DataItem STRING,
   CompleteNumerator INT,
   CompleteDenominator INT,
   ValidNumerator INT,
   DefaultNumerator INT,
   ValidDenominator INT,
   SOURCE_DB string
 ) USING DELTA;

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db.DQMI_Integrity(
   Month_ID STRING,
   Period STRING,
   Dataset STRING,
   Status STRING,
   OrgIdProv STRING,
   MeasureId STRING,
   MeasureName STRING,
   Numerator INT,
   Denominator INT,
   SOURCE_DB string
 ) USING DELTA;

# COMMAND ----------

