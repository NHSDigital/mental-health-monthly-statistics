# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.ips_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.ips_referrals
 (
 UniqServReqID STRING, 
 Person_ID STRING,
 UniqMonthID BIGINT,
 RecordNumber STRING,
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 Der_FY STRING,
 ReferralRequestReceivedDate DATE,
 ServDischDate DATE,
 OrgIDProv STRING,
 IC_Rec_CCG STRING,
 Identifier STRING 
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ips_interventions;
 CREATE TABLE IF NOT EXISTS $db_output.ips_interventions
 (
 UniqMonthID BIGINT,
 OrgIDProv STRING,
 RecordNumber STRING,
 UniqServReqID STRING, 
 Der_SNoMEDProcQual STRING  
 ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_referrals_distinct;
 CREATE TABLE IF NOT EXISTS $db_output.ips_referrals_distinct
 (
 UniqServReqID STRING, 
 Person_ID STRING,
 UniqMonthID BIGINT,
 RecordNumber STRING,
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 Der_FY STRING,
 ReferralRequestReceivedDate DATE,
 ServDischDate DATE,
 OrgIDProv STRING,
 IC_Rec_CCG STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity;
 CREATE TABLE IF NOT EXISTS $db_output.ips_activity
 (
 UniqMonthID BIGINT,
 Person_ID STRING,
 RecordNumber STRING,
 OrgIDProv STRING,
 IC_Rec_CCG STRING,
 UniqServReqID STRING, 
 Identifier STRING,
 UniqCareContID STRING,
 Der_ContactDate DATE,
 Der_FY STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_distinct;
 CREATE TABLE IF NOT EXISTS $db_output.ips_activity_distinct
 (
 UniqMonthID BIGINT,
 Person_ID STRING,
 RecordNumber STRING,
 OrgIDProv STRING,
 IC_Rec_CCG STRING,
 UniqServReqID STRING, 
 UniqCareContID STRING,
 Der_ContactDate DATE,
 Der_FY STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ips_activity_order;
 CREATE TABLE IF NOT EXISTS $db_output.ips_activity_order
 (
 RecordNumber STRING,
 UniqMonthID BIGINT,
 Person_ID STRING,
 UniqServReqID STRING, 
 AccessFlag STRING,
 FYAccessFlag STRING,
 Der_ContactDate DATE
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ips_activity_agg;
 CREATE TABLE IF NOT EXISTS $db_output.ips_activity_agg
 (
 RecordNumber STRING,
 UniqMonthID BIGINT,
 Person_ID STRING,
 UniqServReqID STRING, 
 AccessFlag STRING,
 FYAccessFlag STRING,
 AccessDate DATE,
 TotalContacts INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_master;
 CREATE TABLE IF NOT EXISTS $db_output.ips_master
 (
 UniqServReqID STRING, 
 Person_ID STRING,
 UniqMonthID BIGINT,
 RecordNumber STRING,
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 Der_FY STRING,
 ReferralRequestReceivedDate DATE,
 ServDischDate date,
 Age_Band string,
 Der_Gender string,
 Der_Gender_Desc string,
 UpperEthnicity string,
 IMD_Decile string,
 OrgIDProv string,
 Provider_Name string,
 IC_Rec_CCG string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 AccessFlag int,
 FYAccessFlag int,
 AccessDate date,
 Contacts int
 ) USING DELTA