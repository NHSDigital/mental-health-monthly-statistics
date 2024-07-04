# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.contact_prep;
 CREATE TABLE IF NOT EXISTS $db_output.contact_prep
 (
 UniqMonthID int,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 ReferralRequestReceivedDateTime timestamp,
 UniqCareContID string,
 AttendStatus string,
 ConsMechanismMH string,
 CareContDate date,
 CareContTime timestamp,
 CareContDateTime timestamp
 ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.attended_direct_contact_order;
 CREATE TABLE IF NOT EXISTS $db_output.attended_direct_contact_order
 (
 UniqMonthID int,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedDateTime timestamp,
 UniqCareContID string,
 AttendStatus string,
 ConsMechanismMH string,
 CareContDate date,
 CareContTime timestamp,
 CareContDateTime timestamp,
 Der_ContactOrder int
 ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.community_crisis_prep;
 CREATE TABLE IF NOT EXISTS $db_output.community_crisis_prep
 (
 CCG_Code string,
 CCG_Name string,
 OrgIDProv string,
 Provider_Name string,
 ClinRespPriorityType string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 ReferralRequestReceivedDateTime timestamp,
 AGE_GROUP string,
 UniqServReqID string,
 SourceOfReferralMH string,
 ServTeamTypeRefToMH string,
 UniqCareContID string,
 AttendStatus string,
 ConsMechanismMH string,
 CareContDate date,
 CareContTime timestamp,
 CareContDateTime timestamp
  ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.community_crisis_master;
 CREATE TABLE IF NOT EXISTS $db_output.community_crisis_master
 (
 UniqServReqID string,
 CCG_Code string,
 CCG_Name string,
 OrgIDProv string,
 Provider_Name string,
 ClinRespPriorityType string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 ReferralRequestReceivedDateTime timestamp,
 AGE_GROUP string,
 SourceOfReferralMH string,
 ServTeamTypeRefToMH string,
 UniqCareContID string,
 AttendStatus string,
 ConsMechanismMH string,
 CareContDate date,
 CareContTime timestamp,
 CareContDateTime timestamp,
 minutes_between_ref_and_first_cont float
 ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.liason_psychiatry_prep;
 CREATE TABLE IF NOT EXISTS $db_output.liason_psychiatry_prep
 (
 UniqServReqID string,
 CCG_Code  string,
 CCG_Name string,
 OrgIDProv string,
 Provider_Name string,
 ClinRespPriorityType string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 ReferralRequestReceivedDateTime timestamp,
 AGE_GROUP string,
 SourceOfReferralMH string,
 ServTeamTypeRefToMH string,
 UniqCareContID string,
 AttendStatus string,
 ConsMechanismMH string,
 CareContDate date,
 CareContTime timestamp,
 CareContDateTime timestamp
  ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.liason_psychiatry_master;
 CREATE TABLE IF NOT EXISTS $db_output.liason_psychiatry_master
 (
 UniqServReqID string,
 CCG_Code string,
 CCG_Name string,
 OrgIDProv string,
 Provider_Name string,
 ClinRespPriorityType string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 ReferralRequestReceivedDateTime timestamp,
 AGE_GROUP string,
 SourceOfReferralMH string,
 ServTeamTypeRefToMH string,
 UniqCareContID string,
 AttendStatus string,
 ConsMechanismMH string,
 CareContDate date,
 CareContTime timestamp,
 CareContDateTime timestamp,
 minutes_between_ref_and_first_cont float
 ) USING DELTA 