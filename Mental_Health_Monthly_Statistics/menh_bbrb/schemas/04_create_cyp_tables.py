# Databricks notebook source
 %sql
 -- DROP TABLE IF EXISTS $db_output.firstcont_final;
 CREATE TABLE IF NOT EXISTS $db_output.firstcont_final
 (
 UniqMonthID int,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 LADistrictAuth string,
 Person_ID string,
 RecordNumber string,
 UniqServReqID string,
 AccessLARN int,
 AccessCCGRN int, 
 AccessCCGProvRN int, 
 AccessProvRN int,
 AccessEngRN int, 
 AccessSTPRN int,
 AccessRegionRN int,
 Metric string    
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.perinatal_m_master;
 CREATE TABLE IF NOT EXISTS $db_output.perinatal_m_master
 (
 UniqMonthID string,
 Person_ID string,
 UniqServReqID string,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 LACode string,
 LowerEthnicityCode string,   
 LowerEthnicityName string,
 UpperEthnicity string,
 AttContacts string,
 NotAttContacts string,
 FYAccessLARN string, 
 FYAccessRNProv string,
 FYAccessCCGRN string,
 FYAccessSTPRN string,
 FYAccessRegionRN string,
 FYAccessEngRN string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.Act_cumulative_master;
 CREATE TABLE IF NOT EXISTS $db_output.Act_cumulative_master
 (
 UniqMonthID int,
 OrgIDProv string,
 Der_OrgComm string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 Der_ContAge string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Der_ContactDate date,
 Der_ContactOrder int,
 PROVIDER_NAME string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 TimeFromRefToFirstCont int,
 TimeFromRefToEndRP int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.Act_cumulative_master_first_contact;
 CREATE TABLE IF NOT EXISTS $db_output.Act_cumulative_master_first_contact
 (
 UniqMonthID int,
 OrgIDProv string,
 Der_OrgComm string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 Der_ContAge string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Der_ContactDate date,
 Der_ContactOrder int,
 PROVIDER_NAME string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 TimeFromRefToFirstCont int,
 TimeFromRefToEndRP int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.Act_cumulative_master_still_waiting;
 CREATE TABLE IF NOT EXISTS $db_output.Act_cumulative_master_still_waiting
 (
 UniqMonthID int,
 OrgIDProv string,
 Der_OrgComm string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 Der_ContAge string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Der_ContactDate date,
 Der_ContactOrder int,
 PROVIDER_NAME string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 TimeFromRefToFirstCont int,
 TimeFromRefToEndRP int
 ) USING DELTA