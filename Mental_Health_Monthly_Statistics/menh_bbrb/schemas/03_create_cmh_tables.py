# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_outpatient_refs3;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_access_outpatient_refs3
 (
 UniqMonthID int,
 OrgIDProv string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 IC_Rec_CCG string,
 ReferralRequestReceivedDate date,
 ServDischDate Date,
 Ref_MnthNum int
 ) USING DELTA 

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_activity;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_activity
 (
 Der_ActivityType string,
 Der_ActivityUniqID string,
 Person_ID string,
 Der_PersonID string,
 UniqMonthID BIGINT,
 OrgIDProv string,
 RecordNumber BIGINT,
 UniqServReqID string,    
 Der_ContactDate date,
 Der_ContactTime timestamp,
 Der_DirectContactOrder int,
 Der_RefDirectContactOrder int
 ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_inpatients;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_inpatients 
 (
 MHS501UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqHospProvSpellID string,
 UniqServReqID string,
 UniqPersRefID string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 MHS502UniqID BIGINT,
 UniqWardStayID string,
 StartDateWardStay date,
 StartTimeWardStay timestamp,
 SiteIDOfWard string,
 WardType string,
 SpecialisedMHServiceCode string,
 MHAdmittedPatientClass string,
 EndDateWardStay date,
 EndTimeWardStay timestamp,
 Der_HospSpellRecordOrder int,
 Der_FirstWardStayRecord int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_activity_linked;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_activity_linked
 (
 UniqMonthID int,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 Der_DirectContactOrder int
 ) USING DELTA 

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_rolling_activity;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_rolling_activity
 (
 UniqMonthID int,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 AccessCCGRN int,
 AccessProvRN int,
 AccessEngRN int,
 AccessSTPRN int,
 AccessRegionRN int
 ) USING DELTA 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_admissions;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_admissions 
 (
 UniqMonthID BIGINT,
 UniqHospProvSpellID string,
 Person_ID string,
 NotWhiteBritish int,
 WhiteBritish int,
 UpperEthnicity string,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 Adm_month date,
 WardIntendedClinCareMH string,
 AgeServReqRecDate int,
 UniqServReqID string,
 RefMonth int,
 RefRecordNumber BIGINT,
 RN int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_previous_contacts;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_previous_contacts 
 (
 UniqHospProvSpellID string,
 OrgIDProv string,
 Person_ID string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 UniqServReqID string,
 Der_ActivityUniqID string,
 Cont_OrgIDProv string,
 Der_ActivityType string,
 AttendStatus string,
 ConsMechanismMH string,   
 Der_ContactDate date,
 Contact_spell string,
 TimeToAdm int,
 RN int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_agg;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_agg 
 (
 ReportingPeriodStartDate date,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 WNW_Ethnicity string,
 UpperEthnicity string,
 Admissions int,
 Contact int,
 NoContact int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_outpatient_medians_refs3;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_access_outpatient_medians_refs3
 (
 UniqMonthID int,
 OrgIDProv string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 IC_Rec_CCG string,
 ReferralRequestReceivedDate date,
 ServDischDate Date,
 Ref_MnthNum int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_activity_linked_medians;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_activity_linked_medians
 (
 UniqMonthID int,
 ReportingPeriodStartDate Date,
 ReportingPeriodEndDate Date,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string, 
 CCG_Name string, 
 Region_Code string, 
 Region_Name string,
 STP_Code string,
 STP_Name string,
 Der_ContactDate date,
 ServDischDate date,
 ReferralRequestReceivedDate date,
 Der_DirectContactOrder int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_rolling_activity_medians;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_rolling_activity_medians
 (
 UniqMonthID int,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 Der_ContactDate date,
 Der_DirectContactOrder int,
 ServDischDate date,
 TIME_TO_2ND_CONTACT int,
 TimeFromRefToEndRP int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_rolling_activity_medians_2nd_cont;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_rolling_activity_medians_2nd_cont
 (
 UniqMonthID int,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 Der_ContactDate date,
 Der_DirectContactOrder int,
 ServDischDate date,
 TIME_TO_2ND_CONTACT int,
 TimeFromRefToEndRP int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_rolling_activity_medians_still_waiting;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_rolling_activity_medians_still_waiting
 (
 UniqMonthID int,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 Region_Code string,
 Region_Name string,
 STP_Code string,
 STP_Name string,
 Person_ID string,
 RecordNumber BIGINT,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 Der_ContactDate date,
 Der_DirectContactOrder int,
 ServDischDate date,
 TIME_TO_2ND_CONTACT int,
 TimeFromRefToEndRP int
 ) USING DELTA