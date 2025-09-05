# Databricks notebook source
 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps_in_scope;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_in_scope
 (      
 ORG_CODE                STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.months;
 CREATE TABLE IF NOT EXISTS $db_output.months
 (
 UniqMonthID BIGINT,
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE
 )USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps_hospprovspell;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_hospprovspell
 (
 UniqServReqID STRING,
 Person_ID STRING,
 UniqMonthID BIGINT,
 OrgIDProv STRING,
 UniqHospProvSpellID STRING,
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 StartDateHospProvSpell DATE,
 DischDateHospProvSpell DATE
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps_wardstay;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_wardstay
 (
 UniqWardStayID STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName STRING,
 WardType STRING,
 BedDaysWSEndRP INT,
 Person_ID STRING,
 UniqMonthID BIGINT,
 UniqHospProvSpellID STRING,
 OrgIDProv STRING,
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 StartDateWardStay DATE,
 EndDateWardStay DATE,
 Acute_Bed STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_referrals
 (
 UniqServReqID STRING,
 ReferralRequestReceivedDate DATE,
 Person_ID STRING,
 ServDischDate DATE,
 UniqMonthID BIGINT,
 ReasonOAT STRING,
 PrimReasonReferralMH STRING,
 OrgIDProv STRING,
 OrgIDReferringOrg STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps_onwardreferrals;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_onwardreferrals
 (
 UniqServReqID STRING,
 Person_ID STRING,
 UniqMonthID BIGINT,
 OrgIDProv STRING,
 OATReason STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps_mha;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_mha
 (
 UniqMonthID BIGINT,
 legalstatuscode STRING,
 Person_ID STRING,
 OrgIDProv STRING,
 StartDateMHActLegalStatusClass STRING,
 UniqSubmissionID STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.overlapping_oaps_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.overlapping_oaps_referrals
 (
 UniqServReqID STRING,
 ReferralRequestReceivedDate DATE,
 Person_ID STRING,
 ServDischDate DATE,
 UniqMonthID BIGINT,
 ReasonOAT STRING,
 PrimReasonReferralMH STRING,
 OrgIDProv STRING,
 OrgIDReferring STRING,
 NewServDischDate DATE
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.oaps;
 CREATE TABLE IF NOT EXISTS $db_output.oaps
 (
 UniqMonthID BIGINT,
 Person_ID STRING,
 OrgIDProv STRING,
 SubICBGPRes STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Group_Lower_Level STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,
 ReasonOAT STRING,
 ReasonOATName STRING,
 UniqServReqID STRING,
 NewServDischDate DATE,
 ReferralRequestReceivedDate DATE,
 PrimReasonReferralMH STRING,
 PrimReasonReferralMHName STRING,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName STRING,
 WardType STRING,
 BedDaysWSEndRP INT,
 OrgIDReceiving STRING,
 OrgIDSubmitting STRING,
 LegalStatusCode STRING,
 LegalStatusName STRING,
 Bed_Days_Month_WS INT,
 Bed_Days_Quarter_WS INT,
 Bed_Days_Year_WS INT,
 Bed_Days_Month_HS INT,
 Bed_Days_Quarter_HS INT,
 Bed_Days_Year_HS INT,
 SumBedDaysHS INT,
 InScope STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_month;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_month
 (
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 UniqMonthID BIGINT,
 Person_ID STRING,
 OrgIDProv STRING,
 SubICBGPRes STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Band STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,
 AutismStatus STRING,
 AutismStatus_desc STRING,
 LDStatus STRING,
 LDStatus_desc STRING,
 ReasonOAT STRING,
 ReasonOATName STRING,
 UniqServReqID STRING,
 NewServDischDate DATE,
 ReferralRequestReceivedDate DATE,
 PrimReasonReferralMH STRING,
 PrimReasonReferralMHName STRING,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName STRING,
 Acute_Bed STRING,
 StartMHAdmittedPatientClass STRING,
 StartMHAdmittedPatientClassName STRING,
 StartAcute_Bed STRING,
 ActiveMHAdmittedPatientClass STRING,
 ActiveMHAdmittedPatientClassName STRING,
 ActiveAcute_Bed STRING,
 EndMHAdmittedPatientClass STRING,
 EndMHAdmittedPatientClassName STRING,
 EndAcute_Bed STRING,
 WardType STRING,
 BedDaysWSEndRP INT,
 OrgIDReceiving STRING,
 ReceivingProvider_Name STRING,
 OrgIDSubmitting STRING,
 SendingProvider_Name STRING,
 LegalStatusCode STRING,
 LegalStatusName STRING,
 Bed_Days_Month_WS INT,
 Bed_Days_Qtr_WS INT,
 Bed_Days_Yr_WS INT,
 Bed_Days_Month_HS INT,
 Bed_Days_Qtr_HS INT,
 Bed_Days_Yr_HS INT,
 RANK INT,
 InScope STRING,
 Received_In_RP INT,
 Submitted_In_RP INT,
 Ended_In_RP INT,
 Active_End INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_quarter;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_quarter
 (
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 UniqMonthID BIGINT,
 Person_ID STRING,
 OrgIDProv STRING,
 SubICBGPRes STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Band STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,
 AutismStatus STRING,
 AutismStatus_desc STRING,
 LDStatus STRING,
 LDStatus_desc STRING,
 ReasonOAT STRING,
 ReasonOATName STRING,
 UniqServReqID STRING,
 NewServDischDate DATE,
 ReferralRequestReceivedDate DATE,
 PrimReasonReferralMH STRING,
 PrimReasonReferralMHName STRING,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName STRING,
 Acute_Bed STRING,
 StartMHAdmittedPatientClass STRING,
 StartMHAdmittedPatientClassName STRING,
 StartAcute_Bed STRING,
 ActiveMHAdmittedPatientClass STRING,
 ActiveMHAdmittedPatientClassName STRING,
 ActiveAcute_Bed STRING,
 EndMHAdmittedPatientClass STRING,
 EndMHAdmittedPatientClassName STRING,
 EndAcute_Bed STRING,
 WardType STRING,
 BedDaysWSEndRP INT,
 OrgIDReceiving STRING,
 ReceivingProvider_Name STRING,
 OrgIDSubmitting STRING,
 SendingProvider_Name STRING,
 LegalStatusCode STRING,
 LegalStatusName STRING,
 Bed_Days_Month_WS INT,
 Bed_Days_Qtr_WS INT,
 Bed_Days_Yr_WS INT,
 Bed_Days_Month_HS INT,
 Bed_Days_Qtr_HS INT,
 Bed_Days_Yr_HS INT,
 RANK INT,
 InScope STRING,
 Received_In_RP INT,
 Submitted_In_RP INT,
 Ended_In_RP INT,
 Active_End INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_year;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_year
 (
 ReportingPeriodStartDate DATE,
 ReportingPeriodEndDate DATE,
 UniqMonthID BIGINT,
 Person_ID STRING,
 OrgIDProv STRING,
 SubICBGPRes STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Band STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,
 AutismStatus STRING,
 AutismStatus_desc STRING,
 LDStatus STRING,
 LDStatus_desc STRING,
 ReasonOAT STRING,
 ReasonOATName STRING,
 UniqServReqID STRING,
 NewServDischDate DATE,
 ReferralRequestReceivedDate DATE,
 PrimReasonReferralMH STRING,
 PrimReasonReferralMHName STRING,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName STRING,
 Acute_Bed STRING,
 StartMHAdmittedPatientClass STRING,
 StartMHAdmittedPatientClassName STRING,
 StartAcute_Bed STRING,
 ActiveMHAdmittedPatientClass STRING,
 ActiveMHAdmittedPatientClassName STRING,
 ActiveAcute_Bed STRING,
 EndMHAdmittedPatientClass STRING,
 EndMHAdmittedPatientClassName STRING,
 EndAcute_Bed STRING,
 WardType STRING,
 BedDaysWSEndRP INT,
 OrgIDReceiving STRING,
 ReceivingProvider_Name STRING,
 OrgIDSubmitting STRING,
 SendingProvider_Name STRING,
 LegalStatusCode STRING,
 LegalStatusName STRING,
 Bed_Days_Month_WS INT,
 Bed_Days_Qtr_WS INT,
 Bed_Days_Yr_WS INT,
 Bed_Days_Month_HS INT,
 Bed_Days_Qtr_HS INT,
 Bed_Days_Yr_HS INT,
 RANK INT,
 InScope STRING,
 Received_In_RP INT,
 Submitted_In_RP INT,
 Ended_In_RP INT,
 Active_End INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_all_admissions_month;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_all_admissions_month
 (
 UniqMonthID BIGINT,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Band STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,   
 AutismStatus STRING,
 AutismStatus_desc STRING,
 LDStatus STRING,
 LDStatus_desc STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName  STRING,
 Acute_Bed STRING,
 StartDateWardStay DATE,
 EndDateWardStay DATE,
 Bed_Days_Month_WS INT,
 Bed_Days_Qtr_WS INT,
 Bed_Days_Yr_WS INT,
 StartDateHospProvSpell DATE,
 DischDateHospProvSpell DATE,
 Bed_Days_Month_HS INT,
 Bed_Days_Qtr_HS INT,
 Bed_Days_Yr_HS INT,
 RANK INT,
 Received_In_RP INT,
 Submitted_In_RP INT,
 Ended_In_RP INT,
 Active_End INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_all_admissions_quarter;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_all_admissions_quarter
 (
 UniqMonthID BIGINT,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Band STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,    
 AutismStatus STRING,
 AutismStatus_desc STRING,
 LDStatus STRING,
 LDStatus_desc STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName  STRING,
 Acute_Bed STRING,
 StartDateWardStay DATE,
 EndDateWardStay DATE,
 Bed_Days_Month_WS INT,
 Bed_Days_Qtr_WS INT,
 Bed_Days_Yr_WS INT,
 StartDateHospProvSpell DATE,
 DischDateHospProvSpell DATE,
 Bed_Days_Month_HS INT,
 Bed_Days_Qtr_HS INT,
 Bed_Days_Yr_HS INT,
 RANK INT,
 Received_In_RP INT,
 Submitted_In_RP INT,
 Ended_In_RP INT,
 Active_End INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_all_admissions_year;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_all_admissions_year
 (
 UniqMonthID BIGINT,
 UniqHospProvSpellID STRING,
 UniqWardStayID STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 AgeRepPeriodEnd INT,
 Age_Band STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 IMD_Decile STRING,    
 AutismStatus STRING,
 AutismStatus_desc STRING,
 LDStatus STRING,
 LDStatus_desc STRING,
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName  STRING,
 Acute_Bed STRING,
 StartDateWardStay DATE,
 EndDateWardStay DATE,
 Bed_Days_Month_WS INT,
 Bed_Days_Qtr_WS INT,
 Bed_Days_Yr_WS INT,
 StartDateHospProvSpell DATE,
 DischDateHospProvSpell DATE,
 Bed_Days_Month_HS INT,
 Bed_Days_Qtr_HS INT,
 Bed_Days_Yr_HS INT,
 RANK INT,
 Received_In_RP INT,
 Submitted_In_RP INT,
 Ended_In_RP INT,
 Active_End INT
 ) USING DELTA