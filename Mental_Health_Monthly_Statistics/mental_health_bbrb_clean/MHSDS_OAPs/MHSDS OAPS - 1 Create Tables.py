# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.oaps_output;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_output(
   reporting_period_start string,
   reporting_period_end string,
   status string,
   breakdown string,
   level_one string,
   level_one_description string,
   level_two string,
   level_two_description string,
   level_three string,
   level_three_description string,
   metric string,
   metric_description string,
   metric_value string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_output_sup;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_output_sup(
   REPORTING_PERIOD_START string,
   REPORTING_PERIOD_END string,
   STATUS string,
   BREAKDOWN string,
   PRIMARY_LEVEL string,
   PRIMARY_LEVEL_DESCRIPTION string,
   SECONDARY_LEVEL string,
   SECONDARY_LEVEL_DESCRIPTION string,
   METRIC string,
   METRIC_DESCRIPTION string,
   METRIC_VALUE string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.oaps_output_raw;
 CREATE TABLE IF NOT EXISTS $db_output.oaps_output_raw(
   REPORTING_PERIOD_START string,
   REPORTING_PERIOD_END string,
   STATUS string,
   BREAKDOWN string,
   PRIMARY_LEVEL string,
   PRIMARY_LEVEL_DESCRIPTION string,
   SECONDARY_LEVEL string,
   SECONDARY_LEVEL_DESCRIPTION string,
   METRIC string,
   METRIC_DESCRIPTION string,
   METRIC_VALUE string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.metric_info;
 CREATE TABLE IF NOT EXISTS $db_output.metric_info
   (metric string,
    metric_description string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPS_In_Scope;
 CREATE TABLE IF NOT EXISTS $db_output.OAPS_In_Scope
   (ORG_CODE string)
 USING DELTA

# COMMAND ----------

 %sql
  
 DROP TABLE IF EXISTS $db_output.OAPs_ORG_DAILY;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_ORG_DAILY
   (ORG_KEY int,
    ORG_CODE string,
    ORG_TYPE_CODE string,
    NAME string,
    SHORT_NAME string,
    ORG_OPEN_DATE date,
    ORG_CLOSE_DATE date,
    COMMENTS string,
    SUCC_EXISTS int,
    COUNTRY_CODE string,
    BUSINESS_START_DATE date,
    BUSINESS_END_DATE date,
    ORG_IS_CURRENT int,
    SYSTEM_CREATED_DATE date,
    SYSTEM_UPDATED_DATE date)
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_ORG_RELATIONSHIP_DAILY;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_ORG_RELATIONSHIP_DAILY
   (REL_TYPE_CODE string,
    REL_FROM_ORG_CODE string,
    REL_TO_ORG_CODE string, 
    REL_OPEN_DATE date,
    REL_CLOSE_DATE date
   )
 USING DELTA

# COMMAND ----------

# DBTITLE 1,CCG to STP and Region Mapping
 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_STP_Region_mapping;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_STP_Region_mapping
   (STP_code string,
    STP_name string,
    CCG_CODE string,
    CCG_NAME string,
    Region_code string,
    Region_name string)
 USING DELTA

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.OAPs_CCG_List;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_CCG_List
   (type string,
    level string,
    level_description string)
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Months;
 CREATE TABLE IF NOT EXISTS $db_output.Months
   (UniqMonthID bigint,
    ReportingPeriodStartDate date,
    ReportingPeriodEndDate date)
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.HospProvSpell;
 CREATE TABLE IF NOT EXISTS $db_output.HospProvSpell
   (UniqServReqID string,
    Person_ID string,
    Uniqmonthid bigint,
    OrgIDProv string,
    UniqHospProvSpellID string,
    ReportingPeriodStartDate date,
    ReportingPeriodEndDate date,
    StartDateHospProvSpell date,
    DischDateHospProvSpell date
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.WardStay;
 CREATE TABLE IF NOT EXISTS $db_output.WardStay
   (UniqWardStayID string,
    HospitalBedTypeMH string,
    WardType string,
    BedDaysWSEndRP int,
    Person_ID string,
    Uniqmonthid bigint,
    UniqHospProvSpellID string,
    OrgIDProv string,
    ReportingPeriodStartDate date,
    ReportingPeriodEndDate date,
    StartDateWardStay date,
    EndDateWardStay date
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_referrals
   (UniqServReqId string,
    ReferralRequestReceivedDate date,
    Person_ID string,
    ServDischDate date,
    uniqmonthid bigint,
    ReasonOAT string,
    PrimReasonReferralMH string,
    OrgIDProv string,
    OrgIDReferring string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_onwardreferrals;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_onwardreferrals
   (UniqServReqID string,
    Person_ID string,
    Uniqmonthid bigint,
    OrgIDProv string,
    OATReason string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_MHA;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_MHA
   (uniqmonthid bigint,
    legalstatuscode string,
    Person_ID string,
    OrgIDProv string,
    StartDateMHActLegalStatusClass string,
    UniqSubmissionID string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Overlapping_OAPs_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.Overlapping_OAPs_referrals
   (UniqServReqId string,
    ReferralRequestReceivedDate date,
    Person_ID string,
    ServDischDate date,
    uniqmonthid bigint,
    ReasonOAT string,
    PrimReasonReferralMH string,
    OrgIDProv string,
    OrgIDReferring string,
    NewServDischDate date
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs
   (UniqMonthID bigint,
    Person_ID string,
    OrgIDProv string,
    SubICBGPRes string,
    Der_Gender string,
    Der_GenderName string,
    AgeRepPeriodEnd int,
    Der_AgeGroup string,
    NHSDEthnicity string,
    NHSDEthnicityName string,
    IMD_Decile string,
    ReasonOAT string,
    ReasonOATName string,
    UniqServReqID string,
    NewServDischDate date,
    ReferralRequestReceivedDate date,
    PrimReasonReferralMH string,
    PrimReasonReferralMHName string,
    UniqHospProvSpellID string,
    UniqWardStayID string,
    HospitalBedTypeMH string,
    HospitalBedTypeMHName string,
    WardType string,
    BedDaysWSEndRP int,
    OrgIDReceiving string,
    OrgIDSubmitting string,
    LegalStatusCode string,
    LegalStatusName string,
    Bed_Days_Month_WS int,
    Bed_Days_Quarter_WS int,
    Bed_Days_Year_WS int,
    Bed_Days_Month_HS int,
    Bed_Days_Quarter_HS int,
    Bed_Days_Year_HS int,
    SumBedDaysHS int,
    InScope string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_Month;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_Month
   (UniqMonthID bigint,
    Person_ID string,
    OrgIDProv string,
    SubICBGPRes string,
    Der_Gender string,
    Der_GenderName string,
    AgeRepPeriodEnd int,
    Der_AgeGroup string,
    NHSDEthnicity string,
    NHSDEthnicityName string,
    IMD_Decile string,
    ReasonOAT string,
    ReasonOATName string,
    UniqServReqID string,
    NewServDischDate date,
    ReferralRequestReceivedDate date,
    PrimReasonReferralMH string,
    PrimReasonReferralMHName string,
    UniqHospProvSpellID string,
    UniqWardStayID string,
    HospitalBedTypeMH string,
    HospitalBedTypeMHName string,
    WardType string,
    BedDaysWSEndRP int,
    OrgIDReceiving string,
    OrgIDSubmitting string,
    LegalStatusCode string,
    LegalStatusName string,
    Bed_Days_Month_WS int,
    Bed_Days_Quarter_WS int,
    Bed_Days_Year_WS int,
    Bed_Days_Month_HS int,
    Bed_Days_Quarter_HS int,
    Bed_Days_Year_HS int,
    SumBedDaysHS int,
    InScope string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_Quarter;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_Quarter
   (UniqMonthID bigint,
    Person_ID string,
    OrgIDProv string,
    SubICBGPRes string,
    Der_Gender string,
    Der_GenderName string,
    AgeRepPeriodEnd int,
    Der_AgeGroup string,
    NHSDEthnicity string,
    NHSDEthnicityName string,
    IMD_Decile string,
    ReasonOAT string,
    ReasonOATName string,
    UniqServReqID string,
    NewServDischDate date,
    ReferralRequestReceivedDate date,
    PrimReasonReferralMH string,
    PrimReasonReferralMHName string,
    UniqHospProvSpellID string,
    UniqWardStayID string,
    HospitalBedTypeMH string,
    HospitalBedTypeMHName string,
    WardType string,
    BedDaysWSEndRP int,
    OrgIDReceiving string,
    OrgIDSubmitting string,
    LegalStatusCode string,
    LegalStatusName string,
    Bed_Days_Month_WS int,
    Bed_Days_Quarter_WS int,
    Bed_Days_Year_WS int,
    Bed_Days_Month_HS int,
    Bed_Days_Quarter_HS int,
    Bed_Days_Year_HS int,
    SumBedDaysHS int,
    InScope string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPs_Year;
 CREATE TABLE IF NOT EXISTS $db_output.OAPs_Year
   (UniqMonthID bigint,
    Person_ID string,
    OrgIDProv string,
    SubICBGPRes string,
    Der_Gender string,
    Der_GenderName string,
    AgeRepPeriodEnd int,
    Der_AgeGroup string,
    NHSDEthnicity string,
    NHSDEthnicityName string,
    IMD_Decile string,
    ReasonOAT string,
    ReasonOATName string,
    UniqServReqID string,
    NewServDischDate date,
    ReferralRequestReceivedDate date,
    PrimReasonReferralMH string,
    PrimReasonReferralMHName string,
    UniqHospProvSpellID string,
    UniqWardStayID string,
    HospitalBedTypeMH string,
    HospitalBedTypeMHName string,
    WardType string,
    BedDaysWSEndRP int,
    OrgIDReceiving string,
    OrgIDSubmitting string,
    LegalStatusCode string,
    LegalStatusName string,
    Bed_Days_Month_WS int,
    Bed_Days_Quarter_WS int,
    Bed_Days_Year_WS int,
    Bed_Days_Month_HS int,
    Bed_Days_Quarter_HS int,
    Bed_Days_Year_HS int,
    SumBedDaysHS int,
    InScope string
   )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.OAPS_CCG_LATEST;
 CREATE TABLE IF NOT EXISTS $db_output.OAPS_CCG_LATEST
   (
   Person_ID string,
   SubICBGPRes string,
   NAME string
   )
 USING DELTA