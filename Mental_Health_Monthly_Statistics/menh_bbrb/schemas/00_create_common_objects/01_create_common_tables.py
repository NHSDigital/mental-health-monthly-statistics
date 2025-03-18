# Databricks notebook source
 %run ../../notebooks/mhsds_functions

# COMMAND ----------

 %md
 ###Final output tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_final_raw;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_final_raw 
 (
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 STATUS string,
 BREAKDOWN string,
 PRIMARY_LEVEL string,
 PRIMARY_LEVEL_DESCRIPTION string,
 SECONDARY_LEVEL string,
 SECONDARY_LEVEL_DESCRIPTION String,
 MEASURE_ID string,
 MEASURE_NAME string,
 MEASURE_VALUE float,
 SOURCE_DB string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_final_suppressed;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_final_suppressed 
 (
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 STATUS string,
 BREAKDOWN string,
 PRIMARY_LEVEL string,
 PRIMARY_LEVEL_DESCRIPTION string,
 SECONDARY_LEVEL string,
 SECONDARY_LEVEL_DESCRIPTION String,
 MEASURE_ID string,
 MEASURE_NAME string,
 MEASURE_VALUE string,
 SOURCE_DB string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_final;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_final
 (
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 STATUS string,
 BREAKDOWN string,
 PRIMARY_LEVEL string,
 PRIMARY_LEVEL_DESCRIPTION string,
 SECONDARY_LEVEL string,
 SECONDARY_LEVEL_DESCRIPTION String,
 MEASURE_ID string,
 MEASURE_NAME string,
 MEASURE_VALUE string,
 SOURCE_DB string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.audit_menh_bbrb;
 CREATE TABLE IF NOT EXISTS $db_output.audit_menh_bbrb 
 (
 MONTH_ID int,
 STATUS string,
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 SOURCE_DB string,
 RUN_START timestamp,
 RUN_END timestamp,
 NOTES string
 ) USING DELTA PARTITIONED BY (MONTH_ID, STATUS)

# COMMAND ----------

 %md
 ###CSV Lookup Tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_breakdown_values;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_breakdown_values 
 (
 BREAKDOWN STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_level_values;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_level_values 
 (
 BREAKDOWN STRING,
 PRIMARY_LEVEL STRING,
 PRIMARY_LEVEL_DESCRIPTION STRING,
 SECONDARY_LEVEL STRING,
 SECONDARY_LEVEL_DESCRIPTION STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_csv_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_csv_lookup
 (
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 STATUS string,
 BREAKDOWN string,
 PRIMARY_LEVEL string,
 PRIMARY_LEVEL_DESCRIPTION string,
 SECONDARY_LEVEL string,
 SECONDARY_LEVEL_DESCRIPTION String,
 MEASURE_ID string,
 MEASURE_NAME string,
 SOURCE_DB string
 ) USING DELTA

# COMMAND ----------

 %md
 ###Organisation reference data tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_ccg_latest;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_ccg_latest 
 (
 original_ORG_CODE STRING,
 original_NAME     STRING,
 ORG_CODE          STRING,
 NAME              STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_ccg_in_month;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_ccg_in_month
 (
 Person_ID   STRING,
 IC_Rec_CCG  STRING,
 NAME        STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_ccg_in_quarter;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_ccg_in_quarter
 (
 Person_ID   STRING,
 SubICBGPRes STRING,
 NAME        STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_ccg_in_year;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_ccg_in_year 
 (
 Person_ID   STRING,
 SubICBGPRes STRING,
 NAME        STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_daily_in_year;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_daily_in_year 
 (
 ORG_CODE STRING,
 NAME     STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_daily_latest;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_daily_latest 
 (
 ORG_CODE STRING,
 NAME     STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_daily_latest_mhsds_providers;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_daily_latest_mhsds_providers 
 (
 ORG_CODE STRING,
 NAME     STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_daily_past_12_months_mhsds_providers;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_daily_past_12_months_mhsds_providers
 (
 ORG_CODE STRING,
 NAME     STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_daily_past_quarter_mhsds_providers;                                 
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_daily_past_quarter_mhsds_providers 
 (
 ORG_CODE STRING,
 NAME     STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ccg_mapping_2021;
 CREATE TABLE IF NOT EXISTS $db_output.ccg_mapping_2021
 (
 CCG_UNMAPPED STRING, 
 CCG21CDH STRING, 
 CCG21NM STRING, 
 STP21CDH STRING, 
 STP21NM STRING,
 NHSER21CDH STRING, 
 NHSER21NM STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_daily;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_daily
 (
 ORG_CODE STRING,
 NAME STRING,
 ORG_TYPE_CODE STRING,
 ORG_OPEN_DATE STRING, 
 ORG_CLOSE_DATE STRING, 
 BUSINESS_START_DATE STRING, 
 BUSINESS_END_DATE STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_org_relationship_daily;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_org_relationship_daily 
 (
 REL_TYPE_CODE STRING,
 REL_FROM_ORG_CODE STRING,
 REL_TO_ORG_CODE STRING, 
 REL_OPEN_DATE DATE,
 REL_CLOSE_DATE DATE
 ) USING DELTA 

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.bbrb_stp_mapping;
 CREATE TABLE IF NOT EXISTS $db_output.bbrb_stp_mapping
 (
 STP_CODE STRING, 
 STP_NAME STRING, 
 CCG_CODE STRING, 
 CCG_NAME STRING,
 REGION_CODE STRING,
 REGION_NAME STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.mhs001mpi_latest_month_data; 
 CREATE TABLE IF NOT EXISTS $db_output.mhs001mpi_latest_month_data 
 (
 AgeDeath bigint,
 AgeRepPeriodEnd bigint,
 AgeRepPeriodStart  bigint,
 CCGGPRes string, 
 County string,
 DefaultPostcode string,
 ElectoralWard string,
 EthnicCategory string,
 Gender string,
 IMDQuart string,
 LADistrictAuth string,
 LDAFlag boolean,
 LSOA string,
 LSOA2011 string,
 LanguageCodePreferred string,
 LocalPatientId string,
 MHS001UniqID bigint,
 MPSConfidence string,
 MaritalStatus string,
 NHSDEthnicity string,
 NHSNumber string,
 NHSNumberStatus bigint,
 OrgIDCCGRes string,
 --SubICB Residence isnt included because it would mean a changeto table structure and not used in the notebook.
 OrgIDEduEstab string,
 OrgIDLocalPatientId string,
 OrgIDProv string,
 PatMRecInRP boolean,
 Person_ID string,
 PostcodeDistrict string,
 RecordEndDate date,
 RecordNumber bigint,
 RecordStartDate date,
 RowNumber bigint,
 UniqMonthID bigint,
 UniqSubmissionID bigint,
 IC_Rec_CCG string,
 NAME string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.mhs001mpi_12_months_data; 
 CREATE TABLE IF NOT EXISTS $db_output.mhs001mpi_12_months_data 
 (
 UniqMonthID bigint,
 Person_ID string,
 OrgIDProv string,
 OrgIDProvName string,
 RecordNumber bigint,
 PatMRecInRP boolean,
 NHSDEthnicity string,
 LowerEthnicity string,
 LowerEthnicity_Desc string,
 UpperEthnicity string,
 Gender string,
 GenderIDCode string,
 Der_Gender string,
 AgeRepPeriodEnd bigint,
 Age_Band string,
 IMD_Decile string
 ) USING DELTA

# COMMAND ----------

 %md
 ###Population data tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.eng_pop;
 CREATE TABLE IF NOT EXISTS $db_output.eng_pop
 (
 Age_Group STRING,
 POPULATION_COUNT INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.age_band_pop;
 CREATE TABLE IF NOT EXISTS $db_output.age_band_pop
 (
 Age_Group STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Age_Group_IPS STRING,
 Age_Group_OAPs STRING,
 Age_Group_MHA STRING,
 Age_Group_CYP STRING,
 Age_Group_Higher_Level STRING,
 POPULATION_COUNT INT
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.gender_pop;
 CREATE TABLE IF NOT EXISTS $db_output.gender_pop
 (
 Age_Group STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 POPULATION_COUNT INT
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.eth_pop;
 CREATE TABLE IF NOT EXISTS $db_output.eth_pop
 (
 Age_Group STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 POPULATION_COUNT INT
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.imd_pop;
 CREATE TABLE IF NOT EXISTS $db_output.imd_pop
 (
 Age_Group STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 IMD_Decile STRING,
 IMD_Quintile STRING,
 POPULATION_COUNT INT
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.commissioner_pop;
 CREATE TABLE IF NOT EXISTS $db_output.commissioner_pop
 (
 Age_Group STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 STP_Code STRING,
 STP_Name STRING,
 Region_Code STRING,
 Region_Name STRING,
 POPULATION_COUNT INT
 ) USING DELTA

# COMMAND ----------

 %md
 ###MHSDS reference data tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.mh_ass;
 CREATE TABLE IF NOT EXISTS $db_output.mh_ass 
 (
 Category STRING,
 Assessment_Tool_Name STRING,
 Preferred_Term_SNOMED STRING,
 Active_Concept_ID_SNOMED BIGINT,
 SNOMED_Version STRING,
 Lower_Range INT,
 Upper_Range INT,
 CYPMH STRING,
 EIP STRING,
 Rater STRING
 ) USING DELTA

# COMMAND ----------

 %md
 ###NHSE Pre Processing Tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_header;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_header
 (
 uniqmonthid INT,
 reportingperiodstartdate DATE,
 reportingperiodenddate DATE,
 Der_FY STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_referral;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_referral 
 (
 Der_FY STRING,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 MHS101UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqServReqID string,
 OrgIDComm string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 SpecialisedMHServiceCode string,
 PrimReasonReferralMH string,
 ReasonOAT string,
 DischPlanCreationDate date,
 DischPlanCreationTime timestamp,
 DischPlanLastUpdatedDate date,
 DischPlanLastUpdatedTime timestamp,
 ServDischDate date,
 ServDischTime timestamp,
 AgeServReferRecDate BIGINT,
 AgeServReferDischDate BIGINT,
 RecordStartDate date,
 RecordEndDate date,
 InactTimeRef date,
 MHS001UniqID BIGINT,
 OrgIDCCGRes string,
 IC_Rec_CCG string,
 OrgIDEduEstab string,
 EthnicCategory string,
 EthnicCategory2021 string,
 NHSDEthnicity string,
 Gender string,
 Gender2021 string,
 MaritalStatus string,
 PersDeathDate date,
 AgeDeath BIGINT,
 LocalPatientId string,
 OrgIDResidenceResp string,
 LADistrictAuth string,
 PostcodeDistrict string,
 DefaultPostcode string,
 AgeRepPeriodStart BIGINT,
 AgeRepPeriodEnd BIGINT,
 UniqCareProfTeamID string,
 ServTeamTypeRefToMH string,
 ReferRejectionDate date,
 ReferRejectionTime timestamp,
 ReferRejectReason string,
 ReferClosureDate date,
 ReferClosureTime timestamp,
 ReferClosReason string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_distinct_indirect_activity;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_distinct_indirect_activity
 (
 UniqSubmissionID string,
 UniqMonthID bigint,
 OrgIDProv string,
 Person_ID string,
 Der_PersonID string,
 RecordNumber string,
 UniqServReqID string,
 OrgIDComm string,
 CareProfTeamLocalId string,
 IndirectActDate date,
 IndirectActTime timestamp,
 DurationIndirectAct bigint,
 MHS204UniqID string,
 Der_ActRN int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_activity;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_activity 
 (
 Der_FY string,
 Der_ActivityType string,
 Der_ActivityUniqID BIGINT,
 Person_ID string,
 UniqMonthID BIGINT,
 OrgIDProv string,
 RecordNumber BIGINT,
 UniqServReqID string,
 UniqCareContID string,
 Der_ContactDate date,
 Der_ContactTime timestamp,
 ConsMechanismMH string,
 AttendStatus string,
 Der_PersonID string,
 Der_DirectContact string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_activity;
 CREATE TABLE IF NOT EXISTS $db_output.der_nhse_pre_proc_activity 
 (
 Der_FY string,
 Der_ActivityType string,
 Der_ActivityUniqID BIGINT,
 Person_ID string,
 UniqMonthID BIGINT,
 OrgIDProv string,
 RecordNumber BIGINT,
 UniqServReqID string,
 UniqCareContID string,
 Der_ContactDate date,
 Der_ContactTime timestamp,
 ConsMechanismMH string,
 AttendStatus string,
 Der_PersonID string,
 Der_DirectContact string,
 Der_ContactOrder INT,
 Der_FYContactOrder INT
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_inpatients;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_inpatients 
 (
 Der_FY STRING,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 MHS501UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqHospProvSpellID string,
 UniqServReqID string,
 UniqPersRefID string,
 UniqPersRefID_FY string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 SourceAdmMHHospProvSpell string,
 MethAdmMHHospProvSpell string,
 EstimatedDischDateHospProvSpell date,
 PlannedDischDateHospProvSpell date,
 DischDateHospProvSpell date,
 DischTimeHospProvSpell timestamp,
 MethOfDischMHHospProvSpell string,
 DestOfDischHospProvSpell string,
 InactTimeHPS date,
 PlannedDestDisch string,
 PostcodeDistrictMainVisitor string,
 PostcodeDistrictDischDest string,
 MHS502UniqID BIGINT,
 UniqWardStayID string,
 StartDateWardStay date,
 StartTimeWardStay timestamp,
 SiteIDOfWard string,
 WardType string,
 WardIntendedSex string,
 WardIntendedClinCareMH string,
 WardSecLevel string,
 SpecialisedMHServiceCode string,
 WardCode string,
 WardLocDistanceHome bigint,
 LockedWardInd string,
 InactTimeWS date,
 WardAge string,
 MHAdmittedPatientClass string,
 EndDateMHTrialLeave date,
 EndDateWardStay date,
 EndTimeWardStay timestamp,
 Der_HospSpellStatus string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_inpatients;
 CREATE TABLE IF NOT EXISTS $db_output.der_nhse_pre_proc_inpatients 
 (
 Der_FY STRING,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 MHS501UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqHospProvSpellID string,
 UniqServReqID string,
 UniqPersRefID string,
 UniqPersRefID_FY string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 SourceAdmMHHospProvSpell string,
 MethAdmMHHospProvSpell string,
 EstimatedDischDateHospProvSpell date,
 PlannedDischDateHospProvSpell date,
 DischDateHospProvSpell date,
 DischTimeHospProvSpell timestamp,
 MethOfDischMHHospProvSpell string,
 DestOfDischHospProvSpell string,
 InactTimeHPS date,
 PlannedDestDisch string,
 PostcodeDistrictMainVisitor string,
 PostcodeDistrictDischDest string,
 MHS502UniqID BIGINT,
 UniqWardStayID string,
 StartDateWardStay date,
 StartTimeWardStay timestamp,
 SiteIDOfWard string,
 WardType string,
 WardIntendedSex string,
 WardIntendedClinCareMH string,
 WardSecLevel string,
 SpecialisedMHServiceCode string,
 WardCode string,
 WardLocDistanceHome bigint,
 LockedWardInd string,
 InactTimeWS date,
 WardAge string,
 MHAdmittedPatientClass string,
 EndDateMHTrialLeave date,
 EndDateWardStay date,
 EndTimeWardStay timestamp,
 Der_HospSpellStatus string,
 Der_HospSpellRecordOrder int, 
 Der_LastWardStayRecord int,
 Der_FirstWardStayRecord int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.prep_nhse_pre_proc_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.prep_nhse_pre_proc_assessments
 (
 Der_AssTable string,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID bigint,
 UniqMonthID bigint,
 CodedAssToolType string,
 PersScore string,
 Der_AssToolCompDate date,
 RecordNumber string,
 Der_AssUniqID bigint,
 OrgIDProv string,
 Person_ID string,
 UniqServReqID string,
 Der_AgeAssessTool string,
 UniqCareContID string,
 UniqCareActID string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_assessments
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 Der_AssUniqID bigint,
 Der_AssTable string, 
 Person_ID string,    
 UniqMonthID bigint,    
 OrgIDProv string,
 RecordNumber string,   
 UniqServReqID string,    
 UniqCareContID string,
 UniqCareActID string,       
 Der_AssToolCompDate date,
 CodedAssToolType string,
 PersScore string,
 Der_AgeAssessTool string,
 Der_AssessmentCategory string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange string,
 Der_UpperRange string,
 Rater string,
 Der_ValidScore string,
 Der_UniqAssessment string,
 Der_AssKey string,
 Der_AssInMonth int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_assessments_unique;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_assessments_unique
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 RecordNumber string,
 UniqServReqID string,
 UniqCareContID string,
 UniqCareActID string,
 CodedAssToolType string,
 PersScore string,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate string,
 Der_AgeAssessTool string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange int,
 Der_UpperRange int,
 Der_ValidScore string,
 Der_AssessmentCategory string,
 Der_AssKey string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_assessments_unique_valid;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_assessments_unique_valid
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 RecordNumber string,
 UniqServReqID string,
 UniqCareContID string,
 UniqCareActID string,
 CodedAssToolType string,
 PersScore string,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate string,
 Der_AgeAssessTool string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange int,
 Der_UpperRange int,
 Der_ValidScore string,
 Der_AssessmentCategory string,
 Der_AssOrderAsc_OLD int,
 Der_AssOrderDesc_OLD int,
 Der_AssOrderAsc_NEW int,
 Der_AssOrderDesc_NEW int,
 Der_AssKey string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.nhse_pre_proc_interventions;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_interventions
 (
 RecordNumber string,
 OrgIDProv string,
 Person_ID string,
 UniqMonthID string,
 UniqServReqID string,
 UniqCareContID string,
 Der_ContactDate date,
 UniqCareActID string,
 Der_InterventionUniqID string,
 Procedure string,
 Der_SNoMEDProcCode string,
 Der_SNoMEDProcQual string,   
 Observation string
 ) USING DELTA

# COMMAND ----------

 %md
 ## Field Reference Tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.eng_desc;
 CREATE TABLE IF NOT EXISTS $db_output.eng_desc
 (
 England STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.eng_desc;
 INSERT INTO $db_output.eng_desc VALUES
 ("England", 1390, null)

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.gender_desc;
 CREATE TABLE IF NOT EXISTS $db_output.gender_desc
 (
 Der_Gender STRING,
 Der_Gender_Desc STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.gender_desc;
 INSERT INTO $db_output.gender_desc VALUES
 ("1", "Male", 1390, null),
 ("2", "Female", 1390, null),
 ("3", "Non-binary", 1390, null),
 ("4", "Other (not listed)", 1390, null),
 ("9", "Indeterminate", 1390, null),
 ("UNKNOWN", "UNKNOWN", 1390, null)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.age_band_desc;
 CREATE TABLE IF NOT EXISTS $db_output.age_band_desc
 (
 AgeRepPeriodEnd STRING,
 Age_Group_Higher_Level STRING,
 Age_Group_OAPs STRING,
 Age_Group_IPS STRING,
 Age_Group_MHA STRING,
 Age_Group_CYP STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
from pyspark.sql import functions as F
 
AgeCat = {
  "Age_Group_Higher_Level": ["Under 18", "18 and over"],          
  "Age_Group_OAPs": ["0 to 17", "18 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 or over"],
  "Age_Group_IPS": ["0 to 5", "6 to 10", "11 to 15", "16", "17", "18", "19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69" , "70 to 74",
                    "75 to 79", "80 to 84", "85 to 89", "90 and over"],
  ##MHA_Measures
  "Age_Group_MHA": ["Under 18", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69" , "70 to 74",
                    "75 to 79", "80 to 84", "85 to 89", "90 and over"],
   #CYP_Outcome_Measures
  "Age_Group_CYP": ["0 to 5", "6 to 10", "11 to 14", "15", "16", "17"]
}
 
AgeCat1 = {key: {x1: x for x in AgeCat[key] for x1 in mapage(x)} for key in AgeCat}
AgeData = [[i] + [AgeCat1[key][i] if i in AgeCat1[key] else "NA" for key in AgeCat1] for i in range(125)]
 
lh = ["Age int"] + [f"{x} string" for x in AgeCat]
schema1 = ', '.join(lh)
df1 = spark.createDataFrame(AgeData, schema = schema1)
unknown_row = spark.createDataFrame([("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN")], df1.columns)
df1 = df1.union(unknown_row)
df1 = df1.select(
  "*",
  F.lit(1390).alias("FirstMonth"),
  F.lit(None).alias("LastMonth")
)
df1.write.insertInto(f"{db_output}.age_band_desc", overwrite=True)

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ethnicity_desc;
 CREATE TABLE IF NOT EXISTS $db_output.ethnicity_desc
 (
 Census21EthnicityCode INT,
 LowerEthnicityCode STRING,
 LowerEthnicityName STRING,
 UpperEthnicity STRING,
 WNWEthnicity STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.ethnicity_desc;
 INSERT INTO $db_output.ethnicity_desc VALUES
 (13, "A", "British", "White or White British", "White British", 1390, null),
 (14, "B", "Irish", "White or White British", "Non-white British", 1390, null),
 (17, "C", "Any Other White background", "White or White British", "Non-white British", 1390, null),
 (11, "D", "White and Black Caribbean", "Mixed", "Non-white British", 1390, null),
 (10, "E", "White and Black African", "Mixed", "Non-white British", 1390, null),
 (9, "F", "White and Asian", "Mixed", "Non-white British", 1390, null),
 (12, "G", "Any Other mixed background", "Mixed", "Non-white British", 1390, null),
 (3, "H", "Indian", "Asian or Asian British", "Non-white British", 1390, null),
 (4, "J", "Pakistani", "Asian or Asian British", "Non-white British", 1390, null),
 (1, "K", "Bangladeshi", "Asian or Asian British", "Non-white British", 1390, null),
 (5, "L", "Any Other Asian background", "Asian or Asian British", "Non-white British", 1390, null),
 (7, "M", "Caribbean", "Black or Black British", "Non-white British", 1390, null),
 (6, "N", "African", "Black or Black British", "Non-white British", 1390, null),
 (8, "P", "Any Other Black background", "Black or Black British", "Non-white British", 1390, null),
 (2, "R", "Chinese", "Other Ethnic Groups", "Non-white British", 1390, null),
 (19, "S", "Any Other Ethnic Group", "Other Ethnic Groups", "Non-white British", 1390, null),
 (15, "T", "Gypsy or Irish Traveller", "White or White British", "Non-white British", 1390, 1390),
 (16, "U", "Roma", "White or White British", "Non-white British", 1390, 1390),
 (18, "V", "Arab", "Asian or Asian British", "Non-white British", 1390, 1390),
 (-8, "Z", "Not Stated", "Not Stated", "Missing/invalid", 1390, null),
 (-8, "99", "Not Known", "Not Known", "Missing/invalid", 1390, null),
 (-8, "UNKNOWN", "UNKNOWN", "UNKNOWN", "Missing/invalid", 1390, null)

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.imd_desc;
 CREATE TABLE IF NOT EXISTS $db_output.imd_desc
 (
 IMD_Number STRING,
 IMD_Decile STRING,
 IMD_Quintile STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.imd_desc;
 INSERT INTO $db_output.imd_desc VALUES
 ("1", "01 Most deprived", "01 Most deprived", 1390, null),
 ("2", "02 More deprived", "01 Most deprived", 1390, null),
 ("3", "03 More deprived", "02", 1390, null),
 ("4", "04 More deprived", "02", 1390, null),
 ("5", "05 More deprived", "03", 1390, null),
 ("6", "06 Less deprived", "03", 1390, null),
 ("7", "07 Less deprived", "04", 1390, null),
 ("8", "08 Less deprived", "04", 1390, null),
 ("9", "09 Less deprived", "05 Least deprived", 1390, null),
 ("10", "10 Least deprived", "05 Least deprived", 1390, null),
 ("UNKNOWN", "UNKNOWN", "UNKNOWN", 1390, null)

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.hosp_bed_desc;
 CREATE TABLE IF NOT EXISTS $db_output.hosp_bed_desc
 (
 MHAdmittedPatientClass STRING,
 MHAdmittedPatientClassName STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.hosp_bed_desc;
 INSERT INTO $db_output.hosp_bed_desc VALUES
 ("10","Acute adult mental health care", 1459, 1488),
 ("11","Acute older adult mental health care (organic and functional)", 1459, 1488),
 ("35","Adult admitted patient continuing care", 1459, 1488),
 ("36","Adult community rehabilitation unit", 1459, 1488),
 ("13","Adult Eating Disorders", 1459, 1488),
 ("17","Adult High dependency rehabilitation", 1459, 1488),
 ("21","Adult High secure", 1459, 1488),
 ("37","Adult highly specialist high dependency rehabilitation unit", 1459, 1488),
 ("15","Adult Learning Disabilities", 1459, 1488),
 ("38","Adult longer term high dependency rehabilitation unit", 1459, 1488),
 ("19","Adult Low secure", 1459, 1488),
 ("20","Adult Medium secure", 1459, 1488),
 ("39","Adult mental health admitted patient services for the Deaf", 1459, 1488),
 ("22","Adult Neuro-psychiatry / Acquired Brain Injury", 1459, 1488),
 ("40","Adult personality disorder", 1459, 1488),
 ("12","Adult Psychiatric Intensive Care Unit (acute mental health care)", 1459, 1488),
 ("30","Child and Young Person Learning Disabilities / Autism admitted patient", 1459, 1488),
 ("31","Child and Young Person Low Secure Learning Disabilities", 1459, 1488),
 ("27","Child and Young Person Low Secure Mental Illness", 1459, 1488),
 ("32","Child and Young Person Medium Secure Learning Disabilities", 1459, 1488),
 ("28","Child and Young Person Medium Secure Mental Illness", 1459, 1488),
 ("34","Child and Young Person Psychiatric Intensive Care Unit", 1459, 1488),
 ("29","Child Mental Health admitted patient services for the Deaf", 1459, 1488),
 ("26","Eating Disorders admitted patient - Child (12 years and under)", 1459, 1488),
 ("25","Eating Disorders admitted patient - Young person (13 years and over)", 1459, 1488),
 ("23","General child and young PERSON admitted PATIENT - Child (including High Dependency)", 1459, 1488),
 ("24","General child and young PERSON admitted PATIENT - Young PERSON (including High Dependency)", 1459, 1488),
 ("14","Mother and baby", 1459, 1488),
 ("33","Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Young person", 1459, 1488),
 ("200","Acute Adult Mental Health Care", 1489, null),
 ("201","Acute Older Adult Mental Health Care (Organic and Functional)", 1489, null),
 ("309","Child and Young Person Psychiatric Intensive Care Unit", 1489, null),
 ("310","Child and Young Person Learning Disabilities", 1489, null),
 ("203","Adult Eating Disorders", 1489, null),
 ("206","Adult Low Secure", 1489, null),
 ("209","Adult Neuro-Psychiatry / Acquired Brain Injury", 1489, null),
 ("311","Child and Young Person Autism", 1489, null),
 ("205","Acute Mental Health Unit for Adults with a Learning Disability and/or Autism", 1489, null),
 ("207","Adult Medium Secure", 1489, null),
 ("208","Adult High Secure", 1489, null),    
 ("210","Adult Personality Disorder", 1489, null),    
 ("202","Adult Psychiatric Intensive Care Unit (Acute Mental Health Care)", 1489, null),
 ("304","Child and Young Person Medium Secure Mental Illness", 1489, null),
 ("305","Child Mental Health Services for the Deaf", 1489, null),
 ("301","General Child and Young Person - Young Person (13 years up to and including 17 years)", 1489, null), 
 ("306","Child and Young Person Low Secure Learning Disabilities", 1489, null),
 ("302","Eating Disorders - Child and Young Person", 1489, null),
 ("308","Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Young Person", 1489, null),
 ("303","Child and Young Person Low Secure Mental Illness", 1489, null),
 ("300","General Child and Young Person - Child (up to and including 12 years)", 1489, null),
 ("213","Adult Mental Health Rehabilitation for Adults with a Learning Disability and/or Autism (Specialist Service)", 1489, null),
 ("211","Adult Mental Health Services for the Deaf", 1489, null),
 ("212","Adult Mental Health Rehabilitation (Mainstream Service)", 1489, null),
 ("204","Mother and Baby", 1489, null),
 ("307","Child and Young Person Medium Secure Learning Disabilities", 1489, null),
 ("No Hospital Spell", "No Hospital Spell", 1459, null),
 ("UNKNOWN", "UNKNOWN", 1459, null)

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ref_reason_desc;
 CREATE TABLE IF NOT EXISTS $db_output.ref_reason_desc
 (
 PrimReasonReferralMH STRING,
 PrimReasonReferralMHName STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.ref_reason_desc;
 INSERT INTO $db_output.ref_reason_desc VALUES
 ("01", "(Suspected) First Episode Psychosis", 1459, null),
 ("02", "Ongoing or Recurrent Psychosis", 1459, null),
 ("03", "Bi polar disorder", 1459, null),
 ("04", "Depression", 1459, null),
 ("05", "Anxiety", 1459, null),
 ("06", "Obsessive compulsive disorder", 1459, null),
 ("07", "Phobias", 1459, null),
 ("08", "Organic brain disorder", 1459, null),
 ("09", "Drug and alcohol difficulties", 1459, null),
 ("10", "Unexplained physical symptoms", 1459, null),
 ("11", "Post-traumatic stress disorder", 1459, null),
 ("12", "Eating disorders", 1459, null),
 ("13", "Perinatal mental health issues", 1459, null),
 ("14", "Personality disorders", 1459, null),
 ("15", "Self harm behaviours", 1459, null),
 ("16", "Conduct disorders", 1459, null),
 ("18", "In crisis", 1459, null),
 ("19", "Relationship difficulties", 1459, null),
 ("20", "Gender Discomfort issues", 1459, null),
 ("21", "Attachment difficulties", 1459, null),
 ("22", "Self - care issues", 1459, null),
 ("23", "Adjustment to health issues", 1459, null),
 ("24", "Neurodevelopmental Conditions, excluding Autism", 1459, null),
 ("25", "Suspected Autism", 1459, null),
 ("26", "Diagnosed Autism", 1459, null),
 ("27", "Preconception perinatal mental health concern", 1459, null),
 ("28", "Gambling disorder", 1459, null),
 ("29", "Community Perinatal Mental Health Partner Assessment", 1459, null),
 ("30", "Behaviours that challenge due to a Learning Disability", 1459, null),
 ("31", "Employment Support", 1489, null),
 ("UNKNOWN", "UNKNOWN", 1459, null)

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.serv_team_type_ref_to_desc;
 CREATE TABLE IF NOT EXISTS $db_output.serv_team_type_ref_to_desc
 (
 ServTeamTypeRefToMH STRING,
 ServTeamTypeRefToMHDesc STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.serv_team_type_ref_to_desc;
 INSERT INTO $db_output.serv_team_type_ref_to_desc VALUES
 ("A01", "Day Care Service", 1429, null),
 ("A02", "Crisis Resolution Team/Home Treatment Service", 1429, null),
 ("A03", "Crisis Resolution Team", 1429, 1458),
 ("A04", "Home Treatment Service", 1429, 1458),
 ("A05", "Primary Care Mental Health Service", 1429, null),
 ("A06", "Community Mental Health Team - Functional", 1429, null),
 ("A07", "Community Mental Health Team - Organic", 1429, null),
 ("A08", "Assertive Outreach Team", 1429, null),
 ("A09", "Community Rehabilitation Service", 1429, null),
 ("A10", "General Psychiatry Service", 1429, null),
 ("A11", "Psychiatric Liaison Service", 1429, null),
 ("A12", "Psychotherapy Service", 1429, null),
 ("A13", "Psychological Therapy Service (non IAPT)", 1429, null),
 ("A14", "Early Intervention Team for Psychosis", 1429, null),
 ("A15", "Young Onset Dementia Team", 1429, null),
 ("A16", "Personality Disorder Service", 1429, null),
 ("A17", "Memory Services/Clinic/Drop in service", 1429, null),
 ("A18", "Single Point of Access Service", 1429, null),
 ("A19", "24/7 Crisis Response Line", 1429, null),
 ("A20", "Health Based Place Of Safety Service", 1429, null),
 ("A21", "Crisis Café/Safe Haven/Sanctuary Service", 1429, null),
 ("A22", "Walk-in Crisis Assessment Unit Service", 1429, null),
 ("A23", "Psychiatric Decision Unit Service", 1429, null),
 ("A24", "Acute Day Service", 1429, null),
 ("A25", "Crisis House Service", 1429, null),
 ("B01", "Forensic Mental Health Service", 1429, null),
 ("B02", "Forensic Learning Disability Service", 1429, null),
 ("C01", "Autism Service", 1429, null),
 ("C02", "Specialist Perinatal Mental Health Community Service", 1429, null),
 ("C04", "Neurodevelopment Team", 1429, null),
 ("C05", "Paediatric Liaison Service", 1429, null),
 ("C06", "Looked After Children Service", 1429, null),
 ("C07", "Youth Offending Service", 1429, null),
 ("C08", "Acquired Brain Injury Service", 1429, null),
 ("C10", "Community Eating Disorder Service", 1429, null),
 ("D01", "Substance Misuse Team", 1429, null),
 ("D02", "Criminal Justice Liaison and Diversion Service", 1429, null),
 ("D03", "Prison Psychiatric Inreach Service", 1429, null),
 ("D04", "Asylum Service", 1429, null),
 ("D05", "Individual Placement and Support Service", 1429, null),
 ("D06", "Mental Health In Education Service", 1429, null),
 ("D07", "Problem Gambling Service", 1429, null),
 ("D08", "Rough Sleeping Service", 1429, null),
 ("E01", "Community Team for Learning Disabilities", 1429, null),
 ("E02", "Epilepsy/Neurological Service", 1429, null),
 ("E03", "Specialist Parenting Service", 1429, null),
 ("E04", "Enhanced/Intensive Support Service", 1429, null),
 ("F01", "Education-based Mental Health Support Team", 1459, null),
 ("F02", "Maternal Mental Health Service", 1459, null),
 ("F03", "Mental Health Services for Deaf people", 1459, null),
 ("F04", "Veterans Complex Treatment Service", 1459, 1488),
 ("F05", "Enhanced care in care homes teams", 1459, null),
 ("F06", "Mental Health and Wellbeing Hubs", 1459, null),
 ("Z01", "Other Mental Health Service - in scope of National Tariff Payment System", 1459, null),
 ("Z02", "Other Mental Health Service - out of scope of National Tariff Payment System", 1459, null),
 ("UNKNOWN", "UNKNOWN", 1459, null)

# COMMAND ----------

 %md
 ### Tables for MHA Measures: Autism and LD Status

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.autism_status_desc;
 CREATE TABLE IF NOT EXISTS $db_output.autism_status_desc
 (
 AutismStatus STRING,
 AutismStatus_desc STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.autism_status_desc;
 INSERT INTO $db_output.autism_status_desc VALUES
 ("1", "Confirmed patient diagnosis of autism", 1489, null),
 ("2", "Suspected patient diagnosis of autism and the patient is on a diagnostic patient pathway for a patient diagnosis of autism", 1489, null),
 ("3", "Suspected patient diagnosis of autism but the patient is not on a diagnostic patient pathway for a patient diagnosis of autism", 1489, null),
 ("4", "Suspected patient diagnosis of autism but it is not known whether the patient is on a diagnostic patient pathway for a patient diagnosis of autism", 1489, null),
 ("5", "No patient diagnosis of autism", 1489, null),
 ("U", "Patient asked but autism status not known", 1489, null),
 ("X", "Not Known (Not Recorded)", 1489, null),
 ("Z", "Not Stated (PATIENT asked but declined to provide a response)", 1489, null),
 ("UNKNOWN", "UNKNOWN", 1489, null)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.LD_status_desc;
 CREATE TABLE IF NOT EXISTS $db_output.LD_status_desc
 (
 LDStatus STRING,
 LDStatus_desc STRING,
 FirstMonth INT,
 LastMonth INT
 ) USING DELTA

# COMMAND ----------

 %sql

 TRUNCATE TABLE $db_output.LD_status_desc;
 INSERT INTO $db_output.LD_status_desc VALUES

 ("1", "Confirmed patient diagnosis of a learning disability", 1489, null),
 ("2", "Suspected patient diagnosis of a learning disability and the patient is on a diagnostic patient pathway for a patient diagnosis of a learning disability", 1489, null),
 ("3", "Suspected patient diagnosis of a learning disability but the patient is not on a diagnostic patient pathway for a patient diagnosis of a learning disability", 1489, null),
 ("4", "Suspected patient diagnosis of a learning disability but it is not known whether the patient is on a diagnostic patient pathway for a patient diagnosis of a learning disability", 1489, null),
 ("5", "No patient diagnosis of a learning disability", 1489, null),
 ("U", "Patient asked but learning disability status not known", 1489, null),
 ("X", "Not Known (Not Recorded)", 1489, null),
 ("Z", "Not Stated (PATIENT asked but declined to provide a response)", 1489, null),
 ("UNKNOWN", "UNKNOWN", 1489, null)

# COMMAND ----------

 %md
 ## v6 Permanent Tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.ServiceTeamType;
 CREATE TABLE IF NOT EXISTS $db_output.ServiceTeamType
 (
 UniqMonthID BIGINT,
 OrgIDProv STRING,
 Person_ID STRING,
 UniqServReqID STRING,
 UniqCareProfTeamID STRING,
 ServTeamTypeRefToMH STRING,
 ServTeamIntAgeGroup STRING,
 ReferClosureDate DATE,
 ReferClosureTime TIMESTAMP,
 ReferClosReason STRING,
 ReferRejectionDate DATE,
 ReferRejectionTime TIMESTAMP,
 ReferRejectReason STRING,
 RecordNumber BIGINT,
 RecordStartDate DATE,
 RecordEndDate DATE
 ) USING DELTA