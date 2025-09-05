# Databricks notebook source
# dbutils.widgets.text("db_output","","db_output")
# dbutils.widgets.text("db_source","testdata_menh_analysis_mhsds_database","db_source")

# COMMAND ----------

# %sql
# DESCRIBE TABLE $db_source.MHS001MPI

# COMMAND ----------

# DBTITLE 1,Creating the Main_monthly_unformatted_exp table that holds yet to be approved measures that are generated
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_unformatted_exp
 (
     REPORTING_PERIOD_START DATE,
     REPORTING_PERIOD_END DATE,
     STATUS STRING,
     BREAKDOWN string,
     PRIMARY_LEVEL string,
     PRIMARY_LEVEL_DESCRIPTION string,
     SECONDARY_LEVEL string,
     SECONDARY_LEVEL_DESCRIPTION string,
     METRIC string,
     METRIC_VALUE float,
     SOURCE_DB string
 )
 USING DELTA
 PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs001mpi_latest_month_data; 

 CREATE TABLE if not exists $db_output.tmp_mhmab_mhs001mpi_latest_month_data (
 AgeDeath bigint
 ,AgeRepPeriodEnd	bigint
 ,Age_Band	string
 ,AgeRepPeriodStart	bigint
 ,County	string
 ,DefaultPostcode	string
 ,ElectoralWard	string
 ,EthnicCategory	string
 ,Gender	string
 ,GenderIDCode	string
 ,Der_Gender	string
 ,Der_GenderName string
 ,IMDQuart	string
 ,LADistrictAuth	string
 ,LDAFlag	boolean
 ,LSOA	string
 ,LSOA2011	string
 ,LanguageCodePreferred	string
 ,LocalPatientId	string
 ,MHS001UniqID	bigint
 ,NHSDEthnicity	string
 ,LowerEthnicity	string
 ,LowerEthnicity_Desc	string
 ,NHSNumber	string
 ,NHSNumberStatus	string
 ,OrgIDCCGRes	string
 ,OrgIDEduEstab	string
 ,OrgIDLocalPatientId	string
 ,OrgIDProv	string
 ,OrgIDResidenceResp	string
 ,PatMRecInRP	boolean
 ,Person_ID	string
 ,PostcodeDistrict	string
 ,RecordEndDate	date
 ,RecordNumber	bigint
 ,RecordStartDate	date
 ,RowNumber	bigint
 ,UniqMonthID	bigint
 ,UniqSubmissionID	bigint
 ,IC_Rec_CCG	string
 ,NAME	string
 ,IMD_Decile	string
 ,AccommodationType	string
 ,AccommodationType_Desc	string 
 ,EmployStatus	string
 ,EmployStatus_Desc	string
 ,DisabCode	string
 ,DisabCode_Desc	string
 ,Sex_Orient	string
 ,RuralUrbanClassName string
 ,AutismStatus string
 ,AutismStatus_desc string
 ,LDStatus string
 ,LDStatus_desc string
 ) USING DELTA PARTITIONED BY (UniqMonthID)

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_rd_ccg_latest;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_rd_ccg_latest (
 ORG_CODE string, NAME string) USING DELTA

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_ccg;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_ccg (
 Person_ID string, IC_REC_GP_RES string, NAME string) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30f_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30f_prep (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendStatus string,
 Person_ID string,
 UniqServReqID string,
 ConsMechanismMH string,
 CareContCancelDate date,
 CareContDate date,
 DNA_Reason string,
 ConsMedUsed string,
 CMU string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30f_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30f_prep_prov (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendStatus	string,
 Person_ID string,
 UniqServReqID string,
 ConsMechanismMH string,
 CareContCancelDate date,
 CareContDate date,
 DNA_Reason string,
 ConsMedUsed string,
 CMU string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30h_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30h_prep (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendStatus string,
 Person_ID string,
 UniqServReqID string,
 ConsMechanismMH string,
 CareContCancelDate date,
 CareContDate date,
 DNA_Reason string,
 ConsMedUsed string,
 CMU string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30h_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30h_prep_prov (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendStatus string,
 Person_ID string,
 UniqServReqID string,
 ConsMechanismMH string,
 CareContCancelDate date,
 CareContDate date,
 DNA_Reason string,
 ConsMedUsed string,
 CMU string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs32c_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs32c_prep
 (
 UniqServReqID	string,
 SourceOfReferralMH	string,
 IC_REC_CCG	string,
 NAME	string,
 Referral_Source	string,
 Referral_Description	string,
 AgeGroup	string,
 AgeGroupName string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs32_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs32_prep
 (
 UniqServReqID    string,
 Person_ID    string,
 Age_Band    string,
 Der_Gender    string,
 Der_GenderName    string,
 LowerEthnicity    string,
 LowerEthnicity_Desc    string,
 IMD_Decile    string,
 AccommodationType    string,
 AccommodationType_Desc    string,
 EmployStatus    string,
 EmployStatus_Desc    string,
 DisabCode    string,
 DisabCode_Desc    string,
 Sex_Orient    string,
 RuralUrbanClassName string,
 AutismStatus string,
 AutismStatus_desc string,
 LDStatus string,
 LDStatus_desc string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS  $db_output.tmp_mhmab_mhs32c_prep_prov;
 CREATE TABLE IF NOT EXISTS  $db_output.tmp_mhmab_mhs32c_prep_prov
 (
 UniqServReqID	string,
 SourceOfReferralMH	string,
 OrgIDProv	string,
 Referral_Source	string,
 Referral_Description	string,
 AgeGroup	string,
 AgeGroupName string
 );

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs32d_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs32d_prep
 (
 UniqServReqID	string,
 SourceOfReferralMH	string,
 IC_REC_CCG	string,
 NAME	string,
 Referral_Source	string,
 Referral_Description	string,
 AgeGroup	string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs32d_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs32d_prep_prov
 (
 UniqServReqID	string,
 SourceOfReferralMH	string,
 OrgIDProv	string,
 Referral_Source	string,
 Referral_Description	string,
 AgeGroup	string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57b_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57b_prep
 (
 Person_ID string,
 IC_REC_CCG string,
 NAME string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57b_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57b_prep_prov
 (
 Person_ID string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57c_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57c_prep
 (
 Person_ID string,
 IC_REC_CCG string,
 NAME string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57c_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57c_prep_prov
 (
 Person_ID string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs01_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs01_prep
 (UniqServReqID    string,
 Person_ID    string,
 Age_Band    string,
 Der_Gender    string,
 Der_GenderName string,
 LowerEthnicity    string,
 LowerEthnicity_Desc    string,
 IMD_Decile    string,
 AccommodationType    string,
 AccommodationType_Desc    string,
 EmployStatus    string,
 EmployStatus_Desc    string,
 DisabCode    string,
 DisabCode_Desc    string,
 Sex_Orient    string,
 RuralUrbanClassName string,
 AutismStatus string,
 AutismStatus_desc string,
 LDStatus string,
 LDStatus_desc string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs501hospprovspell_latest_month_data;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs501hospprovspell_latest_month_data
 (
 DecidedToAdmitDate	date,
 DecidedToAdmitTime	timestamp,
 DestOfDischHospProvSpell	string,
 DischDateHospProvSpell	date,
 DischTimeHospProvSpell	timestamp,
 EstimatedDischDateHospProvSpell	date,
 HospProvSpellID	string,
 InactTimeHPS	date,
 LOSDischHosSpell	bigint,
 LOSHosSpellEoRP	bigint,
 MHS501UniqID	bigint,
 MethAdmMHHospProvSpell	string,
 MethOfDischMHHospProvSpell	string,
 OrgIDProv	string,
 Person_ID	string,
 PlannedDestDisch	string,
 PlannedDischDateHospProvSpell	date,
 PostcodeDistrictDischDest	string,
 PostcodeDistrictMainVisitor	string,
 RecordEndDate	date,
 RecordNumber	bigint,
 RecordStartDate	date,
 RowNumber	bigint,
 ServiceRequestId	string,
 SourceAdmMHHospProvSpell	string,
 StartDateHospProvSpell	date,
 StartTimeHospProvSpell	timestamp,
 TimeEstDischDate	bigint,
 TimePlanDischDate	bigint,
 TransformingCareCategory	string,
 TransformingCareInd	string,
 UniqHospProvSpellID	string,
 UniqMonthID	bigint,
 UniqServReqID	string,
 UniqSubmissionID	bigint
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs07_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs07_prep (
 UniqServReqID    string,
 Person_ID    string,
 UniqHospProvSpellID    string,
 Age_Band    string,
 Der_Gender    string,
 Der_GenderName    string,
 LowerEthnicity    string,
 LowerEthnicity_Desc    string,
 IMD_Decile    string,
 AccommodationType    string,
 AccommodationType_Desc    string,
 EmployStatus    string,
 EmployStatus_Desc    string,
 DisabCode    string,
 DisabCode_Desc    string,
 Sex_Orient    string,
 RuralUrbanClassName  string,
 AutismStatus string,
 AutismStatus_desc string,
 LDStatus string,
 LDStatus_desc string) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs101referral_open_end_rp;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs101referral_open_end_rp
 (AgeServReferDischDate	bigint,
 AgeServReferRecDate	bigint,
 ClinRespPriorityType	string,
 DischPlanCreationDate	date,
 DischPlanCreationTime	timestamp,
 DischPlanLastUpdatedDate	date,
 DischPlanLastUpdatedTime	timestamp,
 InactTimeRef	date,
 ReferralServiceAreasOpenEndRPLDA	boolean,
 LocalPatientId	string,
 MHS101UniqID	bigint,
 NHSServAgreeLineNum	string,
 OrgIDComm	string,
 OrgIDProv	string,
 OrgIDReferring	string,
 Person_ID	string,
 PrimReasonReferralMH	string,
 ReasonOAT	string,
 RecordEndDate	date,
 RecordNumber	bigint,
 RecordStartDate	date,
 ReferralRequestReceivedDate	date,
 ReferralRequestReceivedTime	timestamp,
 ReferringCareProfessionalStaffGroup	string,
 ServDischDate	date,
 ServDischTime	timestamp,
 ServiceRequestId	string,
 SourceOfReferralMH	string,
 SpecialisedMHServiceCode	string,
 UniqMonthID	bigint,
 UniqServReqID	string,
 UniqSubmissionID	bigint,
 CYPServiceRefEndRP_temp	boolean,
 LDAServiceRefEndRP_temp	boolean,
 AMHServiceRefEndRP_temp	boolean
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RefAgeGroup;
 CREATE TABLE IF NOT EXISTS $db_output.RefAgeGroup
 (AgeGroup string, AgeGroupName string);

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RefAgeBand;
 CREATE TABLE IF NOT EXISTS $db_output.RefAgeBand (AgeBand string);


# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RefBedType;
 CREATE TABLE IF NOT EXISTS $db_output.RefBedType (BedType string);

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RefIMD;
 CREATE TABLE IF NOT EXISTS $db_output.RefIMD (IMD_Desc string);

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