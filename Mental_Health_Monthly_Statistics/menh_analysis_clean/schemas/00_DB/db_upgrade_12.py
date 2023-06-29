# Databricks notebook source
# dbutils.widgets.text("db_output","","db_output")
# dbutils.widgets.text("db_source","testdata_menh_analysis_$db_source","db_source")

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
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs001mpi_latest_month_data; 
 
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
 ) USING DELTA PARTITIONED BY (UniqMonthID)

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_rd_ccg_latest;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_rd_ccg_latest (
 ORG_CODE string, NAME string);

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_ccg;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_ccg (
 Person_ID string, IC_REC_GP_RES string, NAME string);

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30f_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30f_prep (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendOrDNACode string,
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
 );

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30f_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30f_prep_prov (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendOrDNACode	string,
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
 );

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30h_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30h_prep (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendOrDNACode string,
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
 );

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs30h_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs30h_prep_prov (
 UniqCareContID string,
 NAME string,
 IC_REC_GP_RES string,
 OrgIDProv string,
 AttendOrDNACode string,
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
 );

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
 );

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs32_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs32_prep
 (
 UniqServReqID	string,
 Person_ID	string,
 Age_Band	string,
 Der_Gender	string,
 LowerEthnicity	string,
 LowerEthnicity_Desc	string,
 IMD_Decile	string,
 AccommodationType	string,
 AccommodationType_Desc	string,
 EmployStatus	string,
 EmployStatus_Desc	string,
 DisabCode	string,
 DisabCode_Desc	string,
 Sex_Orient	string
 );

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
 );

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
 );

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57b_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57b_prep
 (
 Person_ID string,
 IC_REC_CCG string,
 NAME string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 );

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57b_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57b_prep_prov
 (
 Person_ID string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 );

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57c_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57c_prep
 (
 Person_ID string,
 IC_REC_CCG string,
 NAME string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 );

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs57c_prep_prov;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs57c_prep_prov
 (
 Person_ID string,
 OrgIDProv string,
 AgeGroup string,
 AgeGroupName string
 );

# COMMAND ----------

 %sql
 --DROP TABLE IF EXISTS $db_output.tmp_mhmab_mhs01_prep;
 CREATE TABLE IF NOT EXISTS $db_output.tmp_mhmab_mhs01_prep
 (UniqServReqID	string,
 Person_ID	string,
 Age_Band	string,
 Der_Gender	string,
 LowerEthnicity	string,
 LowerEthnicity_Desc	string,
 IMD_Decile	string,
 AccommodationType	string,
 AccommodationType_Desc	string,
 EmployStatus	string,
 EmployStatus_Desc	string,
 DisabCode	string,
 DisabCode_Desc	string,
 Sex_Orient	string
 );

# COMMAND ----------

