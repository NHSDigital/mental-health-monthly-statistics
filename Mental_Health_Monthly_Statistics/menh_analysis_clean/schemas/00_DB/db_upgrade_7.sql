-- Databricks notebook source
%md 
# LDA Output Tables

-- COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.lda_counts ;

CREATE TABLE IF NOT EXISTS $db_output.LDA_Counts 
(
PreviousMonthEnd string,
PeriodStart string,
PeriodEnd string,
Geography string,
OrgCode string,
OrgName string,
TableNumber int,
PrimaryMeasure string,
PrimaryMeasureNumber string,
PrimarySplit string,
SecondaryMeasure string,
SecondaryMeasureNumber string,
SecondarySplit string,
RestraintsCountOfPeople int,
RestraintsCountOfRestraints int,
HospitalSpellsInCarePreviousMonth int,
HospitalSpellsAdmissionsInMonth int, 
HospitalSpellsDischargedInMonth int, 
HospitalSpellsAdmittedAndDischargedInMonth int, 
HospitalSpellsOpenAtEndOfMonth int, 
WardStaysInCarePreviousMonth int, 
WardStaysAdmissionsInMonth int, 
WardStaysDischargedInMonth int, 
WardStaysAdmittedAndDischargedInMonth int, 
WardStaysOpenAtEndOfMonth int, 
OpenReferralsPreviousMonth int, 
ReferralsStartingInTheMonth int, 
ReferralsEndingInTheMonth int, 
ReferralsStartingAndEndingInTheMonth int, 
ReferralsOpenAtEndOfMonth int,
ORG_TYPE_CODE string,
PRODUCT string,
SOURCE_DB string
) 
USING DELTA 
PARTITIONED BY (PRODUCT, PeriodEnd)

-- COMMAND ----------

%sql

DROP TABLE IF EXISTS $db_output.lda_data_1 ;

CREATE TABLE IF NOT EXISTS $db_output.LDA_Data_1
( 
        Person_ID string,
		AgeRepPeriodEnd int,
		Gender string,
		NHSDEthnicity string,
		PatMRecInRP string,
		UniqServReqID string,
		ReferralRequestReceivedDate string,
		ServDischDate string,
		REF_OrgCodeProv string,
		OrgIDComm string,
		UniqHospProvSpellID string,
		StartDateHospProvSpell string,
		DischDateHospProvSpell string,
		MethAdmMHHospProvSpell string,
		DestOfDischHospProvSpell string,
		HSP_OrgCodeProv string,
		PlannedDischDateHospProvSpell string,
		UniqWardStayID string,
        SiteIDOfTreat string,
		StartDateWardStay string,
		EndDateWardStay string,
		WardLocDistanceHome string,
		WardSecLevel string,
		WardType string,
		StartDateDelayDisch string,
		EndDateDelayDisch string,
		DelayDischReason string,
        RespiteCare string,
		HSP_LOS string,
		WARD_LOS string,
        CombinedProvider string,
		CombinedCommissioner string,
        realrestraintinttype string,
        RestrictiveIntType string,
        StartDateRestrictiveInt string,
        EndDateRestrictiveInt string,
        RestrictiveIntTypeDesc string,	
        RestrictiveID string,
        NAME string,
        ORG_TYPE_CODE_PROV string,
        OrgCode string,
        OrgName string,
        TCP_Code string,
        TCP_NAME string,
        Region_code string,
        Region_name string,
        ORG_TYPE_ORG_R string, 
        ORG_CODE_R string,
        ORG_NAME_R string,
        ORG_TYPE_CODE string
) 
USING DELTA 
-- PARTITIONED BY (PRODUCT, PeriodEnd)

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_geography_values;
CREATE TABLE IF NOT EXISTS $db_output.lda_geography_values 
(
Geography STRING,
OrgCode STRING,
OrgName STRING
) USING DELTA

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_level_values;
CREATE TABLE IF NOT EXISTS $db_output.lda_level_values 
(
Geography STRING,
PrimaryMeasure STRING,
PrimarySplit STRING,
SecondaryMeasure STRING,
SecondarySplit STRING
) USING DELTA

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_csv_lookup;
CREATE TABLE IF NOT EXISTS $db_output.lda_csv_lookup
(
Geography STRING,
OrgCode STRING,
OrgName STRING,
PrimaryMeasure STRING,
PrimarySplit STRING,
SecondaryMeasure STRING,
SecondarySplit STRING
) USING DELTA

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_all;
CREATE TABLE IF NOT EXISTS $db_output.lda_all
(
PreviousMonthEnd string,
PeriodStart string,
PeriodEnd string,
Geography string,
OrgCode string,
OrgName string,
TableNumber int,
PrimaryMeasure string,
PrimaryMeasureNumber string,
PrimarySplit string,
SecondaryMeasure string,
SecondaryMeasureNumber string,
SecondarySplit string,
RestraintsCountOfPeople int,
RestraintsCountOfRestraints int,
HospitalSpellsInCarePreviousMonth int,
HospitalSpellsAdmissionsInMonth int, 
HospitalSpellsDischargedInMonth int, 
HospitalSpellsAdmittedAndDischargedInMonth int, 
HospitalSpellsOpenAtEndOfMonth int, 
WardStaysInCarePreviousMonth int, 
WardStaysAdmissionsInMonth int, 
WardStaysDischargedInMonth int, 
WardStaysAdmittedAndDischargedInMonth int, 
WardStaysOpenAtEndOfMonth int, 
OpenReferralsPreviousMonth int, 
ReferralsStartingInTheMonth int, 
ReferralsEndingInTheMonth int, 
ReferralsStartingAndEndingInTheMonth int, 
ReferralsOpenAtEndOfMonth int,
NHS_NHD_SPLIT string,
ORG_TYPE_CODE string
) 
USING DELTA 

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_mi;
CREATE TABLE IF NOT EXISTS $db_output.lda_mi
( 
Person_ID string,
AgeRepPeriodEnd int,
Gender string,
NHSDEthnicity string,
UniqHospProvSpellID string,
HSP_OrgCodeProv string,
Name string,
OrgCode string,
OrgName string,
RespiteCare string,
StartDateHospProvSpell string,
DischDateHospProvSpell string,
DestOfDischHospProvSpell string,
UniqWardStayID string,
SiteIDOfTreat string,
StartDateWardStay string,
EndDateWardStay string,
WardSecLevel string,
WardType string
) 
USING DELTA 

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_nonR;
CREATE TABLE IF NOT EXISTS $db_output.lda_nonR
(
PreviousMonthEnd string,
PeriodStart string,
PeriodEnd string,
Geography string,
OrgCode string,
OrgName string,
TableNumber int,
PrimaryMeasure string,
PrimaryMeasureNumber string,
PrimarySplit string,
SecondaryMeasure string,
SecondaryMeasureNumber string,
SecondarySplit string,
RestraintsCountOfPeople int,
RestraintsCountOfRestraints int,
HospitalSpellsInCarePreviousMonth int,
HospitalSpellsAdmissionsInMonth int, 
HospitalSpellsDischargedInMonth int, 
HospitalSpellsAdmittedAndDischargedInMonth int, 
HospitalSpellsOpenAtEndOfMonth int, 
WardStaysInCarePreviousMonth int, 
WardStaysAdmissionsInMonth int, 
WardStaysDischargedInMonth int, 
WardStaysAdmittedAndDischargedInMonth int, 
WardStaysOpenAtEndOfMonth int, 
OpenReferralsPreviousMonth int, 
ReferralsStartingInTheMonth int, 
ReferralsEndingInTheMonth int, 
ReferralsStartingAndEndingInTheMonth int, 
ReferralsOpenAtEndOfMonth int,
NHS_NHD_SPLIT string,
ORG_TYPE_CODE string
) 
USING DELTA 

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_table14;
CREATE TABLE IF NOT EXISTS $db_output.lda_table14
(
CombinedProvider string,
CombinedProviderName string,
CombinedProviderType string,
CommunityCTR int,
PostAdmissionCTR int,
InpatientCTR int
) 
USING DELTA 

-- COMMAND ----------

%sql
-- DROP TABLE IF EXISTS $db_output.lda_published;
CREATE TABLE IF NOT EXISTS $db_output.lda_published
(
Geography string,
OrgCode	string,
OrgName	string,
PrimaryMeasure string,
PrimarySplit string,
SecondaryMeasure string,
SecondarySplit string,
Restraints_Count_of_people string,
Restraints_Count_of_restraints string,
LDA43_Hospital_spells_open_at_start_of_RP string,
LDA27_Hospital_admissions_in_the_RP	string,
LDA28_Hospital_discharges_in_the_RP	string,
LDA44_Hospital_admissions_and_discharges_in_the_RP string,
LDA45_Hospital_spells_open_at_the_end_of_the_RP	string,
LDA46_Ward_stays_open_at_start_of_the_RP string,
LDA47_Ward_stays_starting_in_the_RP	string,
LDA48_Ward_stays_ending_in_the_RP string,
LDA49_Ward_stays_starting_and_ending_in_the_RP string,
LDA21_Ward_stays_open_at_the_end_of_the_RP string,
LDA50_Referrals_open_at_the_start_of_the_RP	string,
LDA32_Referrals_starting_in_the_RP string,
LDA51_Referrals_ending_in_the_RP string,
LDA52_Referrals_starting_and_ending_in_the_RP string,
LDA23_Referrals_open_at_the_end_of_the_RP string,
Community_Care_and_Treatment_Review_Pre_admission string,
Post_admission_Care_and_Treatment_Review string,
Inpatient_Care_and_Treatment_Review_More_than_two_weeks_post_admission string
) 
USING DELTA 