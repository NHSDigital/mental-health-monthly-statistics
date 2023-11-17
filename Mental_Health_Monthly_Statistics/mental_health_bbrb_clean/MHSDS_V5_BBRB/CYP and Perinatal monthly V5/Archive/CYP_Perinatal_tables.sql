-- Databricks notebook source
%py
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

-- COMMAND ----------

--DROP TABLE IF EXISTS $db_output.ASS_FINAL

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.CYP_OUTCOMES ; --- gvf
CREATE TABLE IF NOT EXISTS $db_output.CYP_OUTCOMES

(UniqMonthID bigint,
Person_ID string,
UniqServReqID string,
RecordNumber string,
OrgIDProv string,
AgeServReferRecDate bigint,
ReferralRequestReceivedDate date,
ServDischDate date,
Ref_Length bigint,
Der_InYearContacts bigint,
Der_FirstAssessmentDate date,
Der_FirstAssessmentToolName string,
Der_LastAssessmentDate date,
Der_LastAssessmentToolName string,
Der_ReftoFirstAss bigint, 
Der_LastAsstoDisch bigint)
using delta 

-- COMMAND ----------

TRUNCATE TABLE $db_output.CYP_OUTCOMES

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.CYP_PERI_monthly 


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $db_output.CYP_PERI_monthly 
(
REPORTING_PERIOD_START date,
REPORTING_PERIOD_END date,
STATUS string,
BREAKDOWN string,
PRIMARY_LEVEL string,
PRIMARY_LEVEL_DESCRIPTION string,
SECONDARY_LEVEL string,
SECONDARY_LEVEL_DESCRIPTION string,
MEASURE_ID string,
MEASURE_NAME string,
MEASURE_VALUE float
) USING DELTA

-- COMMAND ----------

TRUNCATE TABLE $db_output.CYP_PERI_monthly 

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.FirstCont_Final;
CREATE TABLE IF NOT EXISTS $db_output.FirstCont_Final

(UniqMonthID int,
	OrgIDProv string,
	PROV_NAME string,
    IC_REC_CCG string,
    CCG_NAME string,
    STP_CODE string,
    STP_NAME string,
    REGION_CODE string,
    REGION_NAME string,
	LADistrictAuth string,
	Person_ID string,
	RecordNumber string,
	UniqServReqID string,
	AccessLARN int,
	AccessCCGRN int, 
	AccessCCGProvRN int, 
	AccessRNProv int,
	AccessEngRN int, 
	AccessSTPRN int,
	AccessRegionRN int
    ) USING DELTA

-- COMMAND ----------

TRUNCATE TABLE $db_output.FirstCont_Final

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.ASSESSMENTS;

CREATE TABLE IF NOT EXISTS $db_output.ASSESSMENTS

(TYPE string,
  UniqMonthID int,
  CodedAssToolType string,
  PersScore int,
  Der_AssToolCompDate date,
  RecordNumber string,
  Der_AssUniqID string,
  OrgIDProv string,
  Person_ID string,
  UniqServReqID string,
  Der_AgeAssessTool string,
  UniqCareContID string,
  UniqCareActID string)
  using delta

-- COMMAND ----------

TRUNCATE TABLE $db_output.ASSESSMENTS

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.CLOSED_REFS;
CREATE TABLE IF NOT EXISTS $db_output.CLOSED_REFS

(UNIQMONTHID string,
PERSON_ID string,
UNIQSERVREQID string,
RECORDNUMBER string,
ORGIDPROV string,
REFERRALREQUESTRECEIVEDDATE date,
SERVDISCHDATE date,
AgeServReferRecDate int,
REF_LENGTH int)
using delta

-- COMMAND ----------

TRUNCATE TABLE $db_output.CLOSED_REFS

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.CONT_FINAL;

CREATE TABLE IF NOT EXISTS $db_output.CONT_FINAL

(CONT_TYPE string,
  UniqMonthID int,
  Person_ID string,
  UniqServReqID string,
  AgeCareContDate int,
  ContID string,
  ContDate date,
  UniqID string,
  RN1 int,
  DFC_RN1 int)
  using delta

-- COMMAND ----------

TRUNCATE TABLE $db_output.CONT_FINAL

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.ASS_FINAL;

CREATE TABLE IF NOT EXISTS $db_output.ASS_FINAL

(Der_AssUniqID string,
	TYPE string,
	Person_ID string,	
	UniqMonthID string,	
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
	Assessment_Tool_Name string,
	Der_PreferredTermSNOMED string,
	Der_SNOMEDCodeVersion string,
	Der_LowerRange string,
	Der_UpperRange string,
	Der_ValidScore string,
	Der_UniqAssessment string,
    Der_AssOrderAsc int,
    Der_AssOrderDesc int)
    using delta

-- COMMAND ----------

TRUNCATE TABLE $db_output.ASS_FINAL

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.CYP_OUTCOMES_OUTPUT;

CREATE TABLE IF NOT EXISTS $db_output.CYP_OUTCOMES_OUTPUT

(OrgID STRING,
MHS92 int, 
MHS93 int,
MHS94 float)
 USING DELTA

-- COMMAND ----------

TRUNCATE TABLE $db_output.CYP_OUTCOMES_OUTPUT

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.Perinatal_M_Master;

CREATE TABLE IF NOT EXISTS $db_output.Perinatal_M_Master

(UniqMonthID string,
Person_ID string,
UniqServReqID string,
OrgIDProv string,
NAME string,
OrgIDCCGRes string,
CCG_NAME string,
STP_Code string,
STP_NAME string,
REGION_CODE string,
REGION_NAME string,
LACode string,
AttContacts string,
NotAttContacts string,
FYAccessLARN string, 
FYAccessRNProv string,
FYAccessCCGRN string,
FYAccessSTPRN string,
FYAccessRegionRN string,
FYAccessEngRN string
) USING DELTA

-- COMMAND ----------

TRUNCATE TABLE $db_output.Perinatal_M_Master