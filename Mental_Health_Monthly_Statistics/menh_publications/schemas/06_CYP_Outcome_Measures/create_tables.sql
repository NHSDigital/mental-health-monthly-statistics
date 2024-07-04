-- Databricks notebook source
 %sql
 CREATE WIDGET TEXT db_output DEFAULT "menh_publications";

-- COMMAND ----------

 %py
 db_output=dbutils.widgets.get("db_output")
 print(db_output)
 assert db_output
 mhsds_database=dbutils.widgets.get("mhsds_database")
 print(mhsds_database)
 assert mhsds_database

-- COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.peri_breakdown_values;
 CREATE TABLE IF NOT EXISTS $db_output.peri_breakdown_values (breakdown string) USING DELTA;
 
 DROP TABLE IF EXISTS $db_output.peri_level_values_1;
 CREATE TABLE IF NOT EXISTS $db_output.peri_level_values (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA; --See above.
 
 DROP TABLE IF EXISTS $db_output.peri_metric_values;
 CREATE TABLE IF NOT EXISTS $db_output.peri_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- We don't want this table - should only be keeping unformatted measures in product specific tables - others should be in main ALL_products_formatted

DROP TABLE IF EXISTS $db_output.mh_cyp_all_products_formatted

-- CREATE TABLE IF NOT EXISTS $db_output.mh_cyp_all_products_formatted
-- (
--     REPORTING_PERIOD_START string,
--     REPORTING_PERIOD_END string,
--     STATUS string,
--     BREAKDOWN string,
--     PRIMARY_LEVEL string,
--     PRIMARY_LEVEL_DESCRIPTION string,
--     SECONDARY_LEVEL string,
--     SECONDARY_LEVEL_DESCRIPTION string,
--     MEASURE_ID string,
--     MEASURE_NAME string,
--     MEASURE_VALUE string,
--     MEASURE_VALUE_ROUNDED string
-- )
-- USING DELTA

-- COMMAND ----------

-- We don't really need this table - this was a replacement for the existing process of listing all possible measures

DROP TABLE IF EXISTS $db_output.mh_cyp_outcome_monthly_csv

-- CREATE TABLE IF NOT EXISTS $db_output.mh_cyp_outcome_Monthly_CSV
-- (
--     BREAKDOWN string,
--     PRIMARY_LEVEL string,
--     PRIMARY_LEVEL_DESCRIPTION string,
--     SECONDARY_LEVEL string,
--     SECONDARY_LEVEL_DESCRIPTION string,
--     MEASURE_ID string,
--     MEASURE_NAME string
-- )
-- USING DELTA

-- COMMAND ----------

-- DBTITLE 1,create table Assessment_Reference_Data
DROP TABLE IF EXISTS $db_output.mh_assessment_reference_data;

-- Updated Preferred_Term_SNOMED & SNOMED_Version to string from int to allow the data to land in the table!
-- table is truncated and replaced each run during standard run of code

CREATE TABLE IF NOT EXISTS $db_output.MH_Assessment_Reference_Data
(
Category string,
Assessment_Tool_Name string,
Preferred_Term_SNOMED string,
Active_Concept_ID_SNOMED string,
SNOMED_Version string,
Lower_Range	int,
Upper_Range	int,
CYPMH string,
EIP	string,
Rater string
)
USING DELTA;

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db_output.closed_refs;

CREATE TABLE IF NOT EXISTS $db_output.closed_refs
(
UNIQMONTHID long,
PERSON_ID string ,
UNIQSERVREQID string,
RECORDNUMBER  long,
ORGIDPROV  string,
REFERRALREQUESTRECEIVEDDATE date,
SERVDISCHDATE date,
AgeServReferRecDate long,
REF_LENGTH int
)
USING DELTA

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db_output.cont_final;

CREATE TABLE IF NOT EXISTS $db_output.cont_final
(
CONT_TYPE string,
UniqMonthID long,
Person_ID string,
UniqServReqID string,
AgeCareContDate long,
ContID string,
ContDate date,
UniqID long,
RN1 int,
DFC_RN1 int
)
USING DELTA

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db_output.assessments;

CREATE TABLE IF NOT EXISTS $db_output.assessments
(
TYPE string, 
UniqMonthID long, 
codedAssToolType string, 
PersScore int, 
Der_AssToolCompDate date, 
RecordNumber long, 
Der_AssUniqID long, 
OrgIDProv string, 
Person_ID string, 
UniqServReqID string, 
Der_AgeAssessTool long, 
UniqCareContID string, 
UniqCareActID string
)
USING DELTA

-- COMMAND ----------

-- table is truncated during run so DROP is fine
-- DROP TABLE IF EXISTS $db_output.ass_final;

CREATE TABLE IF NOT EXISTS $db_output.ass_final
(
Der_AssUniqID long , 
TYPE string, 
Person_ID string , 
UniqMonthID long , 
OrgIDProv string, 
RecordNumber long, 
UniqServReqID string, 
UniqCareContID string, 
UniqCareActID string, 
Der_AssToolCompDate date, 
CodedAssToolType string, 
PersScore int, 
Der_AgeAssessTool long, 
Der_AssessmentCategory string, 
Assessment_Tool_Name string, 
Der_PreferredTermSNOMED string, -- this needs to be a string as the source field is a string - updated from int above in source table but missed here :o( 
Der_SNOMEDCodeVersion string, -- this needs to be a string as the source field is a string - updated from int above in source table but missed here :o( 
Der_LowerRange int, 
Der_UpperRange int, 
Der_ValidScore string, 
Der_UniqAssessment string, 
Der_AssOrderAsc int, 
Der_AssOrderDesc int 
)
USING DELTA


-- COMMAND ----------

-- DROP TABLE IF EXISTS $db_output.cyp_outcomes;

CREATE TABLE IF NOT EXISTS $db_output.cyp_outcomes
(
UniqMonthID long, 
Person_ID string, 
UniqServReqID string, 
RecordNumber long, 
OrgIDProv string, 
AgeServReferRecDate long, 
ReferralRequestReceivedDate date, 
ServDischDate date, 
Ref_Length integer, 
Der_InYearContacts integer, 
Der_FirstAssessmentDate date, 
Der_FirstAssessmentToolName string, 
Der_LastAssessmentDate date, 
Der_LastAssessmentToolName string, 
Der_ReftoFirstAss int,
Der_LastAsstoDisch int
)
USING DELTA



-- COMMAND ----------

-- DROP TABLE IF EXISTS $db_output.cyp_outcomes_output;

CREATE TABLE IF NOT EXISTS $db_output.cyp_outcomes_output
(
OrgID string, 
MHS92 long, 
MHS93 long, 
MHS94 double
)
USING DELTA

-- COMMAND ----------

-- Removing the old table that was truncated each month and replacing with a permanent data store in the correct structure.

DROP TABLE IF EXISTS $db_output.cyp_peri_monthly;

CREATE TABLE IF NOT EXISTS $db_output.cyp_peri_monthly_unformatted
(
REPORTING_PERIOD_START string, 
REPORTING_PERIOD_END string, 
STATUS string , 
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

-- COMMAND ----------

-- DBTITLE 1,MHA91 - Create CCG_MAPPING_2021 table for reference data
 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.CCG_MAPPING_2021 
 (
   CCG_UNMAPPED STRING, 
   CCG21CDH STRING, 
   CCG21CD STRING, 
   CCG21NM STRING, 
   STP21CD STRING, 
   STP21CDH STRING, 
   STP21NM STRING, 
   NHSER21CD STRING, 
   NHSER21CDH STRING, 
   NHSER21NM STRING
 )
 USING DELTA

-- COMMAND ----------

-- DBTITLE 1,MHA91 - Create MH_ASS table for reference data
 %sql
 
 -- this table is recreated with a longer name IN THIS NOTEBOOK and this MH_ASS version is not used anywhere else other than being populated with nearly duplicate data
 
 DROP TABLE IF EXISTS $db_output.mh_ass; 
 
 -- CREATE TABLE IF NOT EXISTS $db_output.MH_ASS 
 -- ( Category STRING,
 --   Assessment_Tool_Name STRING,
 --   Preferred_Term_SNOMED STRING,
 --   Active_Concept_ID_SNOMED BIGINT,
 --   SNOMED_Version STRING,
 --   Lower_Range INT,
 --   Upper_Range INT,
 --   CYPMH STRING,
 --   EIP STRING,
 --   Rater STRING
 -- )
 -- USING DELTA

-- COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.MHS95_Main 
 ( 
 UniqMonthID bigint,
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
 RecordNumber bigint,
 UniqServReqID string,
 AccessLARN int,
 AccessCCGRN int,
 AccessCCGProvRN int,
 AccessRNProv int,
 AccessEngRN int,
 AccessSTPRN int,
 AccessRegionRN int
 )
 USING DELTA