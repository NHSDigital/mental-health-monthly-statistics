-- Databricks notebook source
%py
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("end_month_id")
rp_startdate = dbutils.widgets.get("rp_startdate_1m")
rp_enddate = dbutils.widgets.get("rp_enddate")

-- COMMAND ----------

%py
# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $db_source.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $db_source.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $db_source.mhs000header order by Uniqmonthid").collect()]

# dbutils.widgets.dropdown("rp_startdate", "2021-03-01", startchoices)
# dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
# dbutils.widgets.dropdown("month_id", "1452", monthid)
# dbutils.widgets.text("status","Performance")
# dbutils.widgets.text("db_output","$user_id")
# dbutils.widgets.text("db_source","$mhsds_database")

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.sct_concepts; 
CREATE TABLE $db_output.SCT_Concepts
(
DSS_KEY int,
ID string,
EFFECTIVE_TIME date,
ACTIVE string,
MODULE_ID string,
DEFINITION_STATUS_ID string,
SYSTEM_CREATED_DATE date
) USING DELTA

-- COMMAND ----------

INSERT INTO $db_output.SCT_Concepts
SELECT *
FROM $reference_data.snomed_ct_rf2_concepts

-- COMMAND ----------

INSERT INTO $db_output.SCT_Concepts
SELECT *
FROM $reference_data.SNOMED_CT_RF2_DRUG_CONCEPTS

-- COMMAND ----------

OPTIMIZE $db_output.SCT_Concepts

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.sct_descriptions; 
CREATE TABLE $db_output.SCT_Descriptions(
DSS_KEY int,
ID string,
EFFECTIVE_TIME date,
ACTIVE string,
MODULE_ID string,
CONCEPT_ID string,
LANGUAGE_CODE string,
TYPE_ID string,
TERM string,
CASE_SIGNIFICANCE_ID string,
SYSTEM_CREATED_DATE date
) USING DELTA

-- COMMAND ----------

INSERT INTO $db_output.SCT_Descriptions
SELECT *
FROM $reference_data.snomed_ct_rf2_descriptions

-- COMMAND ----------

INSERT INTO $db_output.SCT_Descriptions
SELECT DSS_KEY ,ID ,EFFECTIVE_TIME ,ACTIVE ,MODULE_ID ,CONCEPT_ID ,LANGUAGE_CODE ,TYPE_ID ,TERM ,CASE_SIGNIFICANCE_ID ,DSS_SYSTEM_CREATED_DATE
FROM $reference_data.snomed_ct_rf2_drug_desc_v01

-- COMMAND ----------

OPTIMIZE $db_output.SCT_Descriptions

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.sct_language; 
CREATE TABLE $db_output.SCT_Language(
DSS_KEY int,
ID string,
EFFECTIVE_TIME date,
ACTIVE string,
MODULE_ID string,
REFSET_ID string,
REFERENCED_COMPONENT_ID string,
ACCEPTABILITY_ID string,
SYSTEM_CREATED_DATE date
) USING DELTA

-- COMMAND ----------

INSERT INTO $db_output.SCT_Language
SELECT DSS_KEY, ID , EFFECTIVE_TIME , ACTIVE ,MODULE_ID ,REFSET_ID ,REFERENCED_COMPONENT_ID ,ACCEPTABILITY_ID ,DSS_SYSTEM_CREATED_DATE 
FROM $reference_data.snomed_ct_rf2_crefset_language

-- COMMAND ----------

INSERT INTO $db_output.SCT_Language
SELECT DSS_KEY, ID , EFFECTIVETIME , ACTIVE ,MODULEID ,REFSETID ,REFERENCEDCOMPONENTID ,ACCEPTABILITYID ,DSS_SYSTEM_CREATED_DATE 
FROM $reference_data.snomed_ct_rf2_drug_language

-- COMMAND ----------

OPTIMIZE $db_output.SCT_Language

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW CONCEPT_SNAP AS 
 
SELECT *
FROM $db_output.SCT_Concepts c1
WHERE c1.EFFECTIVE_TIME=(SELECT MAX(c2.EFFECTIVE_TIME)
FROM $db_output.SCT_Concepts c2 where c1.ID=c2.ID)

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW DESCRIPTION_SNAP AS 
 
SELECT *
FROM $db_output.sct_descriptions c1
WHERE c1.EFFECTIVE_TIME=(SELECT MAX(c2.EFFECTIVE_TIME)
FROM $db_output.sct_descriptions c2 where c1.ID=c2.ID)

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW LANGUAGE_SNAP AS 
 
SELECT *
FROM $db_output.sct_language c1
WHERE c1.EFFECTIVE_TIME=(SELECT MAX(c2.EFFECTIVE_TIME) FROM $db_output.sct_language c2 where c1.ID=c2.ID)

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW DESCRIPTION_TYPE AS 
 
SELECT d.CONCEPT_ID, d.ID, d.TERM,
CASE 
  WHEN r.ACCEPTABILITY_ID IN ('900000000000548007') AND d.type_id IN ('900000000000003001') THEN 'F'
  WHEN r.ACCEPTABILITY_ID IN ('900000000000548007') AND d.type_id IN ('900000000000013009') THEN 'P'
  WHEN r.ACCEPTABILITY_ID IN ('900000000000549004') AND d.type_id IN ('900000000000013009') THEN 'S'
ELSE null END AS TYPE
FROM  global_temp.DESCRIPTION_SNAP d
LEFT JOIN global_temp.LANGUAGE_SNAP r
ON d.ID=r.REFERENCED_COMPONENT_ID
WHERE r.REFSET_ID IN ('999001261000000100', '999000691000001104')
AND r.active=1 

-- COMMAND ----------

%sql
select count(ID), count(distinct ID) from global_temp.concept_snap

-- COMMAND ----------

select count(CONCEPT_ID), count(distinct concept_id) from global_temp.description_type where TYPE = 'F'

-- COMMAND ----------

select count(CONCEPT_ID), count(distinct concept_id) from global_temp.description_type where TYPE = 'P'

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.sct_concepts_fsn; 
CREATE TABLE $db_output.SCT_Concepts_FSN(
 
ID string,
TERM string,
ACTIVE string,
EFFECTIVE_TIME date,
SYSTEM_CREATED_DATE date
) USING DELTA;
 
INSERT INTO $db_output.SCT_Concepts_FSN
SELECT a.ID, b.TERM, a.ACTIVE, a.EFFECTIVE_TIME, a.SYSTEM_CREATED_DATE
FROM GLOBAL_TEMP.CONCEPT_SNAP a
JOIN GLOBAL_TEMP.DESCRIPTION_TYPE b
on a.ID=b.CONCEPT_ID
WHERE b.TYPE='F'

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.sct_concepts_prefterm; 
CREATE TABLE $db_output.SCT_Concepts_PrefTerm(
 
ID string,
TERM string,
ACTIVE string,
EFFECTIVE_TIME date,
SYSTEM_CREATED_DATE date
) USING DELTA;
 
INSERT INTO $db_output.SCT_Concepts_PrefTerm
SELECT a.ID, b.TERM, a.ACTIVE, a.EFFECTIVE_TIME, a.SYSTEM_CREATED_DATE
FROM GLOBAL_TEMP.CONCEPT_SNAP a
JOIN GLOBAL_TEMP.DESCRIPTION_TYPE b
on a.ID=b.CONCEPT_ID
WHERE b.TYPE='P'