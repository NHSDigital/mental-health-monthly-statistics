# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.spells;
 CREATE TABLE IF NOT EXISTS $db_output.spells 
 (
 Person_ID              STRING, 
 UniqHospProvSpellID    STRING,
 OrgIDProv              STRING,
 Provider_Name          STRING,
 StartDateHospProvSpell DATE,
 DischDateHospProvSpell DATE,
 HOSP_LOS               INT,
 UniqWardStayID         STRING,
 MHAdmittedPatientClass STRING,
 StartDateWardStay      DATE,
 EndDateWardStay        DATE,
 WARD_LOS               INT,
 AgeRepPeriodEnd        INT,
 CCG_Code               STRING,
 CCG_Name               STRING,
 Region_Code            STRING,
 Region_Name            STRING,
 STP_Code               STRING,
 STP_Name               STRING,
 Acute_Bed              STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.distinct_spells;
 CREATE TABLE IF NOT EXISTS $db_output.distinct_spells 
 (
 Person_ID              STRING, 
 UniqHospProvSpellID    STRING,
 OrgIDProv              STRING,
 Provider_Name          STRING,
 StartDateHospProvSpell DATE,
 DischDateHospProvSpell DATE,
 HOSP_LOS               INT,
 AgeRepPeriodEnd        INT,
 CCG_Code               STRING,
 CCG_Name               STRING,
 Region_Code            STRING,
 Region_Name            STRING,
 STP_Code               STRING,
 STP_Name               STRING
 ) USING DELTA