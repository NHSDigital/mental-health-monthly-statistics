# Databricks notebook source
# DBTITLE 1,AWT
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.AWT_breakdown_values (breakdown string) USING DELTA;
 
 -- DROP TABLE IF EXISTS $db_output.awt_level_values;
 CREATE TABLE IF NOT EXISTS $db_output.awt_level_values (level string, level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA;
 
 -- DROP TABLE IF EXISTS $db_output.AWT_metric_values;
 CREATE TABLE IF NOT EXISTS $db_output.AWT_metric_values (metric string, metric_name string) USING DELTA;

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.awt_unformatted
 (
   REPORTING_PERIOD_START DATE,
   REPORTING_PERIOD_END DATE,
   STATUS STRING,
   BREAKDOWN STRING,
   LEVEL STRING,
   LEVEL_DESCRIPTION STRING,
   METRIC STRING,
   METRIC_VALUE FLOAT,
   SOURCE_DB string,
   SECONDARY_LEVEL string,
   SECONDARY_LEVEL_DESCRIPTION string
 )
 USING DELTA
 PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.EIP01_common
     (AGE_GROUP                           STRING,
      UniqServReqID                       STRING,
      IC_Rec_CCG                          STRING,
      NHSDEthnicity                       STRING,
      CLOCK_STOP                          DATE)
 USING delta 
 PARTITIONED BY (AGE_GROUP)   

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db_output.eip23a_common 
 (AGE_GROUP                                STRING,
  UniqServReqID                            STRING,
  IC_Rec_CCG                               STRING,
  NHSDEthnicity                            STRING,
  days_between_ReferralRequestReceivedDate INT)
 USING delta 
 PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db_output.eip23a_common_prov 
  (AGE_GROUP                                 STRING,
   UniqServReqID                             STRING,
   OrgIDPRov                                 STRING,
   NHSDEthnicity                             STRING,
   days_between_ReferralRequestReceivedDate  INT)
 USING delta 
 PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.EIP23d_common
       (AGE_GROUP                                        STRING,
        UniqServReqID                                    STRING,
        IC_Rec_CCG                                       STRING,
        NHSDEthnicity                                    STRING,
        days_between_endate_ReferralRequestReceivedDate  INT)
 USING delta 
 PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db_output.EIP63_common
     (UniqServReqID                                     STRING,
      OrgIDProv                                         STRING,
      IC_Rec_CCG                                        STRING,
      NHSDEthnicity                                     STRING,
      AGE_GROUP                                         STRING)
 USING delta 
 PARTITIONED BY (AGE_GROUP)

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.EIP01_common_prov
     (AGE_GROUP                                                   STRING,
      UniqServReqID                                               STRING,
      OrgIDProv                                                   STRING,
      NHSDEthnicity                                               STRING,
      CLOCK_STOP                                                  DATE)
 USING delta 
 PARTITIONED BY (AGE_GROUP)  

# COMMAND ----------

 %sql
 
 CREATE TABLE IF NOT EXISTS $db_output.eip64abc_common 
  (AGE_GROUP                                 STRING,
   NHSDEthnicity                             STRING,
   OrgIDProv                                 STRING,
   UniqServReqID                             STRING,
   IC_Rec_CCG                                STRING,
   ReferralRequestReceivedDate               DATE,
   CLOCK_STOP                                DATE)
 USING delta 
 PARTITIONED BY (AGE_GROUP)   

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db_output.EIP32_ED32_common
     (UniqServReqID                                        STRING,
      OrgIDProv                                            STRING,
      IC_Rec_CCG                                           STRING,
      PrimReasonReferralMH                                 STRING,
      AgeServReferRecDate                                  BIGINT)
 USING delta 
 PARTITIONED BY (OrgIDProv)  

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db_output.EIP23d_common_Prov 
      (AGE_GROUP                                          STRING,
       UniqServReqID                                      STRING,
       OrgIDProv                                          STRING,
       NHSDEthnicity                                      STRING,
       days_between_endate_ReferralRequestReceivedDate    INT)
 USING delta 
 PARTITIONED BY (AGE_GROUP)

# COMMAND ----------

 %sql
 --drop table $db_output.eip_nice_snomed
 create table if not exists $db_output.EIP_Nice_Snomed
 (
 CONCEPTID STRING,
 TERM STRING)
 using DELTA

# COMMAND ----------

 %sql
 CREATE TABLE if not exists $db_output.SCT_Concepts
 (
 DSS_KEY int,
 ID string,
 EFFECTIVE_TIME date,
 ACTIVE string,
 MODULE_ID string,
 DEFINITION_STATUS_ID string,
 SYSTEM_CREATED_DATE date
 )
 USING delta 

# COMMAND ----------

 %sql
 CREATE TABLE if not existS $db_output.SCT_Descriptions(
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
 )
 USING delta 

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS $db_output.SCT_Language(
 DSS_KEY int,
 ID string,
 EFFECTIVE_TIME date,
 ACTIVE string,
 MODULE_ID string,
 REFSET_ID string,
 REFERENCED_COMPONENT_ID string,
 ACCEPTABILITY_ID string, 
 SYSTEM_CREATED_DATE date
 )
 USING delta 

# COMMAND ----------

 %sql 
 CREATE TABLE if not exists  $db_output.SCT_Concepts_FSN(
  
 ID string,
 TERM string,
 ACTIVE string,
 EFFECTIVE_TIME date,
 SYSTEM_CREATED_DATE date
 )
 USING delta 

# COMMAND ----------

 %sql
 CREATE TABLE if not exists $db_output.SCT_Concepts_PrefTerm(
  
 ID string,
 TERM string,
 ACTIVE string,
 EFFECTIVE_TIME date,
 SYSTEM_CREATED_DATE date
 )
 USING delta 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Referrals;
 CREATE TABLE if not exists $db_output.EIP_Referrals(
 MHS101UNIQID bigint,
 UNIQMONTHID bigint,
 ORGIDPROV string,
 PERSON_ID string,
 RECORDNUMBER bigint,
 UNIQSERVREQID string,
 REFERRALREQUESTRECEIVEDDATE date,
 SERVDISCHDATE date,
 REFERCLOSREASON string,
 REFERREJECTIONDATE date,
 REFERREJECTREASON string,
 UNIQCAREPROFTEAMID string,
 SERVTEAMTYPEREFTOMH string,
 PRIMREASONREFERRALMH string,
 IC_RESIDENCE string
 
 )
 USING delta 

# COMMAND ----------

 %sql
 -- This is intermediate table which gets truncated during the job run, and Spark 3 giving exception when ORGIDCOMM as date type. As it is intermediate table, dropping it to update the schema, ORGIDCOMM to string type
 
 DROP TABLE IF EXISTS $db_output.eip_pre_proc_activity;
 CREATE TABLE if not exists $db_output.EIP_Pre_Proc_Activity(
 
 DER_ACTIVITYTYPE string,
 DER_ACTIVITYUNIQID bigint,
 PERSON_ID string,
 UNIQMONTHID string,
 ORGIDPROV string,
 RECORDNUMBER string,
 UNIQSERVREQID string,
 ORGIDCOMM string,
 DER_CONTACTDATE timestamp,
 DER_CONTACTTIME timestamp,
 DER_CONTACTDATETIME string,
 ADMINCATCODE string,
 SPECIALISEDMHSERVICECODE bigint,
 DER_CONTACTDURATION string,
 CONSTYPE string,
 CARECONTSUBJ string,
 CONSMECHANISMMH string,
 ACTLOCTYPECODE string,
 SITEIDOFTREAT string,
 --GROUPTHERAPYIND string,
 AttendStatus string,
 EARLIESTREASONOFFERDATE string,
 EARLIESTCLINAPPDATE string,
 CARECONTCANCELDATE string,
 CARECONTCANCELREAS string,
 --REPAPPTOFFERDATE string,
 --REPAPPTBOOKDATE string,
 UNIQCARECONTID string,
 AGECARECONTDATE string,
 CONTLOCDISTANCEHOME string,
 TIMEREFERANDCARECONTACT string,
 DER_UNIQCAREPROFTEAMID string,
 PLACEOFSAFETYIND string,
 DER_PERSONID string,
 DER_CONTACTORDER string,
 DER_FYCONTACTORDER string,
 DER_DIRECTCONTACTORDER string,
 DER_FYDIRECTCONTACTORDER string,
 DER_FACETOFACECONTACTORDER string,
 DER_FYFACETOFACECONTACTORDER STRING
 )
 USING delta 

# COMMAND ----------

 %sql
 CREATE TABLE if not exists $db_output.EIP_Activity(
 
 DER_ACTIVITYTYPE string,
 DER_ACTIVITYUNIQID bigint,
 PERSON_ID string,
 DER_PERSONID string,
 UNIQMONTHID bigint,
 ORGIDPROV string,
 RECORDNUMBER bigint,
 UNIQSERVREQID string,
 DER_CONTACTDATE date,
 DER_CONTACTTIME timestamp,
 DER_CONTACTDATETIME timestamp,
 DER_CONTACTORDER int
 )
 USING delta 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Pre_Proc_Interventions;
 CREATE TABLE if not exists $db_output.EIP_Pre_Proc_Interventions(
 
 RECORDNUMBER bigint,
 PERSON_ID string,
 UNIQMONTHID bigint,
 UNIQSERVREQID string,
 UNIQCARECONTID string,
 DER_CONTACTDATE date,
 UNIQCAREACTID string,
 DER_INTERVENTIONUNIQID bigint,
 Procedure string,
 DER_SNOMEDPROCCODE string,
 Observation string
 
 )
 USING delta 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Snomed;
 CREATE TABLE if not exists  $db_output.EIP_Snomed(
 
 PERSON_ID string,
 UNIQSERVREQID string,
 UNIQMONTHID bigint,
 RECORDNUMBER bigint,
 DER_INTERVENTIONUNIQID bigint,
 Procedure string,
 DER_SNOMEDPROCCODE string,
 Observation string,
 DER_SNOMEDTERM string,
 INTERVENTION_TYPE string
 )
 USING delta 

# COMMAND ----------

 %sql
 CREATE TABLE if not exists  $db_output.EIP_Snomed_Agg(
 UNIQMONTHID bigint,
 ORGIDPROV string,
 RECORDNUMBER bigint,
 UNIQSERVREQID string,
 ANYSNOMED bigint,
 NICESNOMED bigint)
 USING delta 

# COMMAND ----------

 %sql
 CREATE TABLE if not exists  $db_output.EIP_Activity_Agg(
 PERSON_ID string,
 UNIQMONTHID bigint,
 RECORDNUMBER bigint,
 UNIQSERVREQID string,
 DER_INMONTHCONTACTS bigint)
 USING delta 

# COMMAND ----------

 
 %sql
 CREATE TABLE if not exists  $db_output.EIP_Activity_Cumulative(
 PERSON_ID string,
 UNIQMONTHID bigint,
 RECORDNUMBER bigint,
 UNIQSERVREQID string,
 DER_CUMULATIVECONTACTS bigint)
 USING delta 