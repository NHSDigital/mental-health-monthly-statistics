# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
%sql
-- 12/Feb/2021 : This code has to be commented after first use/run
DROP TABLE IF EXISTS $db_output.eip23a_common;

-- EIP measures moved to menh_publications
-- CREATE TABLE IF NOT EXISTS $db_output.eip23a_common 
-- (AGE_GROUP                                STRING,
--  UniqServReqID                            STRING,
--  IC_Rec_CCG                               STRING,
--  NHSDEthnicity                            STRING,
--  days_between_ReferralRequestReceivedDate INT)
-- USING delta 
-- PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
%sql
-- 12/Feb/2021 : This code has to be commented after first use/run
DROP TABLE IF EXISTS $db_output.eip23a_common_prov;

-- # EIP measures moved to menh_publications
-- CREATE TABLE IF NOT EXISTS $db_output.eip23a_common_prov 
--  (AGE_GROUP                                 STRING,
--   UniqServReqID                             STRING,
--   OrgIDPRov                                 STRING,
--   NHSDEthnicity                             STRING,
--   days_between_ReferralRequestReceivedDate  INT)
-- USING delta 
-- PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
%sql
-- Uncommented below code in order to add NHSDEthnicity
DROP TABLE IF EXISTS $db_output.eip64abc_common;

-- # EIP measures moved to menh_publications
-- CREATE TABLE IF NOT EXISTS $db_output.eip64abc_common 
--  (AGE_GROUP                                 STRING,
--   NHSDEthnicity                             STRING,
--   OrgIDProv                                 STRING,
--   UniqServReqID                             STRING,
--   IC_Rec_CCG                                STRING,
--   ReferralRequestReceivedDate               DATE,
--   CLOCK_STOP                                DATE)
-- USING delta 
-- PARTITIONED BY (AGE_GROUP)   

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs09_intermediate;

CREATE TABLE IF NOT EXISTS $db_output.mhs09_intermediate 
 (Person_ID                                    STRING,
  RecordNumber                                 BIGINT,
  AMHServiceWSEndRP_temp                       STRING,
  CYPServiceWSEndRP_temp                       STRING,
  LDAServiceWSEndRP_temp                       STRING,
  AgeRepPeriodEnd                              BIGINT,
  IC_REC_CCG                                   STRING,
  NAME                                         STRING)
USING delta 
PARTITIONED BY (AgeRepPeriodEnd)  

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs09prov_intermediate;

CREATE TABLE IF NOT EXISTS $db_output.mhs09prov_intermediate 
  (Person_ID                                   STRING,
   RecordNumber                                BIGINT,
   OrgIDProv                                   STRING,
   CYPServiceWSEndRP_temp                      STRING,
   LDAServiceWSEndRP_temp                      STRING,
   AMHServiceWSEndRP_temp                      STRING,
   AMHServiceRefEndRP_temp                     BOOLEAN,
   AgeRepPeriodEnd                             BIGINT)
USING delta 
PARTITIONED BY (AgeRepPeriodEnd)  

# COMMAND ----------

%sql

DROP TABLE IF EXISTS $db_output.mhs101referral_open_end_rp;

CREATE TABLE IF NOT EXISTS $db_output.mhs101referral_open_end_rp
 (AMHServiceRefEndRP                           BOOLEAN,
  AgeServReferDischDate                        BIGINT,
  AgeServReferRecDate                          BIGINT,
  CYPServiceRefEndRP                           BOOLEAN,
  CYPServiceRefStartRP                         BOOLEAN,
  ClinRespPriorityType                         STRING,
  DischPlanCreationDate                        DATE,
  DischPlanCreationTime                        TIMESTAMP,
  DischPlanLastUpdatedDate                     DATE,
  DischPlanLastUpdatedTime                     TIMESTAMP,
  InactTimeRef                                 DATE,
  LDAServiceRefEndRP                           BOOLEAN,
  LocalPatientId                               STRING,
  MHS101UniqID                                 BIGINT,
  NHSServAgreeLineNum                          STRING,
  OrgIDComm                                    STRING,
  OrgIDProv                                    STRING,
  OrgIDReferring                               STRING,
  Person_ID                                    STRING,
  PrimReasonReferralMH                         STRING,
  ReasonOAT                                    STRING,
  RecordEndDate                                DATE,
  RecordNumber                                 BIGINT,
  RecordStartDate                              DATE,
  ReferralRequestReceivedDate                  DATE,
  ReferralRequestReceivedTime                  TIMESTAMP,
  ReferringCareProfessionalStaffGroup          STRING,
  RowNumber                                    BIGINT,
  ServDischDate                                DATE,
  ServDischTime                                TIMESTAMP,
  ServiceRequestId                             STRING,
  SourceOfReferralMH                           STRING,
  SpecialisedMHServiceCode                     STRING,
  UniqMonthID                                  BIGINT,
  UniqServReqID                                STRING,
  UniqSubmissionID                             BIGINT,
  CYPServiceRefEndRP_temp                      BOOLEAN,
  LDAServiceRefEndRP_temp                      BOOLEAN,
  AMHServiceRefEndRP_temp                      BOOLEAN)
USING delta 
PARTITIONED BY (UniqMonthID)

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs10_intermediate;

CREATE TABLE IF NOT EXISTS $db_output.mhs10_intermediate 
  (Person_ID                                   STRING,
   RecordNumber                                BIGINT,
   AMHServiceRefEndRP_temp                     BOOLEAN,
   CYPServiceRefEndRP_temp                     BOOLEAN,
   LDAServiceRefEndRP_temp                     BOOLEAN,
   AgeRepPeriodEnd                             BIGINT,
   IC_REC_CCG                                  STRING,
   NAME                                        STRING)
USING delta 
PARTITIONED BY (AgeRepPeriodEnd)  

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs10prov_intermediate;

CREATE TABLE IF NOT EXISTS $db_output.mhs10prov_intermediate 
  (Person_ID                                    STRING,
   RecordNumber                                 BIGINT,
   OrgIDProv                                    STRING,
   AMHServiceRefEndRP_temp                      BOOLEAN,
   CYPServiceRefEndRP_temp                      BOOLEAN,
   LDAServiceRefEndRP_temp                      BOOLEAN,
   AgeRepPeriodEnd                              BIGINT)
USING delta 
PARTITIONED BY (AgeRepPeriodEnd) 

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs11_intermediate;

CREATE TABLE IF NOT EXISTS $db_output.mhs11_intermediate 
  (Person_ID                                    STRING,
   RecordNumber                                 BIGINT,
   AMHServiceRefEndRP_temp                      BOOLEAN,
   CYPServiceRefEndRP_temp                      BOOLEAN,
   LDAServiceRefEndRP_temp                      BOOLEAN,
   AgeRepPeriodEnd                              BIGINT,
   IC_REC_CCG                                   STRING,
   NAME                                         STRING)
USING delta 
PARTITIONED BY (AgeRepPeriodEnd)  

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs11prov_intermediate;

CREATE TABLE IF NOT EXISTS $db_output.mhs11prov_intermediate 
   (Person_ID                                  STRING,
    RecordNumber                               BIGINT,
    OrgIDProv                                  STRING,
    AMHServiceRefEndRP_temp                    BOOLEAN,
    CYPServiceRefEndRP_temp                    BOOLEAN,
    LDAServiceRefEndRP_temp                    BOOLEAN,
    AgeRepPeriodEnd                            BIGINT)
USING delta 
PARTITIONED BY (AgeRepPeriodEnd) 

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs21_prep;

CREATE TABLE IF NOT EXISTS $db_output.mhs21_prep
  (UniqWardStayID                              STRING,
   Person_ID                                   STRING,
   IC_Rec_CCG                                  STRING,
   NAME                                        STRING,
   Bed_Type                                    INT,
   WardLocDistanceHome                         BIGINT,
   CYPServiceWSEndRP_temp                      STRING,
   AMHServiceWSEndRP_temp                      STRING,
   AGE_GROUP                                   STRING)
USING delta 
PARTITIONED BY (AGE_GROUP)  

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs21_prov_prep;

CREATE TABLE IF NOT EXISTS $db_output.mhs21_prov_prep
  (Person_ID                                   STRING,
   OrgIDProv                                   STRING,
   UniqWardStayID                              STRING,
   Bed_Type                                    INT,
   CYPServiceWSEndRP_temp                      STRING,
   AMHServiceWSEndRP_temp                      STRING,
   WardLocDistanceHome                         BIGINT,
   AGE_GROUP                                   STRING)
USING delta 
PARTITIONED BY (AGE_GROUP)  

# COMMAND ----------

%sql

-- DROP TABLE IF EXISTS $db_output.mhs803carecluster_common;

CREATE TABLE IF NOT EXISTS $db_output.mhs803carecluster_common
   (UniqClustID                                STRING,
    OrgIDProv                                  STRING,
    UniqMonthID                                BIGINT,
    MHS803UniqID                               BIGINT,
    AMHCareClustCodeFin                        STRING,
    startdatecareclust                         DATE,
    CLUSTER                                    STRING)
USING delta 
PARTITIONED BY (UniqMonthID)