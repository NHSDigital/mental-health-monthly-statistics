# Databricks notebook source
# Code to create materialised tables to speed up MH processing
# Jonathan Bliss JIRA DSP-8958


# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.rd_ccg_latest;
 
 CREATE TABLE IF NOT EXISTS 
 $db_output.rd_ccg_latest 
 (original_ORG_CODE STRING,
  original_NAME     STRING,
  ORG_CODE        STRING,
  NAME            STRING)
 USING delta

# COMMAND ----------

 %sql
 
 -- DROP TABLE IF EXISTS $db_output.Provider_list; 
 
 CREATE TABLE IF NOT EXISTS $db_output.Provider_list
  (ORG_CODE          STRING,
   NAME              STRING)
 USING delta

# COMMAND ----------

 %sql
 
 -- DROP TABLE IF EXISTS $db_output.BED_DAYS_IN_RP;
 
 CREATE TABLE IF NOT EXISTS $db_output.BED_DAYS_IN_RP
     (LEVEL                           STRING,
      IC_Rec_CCG                      STRING,
      NAME                            STRING,
      AgeRepPeriodEnd                 BIGINT,
      WardType                        STRING,
      StartDateWardStay               DATE,
      EndDateWardStay                 DATE,
      METRIC_VALUE                    BIGINT)
 USING delta 
 PARTITIONED BY (IC_Rec_CCG)

# COMMAND ----------

 %sql
 
 -- DROP TABLE IF EXISTS $db_output.MHS701CPACareEpisode_latest;
 
 CREATE TABLE IF NOT EXISTS $db_output.MHS701CPACareEpisode_latest
     (CPAEpisodeId                                                STRING,
      EndDateCPA                                                  DATE,
      LocalPatientId                                              STRING,
      MHS701UniqID                                                BIGINT,
      OrgIDProv                                                   STRING,
      Person_ID                                                   STRING,
      RecordEndDate                                               DATE,
      RecordNumber                                                BIGINT,
      RecordStartDate                                             DATE,
      RowNumber                                                   BIGINT,
      StartDateCPA                                                DATE,
      UniqCPAEpisodeID                                            STRING,
      UniqMonthID                                                 BIGINT,
      UniqSubmissionID                                            BIGINT)
 USING delta 
 PARTITIONED BY (UniqMonthID)    

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH03_prep;
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH03_prep
     (Person_ID                                  STRING,
      GENDER                                     STRING,
      IC_Rec_CCG                                 STRING,
      NAME                                       STRING,
      CASSR                                      STRING,
      CASSR_description                          STRING)
 USING delta 

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH03_prep_prov;
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH03_prep_prov
     (Person_ID                         STRING,
      GENDER                            STRING,
      OrgIDProv                         STRING)
 USING delta 

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.amh14_prep;
 
 -- NB just changing the name of a field here will not work as a v5 change: DMS001-1117
 -- also need to ensure the table is dropped to force the change to happen
 -- DROP TABLE command reinstated - this is ok in this case because this prep table gets truncated and replaced each run anyway.  THIS IS NOT THE ANSWER for a persisted DATA table.
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH14_prep 
     (Person_ID                                   STRING,
      GENDER                                      STRING,
      IC_Rec_CCG                                  STRING,
      NAME                                        STRING,
      CASSR                                       STRING,
      CASSR_description                           STRING,
      AccommodationTypeDate                     DATE)
 USING delta 
      

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH14_prep_prov;
 
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH14_prep_prov 
     (Person_ID                         STRING,
      GENDER                            STRING,
      OrgIDProv                         STRING)
 USING delta 

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.MHS001_CCG_LATEST;
 
 CREATE TABLE IF NOT EXISTS $db_output.MHS001_CCG_LATEST 
         (Person_ID            STRING,
          IC_Rec_CCG           STRING)
 USING delta 
  

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH17_prep;
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH17_prep
     (Person_ID                              STRING,
      GENDER                                 STRING,
      IC_Rec_CCG                             STRING,
      NAME                                   STRING,
      EmployStatusRecDate                    DATE,
      CASSR                                  STRING,
      CASSR_description                      STRING)
 USING delta 

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH17_prep_prov;
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH17_prep_prov
     (Person_ID                                   STRING,
      GENDER                                      STRING,
      OrgIDProv                                   STRING,
      EmployStatusRecDate                         DATE)
 USING delta 
  

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH03e_prep;
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH03e_prep
         (Person_ID                               STRING,
          IC_Rec_CCG                              STRING,
          NAME                                    STRING,
          Region_code                             STRING,
          Region_description                      STRING,
          STP_code                                STRING,
          STP_description                         STRING)
 USING delta 

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.amh13e_14e_prep;
 
 -- NB just changing the name of a field here will not work as a v5 change: DMS001-1117
 -- also need to ensure the table is dropped to force the change to happen
 -- DROP TABLE command reinstated - this is ok in this case because this prep table gets truncated and replaced each run anyway.  THIS IS NOT THE ANSWER for a persisted DATA table.
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH13e_14e_prep
     (Person_ID                                          STRING,
      IC_Rec_CCG                                         STRING,
      NAME                                               STRING,
      Region_code                                        STRING,
      Region_description                                 STRING,
      STP_code                                           STRING,
      STP_description                                    STRING,
      AccommodationTypeDate                            DATE,
      SettledAccommodationInd                            STRING,
      rank                                               INT)
 USING delta 
      

# COMMAND ----------

 %sql
 --remove
 -- DROP TABLE IF EXISTS $db_output.AMH16e_17e_prep;
 
 CREATE TABLE IF NOT EXISTS $db_output.AMH16e_17e_prep
      (Person_ID                                       STRING,
       IC_Rec_CCG                                      STRING,
       NAME                                            STRING,
       Region_code                                     STRING,
       Region_description                              STRING,
       STP_code                                        STRING,
       STP_description                                 STRING,
       EmployStatusRecDate                             DATE,
       EmployStatus                                    STRING,
       RANK                                            INT)
 USING delta 
       

# COMMAND ----------

 %sql
 
 --GBT: DROP TABLE is used here to DROP a column that is no longer needed - is there a better way of doing this??
 
 DROP TABLE IF EXISTS $db_output.cypfinal_2nd_contact_quarterly;
 
 CREATE TABLE IF NOT EXISTS $db_output.CYPFinal_2nd_contact_Quarterly
     (Person_ID                                          STRING,
      UniqServReqID                                      STRING,
      OrgIDProv                                          STRING,
      OrgIDComm                                          STRING,
      UniqMonthID                                        BIGINT,
      ContID                                             STRING,
      ContDate                                           DATE,
      AgeCareContDate                                    BIGINT,
      RN1                                                INT,
 --     Qtr                                                STRING,
      ContDate2                                          DATE,
      AgeCareContDate2                                   BIGINT)
 USING delta 
      

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.MHS401MHActPeriod_GRD_open_end_rp;
 
 CREATE TABLE IF NOT EXISTS $db_output.MHS401MHActPeriod_GRD_open_end_rp
     (EndDateMHActLegalStatusClass                                      DATE,
      EndTimeMHActLegalStatusClass                                      TIMESTAMP,
      ExpiryDateMHActLegalStatusClass                                   DATE,
      ExpiryTimeMHActLegalStatusClass                                   TIMESTAMP,
      InactTimeMHAPeriod                                                DATE,
      LegalStatusClassPeriodEndReason                                   STRING,
      LegalStatusClassPeriodStartReason                                 STRING,
      LegalStatusCode                                                   STRING,
      LocalPatientId                                                    STRING,
      MHActLegalStatusClassPeriodId                                     STRING,
      MHS401UniqID                                                      BIGINT,
      MentalCat                                                         STRING,
      NHSDLegalStatus                                                   STRING,
      OrgIDProv                                                         STRING,
      Person_ID                                                         STRING,
      RecordEndDate                                                     DATE,
      RecordNumber                                                      BIGINT,
      RecordStartDate                                                   DATE,
      RowNumber                                                         BIGINT,
      StartDateMHActLegalStatusClass                                    DATE,
      StartTimeMHActLegalStatusClass                                    TIMESTAMP,
      UniqMHActEpisodeID                                                STRING,
      UniqMonthID                                                       BIGINT,
      UniqSubmissionID                                                  BIGINT)
 USING delta 

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
 %sql
 --check if this can be removed
 
 -- GBT: is the table being dropped and recreated here to add the extra column 'NHSEthnicity'?  Is this the best way of doing this?  SHould we not instead be using ALTER TABLE in a later notebook??
 -- BS: This is a temporary table and this table gets truncated everytime a run happens and hence safe to drop and recreate for adding new column. However, it has to be set off next time onwards.
 
 DROP TABLE IF EXISTS $db_output.eip01_common;
 
 -- # EIP measures moved to menh_publications
 -- CREATE TABLE IF NOT EXISTS $db_output.EIP01_common
 --     (AGE_GROUP                           STRING,
 --      UniqServReqID                       STRING,
 --      IC_Rec_CCG                          STRING,
 --      NHSDEthnicity                       STRING,
 --      CLOCK_STOP                          DATE)
 -- USING delta 
 -- PARTITIONED BY (AGE_GROUP)      

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
 %sql
 
 DROP TABLE IF EXISTS $db_output.eip01_common_prov;
 
 -- # EIP measures moved to menh_publications
 -- CREATE TABLE IF NOT EXISTS $db_output.EIP01_common_prov
 --     (AGE_GROUP                                                   STRING,
 --      UniqServReqID                                               STRING,
 --      OrgIDProv                                                   STRING,
 --      NHSDEthnicity                                               STRING,
 --      CLOCK_STOP                                                  DATE)
 -- USING delta 
 -- PARTITIONED BY (AGE_GROUP)       

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
 %sql
 
 DROP TABLE IF EXISTS $db_output.eip23d_common;
 
 -- # EIP measures moved to menh_publications
 -- CREATE TABLE IF NOT EXISTS $db_output.EIP23d_common
 --       (AGE_GROUP                                        STRING,
 --        UniqServReqID                                    STRING,
 --        IC_Rec_CCG                                       STRING,
 --        NHSDEthnicity                                    STRING,
 --        days_between_endate_ReferralRequestReceivedDate  INT)
 -- USING delta 
 -- PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
 %sql
 
 DROP TABLE IF EXISTS $db_output.eip23d_common_prov;
 
 -- # EIP measures moved to menh_publications
 -- CREATE TABLE IF NOT EXISTS $db_output.EIP23d_common_Prov 
 --      (AGE_GROUP                                          STRING,
 --       UniqServReqID                                      STRING,
 --       OrgIDProv                                          STRING,
 --       NHSDEthnicity                                      STRING,
 --       days_between_endate_ReferralRequestReceivedDate    INT)
 -- USING delta 
 -- PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
 %sql
 
 DROP TABLE IF EXISTS $db_output.eip32_ed32_common;
 
 -- # EIP measures moved to menh_publications
 -- CREATE TABLE IF NOT EXISTS $db_output.EIP32_ED32_common
 --     (UniqServReqID                                        STRING,
 --      OrgIDProv                                            STRING,
 --      IC_Rec_CCG                                           STRING,
 --      PrimReasonReferralMH                                 STRING,
 --      AgeServReferRecDate                                  BIGINT)
 -- USING delta 
 -- PARTITIONED BY (OrgIDProv)        

# COMMAND ----------

# DBTITLE 1,# EIP measures moved to menh_publications
 %sql
 
 DROP TABLE IF EXISTS $db_output.eip63_common;
 
 -- # EIP measures moved to menh_publications
 -- CREATE TABLE IF NOT EXISTS $db_output.EIP63_common
 --     (UniqServReqID                                     STRING,
 --      OrgIDProv                                         STRING,
 --      IC_Rec_CCG                                        STRING,
 --      NHSDEthnicity                                     STRING,
 --      AGE_GROUP                                         STRING)
 -- USING delta 
 -- PARTITIONED BY (AGE_GROUP) 

# COMMAND ----------

# DBTITLE 1,DelayedDischDim (candidate for REF_DATA)
 %sql
 
 -- table needs to be dropped in order to change structure - adding FirstMonth & LastMonth fields
 DROP TABLE IF EXISTS $db_output.delayeddischdim;
 
 CREATE TABLE IF NOT EXISTS $db_output.DelayedDischDim(
   key STRING,
   code STRING,
   description STRING,
   FirstMonth INT,
   LastMonth INT
 ) USING DELTA