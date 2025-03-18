# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output
$mhsds_db = dbutils.widgets.get("$mhsds_db")
print($mhsds_db)
assert $mhsds_db

# COMMAND ----------

 %md

 # tables created in this notebook

 - main_monthly_unformatted_new
 - all_products_cached
 - third_level_products_cached
 - All_products_formatted
 - third_level_products_formatted


   - Main_monthly_breakdown_values
   - Main_monthly_level_values_1
   - Main_monthly_metric_values
   
 - Provider_list
 - CCG
 - RD_ORG_DAILY_LATEST
 - rd_ccg_latest
 - STP_Region_mapping_post_2020

 - MHS001_CCG_LATEST
 - MHS001MPI_latest_month_data
 - MHS101Referral_service_area
 - MHS101Referral_open_end_rp

 - Accommodation_latest
 - Employment_latest

 - Ref_GenderCodes
 - Ref_EthnicGroup
 - Ref_AgeBand
 - Ref_IMD_Decile
 - population_breakdowns

# COMMAND ----------

# DBTITLE 1,main_monthly_unformatted
 %sql
 --DROP TABLE IF EXISTS $db_output.main_monthly_unformatted_new;
 CREATE TABLE IF NOT EXISTS $db_output.main_monthly_unformatted_new
 (
     MONTH_ID int, 
     STATUS STRING,
     REPORTING_PERIOD_START DATE,
     REPORTING_PERIOD_END DATE,
     BREAKDOWN string,
     PRIMARY_LEVEL string,
     PRIMARY_LEVEL_DESCRIPTION string,
     SECONDARY_LEVEL string,
     SECONDARY_LEVEL_DESCRIPTION string,
     METRIC string,
     METRIC_VALUE float,
     SOURCE_DB string,
     PRODUCT_NO INT
 )
 USING DELTA
 PARTITIONED BY (MONTH_ID, STATUS)

# COMMAND ----------

# DBTITLE 1,all_products_cached
 %sql
 -- DROP TABLE IF EXISTS $db_output.all_products_cached;

 CREATE TABLE IF NOT EXISTS $db_output.all_products_cached (
    MONTH_ID INT, 
    STATUS STRING,
    PRODUCT_NO INT, 
    REPORTING_PERIOD_START DATE, 
    REPORTING_PERIOD_END DATE, 
    BREAKDOWN STRING, 
    PRIMARY_LEVEL STRING, 
    PRIMARY_LEVEL_DESCRIPTION STRING, 
    SECONDARY_LEVEL STRING, 
    SECONDARY_LEVEL_DESCRIPTION STRING, 
    METRIC STRING, 
    METRIC_NAME STRING, 
    METRIC_VALUE STRING,
    SOURCE_DB string
  )
  USING DELTA
  PARTITIONED BY (MONTH_ID, STATUS)

# COMMAND ----------

# DBTITLE 1,third_level_products_cached
 %sql

 -- DROP TABLE IF EXISTS $db_output.third_level_products_cached;
 CREATE TABLE IF NOT EXISTS $db_output.third_level_products_cached (
     MONTH_ID INT, 
     STATUS STRING,
     PRODUCT_NO STRING,
     REPORTING_PERIOD_START DATE,
     REPORTING_PERIOD_END DATE,
     BREAKDOWN STRING,
     PRIMARY_LEVEL STRING,
     PRIMARY_LEVEL_DESCRIPTION STRING,
     SECONDARY_LEVEL STRING,
     SECONDARY_LEVEL_DESCRIPTION STRING,
     THIRD_LEVEL STRING,
     METRIC STRING, 
     METRIC_NAME STRING, 
     METRIC_VALUE STRING,
     SOURCE_DB STRING
 )
 USING DELTA
 PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

# COMMAND ----------

# DBTITLE 1,All_products_formatted
 %sql
 -- DROP TABLE IF EXISTS $db_output.all_products_formatted;
 CREATE TABLE IF NOT EXISTS $db_output.All_products_formatted (
   MONTH_ID INT,
   PRODUCT_NO STRING,
   REPORTING_PERIOD_START DATE,
   REPORTING_PERIOD_END DATE,
   STATUS STRING,
   BREAKDOWN STRING,
   PRIMARY_LEVEL STRING,
   PRIMARY_LEVEL_DESCRIPTION STRING,
   SECONDARY_LEVEL STRING,
   SECONDARY_LEVEL_DESCRIPTION STRING,
   MEASURE_ID STRING,
   MEASURE_NAME STRING,
   MEASURE_VALUE STRING,
   DATETIME_INSERTED TIMESTAMP,
   SOURCE_DB string
 )
 USING DELTA
 PARTITIONED BY (MONTH_ID, STATUS)

# COMMAND ----------

# DBTITLE 1,third_level_products_formatted
 %sql

 -- DROP TABLE IF EXISTS $db_output.third_level_products_formatted;
 CREATE TABLE IF NOT EXISTS $db_output.third_level_products_formatted (
     MONTH_ID INT, 
     PRODUCT_NO STRING,
     REPORTING_PERIOD_START DATE,
     REPORTING_PERIOD_END DATE,
     STATUS STRING,
     BREAKDOWN STRING,
     PRIMARY_LEVEL STRING,
     PRIMARY_LEVEL_DESCRIPTION STRING,
     SECONDARY_LEVEL STRING,
     SECONDARY_LEVEL_DESCRIPTION STRING,
     THIRD_LEVEL STRING,
     MEASURE_ID STRING,
     MEASURE_NAME STRING,
     MEASURE_VALUE STRING,
     DATETIME_INSERTED TIMESTAMP,
     SOURCE_DB STRING
 )
 USING DELTA
 PARTITIONED BY (MONTH_ID, STATUS)

# COMMAND ----------

# DBTITLE 1,1. Main monthly
 %sql
 -- DROP TABLE IF EXISTS $db_output.main_monthly_breakdown_values;
 CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_breakdown_values (breakdown string) USING DELTA;

 -- DROP TABLE IF EXISTS $db_output.main_monthly_level_values_1;
 CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_level_values_1 (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA; --See above.

 -- DROP TABLE IF EXISTS $db_output.main_monthly_metric_values;
 CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_metric_values (metric string, metric_name string) USING DELTA;

# COMMAND ----------

# DBTITLE 1,Provider_list
 %sql

 -- DROP TABLE IF EXISTS $db_output.provider_list; 

 CREATE TABLE IF NOT EXISTS $db_output.Provider_list
  (ORG_CODE          STRING,
   NAME              STRING)
 USING delta

# COMMAND ----------

 %sql

 -- DROP TABLE IF EXISTS $db_output.provider_list; 

 CREATE TABLE IF NOT EXISTS $db_output.providers_between_rp_start_end_dates
  (ORG_CODE          STRING,
   NAME              STRING)
 USING delta

# COMMAND ----------

 %sql

 -- DROP TABLE IF EXISTS $db_output.provider_list; 

 CREATE TABLE IF NOT EXISTS $db_output.providers_between_rp_start_end_dates_12m
  (ORG_CODE          STRING,
   NAME              STRING)
 USING delta

# COMMAND ----------

# DBTITLE 1,CCG
 %sql
 -- drop table IF EXISTS $db_output.ccg;
 CREATE TABLE IF NOT EXISTS 
 $db_output.CCG 
 (Person_ID       STRING,
  IC_Rec_CCG      STRING,
  NAME            STRING)
 USING delta

# COMMAND ----------

# DBTITLE 1,RD_ORG_DAILY_LATEST
 %sql
 -- drop table IF EXISTS $db_output.rd_org_daily_latest;
 CREATE TABLE IF NOT EXISTS 
 $db_output.RD_ORG_DAILY_LATEST 
 (ORG_CODE      STRING,
  NAME          STRING)
 USING delta

# COMMAND ----------

# DBTITLE 1,ed_ccg_latest
 %sql
 -- drop table IF EXISTS $db_output.ed_ccg_latest;

 -- drop table IF EXISTS $db_output.rd_ccg_latest;

 -- these tables were duplicates with different names - this is just a clear out - new table created below

# COMMAND ----------

# DBTITLE 1,rd_ccg_latest
 %sql

 CREATE TABLE IF NOT EXISTS 
 $db_output.rd_ccg_latest 
 (original_ORG_CODE STRING,
  original_NAME     STRING,
  ORG_CODE          STRING,
  NAME              STRING)
 USING delta

# COMMAND ----------

# DBTITLE 1,STP/Region breakdowns April 2020
 %sql

 -- DROP TABLE IF EXISTS $db_output.stp_region_mapping_post_2020;

 CREATE TABLE IF NOT EXISTS $db_output.STP_Region_mapping_post_2020 (STP_code string, STP_description string, CCG_code string, CCG_description string, Region_code string, Region_description string) USING DELTA;

# COMMAND ----------

# DBTITLE 1,MHS001_CCG_LATEST 
 %sql

 --DROP TABLE IF EXISTS $db_output.mhs001_ccg_latest;

 CREATE TABLE IF NOT EXISTS $db_output.MHS001_CCG_LATEST 
         (Person_ID            STRING,
          IC_Rec_CCG           STRING)
 USING delta 

# COMMAND ----------

 %sql

 --DROP TABLE IF EXISTS $db_output.mhs001_ccg_latest_12m;

 CREATE TABLE IF NOT EXISTS $db_output.MHS001_CCG_LATEST_12m
         (Person_ID            STRING,
          IC_Rec_CCG           STRING)
 USING delta 

# COMMAND ----------

# DBTITLE 1,MHS001MPI_latest_month_data
 %sql

 -- DROP TABLE IF EXISTS $db_output.mhs001mpi_latest_month_data;

 -- NB just changing the name of a field here will not work as a v5 change
 -- also need to ensure the table is dropped to force the change to happen
 -- DROP TABLE command reinstated - this is ok in this case because this prep table gets truncated and replaced each run anyway.  THIS IS NOT THE ANSWER for a persisted DATA table.

 CREATE TABLE IF NOT EXISTS $db_output.MHS001MPI_latest_month_data
  (AgeDeath              BIGINT,
   AgeRepPeriodEnd       BIGINT,
   AgeRepPeriodStart     BIGINT,
   CCGGPRes              STRING,
   County                STRING,
   DefaultPostcode       STRING,
   ElectoralWard         STRING,
   EthnicCategory        STRING,
   Gender                STRING,
   IMDQuart              STRING,
   LADistrictAuth        STRING,
   LDAFlag               BOOLEAN,
   LSOA                  STRING,
   LSOA2011              STRING,
   LanguageCodePreferred STRING,
   LocalPatientId        STRING,
   MHS001UniqID          BIGINT,
   MaritalStatus         STRING,
   NHSDEthnicity         STRING,
   NHSNumber             STRING,
   NHSNumberStatus       STRING,
   OrgIDCCGRes           STRING,
   OrgIDEduEstab         STRING,
   OrgIDLocalPatientId   STRING,
   OrgIDProv             STRING,
   PatMRecInRP           BOOLEAN,
   Person_ID             STRING,
   PostcodeDistrict      STRING,
   RecordEndDate         DATE,
   RecordNumber          BIGINT,
   RecordStartDate       DATE,
   RowNumber             BIGINT,
   UniqMonthID           BIGINT,
   UniqSubmissionID      BIGINT,
   IC_Rec_CCG            STRING,
   NAME                  STRING)
 USING delta
 PARTITIONED BY (UniqMonthID)



# COMMAND ----------

# DBTITLE 1,MHS101Referral_service_area
 %sql

 DROP TABLE IF EXISTS $db_output.mhs101referral_service_area ;

 CREATE TABLE IF NOT EXISTS $db_output.MHS101Referral_service_area 
      (  
       AgeServReferDischDate                                              bigint, 
       AgeServReferRecDate                                                bigint, 
 --      CYPServiceRefEndRP                                                 BOOLEAN,  
 --      CYPServiceRefStartRP                                               BOOLEAN,  
       ClinRespPriorityType                                               string,  
       CountAttendedCareContactsInFinancialYearPersonWas017AtTimeOfContact bigint,  
       CountOfAttendedCareContacts                                        bigint,   
       CountOfAttendedCareContactsInFinancialYear                         bigint,  
       DecisionToTreatDate                                                date,   
       DecisionToTreatTime                                                timestamp,   
 --      DischLetterIssDate                                                 date,   
       DischPlanCreationDate                                              date,   
       DischPlanCreationTime                                              timestamp,   
       DischPlanLastUpdatedDate                                           date,   
       DischPlanLastUpdatedTime                                           timestamp,   
       FirstAttendedContactInFinancialYearDate                            date,   
       FirstAttendedContactInRPdate                                       date,   
       FirstContactEverDate                                               date,   
       FirstContactEverWhereAgeAtContactUnder18Date                       date,   
       InactTimeRef                                                       date,   
 --      LDAServiceRefEndRP                                                 BOOLEAN,   
       LocalPatientId                                                     string,   
       MHS101UniqID                                                       bigint,   
       NHSServAgreeLineID                                                 string,   
       OrgIDComm                                                          string,   
       OrgIDProv                                                          string,   
       OrgIDReferringOrg                                                  string,   
       Person_ID                                                          string,   
       PrimReasonReferralMH                                               string,   
       ReasonOAT                                                          string,   
       RecordEnddate                                                      date,   
       RecordNumber                                                       bigint,   
       RecordStartdate                                                    date,   
       ReferralRequestReceiveddate                                        date,   
       ReferralRequestReceivedTime                                        timestamp,            
 --      ReferralServiceAreasOpenEndRPLDA                                   boolean,         
 --      ReferralServiceAreasStartingInRPLDA                                boolean,     
       ReferringCareProfessionalType                                      string,   
       RowNumber                                                          bigint,   
       SecondAttendedContactEverDate                                      date,   
       SecondAttendedContactInFinancialYearDate                           date,   
       SecondContactEverWhereAgeAtContactUnder18Date                      date,   
       ServDischDate                                                      date,   
       ServDischTime                                                      timestamp,   
       ServiceRequestId                                                   string,   
       SourceOfReferralMH                                                 string,   
       SpecialisedMHServiceCode                                           string,   
       UniqMonthID                                                        bigint,   
       UniqServReqID                                                      string,   
       UniqSubmissionID                                                   bigint,   
       CAMHS                                                              string,   
       LD                                                                 string,   
       MH                                                                 string)
 USING delta 
 PARTITIONED BY (UniqMonthID)


# COMMAND ----------

# DBTITLE 1,MHS101Referral_open_end_rp
 %sql
 DROP TABLE IF EXISTS $db_output.mhs101referral_open_end_rp;

 CREATE TABLE IF NOT EXISTS $db_output.MHS101Referral_open_end_rp 
  (
   AgeServReferDischDate                        BIGINT,
   AgeServReferRecDate                          BIGINT,
 --  CYPServiceRefEndRP                           BOOLEAN,
 --  CYPServiceRefStartRP                         BOOLEAN,
   ClinRespPriorityType                         STRING,
   DischPlanCreationDate                        DATE,
   DischPlanCreationTime                        TIMESTAMP,
   DischPlanLastUpdatedDate                     DATE,
   DischPlanLastUpdatedTime                     TIMESTAMP,
   InactTimeRef                                 DATE,
 --  LDAServiceRefEndRP                           BOOLEAN,
   LocalPatientId                               STRING,
   MHS101UniqID                                 BIGINT,
   NHSServAgreeLineID                           STRING,
   OrgIDComm                                    STRING,
   OrgIDProv                                    STRING,
   OrgIDReferringOrg                            STRING,
   Person_ID                                    STRING,
   PrimReasonReferralMH                         STRING,
   ReasonOAT                                    STRING,
   RecordEndDate                                DATE,
   RecordNumber                                 BIGINT,
   RecordStartDate                              DATE,
   ReferralRequestReceivedDate                  DATE,
   ReferralRequestReceivedTime                  TIMESTAMP,
   ReferringCareProfessionalType                STRING,
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

# DBTITLE 1,Accommodation_latest
 %sql


 DROP TABLE IF EXISTS $db_output.accommodation_latest;

 CREATE TABLE IF NOT EXISTS $db_output.accommodation_latest 
      (AccommodationType                                          string, 
       AccommodationTypeDate                                      date,     
       AccommodationTypeEndDate                                   date,     
       AccommodationTypeStartDate                                 date,     
       AgeAccomTypeDate                                           bigint,  
       LocalPatientId                                             string,   
       MHS003UniqID                                               bigint,   
       OrgIDProv                                                  string,   
       Person_ID                                                  string,   
       RecordNumber                                               bigint,   
       RowNumber                                                  bigint,   
       SCHPlacementType                                           string,   
       SettledAccommodationInd                                    string,   
       UniqMonthID                                                bigint,   
       UniqSubmissionID                                           bigint,   
       RANK                                                       int,      
       PROV_RANK                                                  int)
 USING delta 
 PARTITIONED BY (UniqMonthID)


# COMMAND ----------

# DBTITLE 1,Employment_Latest
 %sql
 DROP TABLE IF EXISTS $db_output.employment_latest;

 CREATE TABLE IF NOT EXISTS $db_output.employment_latest 
      (
       EmployStatus                                          string,   
       EmployStatusEndDate                                   date,     
       EmployStatusRecDate                                   date,     
       EmployStatusStartDate                                 date,     
       LocalPatientId                                        string,   
       MHS004UniqID                                          bigint,   
       OrgIDProv                                             string,  
       patprimempconttypemh                                  string,
       Person_ID                                             string,   
       RecordNumber                                          bigint,   
       RowNumber                                             bigint,   
       UniqMonthID                                           bigint,   
       UniqSubmissionID                                      bigint,   
       WeekHoursWorked                                       string,   
       RANK                                                  int,      
       PROV_RANK                                             int )
 USING delta 
 PARTITIONED BY (UniqMonthID)

# COMMAND ----------

# DBTITLE 1,Audit table to log the monthly runs
 %sql

 CREATE TABLE IF NOT EXISTS $db_output.audit_menh_publications (
   MONTH_ID int,
   STATUS string,
   REPORTING_PERIOD_START date,
   REPORTING_PERIOD_END date,
   SOURCE_DB string,
   RUN_START timestamp,
   RUN_END timestamp,
   ADHOC_DESC string
 ) USING DELTA PARTITIONED BY (MONTH_ID, STATUS)


# COMMAND ----------

# DBTITLE 1,Gender codes reference table (new methodology)
 %sql
 DROP TABLE IF EXISTS $db_output.Ref_GenderCodes;
 CREATE TABLE IF NOT EXISTS $db_output.Ref_GenderCodes (
   PRIMARYCODE string,
   DESCRIPTION string
 )

# COMMAND ----------

# DBTITLE 1,Ethnic group reference table
 %sql
 DROP TABLE IF EXISTS $db_output.Ref_EthnicGroup;
 CREATE TABLE IF NOT EXISTS $db_output.Ref_EthnicGroup (
   ETHNICGROUP string
 )

# COMMAND ----------

# DBTITLE 1,Age Band reference table
 %sql
 DROP TABLE IF EXISTS $db_output.Ref_AgeBand;
 CREATE TABLE IF NOT EXISTS $db_output.Ref_AgeBand (
   AGEBAND string
 )

# COMMAND ----------

# DBTITLE 1,IMD Decile reference table
 %sql
 DROP TABLE IF EXISTS $db_output.Ref_IMD_Decile;
 CREATE TABLE IF NOT EXISTS $db_output.Ref_IMD_Decile (
   IMDDECILE string
 )

# COMMAND ----------

# DBTITLE 1,Population Breakdowns
 %sql
 DROP TABLE IF EXISTS $db_output.population_breakdowns;
 CREATE TABLE IF NOT EXISTS $db_output.population_breakdowns (
   POPULATION_ID string,
   BREAKDOWN string,
   SOURCE_TABLE string,
   PRIMARY_LEVEL string,
   PRIMARY_LEVEL_DESC string,
   SECONDARY_LEVEL string,
   SECONDARY_LEVEL_DESC string,
   METRIC_VALUE long
 )