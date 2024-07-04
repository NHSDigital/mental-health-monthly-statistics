# Databricks notebook source
# DBTITLE 1,CREATE TABLE MHS001MPI_latest_month_data
 %sql
 
 DROP TABLE IF EXISTS $db_output.mhs001mpi_latest_month_data;
 
 -- NB just changing the name of a field here will not work as a v5 change: DMS001-1118
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
   MPSConfidence         STRUCT<DateOfBirthScorePercentage: DECIMAL(5,2), 
                                 FamilyNameScorePercentage: DECIMAL(5,2),
                                 GenderScorePercentage: DECIMAL(5,2),
                                 GivenNameScorePercentage: DECIMAL(5,2),
                                 MatchedAlgorithmIndicator: STRING,
                                 MatchedConfidencePercentage: DECIMAL(5,2),
                                 PostcodeScorePercentage: DECIMAL(5,2)>, 
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