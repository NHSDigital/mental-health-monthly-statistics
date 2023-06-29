# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY FUNCTION combine_mhsds_date_and_time(date_col DATE, time_col TIMESTAMP)
 RETURNS TIMESTAMP
 COMMENT 'Combines MHSDS date and time columns into a single datetime, as MHSDS time values contain 1970-01-01'
 RETURN to_timestamp(concat(date_col, 'T', substring(time_col, 12, 8)))

# COMMAND ----------

 %sql
 --This table returns rows, carry forward
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PRAC AS
  
 SELECT GP.Person_ID,
       GP.OrgIDCCGGPPractice,
       GP.OrgIDSubICBLocGP,
       GP.RecordNumber
  FROM $db_source.MHS002GP GP
       INNER JOIN 
                  (
                    SELECT Person_ID, 
                           MAX(RecordNumber) as RecordNumber
                      FROM $db_source.MHS002GP
                     WHERE UniqMonthID = '$end_month_id'
                           AND GMPCodeReg NOT IN ('V81999','V81998','V81997')
                           AND EndDateGMPRegistration is NULL
                  GROUP BY Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GP.UniqMonthID = '$end_month_id'
        AND GMPCodeReg NOT IN ('V81999','V81998','V81997')
        AND EndDateGMPRegistration is null

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PREP AS
      SELECT a.Person_ID,
             CASE
               WHEN UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice
               WHEN UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP 
               WHEN UNIQMONTHID <= 1467 then OrgIDCCGRes 
               WHEN UNIQMONTHID > 1467 then OrgIDSubICBLocResidence
               ELSE 'ERROR'
               END as IC_Rec_CCG 
        FROM $db_source.MHS001MPI a
   LEFT JOIN global_temp.CCG_PRAC c 
             ON a.Person_ID = c.Person_ID 
             AND a.RecordNumber = c.RecordNumber
       WHERE a.UniqMonthID = '$end_month_id' 
             AND a.PatMRecInRP = true

# COMMAND ----------

 %sql
 --now this table is populated using month_id as 1472
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG AS
  
 SELECT Person_ID
 ,CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS IC_Rec_CCG
 ,CASE WHEN NAME IS null THEN 'UNKNOWN' ELSE NAME END AS NAME
  
 FROM global_temp.CCG_PREP a
  
 LEFT JOIN $db_output.RD_CCG_LATEST b 
         ON a.IC_Rec_CCG = b.original_ORG_CODE

# COMMAND ----------

 %sql 
 DROP table IF EXISTS $db_output.MHS001MPI_latest_month_data; 
 CREATE TABLE IF NOT EXISTS $db_output.MHS001MPI_latest_month_data 
     (SELECT MPI.AgeDeath 
            ,MPI.AgeRepPeriodEnd 
            ,MPI.AgeRepPeriodStart 
            ,MPI.CCGGPRes 
            ,MPI.County 
            ,MPI.DefaultPostcode 
            ,MPI.ElectoralWard 
            ,MPI.EthnicCategory 
            ,MPI.Gender 
            ,MPI.IMDQuart 
            ,MPI.LADistrictAuth 
            ,MPI.LDAFlag 
            ,"" AS LSOA 
            ,MPI.LSOA2011 
            ,MPI.LanguageCodePreferred 
            ,MPI.LocalPatientId 
            ,MPI.MHS001UniqID 
            ,MPI.MPSConfidence 
            ,MPI.MaritalStatus 
            ,MPI.NHSDEthnicity 
            ,MPI.NHSNumber 
            ,MPI.NHSNumberStatus 
            ,MPI.OrgIDCCGRes 
            ,MPI.OrgIDEduEstab 
            ,MPI.OrgIDLocalPatientId 
            ,MPI.OrgIDProv 
            ,MPI.PatMRecInRP 
            ,MPI.Person_ID 
            ,MPI.PostcodeDistrict 
            ,MPI.RecordEndDate 
            ,MPI.RecordNumber 
            ,MPI.RecordStartDate 
            ,MPI.RowNumber 
            ,MPI.UniqMonthID 
            ,MPI.UniqSubmissionID
            ,CCG.IC_Rec_CCG
            ,CCG.NAME
       FROM $db_source.MHS001MPI MPI
  LEFT JOIN global_temp.CCG CCG
            ON MPI.Person_ID = CCG.Person_ID
      WHERE UniqMonthID = '$end_month_id'
            AND MPI.PatMRecInRP = true)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.contact_prep;
 CREATE TABLE IF NOT EXISTS $db_output.contact_prep AS
 SELECT
 REF.UniqMonthID
 ,REF.UniqServReqID
 ,REF.ReferralRequestReceivedDate
 ,REF.ReferralRequestReceivedTime
 ,CASE WHEN REF.ReferralRequestReceivedTime IS NULL THEN to_timestamp(REF.ReferralRequestReceivedDate)
       ELSE combine_mhsds_date_and_time(REF.ReferralRequestReceivedDate, REF.ReferralRequestReceivedTime) 
       END as ReferralRequestReceivedDateTime
 ,CON.UniqCareContID
 ,CON.AttendOrDNACode
 ,CON.ConsMechanismMH
 ,CON.CareContDate
 ,CON.CareContTime
 ,CASE WHEN CON.CareContTime IS NULL THEN to_timestamp(CON.CareContDate)
       ELSE combine_mhsds_date_and_time(CON.CareContDate, CON.CareContTime) 
       END as CareContDateTime
 FROM $db_source.MHS101Referral AS REF  
 INNER JOIN $db_source.MHS201CareContact CON
                      ON REF.UniqServReqID = CON.UniqServReqID 
                      AND CON.UniqMonthID = '$end_month_id' 
 
 WHERE CON.CareContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
 AND REF.UniqMonthID = '$end_month_id'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.attended_direct_contact_order;
 CREATE TABLE IF NOT EXISTS $db_output.attended_direct_contact_order AS
 SELECT
 CON.UniqMonthID
 ,CON.UniqServReqID
 ,CON.ReferralRequestReceivedDate
 ,CON.ReferralRequestReceivedDateTime
 ,CON.UniqCareContID
 ,CON.AttendOrDNACode
 ,CON.ConsMechanismMH
 ,CON.CareContDate
 ,CON.CareContTime
 ,CON.CareContDateTime
 ,ROW_NUMBER() OVER (PARTITION BY CON.UniqServReqID ORDER BY CON.CareContDate ASC, CON.CareContTime ASC, CON.UniqCareContID ASC) AS Der_ContactOrder
 FROM $db_output.contact_prep CON
 WHERE CON.CareContDateTime >= CON.ReferralRequestReceivedDateTime ---limit to contacts which occurred on or after the day referral was received
 AND CON.AttendOrDNACode in ('5','6')                     
 AND CON.ConsMechanismMH = '01'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.community_crisis_prep;
 CREATE TABLE IF NOT EXISTS $db_output.community_crisis_prep AS
              SELECT  MPI.IC_Rec_CCG as CCG_Code
                     ,MPI.NAME as CCG_Name
                     ,REF.OrgIDProv
                     ,OD.NAME as Provider_Name
                     ,REF.ClinRespPriorityType
                     ,REF.ReferralRequestReceivedDate
                     ,REF.ReferralRequestReceivedTime
                     ,CASE WHEN REF.ReferralRequestReceivedTime IS NULL THEN to_timestamp(REF.ReferralRequestReceivedDate)
                           ELSE combine_mhsds_date_and_time(REF.ReferralRequestReceivedDate, REF.ReferralRequestReceivedTime) 
                           END as ReferralRequestReceivedDateTime
                     ,CASE WHEN REF.AgeServReferRecDate BETWEEN 0 and 17 THEN '0-17'
                           WHEN REF.AgeServReferRecDate >=18 THEN '18 and over' 
                           END AS AGE_GROUP
                      ,REF.UniqServReqID
                      ,REF.SourceOfReferralMH
                      ,REFTO.ServTeamTypeRefToMH
                      ,CON.UniqCareContID
                      ,CON.AttendOrDNACode
                      ,CON.ConsMechanismMH
                      ,CON.CareContDate
                      ,CON.CareContTime
                      ,CASE WHEN CON.CareContTime IS NULL THEN to_timestamp(CON.CareContDate)
                            ELSE combine_mhsds_date_and_time(CON.CareContDate, CON.CareContTime) 
                            END as CareContDateTime
                      
                 FROM $db_output.MHS001MPI_latest_month_data AS MPI
           INNER JOIN $db_source.MHS101Referral AS REF 
                      ON MPI.Person_ID = REF.Person_ID
                      AND REF.UniqMonthID = '$end_month_id' 
           INNER JOIN $db_source.MHS102ServiceTypeReferredTo AS REFTO
                      ON REF.UniqServReqID = REFTO.UniqServReqID 
                      AND REFTO.UniqMonthID = '$end_month_id' 
           
           LEFT JOIN $db_output.attended_direct_contact_order CON 
                      ON REF.UniqServReqID = CON.UniqServReqID 
                      AND Der_ContactOrder = 1 ---first care contact only
                      
          LEFT JOIN $db_output.mhsds_org_daily OD
                    ON REF.OrgIDProv = OD.ORG_CODE
                      
                WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
               AND REFTO.ServTeamTypeRefToMH in ("A02", "A18", "A19") 

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.community_crisis_master
 SELECT  UniqServReqID
         ,CCG_Code
         ,CCG_Name
         ,OrgIDProv
         ,Provider_Name
         ,ClinRespPriorityType
         ,ReferralRequestReceivedDate
         ,ReferralRequestReceivedTime
         ,ReferralRequestReceivedDateTime
         ,AGE_GROUP        
         ,SourceOfReferralMH
         ,ServTeamTypeRefToMH
         ,UniqCareContID
         ,AttendOrDNACode
         ,ConsMechanismMH
         ,CareContDate
         ,CareContTime
         ,CareContDateTime
         ,CAST(CAST(CareContDateTime as long) - CAST(ReferralRequestReceivedDateTime as long) as float) / 60 as minutes_between_ref_and_first_cont
 FROM $db_output.community_crisis_prep

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.liason_psychiatry_prep;
 CREATE TABLE IF NOT EXISTS $db_output.liason_psychiatry_prep AS
              SELECT  REF.UniqServReqID
                     ,MPI.IC_Rec_CCG as CCG_Code 
                     ,MPI.NAME as CCG_Name
                     ,REF.OrgIDProv
                     ,OD.NAME as Provider_Name
                     ,REF.ClinRespPriorityType
                     ,REF.ReferralRequestReceivedDate
                     ,REF.ReferralRequestReceivedTime
                     ,CASE WHEN REF.ReferralRequestReceivedTime IS NULL THEN to_timestamp(REF.ReferralRequestReceivedDate)
                           ELSE combine_mhsds_date_and_time(REF.ReferralRequestReceivedDate, REF.ReferralRequestReceivedTime) 
                           END as ReferralRequestReceivedDateTime
                     ,CASE WHEN REF.AgeServReferRecDate BETWEEN 0 and 17 THEN '0-17'
                           WHEN REF.AgeServReferRecDate >=18 THEN '18 and over' 
                           END AS AGE_GROUP
                     
                      ,REF.SourceOfReferralMH
                      ,REFTO.ServTeamTypeRefToMH
                      ,CON.UniqCareContID
                      ,CON.AttendOrDNACode
                      ,CON.ConsMechanismMH
                      ,CON.CareContDate
                      ,CON.CareContTime
                      ,CASE WHEN CON.CareContTime IS NULL THEN to_timestamp(CON.CareContDate)
                            ELSE combine_mhsds_date_and_time(CON.CareContDate, CON.CareContTime) 
                            END as CareContDateTime
                            
                 FROM $db_output.MHS001MPI_latest_month_data AS MPI
           INNER JOIN $db_source.MHS101Referral AS REF 
                      ON MPI.Person_ID = REF.Person_ID
                      AND REF.UniqMonthID = '$end_month_id' 
           INNER JOIN $db_source.MHS102ServiceTypeReferredTo AS REFTO
                      ON REF.UniqServReqID = REFTO.UniqServReqID 
                      AND REFTO.UniqMonthID = '$end_month_id' 
           
           LEFT JOIN $db_output.attended_direct_contact_order CON 
                      ON REF.UniqServReqID = CON.UniqServReqID 
                      AND Der_ContactOrder = 1 ---first care contact only
                      
           LEFT JOIN $db_output.mhsds_org_daily OD
                    ON REF.OrgIDProv = OD.ORG_CODE
                      
                WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
               AND REFTO.ServTeamTypeRefToMH in ("A11", "C05")
               AND REF.SourceOfReferralMH = "H1"

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.liason_psychiatry_master
 SELECT   UniqServReqID
         ,CCG_Code
         ,CCG_Name
         ,OrgIDProv
         ,Provider_Name
         ,ClinRespPriorityType
         ,ReferralRequestReceivedDate
         ,ReferralRequestReceivedTime
         ,ReferralRequestReceivedDateTime
         ,AGE_GROUP       
         ,SourceOfReferralMH
         ,ServTeamTypeRefToMH
         ,UniqCareContID
         ,AttendOrDNACode
         ,ConsMechanismMH
         ,CareContDate
         ,CareContTime
         ,CareContDateTime
         ,CAST(CAST(CareContDateTime as long) - CAST(ReferralRequestReceivedDateTime as long) as float) / 60 as minutes_between_ref_and_first_cont
 FROM $db_output.liason_psychiatry_prep