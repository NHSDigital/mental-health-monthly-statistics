# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY FUNCTION combine_mhsds_date_and_time(date_col DATE, time_col TIMESTAMP)
 RETURNS TIMESTAMP
 COMMENT 'Combines MHSDS date and time columns into a single datetime, as MHSDS time values contain 1970-01-01'
 RETURN to_timestamp(concat(date_col, 'T', substring(time_col, 12, 8)))

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.contact_prep
 SELECT
 REF.UniqMonthID,
 REF.UniqServReqID,
 REF.ReferralRequestReceivedDate,
 REF.ReferralRequestReceivedTime,
 CASE WHEN REF.ReferralRequestReceivedTime IS NULL THEN to_timestamp(REF.ReferralRequestReceivedDate)
       ELSE combine_mhsds_date_and_time(REF.ReferralRequestReceivedDate, REF.ReferralRequestReceivedTime) 
       END as ReferralRequestReceivedDateTime,
 CON.UniqCareContID,
 CON.AttendStatus,
 CON.ConsMechanismMH,
 CON.CareContDate,
 CON.CareContTime,
 CASE WHEN CON.CareContTime IS NULL THEN to_timestamp(CON.CareContDate)
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
 INSERT OVERWRITE TABLE $db_output.attended_direct_contact_order
 SELECT
 CON.UniqMonthID
 ,CON.UniqServReqID
 ,CON.ReferralRequestReceivedDate
 ,CON.ReferralRequestReceivedDateTime
 ,CON.UniqCareContID
 ,CON.AttendStatus
 ,CON.ConsMechanismMH
 ,CON.CareContDate
 ,CON.CareContTime
 ,CON.CareContDateTime
 ,ROW_NUMBER() OVER (PARTITION BY CON.UniqServReqID ORDER BY CON.CareContDate ASC, CON.CareContTime ASC, CON.UniqCareContID ASC) AS Der_ContactOrder
 FROM $db_output.contact_prep CON
 WHERE CON.CareContDateTime >= CON.ReferralRequestReceivedDateTime ---limit to contacts which occurred on or after the day referral was received
 AND CON.AttendStatus in ('5','6')                     
 AND CON.ConsMechanismMH = '01'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE  $db_output.community_crisis_prep
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
        ,CON.AttendStatus
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
 INNER JOIN $db_output.ServiceTeamType AS REFTO
        ON REF.UniqServReqID = REFTO.UniqServReqID 
        AND REFTO.UniqMonthID = '$end_month_id' 
  
 LEFT JOIN $db_output.attended_direct_contact_order CON 
        ON REF.UniqServReqID = CON.UniqServReqID 
        AND Der_ContactOrder = 1 ---first care contact only
  
 LEFT JOIN $db_output.bbrb_org_daily_latest OD
      ON REF.OrgIDProv = OD.ORG_CODE
  
  WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
 AND REFTO.ServTeamTypeRefToMH in ("A02", "A18", "A19") ---maybe use $db_output.validcodes here

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
         ,AttendStatus
         ,ConsMechanismMH
         ,CareContDate
         ,CareContTime
         ,CareContDateTime
         ,CAST(CAST(CareContDateTime as long) - CAST(ReferralRequestReceivedDateTime as long) as float) / 60 as minutes_between_ref_and_first_cont
 FROM $db_output.community_crisis_prep

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.liason_psychiatry_master

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.liason_psychiatry_prep
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
        ,CON.AttendStatus
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
 INNER JOIN $db_output.ServiceTeamType AS REFTO
        ON REF.UniqServReqID = REFTO.UniqServReqID 
        AND REFTO.UniqMonthID = '$end_month_id' 
  
 LEFT JOIN $db_output.attended_direct_contact_order CON 
        ON REF.UniqServReqID = CON.UniqServReqID 
        AND Der_ContactOrder = 1 ---first care contact only
  
 LEFT JOIN $db_output.bbrb_org_daily_latest OD
      ON REF.OrgIDProv = OD.ORG_CODE
  
  WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
 AND REFTO.ServTeamTypeRefToMH in ("A11", "C05") ---maybe use $db_output.validcodes here
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
         ,AttendStatus
         ,ConsMechanismMH
         ,CareContDate
         ,CareContTime
         ,CareContDateTime
         ,CAST(CAST(CareContDateTime as long) - CAST(ReferralRequestReceivedDateTime as long) as float) / 60 as minutes_between_ref_and_first_cont
 FROM $db_output.liason_psychiatry_prep

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.liason_psychiatry_master