# Databricks notebook source
 %md 
 ##MHS30a prep tables

# COMMAND ----------

# DBTITLE 1,SERV.ServTeamTypeRefToMH = 'C02'
 %sql
 
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS30a_prep;
 CREATE TABLE $db_output.tmp_MHMAB_MHS30a_prep USING DELTA AS
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                       ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup                    
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
             ON MPI.Person_ID = CC.Person_ID 
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
             AND cc.UniqCareProfTeamID = serv.UniqCareProfTeamID 
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate')
            AND SERV.ServTeamTypeRefToMH = 'C02';
  

# COMMAND ----------

# DBTITLE 1,MHS30a Prep_Prov (ServTeamTypeRefToMH = 'C02' )
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS30a_prep_prov;
 CREATE TABLE $db_output.tmp_MHMAB_MHS30a_prep_prov USING DELTA AS
 
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                       ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup                     
   
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  INNER JOIN $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = CC.Person_ID
            AND CC.OrgIDPRov = MPI.OrgIDProv
            AND MPI.UniqMonthID = CC.UniqMonthID
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND cc.UniqCareProfTeamID = serv.UniqCareProfTeamID --added missing join 08/08/22
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate')
            AND SERV.ServTeamTypeRefToMH = 'C02';
   

# COMMAND ----------

 %md 
 ##MHS30f prep tables

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS30f_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS30f_prep USING DELTA AS
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                       ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup                    
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
             ON MPI.Person_ID = CC.Person_ID 
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND CC.UniqCareProfTeamID = SERV.UniqCareProfTeamID 
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate')
            AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS30f_prep_prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS30f_prep_prov USING DELTA AS
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                       ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup                     
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  INNER JOIN $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = CC.Person_ID
            AND CC.OrgIDPRov = MPI.OrgIDProv
            AND MPI.UniqMonthID = CC.UniqMonthID
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV 
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND CC.UniqCareProfTeamID = SERV.UniqCareProfTeamID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate')
            AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS30h_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS30h_prep USING DELTA AS
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                       ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup                     
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
             ON MPI.Person_ID = CC.Person_ID 
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate');

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS30h_prep_prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS30h_prep_prov USING DELTA AS
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                       ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup                    
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  INNER JOIN $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = CC.Person_ID
            AND CC.OrgIDPRov = MPI.OrgIDProv
            AND MPI.UniqMonthID = CC.UniqMonthID
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate');

# COMMAND ----------

 %md
 
 ## MHS30a outputs

# COMMAND ----------

# DBTITLE 1,MHS30a - CONTACTS IN REPORTING PERIOD, National 13/07/22
 %sql
 --MHS30a - CONTACTS IN REPORTING PERIOD, National**/
 
 INSERT INTO $db_output.output1
  
     SELECT
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30a_prep 
 
 WHERE        AttendOrDNACode IN ('5', '6')

# COMMAND ----------

# DBTITLE 1,MHS30a - CONTACTS IN REPORTING PERIOD, National, Consultant Mechanism 13/7/22
 %sql
 -- MHS30a - CONTACTS IN REPORTING PERIOD, National, CMU **/
  INSERT INTO $db_output.output1
  
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
              ,'$status' AS STATUS
             ,'England; ConsMechanismMH' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30a_prep 
 
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    ConsMedUsed,CMU

# COMMAND ----------

# DBTITLE 1,MHS30a - CONTACTS IN REPORTING PERIOD, CCG 13/07/22
 %sql
 --/**MHS30a - CONTACTS IN REPORTING PERIOD, CCG**/
 INSERT INTO $db_output.output1
  
  SELECT
              '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
              ,'$status' AS STATUS
              ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
            
 FROM        $db_output.tmp_MHMAB_MHS30a_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    IC_REC_GP_RES
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS30a - CONTACTS IN REPORTING PERIOD, CCG, Consultant Mechanism 13/07/22
 %sql
 --/**MHS30a - CONTACTS IN REPORTING PERIOD, CCG, Consultant Mechanism**/
  
 INSERT INTO $db_output.output1
  
    SELECT
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; ConsMechanismMH' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
     
 FROM        $db_output.tmp_MHMAB_MHS30a_prep 
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,ConsMedUsed
             ,CMU

# COMMAND ----------

# DBTITLE 1,MHS30a - CONTACTS IN REPORTING PERIOD, PROVIDER --13/07/22
 %sql
 --/**MHS30a - CONTACTS IN REPORTING PERIOD, PROVIDER**/
 INSERT INTO $db_output.output1
 SELECT
      '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION -- name?
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30a_prep_prov -- prep table in main monthly prep folder
 
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    OrgIDProv

# COMMAND ----------

# %sql
# --/**MHS30a - CONTACTS IN REPORTING PERIOD, PROVIDER, Age Group**/ --TBC
 
# INSERT INTO $db_output.output1

#     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,'Provider; Age Group' AS BREAKDOWN
#             ,OrgIDProv AS PRIMARY_LEVEL
#             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#             ,AgeGroup AS SECONDARY_LEVEL
#             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS30a' AS MEASURE_ID
#             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
#             ,'$db_source' AS SOURCE_DB
            
# FROM        $db_output.tmp_MHMAB_MHS30a_prep_prov -- prep table in main monthly prep folder
# WHERE        AttendOrDNACode IN ('5', '6')
# GROUP BY    OrgIDProv
#             ,AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS30a - CONTACTS IN REPORTING PERIOD, PROVIDER, Consultant Mechanism 13/07/22
 %sql
 --/**MHS30a - CONTACTS IN REPORTING PERIOD, PROVIDER, Consultant Mechanism **/
  
 INSERT INTO $db_output.output1
 
  SELECT  
       '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; ConsMechanismMH' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
            
 FROM        $db_output.tmp_MHMAB_MHS30a_prep_prov -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    OrgIDProv
             ,ConsMedUsed
             ,CMU
     

# COMMAND ----------

 %md
 
 ## MHS30f outputs

# COMMAND ----------

 %sql
 --MHS30f - CONTACTS IN REPORTING PERIOD, National**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30f_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')

# COMMAND ----------

 %sql
 --MHS30f - CONTACTS IN REPORTING PERIOD, National, Age Group**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30f_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS30f - CONTACTS IN REPORTING PERIOD, National, Consultant Mechanism 12/7/22
 %sql
 --MHS30f - CONTACTS IN REPORTING PERIOD, National, CMU **/
  
 INSERT INTO $db_output.output1
  
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; ConsMechanismMH' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
         
 FROM        $db_output.tmp_MHMAB_MHS30f_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    ConsMedUsed,CMU

# COMMAND ----------

 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, CCG**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30f_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    IC_REC_GP_RES
             ,NAME

# COMMAND ----------

 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, CCG, Age Group**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Age Group' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30f_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS30f - CONTACTS IN REPORTING PERIOD, CCG, Consultant Mechanism 12/07/22
 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, CCG, Consultant Mechanism**/
  
 INSERT INTO $db_output.output1
  
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; ConsMechanismMH' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
    
 FROM        $db_output.tmp_MHMAB_MHS30f_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,ConsMedUsed
             ,CMU
             
            

# COMMAND ----------

 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30f_prep_prov -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    OrgIDProv

# COMMAND ----------

 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER, Age Group**/
  
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30f_prep_prov -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    OrgIDProv
             ,AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER, Consultant Mechanism
 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER, Consultant Mechanism **/
  
 INSERT INTO $db_output.output1
 
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; ConsMechanismMH' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
         
 FROM        $db_output.tmp_MHMAB_MHS30f_prep_prov -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    OrgIDProv
 ,ConsMedUsed
             ,CMU

# COMMAND ----------

 %sql
 --MHS30h - CONTACTS IN REPORTING PERIOD, National, Consulting Medium**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; ConsMechanismMH' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30h' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30h_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    ConsMedUsed
             ,CMU

# COMMAND ----------

 %sql
 --/**MHS30h - CONTACTS IN REPORTING PERIOD, CCG**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; ConsMechanismMH' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30h' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30h_prep -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,ConsMedUsed
             ,CMU

# COMMAND ----------

 %sql
 --/**MHS30h - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; ConsMechanismMH' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30h' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS30h_prep_prov -- prep table in main monthly prep folder
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    OrgIDProv
             ,ConsMedUsed
             ,CMU

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))