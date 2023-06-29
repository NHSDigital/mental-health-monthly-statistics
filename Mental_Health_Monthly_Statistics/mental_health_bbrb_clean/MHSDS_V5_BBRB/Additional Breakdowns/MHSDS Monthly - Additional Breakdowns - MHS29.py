 %md
 ##MHS29 prep tables

# COMMAND ----------

 %sql
  -- CHANGED TITLE CASE UNKNOWN TO UPPER FOR CONCISTENCY 27/07/22
 DROP table IF EXISTS $db_output.tmp_mhmab_MHS29_prep;
  
 CREATE TABLE $db_output.tmp_mhmab_MHS29_prep USING DELTA AS
 -- CREATE OR REPLACE GLOBAL TEMP VIEW MHS29d_prep AS
 SELECT                CC.UniqCareContID                     
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,COALESCE(MPI.Age_Band, "UNKNOWN") as Age_Band
                       ,COALESCE(MPI.Der_Gender, "UNKNOWN") as Der_Gender
                       ,COALESCE(MPI.LowerEthnicity, "UNKNOWN") as LowerEthnicity
                       ,COALESCE(MPI.LowerEthnicity_Desc, "UNKNOWN") as LowerEthnicity_Desc
                       ,COALESCE(MPI.IMD_Decile, "UNKNOWN") as IMD_Decile
                       ,COALESCE(MPI.AccommodationType, "UNKNOWN") as AccommodationType
                       ,COALESCE(MPI.AccommodationType_Desc, "UNKNOWN") as AccommodationType_Desc
                       ,COALESCE(MPI.EmployStatus, "UNKNOWN") as EmployStatus
                       ,COALESCE(MPI.EmployStatus_Desc, "UNKNOWN") as EmployStatus_Desc
                       ,COALESCE(MPI.DisabCode, "UNKNOWN") as DisabCode
                       ,COALESCE(MPI.DisabCode_Desc, "UNKNOWN") as DisabCode_Desc
                       ,COALESCE(MPI.Sex_Orient, "UNKNOWN") as Sex_Orient
  FROM $db_output.tmp_mhmab_mhs001mpi_latest_month_data AS MPI      
  LEFT JOIN $db_source.MHS201CareContact AS CC ON MPI.Person_ID = CC.Person_ID AND MPI.PatMRecInRP = TRUE
  WHERE      CC.UniqMonthID = '$end_month_id' 
  AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate')

# COMMAND ----------

 %md
 ##MHS29a prep tables

# COMMAND ----------

# DBTITLE 1,MHS29a Prep Table 12/07/22  SERV.ServTeamTypeRefToMH in ('C02')
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS29a_prep;
 CREATE TABLE $db_output.tmp_MHMAB_MHS29a_prep USING DELTA AS
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES--- changed from CCG.IC_REC_CCG AT 26/09/22
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
                             ELSE 'UNKNOWN' END as AgeGroup -- Possibly use Age at Rep Period End from the MPI table  
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 5
                             WHEN CC.AttendOrDNACode = 6 THEN 6
                             WHEN CC.AttendOrDNACode = 2 THEN 2
                             WHEN CC.AttendOrDNACode = 3 THEN 3
                             WHEN CC.AttendOrDNACode = 4 THEN 4
                             WHEN CC.AttendOrDNACode = 7 THEN 7
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Code
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 'Attended on time or, if late, before the relevant professional was ready to see the patient'
                             WHEN CC.AttendOrDNACode = 6 THEN 'Arrived late, after the relevant professional was ready to see the patient, but was seen'
                             WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Name
                             
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
            AND SERV.ServTeamTypeRefToMH = 'C02';

# COMMAND ----------

# DBTITLE 1,MHS29a Prep_prov Table 12/07/22 SERV.ServTeamTypeRefToMH in ('C02')
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS29a_prep_prov;
 CREATE TABLE $db_output.tmp_MHMAB_MHS29a_prep_prov USING DELTA AS
 
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_GP_RES--- changed from CCG.IC_REC_CCG AT 26/09/22
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
                             ELSE 'UNKNOWN' END as AgeGroup -- Possibly use Age at Rep Period End from the MPI table    
                        ,CASE WHEN CC.AttendOrDNACode = 5 THEN 5
                             WHEN CC.AttendOrDNACode = 6 THEN 6
                             WHEN CC.AttendOrDNACode = 2 THEN 2
                             WHEN CC.AttendOrDNACode = 3 THEN 3
                             WHEN CC.AttendOrDNACode = 4 THEN 4
                             WHEN CC.AttendOrDNACode = 7 THEN 7
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Code
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 'Attended on time or, if late, before the relevant professional was ready to see the patient'
                             WHEN CC.AttendOrDNACode = 6 THEN 'Arrived late, after the relevant professional was ready to see the patient, but was seen'
                             WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Name
 
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
            AND SERV.ServTeamTypeRefToMH = 'C02';

# COMMAND ----------

 %md
 ##MHS29d prep tables

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS29d_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS29d_prep USING DELTA AS
 -- CREATE OR REPLACE GLOBAL TEMP VIEW MHS29d_prep AS
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
                             ELSE 'UNKNOWN' END as AgeGroup -- Possibly use Age at Rep Period End from the MPI table 
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 5
                             WHEN CC.AttendOrDNACode = 6 THEN 6
                             WHEN CC.AttendOrDNACode = 2 THEN 2
                             WHEN CC.AttendOrDNACode = 3 THEN 3
                             WHEN CC.AttendOrDNACode = 4 THEN 4
                             WHEN CC.AttendOrDNACode = 7 THEN 7
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Code
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 'Attended on time or, if late, before the relevant professional was ready to see the patient'
                             WHEN CC.AttendOrDNACode = 6 THEN 'Arrived late, after the relevant professional was ready to see the patient, but was seen'
                             WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Name
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
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS29d_prep_prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS29d_prep_prov USING DELTA AS
 -- CREATE OR REPLACE GLOBAL TEMP VIEW MHS29d_prep_prov AS
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
                             ELSE 'UNKNOWN' END as AgeGroup -- Possibly use Age at Rep Period End from the MPI table   
                        ,CASE WHEN CC.AttendOrDNACode = 5 THEN 5
                             WHEN CC.AttendOrDNACode = 6 THEN 6
                             WHEN CC.AttendOrDNACode = 2 THEN 2
                             WHEN CC.AttendOrDNACode = 3 THEN 3
                             WHEN CC.AttendOrDNACode = 4 THEN 4
                             WHEN CC.AttendOrDNACode = 7 THEN 7
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Code
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 'Attended on time or, if late, before the relevant professional was ready to see the patient'
                             WHEN CC.AttendOrDNACode = 6 THEN 'Arrived late, after the relevant professional was ready to see the patient, but was seen'
                             WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Name
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
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS29f_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS29f_prep USING DELTA AS
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
                        ,CASE WHEN CC.AttendOrDNACode = 5 THEN 5
                             WHEN CC.AttendOrDNACode = 6 THEN 6
                             WHEN CC.AttendOrDNACode = 2 THEN 2
                             WHEN CC.AttendOrDNACode = 3 THEN 3
                             WHEN CC.AttendOrDNACode = 4 THEN 4
                             WHEN CC.AttendOrDNACode = 7 THEN 7
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Code
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 'Attended on time or, if late, before the relevant professional was ready to see the patient'
                             WHEN CC.AttendOrDNACode = 6 THEN 'Arrived late, after the relevant professional was ready to see the patient, but was seen'
                             WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Name
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$end_month_id' 
            AND (CareContDate >= '$rp_startdate_1m' AND CareContDate <= '$rp_enddate');

# COMMAND ----------

 %md
 ##MHS29 data into output

# COMMAND ----------

# DBTITLE 1,MHS29 - England (added for testing purposes)
 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Accommodation Type**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep 

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Accommodation Type**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Accommodation Type' AS BREAKDOWN
             ,AccommodationType AS PRIMARY_LEVEL
             ,AccommodationType_Desc AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep 
 GROUP BY AccommodationType, AccommodationType_Desc

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Age**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age' AS BREAKDOWN
             ,Age_Band AS PRIMARY_LEVEL
             ,Age_Band AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY Age_Band

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Disability**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Disability' AS BREAKDOWN
             ,DisabCode AS PRIMARY_LEVEL
             ,DisabCode_Desc AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY DisabCode, DisabCode_Desc

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Employment Status**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Employment Status' AS BREAKDOWN
             ,EmployStatus AS PRIMARY_LEVEL
             ,EmployStatus_Desc AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY EmployStatus, EmployStatus_Desc

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Ethnicity**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Ethnicity' AS BREAKDOWN
             ,LowerEthnicity AS PRIMARY_LEVEL
             ,LowerEthnicity_Desc AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY LowerEthnicity, LowerEthnicity_Desc

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Gender**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Gender' AS BREAKDOWN
             ,Der_Gender AS PRIMARY_LEVEL
             ,Der_Gender AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, IMD_Decile**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; IMD Decile' AS BREAKDOWN
             ,IMD_Decile AS PRIMARY_LEVEL
             ,IMD_Decile AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD, Sexual Orientation**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Sexual Orientation' AS BREAKDOWN
             ,Sex_Orient AS PRIMARY_LEVEL
             ,Sex_Orient AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_mhmab_MHS29_prep
 GROUP BY Sex_Orient

# COMMAND ----------

 %md 
 ##MHS29a data into output

# COMMAND ----------

# DBTITLE 1,MHS29a England  
 %sql
  
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
             ,'MHS29a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29a_prep -- prep table in main monthly prep folder

# COMMAND ----------

# DBTITLE 1,MHS29a - CONTACTS IN REPORTING PERIOD, National, Age Group 
 %sql
 --MHS29a - CONTACTS IN REPORTING PERIOD, National, Age Group**/
 
 -- INSERT INTO $db_output.output1
 
 --     SELECT 
 --              '$rp_startdate_1m' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --             ,'England; Age Group' AS BREAKDOWN
 --             ,'England' AS PRIMARY_LEVEL
 --             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 --             ,AgeGroup AS SECONDARY_LEVEL
 --             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
 --             ,'MHS29a' AS MEASURE_ID
 --             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
 --             ,'$db_source' AS SOURCE_DB
             
 -- FROM        $db_output.tmp_MHMAB_MHS29a_prep -- prep table in main monthly prep folder
 -- GROUP BY    AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS29a - CONTACTS IN REPORTING PERIOD, National, Attendance
 %sql
 --MHS29a - CONTACTS IN REPORTING PERIOD, National, Attendance**/
  
 INSERT INTO $db_output.output1
 
      SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Attendance' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL  -- as per MHS29f
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29a_prep -- prep table in main monthly prep folder
 
 GROUP BY    Attend_Code, Attend_Name
 --7,8

# COMMAND ----------

# DBTITLE 1,MHS29a - CONTACTS IN REPORTING PERIOD, CCG 
 %sql
 --/**MHS29a - CONTACTS IN REPORTING PERIOD, CCG**/
 
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
             ,'MHS29a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29a_prep -- prep table in main monthly prep folder
 
 GROUP BY    IC_REC_GP_RES
             ,NAME


# COMMAND ----------

# DBTITLE 1,MHS29a - CONTACTS IN REPORTING PERIOD, CCG, Attendance 
 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, CCG, Attendance **/
   
 INSERT INTO $db_output.output1
 
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Attendance' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
 
             ,'MHS29a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM       $db_output.tmp_MHMAB_MHS29a_prep -- prep table in main monthly prep folder
 
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,Attend_Code
             ,Attend_Name

# COMMAND ----------

# DBTITLE 1,MHS29a - CONTACTS IN REPORTING PERIOD, PROVIDER
 %sql
 --/**MHS29a - CONTACTS IN REPORTING PERIOD, PROVIDER**/

 INSERT INTO $db_output.output1
 
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION  -- no provider name? HL
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29a_prep_prov -- prep table in main monthly prep folder
 
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS29a - CONTACTS IN REPORTING PERIOD, PROVIDER, Attendance
 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER, Attendance**/
 
 INSERT INTO $db_output.output1
 
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Attendance' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION 
             ,Attend_Code AS SECONDARY_LEVEL 
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
 
             ,'MHS29a' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29a_prep_prov -- prep table in main monthly prep folder
 
 GROUP BY    OrgIDProv, Attend_Code, Attend_Name

# COMMAND ----------

 %md
 ##MHS29d data into output

# COMMAND ----------

 %sql
 --MHS29d - CONTACTS IN REPORTING PERIOD, National**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep -- prep table in main monthly prep folder

# COMMAND ----------

 %sql
 --MHS29d - CONTACTS IN REPORTING PERIOD, National, Age Group**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep
 GROUP BY    AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS29d - CONTACTS IN REPORTING PERIOD, National, Attendance 
 %sql
 --MHS29d - CONTACTS IN REPORTING PERIOD, National, Attendance**/
  
 INSERT INTO $db_output.output1
 
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Attendance' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL  -- as per MHS29f
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
 
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    Attend_Code, Attend_Name
 --7, 8 

# COMMAND ----------

 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, CCG**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    IC_REC_GP_RES
             ,NAME

# COMMAND ----------

 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, CCG, Age Group**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Age Group' AS BREAKDOWN
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS29d - CONTACTS IN REPORTING PERIOD, CCG, Attendance
 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, CCG, Attendance **/
  
 INSERT INTO $db_output.output1
 
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Attendance' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL  -- as per MHS29f
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
 
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep -- prep table in main monthly prep folder
 
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,Attend_Code
             ,Attend_Name

# COMMAND ----------

 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep_prov -- prep table in main monthly prep folder
 GROUP BY    OrgIDProv

# COMMAND ----------

 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER, Age Group**/
  
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep_prov -- prep table in main monthly prep folder
 GROUP BY    OrgIDProv
             ,AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER, Attendance
 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER, Attendance**/
  
 INSERT INTO $db_output.output1
     SELECT 
             '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'Provider; Attendance' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION 
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
 
             ,'MHS29d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29d_prep_prov -- prep table in main monthly prep folder
 
 GROUP BY    OrgIDProv, Attend_Code, Attend_Name

# COMMAND ----------

 %md
 ##MHS29e data into output

# COMMAND ----------

# %sql
# --MHS29e - CONTACTS IN REPORTING PERIOD, National**/
 
# INSERT INTO $db_output.output1
 
#     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,'England' AS BREAKDOWN
#             ,'England' AS PRIMARY_LEVEL
#             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
#             ,'NONE' AS SECONDARY_LEVEL
#             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS29e' AS MEASURE_ID
#             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
#             ,'$db_source' AS SOURCE_DB
            
# FROM        $db_output.tmp_MHMAB_MHS29e_prep -- prep table in main monthly prep folder

# COMMAND ----------

# %sql
# --/**MHS29e - CONTACTS IN REPORTING PERIOD, CCG**/
 
# INSERT INTO $db_output.output1
 
#     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,'CCG - GP Practice or Residence' AS BREAKDOWN
#             ,IC_Rec_CCG AS PRIMARY_LEVEL
#             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
#             ,'NONE' AS SECONDARY_LEVEL
#             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS29e' AS MEASURE_ID
#             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
#             ,'$db_source' AS SOURCE_DB
            
# FROM        $db_output.tmp_MHMAB_MHS29e_prep -- prep table in main monthly prep folder
# GROUP BY    IC_Rec_CCG
#             ,NAME

# COMMAND ----------

# %sql
# --/**MHS29e - CONTACTS IN REPORTING PERIOD, PROVIDER**/
 
# INSERT INTO $db_output.output1

#     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,'Provider' AS BREAKDOWN
#             ,OrgIDProv AS PRIMARY_LEVEL
#             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#             ,'NONE' AS SECONDARY_LEVEL
#             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS29e' AS MEASURE_ID
#             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
#             ,'$db_source' AS SOURCE_DB
            
# FROM        $db_output.tmp_MHMAB_MHS29e_prep_prov -- prep table in main monthly prep folder
# GROUP BY    OrgIDProv

# COMMAND ----------

 %md
 ##MHS29f data into output

# COMMAND ----------

 %sql
 --MHS29f - CONTACTS IN REPORTING PERIOD, National**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Attendance' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29f_prep -- prep table in main monthly prep folder
 GROUP BY    Attend_Code, Attend_Name

# COMMAND ----------

 %sql
 --/**MHS29f - CONTACTS IN REPORTING PERIOD, CCG**/
  
 INSERT INTO $db_output.output1
  
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Attendance' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29f_prep -- prep table in main monthly prep folder
 GROUP BY    IC_REC_GP_RES
             ,NAME
             ,Attend_Code
             ,Attend_Name

# COMMAND ----------

 %sql
 --/**MHS29f - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Attendance' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29f' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS29f_prep
 GROUP BY    OrgIDProv
             ,Attend_Code
             ,Attend_Name



# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))