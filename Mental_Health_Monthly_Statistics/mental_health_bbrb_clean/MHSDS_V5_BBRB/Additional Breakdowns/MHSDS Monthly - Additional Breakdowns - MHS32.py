# Databricks notebook source
 %md
 ##MHS32 prep table

# COMMAND ----------

 %sql
  -- CHANGED LOWERCASE UNKNOWN TO UPPER FOR CONSISTENCY 27/7/22
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs32_prep;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs32_prep USING DELTA AS
 
   SELECT    REF.UniqServReqID 
            ,REF.Person_ID
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
 FROM $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 LEFT JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND MPI.PatMRecInRP = TRUE
 WHERE REF.UniqMonthID = '$end_month_id' 
 AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate_1m' AND '$rp_enddate'

# COMMAND ----------

 %md
 ##MHS32c prep tables

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS32c_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS32c_prep USING DELTA AS
 
   SELECT   REF.UniqServReqID 
            ,REF.SourceOfReferralMH
            ,MPI.IC_REC_GP_RES
            ,MPI.NAME
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
                  WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
                  WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
                  WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
                  WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
                  WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
                  WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
                  WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
                  WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
                  WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
                  WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
                  WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
                  WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
                  WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                  ELSE 'Invalid' 
            END AS Referral_Source 
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
                 WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
                 WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
                 WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
                 WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
                 WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
                 WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
                 WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
                 WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
                 WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
                 WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
                 WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
                 WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
                 WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                 ELSE 'Invalid' 
             END AS Referral_Description
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
        FROM $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
  INNER JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
       WHERE REF.UniqMonthID = '$end_month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate_1m' AND '$rp_enddate'
         AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');

# COMMAND ----------

# DBTITLE 1,Core Community Prep table
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS32c_prep_prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS32c_prep_prov USING DELTA AS
 
   SELECT   REF.UniqServReqID
            ,REF.SourceOfReferralMH
            ,REF.OrgIDProv
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
                  WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
                  WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
                  WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
                  WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
                  WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
                  WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
                  WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
                  WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
                  WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
                  WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
                  WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
                  WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
                  WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                  ELSE 'Invalid' 
            END AS Referral_Source 
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
                 WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
                 WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
                 WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
                 WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
                 WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
                 WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
                 WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
                 WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
                 WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
                 WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
                 WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
                 WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
                 WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                 ELSE 'Invalid' 
             END AS Referral_Description 
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup
        FROM $db_source.MHS101Referral AS REF
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_source.MHS001MPI AS MPI 
             ON MPI.Person_ID = REF.Person_ID 
            AND MPI.OrgIDProv = REF.OrgIDProv
            AND MPI.UniqMonthID = REF.UniqMonthID
       WHERE REF.UniqMonthID = '$end_month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate_1m' AND '$rp_enddate'
         AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');

# COMMAND ----------

# DBTITLE 1,Specialist Perinatal prep table 1
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS32d_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS32d_prep USING DELTA AS
 
   SELECT   REF.UniqServReqID 
            ,REF.SourceOfReferralMH
            ,MPI.IC_REC_GP_RES
            ,MPI.NAME
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
                  WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
                  WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
                  WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
                  WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
                  WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
                  WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
                  WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
                  WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
                  WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
                  WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
                  WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
                  WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
                  WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                  ELSE 'Invalid' 
            END AS Referral_Source 
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
                 WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
                 WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
                 WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
                 WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
                 WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
                 WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
                 WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
                 WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
                 WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
                 WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
                 WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
                 WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
                 WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                 ELSE 'Invalid' 
             END AS Referral_Description
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
        FROM $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
  INNER JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
       WHERE REF.UniqMonthID = '$end_month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate_1m' AND '$rp_enddate'
         AND SERV.ServTeamTypeRefToMH in ('C02');

# COMMAND ----------

# DBTITLE 1,Specialist Perinatal prep table 2
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS32d_prep_prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS32d_prep_prov USING DELTA AS
   SELECT   REF.UniqServReqID
            ,REF.SourceOfReferralMH
            ,REF.OrgIDProv
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
                  WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
                  WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
                  WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
                  WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
                  WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
                  WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
                  WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
                  WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
                  WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
                  WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
                  WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
                  WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
                  WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                  ELSE 'Invalid' 
            END AS Referral_Source 
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
                 WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
                 WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
                 WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
                 WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
                 WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
                 WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
                 WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
                 WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
                 WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
                 WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
                 WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
                 WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
                 WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                 ELSE 'Invalid' 
             END AS Referral_Description
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
        FROM $db_source.MHS101Referral AS REF
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$end_month_id'
  INNER JOIN $db_source.MHS001MPI AS MPI 
             ON MPI.Person_ID = REF.Person_ID 
            AND MPI.OrgIDProv = REF.OrgIDProv
            AND MPI.UniqMonthID = REF.UniqMonthID
       WHERE REF.UniqMonthID = '$end_month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate_1m' AND '$rp_enddate'
         AND SERV.ServTeamTypeRefToMH in ('C02');

# COMMAND ----------

 %md
 ##MHS32 data into output

# COMMAND ----------

# DBTITLE 1,England breakdown added for testing purpose, will need removing at BBRB final output 01/09/22
 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Accommodation Type' AS BREAKDOWN
            ,AccommodationType AS PRIMARY_LEVEL
            ,AccommodationType_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  AccommodationType, AccommodationType_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age' AS BREAKDOWN
            ,Age_Band AS PRIMARY_LEVEL
            ,Age_Band AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  Age_Band

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Disability' AS BREAKDOWN
            ,DisabCode AS PRIMARY_LEVEL
            ,DisabCode_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  DisabCode, DisabCode_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Employment Status' AS BREAKDOWN
            ,EmployStatus AS PRIMARY_LEVEL
            ,EmployStatus_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  EmployStatus, EmployStatus_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Ethnicity' AS BREAKDOWN
            ,LowerEthnicity AS PRIMARY_LEVEL
            ,LowerEthnicity_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  LowerEthnicity, LowerEthnicity_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Gender' AS BREAKDOWN
            ,Der_Gender AS PRIMARY_LEVEL
            ,Der_Gender AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; IMD Decile' AS BREAKDOWN
            ,IMD_Decile AS PRIMARY_LEVEL
            ,IMD_Decile AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Sexual Orientation' AS BREAKDOWN
            ,Sex_Orient AS PRIMARY_LEVEL
            ,Sex_Orient AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS32_prep
  GROUP BY  Sex_Orient

# COMMAND ----------

 %md
 ##MHS32c data into output

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32c' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_MHMAB_MHS32c_prep;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age Group' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,AgeGroup AS SECONDARY_LEVEL
            ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32c' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_MHMAB_MHS32c_prep
  GROUP BY  AgeGroup;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32c' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM  $db_output.tmp_MHMAB_MHS32c_prep
   GROUP BY IC_REC_GP_RES, NAME;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Age Group' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32c' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM  $db_output.tmp_MHMAB_MHS32c_prep
   GROUP BY IC_REC_GP_RES, NAME, AgeGroup;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider'    AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE'    AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE'    AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32c' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_MHMAB_MHS32c_prep_prov
   GROUP BY OrgIDProv;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group'    AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE'    AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup    AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32c' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_MHMAB_MHS32c_prep_prov
   GROUP BY OrgIDProv, AgeGroup;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32d' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_MHMAB_MHS32d_prep;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32d' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM  $db_output.tmp_MHMAB_MHS32d_prep
   GROUP BY IC_REC_GP_RES, NAME;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider'    AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE'    AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE'    AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32d' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_MHMAB_MHS32d_prep_prov
   GROUP BY OrgIDProv;

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))