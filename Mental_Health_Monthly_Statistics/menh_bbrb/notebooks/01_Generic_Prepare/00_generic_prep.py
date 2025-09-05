# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY VIEW RD_CCG_LATEST AS
 SELECT DISTINCT ORG_TYPE_CODE, ORG_CODE, NAME
 FROM reference_data.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
   AND BUSINESS_START_DATE <= '$rp_enddate'
     AND ORG_TYPE_CODE = "CC"
       AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
         AND ORG_OPEN_DATE <= '$rp_enddate'
           AND NAME NOT LIKE '%HUB'
             AND NAME NOT LIKE '%NATIONAL%'
             AND NAME NOT LIKE '%ENTITY%'

# COMMAND ----------

 %sql
 -- TRUNCATE TABLE $db_output.bbrb_ccg_latest;
  
 with mappedCCGs as
  
 (
 SELECT original_ORG_CODE, original_NAME, ORG_CODE 
 FROM
 (SELECT rd.OrganisationID as original_ORG_CODE, 
                 Name as original_NAME,
 --  This will select 'Legal' dates over 'Operational' dates because of "order by rd.DateType"
 --  If it's decided we need 'Operational' dates as preference then add desc                         
                 row_number() over (partition by rd.OrganisationId order by rd.DateType ) as RN1,
                 COALESCE(odssd.TargetOrganisationID, rd.OrganisationID) as ORG_CODE
                 
         FROM $reference_data.ODSAPIRoleDetails rd
         
         LEFT JOIN $reference_data.ODSAPIOrganisationDetails od
         ON rd.OrganisationID = od.OrganisationID and rd.DateType = od.DateType
         
         LEFT JOIN $reference_data.ODSAPISuccessorDetails as odssd
         ON rd.OrganisationID = odssd.OrganisationID and odssd.Type = 'Successor' and odssd.StartDate <= '$rp_enddate'
         
         WHERE 
 -- a Primary RoleID of 'RO98' is a CCG
 -- a non-Primary RoleID of 'RO319' is a Sub-ICB
          CASE WHEN $end_month_id > 1467 THEN (rd.RoleId = 'RO319' and rd.PrimaryRole = 0)
             WHEN $end_month_id <= 1467 THEN (rd.RoleId = 'RO98' and rd.PrimaryRole = 1) 
             END
         AND (rd.EndDate >= add_months('$rp_enddate', 1) OR ISNULL(rd.EndDate))
         AND rd.StartDate <= add_months('$rp_enddate', 1)
         AND Name NOT LIKE '%HUB'
         AND Name NOT LIKE '%NATIONAL%' 
         AND Name NOT LIKE '%ENTITY%'
 )
 WHERE RN1 = 1
 )
  
 INSERT OVERWRITE TABLE $db_output.bbrb_ccg_latest
  
 SELECT 
       mappedCCGs.original_ORG_CODE,
       mappedCCGs.original_NAME,
       mappedCCGs.ORG_CODE,
       od1.NAME as NAME
     FROM mappedCCGs
     LEFT JOIN (
             SELECT
             row_number() over (partition by rd.OrganisationId order by case when rd.EndDate is null then 1 else 0 end desc, rd.EndDate desc) as RN,
             rd.OrganisationId as ORG_CODE,
             Name as NAME
  
         FROM $reference_data.ODSAPIRoleDetails rd
         LEFT JOIN $reference_data.ODSAPIOrganisationDetails od
         ON rd.OrganisationID = od.OrganisationID and rd.DateType = od.DateType
         
        WHERE 
          CASE WHEN $end_month_id > 1467 THEN (rd.RoleId = 'RO319' and rd.PrimaryRole = 0)  
             WHEN $end_month_id <= 1467 THEN (rd.RoleId = 'RO98' and rd.PrimaryRole = 1)
             END
             
         AND (rd.EndDate >= add_months('$rp_enddate', 1) OR ISNULL(rd.EndDate))
         AND Name NOT LIKE '%HUB'
         AND Name NOT LIKE '%NATIONAL%' 
         AND Name NOT LIKE '%ENTITY%'
         AND rd.StartDate <= '$rp_enddate'
             ) od1
             ON mappedCCGs.ORG_CODE = od1.ORG_CODE
             AND od1.RN = 1
                 
 UNION
  
 SELECT 'UNKNOWN' AS original_ORG_CODE,
        'UNKNOWN' AS original_NAME,
        'UNKNOWN' AS ORG_CODE,
        'UNKNOWN' AS NAME
           
 ORDER BY original_ORG_CODE;

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
                           AND GMPReg NOT IN ('V81999','V81998','V81997')
                           --AND OrgIDGPPrac <> '-1' --CCG methodology change from TP - change made by DC
                           AND EndDateGMPRegistration is NULL
                  GROUP BY Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GP.UniqMonthID = '$end_month_id'
        --AND MPI.PatMRecInRP = TRUE 
        AND GMPReg NOT IN ('V81999','V81998','V81997')
        --AND OrgIDGPPrac <> '-1' 
        AND EndDateGMPRegistration is null

# COMMAND ----------

 %sql
 --Under month_id = 1472 this table returns rows
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PREP AS
      SELECT a.Person_ID,
             CASE
               WHEN UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice
               WHEN UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP 
               WHEN UNIQMONTHID <= 1467 then OrgIDCCGRes 
               WHEN UNIQMONTHID > 1467 then OrgIDSubICBLocResidence
               ELSE 'ERROR'
               END as IC_Rec_CCG ---> this column name is replaced by IC_REC_GP_RESC see code below, but this name is persisted for now as this column name is used in many group by clauses 
               --END AS IC_REC_GP_RES
        FROM $db_source.MHS001MPI a
   LEFT JOIN global_temp.CCG_PRAC c 
             ON a.Person_ID = c.Person_ID 
             AND a.RecordNumber = c.RecordNumber
       WHERE a.UniqMonthID = '$end_month_id' 
             AND a.PatMRecInRP = true

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_ccg_in_month
  
 SELECT Person_ID
 ,CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS IC_Rec_CCG
 ,CASE WHEN NAME IS null THEN 'UNKNOWN' ELSE NAME END AS NAME
  
 FROM global_temp.CCG_PREP a
  
 LEFT JOIN $db_output.bbrb_ccg_latest b 
         ON a.IC_Rec_CCG = b.original_ORG_CODE

# COMMAND ----------

 %sql 
 INSERT OVERWRITE TABLE $db_output.MHS001MPI_latest_month_data 
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
            ,MPI.OrgIDCCGRes --SubICB Residence isnt included because it would mean a changeto table structure and not used in the notebook.
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
  LEFT JOIN $db_output.bbrb_ccg_in_month CCG
            ON MPI.Person_ID = CCG.Person_ID
      WHERE UniqMonthID = '$end_month_id'
            AND MPI.PatMRecInRP = true)

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW CCG_year_prep AS
 SELECT DISTINCT    a.Person_ID,
                    max(a.RecordNumber) as recordnumber                
 FROM               $db_source.MHS001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null                
 LEFT JOIN          RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST d on a.OrgIDSubICBLocResidence = d.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST f on b.OrgIDSubICBLocGP = f.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null
                     or d.ORG_CODE is not null or f.ORG_CODE is not null)
                    and a.uniqmonthid between '$start_month_id' and '$end_month_id'        
 GROUP BY           a.Person_ID

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW CCG_quarter_prep AS
 SELECT DISTINCT    a.Person_ID,
                    max(a.RecordNumber) as recordnumber                
 FROM               $db_source.MHS001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null                
 LEFT JOIN          RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST d on a.OrgIDSubICBLocResidence = d.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST f on b.OrgIDSubICBLocGP = f.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null
                     or d.ORG_CODE is not null or f.ORG_CODE is not null)
                    and a.uniqmonthid between '$end_month_id'-2 and '$end_month_id'        
 GROUP BY           a.Person_ID

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_ccg_in_quarter
 select distinct    a.Person_ID,
                    CASE 
                         WHEN a.UniqMonthID <= 1467 and b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
                         WHEN a.UniqMonthID > 1467 and b.OrgIDSubICBLocGP IS NOT NULL and g.ORG_CODE is not null THEN b.OrgIDSubICBLocGP
                         WHEN a.UniqMonthID <= 1467 and a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
                         WHEN a.UniqMonthID > 1467 and a.OrgIDSubICBLocResidence IS NOT NULL and f.ORG_CODE is not null THEN a.OrgIDSubICBLocResidence
                         ELSE 'UNKNOWN' END AS SubICBGPRes, 
                    CASE 
                         WHEN a.UniqMonthID <= 1467 and b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN e.NAME
                         WHEN a.UniqMonthID > 1467 and b.OrgIDSubICBLocGP IS NOT NULL and g.ORG_CODE is not null THEN g.NAME
                         WHEN a.UniqMonthID <= 1467 and a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN c.NAME
                         WHEN a.UniqMonthID > 1467 and a.OrgIDSubICBLocResidence IS NOT NULL and f.ORG_CODE is not null THEN f.Name
                         ELSE 'UNKNOWN' END AS NAME    
 FROM               $db_source.mhs001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    --and b.OrgIDGPPrac <> '-1' 
                    and b.EndDateGMPRegistration is null
 INNER JOIN         CCG_quarter_prep ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST f on a.OrgIDSubICBLocResidence  = f.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST g on b.OrgIDSubICBLocGP  = g.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or f.ORG_CODE is not null or g.ORG_CODE is not null)
                    and a.uniqmonthid between ('$end_month_id'-2) and '$end_month_id'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_ccg_in_year
 select distinct    a.Person_ID,
                    CASE 
                         WHEN a.UniqMonthID <= 1467 and b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
                         WHEN a.UniqMonthID > 1467 and b.OrgIDSubICBLocGP IS NOT NULL and g.ORG_CODE is not null THEN b.OrgIDSubICBLocGP
                         WHEN a.UniqMonthID <= 1467 and a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
                         WHEN a.UniqMonthID > 1467 and a.OrgIDSubICBLocResidence IS NOT NULL and f.ORG_CODE is not null THEN a.OrgIDSubICBLocResidence
                         ELSE 'UNKNOWN' END AS SubICBGPRes, 
                    CASE 
                         WHEN a.UniqMonthID <= 1467 and b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN e.NAME
                         WHEN a.UniqMonthID > 1467 and b.OrgIDSubICBLocGP IS NOT NULL and g.ORG_CODE is not null THEN g.NAME
                         WHEN a.UniqMonthID <= 1467 and a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN c.NAME
                         WHEN a.UniqMonthID > 1467 and a.OrgIDSubICBLocResidence IS NOT NULL and f.ORG_CODE is not null THEN f.Name
                         ELSE 'UNKNOWN' END AS NAME    
 FROM               $db_source.mhs001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    --and b.OrgIDGPPrac <> '-1' 
                    and b.EndDateGMPRegistration is null
 INNER JOIN         CCG_year_prep ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST f on a.OrgIDSubICBLocResidence  = f.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST g on b.OrgIDSubICBLocGP  = g.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or f.ORG_CODE is not null or g.ORG_CODE is not null)
                    and a.uniqmonthid between ('$end_month_id'-11) and '$end_month_id'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.mhs001mpi_12_months_data
 SELECT 
 MPI.UniqMonthID
 ,MPI.Person_ID
 ,MPI.OrgIDProv
 ,OD.Name as OrgIDProvName
 ,MPI.RecordNumber
 ,MPI.PatMRecInRP
 ,MPI.NHSDEthnicity
 ,COALESCE(ETH.LowerEthnicityCode,'UNKNOWN') AS LowerEthnicity
 ,COALESCE(ETH.LowerEthnicityName,'UNKNOWN') AS LowerEthnicity_Desc
 ,COALESCE(ETH.UpperEthnicity,'UNKNOWN') AS UpperEthnicity
 ,COALESCE(ETH.WNWEthnicity,'UNKNOWN') AS WNW_Ethnicity
 ,MPI.Gender
 ,MPI.GenderIDCode
 ,COALESCE(GEN.Der_Gender, 'UNKNOWN') AS Der_Gender
 ,COALESCE(GEN.Der_Gender_Desc, 'UNKNOWN') AS Der_Gender_Desc 
 ,MPI.AgeRepPeriodEnd
 ,AB.Age_Group_IPS as Age_Band
 ,AB.Age_Group_CYP
 ,COALESCE(DEC.IMD_Decile,'UNKNOWN') AS IMD_Decile
 ,COALESCE(DEC.IMD_Quintile,'UNKNOWN') AS IMD_Quintile
 ,COALESCE(DEC.IMD_Core20,'UNKNOWN') AS IMD_Core20
  
 FROM $db_source.MHS001MPI MPI
 LEFT JOIN $reference_data.ENGLISH_INDICES_OF_DEP_V02 IMD 
                     on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
                     and IMD.imd_year = '2019'
 LEFT JOIN $db_output.bbrb_org_daily_past_12_months_mhsds_providers OD on MPI.OrgIDProv = OD.ORG_CODE
 LEFT JOIN $db_output.ethnicity_desc ETH on MPI.NHSDEthnicity = ETH.LowerEthnicityCode and '$end_month_id' >= ETH.FirstMonth and (ETH.LastMonth is null or '$end_month_id' <= ETH.LastMonth)
 LEFT JOIN $db_output.imd_desc DEC on IMD.DECI_IMD = DEC.IMD_Number and '$end_month_id' >= DEC.FirstMonth and (DEC.LastMonth is null or '$end_month_id' <= DEC.LastMonth)
 LEFT JOIN $db_output.age_band_desc AB on MPI.AgeRepPeriodEnd = AB.AgeRepPeriodEnd and '$end_month_id' >= AB.FirstMonth and (AB.LastMonth is null or '$end_month_id' <= AB.LastMonth)
 LEFT JOIN $db_output.gender_desc GEN on 
       CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode 
       WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender 
       ELSE 'UNKNOWN' END = GEN.Der_Gender
       
 WHERE MPI.UniqMonthID between '$start_month_id' and '$end_month_id'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.ServiceTeamType
 ----MHS102 All
 SELECT
 s.UniqMonthID,
 s.OrgIDProv,
 s.Person_ID,
 s.UniqServReqID,
 COALESCE(s.UniqCareProfTeamID, s.UniqOtherCareProfTeamLocalID) as UniqCareProfTeamID,
 s.ServTeamTypeRefToMH,
 s.ServTeamIntAgeGroup,
 s.ReferClosureDate,
 s.ReferClosureTime,
 s.ReferClosReason,
 s.ReferRejectionDate,
 s.ReferRejectionTime,
 s.ReferRejectReason,
 s.RecordNumber,
 s.RecordStartDate,
 s.RecordEndDate
 from $db_source.mhs102otherservicetype s
 UNION ALL
 ----MHS101 v6
 SELECT
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.UniqServReqID,
 r.UniqCareProfTeamLocalID as UniqCareProfTeamID, 
 r.ServTeamType as ServTeamTypeRefToMH,
 r.ServTeamIntAgeGroup,
 r.ServDischDate as ReferClosureDate,
 r.ServDischTime as ReferClosureTime,
 r.ReferClosReason,
 r.ReferRejectionDate,
 r.ReferRejectionTime,
 r.ReferRejectReason,
 r.RecordNumber,
 r.RecordStartDate,
 r.RecordEndDate
 from $db_source.mhs101referral r
 where UniqMonthID > 1488