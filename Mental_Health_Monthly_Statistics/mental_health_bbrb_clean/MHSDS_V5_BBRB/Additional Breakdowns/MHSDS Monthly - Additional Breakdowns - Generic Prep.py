# Databricks notebook source
 %md
 #MHSDS Monthly - Additional Breakdowns
 ##Generic Preparation Notebook

# COMMAND ----------

 %md
 ##Generic Preparation

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_rd_ccg_latest;
  
 CREATE TABLE $db_output.tmp_MHMAB_RD_CCG_LATEST USING DELTA AS
 SELECT DISTINCT ORG_CODE,
                 NAME
            FROM $corporate_ref.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%';
               
 OPTIMIZE $db_output.tmp_MHMAB_RD_CCG_LATEST

# COMMAND ----------

 %sql
  
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
      SELECT a.Person_ID
      ,CASE WHEN UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice                 
      WHEN UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP
      WHEN UNIQMONTHID <= 1467 then OrgIDCCGRes                 
      WHEN UNIQMONTHID > 1467 then OrgIDSubICBLocResidence                 
      ELSE 'ERROR' END AS IC_REC_GP_RES       
      FROM $db_source.MHS001MPI a
   LEFT JOIN global_temp.CCG_PRAC c 
             ON a.Person_ID = c.Person_ID 
             AND a.RecordNumber = c.RecordNumber
       WHERE a.UniqMonthID = '$end_month_id' 
             AND a.PatMRecInRP = true

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.tmp_mhmab_ccg;
  
 CREATE TABLE $db_output.tmp_MHMAB_CCG USING DELTA AS
     SELECT a.Person_ID,
            CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS  IC_REC_GP_RES,
            CASE WHEN b.NAME IS null THEN 'UNKNOWN' ELSE b.NAME END AS NAME
       FROM global_temp.CCG_PREP a
  LEFT JOIN $db_output.tmp_MHMAB_RD_CCG_LATEST b 
            ON a.IC_REC_GP_RES = b.ORG_CODE; 
 
 OPTIMIZE $db_output.tmp_MHMAB_CCG

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs003accommstatus_latest_month_data;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs003accommstatus_latest_month_data USING DELTA AS
     (
     SELECT  *
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY AccommodationTypeDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY AccommodationTypeDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS003AccommStatus
      WHERE  AccommodationTypeDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
             AND  AccommodationType IS NOT NULL
             AND UniqMonthID <= '$end_month_id'
   ORDER BY  Person_ID)

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs004empstatus_latest_month_data;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs004empstatus_latest_month_data USING DELTA AS
     (
 SELECT  *
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY EmployStatusRecDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY EmployStatusRecDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS004EmpStatus
      WHERE  EmployStatusRecDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
             AND EmployStatus IS NOT NULL
             AND UniqMonthID <= '$end_month_id'
   ORDER BY  Person_ID)

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs011socpercircircumstance_latest_month_data;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs011socpercircircumstance_latest_month_data USING DELTA AS
     (
 SELECT 
 PERSON_ID, 
 ORGIDPROV, 
 SocPerCircumstance,
 SocPerCircumstanceRecTimestamp
 ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY uniqmonthid desc, SocPerCircumstanceRecTimestamp DESC, CASE WHEN SocPerCircumstance in ('699042003','440583007') THEN 2 else 1 end asc) AS RANK
 ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY uniqmonthid desc, SocPerCircumstanceRecTimestamp DESC) AS PROV_RANK
 FROM 
 $db_source.MHS011SocPerCircumstances A
 INNER JOIN $corporate_ref.SNOMED_SCT2_REFSET_FULL  B ON a.SocPerCircumstance = ReferencedComponentID and ACTIVE = '1' AND REFSETID = '999003081000000103'                                                     
 INNER JOIN (SELECT $corporate_ref, MAX(EFFECTIVETIME) AS EFFECTIVETIME FROM DSS_CORPORATE.SNOMED_SCT2_REFSET_FULL WHERE REFSETID = '999003081000000103' GROUP BY REFERENCEDCOMPONENTID) C ON B.EFFECTIVETIME = C.EFFECTIVETIME
 WHERE
 UNIQMONTHID <= '$end_month_id') 

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs001mpi_latest_month_data;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs001mpi_latest_month_data USING DELTA AS
     (SELECT MPI.AgeDeath 
            ,MPI.AgeRepPeriodEnd
            ,case when MPI.AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                      when MPI.AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                      when MPI.AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                      when MPI.AgeRepPeriodEnd = 16 then '16'
                      when MPI.AgeRepPeriodEnd = 17 then '17'
                      when MPI.AgeRepPeriodEnd = 18 then '18'
                      when MPI.AgeRepPeriodEnd = 19 then '19'
                      when MPI.AgeRepPeriodEnd between 20 and 24 then '20 to 24'
                      when MPI.AgeRepPeriodEnd between 25 and 29 then '25 to 29'
                      when MPI.AgeRepPeriodEnd between 30 and 34 then '30 to 34'
                      when MPI.AgeRepPeriodEnd between 35 and 39 then '35 to 39'
                      when MPI.AgeRepPeriodEnd between 40 and 44 then '40 to 44'
                      when MPI.AgeRepPeriodEnd between 45 and 49 then '45 to 49'
                      when MPI.AgeRepPeriodEnd between 50 and 54 then '50 to 54'
                      when MPI.AgeRepPeriodEnd between 55 and 59 then '55 to 59'
                      when MPI.AgeRepPeriodEnd between 60 and 64 then '60 to 64'
                      when MPI.AgeRepPeriodEnd between 65 and 69 then '65 to 69'
                      when MPI.AgeRepPeriodEnd between 70 and 74 then '70 to 74'
                      when MPI.AgeRepPeriodEnd between 75 and 79 then '75 to 79'
                      when MPI.AgeRepPeriodEnd between 80 and 84 then '80 to 84'
                      when MPI.AgeRepPeriodEnd between 85 and 89 then '85 to 89'
                      when MPI.AgeRepPeriodEnd >= '90' then '90 or over' else 'UNKNOWN' end AS Age_Band 
            ,MPI.AgeRepPeriodStart            
            ,MPI.County 
            ,MPI.DefaultPostcode 
            ,MPI.ElectoralWard 
            ,MPI.EthnicCategory 
            ,MPI.Gender
            ,MPI.GenderIDCode
            ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode
                  WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender
                  ELSE 'UNKNOWN' END AS Der_Gender 
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
            ,CASE WHEN MPI.NHSDEthnicity = 'A' THEN 'A'
                  WHEN MPI.NHSDEthnicity = 'B' THEN 'B'
                  WHEN MPI.NHSDEthnicity = 'C' THEN 'C'
                  WHEN MPI.NHSDEthnicity = 'D' THEN 'D'
                  WHEN MPI.NHSDEthnicity = 'E' THEN 'E'
                  WHEN MPI.NHSDEthnicity = 'F' THEN 'F'
                  WHEN MPI.NHSDEthnicity = 'G' THEN 'G'
                  WHEN MPI.NHSDEthnicity = 'H' THEN 'H'
                  WHEN MPI.NHSDEthnicity = 'J' THEN 'J'
                  WHEN MPI.NHSDEthnicity = 'K' THEN 'K'
                  WHEN MPI.NHSDEthnicity = 'L' THEN 'L'
                  WHEN MPI.NHSDEthnicity = 'M' THEN 'M'
                  WHEN MPI.NHSDEthnicity = 'N' THEN 'N'
                  WHEN MPI.NHSDEthnicity = 'P' THEN 'P'
                  WHEN MPI.NHSDEthnicity = 'R' THEN 'R'
                  WHEN MPI.NHSDEthnicity = 'S' THEN 'S'
                  WHEN MPI.NHSDEthnicity = 'Z' THEN 'Z'
                  WHEN MPI.NHSDEthnicity = '99' THEN '99'
                  ELSE 'UNKNOWN' END AS LowerEthnicity
             ,CASE WHEN MPI.NHSDEthnicity = 'A' THEN 'British'
                   WHEN MPI.NHSDEthnicity = 'B' THEN 'Irish'
                   WHEN MPI.NHSDEthnicity = 'C' THEN 'Any Other White Background'
                   WHEN MPI.NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
                   WHEN MPI.NHSDEthnicity = 'E' THEN 'White and Black African'
                   WHEN MPI.NHSDEthnicity = 'F' THEN 'White and Asian'
                   WHEN MPI.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
                   WHEN MPI.NHSDEthnicity = 'H' THEN 'Indian'
                   WHEN MPI.NHSDEthnicity = 'J' THEN 'Pakistani'
                   WHEN MPI.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                   WHEN MPI.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
                   WHEN MPI.NHSDEthnicity = 'M' THEN 'Caribbean'
                   WHEN MPI.NHSDEthnicity = 'N' THEN 'African'
                   WHEN MPI.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
                   WHEN MPI.NHSDEthnicity = 'R' THEN 'Chinese'
                   WHEN MPI.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                   WHEN MPI.NHSDEthnicity = 'Z' THEN 'Not Stated'
                   WHEN MPI.NHSDEthnicity = '99' THEN 'Not Known' 
                   ELSE 'UNKNOWN' END AS LowerEthnicity_Desc         
            ,MPI.NHSNumber 
            ,MPI.NHSNumberStatus 
            ,MPI.OrgIDCCGRes 
            ,MPI.OrgIDEduEstab 
            ,MPI.OrgIDLocalPatientId 
            ,MPI.OrgIDProv 
            ,MPI.OrgIDResidenceResp 
            ,MPI.PatMRecInRP 
            ,MPI.Person_ID 
            ,MPI.PostcodeDistrict 
            ,MPI.RecordEndDate 
            ,MPI.RecordNumber 
            ,MPI.RecordStartDate 
            ,MPI.RowNumber 
            ,MPI.UniqMonthID 
            ,MPI.UniqSubmissionID           
            --,CCG.IC_Rec_CCG
            ,CCG.IC_REC_GP_RES
            ,CCG.NAME
            ,CASE
                 WHEN IMD.DECI_IMD = 10 THEN '10 Least deprived'
                 WHEN IMD.DECI_IMD = 9 THEN '09 Less deprived'
                 WHEN IMD.DECI_IMD = 8 THEN '08 Less deprived'
                 WHEN IMD.DECI_IMD = 7 THEN '07 Less deprived'
                 WHEN IMD.DECI_IMD = 6 THEN '06 Less deprived'
                 WHEN IMD.DECI_IMD = 5 THEN '05 More deprived'
                 WHEN IMD.DECI_IMD = 4 THEN '04 More deprived'
                 WHEN IMD.DECI_IMD = 3 THEN '03 More deprived'
                 WHEN IMD.DECI_IMD = 2 THEN '02 More deprived'
                 WHEN IMD.DECI_IMD = 1 THEN '01 Most deprived'
                 ELSE 'UNKNOWN' 
                 END AS IMD_Decile
            ,CASE WHEN ACC.AccommodationType IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','98') THEN ACC.AccommodationType
                  ELSE 'UNKNOWN' END AS AccommodationType 
            ,CASE WHEN acc.AccommodationType = '01' THEN 'Owner occupier'
                  WHEN acc.AccommodationType = '02'	THEN 'Tenant - Local Authority/Arms Length Management Organisation/registered social housing provider'
                  WHEN acc.AccommodationType = '03'	THEN 'Tenant - private landlord'
                  WHEN acc.AccommodationType = '04'	THEN 'Living with family'
                  WHEN acc.AccommodationType = '05'	THEN 'Living with friends'
                  WHEN acc.AccommodationType = '06'	THEN 'University or College accommodation'
                  WHEN acc.AccommodationType = '07'	THEN 'Accommodation tied to job (including Armed Forces)'
                  WHEN acc.AccommodationType = '08'	THEN 'Mobile accommodation' 
                  WHEN acc.AccommodationType = '09'	THEN 'Care home without nursing'
                  WHEN acc.AccommodationType = '10'	THEN 'Care home with nursing'
                  WHEN acc.AccommodationType = '11'	THEN 'Specialist Housing (with suitable adaptations to meet impairment needs and support to live independently)'
                  WHEN acc.AccommodationType = '12'	THEN 'Rough sleeper'
                  WHEN acc.AccommodationType = '13'	THEN 'Squatting'
                  WHEN acc.AccommodationType = '14'	THEN 'Sofa surfing (sleeps on different friends floor each night)'
                  WHEN acc.AccommodationType = '15'	THEN 'Staying with friends/family as a short term guest'
                  WHEN acc.AccommodationType = '16'	THEN 'Bed and breakfast accommodation to prevent or relieve homelessness'
                  WHEN acc.AccommodationType = '17'	THEN 'Sleeping in a night shelter'
                  WHEN acc.AccommodationType = '18'	THEN 'Hostel to prevent or relieve homelessness'
                  WHEN acc.AccommodationType = '19'	THEN 'Temporary housing to prevent or relieve homelessness'
                  WHEN acc.AccommodationType = '20'	THEN 'Admitted patient settings'
                  WHEN acc.AccommodationType = '21'	THEN 'Criminal justice settings'
                  WHEN acc.AccommodationType = '98'	THEN 'Other (not listed)'
                  ELSE 'UNKNOWN' END AS AccommodationType_Desc 
            ,CASE WHEN EMP.employstatus in ('01','02','03','04','05','06','07','08','ZZ') then EMP.employstatus
                  ELSE 'UNKNOWN' END as EmployStatus 
            ,CASE WHEN EMP.employstatus = '01' THEN 'Employed'
                  WHEN EMP.employstatus = '02'	THEN 'Unemployed and actively seeking work'
                  WHEN EMP.employstatus = '03'	THEN 'Undertaking full (at least 16 hours per week) or part-time (less than 16 hours per week) education or training as a student and not working or actively seeking work'
                  WHEN EMP.employstatus = '04'	THEN 'Long-term sick or disabled, those receiving government sickness and disability benefits'
                  WHEN EMP.employstatus = '05'	THEN 'Looking after the family or home as a homemaker and not working or actively seeking work'
                  WHEN EMP.employstatus = '06'	THEN 'Not receiving government sickness and disability benefits and not working or actively seeking work'
                  WHEN EMP.employstatus = '07'	THEN 'Unpaid voluntary work and not working or actively seeking work'
                  WHEN EMP.employstatus = '08'	THEN 'Retired'
                  WHEN EMP.employstatus = 'ZZ'	THEN 'Not Stated (PERSON asked but declined to provide a response)'
                  ELSE 'UNKNOWN' END AS EmployStatus_Desc 
            ,CASE WHEN DIS.DisabCode IN ('01','02','03','04','05','06','07','08','09','10','XX','NN','ZZ') THEN DIS.DisabCode
                  ELSE 'UNKNOWN' END AS DisabCode 
            ,CASE WHEN DIS.DisabCode = '01' THEN	'Behaviour and Emotional'
                  WHEN DIS.DisabCode = '02' THEN	'Hearing'
                  WHEN DIS.DisabCode = '03' THEN	'Manual Dexterity'
                  WHEN DIS.DisabCode = '04' THEN	'Memory or ability to concentrate, learn or understand (Learning Disability)'
                  WHEN DIS.DisabCode = '05' THEN	'Mobility and Gross Motor'
                  WHEN DIS.DisabCode = '06' THEN	'Perception of Physical Danger'
                  WHEN DIS.DisabCode = '07' THEN	'Personal, Self Care and Continence'
                  WHEN DIS.DisabCode = '08' THEN	'Progressive Conditions and Physical Health (such as HIV, cancer, multiple sclerosis, fits etc)'
                  WHEN DIS.DisabCode = '09' THEN	'Sight'
                  WHEN DIS.DisabCode = '10' THEN	'Speech'
                  WHEN DIS.DisabCode = 'XX' THEN	'Other (not listed)'
                  WHEN DIS.DisabCode = 'NN' THEN	'No Disability'
                  WHEN DIS.DisabCode = 'ZZ' THEN	'Not Stated (Person asked but declined to provide a response)'
                  ELSE 'UNKNOWN' END AS DisabCode_Desc 
            ,CASE WHEN SOC.SocPerCircumstance = '765288000' THEN 'Asexual (not sexually attracted to either gender)'
                  WHEN SOC.SocPerCircumstance = '20430005' THEN 'Heterosexual or Straight'
                  WHEN SOC.SocPerCircumstance = '1064711000000108' THEN 'Person asked and does not know or is not sure'
                  WHEN SOC.SocPerCircumstance IN ('729951000000104','699042003') THEN 'Not Stated (Person asked but declined to provide a response)'
                  WHEN SOC.SocPerCircumstance = '440583007' THEN 'Not known (not recorded)'
                  WHEN SOC.SocPerCircumstance = '89217008' THEN 'Gay or Lesbian'
                  WHEN SOC.SocPerCircumstance = '76102007' THEN 'Gay or Lesbian'
                  WHEN SOC.SocPerCircumstance = '42035005' THEN 'Bisexual'
                  WHEN SOC.SocPerCircumstance = '472985009' THEN 'Asexual (not sexually attracted to either gender)'
                  ELSE 'UNKNOWN' END AS Sex_Orient 
 FROM $db_source.MHS001MPI MPI
 LEFT JOIN $db_output.tmp_MHMAB_CCG CCG
            ON MPI.Person_ID = CCG.Person_ID
 LEFT JOIN dss_corporate.ENGLISH_INDICES_OF_DEP_V02 IMD 
                     on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
                     and IMD.imd_year = '2019'
 LEFT JOIN $db_output.tmp_mhmab_mhs003accommstatus_latest_month_data ACC on MPI.Person_ID = ACC.Person_ID and MPI.PatMRecInRP = TRUE and ACC.RANK = 1
 LEFT JOIN $db_output.tmp_mhmab_mhs004empstatus_latest_month_data EMP on MPI.Person_ID = EMP.Person_ID and MPI.PatMRecInRP = TRUE and EMP.RANK = 1
 LEFT JOIN $db_source.MHS007DisabilityType DIS on MPI.Person_ID = DIS.Person_ID and DIS.UniqMonthID = '$end_month_id' and MPI.OrgIDProv = DIS.OrgIDProv
 LEFT JOIN $db_output.tmp_mhmab_mhs011socpercircircumstance_latest_month_data SOC on MPI.Person_ID = SOC.Person_ID and MPI.PatMRecInRP = TRUE and SOC.RANK = 1
 WHERE MPI.UniqMonthID = '$end_month_id'
 AND MPI.PatMRecInRP = true)    

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW REFS AS 
  
 SELECT 
 DISTINCT 
 a.UniqServReqID,
 a.Person_ID,
 CASE 
             WHEN AgeServReferRecDate BETWEEN 3 AND 17 THEN 'Under 18'  
             WHEN AgeServReferRecDate >=18 THEN '18 or over'
             ELSE 'UNKNOWN'
 END AS AgeCat,
 s.ServTeamTypeRefToMH AS TeamType,
 s.UniqCareProfTeamID
 FROM
 $db_source.MHS101REFERRAL A 
 LEFT JOIN $db_source.MHS102SERVICETYPEREFERREDTO S ON A.UNIQSERVREQID = S.UNIQSERVREQID AND A.PERSON_ID = S.PERSON_ID AND S.UNIQMONTHID = '$end_month_id'
 WHERE 
 A.UniqMonthID = '$end_month_id'

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW TEAMTYPE AS 
  
 SELECT
 r.UniqCareProfTeamID,
 SUM(CASE WHEN r.UniqServReqID IS NOT NULL THEN 1 ELSE 0 END) AS TotalRefs,
 SUM(CASE WHEN r.AgeCat = 'Under 18' THEN 1 ELSE 0 END) AS TotalU18Refs,
 (SUM(CASE WHEN r.AgeCat = 'Under 18' THEN 1 ELSE 0 END) / SUM(CASE WHEN r.UniqServReqID IS NOT NULL THEN 1 ELSE 0 END)) *100 AS PRCNT_U18
             
 FROM global_temp.REFS r
  
 GROUP BY r.UniqCareProfTeamID

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list AS
     SELECT  DISTINCT CASE
                           WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN NULL
                           WHEN TreatFuncCodeMH = '700' THEN NULL
                           WHEN WardType = '05' THEN NULL 
                           
                           WHEN TreatFuncCodeMH = '711' THEN 'Y'
                           WHEN WardType IN ('01', '02') THEN 'Y'
                           WHEN WardAge IN ('10','11','12') THEN 'Y'
                           
                           WHEN PRCNT_U18 > 50 THEN 'Y'
                           
                           ELSE NULL END AS CAMHS
                           
             ,CASE WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
                   WHEN TreatFuncCodeMH = '700' THEN 'Y'
                   WHEN WardType = '05' THEN 'Y'
                   ELSE NULL END AS LD
                   
             ,CASE WHEN WardType IN ('01', '02', '05') THEN NULL 
                   WHEN IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
                   WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
                   WHEN WardAge IN ('10', '11', '12') THEN NULL
                   
                   WHEN PRCNT_U18 > 50 THEN NULL
                   
                   WHEN WardAge IN ('13', '14', '15') THEN 'Y'
                   WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53') THEN 'Y'
                   WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
                   WHEN WardType IN ('03', '04', '06') THEN 'Y'
                   
                   WHEN PRCNT_U18 <= 50 THEN 'Y'
                   
                   ELSE 'Y' END AS MH
                   
             ,UniqWardStayID
             
       FROM    $db_source.MHS001MPI AS A
 INNER JOIN  $db_source.MHS502WardStay AS B
             ON A.Person_ID = B.Person_ID 
             AND B.UniqMonthID = '$end_month_id'  
  LEFT JOIN  $db_source.MHS503AssignedCareProf AS C
             ON B.UniqHospProvSpellID = C.UniqHospProvSpellID 
             AND C.UniqMonthID = '$end_month_id' 
             AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf > '$rp_enddate')
  LEFT JOIN  $db_source.MHS102ServiceTypeReferredTo AS D
             ON B.UniqServReqID = D.UniqServReqID 
             AND D.UniqMonthID = '$end_month_id' 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate'))
             
  LEFT JOIN global_temp.TEAMTYPE AS Z 
             ON D.UniqCareProfTeamID = Z.UniqCareProfTeamID
  
      WHERE    A.UniqMonthID = '$end_month_id'
             AND A.PatMRecInRP = true
             AND (EndDateWardStay IS NULL OR EndDateWardStay > '$rp_enddate')

# COMMAND ----------

 %sql
  
 --CREATES A DISTINCT VERSION OF THE SERVICE AREA BREAKDOWNS
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_stay_cats AS
 SELECT  DISTINCT UniqWardStayID
                  ,MIN (LD) AS LD
                  ,MIN (CAMHS) AS CAMHS
                  ,MIN (MH) AS MH
   FROM  global_temp.ward_type_list
 GROUP BY    UniqWardStayID

# COMMAND ----------

 %sql
  
 /**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list AS
     SELECT    CASE WHEN F.LD = 'Y' THEN 'Y'
                  WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN 'Y'
                  ELSE NULL END AS LD
                  
             ,CASE WHEN F.CAMHS = 'Y' THEN 'Y'
                   WHEN F.LD = 'Y' THEN NULL
                   WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN NULL
                   WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07') THEN 'Y'
                   WHEN PRCNT_U18 > 50 THEN 'Y'               
                   ELSE NULL END AS CAMHS
                 
             ,CASE 
                   WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01', 'C05', 'C06', 'C07') THEN NULL 
                   WHEN F.LD = 'Y' THEN NULL 
                   WHEN F.CAMHS = 'Y' THEN NULL 
                   WHEN F.MH = 'Y' THEN 'Y'        
                   WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
                   WHEN PRCNT_U18 > 50 THEN NULL
                   WHEN ServTeamTypeRefToMH IN ('A01', 'A02', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'A21', 'A22', 'A23', 'A24', 'A25', 'B01', 'C02', 'C04', 'C08', 'C10', 'D01', 'D02', 'D03', 'D04', 'D06', 'D07', 'D08', 'Z01', 'Z02') THEN 'Y' 
                   WHEN PRCNT_U18 <= 50 THEN 'Y'
                   ELSE 'Y' END AS MH
                   
             ,D.UniqServReqID
             
       FROM    $db_source.MHS101Referral AS D 
  LEFT JOIN  $db_source.MHS001MPI AS A
             ON A.Person_ID = D.Person_ID 
             AND A.PatMRecInRP = 'Y'
             AND A.UniqMonthID = '$end_month_id'
  LEFT JOIN  $db_source.MHS501HospProvSpell AS G
             ON G.UniqServReqID = D.UniqServReqID 
             AND G.UniqMonthID = '$end_month_id'
  LEFT JOIN  $db_source.MHS502WardStay AS B
             ON G.UniqHospProvSpellID = B.UniqHospProvSpellID 
             AND B.UniqMonthID = '$end_month_id' 
  LEFT JOIN  global_temp.ward_stay_cats AS F
             ON B.UniqWardStayID = F.UniqWardStayID
  LEFT JOIN  $db_source.MHS102ServiceTypeReferredTo AS E
             ON D.UniqServReqID = E.UniqServReqID 
             AND E.UniqMonthID = '$end_month_id' 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate'))
             
   LEFT JOIN global_temp.TEAMTYPE AS Z 
             ON E.UniqCareProfTeamID = Z.UniqCareProfTeamID
  
      WHERE    (D.ServDischDate IS NULL OR D.ServDischDate >'$rp_enddate')
             AND D.UniqMonthID = '$end_month_id'

# COMMAND ----------

 %sql
  
 --CREATES A DISTINCT VERSION OF THE SERVICE AREA BREAKDOWNS
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_cats AS
 SELECT  DISTINCT UniqServReqID
         ,MIN (LD) AS LD
         ,MIN (CAMHS) AS CAMHS
         ,MIN (MH) AS MH
   FROM    global_temp.referral_list
 GROUP BY    UniqServReqID

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS101Referral_service_area AS
 SELECT  A.*
         ,B.CAMHS
         ,B.LD
         ,B.MH
   FROM    $db_source.MHS101Referral AS A
 LEFT JOIN  global_temp.referral_cats AS B
         ON A.UniqServReqID = B.UniqServReqID
  WHERE    (ServDischDate IS NULL OR ServDischDate > '$rp_enddate')
         AND A.UniqMonthID = '$end_month_id'

# COMMAND ----------

 %sql 
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs101referral_open_end_rp;
 CREATE TABLE $db_output.tmp_MHMAB_MHS101Referral_open_end_rp USING DELTA AS
 SELECT
   ref.AgeServReferDischDate,
   ref.AgeServReferRecDate,
   ref.ClinRespPriorityType,
   ref.DischPlanCreationDate,
   ref.DischPlanCreationTime,
   ref.DischPlanLastUpdatedDate,
   ref.DischPlanLastUpdatedTime,
   ref.InactTimeRef,
   ref.ReferralServiceAreasOpenEndRPLDA,
   ref.LocalPatientId,
   ref.MHS101UniqID,
   ref.NHSServAgreeLineNum,
   ref.OrgIDComm,
   ref.OrgIDProv,
   ref.OrgIDReferring,
   ref.Person_ID,
   ref.PrimReasonReferralMH,
   ref.ReasonOAT,
   ref.RecordEndDate,
   ref.RecordNumber,
   ref.RecordStartDate,
   ref.ReferralRequestReceivedDate,
   ref.ReferralRequestReceivedTime,
   ref.ReferringCareProfessionalStaffGroup,
   ref.ServDischDate,
   ref.ServDischTime,
   ref.ServiceRequestId,
   ref.SourceOfReferralMH,
   ref.SpecialisedMHServiceCode,
   ref.UniqMonthID,
   ref.UniqServReqID,
   ref.UniqSubmissionID,case
     when refa.CAMHS = 'Y' then TRUE
     else FALSE
   end AS CYPServiceRefEndRP_temp,case
     when refa.LD = 'Y' then TRUE
     else FALSE
   end AS LDAServiceRefEndRP_temp,case
     when refa.MH = 'Y' then TRUE
     else FALSE
   end AS AMHServiceRefEndRP_temp
 FROM
   $db_source.MHS101Referral AS ref
   INNER JOIN global_temp.MHS101Referral_service_area AS refa ON ref.UniqServReqID = refa.UniqServReqID
 WHERE
   ref.UniqMonthID = '$end_month_id'
   AND (
     ref.ServDischDate IS NULL
     OR ref.ServDischDate > '$rp_enddate'
   )

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs102servicetypereferredto;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS102ServiceTypeReferredTo USING DELTA AS
       SELECT SRV.*              
         FROM $db_source.MHS102ServiceTypeReferredTo AS SRV
        WHERE SRV.UniqMonthID = '$end_month_id'
              AND ((((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate > '$rp_enddate') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate > '$rp_enddate'))) OR SRV.ReferClosureDate <= '$rp_enddate' OR        SRV.ReferRejectionDate <= '$rp_enddate');
              
 OPTIMIZE $db_output.tmp_MHMAB_MHS102ServiceTypeReferredTo

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs501hospprovspell_latest_month_data;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs501hospprovspell_latest_month_data USING DELTA AS
 SELECT HSP.*
       FROM $db_source.MHS501HospProvSpell HSP
      WHERE HSP.UniqMonthID = '$end_month_id'
            AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell > '$rp_enddate') 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output1;
 
 CREATE TABLE IF NOT EXISTS $db_output.output1
  (REPORTING_PERIOD_START      DATE,
   REPORTING_PERIOD_END        DATE,
   STATUS                      STRING,
   BREAKDOWN                   STRING,
   PRIMARY_LEVEL               STRING,
   PRIMARY_LEVEL_DESCRIPTION   STRING,
   SECONDARY_LEVEL             STRING,
   SECONDARY_LEVEL_DESCRIPTION STRING,
   MEASURE_ID                     STRING,
   MEASURE_VALUE               STRING,
   SOURCE_DB                   STRING)
 USING delta

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))