# Databricks notebook source
 %sql
 INSERT INTO $db_output.OAPS_In_Scope
 SELECT ORG_CODE
 FROM $corporate_ref.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
   AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
   AND ORG_CODE in ('NQL','NR5','R1A','R1C','R1F','R1L','RAT','RDY','RGD','RH5','RHA','RJ8','RKL','RLY','RMY','RNN',
                    'RNU','RP1','RP7','RPG','RQ3','RQY','RRE','RRP','RT1','RT2','RT5','RTQ','RTV','RV3','RV5','RV9',
                    'RVN','RW1','RW4','RW5','RWK','RWR','RWV','RWX','RX2','RX3','RX4','RXA','RXE','RXG','RXM','RXT',
                    'RXV','RXX','RXY','RYG','RYK','TAD','TAF','TAH','TAJ')

# COMMAND ----------

 %sql
  
 INSERT INTO $db_output.OAPs_ORG_DAILY
 SELECT *
 FROM $corporate_ref.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
   AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1) 
   AND ORG_CLOSE_DATE <= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE));
   
 OPTIMIZE $db_output.OAPs_ORG_DAILY;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_ORG_RELATIONSHIP_DAILY
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $corporate_ref.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_STP_Region_mapping
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM $db_output.OAPS_ORG_DAILY A
 LEFT JOIN $db_output.OAPS_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.OAPS_ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.OAPS_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.OAPS_ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE A.ORG_TYPE_CODE = 'ST'
   AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

 %sql
 
 INSERT INTO $db_output.OAPs_CCG_List
 SELECT               'CCG - Registration or Residence' as type
                      ,ORG_CODE as level
                            ,NAME as level_description
 FROM                 $db_output.OAPs_ORG_DAILY
 WHERE                (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
                      AND ORG_TYPE_CODE = 'CC'
                      AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
                      AND ORG_OPEN_DATE <= '$rp_enddate'
                      AND NAME NOT LIKE '%HUB'
                      AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Months
 
 select a.*
 from (  select DISTINCT UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate
         from $db_source.MHS000Header as a
         where uniqmonthid between $end_month_id-11 and $end_month_id) AS a

# COMMAND ----------

# DBTITLE 1,Calculate latest Sub ICB - GP Practice or Residence
 %sql
 
 CREATE OR REPLACE TEMPORARY VIEW RD_CCG_LATEST AS
 SELECT DISTINCT ORG_TYPE_CODE, ORG_CODE, NAME
 FROM $corporate_ref.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
   AND BUSINESS_START_DATE <= '$rp_enddate'
     AND ORG_TYPE_CODE = "CC"
       AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
         AND ORG_OPEN_DATE <= '$rp_enddate'
           AND NAME NOT LIKE '%HUB'
             AND NAME NOT LIKE '%NATIONAL%';
 
 CREATE OR REPLACE TEMPORARY VIEW CCG_prep AS
 SELECT DISTINCT    a.Person_ID,
 				   max(a.RecordNumber) as recordnumber                
 FROM               $db_source.MHS001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
 		           on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
 		           and a.recordnumber = b.recordnumber
 		           and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
 		           and b.EndDateGMPRegistration is null                
 LEFT JOIN          RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST d on a.OrgIDSubICBLocResidence = d.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST f on b.OrgIDSubICBLocGP = f.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null
                     or d.ORG_CODE is not null or f.ORG_CODE is not null)
                    and a.uniqmonthid between ('$end_month_id'-11) and '$end_month_id'        
 GROUP BY           a.Person_ID;
 
 INSERT INTO $db_output.OAPS_CCG_LATEST
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
                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                    --and b.OrgIDGPPrac <> '-1' 
                    and b.EndDateGMPRegistration is null
 INNER JOIN         CCG_prep ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST f on a.OrgIDSubICBLocResidence  = f.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 LEFT JOIN          RD_CCG_LATEST g on b.OrgIDSubICBLocGP  = g.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or f.ORG_CODE is not null or g.ORG_CODE is not null)
                    and a.uniqmonthid between ('$end_month_id'-11) and '$end_month_id';

# COMMAND ----------

 %sql
 INSERT INTO $db_output.HospProvSpell
 
 select a.UniqServReqID,
        a.Person_ID,
        a.Uniqmonthid,
        a.OrgIDProv,
        a.UniqHospProvSpellID,
        b.ReportingPeriodStartDate,
        b.ReportingPeriodEndDate,
        a.StartDateHospProvSpell,
        a.DischDateHospProvSpell
 from $db_source.MHS501HospProvSpell as a
 inner join $db_output.Months as b
    on a.UniqMonthID = b.UniqMonthID
   and a.StartDateHospProvSpell <= b.ReportingPeriodEndDate
   and (a.DischDateHospProvSpell is null or a.DischDateHospProvSpell >= b.ReportingPeriodStartDate)
   and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate')  --Added recordenddate to get latest version of record

# COMMAND ----------

 %sql
 INSERT INTO $db_output.WardStay
 
 select a.UniqWardStayID,
        a.HospitalBedTypeMH,
        a.WardType,
        a.BedDaysWSEndRP,
        a.Person_ID,
        a.Uniqmonthid,
        a.UniqHospProvSpellID,
        a.OrgIDProv,
        b.ReportingPeriodStartDate,
        b.ReportingPeriodEndDate,
        a.StartDateWardStay,
        a.EndDateWardStay
 from $db_source.MHS502WardStay as a
 inner join $db_output.Months as b
    on a.UniqMonthID = b.UniqMonthID
   and a.StartDateWardStay <= b.ReportingPeriodEndDate
   and (a.EndDateWardStay is null or a.EndDateWardStay >= b.ReportingPeriodStartDate)
   and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate')  --Added recordenddate to get latest version of record

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_referrals
 select a.UniqServReqId,
        a.ReferralRequestReceivedDate,
        a.Person_ID,
        a.ServDischDate,
        a.uniqmonthid,
        a.ReasonOAT,
        a.PrimReasonReferralMH,
        a.OrgIDProv,
        a.OrgIDReferring
 from $db_source.MHS101Referral as a
 where uniqmonthid between $end_month_id-11 and $end_month_id
   and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate') 
   and ReasonOAT = '10'       

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_onwardreferrals
 select a.UniqServReqID,
        a.Person_ID,
        a.Uniqmonthid,
        a.OrgIDProv,
        a.OATReason
 from $db_source.MHS105OnwardReferral as a
 where uniqmonthid between $end_month_id-11 and $end_month_id
   and OATReason = '10'      
 ;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Overlapping_OAPs_referrals
 select a.UniqServReqId,
        a.ReferralRequestReceivedDate,
        a.Person_ID,
        a.ServDischDate,
        a.uniqmonthid,
        a.ReasonOAT,
        a.PrimReasonReferralMH,
        a.OrgIDProv,
        a.OrgIDReferring,
        CASE WHEN b.NewDate is null THEN a.ServDischDate ELSE b.NewDate END as NewServDischDate
 from $db_output.OAPs_referrals a
 left join (   select FirstID, MAX(LateDate) NewDate
               from
                   (
                   select a.UniqServReqId as FirstId, b.UniqServReqId as LaterId, b.ReferralRequestReceivedDate as LateDate
                   from $db_output.OAPs_referrals as a
                   inner join $db_output.OAPs_referrals as b
                      on a.Person_ID = b.Person_ID
                   where a.UniqServReqId != b.UniqServReqId 
                     and a.ReferralRequestReceivedDate < b.ReferralRequestReceivedDate
                     and (b.ReferralRequestReceivedDate < a.ServDischDate Or a.ServDischDate is null)
                   )
               group by FirstID) AS b on a.UniqServReqId = b.FirstId
;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_Month
 select distinct OAPs.UniqMonthID
         ,OAPs.Person_ID
         ,OAPs.OrgIDProv
         ,ccg.SubICBGPRes
         ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode
               WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender
               ELSE 'UNKNOWN' END AS Der_Gender
         ,CASE WHEN MPI.GenderIDCode IN ('3','4') THEN dd_genderID.Description
               WHEN MPI.Gender IN ('9') THEN dd_gender.Description
               WHEN MPI.GenderIDCode = '1' OR MPI.Gender = '1' THEN 'Male'
               WHEN MPI.GenderIDCode = '2' OR MPI.Gender = '2' THEN 'Female'
               ELSE 'UNKNOWN' END AS Der_GenderName
         ,MPI.AgeRepPeriodEnd
         ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN  0 AND 17 THEN '0 to 17'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 24 THEN '18 to 24'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
               WHEN MPI.AgeRepPeriodEnd >= 65 THEN '65 plus'
               ELSE 'UNKNOWN' END AS Der_AgeGroup
         ,MPI.NHSDEthnicity
         ,dd_ethnicity.Description AS NHSDEthnicityName
         ,CASE WHEN IMD.DECI_IMD = '10' THEN '10 Least deprived'
               WHEN IMD.DECI_IMD = '9' THEN '09 Less deprived'
               WHEN IMD.DECI_IMD = '8' THEN '08 Less deprived'
               WHEN IMD.DECI_IMD = '7' THEN '07 Less deprived'
               WHEN IMD.DECI_IMD = '6' THEN '06 Less deprived'
               WHEN IMD.DECI_IMD = '5' THEN '05 More deprived'
               WHEN IMD.DECI_IMD = '4' THEN '04 More deprived'
               WHEN IMD.DECI_IMD = '3' THEN '03 More deprived'
               WHEN IMD.DECI_IMD = '2' THEN '02 More deprived'
               WHEN IMD.DECI_IMD = '1' THEN '01 Most deprived'
               ELSE 'Unknown'
               END AS IMD_Decile
         ,OAPs.ReasonOAT
         ,dd_reasonoat.Description AS ReasonOATName
         ,OAPs.UniqServReqID
         ,OAPs.NewServDischDate
         ,OAPs.ReferralRequestReceivedDate
         ,OAPs.PrimReasonReferralMH
         ,dd_reasonref.Description AS PrimReasonReferralMHName
         ,HS.UniqHospProvSpellID
         ,WS.UniqWardStayID
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
               ELSE WS.HospitalBedTypeMH END AS HospitalBedTypeMH
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
               WHEN HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
               WHEN HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
               WHEN HospitalBedTypeMH = '12' THEN 'Adult Psychiatric Intensive Care Unit (acute mental health care)'
               WHEN HospitalBedTypeMH = '13' THEN 'Adult Eating Disorders'
               WHEN HospitalBedTypeMH = '14' THEN 'Mother and baby'
               WHEN HospitalBedTypeMH = '15' THEN 'Adult Learning Disabilities'
               WHEN HospitalBedTypeMH = '16' THEN 'Adult Low secure/locked rehabilitation'
               WHEN HospitalBedTypeMH = '17' THEN 'Adult High dependency rehabilitation'
               WHEN HospitalBedTypeMH = '18' THEN 'Adult Long term complex rehabilitation/ Continuing Care'
               WHEN HospitalBedTypeMH = '19' THEN 'Adult Low secure'
               WHEN HospitalBedTypeMH = '20' THEN 'Adult Medium secure'
               WHEN HospitalBedTypeMH = '21' THEN 'Adult High secure'
               WHEN HospitalBedTypeMH = '22' THEN 'Adult Neuro-psychiatry / Acquired Brain Injury'
               WHEN HospitalBedTypeMH = '23' THEN 'General child and young PERSON admitted PATIENT - Child (including High Dependency)'
               WHEN HospitalBedTypeMH = '24' THEN 'General child and young PERSON admitted PATIENT - Young PERSON (including High Dependency)'
               WHEN HospitalBedTypeMH = '25' THEN 'Eating Disorders admitted patient - Young person (13 years and over)'
               WHEN HospitalBedTypeMH = '26' THEN 'Eating Disorders admitted patient - Child (12 years and under)'
               WHEN HospitalBedTypeMH = '27' THEN 'Child and Young Person Low Secure Mental Illness'
               WHEN HospitalBedTypeMH = '28' THEN 'Child and Young Person Medium Secure Mental Illness'
               WHEN HospitalBedTypeMH = '29' THEN 'Child Mental Health admitted patient services for the Deaf'
               WHEN HospitalBedTypeMH = '30' THEN 'Child and Young Person Learning Disabilities / Autism admitted patient'
               WHEN HospitalBedTypeMH = '31' THEN 'Child and Young Person Low Secure Learning Disabilities'
               WHEN HospitalBedTypeMH = '32' THEN 'Child and Young Person Medium Secure Learning Disabilities'
               WHEN HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Young person'
               WHEN HospitalBedTypeMH = '34' THEN 'Child and Young Person Psychiatric Intensive Care Unit'
               WHEN HospitalBedTypeMH = '35' THEN 'Adult admitted patient continuing care'
               WHEN HospitalBedTypeMH = '36' THEN 'Adult community rehabilitation unit'
               WHEN HospitalBedTypeMH = '37' THEN 'Adult highly specialist high dependency rehabilitation unit'
               WHEN HospitalBedTypeMH = '38' THEN 'Adult longer term high dependency rehabilitation unit'
               WHEN HospitalBedTypeMH = '39' THEN 'Adult mental health admitted patient services for the Deaf'
               WHEN HospitalBedTypeMH = '40' THEN 'Adult personality disorder'
               ELSE 'UNKNOWN' 
               END AS HospitalBedTypeMHName
         ,WS.WardType
         ,WS.BedDaysWSEndRP
         ,OAPs.OrgIDProv as OrgIDReceiving
         ,OAPs.OrgIDReferring as OrgIDSubmitting
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_1m' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_1m') THEN '$rp_startdate_1m'
                                  WHEN StartDateWardStay >= '$rp_startdate_1m' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Month_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_qtr' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_qtr') THEN '$rp_startdate_qtr'
                                  WHEN StartDateWardStay >= '$rp_startdate_qtr' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Qtr_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_12m' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_12m') THEN '$rp_startdate_12m'
                                  WHEN StartDateWardStay >= '$rp_startdate_12m' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Yr_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN StartDateHospProvSpell < '$rp_startdate_1m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_1m') THEN '$rp_startdate_1m'
                                  WHEN StartDateHospProvSpell >= '$rp_startdate_1m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>StartDateHospProvSpell) THEN StartDateHospProvSpell
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Month_HS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN ReferralRequestReceivedDate < '$rp_startdate_qtr' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_qtr') THEN '$rp_startdate_qtr'
                                  WHEN ReferralRequestReceivedDate >= '$rp_startdate_qtr' and (DischDateHospProvSpell is null or DischDateHospProvSpell>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Qtr_HS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN ReferralRequestReceivedDate < '$rp_startdate_12m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_12m') THEN '$rp_startdate_12m'
                                  WHEN ReferralRequestReceivedDate >= '$rp_startdate_12m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Yr_HS
         ,dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK
         ,CASE WHEN isc.ORG_CODE is not null THEN 'Y' ELSE 'N' END as InScope
 from (select * from $db_output.Overlapping_OAPs_referrals where UniqMonthID = '$end_month_id') as OAPs
 left join (select *
             from $db_source.MHS001MPI
             where uniqmonthid = '$end_month_id'
               and PatMRecInRP = 'true'
               and (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')
               ) as MPI 
    on MPI.Person_ID = OAPs.Person_ID
 left join $db_output.HospProvSpell as HS 
    on OAPs.UniqServReqID = HS.UniqServReqID
   and OAPs.Person_ID = HS.Person_ID
   and OAPs.OrgIDProv = HS.OrgIDProv
   and HS.UniqMonthID = '$end_month_id'
 left join  $db_output.WardStay as WS  
    on OAPs.Person_ID = WS.Person_ID
   and HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
   and OAPs.OrgIDProv = WS.OrgIDProv
   and WS.UniqMonthID = '$end_month_id'
 left join $db_output.OAPs_onwardreferrals as ONW
    on OAPs.UniqServReqID = ONW.UniqServReqID
   and OAPs.Person_ID = ONW.Person_ID
   and OAPs.Uniqmonthid = ONW.Uniqmonthid
   and OAPs.OrgIDProv = ONW.OrgIDProv
   and ONW.UniqMonthID = '$end_month_id'
 left join $db_output.OAPS_In_Scope as isc
    on LEFT(OAPs.OrgIDReferring,3) = isc.ORG_CODE
 left join $corporate_ref.ENGLISH_INDICES_OF_DEP_V02 IMD
    on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
   and IMD.IMD_YEAR = '2019'
 left join $db_output.OAPS_CCG_LATEST as ccg
    on MPI.Person_ID = ccg.Person_ID
 left join $corporate_ref.DATADICTIONARYCODES as dd_gender
    on MPI.Gender = dd_gender.PrimaryCode
   and dd_gender.ItemName = 'PERSON_STATED_GENDER_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_genderID
    on MPI.GenderIDCode = dd_genderID.PrimaryCode
   and dd_genderID.ItemName = 'GENDER_IDENTITY_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_ethnicity
    on MPI.NHSDEthnicity = dd_ethnicity.PrimaryCode
   and dd_ethnicity.ItemName = 'ETHNIC_CATEGORY_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_reasonoat
    on OAPs.ReasonOAT = dd_reasonoat.PrimaryCode
   and dd_reasonoat.ItemName = 'REASON_FOR_OUT_OF_AREA_REFERRAL_FOR_ADULT_ACUTE_MENTAL_HEALTH'
 left join $corporate_ref.DATADICTIONARYCODES as dd_reasonref
    on OAPs.PrimReasonReferralMH = dd_reasonref.PrimaryCode
   and dd_reasonref.ItemName = 'REASON_FOR_REFERRAL_TO_MENTAL_HEALTH'
 ;
   
 OPTIMIZE $db_output.OAPs_Month;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_Quarter
 select distinct OAPs.UniqMonthID
         ,OAPs.Person_ID
         ,OAPs.OrgIDProv
         ,ccg.SubICBGPRes
         ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode
               WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender
               ELSE 'UNKNOWN' END AS Der_Gender
         ,CASE WHEN MPI.GenderIDCode IN ('3','4') THEN dd_genderID.Description
               WHEN MPI.Gender IN ('9') THEN dd_gender.Description
               WHEN MPI.GenderIDCode = '1' OR MPI.Gender = '1' THEN 'Male'
               WHEN MPI.GenderIDCode = '2' OR MPI.Gender = '2' THEN 'Female'
               ELSE 'UNKNOWN' END AS Der_GenderName
         ,MPI.AgeRepPeriodEnd
         ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN  0 AND 17 THEN '0 to 17'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 24 THEN '18 to 24'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
               WHEN MPI.AgeRepPeriodEnd >= 65 THEN '65 plus'
               ELSE 'UNKNOWN' END AS Der_AgeGroup
         ,MPI.NHSDEthnicity
         ,dd_ethnicity.Description AS NHSDEthnicityName
         ,CASE WHEN IMD.DECI_IMD = '10' THEN '10 Least deprived'
               WHEN IMD.DECI_IMD = '9' THEN '09 Less deprived'
               WHEN IMD.DECI_IMD = '8' THEN '08 Less deprived'
               WHEN IMD.DECI_IMD = '7' THEN '07 Less deprived'
               WHEN IMD.DECI_IMD = '6' THEN '06 Less deprived'
               WHEN IMD.DECI_IMD = '5' THEN '05 More deprived'
               WHEN IMD.DECI_IMD = '4' THEN '04 More deprived'
               WHEN IMD.DECI_IMD = '3' THEN '03 More deprived'
               WHEN IMD.DECI_IMD = '2' THEN '02 More deprived'
               WHEN IMD.DECI_IMD = '1' THEN '01 Most deprived'
               ELSE 'Unknown'
               END AS IMD_Decile
         ,OAPs.ReasonOAT
         ,dd_reasonoat.Description AS ReasonOATName
         ,OAPs.UniqServReqID
         ,OAPs.NewServDischDate
         ,OAPs.ReferralRequestReceivedDate
         ,OAPs.PrimReasonReferralMH
         ,dd_reasonref.Description AS PrimReasonReferralMHName
         ,HS.UniqHospProvSpellID
         ,WS.UniqWardStayID
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
               ELSE WS.HospitalBedTypeMH END AS HospitalBedTypeMH
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
               WHEN HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
               WHEN HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
               WHEN HospitalBedTypeMH = '12' THEN 'Adult Psychiatric Intensive Care Unit (acute mental health care)'
               WHEN HospitalBedTypeMH = '13' THEN 'Adult Eating Disorders'
               WHEN HospitalBedTypeMH = '14' THEN 'Mother and baby'
               WHEN HospitalBedTypeMH = '15' THEN 'Adult Learning Disabilities'
               WHEN HospitalBedTypeMH = '16' THEN 'Adult Low secure/locked rehabilitation'
               WHEN HospitalBedTypeMH = '17' THEN 'Adult High dependency rehabilitation'
               WHEN HospitalBedTypeMH = '18' THEN 'Adult Long term complex rehabilitation/ Continuing Care'
               WHEN HospitalBedTypeMH = '19' THEN 'Adult Low secure'
               WHEN HospitalBedTypeMH = '20' THEN 'Adult Medium secure'
               WHEN HospitalBedTypeMH = '21' THEN 'Adult High secure'
               WHEN HospitalBedTypeMH = '22' THEN 'Adult Neuro-psychiatry / Acquired Brain Injury'
               WHEN HospitalBedTypeMH = '23' THEN 'General child and young PERSON admitted PATIENT - Child (including High Dependency)'
               WHEN HospitalBedTypeMH = '24' THEN 'General child and young PERSON admitted PATIENT - Young PERSON (including High Dependency)'
               WHEN HospitalBedTypeMH = '25' THEN 'Eating Disorders admitted patient - Young person (13 years and over)'
               WHEN HospitalBedTypeMH = '26' THEN 'Eating Disorders admitted patient - Child (12 years and under)'
               WHEN HospitalBedTypeMH = '27' THEN 'Child and Young Person Low Secure Mental Illness'
               WHEN HospitalBedTypeMH = '28' THEN 'Child and Young Person Medium Secure Mental Illness'
               WHEN HospitalBedTypeMH = '29' THEN 'Child Mental Health admitted patient services for the Deaf'
               WHEN HospitalBedTypeMH = '30' THEN 'Child and Young Person Learning Disabilities / Autism admitted patient'
               WHEN HospitalBedTypeMH = '31' THEN 'Child and Young Person Low Secure Learning Disabilities'
               WHEN HospitalBedTypeMH = '32' THEN 'Child and Young Person Medium Secure Learning Disabilities'
               WHEN HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Young person'
               WHEN HospitalBedTypeMH = '34' THEN 'Child and Young Person Psychiatric Intensive Care Unit'
               WHEN HospitalBedTypeMH = '35' THEN 'Adult admitted patient continuing care'
               WHEN HospitalBedTypeMH = '36' THEN 'Adult community rehabilitation unit'
               WHEN HospitalBedTypeMH = '37' THEN 'Adult highly specialist high dependency rehabilitation unit'
               WHEN HospitalBedTypeMH = '38' THEN 'Adult longer term high dependency rehabilitation unit'
               WHEN HospitalBedTypeMH = '39' THEN 'Adult mental health admitted patient services for the Deaf'
               WHEN HospitalBedTypeMH = '40' THEN 'Adult personality disorder'
               ELSE 'UNKNOWN' 
               END AS HospitalBedTypeMHName
         ,WS.WardType
         ,WS.BedDaysWSEndRP
         ,OAPs.OrgIDProv as OrgIDReceiving
         ,OAPs.OrgIDReferring as OrgIDSubmitting
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_1m' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_1m') THEN '$rp_startdate_1m'
                                  WHEN StartDateWardStay >= '$rp_startdate_1m' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Month_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_qtr' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_qtr') THEN '$rp_startdate_qtr'
                                  WHEN StartDateWardStay >= '$rp_startdate_qtr' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Qtr_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_12m' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_12m') THEN '$rp_startdate_12m'
                                  WHEN StartDateWardStay >= '$rp_startdate_12m' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Yr_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN StartDateHospProvSpell < '$rp_startdate_1m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_1m') THEN '$rp_startdate_1m'
                                  WHEN StartDateHospProvSpell >= '$rp_startdate_1m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>StartDateHospProvSpell) THEN StartDateHospProvSpell
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Month_HS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN ReferralRequestReceivedDate < '$rp_startdate_qtr' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_qtr') THEN '$rp_startdate_qtr'
                                  WHEN ReferralRequestReceivedDate >= '$rp_startdate_qtr' and (DischDateHospProvSpell is null or DischDateHospProvSpell>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Qtr_HS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN ReferralRequestReceivedDate < '$rp_startdate_12m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_12m') THEN '$rp_startdate_12m'
                                  WHEN ReferralRequestReceivedDate >= '$rp_startdate_12m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Yr_HS
         ,dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK
         ,CASE WHEN isc.ORG_CODE is not null THEN 'Y' ELSE 'N' END as InScope
 from (select * from $db_output.Overlapping_OAPs_referrals where UniqMonthID between $end_month_id-2 and $end_month_id) as OAPs
 left join (select *
             from $db_source.MHS001MPI
             where Uniqmonthid between $end_month_id-2 and $end_month_id
               and PatMRecInRP = 'true'
               ) as MPI  
    on MPI.Person_ID = OAPs.Person_ID
   and MPI.UniqMonthID = OAPs.UniqMonthID
 left join $db_output.HospProvSpell as HS 
    on OAPs.UniqServReqID = HS.UniqServReqID
   and OAPs.Person_ID = HS.Person_ID
   and OAPs.OrgIDProv = HS.OrgIDProv
   and HS.UniqMonthID between $end_month_id-2 and $end_month_id
 left join  $db_output.WardStay as WS  
    on OAPs.Person_ID = WS.Person_ID
   and HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
   and OAPs.OrgIDProv = WS.OrgIDProv
   and WS.UniqMonthID between $end_month_id-2 and $end_month_id
 left join $db_output.OAPs_onwardreferrals as ONW
    on OAPs.UniqServReqID = ONW.UniqServReqID
   and OAPs.Person_ID = ONW.Person_ID
   and OAPs.Uniqmonthid = ONW.Uniqmonthid
   and OAPs.OrgIDProv = ONW.OrgIDProv
   and ONW.UniqMonthID between $end_month_id-2 and $end_month_id
 left join $db_output.OAPS_In_Scope as isc
    on LEFT(OAPs.OrgIDReferring,3) = isc.ORG_CODE
 left join $corporate_ref.ENGLISH_INDICES_OF_DEP_V02 IMD
    on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
   and IMD.IMD_YEAR = '2019'
 left join $db_output.OAPS_CCG_LATEST as ccg
    on MPI.Person_ID = ccg.Person_ID
 left join $corporate_ref.DATADICTIONARYCODES as dd_gender
    on MPI.Gender = dd_gender.PrimaryCode
   and dd_gender.ItemName = 'PERSON_STATED_GENDER_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_genderID
    on MPI.GenderIDCode = dd_genderID.PrimaryCode
   and dd_genderID.ItemName = 'GENDER_IDENTITY_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_ethnicity
    on MPI.NHSDEthnicity = dd_ethnicity.PrimaryCode
   and dd_ethnicity.ItemName = 'ETHNIC_CATEGORY_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_reasonoat
    on OAPs.ReasonOAT = dd_reasonoat.PrimaryCode
   and dd_reasonoat.ItemName = 'REASON_FOR_OUT_OF_AREA_REFERRAL_FOR_ADULT_ACUTE_MENTAL_HEALTH'
 left join $corporate_ref.DATADICTIONARYCODES as dd_reasonref
    on OAPs.PrimReasonReferralMH = dd_reasonref.PrimaryCode
   and dd_reasonref.ItemName = 'REASON_FOR_REFERRAL_TO_MENTAL_HEALTH'
 ;
   
 OPTIMIZE $db_output.OAPs_Quarter;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.OAPs_Year
 select distinct OAPs.UniqMonthID
         ,OAPs.Person_ID
         ,OAPs.OrgIDProv
         ,ccg.SubICBGPRes
         ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode
               WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender
               ELSE 'UNKNOWN' END AS Der_Gender
         ,CASE WHEN MPI.GenderIDCode IN ('3','4') THEN dd_genderID.Description
               WHEN MPI.Gender IN ('9') THEN dd_gender.Description
               WHEN MPI.GenderIDCode = '1' OR MPI.Gender = '1' THEN 'Male'
               WHEN MPI.GenderIDCode = '2' OR MPI.Gender = '2' THEN 'Female'
               ELSE 'UNKNOWN' END AS Der_GenderName
         ,MPI.AgeRepPeriodEnd
         ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN  0 AND 17 THEN '0 to 17'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 24 THEN '18 to 24'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
               WHEN MPI.AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
               WHEN MPI.AgeRepPeriodEnd >= 65 THEN '65 plus'
               ELSE 'UNKNOWN' END AS Der_AgeGroup
         ,MPI.NHSDEthnicity
         ,dd_ethnicity.Description AS NHSDEthnicityName
         ,CASE WHEN IMD.DECI_IMD = '10' THEN '10 Least deprived'
               WHEN IMD.DECI_IMD = '9' THEN '09 Less deprived'
               WHEN IMD.DECI_IMD = '8' THEN '08 Less deprived'
               WHEN IMD.DECI_IMD = '7' THEN '07 Less deprived'
               WHEN IMD.DECI_IMD = '6' THEN '06 Less deprived'
               WHEN IMD.DECI_IMD = '5' THEN '05 More deprived'
               WHEN IMD.DECI_IMD = '4' THEN '04 More deprived'
               WHEN IMD.DECI_IMD = '3' THEN '03 More deprived'
               WHEN IMD.DECI_IMD = '2' THEN '02 More deprived'
               WHEN IMD.DECI_IMD = '1' THEN '01 Most deprived'
               ELSE 'Unknown'
               END AS IMD_Decile
         ,OAPs.ReasonOAT
         ,dd_reasonoat.Description AS ReasonOATName
         ,OAPs.UniqServReqID
         ,OAPs.NewServDischDate
         ,OAPs.ReferralRequestReceivedDate
         ,OAPs.PrimReasonReferralMH
         ,dd_reasonref.Description AS PrimReasonReferralMHName
         ,HS.UniqHospProvSpellID
         ,WS.UniqWardStayID
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
               ELSE WS.HospitalBedTypeMH END AS HospitalBedTypeMH
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
               WHEN HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
               WHEN HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
               WHEN HospitalBedTypeMH = '12' THEN 'Adult Psychiatric Intensive Care Unit (acute mental health care)'
               WHEN HospitalBedTypeMH = '13' THEN 'Adult Eating Disorders'
               WHEN HospitalBedTypeMH = '14' THEN 'Mother and baby'
               WHEN HospitalBedTypeMH = '15' THEN 'Adult Learning Disabilities'
               WHEN HospitalBedTypeMH = '16' THEN 'Adult Low secure/locked rehabilitation'
               WHEN HospitalBedTypeMH = '17' THEN 'Adult High dependency rehabilitation'
               WHEN HospitalBedTypeMH = '18' THEN 'Adult Long term complex rehabilitation/ Continuing Care'
               WHEN HospitalBedTypeMH = '19' THEN 'Adult Low secure'
               WHEN HospitalBedTypeMH = '20' THEN 'Adult Medium secure'
               WHEN HospitalBedTypeMH = '21' THEN 'Adult High secure'
               WHEN HospitalBedTypeMH = '22' THEN 'Adult Neuro-psychiatry / Acquired Brain Injury'
               WHEN HospitalBedTypeMH = '23' THEN 'General child and young PERSON admitted PATIENT - Child (including High Dependency)'
               WHEN HospitalBedTypeMH = '24' THEN 'General child and young PERSON admitted PATIENT - Young PERSON (including High Dependency)'
               WHEN HospitalBedTypeMH = '25' THEN 'Eating Disorders admitted patient - Young person (13 years and over)'
               WHEN HospitalBedTypeMH = '26' THEN 'Eating Disorders admitted patient - Child (12 years and under)'
               WHEN HospitalBedTypeMH = '27' THEN 'Child and Young Person Low Secure Mental Illness'
               WHEN HospitalBedTypeMH = '28' THEN 'Child and Young Person Medium Secure Mental Illness'
               WHEN HospitalBedTypeMH = '29' THEN 'Child Mental Health admitted patient services for the Deaf'
               WHEN HospitalBedTypeMH = '30' THEN 'Child and Young Person Learning Disabilities / Autism admitted patient'
               WHEN HospitalBedTypeMH = '31' THEN 'Child and Young Person Low Secure Learning Disabilities'
               WHEN HospitalBedTypeMH = '32' THEN 'Child and Young Person Medium Secure Learning Disabilities'
               WHEN HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Young person'
               WHEN HospitalBedTypeMH = '34' THEN 'Child and Young Person Psychiatric Intensive Care Unit'
               WHEN HospitalBedTypeMH = '35' THEN 'Adult admitted patient continuing care'
               WHEN HospitalBedTypeMH = '36' THEN 'Adult community rehabilitation unit'
               WHEN HospitalBedTypeMH = '37' THEN 'Adult highly specialist high dependency rehabilitation unit'
               WHEN HospitalBedTypeMH = '38' THEN 'Adult longer term high dependency rehabilitation unit'
               WHEN HospitalBedTypeMH = '39' THEN 'Adult mental health admitted patient services for the Deaf'
               WHEN HospitalBedTypeMH = '40' THEN 'Adult personality disorder'
               ELSE 'UNKNOWN' 
               END AS HospitalBedTypeMHName
         ,WS.WardType
         ,WS.BedDaysWSEndRP
         ,OAPs.OrgIDProv as OrgIDReceiving
         ,OAPs.OrgIDReferring as OrgIDSubmitting
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_1m' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_1m') THEN '$rp_startdate_1m'
                                  WHEN StartDateWardStay >= '$rp_startdate_1m' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Month_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_qtr' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_qtr') THEN '$rp_startdate_qtr'
                                  WHEN StartDateWardStay >= '$rp_startdate_qtr' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Qtr_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE EndDateWardStay END,
                             CASE WHEN StartDateWardStay < '$rp_startdate_12m' and (EndDateWardStay is null or EndDateWardStay>'$rp_startdate_12m') THEN '$rp_startdate_12m'
                                  WHEN StartDateWardStay >= '$rp_startdate_12m' and (EndDateWardStay is null or EndDateWardStay>StartDateWardStay) THEN StartDateWardStay
                                  WHEN EndDateWardStay is null or EndDateWardStay>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE EndDateWardStay END) END as Bed_Days_Yr_WS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN StartDateHospProvSpell < '$rp_startdate_1m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_1m') THEN '$rp_startdate_1m'
                                  WHEN StartDateHospProvSpell >= '$rp_startdate_1m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>StartDateHospProvSpell) THEN StartDateHospProvSpell
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Month_HS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN ReferralRequestReceivedDate < '$rp_startdate_qtr' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_qtr') THEN '$rp_startdate_qtr'
                                  WHEN ReferralRequestReceivedDate >= '$rp_startdate_qtr' and (DischDateHospProvSpell is null or DischDateHospProvSpell>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Qtr_HS
         ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 0
               ELSE datediff(CASE WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN DATE_ADD('$rp_enddate',1) ELSE DischDateHospProvSpell END,
                             CASE WHEN ReferralRequestReceivedDate < '$rp_startdate_12m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_startdate_12m') THEN '$rp_startdate_12m'
                                  WHEN ReferralRequestReceivedDate >= '$rp_startdate_12m' and (DischDateHospProvSpell is null or DischDateHospProvSpell>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                                  WHEN DischDateHospProvSpell is null or DischDateHospProvSpell>'$rp_enddate' THEN '$rp_enddate'
                                  ELSE DischDateHospProvSpell END) END as Bed_Days_Yr_HS
         ,dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK
         ,CASE WHEN isc.ORG_CODE is not null THEN 'Y' ELSE 'N' END as InScope
 from (select * from $db_output.Overlapping_OAPs_referrals where UniqMonthID between $end_month_id-11 and $end_month_id) as OAPs
 left join (select *
             from $db_source.MHS001MPI
             where Uniqmonthid between $end_month_id-11 and $end_month_id
               and PatMRecInRP = 'true'
               ) as MPI 
    on MPI.Person_ID = OAPs.Person_ID
   and MPI.UniqMonthID = OAPs.UniqMonthID
 left join $db_output.HospProvSpell as HS 
    on OAPs.UniqServReqID = HS.UniqServReqID
   and OAPs.Person_ID = HS.Person_ID
   and OAPs.OrgIDProv = HS.OrgIDProv
   and HS.UniqMonthID between $end_month_id-11 and $end_month_id
 left join  $db_output.WardStay as WS  
    on OAPs.Person_ID = WS.Person_ID
   and HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
   and OAPs.OrgIDProv = WS.OrgIDProv
   and WS.UniqMonthID between $end_month_id-11 and $end_month_id
 left join $db_output.OAPs_onwardreferrals as ONW
    on OAPs.UniqServReqID = ONW.UniqServReqID
   and OAPs.Person_ID = ONW.Person_ID
   and OAPs.Uniqmonthid = ONW.Uniqmonthid
   and OAPs.OrgIDProv = ONW.OrgIDProv
   and ONW.UniqMonthID between $end_month_id-11 and $end_month_id
 left join $db_output.OAPS_In_Scope as isc
    on LEFT(OAPs.OrgIDReferring,3) = isc.ORG_CODE
 left join $corporate_ref.ENGLISH_INDICES_OF_DEP_V02 IMD
    on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
   and IMD.IMD_YEAR = '2019'
 left join $db_output.OAPS_CCG_LATEST as ccg
    on MPI.Person_ID = ccg.Person_ID
 left join $corporate_ref.DATADICTIONARYCODES as dd_gender
    on MPI.Gender = dd_gender.PrimaryCode
   and dd_gender.ItemName = 'PERSON_STATED_GENDER_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_genderID
    on MPI.GenderIDCode = dd_genderID.PrimaryCode
   and dd_genderID.ItemName = 'GENDER_IDENTITY_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_ethnicity
    on MPI.NHSDEthnicity = dd_ethnicity.PrimaryCode
   and dd_ethnicity.ItemName = 'ETHNIC_CATEGORY_CODE'
 left join $corporate_ref.DATADICTIONARYCODES as dd_reasonoat
    on OAPs.ReasonOAT = dd_reasonoat.PrimaryCode
   and dd_reasonoat.ItemName = 'REASON_FOR_OUT_OF_AREA_REFERRAL_FOR_ADULT_ACUTE_MENTAL_HEALTH'
 left join $corporate_ref.DATADICTIONARYCODES as dd_reasonref
    on OAPs.PrimReasonReferralMH = dd_reasonref.PrimaryCode
   and dd_reasonref.ItemName = 'REASON_FOR_REFERRAL_TO_MENTAL_HEALTH'
 ;
   
 OPTIMIZE $db_output.OAPs_Year;
