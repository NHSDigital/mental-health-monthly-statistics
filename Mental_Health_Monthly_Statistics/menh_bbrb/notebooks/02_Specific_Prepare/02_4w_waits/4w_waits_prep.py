# Databricks notebook source
 %sql
 ---GET ALL ADULT CMH REFERRALS IN REPORTING PERIOD
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_referrals  
 SELECT
     f.ReportingPeriodStartDate
     ,f.ReportingPeriodEndDate
     ,r.UniqMonthID
     ,r.OrgIDProv
     ,r.Person_ID --Calculation of Der_PersonID?
     ,m.OrgIDSubICBLocResidence
     ,r.RecordNumber
     ,r.UniqServReqID
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,CASE WHEN r.ServDischDate IS NULL OR r.ServDischDate > f.ReportingPeriodEndDate THEN f.ReportingPeriodEndDate ELSE r.ServDischDate END AS Der_EndDate
     ,s.ReferClosReason
     ,s.ReferRejectionDate
     ,s.ReferRejectReason
     ,r.SourceOfReferralMH
     ,r.PrimReasonReferralMH
     ,CASE WHEN r.PrimReasonReferralMH = '24' THEN 1 ELSE 0 END Reason_ND
     ,CASE WHEN r.PrimReasonReferralMH = '25' THEN 1 ELSE 0 END Reason_ASD
     ,CASE WHEN s.ServTeamTypeRefToMH = 'C03' THEN 'C10' ELSE s.ServTeamTypeRefToMH END AS Der_ServTeamTypeRefToMH --Why is this transformed?
     ,s.UniqCareProfTeamID
     ,ROW_NUMBER()OVER(PARTITION BY r.Person_ID, r.UniqServReqID ORDER BY r.UniqMonthID DESC) AS Der_LatestRecord
  
 FROM $db_source.MHS101Referral r
  
 LEFT JOIN (select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate from $db_source.MHS000header) f on r.uniqmonthid = f.uniqmonthid
  
 INNER JOIN $db_output.ServiceTeamType s ON r.RecordNumber = s.RecordNumber AND r.UniqServReqID = s.UniqServReqID
  
 INNER JOIN $db_source.MHS001MPI m ON r.RecordNumber = m.RecordNumber
  
 LEFT JOIN $db_source.MHS501HospProvSpell h on r.uniqservreqid = h.uniqservreqid and r.person_id = h.person_id AND h.uniqmonthid <= '$end_month_id'
 --AMENDING TO STOP FUTURE HOSPITAL SPELLS (i.e. referral is in scope to the point the inpatient stay starts)
 ---IN UDAL THIS IS DELETED AS AN EXTRA STEP BY CONCAT(Person_ID, UniqServReqID) Doesn't account for hosp spells which have occurred after the referral was received
 ---Are we intending to delete a referral if the patient had an inpatient stay at ANY point or just when referral has an inpatient stay alongside
  
 WHERE r.AgeServReferRecDate >= 18 -- 18 and over
     AND s.ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10') -- Core community MH teams
     AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = "") -- only people resident in England
     AND r.OrgIDProv <> 'DFC'
     AND r.UniqMonthID <= $end_month_id
     AND h.UniqHospProvSpellID is null --Added to remove inpatients instead of using the delete statement

# COMMAND ----------

 %sql
 ---SPLIT OUT START AND END DATES FOR EACH REFERRAL
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_dates
 SELECT 
 Person_ID,
 OrgIDProv,
 UniqServReqID,
 ReferralRequestReceivedDate AS Der_Date, -- start dates
 1 AS Inc --- + 1 to indicate referral has started
  
 FROM $db_output.cmh_4ww_referrals  
 WHERE Der_LatestRecord = 1 
  
 UNION ALL
  
 SELECT 
 Person_ID,
 OrgIDProv,
 UniqServReqID,
 DATE_ADD(Der_EndDate,5) AS Der_Date, -- end dates : allow up to 5 days between referrals 
 -1 AS Inc --- -1 to indicate referral has ended
  
 FROM $db_output.cmh_4ww_referrals   
 WHERE Der_LatestRecord = 1 

# COMMAND ----------

 %sql
 ---GET CUMULATIVE FREQUENCY FOR EACH SPELL 
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_cumf
 SELECT 
 Person_ID, 
 OrgIDProv, 
 Der_Date, 
 SUM(SUM(Inc)) OVER (ORDER BY Person_ID, OrgIDProv, Der_Date) AS Cumf_Inc
 --- the above gets cumulative sum of Inc the field across a person-provider combination ordered by Der_Date (ReferralRequestReceivedDate (1) or ServDischDate/ReportingPeriodEndDate (-1))
 --- if a person has had multiple referrals which overlap the Cumf_Inc will be > 1
 --- when multiple referrals that overlap have ended, the latest Der_Date is where Cumf_Inc will be 0
  
 FROM $db_output.cmh_4ww_dates   
 GROUP BY Person_ID, OrgIDProv, Der_Date

# COMMAND ----------

 %sql
 ---GROUP INTO SPELLS 
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_groups 
 SELECT 
 Person_ID,
 OrgIDProv,
 Inc_Group, ---Inc Group separates out multiple spells
 CONCAT(Person_ID, OrgIDProv, Inc_Group) AS SpellID, 
 MIN(Der_Date) AS StartDate, ---Earliest Date will be start of spell
 MAX(Der_Date) AS EndDate, ---Latest Date will be end of spell
 DATE_ADD(MAX(Der_Date), -5) AS Der_EndDate ---Take back 5 days which were added previously
  
 FROM 
 (
   SELECT 
   * 
   ,SUM(CASE WHEN Cumf_Inc = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY Der_Date DESC) AS Inc_Group
   ---The Inc_Group field separates out person-provider combinations that have ended (Cumf_Inc = 0) assigns them as 1 and then partitions
   --- i.e. a first spells rows will be assigned as 1 and each time that person-provider combination has a new spell Inc_Group will be +1, 
   FROM $db_output.cmh_4ww_cumf
 ) x 
  
 GROUP BY Person_ID, OrgIDProv, Inc_Group

# COMMAND ----------

 %sql
 ---RANK CONTACTS TO GET SECOND CONTACT WITHIN SPELL
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_spell_rank
 SELECT
 g.Person_ID,
 g.OrgIDProv,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate AS EndDate, --- remove the added days
 c.CareContDate,
 c.UniqServReqID AS Contact_Ref,
 ROW_NUMBER() OVER (PARTITION BY g.SpellID ORDER BY c.CareContDate) AS Der_ContactOrder
  
 FROM $db_output.cmh_4ww_groups g 
  
 INNER JOIN $db_source.MHS201CareContact c ON g.Person_ID = c.Person_ID
     AND g.OrgIDProv = c.OrgIDProv
     AND c.CareContDate BETWEEN g.StartDate AND g.Der_EndDate
     AND c.AttendStatus IN ('5','6') 
     AND ((c.UniqMonthID < 1459 AND c.ConsMechanismMH NOT IN ('05','06')) OR (c.UniqMonthID >= 1459 AND c.ConsMechanismMH IN ('01', '02', '04', '11')))
  
 INNER JOIN $db_output.cmh_4ww_referrals r ON r.UniqServReqID = c.UniqServReqID AND r.RecordNumber = c.RecordNumber 
 -- contacts must be with in-scope teams  
 --Does using uniqservreqid and recordnumber work here since the referrals are ranked out so you would only have one referral month not all meaning some contacts might be missed? No - latest record not taken yet so is RecordNumber even needed?
 ---Join on CareTeamID to ensure care contact was with the correct team? Maybe consider complexity of v5/v6
 WHERE g.StartDate >= '2016-01-01'

# COMMAND ----------

 %sql
 ---CROSS CHECK CODES IN THESE REF SETS BETWEEN UDAL (MHDInternal.Staging_MH_PRSB_SNOMED_RefSet) AND DAE 
 CREATE OR REPLACE TEMPORARY VIEW MHSDS_SNOMED_REFSET AS 
 SELECT 
 CASE 
   WHEN RefSetID = '1853441000000109' THEN 'Assessments'--Mental Health Services Data Set assessment procedures simple reference set
   WHEN RefSetID = '1853461000000105' THEN 'Psychological therapies'--Mental Health Services Data Set psychological therapies simple reference set
   WHEN RefSetID =  '1853481000000101' THEN 'Psychosocial interventions' --Mental Health Services Data Set psychosocial interventions simple reference set
   WHEN RefSetID = '1853451000000107' THEN 'Medication'
   END AS Ref_Group,
 CASE 
   WHEN RefSetID = '1853441000000109' THEN 'Mental Health Services Data Set assessment procedures simple reference set'  
   WHEN RefSetID = '1853461000000105' THEN 'Mental Health Services Data Set psychological therapies simple reference set'
   WHEN RefSetID =  '1853481000000101' THEN 'Mental Health Services Data Set psychosocial interventions simple reference set' 
   WHEN RefSetID = '1853451000000107' THEN 'Mental Health Services Data Set medication and physical therapy interventions simple reference set'
   END AS Reference_Set,
 RefSetID,
 ReferencedComponentID as SNOMED_Code,
 b.term as SNOMED_Name
 FROM 
 $reference_data.SNOMED_SCT2_REFSET_FULL a
 left join 
 (SELECT 
 *,
 ROW_NUMBER()OVER(PARTITION BY conceptid ORDER BY Active desc, EffectiveTime desc, ID desc, DSS_KEY desc) as RN
 FROM
 $reference_data.snomed_sct2_description_full) b on a.referencedcomponentid = b.conceptid and b.active = '1' and b.RN = '1'
 WHERE
  a.RefSetID IN ('1853441000000109', --Mental Health Services Data Set assessment procedures simple reference set
                 '1853461000000105', --Mental Health Services Data Set psychological therapies simple reference set
                 '1853481000000101', --Mental Health Services Data Set psychosocial interventions simple reference set
                 '1853451000000107')--Mental Health Services Data Set medication and physical therapy interventions simple reference set
 and a.Active = '1'

# COMMAND ----------

 %sql
 ---GET CARE PLAN DATES
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_subs
 SELECT
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Care Plan or Intervention' AS Der_EventCategory,
 'Care Plan' AS Der_EventType,
 MIN(CASE WHEN (c.CarePlanCreatDate >= g.StartDate AND c.CarePlanCreatDate <= c.CarePlanLastUpdateDate) THEN c.CarePlanCreatDate --Where the care plan was created during the spell - use its creation date
          ELSE c.CarePlanLastUpdateDate END) --Else use the last updated date (for care plans that were created before the spell started, but then updated during the spell
          AS Der_EventDate
  
 FROM $db_output.cmh_4ww_groups g
  
 INNER JOIN $db_source.MHS008CarePlanType c ON g.Person_ID = c.Person_ID AND g.OrgIDProv = c.OrgIDProv AND COALESCE(c.CarePlanLastUpdateDate, c.CarePlanCreatDate) BETWEEN g.StartDate and g.Der_EndDate
  
 INNER JOIN $db_source.MHS009CarePlanAgreement cpa ON cpa.RecordNumber = c.RecordNumber AND cpa.UniqCarePlanID = c.UniqCarePlanID -- only agreed care plans ReceivedDate
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET CARE ACTIVITY DATES
 INSERT INTO $db_output.cmh_4ww_subs
  
 SELECT
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Meaningful Assessment' ELSE 'Care Plan or Intervention' END AS Der_EventCategory,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Assessment - SNoMED' ELSE 'Intervention' END AS Der_EventType,
 MIN(c.CareContDate) AS Der_EventDate ---MIN(CareContDate) used here
     
 ---In UDAL Code the base table used is cmh_4ww_groups INNER JOINED to MHS201 by Person_ID, OrgIDProv AND CareContDate BETWEEN StartDate and EndDate + 5 
  
 FROM $db_output.cmh_4ww_groups g
  
 INNER JOIN $db_source.MHS201CareContact c ON g.Person_ID = c.Person_ID AND g.OrgIDProv = c.OrgIDProv AND c.CareContDate BETWEEN g.StartDate and g.Der_EndDate
  
 INNER JOIN $db_source.MHS202CareActivity i ON c.UniqCareContID = i.UniqCareContID AND c.RecordNumber = i.RecordNumber
  
 INNER JOIN $db_output.cmh_4ww_referrals r ON i.UniqServReqID = r.UniqServReqID AND r.RecordNumber = i.RecordNumber 
  
 INNER JOIN MHSDS_SNOMED_REFSET s ON
 --MHDInternal.Staging_MH_PRSB_SNOMED_referralset s ON --NEED TO GET THE SNOMED REFSET HERE--
         (CASE
         WHEN position(':',i.Procedure) > 0
         THEN LEFT(i.Procedure, position(':',i.Procedure)-1)
         ELSE i.Procedure
         END) = s.SNOMED_CODE ---CHARINDEX in spark sql previously we have used POSITION instead? Could be a difference there? Edit: CHARINDEX has been added to Spark 3 :)
  
 WHERE i.Procedure IS NOT NULL 
 ---The filter below is in UDAL not in DAE
 AND SNOMED_Name NOT IN ('Consultation (procedure)','Signposting (procedure)') --- doesn't count for adults ---add field in MHSDS_SNOMED_REFSET for valid/invalid
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Meaningful Assessment' ELSE 'Care Plan or Intervention' END,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Assessment - SNoMED' ELSE 'Intervention' END

# COMMAND ----------

 %sql
 ---GET INDIRECT ACTIVITY
 INSERT INTO $db_output.cmh_4ww_subs
  
 SELECT
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Meaningful Assessment' ELSE 'Care Plan or Intervention' END AS Der_EventCategory,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Assessment - SNoMED' ELSE 'Intervention' END AS Der_EventType,
 MIN(i.IndirectActDate) AS Der_EventDate
     
 ---In UDAL Code the base table used is cmh_4ww_groups INNER JOINED to MHS201 by Person_ID, OrgIDProv AND CareContDate BETWEEN StartDate and EndDate + 5 
  
 FROM $db_output.cmh_4ww_groups g
  
 INNER JOIN $db_source.MHS204IndirectActivity i on g.Person_ID = i.Person_ID and g.OrgIDProv = i.OrgIDProv AND i.IndirectActDate BETWEEN g.StartDate and g.Der_EndDate
  
 INNER JOIN $db_output.cmh_4ww_referrals r ON r.RecordNumber = i.RecordNumber AND r.UniqServReqid = i.UniqServReqID
  
 INNER JOIN MHSDS_SNOMED_REFSET s ON
 --MHDInternal.Staging_MH_PRSB_SNOMED_referralset s ON --NEED TO GET THE SNOMED REFSET HERE--
         (CASE
         WHEN position(':',i.IndActProcedure) > 0
         THEN LEFT(i.IndActProcedure, position(':',i.IndActProcedure)-1)
         ELSE i.IndActProcedure
         END) = s.SNOMED_CODE
         
 WHERE i.IndActProcedure IS NOT NULL
 AND SNOMED_Name NOT IN ('Consultation (procedure)','Signposting (procedure)') --- doesn't count for adults 
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Meaningful Assessment' ELSE 'Care Plan or Intervention' END,
 CASE WHEN s.Ref_Group = 'Assessments' THEN 'Assessment - SNoMED' ELSE 'Intervention' END

# COMMAND ----------

 %sql
 ---GET REFERRAL ASSESSMENT DATES
 INSERT INTO $db_output.cmh_4ww_subs
  
 SELECT 
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Baseline outcome' AS Der_EventCategory,
 'Assessment - Outcome' AS Der_EventType,
 MIN(CAST(ass.AssToolCompTimestamp AS DATE)) AS Der_EventDate
  
 FROM $db_output.cmh_4ww_groups g
  
 INNER JOIN $db_source.MHS606CodedScoreAssessmentRefer ass ON g.Person_ID = ass.Person_ID and g.OrgIDProv = ass.OrgIDProv AND CAST(ass.AssToolCompTimestamp AS DATE) BETWEEN g.StartDate and g.Der_EndDate
  
 INNER JOIN $db_output.cmh_4ww_referrals r ON ass.RecordNumber = r.RecordNumber AND ass.UniqServReqid = r.UniqServReqID
  
 INNER JOIN $db_output.mh_ass m ON ass.CodedAssToolType = m.Active_Concept_ID_SNOMED --NEED TO GET SNOMED REFSETS HERE--
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET CARE ACTIVITY ASSESSMENT DATES
 INSERT INTO $db_output.cmh_4ww_subs
  
 SELECT 
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Baseline outcome' AS Der_EventCategory,
 'Assessment - Outcome' AS Der_EventType,
 MIN(c.CareContDate) AS Der_EventDate
  
 FROM $db_output.cmh_4ww_groups g
  
 INNER JOIN $db_source.MHS201CareContact c ON g.Person_ID = c.Person_ID and g.OrgIDProv = c.OrgIDProv AND c.CareContDate BETWEEN g.StartDate and g.Der_EndDate
  
 INNER JOIN $db_output.cmh_4ww_referrals r ON c.RecordNumber = r.RecordNumber AND c.UniqServReqid = r.UniqServReqID
  
 INNER JOIN $db_source.MHS607CodedScoreAssessmentAct ass ON c.RecordNumber = ass.RecordNumber AND c.UniqCareContID = ass.UniqCareContID
  
 INNER JOIN $db_output.mh_ass m ON ass.CodedAssToolType = m.Active_Concept_ID_SNOMED   --NEED TO GET SNOMED REFSETS HERE-- 
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET CARE CLUSTER DATES (NOT POPULATED AS OF MHSDS V6)
 INSERT INTO $db_output.cmh_4ww_subs
  
 SELECT 
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Baseline outcome' AS Der_EventCategory,
 'Assessment - Outcome' AS Der_EventType,
 MIN(ct.AssToolCompDate) AS Der_EventDate
  
 FROM $db_output.cmh_4ww_groups g
  
 INNER JOIN $db_source.MHS801ClusterTool ct ON g.Person_ID = ct.Person_ID and g.OrgIDProv = ct.OrgIDProv AND ct.AssToolCompDate BETWEEN g.StartDate and g.Der_EndDate
  
 INNER JOIN $db_source.MHS802ClusterAssess ass ON ct.UniqClustID = ass.UniqClustID AND ct.RecordNumber = ass.RecordNumber 
  
 INNER JOIN $db_output.mh_ass m ON ass.CodedAssToolType = m.Active_Concept_ID_SNOMED 
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---IDENTIFY FIRST OCCURANCE OF EACH EVENT (ACROSS PATHWAY)
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_subs_ranked
 SELECT 
 s.Person_ID,
 s.SpellID,
 s.StartDate,
 s.EndDate,
 s.Der_EventCategory,
 MIN(s.Der_EventDate) AS First_Event
  
 FROM $db_output.cmh_4ww_subs s 
 GROUP BY s.Person_ID, s.SpellID, s.StartDate, s.EndDate, s.Der_EventCategory 

# COMMAND ----------

 %sql
 ---COMBINE INTO MASTER EPISODE LEVEL TABLE
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_spell_master
  
 SELECT 
 g.Person_ID,
 g.OrgIDProv,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate AS EndDate,
 c.Refs,
 c.Reason_ND,
 c.Reason_ASD,
 c.Open, -- 1 if open at end of RP
 p1.CareContDate AS First_contact,
 p2.CareContDate AS Second_contact, -- used as proxy
 s1.First_Event AS First_outcome_pathway,
 s2.First_Event AS First_assessment_pathway,
 s3.First_Event AS First_Care_Plan_or_Intervention_pathway,
 CASE----Needs amending so that meaningful assessment requirement is not needed for clockstop for referrals/spells that started before April 2024 --- IS this just for CMH?
     ---Meaningful assessment required for clockstop for patient spells which started from April 2024 onwards
     WHEN g.StartDate >= '2024-04-01' AND p1.CareContDate >= s1.First_Event AND p1.CareContDate >= s2.First_Event AND p1.CareContDate >= s3.First_Event THEN p1.CareContDate
     WHEN g.StartDate >= '2024-04-01' AND s1.First_Event >= p1.CareContDate AND s1.First_Event >= s2.First_Event AND s1.First_Event >= s3.First_Event THEN s1.First_Event
     WHEN g.StartDate >= '2024-04-01' AND s2.First_Event >= p1.CareContDate AND s2.First_Event >= s1.First_Event AND s2.First_Event >= s3.First_Event THEN s2.First_Event
     WHEN g.StartDate >= '2024-04-01' AND s3.First_Event >= p1.CareContDate AND s3.First_Event >= s1.First_Event AND s3.First_Event >= s2.First_Event THEN s3.First_Event
     ---Meaningful assessment still required for patient spells prior to April 2024 but that havent had a care contact as in theory can still flow SNOMED 
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate >= '2024-04-01' AND p1.CareContDate >= s1.First_Event AND p1.CareContDate >= s2.First_Event AND p1.CareContDate >= s3.First_Event THEN p1.CareContDate
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate >= '2024-04-01' AND s1.First_Event >= p1.CareContDate AND s1.First_Event >= s2.First_Event AND s1.First_Event >= s3.First_Event THEN s1.First_Event
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate >= '2024-04-01' AND s2.First_Event >= p1.CareContDate AND s2.First_Event >= s1.First_Event AND s2.First_Event >= s3.First_Event THEN s2.First_Event
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate >= '2024-04-01' AND s3.First_Event >= p1.CareContDate AND s3.First_Event >= s1.First_Event AND s3.First_Event >= s2.First_Event THEN s3.First_Event
     ---Meaningful assessment not required for clockstop for patient spells which started before April 2024 but had contact prior to 2024 (i.e. not expected to flow SNOMED)
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate < '2024-04-01' AND p1.CareContDate >= s1.First_Event AND p1.CareContDate >= s3.First_Event THEN p1.CareContDate
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate < '2024-04-01' AND s1.First_Event >= p1.CareContDate AND s1.First_Event >= s3.First_Event THEN s1.First_Event
     WHEN g.StartDate < '2024-04-01' and p1.CareContDate < '2024-04-01' AND s3.First_Event >= p1.CareContDate AND s3.First_Event >= s1.First_Event THEN s3.First_Event
 ELSE NULL
 END AS Pathway_ClockStop 
  
 FROM $db_output.cmh_4ww_groups g 
  
 LEFT JOIN $db_output.cmh_4ww_spell_rank p1 ON g.SpellID = p1.SpellID  AND p1.Der_ContactOrder = 1
 LEFT JOIN $db_output.cmh_4ww_spell_rank p2 ON g.SpellID = p2.SpellID  AND p2.Der_ContactOrder = 2
 LEFT JOIN $db_output.cmh_4ww_subs_ranked s1 ON g.SpellID = s1.SpellID AND s1.Der_EventCategory = 'Baseline outcome'
 LEFT JOIN $db_output.cmh_4ww_subs_ranked s2 ON g.SpellID = s2.SpellID AND s2.Der_EventCategory = 'Meaningful Assessment'
 LEFT JOIN $db_output.cmh_4ww_subs_ranked s3 ON g.SpellID = s3.SpellID AND s3.Der_EventCategory = 'Care Plan or Intervention'
  
 LEFT JOIN 
     (SELECT 
     g.SpellID, 
     COUNT(*) AS Refs, 
     MAX(CASE WHEN ServDischDate IS NULL AND ReportingPeriodEndDate = '$rp_enddate' THEN 1
              WHEN DATEDIFF('$rp_enddate', EndDate) <= 5 AND ReportingPeriodEndDate = '$rp_enddate' THEN 1 ---adding logic so that if enddate is 5 days before RP End it is still classed as open
              ELSE 0 END) AS Open, --spell is open if >0 referrals are open 
     MAX(r.Reason_ND) AS Reason_ND, MAX(r.Reason_ASD) AS Reason_ASD
     FROM $db_output.cmh_4ww_groups g 
     INNER JOIN $db_output.cmh_4ww_referrals r ON r.Person_ID = g.Person_ID AND r.OrgIDProv = g.OrgIDProv AND r.ReferralRequestReceivedDate BETWEEN g.StartDate AND g.Der_EndDate AND r.Der_LatestRecord = 1 
     GROUP BY g.SpellID
     ) c ON g.SpellID = c.SpellID
 WHERE
 g.StartDate >= '2016-01-01'

# COMMAND ----------

 %sql
 ---EXPLODE INTO LONG-FORM TABLE FOR TIME SERIES ANALYSIS 
 INSERT OVERWRITE TABLE $db_output.cmh_4ww_spell_master_long
 SELECT 
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 m.Person_ID,
 m.OrgIDProv,
 COALESCE(o.NAME, "UNKNOWN") as Provider_Name,
 COALESCE(s.STP_CODE, 'UNKNOWN') AS STP_Code,
 COALESCE(s.STP_NAME, 'UNKNOWN') AS STP_Name,
 m.SpellID,
 m.StartDate,
 m.EndDate,
 m.Open AS Der_Open,
 DATEDIFF('$rp_enddate', m.StartDate) as Time_start_to_end_rp,
 (DATEDIFF('$rp_enddate', m.StartDate)) / 7 as Weeks_To_End_RP,
 m.Second_contact,
 DATEDIFF(m.Second_contact, m.StartDate) as Time_to_second_contact,
 m.First_outcome_pathway,
 m.First_assessment_pathway,
 m.First_Care_Plan_or_Intervention_pathway,
 m.Pathway_ClockStop,
 DATEDIFF(m.Pathway_Clockstop, m.StartDate) as Time_to_clock_stop,
 CASE WHEN m.StartDate BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Spell_start,
 CASE WHEN m.EndDate BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate AND m.Open = 0 THEN 1 ELSE 0 END AS Spell_closed,
 CASE WHEN m.EndDate = h.ReportingPeriodEndDate AND m.Open = 1 AND h.ReportingPeriodEndDate < '$rp_enddate' THEN 1 ELSE 0 END AS Spell_inactive,
 CASE WHEN (m.EndDate > h.ReportingPeriodEndDate) 
           OR (m.EndDate between DATE_ADD(h.ReportingPeriodEndDate, -5) AND h.ReportingPeriodEndDate AND m.Open = 1 AND h.ReportingPeriodEndDate = '$rp_enddate') THEN 1 ELSE 0 END AS Spell_Open,
 CASE WHEN m.First_contact BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS 1st_contact_in_RP,
 CASE WHEN m.First_contact <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END as With_1st_contact,
 CASE WHEN m.Second_contact BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS 2nd_contact_in_RP,
 CASE WHEN m.Second_contact <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_2nd_contact,
 CASE WHEN m.First_outcome_pathway BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Outcome_in_RP, 
 CASE WHEN m.First_outcome_pathway <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_outcome, 
 CASE WHEN m.First_assessment_pathway BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Assessment_in_RP,
 CASE WHEN m.First_assessment_pathway <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_assessment,
 CASE WHEN m.First_Care_Plan_or_Intervention_pathway BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Intervention_or_CP_in_RP,
 CASE WHEN m.First_Care_Plan_or_Intervention_pathway <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_Intervention_or_CP,
 CASE WHEN m.Pathway_ClockStop BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Clock_stop_in_RP,
 CASE WHEN m.Pathway_ClockStop <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_clock_stop --- in treatment? 
  
 FROM (
 SELECT MIN(ReportingPeriodStartDate) as ReportingPeriodStartDate, MAX(ReportingPeriodEndDate) AS ReportingPeriodEndDate
 FROM $db_source.MHS000Header WHERE ReportingPeriodEndDate between '$rp_startdate_qtr' and '$rp_enddate'
 ) h
  
 INNER JOIN $db_output.cmh_4ww_spell_master m ON m.StartDate <= h.ReportingPeriodEndDate AND m.EndDate >= h.ReportingPeriodStartDate 
  
 LEFT JOIN $db_output.bbrb_org_daily_latest o ON m.OrgIDProv = o.ORG_CODE ---provider reference data
  
 LEFT JOIN $db_output.bbrb_ccg_in_quarter c on m.Person_ID = c.Person_ID
  
 LEFT JOIN $db_output.bbrb_stp_mapping s on c.SubICBGPRes = s.CCG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cmh_4ww_spell_master_long

# COMMAND ----------

 %sql
 ---GET ALL CYP REFERRALS IN REPORTING PERIOD 
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_referrals
 SELECT
 f.ReportingPeriodStartDate,
 f.ReportingPeriodEndDate,
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID, --Calculation of Der_PersonID?
 m.OrgIDSubICBLocResidence,
 r.RecordNumber,
 r.UniqServReqID,
 r.ReferralRequestReceivedDate,
 r.ServDischDate,
 CASE WHEN r.ServDischDate IS NULL OR r.ServDischDate > f.ReportingPeriodEndDate THEN f.ReportingPeriodEndDate ELSE r.ServDischDate END AS Der_EndDate,
 s.ReferClosReason,
 s.ReferRejectionDate,
 s.ReferRejectReason,
 r.SourceOfReferralMH,
 s.ServTeamTypeRefToMH,
 CASE WHEN r.PrimReasonReferralMH = '24' THEN 1 ELSE 0 END Reason_ND,
 CASE WHEN r.PrimReasonReferralMH = '25' THEN 1 ELSE 0 END Reason_ASD,
 CASE WHEN s.ServTeamTypeRefToMH = 'C03' THEN 'C10' ELSE s.ServTeamTypeRefToMH END AS Der_ServTeamTypeRefToMH, --Why is this transformed?
 s.UniqCareProfTeamID,
 ROW_NUMBER()OVER(PARTITION BY r.Person_ID, r.UniqServReqID ORDER BY r.UniqMonthID DESC) AS Der_LatestRecord
  
 FROM $db_source.MHS101Referral r
  
 LEFT JOIN (select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate from $db_source.MHS000header) f on r.uniqmonthid = f.uniqmonthid
  
 LEFT JOIN $db_output.ServiceTeamType s ON r.RecordNumber = s.RecordNumber AND r.UniqServReqID = s.UniqServReqID
  
 INNER JOIN $db_source.MHS001MPI m ON r.RecordNumber = m.RecordNumber
  
 LEFT JOIN $db_source.MHS501HospProvSpell h on r.uniqservreqid = h.uniqservreqid and r.person_id = h.person_id AND h.uniqmonthid <= '$end_month_id'
  
 WHERE r.AgeServReferRecDate BETWEEN 0 AND 17 -- 18 and over
     AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = "") -- only people resident in England
     AND r.UniqMonthID <= $end_month_id
     AND h.UniqHospProvSpellID is null --Added to remove inpatients instead of using the delete statement
     AND r.OrgIDProv <>'DFC' --Remove Kooth
     AND CASE WHEN s.ServTeamTypeRefToMH = 'A18' AND r.ClinRespPriorityType IN ('1', '4', '2') THEN 1 ELSE 0 END = 0  --Flag referrals made to SPA teams with priority Emergency, Urgent/Serious, Very Urgent- to be excluded 
     AND CASE WHEN r.PrimReasonReferralMH = '12' AND (s.ServTeamTypeRefToMH NOT IN ('A18','F01') OR s.ServTeamTypeRefToMH IS NULL) THEN 1 ELSE 0 END = 0  --Flag ED referrals that were made to non-SPA/non-MHST team types - to be excluded 
     AND CASE WHEN r.PrimReasonReferralMH = '01' AND s.ServTeamTypeRefToMH = 'A14' THEN 1 ELSE 0 END = 0  --Flag referrals made to EIP teams with a referral reason of suspected FEP - to be excluded 
     AND CASE WHEN s.ServTeamTypeRefToMH IN ('A02','A03','A04','A11','A19','A20','A21','A22','A23','A24')  THEN 1 ELSE 0 END = 0 --Flag referrals made to crisis team types - to be excluded

# COMMAND ----------

 %sql
 ---SPLIT OUT START AND END DATES FOR EACH REFERRAL
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_dates
 SELECT 
 Person_ID,
 OrgIDProv,
 UniqServReqID,
 ReferralRequestReceivedDate AS Der_Date, -- start dates
 1 AS Inc -- coded as +1
  
 FROM $db_output.cyp_4ww_referrals  
 WHERE Der_LatestRecord = 1 
  
 UNION ALL
  
 SELECT 
 Person_ID,
 OrgIDProv,
 UniqServReqID,
 DATE_ADD(Der_EndDate,5) AS Der_Date, -- end dates : allow up to 5 days between referrals 
 -1 AS Inc -- coded as -1
  
 FROM $db_output.cyp_4ww_referrals   
 WHERE Der_LatestRecord = 1 

# COMMAND ----------

 %sql
 ---GET CUMULATIVE FREQUENCY FOR EACH SPELL 
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_cumf
 SELECT 
 Person_ID, 
 OrgIDProv, 
 Der_Date, 
 SUM(SUM(Inc)) OVER (ORDER BY Person_ID, OrgIDProv, Der_Date) AS Cumf_Inc
 --- the above gets cumulative sum of Inc the field across a person-provider combination ordered by Der_Date (ReferralRequestReceivedDate (1) or ServDischDate/ReportingPeriodEndDate (-1))
 --- if a person has had multiple referrals which overlap the Cumf_Inc will be > 1
 --- when multiple referrals that overlap have ended, the latest Der_Date is where Cumf_Inc will be 0
  
 FROM $db_output.cyp_4ww_dates   
 GROUP BY Person_ID, OrgIDProv, Der_Date

# COMMAND ----------

 %sql
 ---GROUP INTO SPELLS 
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_groups 
 SELECT 
 Person_ID,
 OrgIDProv,
 Inc_Group, ---Inc Group separates out multiple spells
 CONCAT(Person_ID, OrgIDProv, Inc_Group) AS SpellID, 
 MIN(Der_Date) AS StartDate, ---Earliest Date will be start of spell
 MAX(Der_Date) AS EndDate, ---Latest Date will be end of spell
 DATE_ADD(MAX(Der_Date), -5) AS Der_EndDate ---Take back 5 days which were added previously
  
 FROM 
 (
   SELECT 
   * 
   ,SUM(CASE WHEN Cumf_Inc = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY Der_Date DESC) AS Inc_Group
   ---The Inc_Group field separates out person-provider combinations that have ended (Cumf_Inc = 0) assigns them as 1 and then partitions
   --- i.e. a first spells rows will be assigned as 1 and each time that person-provider combination has a new spell Inc_Group will be +1, 
   FROM $db_output.cyp_4ww_cumf
 ) x 
  
 GROUP BY Person_ID, OrgIDProv, Inc_Group

# COMMAND ----------

 %sql
 ---RANK CONTACTS TO GET SECOND CONTACT WITHIN SPELL
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_spell_contacts
 SELECT
 'DIR' as Der_Activity_Type,
 g.Person_ID,
 g.OrgIDProv,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate AS EndDate,
 c.CareContDate as ContactDate,
 c.UniqServReqID AS Contact_Ref
  
 FROM $db_output.cyp_4ww_groups g 
  
 INNER JOIN $db_source.MHS201CareContact c ON g.Person_ID = c.Person_ID
     AND g.OrgIDProv = c.OrgIDProv
     AND c.CareContDate BETWEEN g.StartDate AND g.Der_EndDate ---Actual end date (5 days taken off)
     AND c.AttendStatus IN ('5','6') 
     AND ((c.UniqMonthID < 1459 AND c.ConsMechanismMH NOT IN ('05','06')) OR (c.UniqMonthID >= 1459 AND c.ConsMechanismMH IN ('01', '02', '04', '11')))
  
 INNER JOIN $db_output.cyp_4ww_referrals r ON r.UniqServReqID = c.UniqServReqID AND r.RecordNumber = c.RecordNumber 
 -- contacts must be with in-scope teams  
 --Does using uniqservreqid and recordnumber work here since the referrals are ranked out so you would only have one referral month not all meaning some contacts might be missed? No - latest record not taken yet so is RecordNumber even needed?

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_indirect_activity
 SELECT
 i.Person_ID,
 i.OrgIDProv,
 i.IndirectActDate,
 i.RecordNumber,
 i.UniqServReqID,
 ROW_NUMBER () OVER(PARTITION BY i.UniqServReqID, i.IndirectActDate, i.IndirectActTime ORDER BY i.MHS204UniqID DESC) AS Der_ActRN
  
 FROM $db_source.MHS204IndirectActivity i 
  
 INNER JOIN $db_output.cyp_4ww_referrals r ON r.UniqServReqID = i.UniqServReqID AND r.RecordNumber = i.RecordNumber -- contacts must be with in-scope teams ---potentially join on UniqOtherCareProfTeamID (headache)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cyp_4ww_spell_contacts
 SELECT
 'IND' AS Der_ActivityType,
 g.Person_ID,
 g.OrgIDProv,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate AS EndDate,
 i.IndirectActDate AS ContactDate,
 i.UniqServReqID AS Contact_ref
  
 FROM $db_output.cyp_4ww_groups g 
  
 INNER JOIN $db_output.cyp_4ww_indirect_activity i ON g.Person_ID = i.Person_ID
     AND g.OrgIDProv = i.OrgIDProv
     AND i.IndirectActDate BETWEEN g.StartDate AND g.Der_EndDate
  
 WHERE i.Der_ActRN = 1

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_spell_rank
 SELECT
 c.Person_ID,
 c.OrgIDProv,
 c.SpellID,
 c.StartDate,
 c.EndDate,
 c.ContactDate,
 c.Contact_ref,
 ROW_NUMBER() OVER (PARTITION BY c.SpellID ORDER BY c.ContactDate) AS Der_ContactOrder 
  
 FROM $db_output.cyp_4ww_spell_contacts c
 WHERE c.StartDate >= '2016-01-01'

# COMMAND ----------

 %sql
 ---double check if same as CMH
 CREATE OR REPLACE TEMPORARY VIEW MHSDS_SNOMED_REFSET AS 
 SELECT 
 CASE 
   WHEN RefSetID = '1853441000000109' THEN 'Assessments'--Mental Health Services Data Set assessment procedures simple reference set
   WHEN RefSetID = '1853461000000105' THEN 'Psychological therapies'--Mental Health Services Data Set psychological therapies simple reference set
   WHEN RefSetID =  '1853481000000101' THEN 'Psychosocial interventions' --Mental Health Services Data Set psychosocial interventions simple reference set
   WHEN RefSetID = '1853451000000107' THEN 'Medication'
   END AS Ref_Group,
 CASE 
   WHEN RefSetID = '1853441000000109' THEN 'Mental Health Services Data Set assessment procedures simple reference set'  
   WHEN RefSetID = '1853461000000105' THEN 'Mental Health Services Data Set psychological therapies simple reference set'
   WHEN RefSetID =  '1853481000000101' THEN 'Mental Health Services Data Set psychosocial interventions simple reference set' 
   WHEN RefSetID = '1853451000000107' THEN 'Mental Health Services Data Set medication and physical therapy interventions simple reference set'
   END AS Reference_Set,
 RefSetID,
 ReferencedComponentID as SNOMED_Code,
 b.term as SNOMED_Name
 FROM 
 $reference_data.SNOMED_SCT2_REFSET_FULL a
 left join 
 (SELECT 
 *,
 ROW_NUMBER()OVER(PARTITION BY conceptid ORDER BY Active desc, EffectiveTime desc, ID desc, DSS_KEY desc) as RN
 FROM
 $reference_data.snomed_sct2_description_full) b on a.referencedcomponentid = b.conceptid and b.active = '1' and b.RN = '1'
 WHERE
  a.RefSetID IN ('1853441000000109', --Mental Health Services Data Set assessment procedures simple reference set
                 '1853461000000105', --Mental Health Services Data Set psychological therapies simple reference set
                 '1853481000000101', --Mental Health Services Data Set psychosocial interventions simple reference set
                 '1853451000000107')--Mental Health Services Data Set medication and physical therapy interventions simple reference set
 and a.Active = '1'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_subs
 SELECT
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Care Plan or Intervention' AS Der_EventCategory,
 'Care Plan' AS Der_EventType,
 MIN(CASE
     WHEN (c.CarePlanCreatDate >= g.StartDate AND c.CarePlanCreatDate <= c.CarePlanLastUpdateDate) THEN c.CarePlanCreatDate --Where the care plan was created during the referral - use creation date
     ELSE c.CarePlanLastUpdateDate END) --Else use the lsat updated date (for care plans that were created before the referral started, but then updated during the referral
     AS Der_EventDate-- date that care plan was agreed   
  
 FROM $db_output.cyp_4ww_groups g
  
 INNER JOIN $db_source.MHS008CarePlanType c ON g.Person_ID = c.Person_ID AND g.OrgIDProv = c.OrgIDProv AND COALESCE(c.CarePlanLastUpdateDate,c.CarePlanCreatDate) BETWEEN g.StartDate AND g.Der_EndDate
  
 INNER JOIN $db_source.MHS009CarePlanAgreement cpa ON cpa.RecordNumber = c.RecordNumber AND cpa.UniqCarePlanID = c.UniqCarePlanID -- only agreed care plans
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET DIRECT CARE ACTIVITY DATES
 INSERT INTO $db_output.cyp_4ww_subs
  
 SELECT
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate AS EndDate,
 'Care Plan or Intervention' AS Der_EventCategory, ---CYP assessments being assigned as Care Plan or Intervention (Grace to confirm)
 'Intervention' AS Der_EventType,
 MIN(c.CareContDate) AS Der_EventDate
  
 FROM $db_output.cyp_4ww_groups g
  
 INNER JOIN $db_source.MHS201CareContact c ON g.Person_ID = c.Person_ID AND g.OrgIDProv = c.OrgIDProv AND c.CareContDate BETWEEN g.StartDate AND g.Der_EndDate
  
 INNER JOIN $db_source.MHS202CareActivity i ON c.UniqCareContID = i.UniqCareContID AND c.RecordNumber = i.RecordNumber
  
 INNER JOIN $db_output.cyp_4ww_referrals r ON i.UniqServReqID = r.UniqServReqID AND r.RecordNumber = i.RecordNumber 
  
 INNER JOIN MHSDS_SNOMED_REFSET s ON
 --MHDInternal.Staging_MH_PRSB_SNOMED_referralset s ON --NEED TO GET THE SNOMED REFSET HERE--
         (CASE
         WHEN position(':',i.Procedure) > 0
         THEN LEFT(i.Procedure, position(':',i.Procedure)-1)
         ELSE i.Procedure
         END) = s.SNOMED_CODE ---CHARINDEX in spark sql previously we have used POSITION instead? Could be a difference there? Edit: CHARINDEX has been added to Spark 3 :)
  
 WHERE i.Procedure IS NOT NULL 
 AND (s.Ref_Group IN 
     ('Medication',
     'Psychological therapies',
     'Psychosocial interventions')
     OR (s.Ref_Group = 'Assessments' AND (s.SNOMED_code = '1085671000000109' or s.SNOMED_code = '1914891000000100'))) --Allow only ASD assessments from the Assessments set to stop the clock - Code covers ADHD and ASD assessments
     
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET INDIRECT CARE ACTIVITY DATES
 INSERT INTO $db_output.cyp_4ww_subs
  
 SELECT
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Care Plan or Intervention' AS Der_EventCategory, ---CYP assessments being assigned as Care Plan or Intervention (Grace to confirm)
 'Intervention' AS Der_EventType,
 MIN(i.IndirectActDate) AS Der_EventDate
  
 FROM $db_output.cyp_4ww_groups g
  
 INNER JOIN $db_source.MHS204IndirectActivity i ON g.Person_ID = i.Person_ID AND g.OrgIDProv = i.OrgIDProv AND i.IndirectActDate BETWEEN g.StartDate AND g.Der_EndDate
  
 INNER JOIN $db_output.cyp_4ww_referrals r ON i.UniqServReqID = r.UniqServReqID AND r.RecordNumber = i.RecordNumber 
  
 INNER JOIN MHSDS_SNOMED_REFSET s ON
         (CASE
         WHEN position(':',i.IndActProcedure) > 0
         THEN LEFT(i.IndActProcedure, position(':',i.IndActProcedure)-1)
         ELSE i.IndActProcedure
         END)  = s.SNOMED_CODE
  
 WHERE 
     i.IndActProcedure IS NOT NULL
     AND (s.Ref_Group IN 
         ('Medication',
         'Psychological therapies',
         'Psychosocial interventions')
         OR (s.Ref_Group = 'Assessments' AND (s.SNOMED_code = '1085671000000109' or s.SNOMED_code = '1914891000000100'))) --Allow ASD assessment to stop the clock, or any other intervention - will need to add in ADHD assessment too, once code available
         ----Can we have Indirect ASD/ADHD Assessments? maybe remove for clarity confirm with Grace
         
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET REFERRAL ASSESSMENT DATES
 INSERT INTO $db_output.cyp_4ww_subs
  
 SELECT 
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Baseline outcome' AS Der_EventCategory,
 'Assessment - Outcome' AS Der_EventType,
 MIN(CAST(ass.AssToolCompTimestamp AS DATE)) AS Der_EventDate
  
 FROM $db_output.cyp_4ww_groups g
  
 INNER JOIN $db_source.MHS606CodedScoreAssessmentRefer ass ON g.Person_ID = ass.Person_ID AND g.OrgIDProv = ass.OrgIDProv AND CAST(ass.AssToolCompTimestamp AS DATE) BETWEEN g.StartDate AND g.Der_EndDate
  
 INNER JOIN $db_output.cyp_4ww_referrals r ON ass.RecordNumber = r.RecordNumber AND r.UniqServReqID = ass.UniqServReqID
  
 INNER JOIN $db_output.mh_ass m ON ass.CodedAssToolType = m.Active_Concept_ID_SNOMED --NEED TO GET SNOMED REFSETS HERE--
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET CARE ACTIVITY ASSESSMENT DATES
 INSERT INTO $db_output.cyp_4ww_subs
  
 SELECT 
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Baseline outcome' AS Der_EventCategory,
 'Assessment - Outcome' AS Der_EventType,
 MIN(c.CareContDate) AS Der_EventDate
  
 FROM $db_output.cyp_4ww_groups g 
  
 INNER JOIN $db_source.MHS201CareContact c ON g.Person_ID = c.Person_ID AND g.OrgIDProv = c.OrgIDProv AND c.CareContDate BETWEEN g.StartDate AND g.Der_EndDate
  
 INNER JOIN $db_output.cyp_4ww_referrals r ON c.RecordNumber = r.RecordNumber AND c.UniqServReqID = r.UniqServReqID
  
 INNER JOIN $db_source.MHS607CodedScoreAssessmentAct ass ON c.UniqCareContID = ass.UniqCareContID AND c.RecordNumber = ass.RecordNumber
  
 INNER JOIN $db_output.mh_ass m ON ass.CodedAssToolType = m.Active_Concept_ID_SNOMED   --NEED TO GET SNOMED REFSETS HERE-- 
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---GET CARE CLUSTER DATES (NOT POPULATED AS OF MHSDS V6)
 INSERT INTO $db_output.cyp_4ww_subs
  
 SELECT 
 g.Person_ID,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate as EndDate,
 'Baseline outcome' AS Der_EventCategory,
 'Assessment - Outcome' AS Der_EventType,
 MIN(ct.AssToolCompDate) AS Der_EventDate
  
 FROM $db_output.cyp_4ww_groups g
  
 INNER JOIN $db_source.MHS801ClusterTool ct ON g.Person_ID = ct.Person_ID AND g.OrgIDProv = ct.OrgIDProv AND ct.AssToolCompDate BETWEEN g.StartDate AND g.Der_EndDate
  
 INNER JOIN $db_source.MHS802ClusterAssess ass ON ct.UniqClustID = ass.UniqClustID AND ct.RecordNumber = ass.RecordNumber 
  
 INNER JOIN $db_output.mh_ass m ON ass.CodedAssToolType = m.Active_Concept_ID_SNOMED 
  
 -- INNER JOIN $db_output.cyp_4ww_referrals r ON ass.RecordNumber = r.RecordNumber AND ct.AssToolCompDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate,r.ReportingPeriodEndDate)
  
 GROUP BY g.Person_ID, g.SpellID, g.StartDate, g.Der_EndDate

# COMMAND ----------

 %sql
 ---IDENTIFY FIRST OCCURANCE OF EACH EVENT (ACROSS PATHWAY)
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_subs_ranked
 SELECT 
 s.Person_ID,
 s.SpellID,
 s.StartDate,
 s.EndDate,
 s.Der_EventCategory,
 MIN(s.Der_EventDate) AS First_Event
  
 FROM $db_output.cyp_4ww_subs s 
 GROUP BY s.Person_ID, s.SpellID, s.StartDate, s.EndDate, s.Der_EventCategory 

# COMMAND ----------

 %sql
 ---COMBINE INTO MASTER EPISODE LEVEL TABLE
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_spell_master
  
 SELECT 
 g.Person_ID,
 g.OrgIDProv,
 g.SpellID,
 g.StartDate,
 g.Der_EndDate AS EndDate,
 c.Refs,
 c.Reason_ND,
 c.Reason_ASD,
 c.Open, -- 1 if open at end of RP
 p1.ContactDate AS First_contact,
 s1.First_Event AS First_outcome_pathway,
 s2.First_Event AS First_Care_Plan_or_Intervention_pathway,
 CASE    
   WHEN p1.ContactDate >= s1.First_Event AND p1.ContactDate >= s2.First_Event THEN p1.ContactDate
   WHEN s1.First_Event >= p1.ContactDate AND s1.First_Event >= s2.First_Event THEN s1.First_Event
   WHEN s2.First_Event >= p1.ContactDate AND s2.First_Event >= s1.First_Event THEN s2.First_Event
 ELSE NULL
 END AS Pathway_ClockStop  
  
 FROM $db_output.cyp_4ww_groups g 
  
 LEFT JOIN $db_output.cyp_4ww_spell_rank p1 ON g.SpellID = p1.SpellID  AND p1.Der_ContactOrder = 1
 LEFT JOIN $db_output.cyp_4ww_subs_ranked s1 ON g.SpellID = s1.SpellID AND s1.Der_EventCategory = 'Baseline outcome'
 LEFT JOIN $db_output.cyp_4ww_subs_ranked s2 ON g.SpellID = s2.SpellID AND s2.Der_EventCategory = 'Care Plan or Intervention'
  
 LEFT JOIN 
     (SELECT 
     g.SpellID, 
     COUNT(*) AS Refs, 
     MAX(CASE WHEN ServDischDate IS NULL AND ReportingPeriodEndDate = '$rp_enddate' THEN 1
              WHEN DATEDIFF('$rp_enddate', EndDate) <= 5 AND ReportingPeriodEndDate = '$rp_enddate' THEN 1 ---adding logic so that if enddate is 5 days before RP End it is still classed as open
              ELSE 0 END) AS Open, 
     --spell is open if >0 referrals are open.. Added code to pick up those that have ended within 5 days of rp end
     MAX(r.Reason_ND) AS Reason_ND, 
     MAX(r.Reason_ASD) AS Reason_ASD
     FROM $db_output.cyp_4ww_groups g 
     INNER JOIN $db_output.cyp_4ww_referrals r ON r.Person_ID = g.Person_ID AND r.OrgIDProv = g.OrgIDProv AND r.ReferralRequestReceivedDate BETWEEN g.StartDate AND g.Der_EndDate AND r.Der_LatestRecord = 1 
     GROUP BY g.SpellID
     ) c ON g.SpellID = c.SpellID
     
 WHERE g.StartDate >= '2016-01-01' ---if any referral that is part of spell started before 2016 then the entire spell is removed

# COMMAND ----------

 %sql
 ---EXPLODE INTO LONG-FORM TABLE FOR TIME SERIES ANALYSIS 
 INSERT OVERWRITE TABLE $db_output.cyp_4ww_spell_master_long
 SELECT 
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 m.Person_ID,
 m.OrgIDProv,
 COALESCE(o.NAME, "UNKNOWN") as Provider_Name,
 COALESCE(s.STP_CODE, 'UNKNOWN') AS STP_Code,
 COALESCE(s.STP_NAME, 'UNKNOWN') AS STP_Name,
 m.SpellID,
 m.StartDate,
 m.EndDate,
 m.Open AS Der_Open,
 DATEDIFF('$rp_enddate', m.StartDate) as Time_start_to_end_rp,
 (DATEDIFF('$rp_enddate', m.StartDate)) / 7 as Weeks_To_End_RP,
 m.First_contact,
 m.First_outcome_pathway,
 m.First_Care_Plan_or_Intervention_pathway,
 m.Pathway_ClockStop,
 DATEDIFF(m.Pathway_Clockstop, m.StartDate) as Time_to_clock_stop,
 CASE WHEN m.StartDate BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Spell_start,
 CASE WHEN m.EndDate BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate AND m.Open = 0 THEN 1 ELSE 0 END AS Spell_closed,
 CASE WHEN m.EndDate = h.ReportingPeriodEndDate AND m.Open = 1 AND h.ReportingPeriodEndDate < '$rp_enddate' THEN 1 ELSE 0 END AS Spell_inactive,
 CASE WHEN (m.EndDate > h.ReportingPeriodEndDate) 
           OR (m.EndDate between DATE_ADD(h.ReportingPeriodEndDate, -5) AND h.ReportingPeriodEndDate AND m.Open = 1 AND h.ReportingPeriodEndDate = '$rp_enddate') THEN 1 ELSE 0 END AS Spell_Open,
 CASE WHEN m.First_contact BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS 1st_contact_in_RP,
 CASE WHEN m.First_contact <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END as With_1st_contact,
 CASE WHEN m.First_outcome_pathway BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Outcome_in_RP, 
 CASE WHEN m.First_outcome_pathway <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_outcome, 
 CASE WHEN m.First_Care_Plan_or_Intervention_pathway BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Intervention_or_CP_in_RP,
 CASE WHEN m.First_Care_Plan_or_Intervention_pathway <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_Intervention_or_CP,
 CASE WHEN m.Pathway_ClockStop BETWEEN h.ReportingPeriodStartDate AND h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Clock_stop_in_RP,
 CASE WHEN m.Pathway_ClockStop <= h.ReportingPeriodEndDate THEN 1 ELSE 0 END AS With_clock_stop --- in treatment? 
  
 FROM (
 SELECT MIN(ReportingPeriodStartDate) as ReportingPeriodStartDate, MAX(ReportingPeriodEndDate) AS ReportingPeriodEndDate
 FROM $db_source.MHS000Header WHERE ReportingPeriodEndDate between '$rp_startdate_qtr' and '$rp_enddate'
 ) h
  
 INNER JOIN $db_output.cyp_4ww_spell_master m ON m.StartDate <= h.ReportingPeriodEndDate AND m.EndDate >= h.ReportingPeriodStartDate 
  
 LEFT JOIN $db_output.bbrb_org_daily_latest o ON m.OrgIDProv = o.ORG_CODE ---provider reference data
  
 LEFT JOIN $db_output.bbrb_ccg_in_quarter c on m.Person_ID = c.Person_ID
  
 LEFT JOIN $db_output.bbrb_stp_mapping s on c.SubICBGPRes = s.CCG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cyp_4ww_spell_master_long