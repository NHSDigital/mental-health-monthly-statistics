# Databricks notebook source
 %run ../../mhsds_functions

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_closed_referrals
 SELECT 
 DISTINCT ---selecting distinct as same referral could have been referred to different servteamtype/careprofteamid in month
  r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.ReportingPeriodEndDate
 ,r.OrgIDProv AS Provider_Code 
 ,COALESCE(o.NAME,'UNKNOWN') AS Provider_Name
 ,COALESCE(c.STP21CDH,'UNKNOWN') AS ICB_Code
 ,COALESCE(c.STP21NM,'UNKNOWN') AS ICB_Name
 ,COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code
 ,COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name
 ,r.LADistrictAuth
 ,r.AgeServReferRecDate
 ,CASE WHEN r.Gender = '1' THEN 'M' WHEN r.Gender = '2' THEN 'F' ELSE NULL END as Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate
 ,r.ReferRejectionDate
 ,i.Der_HospSpellCount    
  
 FROM $db_output.nhse_pre_proc_referral r 
  
 LEFT JOIN (
   SELECT i.Person_ID, i.UniqServReqID, COUNT(i.Der_HospSpellRecordOrder) AS Der_HospSpellCount
   FROM $db_output.Der_NHSE_Pre_Proc_Inpatients i ---using this table as it has Der_HospSpellRecordOrder deriavtion
   GROUP BY i.Person_ID, i.UniqServReqID
   ) i ON i.Person_ID = r.Person_ID AND i.UniqServReqID = r.UniqServReqID ---to indentify referrals to inpatient services (and subsequently remove) 
  
 LEFT JOIN $db_output.bbrb_org_daily_latest o ON r.OrgIDProv = o.ORG_CODE ---provider reference data
  
 LEFT JOIN $db_output.bbrb_ccg_in_month ccg on r.Person_ID = ccg.Person_ID
  
 LEFT JOIN $db_output.ccg_mapping_2021 c on ccg.IC_Rec_CCG = c.CCG21CDH ---ICB and Region reference data
  
 WHERE r.AgeServReferRecDate BETWEEN 0 AND 17 ---changed from 0-18 to match CQUIN 
 AND r.UniqMonthID BETWEEN $start_month_id AND $end_month_id
 AND r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND r.ReferRejectionDate IS NULL
 AND (r.ServDischDate BETWEEN r.ReportingPeriodStartDate AND r.ReportingPeriodEndDate) ---only bring referral record for the month it was discharged in
 AND i.Der_HospSpellCount IS NULL ---to exclude referrals with an associated hospital spell 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = "") ---to limit to those people whose commissioner is an English organisation 
 AND (r.ServTeamTypeRefToMH NOT IN ('B02','E01','E02','E03','E04','A14') OR r.ServTeamTypeRefToMH IS NULL)--- exclude specialist teams *but include those where team type is null

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_closed_contacts
 SELECT 
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber 
 ,r.Provider_Code
 ,MAX(CASE WHEN a.Der_ContactOrder = 1 THEN a.Der_ContactDate ELSE NULL END) AS Contact1
 ,MAX(CASE WHEN a.Der_ContactOrder = 2 THEN a.Der_ContactDate ELSE NULL END) AS Contact2
  
 FROM $db_output.cyp_closed_referrals r  
 INNER JOIN $db_output.Der_NHSE_Pre_Proc_Activity a 
 ON r.Person_ID = a.Person_ID AND a.UniqServReqID = r.UniqServReqID AND a.Der_ContactDate <= '$rp_enddate' AND a.Der_ContactDate <= r.ServDischDate ---contact before or on day of discharge or end of reporting period
 GROUP BY r.Person_ID, r.UniqServReqID, r.RecordNumber, r.Provider_Code

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_all_assessments
 SELECT
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Provider_Code
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.Der_PreferredTermSNOMED
 ,a.Der_AssessmentToolName
 ,a.Der_AssessmentCategory
 ,a.PersScore
 ,a.Der_ValidScore
 ,mh.Der_Rater as Rater  
  
 FROM $db_output.cyp_closed_referrals r
 INNER JOIN $db_output.nhse_pre_proc_assessments_unique a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.Der_AssToolCompDate <= '$rp_enddate' AND a.Der_AssToolCompDate <= r.ServDischDate
 LEFT JOIN $db_output.cyp_outcomes_ass mh ON a.CodedAssToolType = mh.Active_Concept_ID_SNOMED
 WHERE mh.Assessment_Tool_Name IS NOT NULL and mh.Preferred_Term_SNOMED IS NOT NULL AND is_numeric(a.PersScore) = 1 

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_ref_cont_out
 SELECT 
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.Provider_Code 
 ,r.Provider_Name
 ,r.ICB_Code
 ,r.ICB_Name
 ,r.Region_Code
 ,r.Region_Name
 ,r.AgeServReferRecDate AS Age
 ,r.Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate
 ,c.Contact1
 ,c.Contact2
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.Der_PreferredTermSNOMED AS AssName
 ,a.Der_AssessmentToolName AS AssType 
 ,a.Rater
 ,a.Der_AssessmentCategory
 ,CAST(a.PersScore AS float) AS RawScore
 ,a.Der_ValidScore
  
 FROM $db_output.cyp_closed_referrals r  
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.Person_ID = c.Person_ID AND r.UniqServReqID = c.UniqServReqID AND r.RecordNumber = c.RecordNumber 
 LEFT JOIN $db_output.cyp_all_assessments a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber 

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_valid_unique_assessments_rcads
 SELECT 
 a.Person_ID
 ,a.UniqServReqID
 ,a.RecordNumber
 ,a.Der_FY
 ,a.UniqMonthID
 ,a.ReportingPeriodStartDate
 ,a.Provider_Code 
 ,a.Provider_Name
 ,a.ICB_Code
 ,a.ICB_Name
 ,a.Region_Code
 ,a.Region_Name
 ,a.Age
 ,a.Gender
 ,a.ReferralRequestReceivedDate
 ,a.ServDischDate
 ,a.Contact1
 ,a.Contact2
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.AssName
 ,a.AssType 
 ,a.Rater
 ,a.Der_AssessmentCategory
 ,a.RawScore
 ,a.Der_ValidScore
 ,CASE WHEN t.Gender IS NOT null AND t.Age IS NOT NULL AND t.AssName IS NOT NULL ---if Gender, Age and RCADS assessment are not null then transform
      THEN (
      (a.RawScore - t.Sample_Mean) 
      / t.Sample_SD ---Z score formula (RawScore - Sample Mean) / Sample Standard Distribution
      ) * 10 
      + 50 ---T score formula (Z score * 10) + 50
       ELSE a.RawScore ---only transform RCADS scores else keep same
       END as TScore 
  
 FROM $db_output.cyp_ref_cont_out a
 LEFT JOIN $db_output.rcads_transform_score t ON a.Gender = t.Gender AND a.Age = t.Age AND a.AssName = t.AssName
 WHERE a.Der_ValidScore = 'Y'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_partition_assessments
 SELECT *,
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, AssName, RecordNumber ORDER BY Der_AssToolCompDate ASC, Der_AssUniqID ASC) AS RN
  
 FROM $db_output.cyp_valid_unique_assessments_rcads
  
 WHERE Contact1 <= Der_AssToolCompDate

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_last_assessments
 SELECT 
 a.Person_ID,
 a.UniqServReqID,
 a.AssName,
 MAX(a.RN) AS max_ass
  
 FROM $db_output.cyp_partition_assessments a 
 WHERE a.RN > 1 and a.Der_AssToolCompDate >= a.Contact2  
 GROUP BY a.Person_ID, a.UniqServReqID, a.AssName

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_first_and_last_assessments
 SELECT      
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.Provider_Code 
 ,r.Provider_Name
 ,r.ICB_Code
 ,r.ICB_Name
 ,r.Region_Code
 ,r.Region_Name
 ,r.AgeServReferRecDate AS Age
 ,r.Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate       
 ,c.Contact1
 ,c.Contact2
 ,a.Der_AssessmentCategory
 ,a.Rater
 ,a.AssType
 ,a.AssName
 ,a.Der_AssUniqID AS Der_AssUniqID_first
 ,a.Der_AssToolCompDate AS AssDate_first
 ,a.TScore AS Score_first
 ,b.Der_AssUniqID AS Der_AssUniqID_last
 ,b.Der_AssToolCompDate AS AssDate_last
 ,b.TScore AS Score_last
 ,b.TScore - a.TScore AS Score_Change
 ,rct.Threshold ---if assessment doesn't have threshold (i.e. not in reference data) keep as null
  
 FROM $db_output.cyp_closed_referrals r 
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID 
 LEFT JOIN $db_output.cyp_partition_assessments a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.RN = 1 ---join to get first assessment
  
 LEFT JOIN 
     (SELECT x.* 
     FROM $db_output.cyp_partition_assessments x 
     INNER JOIN $db_output.cyp_last_assessments y 
     ON x.Person_ID = y.Person_ID 
     AND x.UniqServReqID = y.UniqServReqID 
     AND x.Person_ID = y.Person_ID 
     AND x.AssName = y.AssName 
     AND x.RN = y.max_ass) AS b ON a.Person_ID = b.Person_ID AND a.UniqServReqID = b.UniqServReqID AND a.AssName = b.AssName ---join to get last assessment
     
 LEFT JOIN $db_output.cyp_reliable_change_thresholds rct ON a.AssName = rct.AssName

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_rci
 SELECT
 a.Person_ID,
 a.UniqServReqID,
 a.RecordNumber,
 a.Der_FY,
 a.UniqMonthID,
 a.ReportingPeriodStartDate,
 a.Provider_Code,
 a.Provider_Name,
 a.ICB_Code,
 a.ICB_Name,
 a.Region_Code,
 a.Region_Name,
 a.Age,
 a.Gender,
 a.ReferralRequestReceivedDate,
 a.ServDischDate,
 a.Contact1,
 a.Contact2,
 a.Der_AssessmentCategory,
 a.Rater,
 a.AssType,
 a.AssName,
 a.Der_AssUniqID_first,
 a.AssDate_first,
 a.Score_first,
 a.Der_AssUniqID_last,
 a.AssDate_last,
 a.Score_last,
 a.Score_Change,
 a.Threshold,
 CASE WHEN s.Scale = 'Positive' AND a.Score_Change > 0 AND ABS(a.Score_Change) >= a.Threshold ---Positive means high score good symptoms (i.e. improvement means score will go up)
      THEN 1
      WHEN s.Scale = 'Negative' AND a.Score_Change < 0 AND ABS(a.Score_Change) >= a.Threshold ---Negative means high score bad symptoms (i.e. improvement means score will go down)
      THEN 1
      ELSE NULL END AS Reliable_Improvement,
 CASE WHEN s.Scale = 'Positive' AND a.Score_Change < 0 AND ABS(a.Score_Change) >= a.Threshold ---Positive means high score good symptoms (i.e. deterioration means score will go down)
      THEN 1
      WHEN s.Scale = 'Negative' AND a.Score_Change > 0 AND ABS(a.Score_Change) >= a.Threshold ---Negative means high score bad symptoms (i.e. deterioration means score will go up)
      THEN 1
      ELSE NULL END AS Reliable_Deterioration,
 CASE WHEN ABS(a.Score_Change) < a.Threshold ---If Threshold has not been met between first and last assessment scores then class as No Change
      THEN 1
      ELSE NULL END AS No_Change
 FROM $db_output.cyp_first_and_last_assessments a
 LEFT JOIN $db_output.cyp_ass_type_scaling s ON a.AssType = s.AssType

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_rci_referral
 SELECT
 r.Person_ID,
 r.UniqServReqID,
 r.Rater,
 MAX(r.Contact1) AS Contact1, ---first contact for rater
 MAX(r.Contact2) AS Contact2, ---second contact for rater
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_first IS NOT NULL THEN 1 ELSE NULL END) as Assessment, ---had at least 2 contacts and at least 1 assessment for rater
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN 1 ELSE NULL END) as Paired, ---had at least 2 contacts and at least 2 of the same assessment for the rater (paired score)
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN Reliable_Improvement ELSE NULL END) as Improvement, ---paired score with reliable improvement for rater across all assessments
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN No_Change ELSE NULL END) as NoChange, ---paired score with no change for rater across all assessments
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN Reliable_Deterioration ELSE NULL END) as Deter ---paired score with reliable deterioration for rater across all assessments
  
 FROM $db_output.cyp_rci r
 WHERE r.AssName IS NOT NULL  ---valid assessments only as per mh_ass 
 GROUP BY r.Person_ID, r.UniqServReqID, r.Rater

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_meaningful_change
 SELECT 
 Person_ID
 ,UniqServReqID
 ,Rater
 ,Contact1
 ,Contact2
 ,Assessment
 ,Paired 
 ,CASE WHEN Improvement = 1 AND Deter IS NULL THEN 1 ELSE 0 END as Improvement ---patient has improvement and not detriorated across all assessments then meaningful improvement
 ,CASE WHEN NoChange = 1 AND Improvement IS NULL AND Deter IS NULL THEN 1 ELSE 0 END as NoChange ---patient has no change and not improved/deteriorated then no change 
 ,CASE WHEN Deter = 1 THEN 1 ELSE 0 END as Deter ---patient has deteriorated at all across all assessments then meaningful deterioration
  
 FROM $db_output.cyp_rci_referral

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_master
 SELECT 
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.Provider_Code as OrgIDProv
 ,r.Provider_Name
 ,r.ICB_Code as STP_Code
 ,r.ICB_Name as STP_Name
 ,r.Region_Code
 ,r.Region_Name
 ,r.AgeServReferRecDate AS Age
 ,r.Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate  
 ,c.Contact1
 ,c.Contact2
 -- self-rated measures 
 ,m1.Assessment AS Assessment_SR
 ,m1.Paired AS Paired_SR
 ,m1.Improvement AS Improvement_SR
 ,m1.NoChange AS NoChange_SR
 ,m1.Deter AS Deter_SR
 -- parent-rated measures 
 ,m2.Assessment AS Assessment_PR
 ,m2.Paired AS Paired_PR
 ,m2.Improvement AS Improvement_PR
 ,m2.NoChange AS NoChange_PR
 ,m2.Deter AS Deter_PR
 -- clinician-rated measures
 ,m3.Assessment AS Assessment_CR
 ,m3.Paired AS Paired_CR
 ,m3.Improvement AS Improvement_CR
 ,m3.NoChange AS NoChange_CR
 ,m3.Deter AS Deter_CR
 ,CASE WHEN m1.Assessment = 1 OR m2.Assessment = 1 OR m3.Assessment = 1 THEN 1 ELSE 0 END as Assessment_ANY
 ,CASE WHEN m1.Paired = 1 OR m2.Paired = 1 OR m3.Paired = 1 THEN 1 ELSE 0 END as Paired_ANY
  
 FROM $db_output.cyp_closed_referrals r  
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID AND r.RecordNumber = c.RecordNumber 
  
 LEFT JOIN $db_output.cyp_meaningful_change m1 ON r.UniqServReqID = m1.UniqServReqID AND r.Person_ID = m1.Person_ID AND m1.Rater = 'Self' 
 LEFT JOIN $db_output.cyp_meaningful_change m2 ON r.UniqServReqID = m2.UniqServReqID AND r.Person_ID = m2.Person_ID AND m2.Rater = 'Parent' 
 LEFT JOIN $db_output.cyp_meaningful_change m3 ON r.UniqServReqID = m3.UniqServReqID AND r.Person_ID = m3.Person_ID AND m3.Rater = 'Clinician' 
  
 WHERE ReportingPeriodStartDate = "$rp_startdate_1m" ---12 month period for prep table but we are only reporting on a single month in aggregation

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cyp_master