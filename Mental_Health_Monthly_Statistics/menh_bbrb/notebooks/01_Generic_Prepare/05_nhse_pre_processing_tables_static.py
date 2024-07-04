# Databricks notebook source
# DBTITLE 1,Create Header to include new data up to reporting period
 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_header
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate, label as Der_FY
 from $db_source.mhs000header h
 left join $reference_data.calendar_financial_year fy on h.reportingperiodstartdate between fy.START_DATE and fy.END_DATE
 where UniqMonthID <= "$end_month_id"
 order by 1 desc

# COMMAND ----------

# DBTITLE 1,Current FY UniqMonthIDs (need as not all tables have ReportingPeriodStart/EndDate and Der_FY)
 %sql
 create or replace temp view current_fy_header_uniqmonthids as
 select * from $db_output.nhse_pre_proc_header where ReportingPeriodStartDate >= "$financial_year_start"

# COMMAND ----------

# DBTITLE 1,Delete Current FY Data from Pre_Processing_Tables
 %sql
 DELETE FROM $db_output.nhse_pre_proc_referral WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.nhse_pre_proc_distinct_indirect_activity WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.nhse_pre_proc_activity WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.nhse_pre_proc_inpatients WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.prep_nhse_pre_proc_assessments WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.nhse_pre_proc_assessments WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.nhse_pre_proc_assessments_unique WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);
 DELETE FROM $db_output.nhse_pre_proc_interventions WHERE UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids);

# COMMAND ----------

 %sql
 --This table returns rows, carry forward
 CREATE OR REPLACE TEMPORARY VIEW GP_Practice_CCG AS
  
 SELECT GP.UniqMonthID,
       GP.Person_ID,
       GP.OrgIDCCGGPPractice,
       GP.OrgIDSubICBLocGP,
       GP.RecordNumber
  FROM $db_source.MHS002GP GP
       INNER JOIN 
                  (
                    SELECT UniqMonthID,
                           Person_ID, 
                           MAX(RecordNumber) as RecordNumber
                      FROM $db_source.MHS002GP
                      WHERE GMPReg NOT IN ('V81999','V81998','V81997') AND EndDateGMPRegistration is NULL
                  GROUP BY UniqMonthID, Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
                  AND GP.UniqMonthID = max_GP.UniqMonthID
  WHERE GMPReg NOT IN ('V81999','V81998','V81997') AND EndDateGMPRegistration is NULL AND GP.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids) 

# COMMAND ----------

# DBTITLE 1,Insert Referrals for Current FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_referral
 SELECT
 h.Der_FY,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 r.MHS101UniqID,
 r.Person_ID,
 r.OrgIDProv,
 m.UniqMonthID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDComm,
 r.ReferralRequestReceivedDate,
 r.ReferralRequestReceivedTime,
 r.SpecialisedMHServiceCode,
 r.PrimReasonReferralMH,
 r.ReasonOAT,
 r.DischPlanCreationDate,
 r.DischPlanCreationTime,
 r.DischPlanLastUpdatedDate,
 r.DischPlanLastUpdatedTime,
 r.ServDischDate,
 r.ServDischTime,
 r.AgeServReferRecDate,
 r.AgeServReferDischDate,
 r.RecordStartDate,
 r.RecordEndDate,
 r.InactTimeRef,
 m.MHS001UniqID,
 CASE WHEN m.UniqMonthID <= 1467 then m.OrgIDCCGRes ---Added new case when statement AT 27/09/22
       WHEN m.UniqMonthID > 1467 then m.OrgIDSubICBLocResidence
       ELSE 'ERROR' end as OrgIDCCGRes,
 CASE WHEN m.UniqMonthID <= 1467 and gp.OrgIDCCGGPPractice is not null then gp.OrgIDCCGGPPractice
      WHEN m.UniqMonthID > 1467 and gp.OrgIDSubICBLocGP is not null then gp.OrgIDSubICBLocGP 
      WHEN m.UniqMonthID <= 1467 then m.OrgIDCCGRes 
      WHEN m.UniqMonthID > 1467 then m.OrgIDSubICBLocResidence
      ELSE 'ERROR' END as IC_Rec_CCG,
 m.OrgIDEduEstab,
 m.EthnicCategory,
 m.EthnicCategory2021, --new for v5 but not being used in final prep table
 m.NHSDEthnicity,
 m.Gender,
 CASE WHEN m.GenderIDCode IN ('1','2','3','4','X','Z') THEN m.GenderIDCode ELSE m.Gender END AS Gender2021, --new for v5 but not being used in final prep table ---remove hard-coded gender list
 m.MaritalStatus,
 m.PersDeathDate,
 m.AgeDeath,
 m.LocalPatientId, ----change schema
 m.OrgIDResidenceResp,
 m.LADistrictAuth,
 m.PostcodeDistrict,
 m.DefaultPostcode,
 m.AgeRepPeriodStart,
 m.AgeRepPeriodEnd,
 s.UniqCareProfTeamID,
 s.ServTeamTypeRefToMH,
 s.ReferRejectionDate,
 s.ReferRejectionTime,
 s.ReferRejectReason,
 s.ReferClosureDate,
 s.ReferClosureTime,
 s.ReferClosReason
 FROM                $db_source.mhs101referral r
 INNER JOIN          $db_source.mhs001mpi m 
                     ON r.RecordNumber = m.RecordNumber ---joining on recordnumber opposed to person_id as we want OrgIDCCGRes as it was inputted when referral was submitted in that month
 LEFT JOIN           $db_output.ServiceTeamType s 
                     ON r.UniqServReqID = s.UniqServReqID 
                     AND r.RecordNumber = s.RecordNumber --joining on recordnumber aswell to match historic records as they will all have the same uniqservreqid    
 LEFT JOIN           $db_output.NHSE_Pre_Proc_Header h
                     ON r.UniqMonthID = h.UniqMonthID
 LEFT JOIN           GP_Practice_CCG gp
                     ON r.Person_ID = gp.Person_ID
                     AND m.UniqMonthID = gp.UniqMonthID
 WHERE h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids) 

# COMMAND ----------

# DBTITLE 1,Insert Distinct Indirect Activity in FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_distinct_indirect_activity
 SELECT
 i.UniqSubmissionID,
 i.UniqMonthID,
 i.OrgIDProv,
 i.Person_ID,
 CASE WHEN i.OrgIDProv = 'DFC' THEN '1' ELSE i.Person_ID END AS Der_PersonID,
 i.RecordNumber,
 i.UniqServReqID,
 i.OrgIDComm,
 i.CareProfTeamLocalId,
 i.IndirectActDate,
 i.IndirectActTime,
 i.DurationIndirectAct,
 i.MHS204UniqID,
 ROW_NUMBER () OVER(PARTITION BY i.UniqServReqID, i.IndirectActDate, i.IndirectActTime ORDER BY i.IndirectActTime DESC) AS Der_ActRN 
 FROM $db_source.MHS204IndirectActivity i
 WHERE i.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids) 

# COMMAND ----------

# DBTITLE 1,Insert Activity for FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_activity
 SELECT
 h.Der_FY,
 'DIRECT' AS Der_ActivityType,
 c.MHS201UniqID AS Der_ActivityUniqID,
 c.Person_ID,
 c.UniqMonthID,
 c.OrgIDProv,
 c.RecordNumber,
 c.UniqServReqID,
 c.UniqCareContID,
 c.CareContDate AS Der_ContactDate,
 c.CareContTime AS Der_ContactTime,
 c.ConsMechanismMH, --new for v5
 c.AttendStatus,
 CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
 CASE 
     WHEN c.AttendStatus IN ('5','6') 
     AND (((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') --v4.1 ConsMediumUsed
     OR (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')) 
     OR c.OrgIDProv = 'DFC' AND ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') 
     OR (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')))
     THEN 1 ELSE 'NULL' 
 END AS Der_DirectContact 
         
 FROM $db_source.mhs201carecontact c
 LEFT JOIN $db_output.nhse_pre_proc_header h ON c.UniqMonthID = h.UniqMonthID
 WHERE h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  
  
 UNION ALL
  
 SELECT
 h.Der_FY,
 'INDIRECT' AS Der_ActivityType,
 i.MHS204UniqID AS Der_ActivityUniqID,
 i.Person_ID,
 i.UniqMonthID,
 i.OrgIDProv,
 i.RecordNumber,
 i.UniqServReqID,
 'NULL' AS UniqCareContID,
 i.IndirectActDate AS Der_ContactDate,
 i.IndirectActTime AS Der_ContactTime,
 'NULL' AS ConsMechanismMH, --new for v5
 'NULL' AS AttendStatus,
 Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
 'NULL' as Der_DirectContact
     
 FROM $db_output.nhse_pre_proc_distinct_indirect_activity i
 LEFT JOIN $db_output.nhse_pre_proc_header h ON i.UniqMonthID = h.UniqMonthID
  
 WHERE i.Der_ActRN = 1 and h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)

# COMMAND ----------

# DBTITLE 1,Insert Derived Activity in FY
 %sql
 INSERT OVERWRITE TABLE $db_output.der_nhse_pre_proc_activity
  
 SELECT *,
 ROW_NUMBER() OVER (PARTITION BY 
                    CASE WHEN a.OrgIDProv = 'DFC' THEN a.UniqServReqID
                    ELSE a.Person_ID END, 
                    a.UniqServReqID 
                    ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_ContactOrder,
 ROW_NUMBER() OVER (PARTITION BY 
                    CASE WHEN a.OrgIDProv = 'DFC' THEN a.UniqServReqID
                    ELSE a.Person_ID END, 
                    a.UniqServReqID, a.Der_FY 
                    ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_FYContactOrder
                    
 FROM $db_output.nhse_pre_proc_activity a
  
 WHERE a.UniqMonthID < 1459 AND 
      ((a.Der_ActivityType = 'DIRECT' AND a.AttendStatus IN ('5','6') AND (a.ConsMechanismMH NOT IN ('05', '06') OR OrgIDProv = 'DFC' AND a.ConsMechanismMH IN ('05','06'))) OR a.Der_ActivityType = 'INDIRECT') 
 OR
     a.UniqMonthID >= 1459 AND 
 ((a.Der_ActivityType = 'DIRECT' AND a.AttendStatus IN ('5','6') AND (a.ConsMechanismMH IN ('01', '02', '04', '11') OR OrgIDProv = 'DFC' AND a.ConsMechanismMH IN ('05','09', '10', '13'))) OR a.Der_ActivityType = 'INDIRECT')

# COMMAND ----------

# DBTITLE 1,Insert Inpatients in FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_inpatients
 SELECT
 he.Der_FY,
 he.ReportingPeriodStartDate,
 he.ReportingPeriodEndDate,
 h.MHS501UniqID,
 h.Person_ID,
 h.OrgIDProv,
 h.UniqMonthID,
 h.RecordNumber,
 h.UniqHospProvSpellID, --new for v5
 h.UniqServReqID,
 CONCAT(h.Person_ID, h.UniqServReqID) as UniqPersRefID,
 CONCAT(h.Person_ID, h.UniqServReqID, h.UniqMonthID) as UniqPersRefID_FY,
 h.StartDateHospProvSpell,
 h.StartTimeHospProvSpell,
 h.SourceAdmMHHospProvSpell, --new for v5
 h.MethAdmMHHospProvSpell, --new for v5
 h.EstimatedDischDateHospProvSpell,
 h.PlannedDischDateHospProvSpell,
 h.DischDateHospProvSpell,
 h.DischTimeHospProvSpell,
 h.MethOfDischMHHospProvSpell, --new for v5
 h.DestOfDischHospProvSpell, --new for v5
 h.InactTimeHPS,
 h.PlannedDestDisch,
 h.PostcodeDistrictMainVisitor,
 h.PostcodeDistrictDischDest,
 w.MHS502UniqID,
 w.UniqWardStayID,
 w.StartDateWardStay,
 w.StartTimeWardStay,
 COALESCE(wd.SiteIDOfWard, w.SiteIDOfTreat) AS SiteIDOfWard,
 w.WardType,
 w.WardIntendedSex,
 w.WardIntendedClinCareMH,
 w.WardSecLevel,
 w.SpecialisedMHServiceCode,
 w.WardCode,
 w.WardLocDistanceHome,
 w.LockedWardInd,
 w.InactTimeWS,
 w.WardAge,
 w.MHAdmittedPatientClass,
 w.EndDateMHTrialLeave,
 w.EndDateWardStay,
 w.EndTimeWardStay,
 CASE WHEN h.DischDateHospProvSpell IS NOT NULL THEN 'CLOSED' ELSE 'OPEN' END AS Der_HospSpellStatus
     
 FROM $db_source.mhs501hospprovspell h
 LEFT JOIN $db_source.mhs502wardstay w ON h.UniqServReqID = w.UniqServReqID 
                                       AND h.UniqHospProvSpellID = w.UniqHospProvSpellID  --updated for v5
                                       AND h.RecordNumber = w.RecordNumber
 LEFT JOIN  $db_source.MHS903warddetails wd ON w.UniqWardCode = wd.UniqWardCode -- join on latest 903 record to get SiteIDOfWard   
 LEFT JOIN $db_output.nhse_pre_proc_header he ON h.UniqMonthID = he.UniqMonthID  
 WHERE h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  

# COMMAND ----------

# DBTITLE 1,Insert Derived Inpatients in FY
 %sql
 INSERT OVERWRITE TABLE $db_output.der_nhse_pre_proc_inpatients
 SELECT 
 *,
 ROW_NUMBER () OVER (PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC) AS Der_HospSpellRecordOrder, 
 ROW_NUMBER () OVER (PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC, i.EndDateWardStay DESC, i.MHS502UniqID DESC) AS Der_LastWardStayRecord,
 ROW_NUMBER () OVER (PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID ASC, i.EndDateWardStay ASC, i.MHS502UniqID ASC) AS Der_FirstWardStayRecord
     
 FROM $db_output.nhse_pre_proc_inpatients i

# COMMAND ----------

# DBTITLE 1,Insert Assessments Prep in FY
 %sql
 INSERT INTO $db_output.prep_nhse_pre_proc_assessments
 SELECT
 'CON' AS Der_AssTable,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 h.Der_FY,
 a.UniqSubmissionID,
 a.UniqMonthID,
 a.CodedAssToolType,
 a.PersScore,
 c.CareContDate AS Der_AssToolCompDate,
 a.RecordNumber,
 a.MHS607UniqID AS Der_AssUniqID,
 a.OrgIDProv,
 CASE WHEN a.OrgIDProv = 'DFC' THEN '1' ELSE a.Person_ID END AS Person_ID, ---Der_Person_ID Derivation
 a.UniqServReqID,
 a.AgeAssessToolCont AS Der_AgeAssessTool,
 a.UniqCareContID,
 a.UniqCareActID
  
 FROM $db_source.mhs607codedscoreassessmentact a 
  
 LEFT JOIN $db_source.mhs201carecontact c ON a.RecordNumber = c.RecordNumber AND a.UniqServReqID = c.UniqServReqID AND a.UniqCareContID = c.UniqCareContID
  
 LEFT JOIN $db_output.nhse_pre_proc_header h ON h.UniqMonthID = a.UniqMonthID
 WHERE h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  
  
 UNION ALL
  
 SELECT
 'REF' AS Der_AssTable,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 h.Der_FY,
 r.UniqSubmissionID,
 r.UniqMonthID,
 r.CodedAssToolType,
 r.PersScore,    
 date_format(r.AssToolCompTimestamp, "yyyy-MM-dd") AS Der_AssToolCompDate, ---new field for v5 ---changed to COALESCE as this field is not mapped from v4.1 to v5
 r.RecordNumber,
 r.MHS606UniqID AS Der_AssUniqID,
 r.OrgIDProv,
 CASE WHEN r.OrgIDProv = 'DFC' THEN '1' ELSE r.Person_ID END AS Person_ID, ---Der_Person_ID Derivation,
 r.UniqServReqID,
 r.AgeAssessToolReferCompDate AS Der_AgeAssessTool,
 'NULL' AS UniqCareContID,
 'NULL' AS UniqCareActID
  
 FROM $db_source.mhs606codedscoreassessmentrefer r 
  
 LEFT JOIN $db_output.nhse_pre_proc_header h ON h.UniqMonthID = r.UniqMonthID
 WHERE h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  
  
 UNION ALL
  
 SELECT
 'CLU' AS Der_AssTable,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 h.Der_FY,
 a.UniqSubmissionID,
 c.UniqMonthID,
 a.CodedAssToolType,
 a.PersScore,
 c.AssToolCompDate AS Der_AssToolCompDate,
 c.RecordNumber,
 a.MHS802UniqID AS Der_AssUniqID,
 a.OrgIDProv,
 CASE WHEN a.OrgIDProv = 'DFC' THEN '1' ELSE a.Person_ID END AS Person_ID,
 r.UniqServReqID,
 'NULL' AS Der_AgeAssessTool,
 'NULL' AS UniqCareContID,
 'NULL' AS UniqCareActID
  
 FROM $db_source.mhs802clusterassess a
  
 LEFT JOIN $db_source.mhs801clustertool c ON c.UniqClustID = a.UniqClustID AND c.RecordNumber = a.RecordNumber
  
 LEFT JOIN $db_output.nhse_pre_proc_header h ON h.UniqMonthID = a.UniqMonthID
  
 INNER JOIN $db_source.mhs101referral r ON r.RecordNumber = c.RecordNumber 
                                        AND c.AssToolCompDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate,h.ReportingPeriodEndDate) ---ISNULL() used in NHSE code
 WHERE h.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  

# COMMAND ----------

# DBTITLE 1,Insert Assessments in FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_assessments
 SELECT 
 a.ReportingPeriodStartDate,
 a.ReportingPeriodEndDate,
 a.Der_FY,
 a.UniqSubmissionID,
 a.Der_AssUniqID,
 a.Der_AssTable, 
 a.Person_ID,    
 a.UniqMonthID,    
 a.OrgIDProv,
 a.RecordNumber,    
 a.UniqServReqID,    
 a.UniqCareContID,    
 a.UniqCareActID,        
 a.Der_AssToolCompDate,
 a.CodedAssToolType,
 a.PersScore,
 a.Der_AgeAssessTool,
 r.Category AS Der_AssessmentCategory,
 r.Assessment_Tool_Name AS Der_AssessmentToolName,
 r.Preferred_Term_SNOMED AS Der_PreferredTermSNOMED,
 r.SNOMED_Version AS Der_SNOMEDCodeVersion,
 r.Lower_Range AS Der_LowerRange,
 r.Upper_Range AS Der_UpperRange,
 r.Rater,
 CASE 
     WHEN CAST(a.PersScore as float) BETWEEN r.Lower_Range AND r.Upper_Range THEN 'Y' ---TRY_CONVERT() IN NHSE Code
     ELSE NULL 
 END AS Der_ValidScore,
 CASE 
     WHEN ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.Der_AssToolCompDate, COALESCE(a.UniqServReqID,0), r.Preferred_Term_SNOMED, a.PersScore ORDER BY a.Der_AssUniqID ASC) = 1 ---IS_NULL(UniqServReqID, 0) in NHSE
     THEN 'Y' 
     ELSE NULL 
 END AS Der_UniqAssessment,
 CONCAT(a.Der_AssToolCompDate,a.UniqServReqID,a.CodedAssToolType,a.PersScore) AS Der_AssKey,
 CASE WHEN a.Der_AssToolCompDate BETWEEN a.ReportingPeriodStartDate AND a.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Der_AssInMonth
  
 FROM $db_output.prep_nhse_pre_proc_assessments a 
 LEFT JOIN $db_output.mh_ass r ON a.CodedAssToolType = r.Active_Concept_ID_SNOMED
 WHERE a.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)

# COMMAND ----------

# DBTITLE 1,Insert Unique Assessments In Month in FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_assessments_unique
 SELECT 
 a.ReportingPeriodStartDate,
 a.ReportingPeriodEndDate,
 a.Der_FY,
 a.UniqSubmissionID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.Person_ID,
 a.RecordNumber,
 a.UniqServReqID,
 a.UniqCareContID,
 a.UniqCareActID,
 a.CodedAssToolType,
 a.PersScore,
 a.Der_AssUniqID,
 a.Der_AssTable,
 a.Der_AssToolCompDate,
 a.Der_AgeAssessTool,
 a.Der_AssessmentToolName,
 a.Der_PreferredTermSNOMED,
 a.Der_SNOMEDCodeVersion,
 a.Der_LowerRange,
 a.Der_UpperRange,
 a.Der_ValidScore,
 a.Der_AssessmentCategory,
 a.Der_AssKey
     
 FROM $db_output.nhse_pre_proc_assessments a
  
 WHERE Der_UniqAssessment = 'Y' AND Der_AssInMonth = 1 ---add assessments in-month
 AND a.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)

# COMMAND ----------

# DBTITLE 1,Insert Unique Assessments out of Month in FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_assessments_unique
 SELECT 
 a.ReportingPeriodStartDate,
 a.ReportingPeriodEndDate,
 a.Der_FY,
 a.UniqSubmissionID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.Person_ID,
 a.RecordNumber,
 a.UniqServReqID,
 a.UniqCareContID,
 a.UniqCareActID,
 a.CodedAssToolType,
 a.PersScore,
 a.Der_AssUniqID,
 a.Der_AssTable,
 a.Der_AssToolCompDate,
 a.Der_AgeAssessTool,
 a.Der_AssessmentToolName,
 a.Der_PreferredTermSNOMED,
 a.Der_SNOMEDCodeVersion,
 a.Der_LowerRange,
 a.Der_UpperRange,
 a.Der_ValidScore,
 a.Der_AssessmentCategory,    
 a.Der_AssKey
     
 FROM $db_output.nhse_pre_proc_assessments a
 LEFT JOIN $db_output.nhse_pre_proc_assessments b ON a.Der_AssKey = b.Der_AssKey AND b.Der_AssInMonth = 1 AND b.Der_UniqAssessment = 'Y'
 WHERE a.Der_UniqAssessment = 'Y' AND a.Der_AssInMonth = 0 ---add assessments out of month
 AND b.Der_AssKey IS NULL ---NHSE METHOD a.Der_AssKey NOT IN (SELECT Der_AssKey FROM $db_output.NHSE_Pre_Proc_Assessments_Stage2)
 AND a.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  

# COMMAND ----------

# DBTITLE 1,Insert Unique and Valid Assessments in FY
 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_assessments_unique_valid
 SELECT
 ReportingPeriodStartDate,
 ReportingPeriodEndDate,
 Der_FY,
 UniqSubmissionID,
 UniqMonthID,
 OrgIDProv,
 Person_ID,
 RecordNumber,
 UniqServReqID,
 UniqCareContID,
 UniqCareActID,
 CodedAssToolType,
 PersScore,
 Der_AssUniqID,
 Der_AssTable,
 Der_AssToolCompDate,
 Der_AgeAssessTool,
 Der_AssessmentToolName,
 Der_PreferredTermSNOMED,
 Der_SNOMEDCodeVersion,
 Der_LowerRange,
 Der_UpperRange,
 Der_ValidScore,
 Der_AssessmentCategory,        
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, CodedAssToolType ORDER BY Der_AssToolCompDate ASC) AS Der_AssOrderAsc_OLD, --First assessment
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, CodedAssToolType ORDER BY Der_AssToolCompDate DESC) AS Der_AssOrderDesc_OLD, -- Last assessment
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, Der_PreferredTermSNOMED ORDER BY Der_AssToolCompDate ASC) AS Der_AssOrderAsc_NEW, --First assessment
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, Der_PreferredTermSNOMED ORDER BY Der_AssToolCompDate DESC) AS Der_AssOrderDesc_NEW, -- Last assessment
 Der_AssKey
 FROM $db_output.nhse_pre_proc_assessments_unique
 WHERE Der_ValidScore = 'Y' 

# COMMAND ----------

# DBTITLE 1,Insert Interventions in FY
 %sql
 INSERT INTO $db_output.nhse_pre_proc_interventions
 SELECT                
 ca.RecordNumber,
 ca.OrgIDProv,
 ca.Person_ID,
 ca.UniqMonthID,
 ca.UniqServReqID,
 ca.UniqCareContID,
 cc.CareContDate AS Der_ContactDate,
 ca.UniqCareActID,
 ca.MHS202UniqID as Der_InterventionUniqID,
 ca.Procedure as CodeProcAndProcStatus,                 
 CASE WHEN position(':' in ca.Procedure) > 0 ---gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"     
      THEN LEFT(ca.Procedure, position(':' in ca.Procedure)-1) 
      ELSE ca.Procedure
      END AS Der_SNoMEDProcCode,
 CASE WHEN position('=', ca.Procedure) > 0
      THEN RIGHT(ca.Procedure, position('=', REVERSE(ca.Procedure))-1)
      ELSE NULL
      END AS Der_SNoMEDProcQual,   
 ca.Observation
                        
 FROM $db_source.mhs202careactivity ca
 LEFT JOIN $db_source.mhs201carecontact cc ON ca.RecordNumber = cc.RecordNumber AND ca.UniqCareContID = cc.UniqCareContID
 WHERE (ca.Finding IS NOT NULL OR ca.Observation IS NOT NULL OR ca.Procedure IS NOT NULL)
 AND ca.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  
  
 UNION ALL
  
 SELECT                
 i.RecordNumber,
 i.OrgIDProv,
 i.Person_ID,
 i.UniqMonthID,
 i.UniqServReqID,
 'NULL' as UniqCareContID,
 i.IndirectActDate AS Der_ContactDate,
 'NULL' as UniqCareActID,
 i.MHS204UniqID as Der_InterventionUniqID,
 i.IndActProcedure as CodeProcAndProcStatus,                      
 CASE WHEN position(':' in i.IndActProcedure) > 0 ---gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":" 
      THEN LEFT(i.IndActProcedure, position(':' in i.IndActProcedure)-1) 
      ELSE i.IndActProcedure
      END AS Der_SNoMEDProcCode,
 CASE WHEN position('=', i.IndActProcedure) > 0
      THEN RIGHT(i.IndActProcedure, position('=', REVERSE(i.IndActProcedure))-1)
      ELSE NULL
      END AS Der_SNoMEDProcQual,                                
 'NULL' AS Observation                   
  
 FROM $db_source.mhs204indirectactivity i
 WHERE (i.Finding IS NOT NULL OR i.IndActProcedure IS NOT NULL)
 AND i.UniqMonthID in (select distinct UniqMonthID from current_fy_header_uniqmonthids)  