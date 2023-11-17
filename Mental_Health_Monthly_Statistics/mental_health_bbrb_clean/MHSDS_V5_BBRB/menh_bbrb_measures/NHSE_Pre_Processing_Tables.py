# Databricks notebook source
startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]
dbutils.widgets.dropdown("rp_startdate", "2021-04-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
dbutils.widgets.dropdown("start_month_id", "1452", monthid)
dbutils.widgets.dropdown("end_month_id", "1463", monthid)
dbutils.widgets.text("status","Provisional")
dbutils.widgets.text("db_output","$user_id")
dbutils.widgets.text("db_source","$reference_data")

# COMMAND ----------

# DBTITLE 1,Header
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Header;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Header AS
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate, label as Der_FY
 from $reference_data.mhs000header h
 left join $reference_data.calendar_financial_year fy on h.reportingperiodstartdate between fy.START_DATE and fy.END_DATE
 order by 1 desc

# COMMAND ----------

# DBTITLE 1,Referrals
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Referral;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Referral AS
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
 r.DischLetterIssDate,
 r.AgeServReferRecDate,
 r.AgeServReferDischDate,
 r.RecordStartDate,
 r.RecordEndDate,
 r.InactTimeRef,
 m.MHS001UniqID,
 CASE WHEN m.UniqMonthID <= 1467 then m.OrgIDCCGRes ---Added new case when statement AT 27/09/22
      WHEN m.UniqMonthID > 1467 then m.OrgIDSubICBLocResidence
      ELSE 'ERROR' end as OrgIDCCGRes,
 m.OrgIDEduEstab,
 m.EthnicCategory,
 m.EthnicCategory2021, --new for v5 but not being used in final prep table
 m.NHSDEthnicity,
 m.Gender,
 CASE WHEN m.GenderIDCode IN ('1','2','3','4','X','Z') THEN m.GenderIDCode ELSE m.Gender END AS Gender2021, --new for v5 but not being used in final prep table
 m.MaritalStatus,
 m.PersDeathDate,
 m.AgeDeath,
 m.OrgIDLocalPatientId,
 m.OrgIDResidenceResp,
 m.LADistrictAuth,
 m.PostcodeDistrict,
 m.DefaultPostcode,
 m.AgeRepPeriodStart,
 m.AgeRepPeriodEnd,
 s.MHS102UniqID,
 s.UniqCareProfTeamID,
 s.ServTeamTypeRefToMH,
 s.CAMHSTier,
 s.ReferRejectionDate,
 s.ReferRejectionTime,
 s.ReferRejectReason,
 s.ReferClosureDate,
 s.ReferClosureTime,
 s.ReferClosReason,
 s.AgeServReferClosure,
 s.AgeServReferRejection
 FROM                $db_source.mhs101referral r
 INNER JOIN          $db_source.mhs001mpi m 
                     ON r.RecordNumber = m.RecordNumber ---joining on recordnumber opposed to person_id as we want OrgIDCCGRes as it was inputted when referral was submitted in that month
 LEFT JOIN           $db_source.mhs102servicetypereferredto s 
                     ON r.UniqServReqID = s.UniqServReqID 
                     AND r.RecordNumber = s.RecordNumber --joining on recordnumber aswell to match historic records as they will all have the same uniqservreqid    
 LEFT JOIN           $db_output.NHSE_Pre_Proc_Header h
                     ON r.UniqMonthID = h.UniqMonthID

# COMMAND ----------

# DBTITLE 1,Pre_Activity - *****New NHSE Method de-duplicates MHS204Indirect Activity*****
 %sql
 DROP TABLE IF EXISTS $db_output.MHS204_de_duped;
 CREATE TABLE         $db_output.MHS204_de_duped AS
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

# COMMAND ----------

# DBTITLE 1,Activity
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Activity;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Activity AS
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
     c.AttendOrDNACode,
 	CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
     CASE 
         WHEN c.AttendOrDNACode IN ('5','6') 
         AND (((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') --v4.1 ConsMediumUsed
         OR (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')) 
         OR c.OrgIDProv = 'DFC' AND ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') 
         OR (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')))
         THEN 1 ELSE NULL 
     END AS Der_DirectContact
     
     
 FROM $db_source.mhs201carecontact c
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON c.UniqMonthID = h.UniqMonthID
 
 -- WHERE c.UniqMonthID >= $end_month_id
 
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
 	'NULL' AS AttendOrDNACode,
 	Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
     'NULL' as Der_DirectContact
 	
 FROM $db_output.MHS204_de_duped i
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON i.UniqMonthID = h.UniqMonthID
 
 WHERE i.Der_ActRN = 1
 
 -- WHERE i.UniqMonthID >= $end_month_id

# COMMAND ----------

# DBTITLE 1,Der_Activity
 %sql
 DROP TABLE IF EXISTS $db_output.Der_NHSE_Pre_Proc_Activity;
 CREATE TABLE         $db_output.Der_NHSE_Pre_Proc_Activity USING DELTA AS
 
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
                    
 FROM $db_output.NHSE_Pre_Proc_Activity a
 
 WHERE a.UniqMonthID < 1459 AND 
 ------(a.ConsMechanismMH IN ('01', '02', '03', '04') OR a.ConsMechanismMH NOT IN ('05', '06')????
      ((a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (a.ConsMechanismMH NOT IN ('05', '06') OR OrgIDProv = 'DFC' AND a.ConsMechanismMH IN ('05','06'))) OR a.Der_ActivityType = 'INDIRECT') 
 OR
 	a.UniqMonthID >= 1459 AND 
 ((a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (a.ConsMechanismMH IN ('01', '02', '04', '11') OR OrgIDProv = 'DFC' AND a.ConsMechanismMH IN ('05','09', '10', '13'))) OR a.Der_ActivityType = 'INDIRECT')

# COMMAND ----------

# DBTITLE 1,Inpatients
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Inpatients;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Inpatients AS
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
 w.SiteIDOfTreat,
 w.WardType,
 w.WardSexTypeCode,
 w.IntendClinCareIntenCodeMH,
 w.WardSecLevel,
 w.SpecialisedMHServiceCode,
 w.WardCode,
 w.WardLocDistanceHome,
 w.LockedWardInd,
 w.InactTimeWS,
 w.WardAge,
 w.HospitalBedTypeMH,
 w.EndDateMHTrialLeave,
 w.EndDateWardStay,
 w.EndTimeWardStay,
 CASE WHEN h.DischDateHospProvSpell IS NOT NULL THEN 'CLOSED' ELSE 'OPEN' END AS Der_HospSpellStatus    
 FROM $db_source.mhs501hospprovspell h
 LEFT JOIN $db_source.mhs502wardstay w ON h.UniqServReqID = w.UniqServReqID 
                                       AND h.UniqHospProvSpellID = w.UniqHospProvSpellID  --updated for v5
                                       AND h.RecordNumber = w.RecordNumber
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header he ON h.UniqMonthID = he.UniqMonthID                                      
 -- WHERE h.UniqMonthID >= $end_month_id

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Der_NHSE_Pre_Proc_Inpatients;
 CREATE TABLE         $db_output.Der_NHSE_Pre_Proc_Inpatients AS
 SELECT *,
 ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC) AS Der_HospSpellRecordOrder, 
 ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC, i.EndDateWardStay DESC, i.MHS502UniqID DESC) AS Der_LastWardStayRecord,
 ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID ASC, i.EndDateWardStay ASC, i.MHS502UniqID ASC) AS Der_FirstWardStayRecord
 FROM $db_output.NHSE_Pre_Proc_Inpatients i

# COMMAND ----------

# DBTITLE 1,Pre_Proc_Assessments PREP
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Assessments_Prep;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Assessments_Prep AS
 SELECT
 	'CON' AS Der_AssTable,
 	h.ReportingPeriodStartDate,
 	h.ReportingPeriodEndDate,
 	h.Der_FY,
 	a.UniqSubmissionID,
 -- 	a.NHSEUniqSubmissionID, omitted for NHSD code
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
 
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON h.UniqMonthID = a.UniqMonthID
 
 UNION ALL
 
 SELECT
 	'REF' AS Der_AssTable,
 	h.ReportingPeriodStartDate,
 	h.ReportingPeriodEndDate,
 	h.Der_FY,
 	r.UniqSubmissionID,
 -- 	r.NHSEUniqSubmissionID, omitted for NHSD code
 	r.UniqMonthID,
 	r.CodedAssToolType,
 	r.PersScore,	
 	date_format(COALESCE(r.AssToolCompTimestamp, r.AssToolCompDate), "yyyy-MM-dd") AS Der_AssToolCompDate, -- new field for v5 ---changed to COALESCE as this field is not mapped from v4.1 to v5 AT
 	r.RecordNumber,
 	r.MHS606UniqID AS Der_AssUniqID,
 	r.OrgIDProv,
 	CASE WHEN r.OrgIDProv = 'DFC' THEN '1' ELSE r.Person_ID END AS Person_ID, ---Der_Person_ID Derivation,
 	r.UniqServReqID,
 	r.AgeAssessToolReferCompDate AS Der_AgeAssessTool,
 	'NULL' AS UniqCareContID,
 	'NULL' AS UniqCareActID
 
 FROM $db_source.mhs606codedscoreassessmentrefer r 
 
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON h.UniqMonthID = r.UniqMonthID
 
 UNION ALL
 
 SELECT
 	'CLU' AS Der_AssTable,
 	h.ReportingPeriodStartDate,
 	h.ReportingPeriodEndDate,
 	h.Der_FY,
 	a.UniqSubmissionID,
 -- 	a.NHSEUniqSubmissionID, omitted for NHSD code
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
 
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON h.UniqMonthID = a.UniqMonthID
 
 INNER JOIN $db_source.mhs101referral r ON r.RecordNumber = c.RecordNumber 
                                        AND c.AssToolCompDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate,h.ReportingPeriodEndDate) ---ISNULL() used in NHSE code

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Pre_Proc_Assessments Stage 2 - Adding in MH_Assessments Reference data
 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Assessments_Prep;
 REFRESH TABLE mh_clear_collab.mh_assessments_ref;
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Assessments_Stage2;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Assessments_Stage2 AS
 SELECT 
 	a.ReportingPeriodStartDate,
 	a.ReportingPeriodEndDate,
 	a.Der_FY,
 	a.UniqSubmissionID,
 --  	a.[NHSEUniqSubmissionID], omitted for NHSD code
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
 	r.SNOMED_Code_Version AS Der_SNOMEDCodeVersion,
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
 
 FROM $db_output.NHSE_Pre_Proc_Assessments_Prep a
 
 LEFT JOIN mh_clear_collab.mh_assessments_ref r ON a.CodedAssToolType = r.Active_Concept_ID_SNOMED

# COMMAND ----------

# DBTITLE 1,Create Stage 3 Table. Can't UNION need to INSERT ASSESSMENT IN MONTH AND ASSESSMENT NOT IN MONTH
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Assessments_Stage3;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Assessments_Stage3
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 RecordNumber string,
 UniqServReqID string,
 UniqCareContID string,
 UniqCareActID string,
 CodedAssToolType string,
 PersScore string,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate string,
 Der_AgeAssessTool string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange int,
 Der_UpperRange int,
 Der_ValidScore string,
 Der_AssessmentCategory string,
 Der_AssKey string
 )

# COMMAND ----------

# DBTITLE 1,Pre_Proc_Assessments Stage 3 PT1 - Adding Assessments in Month 
 %sql
 INSERT INTO $db_output.NHSE_Pre_Proc_Assessments_Stage3
 --START CODE - ASSESSMENT IN MONTH
 
 SELECT 
 	a.ReportingPeriodStartDate,
 	a.ReportingPeriodEndDate,
 	a.Der_FY,
 	a.UniqSubmissionID,
 -- 	a.NHSEUniqSubmissionID, omitted for NHSD code
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
 	
 FROM $db_output.NHSE_Pre_Proc_Assessments_Stage2 a
 
 WHERE Der_UniqAssessment = 'Y' AND Der_AssInMonth = 1

# COMMAND ----------

# DBTITLE 1,Pre_Proc_Assessments Stage 3 PT2 - Adding Assessments out of Month  ***NEEDS REVIEWING***
 %sql
 --*** METHOD NEEDS REVIEWING AS NHSE METHOD TO REMOVE ASSESSMENTS IN MONTH IS TOO MEMORY INTENSIVE***
 INSERT INTO $db_output.NHSE_Pre_Proc_Assessments_Stage3
 -- --START CODE - ASSESSMENT NOT IN MONTH
 
 SELECT 
 	a.ReportingPeriodStartDate,
 	a.ReportingPeriodEndDate,
 	a.Der_FY,
 	a.UniqSubmissionID,
 -- 	a.NHSEUniqSubmissionID, omitted for NHSD code
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
 	
 FROM $db_output.NHSE_Pre_Proc_Assessments_Stage2 a
 LEFT JOIN $db_output.NHSE_Pre_Proc_Assessments_Stage2 b ON a.Der_AssKey = b.Der_AssKey AND b.Der_AssInMonth = 1 AND b.Der_UniqAssessment = 'Y'
 WHERE a.Der_UniqAssessment = 'Y' AND a.Der_AssInMonth = 0 
 AND b.Der_AssKey IS NULL---NHSD METHOD a.Der_AssKey NOT IN (SELECT Der_AssKey FROM $db_output.NHSE_Pre_Proc_Assessments_Stage2)

# COMMAND ----------

# DBTITLE 1,FINAL Pre_Proc_Assessments - Filtering only Valid Scores and adding assessment order derivations
 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Assessments;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Assessments
 
 SELECT
     ReportingPeriodStartDate,
 	ReportingPeriodEndDate,
 	Der_FY,
 	UniqSubmissionID,
 -- 	NHSEUniqSubmissionID, omitted for NHSD code
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
 FROM $db_output.NHSE_Pre_Proc_Assessments_Stage3
 WHERE Der_ValidScore = 'Y'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.NHSE_Pre_Proc_Interventions;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Interventions AS
  
 SELECT                ca.RecordNumber,
                       ca.OrgIDProv,
                       ca.Person_ID,
                       ca.UniqMonthID,
                       ca.UniqServReqID,
                       ca.UniqCareContID,
                       cc.CareContDate AS Der_ContactDate,
                       ca.UniqCareActID,
                       ca.MHS202UniqID as Der_InterventionUniqID,
                       ca.CodeProcAndProcStatus as CodeProcAndProcStatus, --gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"                     
                       CASE WHEN position(':' in ca.CodeProcAndProcStatus) > 0 THEN LEFT(ca.CodeProcAndProcStatus, position (':' in ca.CodeProcAndProcStatus)-1) 
                            ELSE ca.CodeProcAndProcStatus
                            END AS Der_SNoMEDProcCode,
                       CASE WHEN position('=', ca.CodeProcAndProcStatus) > 0
                       THEN RIGHT(ca.CodeProcAndProcStatus,position('=', REVERSE(ca.CodeProcAndProcStatus))-1)
                       ELSE NULL
                       END AS Der_SNoMEDProcQual,   
                       ca.CodeObs 
                        
 FROM                  $db_source.mhs202careactivity ca
 LEFT JOIN             $db_source.mhs201carecontact cc 
                       ON ca.RecordNumber = cc.RecordNumber 
                       AND ca.UniqCareContID = cc.UniqCareContID
 WHERE                 (ca.CodeFind IS NOT NULL OR ca.CodeObs IS NOT NULL OR ca.CodeProcAndProcStatus IS NOT NULL)
  
 UNION ALL
 SELECT                i.RecordNumber,
                       i.OrgIDProv,
                       i.Person_ID,
                       i.UniqMonthID,
                       i.UniqServReqID,
                       'NULL' as UniqCareContID,
                       i.IndirectActDate AS Der_ContactDate,
                       'NULL' as UniqCareActID,
                       i.MHS204UniqID as Der_InterventionUniqID,
                       i.CodeIndActProcAndProcStatus as CodeProcAndProcStatus,                      
                       CASE WHEN position(':' in i.CodeIndActProcAndProcStatus) > 0 THEN LEFT(i.CodeIndActProcAndProcStatus, position (':' in i.CodeIndActProcAndProcStatus)-1) 
                            ELSE i.CodeIndActProcAndProcStatus
                            END AS Der_SNoMEDProcCode,
                       CASE WHEN position('=',i.CodeIndActProcAndProcStatus) > 0
                         THEN RIGHT(i.CodeIndActProcAndProcStatus,position('=', REVERSE(i.CodeIndActProcAndProcStatus))-1)
                         ELSE NULL
                         END AS Der_SNoMEDProcQual,                                
                       'NULL' AS CodeObs                   
  
 FROM                  $db_source.mhs204indirectactivity i
 WHERE                 (i.CodeFind IS NOT NULL OR i.CodeIndActProcAndProcStatus IS NOT NULL)