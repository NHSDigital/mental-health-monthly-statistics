# Databricks notebook source
 %sql
 create widget text rp_enddate default "2022-03-31";
 create widget text end_month_id default "1464";

# COMMAND ----------

# DBTITLE 1,Header
 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_header;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Header USING DELTA AS
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate, label as Der_FY
 from $reference_data.mhs000header h
 left join $reference_data.calendar_financial_year fy on h.reportingperiodstartdate between fy.START_DATE and fy.END_DATE
 order by 1 desc

# COMMAND ----------

# DBTITLE 1,Referrals
 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_referral;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Referral USING DELTA AS
 SELECT
 h.Der_FY,
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
 CASE WHEN m.UniqMonthID <= 1467 then m.OrgIDCCGRes ---Added new case when statement AT 27/09/22
      WHEN m.UniqMonthID > 1467 then m.OrgIDSubICBLocResidence
      ELSE 'ERROR' end as OrgIDCCGRes, --gvf added 7/9/22 OrgIDSubICBLocResidence 
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

# DBTITLE 1,Activity
 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_activity;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Activity USING DELTA AS
 SELECT
 	h.Der_FY,
     'DIRECT' AS Der_ActivityType,
 	c.MHS201UniqID AS Der_ActivityUniqID,
 	c.Person_ID,
 	c.UniqMonthID,
 	c.OrgIDProv,
 	c.RecordNumber,
 	c.UniqServReqID,
     c.CareContDate AS Der_ContactDate,
 	c.CareContTime AS Der_ContactTime,
     c.ConsMechanismMH, --new for v5
     c.AttendOrDNACode,
 	CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID -- derivation added to better reflect anonymous services where personID may change every month	
     
     
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
     i.IndirectActDate AS Der_ContactDate,
 	i.IndirectActTime AS Der_ContactTime,
     'NULL' AS ConsMechanismMH, --new for v5
 	'NULL' AS AttendOrDNACode,
 	CASE WHEN i.OrgIDProv = 'DFC' THEN '1' ELSE i.Person_ID END AS Der_PersonID -- derivation added to better reflect anonymous services where personID may change every month
 	
 FROM $db_source.mhs204indirectactivity i
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON i.UniqMonthID = h.UniqMonthID
 
 -- WHERE i.UniqMonthID >= $end_month_id

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_activity;
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

# COMMAND ----------

# DBTITLE 1,Inpatients
 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_inpatients;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Inpatients USING DELTA AS
 SELECT
 he.Der_FY,
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
 DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_inpatients;
 CREATE TABLE         $db_output.Der_NHSE_Pre_Proc_Inpatients USING DELTA AS
 SELECT *,
     ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC) AS Der_HospSpellRecordOrder, 
 	ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC, i.EndDateWardStay DESC, i.MHS502UniqID DESC) AS Der_LastWardStayRecord,
 	ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID ASC, i.EndDateWardStay ASC, i.MHS502UniqID ASC) AS Der_FirstWardStayRecord
     
 FROM $db_output.NHSE_Pre_Proc_Inpatients i

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))