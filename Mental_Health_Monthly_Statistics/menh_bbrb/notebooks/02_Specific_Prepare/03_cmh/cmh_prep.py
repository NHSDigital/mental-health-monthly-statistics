# Databricks notebook source
 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Inpatients
 SELECT
 MHS501UniqID,
 Person_ID,
 OrgIDProv,
 UniqMonthID,
 RecordNumber,
 UniqHospProvSpellID, --updated for v5
 UniqServReqID,
 CONCAT(Person_ID, UniqServReqID) as UniqPersRefID,
 StartDateHospProvSpell,
 StartTimeHospProvSpell,
 MHS502UniqID,
 UniqWardStayID,
 StartDateWardStay,
 StartTimeWardStay,
 SiteIDOfWard,
 WardType,
 SpecialisedMHServiceCode,
 MHAdmittedPatientClass,
 EndDateWardStay,
 EndTimeWardStay,
 ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID DESC) AS Der_HospSpellRecordOrder, --updated for v5
 ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID ASC, EndDateWardStay ASC, MHS502UniqID ASC) AS Der_FirstWardStayRecord --updated for v5
 FROM $db_output.NHSE_Pre_Proc_Inpatients

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Admissions
 SELECT    
      i.UniqMonthID
     ,i.UniqHospProvSpellID --updated for v5
     ,i.Person_ID
     ,CASE WHEN r.NHSDEthnicity NOT IN ('A','99','-1') THEN 1 ELSE 0 END as NotWhiteBritish 
     ,CASE WHEN r.NHSDEthnicity = 'A' THEN 1 ELSE 0 END as WhiteBritish     
     ,i.OrgIDProv
     ,od.NAME as Provider_Name
     ,COALESCE(ccg21.CCG21CDH, CASE WHEN r.IC_Rec_CCG in ('X98', '') THEN 'UNKNOWN' ELSE r.IC_Rec_CCG END, 'UNKNOWN') as CCG_Code
     ,COALESCE(ccg21.CCG21NM, 'UNKNOWN') as CCG_Name
     ,COALESCE(ccg21.NHSER21CDH, 'UNKNOWN') as Region_Code
     ,COALESCE(ccg21.NHSER21NM, 'UNKNOWN') as Region_Name
     ,COALESCE(ccg21.STP21CDH, 'UNKNOWN') as STP_Code
     ,COALESCE(ccg21.STP21NM, 'UNKNOWN') as STP_Name
     ,i.StartDateHospProvSpell
     ,i.StartTimeHospProvSpell 
     ,date_add(last_day(add_months(i.StartDateHospProvSpell, -1)),1) AS Adm_month
     ,ia.MHAdmittedPatientClass
     ,r.AgeServReferRecDate
     ,r.UniqServReqID 
     ,r.UniqMonthID AS RefMonth
     ,r.RecordNumber AS RefRecordNumber 
     ,ROW_NUMBER() OVER (PARTITION BY i.UniqHospProvSpellID ORDER BY r.UniqMonthID DESC, r.RecordNumber DESC) AS RN --new for v5
     -- added because joining to refs produces some duplicates --this also gets latest admission for a person in the same provider in the month
     
 FROM $db_output.NHSE_Pre_Proc_Inpatients i     
     
 LEFT JOIN $db_output.CMH_Inpatients ia 
           ON i.UniqHospProvSpellID = ia.UniqHospProvSpellID --updated for v5
           AND i.Person_ID = ia.Person_ID 
           AND i.UniqServReqID = ia.UniqServReqID  ----- records are partitioned on spell, person and ref : therefore have joined on spell, person and ref    
           AND ia.Der_FirstWardStayRecord = 1 ---- ward stay at admission
     
 LEFT JOIN $db_output.NHSE_Pre_Proc_Referral r ON i.RecordNumber = r.RecordNumber 
                                               AND i.Person_ID = r.Person_ID 
                                               AND i.UniqServReqID = r.UniqServReqID 
                                               AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = '')     
 LEFT JOIN $db_output.ccg_mapping_2021 ccg21 ON r.IC_Rec_CCG = ccg21.CCG_UNMAPPED  --- regions/stps taken from CCG rather than provider 
 LEFT JOIN $db_output.bbrb_org_daily_latest od ON i.OrgIDProv = od.ORG_CODE
     
 WHERE i.StartDateHospProvSpell BETWEEN '$rp_startdate_12m' AND '$rp_enddate'    
 AND i.UniqMonthID BETWEEN $start_month_id AND $end_month_id    ---getting only data for RP in question
 AND 
  CASE WHEN i.UniqMonthID > 1488 AND ia.MHAdmittedPatientClass IN ('200','201','202') THEN 'Y'
       WHEN i.UniqMonthID <= 1488 AND ia.MHAdmittedPatientClass IN ('10','11','12') THEN 'Y'
       ELSE 'N'
       END = 'Y'--- adult/older acute and PICU admissions only ---codes changed for v6     
 AND i.SourceAdmMHHospProvSpell NOT IN ('49','53','87') --- excluding people transferred from other MH inpatient settings --updated for v5

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Previous_Contacts
 SELECT     
      a.UniqHospProvSpellID --updated for v5
     ,a.OrgIDProv as Adm_OrgIDProv 
     ,a.Person_ID
     ,a.StartDateHospProvSpell
     ,a.StartTimeHospProvSpell
     ,c.UniqServReqID 
     ,c.Der_ActivityUniqID
     ,c.OrgIDProv as Cont_OrgIDProv 
     ,c.Der_ActivityType 
     ,c.AttendStatus 
     ,c.ConsMechanismMH 
     ,c.Der_ContactDate 
     ,i.UniqHospProvSPellID As Contact_spell --- to be removed 
     ,DATEDIFF(a.StartDateHospProvSpell, c.Der_ContactDate) AS TimeToAdm 
     ,ROW_NUMBER() OVER(PARTITION BY a.UniqHospProvSpellID ORDER BY c.Der_ContactDate DESC) AS RN --- order to get most recent contact prior referral for each hospital spell 
     
 FROM $db_output.NHSE_Pre_Proc_Activity c     
     
 INNER JOIN $db_output.CMH_Admissions a ON c.Person_ID = a.Person_ID --- same person     
     AND DATEDIFF(a.StartDateHospProvSpell, c.Der_ContactDate) <= 365 --- contact up to 1yr before admission
     AND DATEDIFF(a.StartDateHospProvSpell, c.Der_ContactDate) > 2 --- exclude contacts in two days before admission 
     AND a.RN = 1 ---has to be related to latest admission of a person in a given month accounting for duplicate referral records
     
 LEFT JOIN $db_output.CMH_Inpatients i 
     ON c.Person_ID = i.Person_ID AND c.UniqServReqID = i.UniqServReqID AND i.Der_HospSpellRecordOrder = 1    
     AND i.UniqHospProvSpellID IS NULL --- to get contacts related to inpatient activity and then exclude them --updated for v5
     
 WHERE     
 (c.Der_ActivityType = 'DIRECT' AND c.AttendStatus IN ('5','6') AND 
 ((c.ConsMechanismMH NOT IN ('05', '06') AND c.UniqMonthID < 1459) OR c.ConsMechanismMH IN ('01', '02', '04', '11'))) --updated for v5 
 OR c.Der_ActivityType = 'INDIRECT'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Agg
 SELECT 
      a.Adm_month AS ReportingPeriodStartDate
     ,a.OrgIDProv
     ,a.Provider_Name
     ,a.CCG_Code as CCG_Code
     ,a.CCG_Name as CCG_Name  
     ,a.Region_Code as Region_Code
     ,a.Region_Name as Region_Name 
     ,a.STP_Code as STP_Code
     ,a.STP_Name as STP_Name
     ,CASE 
         WHEN a.WhiteBritish = 1 THEN 'White British' 
         WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
         ELSE 'Missing/invalid' 
     END as WNW_Ethnicity
     ,COUNT(DISTINCT a.UniqHospProvSpellID) AS Admissions --updated for v5
     ,SUM(CASE WHEN p.UniqHospProvSpellID IS NOT NULL THEN 1 ELSE 0 END) as Contact --updated for v5
     ,SUM(CASE WHEN p.UniqHospProvSpellID IS NULL THEN 1 ELSE 0 END) as NoContact --updated for v5
  
 FROM $db_output.CMH_Admissions a
  
 LEFT JOIN $db_output.CMH_Previous_Contacts p ON a.UniqHospProvSpellID = p.UniqHospProvSpellID AND p.RN = 1 --updated for v5
  
 WHERE a.RN = 1 AND a.Adm_month >= '$rp_startdate_qtr' AND last_day(a.Adm_month) <= '$rp_enddate'
  
 GROUP BY 
 a.Adm_month, 
 a.OrgIDProv, 
 a.Provider_Name,   
 a.CCG_Code,
 a.CCG_Name,
 a.Region_Code,
 a.Region_Name, 
 a.STP_Code,
 a.STP_Name,
 CASE 
         WHEN a.WhiteBritish = 1 THEN 'White British' 
         WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
         ELSE 'Missing/invalid' 
     END

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cmh_agg

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Access_Outpatient_Refs3
 SELECT 
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 r.IC_Rec_CCG,
 r.ReferralRequestReceivedDate,
 r.ServDischDate,
 ROW_NUMBER() OVER (PARTITION by r.UniqServReqID ORDER BY r.UniqMonthID desc) AS Ref_MnthNum
 FROM (SELECT              Der_FY,
                           UniqMonthID,
                           OrgIDProv,
                           Person_ID,
                           RecordNumber,
                           UniqServReqID,
                           CONCAT(Person_ID, UniqServReqID) as UniqPersRefID,
                           IC_Rec_CCG,
                           ReferralRequestReceivedDate,
                           ServDischDate
       FROM                $db_output.NHSE_Pre_Proc_Referral
       WHERE               AgeServReferRecDate >= 18 --people aged 18 and over
                           AND (('$end_month_id' <= '1476' AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10'))
                           OR ('$end_month_id' > '1476' AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10','A14','D05')))-- Core community MH teams, two teams added for April 2023 onwards
                           AND UniqMonthID BETWEEN $start_month_id AND $end_month_id
                           AND (LADistrictAuth LIKE 'E%' OR LADistrictAuth IS NULL OR LADistrictAuth = '') --to include England or blank Local Authorities only    
       ) r
 LEFT JOIN $db_output.NHSE_Pre_Proc_Inpatients i ON r.UniqPersRefID = i.UniqPersRefID AND r.Der_FY = i.Der_FY
 WHERE i.UniqPersRefID is null
 ---Same as DELETE from Refs1 WHERE Person and Referral_ID and UniqMonthID are in Inpatient Table (NHS E Methodology)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Access_Outpatient_medians_Refs3
 SELECT * FROM $db_output.cmh_access_outpatient_refs3 WHERE Ref_MnthNum = 1

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Activity
  
 SELECT 
 a.Der_ActivityType,
 a.Der_ActivityUniqID,
 a.Person_ID,
 a.Der_PersonID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.RecordNumber,
 a.UniqServReqID,    
 a.Der_ContactDate,
 a.Der_ContactTime,
 ROW_NUMBER() OVER (PARTITION BY a.Der_PersonID, a.UniqServReqID ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_DirectContactOrder,
 ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_RefDirectContactOrder
 FROM $db_output.NHSE_Pre_Proc_Activity a
 WHERE a.Der_ActivityType = 'DIRECT' AND a.AttendStatus IN ('5','6') AND 
 ((a.ConsMechanismMH NOT IN ('05', '06') AND a.UniqMonthID < 1459) OR a.ConsMechanismMH IN ('01', '02', '04', '11')) -- new for v5
 and a.UniqMonthID <= '$end_month_id'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Activity_Linked
 SELECT
 r.UniqMonthID,
 r.OrgIDProv,
 od.NAME as Provider_Name,
 ccg21.CCG21CDH as CCG_Code, 
 ccg21.CCG21NM as CCG_Name, 
 ccg21.NHSER21CDH as Region_Code, 
 ccg21.NHSER21NM as Region_Name,
 ccg21.STP21CDH as STP_Code,
 ccg21.STP21NM as STP_Name,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 ROW_NUMBER() OVER (PARTITION BY a.Person_ID, a.UniqServReqID ORDER BY a.Der_DirectContactOrder ASC) AS Der_DirectContactOrder
  
 FROM $db_output.CMH_Activity a
 INNER JOIN $db_output.CMH_Access_Outpatient_Refs3 r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID AND a.Der_DirectContactOrder IS NOT NULL
  
 LEFT JOIN $db_output.ccg_mapping_2021 ccg21 ON r.IC_Rec_CCG = ccg21.CCG_UNMAPPED
 LEFT JOIN $db_output.bbrb_org_daily_latest od ON r.OrgIDProv = od.ORG_CODE

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Rolling_Activity
 SELECT
     a.UniqMonthID,
     a.OrgIDProv,
     COALESCE(a.Provider_Name, 'UNKNOWN') as Provider_Name,
     COALESCE(a.CCG_Code, 'UNKNOWN') as CCG_Code,
     COALESCE(a.CCG_Name, 'UNKNOWN') as CCG_Name,
     COALESCE(a.Region_Code, 'UNKNOWN') as Region_Code,
     COALESCE(a.Region_Name, 'UNKNOWN') as Region_Name,
     COALESCE(a.STP_Code, 'UNKNOWN') as STP_Code,
     COALESCE(a.STP_Name, 'UNKNOWN') as STP_Name,
     a.Person_ID,
     a.RecordNumber,
     a.UniqServReqID,
     ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.CCG_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessCCGRN,
     ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_DirectContactOrder ASC) AS AccessProvRN,
     ROW_NUMBER () OVER (PARTITION BY a.Person_ID ORDER BY a.Der_DirectContactOrder ASC) AS AccessEngRN,
     ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.STP_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessSTPRN,
     ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.Region_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessRegionRN
  
 FROM $db_output.CMH_Activity_Linked a
  
 WHERE a.Der_DirectContactOrder = 2  ---people who have had 2 or more direct attended contacts
 AND a.UniqMonthID between $start_month_id and $end_month_id  ---getting only data for RP in question

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.CMH_Rolling_Activity

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Access_Outpatient_medians_Refs3
 SELECT * FROM $db_output.cmh_access_outpatient_refs3 WHERE Ref_MnthNum = 1

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Activity_Linked_medians
 SELECT
 r.UniqMonthID,
 '$rp_startdate_qtr' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDProv,
 od.NAME as Provider_Name,
 ccg21.CCG21CDH as CCG_Code, 
 ccg21.CCG21NM as CCG_Name, 
 ccg21.NHSER21CDH as Region_Code, 
 ccg21.NHSER21NM as Region_Name,
 ccg21.STP21CDH as STP_Code,
 ccg21.STP21NM as STP_Name,
 a.Der_ContactDate,
 r.ServDischDate,
 r.ReferralRequestReceivedDate,
 DENSE_RANK() OVER (PARTITION BY a.UniqServReqID ORDER BY a.Der_RefDirectContactOrder ASC) AS Der_DirectContactOrder
  
 FROM $db_output.cmh_access_outpatient_medians_refs3 r
 left JOIN  $db_output.CMH_Activity a ON a.UniqServReqID = r.UniqServReqID AND a.Der_RefDirectContactOrder IS NOT NULL
  
 LEFT JOIN $db_output.CCG_MAPPING_2021 ccg21 ON r.IC_Rec_CCG = ccg21.CCG_UNMAPPED
 LEFT JOIN $db_output.bbrb_org_daily_latest od ON r.OrgIDProv = od.ORG_CODE

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Rolling_Activity_medians
 SELECT
     a.UniqMonthID,
     a.OrgIDProv,
     COALESCE(a.Provider_Name, 'UNKNOWN') as Provider_Name,
     COALESCE(a.CCG_Code, 'UNKNOWN') as CCG_Code,
     COALESCE(a.CCG_Name, 'UNKNOWN') as CCG_Name,
     COALESCE(a.Region_Code, 'UNKNOWN') as Region_Code,
     COALESCE(a.Region_Name, 'UNKNOWN') as Region_Name,
     COALESCE(a.STP_Code, 'UNKNOWN') as STP_Code,
     COALESCE(a.STP_Name, 'UNKNOWN') as STP_Name,
     a.Person_ID,
     a.RecordNumber,
     a.UniqServReqID,
     a.ReferralRequestReceivedDate,
     a.Der_ContactDate,
     a.Der_DirectContactOrder,
     a.ServDischDate,
     CASE WHEN a.Der_DirectContactOrder = 2 AND a.UniqMonthID between $start_month_id and $end_month_id THEN DATEDIFF(Der_ContactDate, ReferralRequestReceivedDate) ELSE NULL END AS TIME_TO_2ND_CONTACT,
     DATEDIFF(DATE_ADD('$rp_enddate',1), ReferralRequestReceivedDate) as TimeFromRefToEndRP
  
 FROM $db_output.CMH_Activity_Linked_medians a

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Rolling_Activity_medians_2nd_cont
 SELECT 
 *
 FROM $db_output.CMH_Rolling_Activity_medians
 WHERE
 REFERRALREQUESTRECEIVEDDATE >= '2019-04-01'
 AND DER_DIRECTCONTACTORDER = 2 
 AND DER_CONTACTDATE BETWEEN '$rp_startdate_qtr' AND '$rp_enddate'

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.CMH_Rolling_Activity_medians_2nd_cont

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.CMH_Rolling_Activity_medians_still_waiting
 SELECT 
 a.*
 FROM 
 $db_output.CMH_Rolling_Activity_medians A
 LEFT JOIN (SELECT 
             UniqServReqID, 
             MAX(DER_DIRECTCONTACTORDER) AS TOTAL_CONTACTS
             FROM
             $db_output.CMH_Rolling_Activity_medians
             GROUP BY 
             UniqServReqID) C
             ON A.UniqServReqID = C.UniqServReqID
 WHERE
 UNIQMONTHID >= '$end_month_id' 
 AND REFERRALREQUESTRECEIVEDDATE >= '2019-04-01'
 AND (SERVDISCHDATE IS NULL OR SERVDISCHDATE > '$rp_enddate')
 AND C.TOTAL_CONTACTS < 2
 AND DER_DIRECTCONTACTORDER = '1'

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.CMH_Rolling_Activity_medians_still_waiting