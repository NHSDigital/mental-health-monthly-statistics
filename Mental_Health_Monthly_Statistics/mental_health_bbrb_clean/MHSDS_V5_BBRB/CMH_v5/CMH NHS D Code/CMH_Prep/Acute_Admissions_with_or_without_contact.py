# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Inpatients

# COMMAND ----------

# DBTITLE 1,Create Hospital Spell Record Order and First Ward Stay Record Derivations from Inpatient Pre-Processing Table
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_inpatients;
 CREATE TABLE         $db_output.CMH_Inpatients USING DELTA AS
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
 SiteIDOfTreat,
 WardType,
 SpecialisedMHServiceCode,
 HospitalBedTypeMH,
 EndDateWardStay,
 EndTimeWardStay,
 ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID DESC) AS Der_HospSpellRecordOrder, --updated for v5
 ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID ASC, EndDateWardStay ASC, MHS502UniqID ASC) AS Der_FirstWardStayRecord --updated for v5
 FROM $db_output.NHSE_Pre_Proc_Inpatients

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Inpatients;
 REFRESH TABLE $db_output.CMH_Inpatients;
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Referral

# COMMAND ----------

# DBTITLE 1,Get all admissions in the RP
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_admissions;
 CREATE TABLE         $db_output.CMH_Admissions USING DELTA AS
 SELECT	
 	 i.UniqMonthID
 	,i.UniqHospProvSpellID --updated for v5
 	,i.Person_ID
     ,CASE WHEN r.NHSDEthnicity NOT IN ('A','99','-1') THEN 1 ELSE 0 END as NotWhiteBritish 
 	,CASE WHEN r.NHSDEthnicity = 'A' THEN 1 ELSE 0 END as WhiteBritish     
 	,i.OrgIDProv
     ,od.NAME as Provider_Name
     ,COALESCE(ccg21.CCG21CDH, CASE WHEN r.OrgIDCCGRes in ('X98', '') THEN 'UNKNOWN' ELSE r.OrgIDCCGRes END, 'UNKNOWN') as SubICB_Code
     ,COALESCE(ccg21.CCG21NM, 'UNKNOWN') as SubICB_Name
     ,COALESCE(ccg21.NHSER21CDH, 'UNKNOWN') as Region_Code
     ,COALESCE(ccg21.NHSER21NM, 'UNKNOWN') as Region_Name
     ,COALESCE(ccg21.STP21CDH, 'UNKNOWN') as ICB_Code
     ,COALESCE(ccg21.STP21NM, 'UNKNOWN') as ICB_Name
 	,i.StartDateHospProvSpell
 	,i.StartTimeHospProvSpell 
 	,date_add(last_day(add_months(i.StartDateHospProvSpell, -1)),1) AS Adm_month
 	,ia.HospitalBedTypeMH
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
 LEFT JOIN $db_output.CCG_MAPPING_2021 ccg21 ON r.OrgIDCCGRes = ccg21.CCG_UNMAPPED  --- regions/stps taken from CCG rather than provider 
 LEFT JOIN $db_output.CMH_ORG_DAILY od ON i.OrgIDProv = od.ORG_CODE
 	
 WHERE i.StartDateHospProvSpell BETWEEN '$rp_startdate_12m' AND '$rp_enddate'	
 AND i.UniqMonthID BETWEEN $start_month_id AND $end_month_id	---getting only data for RP in question
 AND ia.HospitalBedTypeMH IN ('10','11','12') --- adult/older acute and PICU admissions only  	
 AND i.SourceAdmMHHospProvSpell NOT IN ('49','53','87') --- excluding people transferred from other MH inpatient settings --updated for v5

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Activity;
 REFRESH TABLE $db_output.CMH_Admissions;
 REFRESH TABLE $db_output.CMH_Inpatients

# COMMAND ----------

# DBTITLE 1,Get Previous Contacts for people admitted in the RP
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_previous_contacts;
 CREATE TABLE         $db_output.CMH_Previous_Contacts USING DELTA AS
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
 	,c.AttendOrDNACode 
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
 (c.Der_ActivityType = 'DIRECT' AND c.AttendOrDNACode IN ('5','6') AND 
 ((c.ConsMechanismMH NOT IN ('05', '06') AND c.UniqMonthID < 1459) OR c.ConsMechanismMH IN ('01', '02', '04', '11'))) --updated for v5 
 OR c.Der_ActivityType = 'INDIRECT'	

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.CMH_Admissions;
 REFRESH TABLE $db_output.CMH_Previous_Contacts

# COMMAND ----------

# DBTITLE 1,Get CCG Admissions and admissions for people known to services
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_agg;
 CREATE TABLE         $db_output.CMH_Agg USING DELTA AS
 SELECT 
 	 a.Adm_month AS ReportingPeriodStartDate
     ,a.OrgIDProv
     ,a.Provider_Name
 	,a.Region_Code as Region_Code
     ,a.Region_Name as Region_Name	
 	,a.SubICB_Code as SubICB_Code
     ,a.SubICB_Name as SubICB_Name
 	,a.ICB_Code as ICB_Code
     ,a.ICB_Name as ICB_Name
     ,CASE 
 		WHEN a.WhiteBritish = 1 THEN 'White British' 
 		WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
 		ELSE 'Missing/invalid' 
 	END as Ethnicity
     ,COUNT(DISTINCT a.UniqHospProvSpellID) AS Admissions --updated for v5
     ,SUM(CASE WHEN p.UniqHospProvSpellID IS NOT NULL THEN 1 ELSE 0 END) as Contact --updated for v5
 	,SUM(CASE WHEN p.UniqHospProvSpellID IS NULL THEN 1 ELSE 0 END) as NoContact --updated for v5
 
 FROM $db_output.CMH_Admissions a
 
 LEFT JOIN $db_output.CMH_Previous_Contacts p ON a.UniqHospProvSpellID = p.UniqHospProvSpellID AND p.RN = 1 --updated for v5
 
 WHERE a.RN = 1 
 
 GROUP BY 
 a.Adm_month, 
 a.OrgIDProv, 
 a.Provider_Name,
 a.Region_Code,
 a.Region_Name,	
 a.SubICB_Code,
 a.SubICB_Name,
 a.ICB_Code,
 a.ICB_Name,
 CASE 
 		WHEN a.WhiteBritish = 1 THEN 'White British' 
 		WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
 		ELSE 'Missing/invalid' 
 	END

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.CMH_Agg

# COMMAND ----------

# DBTITLE 1,Monthly Output
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_admissionsnc_monthly_output;
 CREATE TABLE         $db_output.CMH_AdmissionsNC_Monthly_Output USING DELTA AS
 
 SELECT 
 	'England' AS OrgType 
 	,'Eng' AS OrgCode
 	,'England' AS OrgName
 	,ReportingPeriodStartDate
     ,Ethnicity
     ,SUM(Admissions) AS Admissions
     ,SUM(Contact) AS Contact
 	,SUM(NoContact) AS Nocontact
 FROM $db_output.CMH_Agg
 
 GROUP BY ReportingPeriodStartDate, Ethnicity 
 
 UNION ALL 
 
 SELECT 
 	'Provider' AS OrgType 
 	,OrgIDProv AS OrgCode
 	,Provider_Name AS OrgName
 	,ReportingPeriodStartDate
     ,Ethnicity
 	,SUM(Admissions) AS Admissions
     ,SUM(Contact) AS Contact
 	,SUM(NoContact) AS Nocontact
     
 FROM $db_output.CMH_Agg
 
 GROUP BY ReportingPeriodStartDate, OrgIDProv, Provider_Name, Ethnicity
 
 UNION ALL 
 
 SELECT 
 	'Sub ICB' AS OrgType 
 	,SubICB_Code AS OrgCode
 	,SubICB_Name AS OrgName
 	,ReportingPeriodStartDate
     ,Ethnicity
 	,SUM(Admissions) AS Admissions
     ,SUM(Contact) AS Contact
 	,SUM(NoContact) AS Nocontact
     
 FROM $db_output.CMH_Agg
 
 GROUP BY ReportingPeriodStartDate, SubICB_Code, SubICB_Name, Ethnicity
 
 UNION ALL 
 
 SELECT 
 	'ICB' AS OrgType 
 	,ICB_Code AS OrgCode
 	,ICB_Name AS OrgName
 	,ReportingPeriodStartDate
     ,Ethnicity
 	,SUM(Admissions) AS Admissions
     ,SUM(Contact) AS Contact
 	,SUM(NoContact) AS Nocontact
     
 FROM $db_output.CMH_Agg
 
 GROUP BY ReportingPeriodStartDate, ICB_Code, ICB_Name, Ethnicity
 
 UNION ALL 
 
 SELECT 
 	'Region' AS OrgType 
 	,Region_Code AS OrgCode
 	,Region_Name AS OrgName
 	,ReportingPeriodStartDate
     ,Ethnicity
 	,SUM(Admissions) AS Admissions
     ,SUM(Contact) AS Contact
 	,SUM(NoContact) AS Nocontact
 FROM $db_output.CMH_Agg
 
 GROUP BY ReportingPeriodStartDate, Region_Code, Region_Name, Ethnicity    

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))