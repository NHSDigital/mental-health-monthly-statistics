# Databricks notebook source
# DBTITLE 1,NULL or Blank LADistrictAuth values in RP
 %sql
 select distinct LADistrictAuth, count(LADistrictAuth) 
 from $db_source.mhs001mpi
 where UniqMonthID between $start_month_id and $end_month_id
 group by LADistrictAuth
 order by LADistrictAuth
 limit 2

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Referral

# COMMAND ----------

# DBTITLE 1,Identify all Core Community MH Referrals in RP
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_refs1;
 CREATE TABLE         $db_output.CMH_Access_Refs1 USING DELTA AS
 
 SELECT	            Der_FY,
                     UniqMonthID,
                     OrgIDProv,
                     Person_ID,
                     RecordNumber,
                     UniqServReqID,
                     CONCAT(Person_ID, UniqServReqID) as UniqPersRefID,
                     OrgIDCCGRes-- OrgIDCCGRes field now derived from either OrgIDCCGRes or OrgIDSubICBLocResidence depending on UniqMonthID 7/9/22 
                    
 FROM                $db_output.NHSE_Pre_Proc_Referral 
 WHERE               AgeServReferRecDate >= 18 --people aged 18 and over
                     AND (('$end_month_id' <= '1476' AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10'))
                           OR ('$end_month_id' > '1476' AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10','A14','D05')))-- Core community MH teams, two teams added for April 2023 onwards
                     AND UniqMonthID BETWEEN $start_month_id AND $end_month_id
                     AND (LADistrictAuth LIKE 'E%' OR LADistrictAuth IS NULL OR LADistrictAuth = '') --to include England or blank Local Authorities only               

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.CMH_Access_Refs1;
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Inpatients

# COMMAND ----------

# DBTITLE 1,Remove Referrals to Inpatient Services
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_outpatient_refs3;
 CREATE TABLE         $db_output.CMH_Access_Outpatient_Refs3 USING DELTA AS
 SELECT 
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDCCGRes-- OrgIDCCGRes field now derived from either OrgIDCCGRes or OrgIDSubICBLocResidence depending on UniqMonthID 7/9/22 
 FROM $db_output.CMH_Access_Refs1 r
 LEFT JOIN $db_output.NHSE_Pre_Proc_Inpatients i ON r.UniqPersRefID = i.UniqPersRefID AND r.Der_FY = i.Der_FY
 WHERE i.UniqPersRefID is null

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Activity

# COMMAND ----------

# DBTITLE 1,Build Der_DirectContactOrder derivation for Direct and Attended Activity only
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_activity;
 CREATE TABLE         $db_output.CMH_Activity USING DELTA AS
 
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
 ROW_NUMBER() OVER (PARTITION BY a.Der_PersonID, a.UniqServReqID ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_DirectContactOrder
 FROM $db_output.NHSE_Pre_Proc_Activity a
 WHERE a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND 
 ((a.ConsMechanismMH NOT IN ('05', '06') AND a.UniqMonthID < 1459) OR a.ConsMechanismMH IN ('01', '02', '04', '11')) -- new for v5

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.cmh_activity;
 REFRESH TABLE $db_output.CMH_Access_Outpatient_Refs3

# COMMAND ----------

# DBTITLE 1,Link Referrals to Direct and Attended Activity
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_activity_linked;
 CREATE TABLE         $db_output.CMH_Activity_Linked USING DELTA AS
 SELECT
 r.UniqMonthID,
 r.OrgIDProv,
 od.NAME as Provider_Name,
 ccg21.CCG21CDH as SubICB_Code, 
 ccg21.CCG21NM as SubICB_Name, 
 ccg21.NHSER21CDH as Region_Code, 
 ccg21.NHSER21NM as Region_Name,
 ccg21.STP21CDH as ICB_Code,
 ccg21.STP21NM as ICB_Name,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 ROW_NUMBER() OVER (PARTITION BY a.Person_ID, a.UniqServReqID ORDER BY a.Der_DirectContactOrder ASC) AS Der_DirectContactOrder
 
 FROM $db_output.CMH_Activity a
 INNER JOIN $db_output.CMH_Access_Outpatient_Refs3 r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID AND a.Der_DirectContactOrder IS NOT NULL
 
 LEFT JOIN $db_output.CCG_MAPPING_2021 ccg21 ON r.OrgIDCCGRes = ccg21.CCG_UNMAPPED
 LEFT JOIN $db_output.CMH_ORG_DAILY od ON r.OrgIDProv = od.ORG_CODE

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.CMH_Activity_Linked

# COMMAND ----------

# DBTITLE 1,Count each Person once at each Org Level (England, Provider, CCG, STP, Region)
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_rolling_activity;
 CREATE TABLE         $db_output.CMH_Rolling_Activity USING DELTA AS
 SELECT
 	a.UniqMonthID,
 	a.OrgIDProv,
     COALESCE(a.Provider_Name, 'UNKNOWN') as Provider_Name,
 	COALESCE(a.SubICB_Code, 'UNKNOWN') as SubICB_Code,
     COALESCE(a.SubICB_Name, 'UNKNOWN') as SubICB_Name,
 	COALESCE(a.Region_Code, 'UNKNOWN') as Region_Code,
     COALESCE(a.Region_Name, 'UNKNOWN') as Region_Name,
 	COALESCE(a.ICB_Code, 'UNKNOWN') as ICB_Code,
     COALESCE(a.ICB_Name, 'UNKNOWN') as ICB_Name,
 	a.Person_ID,
 	a.RecordNumber,
 	a.UniqServReqID,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.SubICB_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessSubICBRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_DirectContactOrder ASC) AS AccessRNProv,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_DirectContactOrder ASC) AS AccessEngRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.ICB_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessICBRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.Region_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessRegionRN
 
 FROM $db_output.CMH_Activity_Linked a
 
 WHERE a.Der_DirectContactOrder = 2  ---people who have had 2 or more direct attended contacts
 AND a.UniqMonthID between $start_month_id and $end_month_id  ---getting only data for RP in question

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))