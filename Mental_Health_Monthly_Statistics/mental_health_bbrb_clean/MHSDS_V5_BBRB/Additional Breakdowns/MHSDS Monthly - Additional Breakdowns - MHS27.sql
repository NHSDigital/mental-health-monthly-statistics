
-- COMMAND ----------

-- DBTITLE 1,England breakdown added for testing purpose, will need removing at BBRB final output 01/09/22
 %sql
 
 insert into $db_output.output1
 Select '$rp_startdate_1m' AS REPORTING_PERIOD_START
 ,'$rp_enddate' AS REPORTING_PERIOD_END
 ,'$status' AS STATUS
 ,'England' AS BREAKDOWN
 ,'England' AS PRIMARY_LEVEL
 ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 ,'NONE' AS SECONDARY_LEVEL
 ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 ,'MHS27a' AS MEASURE_ID
 ,COUNT(DISTINCT a.UniqHospProvSpellID) AS MEASURE_VALUE
 ,'$db_source' AS SOURCE_DB
 FROM $db_source.MHS501HospProvSpell a
 LEFT JOIN $db_source.MHS502WardStay b on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.StartDateHospProvSpell = b.StartDateWardStay and ((a.starttimehospprovspell = b.starttimewardstay) or a.starttimehospprovspell is null) and b.UniqMonthID = '$end_month_id'
 WHERE a.UNiqMonthID = '$end_month_id'
 AND StartDateHospProvSpell >= '$rp_startdate_1m'
 AND StartDateHospProvSpell <= '$rp_enddate'

-- COMMAND ----------

 %sql
 
 insert into $db_output.output1
 Select '$rp_startdate_1m' AS REPORTING_PERIOD_START
 ,'$rp_enddate' AS REPORTING_PERIOD_END
 ,'$status' AS STATUS
 ,'England; Bed Type' AS BREAKDOWN
 ,'England' AS PRIMARY_LEVEL
 ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END AS SECONDARY_LEVEL
 ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END AS SECONDARY_LEVEL_DESCRIPTION
 ,'MHS27a' AS MEASURE_ID
 ,COUNT(DISTINCT a.UniqHospProvSpellID) AS MEASURE_VALUE
 ,'$db_source' AS SOURCE_DB
 FROM $db_source.MHS501HospProvSpell a
 LEFT JOIN $db_source.MHS502WardStay b on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.StartDateHospProvSpell = b.StartDateWardStay and ((a.starttimehospprovspell = b.starttimewardstay) or a.starttimehospprovspell is null) and b.UniqMonthID = '$end_month_id'
 WHERE a.UNiqMonthID = '$end_month_id'
 AND StartDateHospProvSpell >= '$rp_startdate_1m'
 AND StartDateHospProvSpell <= '$rp_enddate'
 GROUP BY
  CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END
 order by 
 SECONDARY_LEVEL

-- COMMAND ----------

 %sql
 
 insert into $db_output.output1
 Select '$rp_startdate_1m' AS REPORTING_PERIOD_START
 ,'$rp_enddate' AS REPORTING_PERIOD_END
 ,'$status' AS STATUS
 ,'Provider; Bed Type' AS BREAKDOWN
 ,a.OrgIDProv AS PRIMARY_LEVEL
 ,'' AS PRIMARY_LEVEL_DESCRIPTION
 ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END AS SECONDARY_LEVEL
 ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END AS SECONDARY_LEVEL_DESCRIPTION
 ,'MHS27a' AS MEASURE_ID
 ,COUNT(DISTINCT a.UniqHospProvSpellID) AS MEASURE_VALUE
 ,'$db_source' AS SOURCE_DB
 FROM $db_source.MHS501HospProvSpell a
 LEFT JOIN $db_source.MHS502WardStay b on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.StartDateHospProvSpell = b.StartDateWardStay and ((a.starttimehospprovspell = b.starttimewardstay) or a.starttimehospprovspell is null) and b.UniqMonthID = '$end_month_id'
 WHERE a.UNiqMonthID = '$end_month_id'
 AND StartDateHospProvSpell >= '$rp_startdate_1m'
 AND StartDateHospProvSpell <= '$rp_enddate'
 GROUP BY 
  CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END
 ,a.OrgIDProv
 order by 
 SECONDARY_LEVEL

-- COMMAND ----------

 %sql
 
 insert into $db_output.output1
 Select '$rp_startdate_1m' AS REPORTING_PERIOD_START
 ,'$rp_enddate' AS REPORTING_PERIOD_END
 ,'$status' AS STATUS
 ,'Sub ICB - GP Practice or Residence; Bed Type' AS BREAKDOWN -- updated added space 04/10/22
 ,c.IC_REC_GP_RES AS PRIMARY_LEVEL
 ,c.NAME AS PRIMARY_LEVEL_DESCRIPTION
 ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END AS SECONDARY_LEVEL
 ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END AS SECONDARY_LEVEL_DESCRIPTION
 ,'MHS27a' AS MEASURE_ID
 ,COUNT(DISTINCT a.UniqHospProvSpellID) AS MEASURE_VALUE
 ,'$db_source' AS SOURCE_DB
 FROM $db_source.MHS501HospProvSpell a
 LEFT JOIN $db_source.MHS502WardStay b on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.StartDateHospProvSpell = b.StartDateWardStay and ((a.starttimehospprovspell = b.starttimewardstay) or a.starttimehospprovspell is null) and b.UniqMonthID = '$end_month_id'
 LEFT JOIN $db_output.tmp_MHMAB_CCG c on a.person_id = c.person_id
 WHERE a.UniqMonthID = '$end_month_id'
 AND StartDateHospProvSpell >= '$rp_startdate_1m'
 AND StartDateHospProvSpell <= '$rp_enddate'
 GROUP BY
  CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
       WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
       ELSE 'UNKNOWN'
       END,
 c.IC_REC_GP_RES,
 C.NAME
 order by 
 SECONDARY_LEVEL

-- COMMAND ----------

 %py
 import json
 dbutils.notebook.exit(json.dumps({
   "status": "OK"
 }))