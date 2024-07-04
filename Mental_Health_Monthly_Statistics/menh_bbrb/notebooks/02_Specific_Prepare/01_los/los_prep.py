# Databricks notebook source
# dbutils.widgets.text("db_output", "sharif_salah_101019")
# db_output = dbutils.widgets.get("db_output")
# dbutils.widgets.text("db_source", "testdata_menh_bbrb_mhsds_v5_database")
# db_source = dbutils.widgets.get("db_source")
# dbutils.widgets.text("end_month_id", "1459")
# end_month_id = dbutils.widgets.get("end_month_id")
# dbutils.widgets.text("rp_startdate_qtr", "2021-08-01")
# rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
# dbutils.widgets.text("rp_enddate", "2021-10-31")
# rp_enddate = dbutils.widgets.get("rp_enddate")

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_enddate = dbutils.widgets.get("rp_enddate")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {db_output}.spells1")
if int(end_month_id) > 1467:
  spark.sql(f"""
             CREATE TABLE {db_output}.spells1
 
              SELECT 
 
              A.PERSON_ID, 
              A.UniqMonthID,
              A.UniqHospProvSpellID,
              A.OrgIDProv,
              E.NAME AS PROV_NAME,
              A.StartDateHospProvSpell,
              A.DischDateHospProvSpell,
              DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
              B.UniqWardStayID,
              B.MHAdmittedPatientClass,
              B.StartDateWardStay,
              B.EndDateWardStay,
              DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS,
              C.AgeRepPeriodEnd,
              ccg.SubICBGPRes AS IC_REC_GP_RES
 
              FROM
              {db_source}.MHS501HospProvSpell a 
              LEFT JOIN {db_source}.MHS502WARDSTAY B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber
              LEFT JOIN {db_source}.MHS001MPI C ON A.RECORDNUMBER = C.RECORDNUMBER
              LEFT JOIN {db_output}.bbrb_org_daily_latest E ON A.ORGIDPROV = E.ORG_CODE
              LEFT JOIN {db_output}.bbrb_ccg_in_quarter ccg ON C.Person_ID = ccg.Person_ID
 
 
              WHERE
              (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE > '{rp_enddate}')
              AND a.RECORDSTARTDATE BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
              AND A.DischDateHospProvSpell BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
             """)
else:
  spark.sql(f"""
             CREATE TABLE {db_output}.spells1
 
              SELECT 
 
              A.PERSON_ID, 
              A.UniqMonthID,
              A.UniqHospProvSpellID,
              A.OrgIDProv,
              E.NAME AS PROV_NAME,
              A.StartDateHospProvSpell,
              A.DischDateHospProvSpell,
              DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
              B.UniqWardStayID,
              B.MHAdmittedPatientClass,
              B.StartDateWardStay,
              B.EndDateWardStay,
              DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS,
              C.AgeRepPeriodEnd,
              ccg.SubICBGPRes AS IC_REC_GP_RES
 
              FROM
              {db_source}.MHS501HospProvSpell a 
              LEFT JOIN {db_source}.MHS502WARDSTAY B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber
              LEFT JOIN {db_source}.MHS001MPI C ON A.RECORDNUMBER = C.RECORDNUMBER
              LEFT JOIN {db_output}.bbrb_org_daily_latest E ON A.ORGIDPROV = E.ORG_CODE 
              LEFT JOIN {db_output}.bbrb_ccg_in_quarter ccg ON C.Person_ID = ccg.Person_ID
 
              WHERE
              (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE > '{rp_enddate}') AND a.RECORDSTARTDATE BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
              AND A.DischDateHospProvSpell BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
             """)

# COMMAND ----------

 %sql
 ----this step needs to be done to bin invalid/expired ccg/sub-icb codes into UNKNOWN
 INSERT OVERWRITE TABLE $db_output.spells 
 SELECT  
 A.PERSON_ID as Person_ID, 
 A.UniqHospProvSpellID,
 A.OrgIDProv,
 A.PROV_NAME as Provider_Name,
 A.StartDateHospProvSpell,
 A.DischDateHospProvSpell,
 A.HOSP_LOS as Hosp_LOS,
 A.UniqWardStayID,
 A.MHAdmittedPatientClass,
 A.StartDateWardStay,
 A.EndDateWardStay,
 A.WARD_LOS as Ward_LOS,
 A.AgeRepPeriodEnd,
 COALESCE(F.ORG_CODE,'UNKNOWN') as CCG_Code,
 COALESCE(F.NAME, 'UNKNOWN') as CCG_Name,
 CASE WHEN (A.UniqMonthID > 1488 AND A.MHAdmittedPatientClass = '200') OR (A.UniqMonthID <= 1488 AND A.MHAdmittedPatientClass = '10') THEN 'Adult Acute'
      WHEN (A.UniqMonthID > 1488 AND A.MHAdmittedPatientClass = '201') OR (A.UniqMonthID <= 1488 AND A.MHAdmittedPatientClass = '11') THEN 'Older Adult Acute'
      ELSE 'Invalid' END AS Acute_Bed
  
 FROM $db_output.spells1 A
 LEFT JOIN $db_output.bbrb_ccg_latest F ON A.IC_REC_GP_RES = F.ORG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.spells