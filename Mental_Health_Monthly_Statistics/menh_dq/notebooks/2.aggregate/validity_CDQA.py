# Databricks notebook source
# dbutils.widgets.removeAll()


# dbutils.widgets.text("db_output" , "menh_dq", "db_output")
# dbutils.widgets.text("dbm" , "testdata_menh_dq_mhsds_v5_database", "dbm")

# dbutils.widgets.text("month_id", "1449", "month_id")
# dbutils.widgets.text("reference_data", "reference_data", "reference_data")

# dbutils.widgets.text("rp_startdate", "2020-12-01", "rp_startdate")
# dbutils.widgets.text("rp_enddate", '2020-12-31', "rp_enddate")


dbm  = dbutils.widgets.get("dbm")
print(dbm)
assert dbm

db_output  = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

rp_startdate  = dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate

rp_enddate  = dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate

reference_data  = dbutils.widgets.get("reference_data")
print(reference_data)
assert reference_data

month_id  = dbutils.widgets.get("month_id")
print(month_id)
assert month_id

# COMMAND ----------

# DBTITLE 1,Accommodation Type
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Accommodation Type' AS MeasureName,
   3 as DimensionTypeId,
   64 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1 
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN vc1.Measure is not null THEN 1
         ELSE 0
       END
       ) AS Other,
 	0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND vc1.Measure is null
         AND AccommodationType <> ''
         AND AccommodationType IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN AccommodationType IS NULL
         OR AccommodationType = ''
         OR AccommodationType = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS003AccommStatus ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS003AccommStatus' and vc.field = 'AccommodationType' and vc.Measure = 'MHS-DQM64' and vc.type = 'VALID' and ref.AccommodationType = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 LEFT JOIN $db_output.validcodes as vc1
 ON vc1.tablename = 'MHS003AccommStatus' and vc1.field = 'AccommodationType' and vc1.Measure = 'MHS-DQM64' and vc1.type = 'OTHER' and ref.AccommodationType = vc1.ValidValue and $month_id >= vc1.FirstMonth and (vc1.LastMonth is null or $month_id <= vc1.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Employment Status
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Employment Status' AS MeasureName,
   3 as DimensionTypeId,
   65 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN upper(EmployStatus) IN ('01', '02', '03', '04', '05', '06', '07', '08', 'ZZ') THEN 1
         ELSE 0
       END
       ) AS Valid,
     0 as Other,
     0 as Default,
   SUM(CASE
         WHEN upper(EmployStatus) NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', 'ZZ')
         AND EmployStatus <> ''
         AND EmployStatus IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN EmployStatus IS NULL
         OR EmployStatus = ''
         OR EmployStatus = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS004EmpStatus
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Disability Code
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Disibility Code' AS MeasureName,
   3 as DimensionTypeId,
   66 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN Upper(DisabCode) IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', 'NN', 'ZZ') THEN 1
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN Upper(DisabCode) IN ('XX') THEN 1
         ELSE 0
       END
       ) AS Other,
     0 as Default,     
   SUM(CASE
         WHEN upper(DisabCode) NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', 'NN', 'ZZ', 'XX')
         AND DisabCode <> ''
         AND DisabCode IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN DisabCode IS NULL
         OR DisabCode = ''
         OR DisabCode = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS007DisabilityType
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Decided to Admit Date
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Decided to Admit Date' AS MeasureName,
   3 as DimensionTypeId,
   67 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN DecidedToAdmitDate <= '$rp_enddate' 
         AND DecidedToAdmitDate IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN DecidedToAdmitDate > '$rp_enddate'
         AND DecidedToAdmitDate IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(DecidedToAdmitDate)) = ''
         OR DecidedToAdmitDate IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.MHS501HospProvSpell
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv; 

# COMMAND ----------

# DBTITLE 1,Decided to Admit Time
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Decided to Admit Time' AS MeasureName,
   3 as DimensionTypeId,
   68 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN DecidedToAdmitTime IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   0 AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(DecidedToAdmitTime)) = ''
         OR DecidedToAdmitTime IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.MHS501HospProvSpell
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Admission Source (Mental Health Provider Spell)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Admission Source (Mental Health Provider Spell)' AS MeasureName,
   3 as DimensionTypeId,
   69 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END
       ) AS Valid,
 	  0 as Other,
   SUM(CASE
         WHEN SourceAdmMHHospProvSpell IN ('98', '99') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND SourceAdmMHHospProvSpell <> ''
         AND SourceAdmMHHospProvSpell NOT IN ('98', '99')
         AND SourceAdmMHHospProvSpell IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN SourceAdmMHHospProvSpell IS NULL
         OR SourceAdmMHHospProvSpell = ''
         OR SourceAdmMHHospProvSpell = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS501HospProvSpell ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS501HospProvSpell' and vc.field = 'SourceAdmMHHospProvSpell' and vc.Measure = 'MHS-DQM69' and vc.type = 'VALID' and ref.SourceAdmMHHospProvSpell = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Destination Of Discharge (Hospital Provider Spell)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Destination Of Discharge (Hospital Provider Spell)' AS MeasureName,
   3 as DimensionTypeId,
   70 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END
       ) AS Valid,
 	  0 as Other,
   SUM(CASE
         WHEN DestOfDischHospProvSpell IN ('98', '99') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND DestOfDischHospProvSpell <> ''
         AND DestOfDischHospProvSpell NOT IN ('98', '99')
         AND DestOfDischHospProvSpell IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN DestOfDischHospProvSpell IS NULL
         OR DestOfDischHospProvSpell = ''
         OR DestOfDischHospProvSpell = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS501HospProvSpell ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS501HospProvSpell' and vc.field = 'DestOfDischHospProvSpell' and vc.Measure = 'MHS-DQM70' and vc.type = 'VALID' and ref.DestOfDischHospProvSpell = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth) 
 WHERE UniqMonthID = $month_id and DischDateHospProvSpell is not null
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Start Time (Restrictive Intervention Incident)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Start Time (Restrictive Intervention Incident)' AS MeasureName,
   3 as DimensionTypeId,
   71 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN StartTimeRestrictiveIntInc IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   0 AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(StartTimeRestrictiveIntInc)) = ''
         OR StartTimeRestrictiveIntInc IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.MHS505RestrictiveInterventInc
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Restrictive Intervention Reason
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Restrictive Intervention Reason' AS MeasureName,
   3 as DimensionTypeId,
   72 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN RestrictiveIntReason IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19') THEN 1
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN RestrictiveIntReason IN ('98') THEN 1
         ELSE 0
       END
       ) AS Other,
   SUM(CASE
         WHEN RestrictiveIntReason IN ('99') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
         WHEN RestrictiveIntReason NOT IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '98', '99')
         AND RestrictiveIntReason <> ''
         AND RestrictiveIntReason IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN RestrictiveIntReason IS NULL
         OR RestrictiveIntReason = ''
         OR RestrictiveIntReason = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS505RestrictiveInterventInc
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Start Time (Restrictive Intervention Type)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Valid Start Time (Restrictive Intervention Type)' AS MeasureName,
   3 as DimensionTypeId,
   73 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN StartTimeRestrictiveIntType IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   0 AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(StartTimeRestrictiveIntType)) = ''
         OR StartTimeRestrictiveIntType IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.MHS515RestrictiveInterventType
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Restrictive Intervention Type
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Restrictive Intervention Type' AS MeasureName,
   3 as DimensionTypeId,
   74 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN RestrictiveIntType IN ('01', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17') THEN 1
         ELSE 0
       END
       ) AS Valid,
     0 as Other,
     0 as Default,
   SUM(CASE
         WHEN RestrictiveIntType NOT IN ('01', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17')
         AND RestrictiveIntType <> ''
         AND RestrictiveIntType IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN RestrictiveIntType IS NULL
         OR RestrictiveIntType = ''
         OR RestrictiveIntType = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS515RestrictiveInterventType
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Ward Setting Type (Mental Health)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Ward Setting Type (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   75 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN WardType IN ('01', '02', '03', '04', '05', '06') THEN 1
         ELSE 0
       END
       ) AS Valid,
     0 as Other,
     0 as Default,
   SUM(CASE
         WHEN WardType NOT IN ('01', '02', '03', '04', '05', '06')
         AND WardType <> ''
         AND WardType IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN WardType IS NULL
         OR WardType = ''
         OR WardType = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS502WardStay
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Intended Age Group (Mental Health)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Intended Age Group (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   76 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN WardAge IN ('10', '11', '12', '13', '14', '15') THEN 1
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN WardAge IN ('99') THEN 1
         ELSE 0
       END
       ) AS Other,
     0 as Default,
   SUM(CASE
         WHEN WardAge NOT IN ('10', '11', '12', '13', '14', '15', '99')
         AND WardAge <> ''
         AND WardAge IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN WardAge IS NULL
         OR WardAge = ''
         OR WardAge = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS502WardStay
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Sex Of Patients Code (Mental Health)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Sex Of Patients Code (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   77 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN WardIntendedSex IN ('1', '2') THEN 1
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN WardIntendedSex IN ('8') THEN 1
         ELSE 0
       END
       ) AS Other,
     0 as Default,
   SUM(CASE
         WHEN WardIntendedSex NOT IN ('1', '2', '8')
         AND WardIntendedSex <> ''
         AND WardIntendedSex IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN WardIntendedSex IS NULL
         OR WardIntendedSex = ''
         OR WardIntendedSex = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS502WardStay
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Intended Clinical Care Intensity Code (Mental Health)
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Intended Clinical Care Intensity Code (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   78 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN WardIntendedClinCareMH IN ('51', '52', '53', '61', '62', '63') THEN 1
         ELSE 0
       END
       ) AS Valid,
     0 as Other,
     0 as Default,
   SUM(CASE
         WHEN WardIntendedClinCareMH NOT IN ('51', '52', '53', '61', '62', '63')
         AND WardIntendedClinCareMH <> ''
         AND WardIntendedClinCareMH IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN WardIntendedClinCareMH IS NULL
         OR WardIntendedClinCareMH = ''
         OR WardIntendedClinCareMH = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS502WardStay
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Ward Security Level
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Ward Security Level' AS MeasureName,
   3 as DimensionTypeId,
   79 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN WardSecLevel IN ('0', '1', '2', '3') THEN 1
         ELSE 0
       END
       ) AS Valid,
     0 as Other,
     0 as Default,
   SUM(CASE
         WHEN WardSecLevel NOT IN ('0', '1', '2', '3')
         AND WardSecLevel <> ''
         AND WardSecLevel IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN WardSecLevel IS NULL
         OR WardSecLevel = ''
         OR WardSecLevel = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS502WardStay
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Locked Ward Indicator
 %sql
 
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Locked Ward Indicator' AS MeasureName,
   3 as DimensionTypeId,
   80 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN Upper(LockedWardInd) IN ('Y', 'N') THEN 1
         ELSE 0
       END
       ) AS Valid,
     0 as Other,
     0 as Default,
   SUM(CASE
         WHEN upper(LockedWardInd) NOT IN ('Y', 'N')
         AND LockedWardInd <> ''
         AND LockedWardInd IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN LockedWardInd IS NULL
         OR LockedWardInd = ''
         OR LockedWardInd = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS502WardStay
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;