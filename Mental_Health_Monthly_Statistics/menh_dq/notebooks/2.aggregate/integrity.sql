-- Databricks notebook source
 %py
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
 # reference_data  = dbutils.widgets.get("reference_data")
 # print(reference_data)
 # assert reference_data
 month_id  = dbutils.widgets.get("month_id")
 print(month_id)
 assert month_id

-- COMMAND ----------

TRUNCATE TABLE $db_output.dq_stg_integrity;

-- COMMAND ----------

-- DBTITLE 1,V6 Changes for Service team type
 %sql
 DROP TABLE IF EXISTS $db_output.ServiceTeamType;
 CREATE TABLE IF NOT EXISTS $db_output.ServiceTeamType AS
 ----MHS102 All
 SELECT
 s.UniqMonthID,
 s.OrgIDProv,
 s.Person_ID,
 s.UniqServReqID,
 COALESCE(s.UniqCareProfTeamID, s.UniqOtherCareProfTeamLocalID) as UniqCareProfTeamID,
 s.ServTeamTypeRefToMH as ServTeamTypeMH,
 s.ServTeamIntAgeGroup,
 s.ReferClosureDate,
 s.ReferClosReason,
 s.ReferRejectionDate,
 s.ReferRejectReason,
 s.RecordNumber,
 s.RecordStartDate,
 s.RecordEndDate
 from $dbm.mhs102otherservicetype s
 UNION ALL
 ----MHS101 v6
 SELECT
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.UniqServReqID,
 r.UniqCareProfTeamLocalID as UniqCareProfTeamID,
 r.ServTeamType as ServTeamTypeMH, 
 r.ServTeamIntAgeGroup,
 r.ServDischDate as ReferClosureDate,
 r.ReferClosReason,
 r.ReferRejectionDate,
 r.ReferRejectReason,
 r.RecordNumber,
 r.RecordStartDate,
 r.RecordEndDate
 from $dbm.mhs101referral r
 where UniqMonthID > 1488

-- COMMAND ----------

/** User note: added to support code needed for v4.1 when CAMHSTier removed **/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW REFS AS 
 
SELECT
DISTINCT
a.UniqServReqID,
a.Person_ID,
CASE
            WHEN AgeServReferRecDate BETWEEN 3 AND 17 THEN 'Under 18' 
            WHEN AgeServReferRecDate >=18 THEN '18 or over'
            ELSE 'Unknown'
END AS AgeCat,
s.ServTeamTypeMH AS TeamType,
s.UniqCareProfTeamID
FROM $dbm.MHS101REFERRAL a
--LEFT JOIN $dbm.MHS102SERVICETYPEREFERREDTO S ON a.UNIQSERVREQID = S.UNIQSERVREQID AND a.PERSON_ID = S.PERSON_ID AND S.UNIQMONTHID = '$month_id'
LEFT JOIN $db_output.ServiceTeamType S ON a.UNIQSERVREQID = S.UNIQSERVREQID AND a.PERSON_ID = S.PERSON_ID AND S.UNIQMONTHID = '$month_id'   ---V6_Changes
WHERE a.UniqMonthID = '$month_id'

-- COMMAND ----------

/** User note: added to support code needed for v4.1 when CAMHSTier removed **/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW TEAMTYPE AS
 
SELECT
r.UniqCareProfTeamID,
SUM(CASE WHEN r.UniqServReqID IS NOT NULL THEN 1 ELSE 0 END) AS TotalRefs,
SUM(CASE WHEN r.AgeCat = 'Under 18' THEN 1 ELSE 0 END) AS TotalU18Refs,
(SUM(CASE WHEN r.AgeCat = 'Under 18' THEN 1 ELSE 0 END) / SUM(CASE WHEN r.UniqServReqID IS NOT NULL THEN 1 ELSE 0 END)) *100 AS PRCNT_U18
           
FROM global_temp.REFS r
 
GROUP BY r.UniqCareProfTeamID

-- COMMAND ----------

-- DBTITLE 1,Referrals to CYP-MH services starting in RP
/** User note: updated for v4.1 when CAMHSTier removed **/

WITH Referral
AS
(
  SELECT
    r.OrgIDProv,
    r.UniqServReqID,
    CASE
       -- WHEN e.CAMHSTier IN ('1','2','3','4','9') THEN 1
        WHEN e.ServTeamTypeMH IN ('C05','C06','C07') THEN 1
        
        WHEN PRCNT_U18 > 50 THEN 1
        
        ELSE 0
    END AS Integrity
    
  FROM $dbm.mhs101referral r
  
    LEFT OUTER JOIN $db_output.ServiceTeamType e ON r.UniqServReqID = e.UniqServReqID AND e.UniqMonthID = '$month_id'   ---V6_Changes
    
    LEFT OUTER JOIN global_temp.TEAMTYPE t ON e.UniqCareProfTeamID = t.UniqCareProfTeamID
    
  WHERE r.UniqMonthID = '$month_id'
    AND r.ReferralRequestReceivedDate >= '$rp_startdate'
    AND r.ReferralRequestReceivedDate <= '$rp_enddate'
	AND r.AgeServReferRecDate  < 19
  UNION
  SELECT
    r.OrgIDProv,
    r.UniqServReqID,
    CASE
		WHEN a.TreatFuncCodeMH = '711' THEN 1
		ELSE 0
	END AS Integrity
  FROM $dbm.mhs101referral r
    INNER JOIN $dbm.mhs501hospprovspell h ON h.UniqServReqID = r.UniqServReqID AND h.UniqMonthID = '$month_id' AND StartDateHospProvSpell BETWEEN r.ReferralRequestReceivedDate AND IFNULL(r.ServDischDate,CURRENT_DATE)
    INNER JOIN $dbm.mhs502wardstay w ON w.UniqHospProvSpellID = h.UniqHospProvSpellID AND w.StartDateWardStay = h.StartDateHospProvSpell AND w.UniqMonthID = '$month_id'
    INNER JOIN $dbm.mhs503assignedcareprof a ON a.UniqHospProvSpellID = h.UniqHospProvSpellID AND a.StartDateAssCareProf = h.StartDateHospProvSpell AND a.UniqMonthID = '$month_id'
  WHERE r.UniqMonthID = '$month_id'
    AND r.ReferralRequestReceivedDate >= '$rp_startdate'
    AND r.ReferralRequestReceivedDate <= '$rp_enddate'
    AND r.AgeServReferRecDate  < 19
  UNION
  SELECT
    r.OrgIDProv,
    r.UniqServReqID,
    CASE
        WHEN w.WardType IN ('01','02') THEN 1
        
        WHEN w.WArdAge IN ('10','11','12') THEN 1
      
        ELSE 0
    END AS Integrity
  FROM $dbm.mhs101referral r
    INNER JOIN $dbm.mhs502wardstay w ON (w.UniqServReqID = r.UniqServReqID AND w.StartDateWardStay = r.ReferralRequestReceivedDate and w.UniqMonthID = '$month_id')
  WHERE r.UniqMonthID = '$month_id'
    AND r.ReferralRequestReceivedDate >= '$rp_startdate'
    AND r.ReferralRequestReceivedDate <= '$rp_enddate'
	AND r.AgeServReferRecDate  < 19
)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Referrals to CYP-MH services starting in RP' AS MeasureName,
  7 AS DimensionTypeId,
  1 AS MeasureId,
  OrgIDProv,
  COUNT(DISTINCT UniqServReqID) AS Denominator,
  SUM(Integrity) AS Integrity
FROM Referral
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Referrals to Eating Disorder services starting in RP
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Referrals to Eating Disorder services starting in RP' AS MeasureName,
  7 AS DimensionTypeId,
  2 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN PrimReasonReferralMH = '12' THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND ReferralRequestReceivedDate >= '$rp_startdate'
AND ReferralRequestReceivedDate <= '$rp_enddate'
AND AgeServReferRecDate < 19
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Coded Procedure (SNOMED Stop Clock for ED)
WITH Referral
AS
(
  SELECT
    r.OrgIDProv,
    r.UniqServReqID,
    (CASE
      WHEN vc.ValidValue IS NOT null THEN 1
      ELSE 0
    END) AS Integrity
  FROM $dbm.mhs101referral r
  LEFT OUTER JOIN $dbm.mhs201carecontact cc ON r.UniqServReqID = cc.UniqServReqID AND cc.UniqMonthID = '$month_id'
  LEFT OUTER JOIN $dbm.mhs202careactivity ca ON cc.UniqCareContID = ca.UniqCareContID AND ca.UniqMonthID = '$month_id'
  LEFT JOIN $db_output.validcodes as vc ON vc.tablename = 'MHS202CareActivity' AND vc.field = 'Procedure' AND vc.Measure = 'MHS-DIM03' AND vc.type = 'include' 
        AND CASE
              WHEN CHARINDEX(':', ca.Procedure) > 0 
              THEN RTRIM(LEFT(ca.Procedure, CHARINDEX(':',ca.Procedure) -1))
              ELSE ca.Procedure        
              END = vc.ValidValue 
        AND ca.UniqMonthID >= vc.FirstMonth 
        AND (vc.LastMonth is null or ca.UniqMonthID <= vc.LastMonth)
  WHERE r.UniqMonthID = '$month_id'
  AND r.PrimReasonReferralMH = '12'
  AND r.AgeServReferRecDate < 19
), Referral_2
AS
(
  SELECT 
    OrgIDProv, 
    UniqServReqID, 
    Integrity, 
    ROW_NUMBER() OVER(PARTITION BY OrgIdProv, UniqServReqID ORDER BY Integrity DESC) AS rn 
  FROM Referral
)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Coded Procedure (SNOMED Stop Clock for ED)' AS MeasureName,
  7 AS DimensionTypeId,
  3 AS MeasureId,  
  OrgIDProv,
  COUNT(DISTINCT UniqServReqID) AS Denominator,
  SUM(CASE
        WHEN Integrity = 1
        AND rn = 1 THEN 1
        ELSE 0
      END) AS Integrity
FROM Referral_2
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Care contact time (Hour)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Care contact time (Hour)' AS MeasureName,
  7 AS DimensionTypeId,
  4 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(CareContTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs201carecontact
WHERE UniqMonthID = '$month_id'
AND CareContTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Care contact time (Midnight)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Care contact time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  5 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(CareContTime) = 0 AND MINUTE(CareContTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs201carecontact
WHERE UniqMonthID = '$month_id'
AND CareContTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Onward referral time (Hour)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Onward referral time (Hour)' AS MeasureName,
  7 AS DimensionTypeId,
  6 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(OnwardReferTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs105onwardreferral
WHERE UniqMonthID = '$month_id'
AND OnwardReferTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Onward referral time (Midnight)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Onward referral time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  7 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(OnwardReferTime) = 0
        AND MINUTE(OnwardReferTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs105onwardreferral
WHERE UniqMonthID = '$month_id'
AND OnwardReferTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Indirect activity time (Hour)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Indirect activity time (Hour)' AS MeasureName,
  7 AS DimensionTypeId,
  8 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(IndirectActTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs204indirectactivity
WHERE UniqMonthID = '$month_id'
AND IndirectActTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Indirect activity time (Midnight)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Indirect activity time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  9 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(IndirectActTime) = 0
        AND MINUTE(IndirectActTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs204indirectactivity
WHERE UniqMonthID = '$month_id'
AND IndirectActTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Discharge plan creation time (Hour)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Discharge plan creation time (Hour)' AS MeasureName,
  7 AS DimensionTypeId,
  10 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(DischPlanCreationTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND DischPlanCreationDate IS NOT NULL
AND DischPlanCreationTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Discharge plan creation time (Midnight)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Discharge plan creation time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  11 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(DischPlanCreationTime) = 0
        AND MINUTE(DischPlanCreationTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND DischPlanCreationDate IS NOT NULL
AND DischPlanCreationTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Referral request received time (Hour)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Referral request received time (Hour)' AS MeasureName,
  7 AS DimensionTypeId,
  12 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(ReferralRequestReceivedTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND ReferralRequestReceivedTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Referral request received time (Midnight)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Referral request received time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  13 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(ReferralRequestReceivedTime) = 0
        AND MINUTE(ReferralRequestReceivedTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND ReferralRequestReceivedTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Service discharge time (Hour)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Service discharge time (Hour)' AS MeasureName,
  7 AS DimensionTypeId,
  14 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(ServDischTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND ServDischDate IS NOT NULL
AND ServDischTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Service discharge time (Midnight)
INSERT INTO $db_output.dq_stg_integrity
SELECT
  --'Service discharge time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  15 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(ServDischTime) = 0
        AND MINUTE(ServDischTime) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs101referral
WHERE UniqMonthID = '$month_id'
AND ServDischDate IS NOT NULL
AND ServDischTime IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,MHA Missing or invalid ethnicity (Proportion)
INSERT INTO $db_output.dq_stg_integrity
 
SELECT
  7 AS DimensionTypeId,
  16 AS MeasureId,  
  a.OrgIDProv,
  COUNT(*) AS Denominator,
  SUM (CASE  
        WHEN b.EthnicCategory IS NULL OR b.EthnicCategory NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'Z', '99') THEN 1 
        ELSE 0
        END) AS Integrity
FROM $dbm.mhs401mhactperiod a
LEFT JOIN $dbm.mhs001mpi b
ON a.RecordNumber = b.RecordNumber
AND a.Person_ID = b.Person_ID
WHERE a.UniqMonthID ='$month_id'
GROUP BY  a.OrgIDProv 

-- COMMAND ----------

-- DBTITLE 1,MHA Episodes starting at midnight (StartTimeMHActLegalStatusClass)
INSERT INTO $db_output.dq_stg_integrity
 
SELECT
  --'MHA Start time (Midnight)' AS MeasureName,
  7 AS DimensionTypeId,
  17 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(StartTimeMHActLegalStatusClass) = 0
        AND MINUTE(StartTimeMHActLegalStatusClass) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs401mhactperiod
WHERE UniqMonthID = '$month_id'
AND StartDateMHActLegalStatusClass IS NOT NULL
AND StartTimeMHActLegalStatusClass IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,MHA Episodes starting at midday
INSERT INTO $db_output.dq_stg_integrity
 
SELECT
  --'MHA Start time (Midday)' AS MeasureName,
  7 AS DimensionTypeId,
  18 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN HOUR(StartTimeMHActLegalStatusClass) = 12
        AND MINUTE(StartTimeMHActLegalStatusClass) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs401mhactperiod
WHERE UniqMonthID = '$month_id'
AND StartDateMHActLegalStatusClass IS NOT NULL
AND StartTimeMHActLegalStatusClass IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,MHA starting on the hour
INSERT INTO $db_output.dq_stg_integrity
 
SELECT
  --'MHA Starting on the hour' AS MeasureName,
  7 AS DimensionTypeId,
  19 AS MeasureId,  
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN MINUTE(StartTimeMHActLegalStatusClass) = 0 THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs401mhactperiod
WHERE UniqMonthID = '$month_id'
AND StartDateMHActLegalStatusClass IS NOT NULL
AND StartTimeMHActLegalStatusClass IS NOT NULL
GROUP BY OrgIDProv;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW mhs401mhact AS 
 
SELECT *
FROM $dbm.mhs401mhactperiod 
WHERE UniqMonthID = '$month_id'

-- COMMAND ----------

-- DBTITLE 1,MHA becoming inactive
INSERT INTO $db_output.dq_stg_integrity
 
SELECT
  --'MHA becoming inactive' AS MeasureName,
  7 AS DimensionTypeId,
  20 AS MeasureId,  
  a.OrgIDProv,
  COUNT(*) AS Denominator,
   SUM(CASE
        WHEN b.UniqMHActEpisodeID IS NULL THEN 1
        ELSE 0
      END) AS Integrity
FROM $dbm.mhs401mhactperiod a 
LEFT JOIN global_temp.mhs401mhact b on a.UniqMHActEpisodeID = b.UniqMHActEpisodeID 
WHERE a. UniqMonthID = '$month_id'-1
AND a.StartDateMHActLegalStatusClass IS NOT NULL
AND a.StartTimeMHActLegalStatusClass IS NOT NULL
AND a.EndDateMHActLegalStatusClass IS NULL
GROUP BY a.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Denominator
WITH InventoryList
AS
(
  SELECT 
    DimensionTypeId, 
    MeasureId, 
    OrgIDProv, 
    'Denominator' AS MeasureTypeName, 
    Denominator AS Value 
  FROM $db_output.dq_stg_integrity
)
MERGE INTO $db_output.dq_inventory AS target
USING
(
  SELECT
    '$month_id' AS UniqMonthID,
    im.DimensionTypeId,
    im.MeasureId,
    im.MeasureTypeId,
    im.MetricTypeId,
    il.OrgIDProv,
    il.Value
  FROM InventoryList il
  INNER JOIN $db_output.dq_vw_inventory_metadata im ON (im.DimensionTypeName = 'Integrity'
                                                 AND il.DimensionTypeId = im.DimensionTypeId
                                                 AND il.MeasureId = im.MeasureId
                                                 AND il.MeasureTypeName = im.MeasureTypeName)
  WHERE im.StartDate <= '$rp_startdate'
  AND (CASE
        WHEN im.EndDate IS NULL THEN '$rp_enddate'
       ELSE im.EndDate
      END) >= '$rp_enddate'
) AS source ON (target.UniqMonthID = source.UniqMonthID
                AND target.DimensionTypeId = source.DimensionTypeId
                AND target.MeasureId = source.MeasureId
                AND target.MeasureTypeId = source.MeasureTypeId
                AND target.OrgIDProv = source.OrgIDProv
                AND (CASE
                      WHEN target.MetricTypeId IS NULL THEN 'null'
                      ELSE target.MetricTypeId
                     END) = (CASE
                               WHEN source.MetricTypeId IS NULL THEN 'null'
                               ELSE source.MetricTypeId
                             END)
                AND target.SOURCE_DB = '$dbm'
)
WHEN MATCHED THEN 
  UPDATE SET target.Value = source.Value
WHEN NOT MATCHED THEN 
  INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
  VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');

-- COMMAND ----------

-- DBTITLE 1,Integrity
WITH InventoryList
AS
(
  SELECT 
    DimensionTypeId, 
    MeasureId, 
    OrgIDProv, 
    'Integrity' AS MetricTypeName, 
    Integrity AS Value 
  FROM $db_output.dq_stg_integrity
)
MERGE INTO $db_output.dq_inventory AS target
USING
(
  SELECT
    '$month_id' AS UniqMonthID,
    im.DimensionTypeId,
    im.MeasureId,
    im.MeasureTypeId,
    im.MetricTypeId,
    il.OrgIDProv,
    il.Value
  FROM InventoryList il
    INNER JOIN $db_output.dq_vw_inventory_metadata im ON (im.DimensionTypeName = 'Integrity'
                                                   AND il.DimensionTypeId = im.DimensionTypeId
                                                   AND il.MeasureId = im.MeasureId
                                                   AND il.MetricTypeName = im.MetricTypeName)
  WHERE im.StartDate <= '$rp_startdate'
  AND (CASE
        WHEN im.EndDate IS NULL THEN '$rp_enddate'
        ELSE im.EndDate
      END) >= '$rp_enddate'
) AS source ON (target.UniqMonthID = source.UniqMonthID
                AND target.DimensionTypeId = source.DimensionTypeId
                AND target.MeasureId = source.MeasureId
                AND target.MeasureTypeId = source.MeasureTypeId
                AND target.OrgIDProv = source.OrgIDProv
                AND (CASE
                      WHEN target.MetricTypeId IS NULL THEN 'null'
                      ELSE target.MetricTypeId
                     END) = (CASE
                              WHEN source.MetricTypeId IS NULL THEN 'null'
                              ELSE source.MetricTypeId
                            END)
                AND target.SOURCE_DB = '$dbm')
WHEN MATCHED THEN
  UPDATE SET target.Value = source.Value
WHEN NOT MATCHED THEN
  INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
  VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');