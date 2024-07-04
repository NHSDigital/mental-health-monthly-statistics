-- Databricks notebook source
-- DBTITLE 1,Create View dq_vw_org_daily
--do not use this view as it must be created in relation to the current reporting period.

-----------------------------------------------------------------------------------------------------------
-- The following VIEW fetches unique organisation records. This will only include valid records of
-- organisations (BUSINESS_END_DATE IS NULL) and only organisations that are still open (ORG_CLOSE_DATE IS NULL)
--
-- Example organisations:
-- 
-- ORG_CODE   BUSINESS_START_DATE   BUSINESS_END_DATE   ORG_CLOSE_DATE
--
-- 01A        2013-05-10            NULL                NULL
-- 01A        2013-04-01            2013-05-09          NULL
--
-- Y02588     2009-04-01            2014-02-12          NULL
-- Y02588     2014-02-13            2017-02-24          2010-04-02
--
-- Y02590     2014-04-18            NULL                2012-09-30
-- Y02590     2009-03-01            2013-10-15          NULL
-- Y02590     2014-04-17            2014-04-17          NULL
-- Y02590     2013-10-16            2014-04-16          2013-09-30
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW $db.dq_vw_org_daily
AS
WITH UniqueOrgList
AS
(
SELECT
  ORG_CODE,
  NAME,
  ROW_NUMBER() OVER(PARTITION BY ORG_CODE ORDER BY IFNULL(BUSINESS_END_DATE, CURRENT_DATE()) DESC, IFNULL(ORG_CLOSE_DATE, CURRENT_DATE()) DESC) AS RowNumber
FROM $reference_data.org_daily
WHERE BUSINESS_END_DATE IS NULL  
--We don't care if the org has a close date since it flowed we just need the details for reporting
)
SELECT
  ORG_CODE,
  NAME
FROM UniqueOrgList
WHERE RowNumber = 1;


-- COMMAND ----------

-- DBTITLE 1,CREATE VIEW dq_vw_inventory_metadata
CREATE OR REPLACE VIEW $db.dq_vw_inventory_metadata
AS
SELECT
  dt.DimensionTypeId,
  m.MeasureId,
  mtt.MetricTypeId,
  (CASE dt.DimensionTypeId
    WHEN 7 THEN -- Integrity
      CONCAT(dt.DimensionTypeId, '.', m.MeasureId) -- MHS-DIM00 metrics: Drop last digit
    ELSE -- Validity
      CONCAT(dt.DimensionTypeId, '.', m.MeasureId, '.', md.MetricTypeId) -- MHS-DQM00 metrics
  END) AS MetricId,
  dt.DimensionTypePrefix,
  dt.DimensionTypeName,
  mst.MeasureTypeId,
  CONCAT(dt.DimensionTypePrefix, LPAD(m.MeasureId, 2, '0')) AS MeasureNumber,
  m.MeasureName,
  m.MeasureDescription,
  m.DataItem,
  mst.MeasureTypeName,
  (CASE
    WHEN mtt.MetricTypeName IS NULL THEN mst.MeasureTypeName
    ELSE mtt.MetricTypeName
  END) AS MetricTypeName,
  md.MeasureTypeDescription,
  md.StartDate,
  md.EndDate
FROM $db.dq_dimension_type dt
  INNER JOIN $db.dq_measure m ON dt.DimensionTypeId = m.DimensionTypeId
  INNER JOIN $db.dq_measure_description md ON (m.DimensionTypeId = md.DimensionTypeId AND m.MeasureId = md.MeasureId)
  INNER JOIN $db.dq_measure_type mst ON md.MeasureTypeId = mst.MeasureTypeId
  LEFT OUTER JOIN $db.dq_metric_type mtt ON (m.DimensionTypeId = mtt.DimensionTypeId AND md.MetricTypeId = mtt.MetricTypeId)

-- COMMAND ----------

-- DBTITLE 1,CREATE VIEW dq_vw_inventory_rollup
CREATE OR REPLACE VIEW $db.dq_vw_inventory_rollup
AS
WITH InventoryList
AS
(
  SELECT
    i.UniqMonthID,
    i.OrgIDProv,
    i.DimensionTypeId,
    i.MeasureId,
    i.MetricTypeId,
    mt.MeasureTypeName,
    SUM(i.Value) AS Value,
    SOURCE_DB
  FROM $db.dq_inventory i
    INNER JOIN $db.dq_measure_type mt ON i.MeasureTypeId = mt.MeasureTypeId
GROUP BY
  GROUPING SETS
  (
    (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeName, MetricTypeId, SOURCE_DB), -- This is the England pseudo-org
    (UniqMonthID, OrgIDProv, DimensionTypeId, MeasureId, MeasureTypeName, MetricTypeId, SOURCE_DB) -- These are the provider orgs
  )
)
SELECT
  il.UniqMonthID,
  CASE
    WHEN il.OrgIDProv IS NULL THEN '-1' -- This is the England pseudo OrgIdProv (WARNING: Must be a string, ie '-1')
    ELSE il.OrgIDProv
  END AS OrgIDProv,
  CASE
    WHEN od.NAME IS NULL THEN 'England'
    ELSE od.NAME
  END AS OrgNameProv,
  il.DimensionTypeId,
  il.MeasureId,
  il.MetricTypeId,
  il.MeasureTypeName,
  il.Value,
  il.SOURCE_DB
FROM InventoryList il
  LEFT OUTER JOIN $db.dq_vw_org_daily od   -- reference_data.org_daily
  ON il.OrgIDProv = od.ORG_CODE
--WHERE od.BUSINESS_END_DATE IS NULL --this is already accounted for in creating dq_vw_org_daily
--AND od.ORG_CLOSE_DATE IS NULL
--ORDER BY OrgIdProv, UniqMonthID, DimensionTypeId, MeasureId, MetricTypeId

-- COMMAND ----------

-- DBTITLE 1,CREATE VIEW dq_vw_vodim_report
-----------------------------------------------------------------------------------------------------------
-- Suppression Rules:
-- 1) Data at National / England level is unrounded.
-- 2) Counts at sub national level are rounded to the nearest 5 and suppressed when less than 5.
-- 3) Proportions at sub national level are rounded to the nearest whole number and suppressed when either the numerator or denominator are less than 5.
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW $db.dq_vw_vodim_report
AS
WITH DivideAndRound
AS
(
  SELECT
    numerators.UniqMonthID,
    numerators.OrgIDProv,
    numerators.OrgNameProv,
    numerators.DimensionTypeId,
    numerators.MeasureId,
    numerators.MetricTypeId,
    (CASE
      WHEN numerators.Value >= 5 AND numerators.OrgIDProv != '-1' THEN -- Provider totals (WARNING: OrgIdProv must be a string, ie '-1')
        CAST(ROUND(numerators.Value * 2, -1) / 2 AS INT) -- DO round to nearest 5
      WHEN numerators.Value >= 0 and numerators.OrgIDProv = '-1' THEN -- England totals (WARNING: OrgIdProv must be a string, ie '-1')
        numerators.Value -- Do NOT round
      ELSE
        NULL -- Surpress value because less than 5
    END) AS Numerator,
    denominators.Value AS Denominator,
    (CASE
      WHEN numerators.Value >= 5 AND numerators.OrgIDProv != '-1'  THEN
        CAST(ROUND(CAST(numerators.Value AS FLOAT) / CAST(denominators.Value AS FLOAT) * 100, 0) AS INT) -- Round to int
        WHEN numerators.Value >0 AND numerators.OrgIDProv = '-1'  THEN
        CAST(ROUND(CAST(numerators.Value AS FLOAT) / CAST(denominators.Value AS FLOAT) * 100, 0) AS INT) -- Round to int
      ELSE
        NULL -- Surpress value because less than 5 count
    END) AS Percentage,
    numerators.SOURCE_DB
  FROM $db.dq_vw_inventory_rollup numerators
  INNER JOIN $db.dq_vw_inventory_rollup denominators ON denominators.MeasureTypeName = 'Denominator'
                                                        AND numerators.OrgIDProv = denominators.OrgIDProv
                                                        AND numerators.DimensionTypeId = denominators.DimensionTypeId
                                                        AND numerators.MeasureId = denominators.MeasureId
                                                        AND numerators.UniqMonthID = denominators.UniqMonthID
                                                        AND numerators.SOURCE_DB = denominators.SOURCE_DB
)
SELECT DISTINCT
  dar.UniqMonthID,
  dar.OrgIDProv,
  dar.OrgNameProv,
  md.DimensionTypeId,
  md.MeasureId,
  md.MetricTypeId,
  md.MetricId,
  md.DimensionTypeName,
  md.MeasureName,
  CONCAT(md.DimensionTypePrefix, LPAD(md.MeasureId, 2, '0'), ' ', md.MeasureName) AS MeasureNumber,
  md.MetricTypeName,
  dar.Numerator,
  dar.Denominator,
  dar.Percentage,
  dar.SOURCE_DB
FROM DivideAndRound dar
INNER JOIN $db.dq_vw_inventory_metadata md ON dar.DimensionTypeId = md.DimensionTypeId
                                              AND dar.MeasureId = md.MeasureId
                                              AND dar.MetricTypeId = md.MetricTypeId
--ORDER BY UniqMonthID, MeasureNumber,OrgIdProv, MetricId                                              