-- Databricks notebook source
DELETE FROM $db_output.integrity_rules

-- COMMAND ----------

INSERT INTO $db_output.integrity_rules
--CREATE OR REPLACE TEMPORARY VIEW dq_coverage_monthly_csv AS 
SELECT 
  MeasureNumber,
  MeasureName,
  MeasureDescription,
  DataItem,
  Denominator,
  (CASE
    WHEN Integrity IS NULL THEN 'N/A'
    ELSE Integrity
  END) AS `Numerator`
FROM
(
  SELECT
    MeasureNumber,
    MeasureName,
    MeasureDescription,
    DataItem,
    MetricTypeName,
    MeasureTypeDescription
  FROM $db_output.dq_vw_inventory_metadata
  WHERE DimensionTypeName = 'Integrity'
  AND MeasureTypeName IN ('Denominator', 'Numerator')
  AND MetricTypeName IN ('Denominator', 'Integrity')
) PIVOT (
  MAX(MeasureTypeDescription)
  FOR MetricTypeName IN ('Denominator', 'Integrity')  
)