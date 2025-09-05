-- Databricks notebook source
--DELETE FROM $db_output.integrity_rules;
CREATE WIDGET TEXT db_output DEFAULT "menh_dq";
CREATE WIDGET TEXT dbm DEFAULT "mhsds_database";
CREATE WIDGET TEXT reference_data DEFAULT "reference_data";
CREATE WIDGET TEXT month_id DEFAULT "1445";
CREATE WIDGET TEXT rp_startdate DEFAULT "2020-08-01";
CREATE WIDGET TEXT rp_enddate DEFAULT "2020-08-31";
CREATE WIDGET TEXT status DEFAULT "Performance";
CREATE WIDGET TEXT Reporting_Period_Start DEFAULT "2020-08-01";
CREATE WIDGET TEXT Reporting_Period_End DEFAULT "2020-08-31";
CREATE WIDGET TEXT MonthPeriod DEFAULT "Aug-2020";  

-- COMMAND ----------

-- DBTITLE 1,Extract Integrity Rules
--INSERT INTO $db_output.integrity_rules
--CREATE OR REPLACE TEMPORARY VIEW dq_coverage_monthly_csv AS 
SELECT 
  MeasureNumber AS `DI Measure Number`,
  MeasureName AS `DI Measure`,
  MeasureDescription AS `Description`,
  DataItem AS `MHSDS Data Item`,
  Denominator AS `Denominator`,
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
;