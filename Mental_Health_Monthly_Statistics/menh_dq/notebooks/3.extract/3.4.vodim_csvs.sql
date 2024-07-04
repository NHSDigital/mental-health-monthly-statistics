-- Databricks notebook source
 %py
 dbm  = dbutils.widgets.get("dbm")
 print(dbm)
 assert dbm

-- COMMAND ----------

-- DBTITLE 1,Extract data into DQ_VODIM_monthly_CSV table (for publication)
DELETE FROM $db_output.DQ_VODIM_monthly_csv WHERE Month_Id = '$month_id' AND status = '$status' and SOURCE_DB = '$dbm';

with vodim_dataset as
(
(select 
  UniqMonthID,
  OrgIDProv, 
  OrgNameProv, 
  DimensionTypeId, 
  MeasureId, 
  MeasureName, 
  MeasureNumber, 
  MetricID, 
  MetricTypeId, 
  MetricTypeName, 
  'Count' as Unit,
  Numerator as Metric_Value 
 from $db_output.dq_vw_vodim_report
 where UniqMonthID = $month_id)
Union
(select  
  UniqMonthID,
  OrgIDProv, 
  OrgNameProv, 
  DimensionTypeId, 
  MeasureId, 
  MeasureName, 
  MeasureNumber, 
  MetricID, 
  MetricTypeId, 
  MetricTypeName, 
  'Percentage' as Unit, 
  Percentage as Metric_Value 
 from $db_output.dq_vw_vodim_report
 where UniqMonthID = $month_id)
)

INSERT INTO $db_output.DQ_VODIM_monthly_csv
select
    '$month_id' as Month_Id,
    '$MonthPeriod' AS Reporting_Period,
    '$status' as Status,
    (CASE OrgIdProv WHEN '-1' THEN 'England' ELSE 'Provider' END) AS Reporting_Level,
    (CASE OrgIdProv WHEN '-1' THEN 'All' ELSE OrgIdProv END) AS Provider_Code,
    (CASE OrgIdProv WHEN '-1' THEN 'All' ELSE OrgNameProv END) AS Provider_Name,
    left(MeasureNumber,9) AS DQ_Measure,
    MeasureName as DQ_Measure_Name,
    MetricTypeName AS DQ_Result,
    MetricId AS DQ_Dataset_Metric_ID,
    Unit,
    IFNULL(Metric_Value,'*') as Value,
    '$dbm' AS SOURCE_DB
FROM vodim_dataset


-- COMMAND ----------

-- SELECT * FROM $db_output.DQ_VODIM_monthly_csv
-- WHERE Month_Id = '$month_id'
-- order by DQ_Measure

-- COMMAND ----------

-- DBTITLE 1,Extract data into DQ_VODIM_monthly_pbi table (for Power BI)
DELETE FROM $db_output.DQ_VODIM_monthly_pbi WHERE Month_Id = '$month_id' AND status = '$status' and SOURCE_DB = '$dbm';
INSERT INTO $db_output.DQ_VODIM_monthly_pbi
SELECT
  '$month_id' as Month_Id,
  '$MonthPeriod' AS Reporting_Period,
  '$status' AS Status,
  (CASE OrgIdProv WHEN '-1' THEN 'National' ELSE 'Provider' END) AS Reporting_Level,
  (CASE OrgIdProv WHEN '-1' THEN 'All' ELSE OrgIdProv END) AS Provider_Code,
  (CASE OrgIdProv WHEN '-1' THEN 'All' ELSE OrgNameProv END) AS Provider_Name,
  MeasureNumber as DQ_Measure,
  MetricTypeName as DQ_Result,
  MetricID as DQ_Dataset_Metric_Id,
  TRY_CAST(IFNULL(Numerator,'*')AS BIGINT ) as Count,
  IFNULL(TRY_CAST(Percentage AS BIGINT),'*')  as Percentage,
  SOURCE_DB
FROM $db_output.dq_vw_vodim_report
WHERE uniqMonthId = '$month_id'
AND SOURCE_DB = '$dbm'

-- COMMAND ----------

-- SELECT * FROM $db_output.DQ_VODIM_monthly_pbi
-- WHERE Month_Id = '$month_id' 
-- order by DQ_Measure

-- COMMAND ----------

