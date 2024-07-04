# Databricks notebook source
# DBTITLE 1,Create required widgets
 %sql
 CREATE WIDGET TEXT db_output DEFAULT "menh_dq";
 CREATE WIDGET TEXT MonthPeriod DEFAULT "Aug-19";
 CREATE WIDGET TEXT status DEFAULT "Performance";
 CREATE WIDGET TEXT month_id DEFAULT "1433";

# COMMAND ----------

# DBTITLE 1,Create vodim csvs unrounded
 %sql
 --with vodim_dataset as
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
     --CASE
       --WHEN numerators.Value >= 5 AND numerators.OrgIDProv != '-1' THEN -- Provider totals (WARNING: OrgIdProv must be a string, ie '-1')
         --CAST(ROUND(numerators.Value * 2, -1) / 2 AS INT) -- DO round to nearest 5
       --WHEN numerators.Value >= 0 and numerators.OrgIDProv = '-1' THEN -- England totals (WARNING: OrgIdProv must be a string, ie '-1')
         numerators.Value -- Do NOT round
       --ELSE
         --NULL -- Surpress value because less than 5
     --END 
     AS Numerator,
     denominators.Value AS Denominator,
     --(--CASE
      -- WHEN numerators.Value >= 5 AND numerators.OrgIDProv != '-1'  THEN
      --   CAST(ROUND(CAST(numerators.Value AS FLOAT) / CAST(denominators.Value AS FLOAT) * 100, 0) AS INT) -- Round to int
      --   WHEN numerators.Value >0 AND numerators.OrgIDProv = '-1'  THEN
         --CAST(ROUND(CAST(numerators.Value AS FLOAT) / CAST(denominators.Value AS FLOAT) * 100, 0) AS INT) -- Round to int
         (CAST(numerators.Value AS FLOAT) / CAST(denominators.Value AS FLOAT)) * 100--, 0) -- Round to int
      -- ELSE
      --   NULL -- Surpress value because less than 5 count
     --END)
     AS Percentage
   FROM $db_output.dq_vw_inventory_rollup numerators
   INNER JOIN $db_output.dq_vw_inventory_rollup denominators ON denominators.MeasureTypeName = 'Denominator'
                                                         AND numerators.OrgIDProv = denominators.OrgIDProv
                                                         AND numerators.DimensionTypeId = denominators.DimensionTypeId
                                                         AND numerators.MeasureId = denominators.MeasureId
                                                         AND numerators.UniqMonthID = denominators.UniqMonthID
 ),
 dq_vw_vodim_report_unrounded as
 (SELECT DISTINCT
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
   dar.Percentage
 FROM DivideAndRound dar
 INNER JOIN $db_output.dq_vw_inventory_metadata md ON dar.DimensionTypeId = md.DimensionTypeId
                                               AND dar.MeasureId = md.MeasureId
                                               AND dar.MetricTypeId = md.MetricTypeId)
 ,vodim_dataset as
 --select * from dq_vw_vodim_report_unrounded
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
  from dq_vw_vodim_report_unrounded
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
  from dq_vw_vodim_report_unrounded
  where UniqMonthID = $month_id)
 )
 
 select
     '$month_id' Month_Id,
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
     cast(IFNULL(Metric_Value,'*') as int) as Value
 FROM vodim_dataset