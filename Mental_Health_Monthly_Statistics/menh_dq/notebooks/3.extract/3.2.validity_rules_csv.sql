-- Databricks notebook source
 %sql
 DELETE FROM $db_output.validity_rules;
 INSERT INTO $db_output.validity_rules
 SELECT 
   MeasureNumber,
   MeasureName,
   MeasureDescription,
   DataItem,
   Denominator,
   CASE
     WHEN Valid IS NULL THEN 'N/A'
     ELSE Valid
   END AS Valid,
   CASE
     WHEN Other IS NULL THEN 'N/A'
     ELSE Other
   END AS Other,
   CASE
     WHEN Default IS NULL THEN 'N/A'
     ELSE Default
   END AS Default,
   CASE
     WHEN Invalid IS NULL THEN 'N/A'
     ELSE Invalid
   END AS Invalid,
   CASE
     WHEN Missing IS NULL THEN 'N/A'
     ELSE Missing
   END AS Missing
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
   WHERE DimensionTypeName = 'Validity'
   AND MeasureTypeName IN ('Denominator', 'Numerator')
   AND MetricTypeName IN ('Denominator', 'Valid', 'Other', 'Default', 'Invalid', 'Missing')
 ) PIVOT (
   MAX(MeasureTypeDescription)
   FOR MetricTypeName IN ('Denominator', 'Valid', 'Other', 'Default', 'Invalid', 'Missing')  
 )