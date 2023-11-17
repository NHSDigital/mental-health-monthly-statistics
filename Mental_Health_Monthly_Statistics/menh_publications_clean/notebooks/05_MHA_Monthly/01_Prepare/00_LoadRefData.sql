-- Databricks notebook source
-- DBTITLE 1,Breakdown Values
TRUNCATE TABLE $db_output.mha_breakdown_values;
INSERT INTO $db_output.mha_breakdown_values VALUES
  ('England'),
  ('CCG - GP Practice or Residence'),
  ('Provider');


-- COMMAND ----------

TRUNCATE TABLE $db_output.mha_level_values_1;
INSERT INTO $db_output.mha_level_values_1
SELECT 
  'England' as primary_level, 
  'England' as primary_level_desc,
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'England' as breakdown
UNION ALL
SELECT DISTINCT
  ORG_CODE as primary_level, 
  NAME as primary_level_desc, 
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
UNION ALL
SELECT DISTINCT
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'CCG - GP Practice or Residence' as breakdown 
FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate




-- COMMAND ----------

TRUNCATE TABLE $db_output.mha_metric_values;
INSERT INTO $db_output.mha_metric_values VALUES 
('MHS81', 'Detentions in the reporting period'),
('MHS82', 'Short Term Orders in the reporting period'),
('MHS83', 'Uses of Section 136 in the reporting period'),
('MHS84', 'CTOs in the reporting period')