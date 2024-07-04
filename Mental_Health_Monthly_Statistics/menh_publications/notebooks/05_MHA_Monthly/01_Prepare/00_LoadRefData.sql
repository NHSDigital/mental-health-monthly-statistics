-- Databricks notebook source
-- DBTITLE 1,Breakdown Values
TRUNCATE TABLE $db_output.mha_breakdown_values;
INSERT INTO $db_output.mha_breakdown_values VALUES
  ('England'),
  ('CCG - GP Practice or Residence'),
  ('Provider'),

  ('England; Age'),
  ('CCG - GP Practice or Residence; Age'),
  ('Provider; Age'),
  
  ('England; Gender'),
  ('CCG - GP Practice or Residence; Gender'),
  ('Provider; Gender'),
  
  ('England; Ethnicity'),
  ('CCG - GP Practice or Residence; Ethnicity'),
  ('Provider; Ethnicity'),
  
  ('England; IMD Decile'),
  ('CCG - GP Practice or Residence; IMD Decile'),
  ('Provider; IMD Decile');

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

UNION ALL

--Age
SELECT 
  'England' as primary_level, 
  'England' as primary_level_desc,
  AgeBand as secondary_level,
  AgeBand as secondary_level_desc,
  'England; Age' as breakdown
FROM $db_output.Ref_AgeBand
UNION ALL
SELECT DISTINCT
  ORG_CODE as primary_level, 
  NAME as primary_level_desc, 
  AgeBand as secondary_level,
  AgeBand as secondary_level_desc,
  'Provider; Age' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 CROSS JOIN $db_output.Ref_AgeBand
UNION ALL
SELECT DISTINCT
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  AgeBand as secondary_level,
  AgeBand as secondary_level_desc,
  'CCG - GP Practice or Residence; Age' as breakdown 
FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
CROSS JOIN $db_output.Ref_AgeBand

UNION ALL 

--Ethnicity
SELECT 
  'England' as primary_level, 
  'England' as primary_level_desc,
  ETHNICGROUP as secondary_level,
  ETHNICGROUP as secondary_level_desc,
  'England; Ethnicity' as breakdown
FROM $db_output.Ref_EthnicGroup
UNION ALL
SELECT DISTINCT
  ORG_CODE as primary_level, 
  NAME as primary_level_desc, 
  ETHNICGROUP as secondary_level,
  ETHNICGROUP as secondary_level_desc,
  'Provider; Ethnicity' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 CROSS JOIN $db_output.Ref_EthnicGroup
UNION ALL
SELECT DISTINCT
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  ETHNICGROUP as secondary_level,
  ETHNICGROUP as secondary_level_desc,
  'CCG - GP Practice or Residence; Ethnicity' as breakdown 
FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
CROSS JOIN $db_output.Ref_EthnicGroup

UNION ALL 

--Gender
SELECT 
  'England' as primary_level, 
  'England' as primary_level_desc,
  PRIMARYCODE as secondary_level,
  DESCRIPTION as secondary_level_desc,
  'England; Gender' as breakdown
FROM $db_output.Ref_GenderCodes
UNION ALL
SELECT DISTINCT
  ORG_CODE as primary_level, 
  NAME as primary_level_desc, 
  PRIMARYCODE as secondary_level,
  DESCRIPTION as secondary_level_desc,
  'Provider; Gender' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 CROSS JOIN $db_output.Ref_GenderCodes
UNION ALL
SELECT DISTINCT
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  PRIMARYCODE as secondary_level,
  DESCRIPTION as secondary_level_desc,
  'CCG - GP Practice or Residence; Gender' as breakdown 
FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
CROSS JOIN $db_output.Ref_GenderCodes

UNION ALL

--IMD Decile
SELECT 
  'England' as primary_level, 
  'England' as primary_level_desc,
  IMDDECILE as secondary_level,
  IMDDECILE as secondary_level_desc,
  'England; IMD Decile' as breakdown
FROM $db_output.Ref_IMD_Decile
UNION ALL
SELECT DISTINCT
  ORG_CODE as primary_level, 
  NAME as primary_level_desc, 
  IMDDECILE as secondary_level,
  IMDDECILE as secondary_level_desc,
  'Provider; IMD Decile' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 CROSS JOIN $db_output.Ref_IMD_Decile
UNION ALL
SELECT DISTINCT
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  IMDDECILE as secondary_level,
  IMDDECILE as secondary_level_desc,
  'CCG - GP Practice or Residence; IMD Decile' as breakdown 
FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
CROSS JOIN $db_output.Ref_IMD_Decile

-- COMMAND ----------

TRUNCATE TABLE $db_output.mha_metric_values;
INSERT INTO $db_output.mha_metric_values VALUES 
('MHS81', 'Detentions in the reporting period'),
('MHS81a', 'Detentions in the reporting period per 100,000 of the population'),
('MHS82', 'Short Term Orders in the reporting period'),
('MHS83', 'Uses of Section 136 in the reporting period'),
('MHS84', 'CTOs in the reporting period'),
('MHS84a', 'CTOs in the reporting period per 100,000 of the population'),
('MHS143', 'Uses of Section 2 in the reporting period'),
('MHS144', 'Uses of Section 3 in the reporting period'),
('MHS145', 'Uses of Part 3 of the Act in the reporting period')