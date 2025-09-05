-- Databricks notebook source
 %py
 db_output=dbutils.widgets.get("db_output")
 print(db_output)
 assert db_output
 provider_table=dbutils.widgets.get("provider_table")
 print(provider_table)
 assert provider_table

-- COMMAND ----------

-- DBTITLE 1,Break down Values
TRUNCATE TABLE $db_output.cyp_ed_wt_breakdown_values;
INSERT INTO $db_output.cyp_ed_wt_breakdown_values VALUES
  ('England'),
  ('CCG - GP Practice or Residence'),
  ('Provider'),
  ('STP - GP Practice or Residence'),
  ('Commissioning Region');


-- COMMAND ----------

TRUNCATE TABLE $db_output.cyp_ed_wt_level_values_1;

INSERT INTO $db_output.cyp_ed_wt_level_values_1
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
 FROM $provider_table
UNION ALL
SELECT DISTINCT
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'CCG - GP Practice or Residence' as breakdown 
FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
UNION ALL
SELECT DISTINCT 
 STP_CODE as primary_level, 
 COALESCE(STP_DESCRIPTION, "UNKNOWN") as primary_level_desc,
 'NONE' as secondary_level,
 'NONE' as secondary_level_desc,
 'STP - GP Practice or Residence' as breakdown 
FROM $db_output.STP_Region_mapping_post_2020
UNION ALL
SELECT DISTINCT 
 Region_code as primary_level, 
 COALESCE(Region_description, "UNKNOWN") as primary_level_desc,
 'NONE' as secondary_level,
 'NONE' as secondary_level_desc,
 'Commissioning Region' as breakdown 
FROM $db_output.STP_Region_mapping_post_2020

-- COMMAND ----------

TRUNCATE TABLE $db_output.cyp_ed_wt_metric_values;
INSERT INTO $db_output.cyp_ed_wt_metric_values VALUES 
('ED85', 'Referrals with eating disorder issues entering treatment in RP, aged 0-17'),
('ED86', 'Referrals with eating disorder issues categorised as urgent entering treatment in RP, aged 0-17'),
('ED86a', 'Referrals with eating disorder issues categorised as urgent entering treatment within one week, in RP, aged 0-17'),
('ED86b', 'Referrals with eating disorder issues categorised as urgent entering treatment within 1-4 weeks, in RP, aged 0-17'),
('ED86c', 'Referrals with eating disorder issues categorised as urgent entering treatment within 4-12 weeks, in RP, aged 0-17'),
('ED86d', 'Referrals with eating disorder issues categorised as urgent entering treatment after 12 weeks, in RP, aged 0-17'),
('ED86e', 'Proportion of referrals with eating disorders categorized as urgent cases entering treatment within one week in RP, aged 0-17'),
('ED86f', 'Proportion of referrals with eating disorder issues categorised as urgent entering treatment within 1-4 weeks, in RP, aged 0-17'),
('ED86g', 'Proportion of referrals  with eating disorder issues categorised as urgent entering treatment within 4-12 weeks, in RP, aged 0-17'),
('ED86h', 'Proportion of referrals  with eating disorder issues categorised as urgent entering treatment after 12 weeks, in RP, aged 0-17'),
('ED87', 'Referrals eating disorder issues categorised as routine entering treatment in RP, aged 0-17'),
('ED87a', 'Referrals with eating disorder issues categorised as routine entering treatment within one week, in RP, aged 0-17'),
('ED87b', 'Referrals with eating disorder issues categorised as routine entering treatment within 1-4 weeks, in RP, aged 0-17'),
('ED87c', 'Referrals with eating disorder issues categorised as routine entering treatment within 4-12 weeks, in RP, aged 0-17'),
('ED87d', 'Referrals with eating disorder issues categorised as routine entering treatment after 12 weeks, in RP, aged 0-17'),
('ED87e', 'Proportion of referrals with eating disorders categorized as routine cases entering treatment within four weeks in RP, aged 0-17'),
('ED87f', 'Proportion of referrals with eating disorder issues categorised as routine entering treatment within one week, in RP, aged 0-17'),
('ED87g', 'Proportion of referrals with eating disorder issues categorised as routine entering treatment within 1-4 weeks, in RP, aged 0-17'),
('ED87h', 'Proportion of referrals with eating disorder issues categorised as routine entering treatment within 4-12 weeks, in RP, aged 0-17'),
('ED87i', 'Proportion of referrals with eating disorder issues categorised as routine entering treatment after 12 weeks, in RP, aged 0-17'),
('ED87j', 'Referrals with eating disorder issues categorised as routine entering treatment within 4 weeks, in RP, aged 0-17'),
('ED88', 'Referrals with eating disorder issues waiting for treatment at end of RP, aged 0-17'),
('ED89', 'Referrals with eating disorder issues categorized as urgent waiting for treatment end RP, aged 0-17'),
('ED89a', 'Referrals with eating disorder issues categorized as urgent waiting for treatment for one week, end RP, aged 0-17'),
('ED89b', 'Referrals with eating disorder issues categorized as urgent waiting for treatment for 1-4 weeks, end RP, aged 0-17'),
('ED89c', 'Referrals with eating disorder issues categorized as urgent waiting for treatment for 4-12 weeks, end RP, aged 0-17'),
('ED89d', 'Referrals with eating disorder issues categorized as urgent waiting for treatment for more than 12 weeks, end RP, aged 0-17'),
('ED89e', 'Proportion of referrals with eating disorder issues categorized as urgent waiting for treatment for one week, end RP, aged 0-17'),
('ED89f', 'Proportion of referrals with eating disorder issues categorized as urgent waiting for treatment for 1-4 weeks, end RP, aged 0-17'),
('ED89g', 'Proportion of referrals with eating disorder issues categorized as urgent waiting for treatment for 4-12 weeks, end RP, aged 0-17'),
('ED89h', 'Proportion of referrals with eating disorder issues categorized as urgent waiting for treatment for more than 12 weeks, end RP, aged 0-17'),
('ED90', 'Referrals with eating disorder issues categorized as routine waiting for treatment end RP, aged 0-17'),
('ED90a', 'Referrals with eating disorder issues categorized as routine waiting for treatment for one week, end RP, aged 0-17'),
('ED90b', 'Referrals with eating disorder issues categorized as routine waiting for treatment for 1-4 weeks, end RP, aged 0-17'),
('ED90c', 'Referrals with eating disorder issues categorized as routine waiting for treatment for 4-12 weeks, end RP, aged 0-17'),
('ED90d', 'Referrals with eating disorder issues categorized as routine waiting for treatment for more than 12 weeks, end RP, aged 0-17'),
('ED90e', 'Proportion of referrals with eating disorder issues categorized as routine waiting for treatment for one week, end RP, aged 0-17'),
('ED90f', 'Proportion of referrals with eating disorder issues categorized as routine waiting for treatment for 1-4 weeks, end RP, aged 0-17'),
('ED90g', 'Proportion of referrals with eating disorder issues categorized as routine waiting for treatment for 4-12 weeks, end RP, aged 0-17'),
('ED90h', 'Proportion of referrals with eating disorder issues categorized as routine waiting for treatment for more than 12 weeks, end RP, aged 0-17'),
('ED91', 'Median waiting time between referral and first contact for referrals that had their first contact in the RP, with eating disorder issues categorized as routine, aged 0-17'),
('ED92', 'Median waiting time between referral and first contact for referrals that had their first contact in the RP, with eating disorder issues categorized as urgent, aged 0-17'),
('ED93', '90th percentile waiting time between referral and first contact for referrals that had their first contact in the RP, with eating disorder issues categorized as routine, aged 0-17'),
('ED94', '90th percentile waiting time between referral and first contact for referrals that had their first contact in the RP, with eating disorder issues categorized as urgent, aged 0-17'),
('ED95', 'Median waiting time for referrals still waiting for treatment, with eating disorder issues categorized as routine, aged 0-17'),
('ED96', 'Median waiting time referrals still waiting for treatment, with eating disorder issues categorized as urgent, aged 0-17'),
('ED97', '90th percentile waiting time for referrals still waiting for treatment, with eating disorder issues categorized as routine, aged 0-17'),
('ED98', '90th percentile waiting time referrals still waiting for treatment, with eating disorder issues categorized as urgent, aged 0-17'),
('ED99', 'Number of referrals recieving a second contact in the RP, with eating disorder issues categorized as routine, aged 0-17'),
('ED100', 'Number of referrals recieving a second contact in the RP, with eating disorder issues categorized as urgent, aged 0-17'),
('ED101', 'Median waiting time from first to second contact for referrals receiving a second contact in the RP, with eating disorder issues categorized as routine, aged 0-17'),
('ED102', 'Median waiting time from first to second contact for referrals receiving a second contact in the RP, with eating disorder issues categorized as urgent, aged 0-17'),
('ED103', '90th percentile waiting time from first to second contact for referrals receiving a second contact in the RP, with eating disorder issues categorized as routine, aged 0-17'),
('ED104', '90th percentile waiting time from first to second contact for referrals receiving a second contact in the RP, with eating disorder issues categorized as urgent, aged 0-17'),
('ED105', 'People with eating disorder issues still waiting for treatment at the end of the RP that turned 18'),
('ED106', 'Median waiting time of referrals with eating disorder issues still waiting for treatment at the end of the RP that turned 18'),
('ED107', '90th percentile waiting time of referrals with eating disorder issues still waiting for treatment at the end of the RP that turned 18')