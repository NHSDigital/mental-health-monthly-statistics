# Databricks notebook source
 %md

 # List possible metrics for each product

 Create full list of providers and CCGs, and fill in unused metrics/breakdown/clusters.

# COMMAND ----------

# DBTITLE 1,List possible Main monthly metrics
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Main_monthly_possible_metrics AS 
 SELECT 
 DISTINCT
 b.breakdown,
 l.primary_level,
 l.primary_level_desc,
 l.secondary_level,
 l.secondary_level_desc,
 m.metric,
 m.metric_name 
 FROM $db_output.Main_monthly_breakdown_values as b 
 INNER JOIN $db_output.Main_monthly_level_values_1 as l ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.Main_monthly_metric_values as m


# COMMAND ----------

# DBTITLE 1,List possible metrics for cyp_ed_wt
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW cyp_ed_wt_possible_metrics AS 
 SELECT 
 DISTINCT
 b.breakdown,
 l.primary_level,
 l.primary_level_desc,
 l.secondary_level,
 l.secondary_level_desc,
 m.metric,
 m.metric_name 
 FROM $db_output.cyp_ed_wt_breakdown_values as b 
 INNER JOIN $db_output.cyp_ed_wt_level_values_1 as l ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.cyp_ed_wt_metric_values as m

# COMMAND ----------

# DBTITLE 1,List possible metrics for MHA_Monthly
#%sql
# CREATE OR REPLACE GLOBAL TEMP VIEW mha_possible_metrics AS 
#SELECT 
#DISTINCT
#b.breakdown,
#l.primary_level,
#l.primary_level_desc,
#l.secondary_level,
#l.secondary_level_desc,
#m.metric,
#m.metric_name 
#FROM $db_output.mha_breakdown_values as b 
#INNER JOIN $db_output.mha_level_values_1 as l ON b.breakdown = l.breakdown 
#CROSS JOIN $db_output.mha_metric_values as m
#WHERE 
#(m.Metric in ('MHS81') 
#  AND b.BREAKDOWN IN ('England',
#  'CCG - GP Practice or Residence',
#  'Provider',
#  'England; Age',
#  'CCG - GP Practice or Residence; Age',
#  'Provider; Age',
#  'England; Gender',
#  'CCG - GP Practice or Residence; Gender',
#  'Provider; Gender',
#  'England; Ethnicity',
#  'CCG - GP Practice or Residence; Ethnicity',
#  'Provider; Ethnicity',
#  'CCG - GP Practice or Residence; IMD Decile',
#  'Provider; IMD Decile',
#  'England; IMD Decile'
 # ))
#OR 
#(m.Metric in ('MHS84','MHS143','MHS144') 
#  AND b.BREAKDOWN IN ('England',
#  'CCG - GP Practice or Residence',
#  'Provider',
#  'England; Age',
#  'England; Gender',
#  'England; Ethnicity',
#  'England; IMD Decile'
#  ))
#OR 
#(m.Metric in ('MHS84a') 
#  AND b.BREAKDOWN IN ('England',
#  'England; Age',
#  'England; Gender',
#  'England; Ethnicity',
#  'England; IMD Decile'
#  ))
#OR
#(m.Metric in ('MHS82', 'MHS83') 
#  AND b.BREAKDOWN IN ('England',
# 'CCG - GP Practice or Residence',
#  'Provider'))
#OR
#(m.Metric in ('MHS81a') 
#  AND b.BREAKDOWN IN ('England',
#  'CCG - GP Practice or Residence',
#  'England; Age',
#  'CCG - GP Practice or Residence; Age',
#  'England; Gender',
#  'CCG - GP Practice or Residence; Gender',
#  'England; Ethnicity',
#  'CCG - GP Practice or Residence; Ethnicity',
#  'England; IMD Decile',
#  'CCG - GP Practice or Residence; IMD Decile'
#  ))
#OR 
#(m.Metric in ('MHS145') 
#  AND b.BREAKDOWN IN ('England')) 

# COMMAND ----------

# DBTITLE 1,Perinatal - CYP Outcome Measures
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW peri_possible_metrics AS

 SELECT      DISTINCT b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.peri_breakdown_values as b
 INNER JOIN  $db_output.peri_level_values as l ON b.breakdown=l.breakdown
 CROSS JOIN  $db_output.peri_metric_values as m
 WHERE b.breakdown IN  ('England', 'Provider')
      
 UNION ALL

 SELECT      DISTINCT b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.peri_breakdown_values as b
 INNER JOIN  $db_output.peri_level_values as l ON b.breakdown=l.breakdown
 CROSS JOIN  $db_output.peri_metric_values as m
 WHERE b.breakdown IN  ('STP', 'Region')
  AND m.metric IN ('MHS91', 'MHS95')
      
 -- Note: At present 'CCG of Residence; Provider' and 'CCG of Residence' are dealt with in 92_Expand_output - only submitted data are output for these breakdowns, no NULL/expected values.

       

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,EIP/AWT
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW EIP_possible_metrics AS

 SELECT DISTINCT breakdown, level, level_desc, secondary_level, secondary_level_desc, metric, metric_name FROM
 (
 SELECT      b.breakdown, l.level, l.level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.AWT_breakdown_values as b
 INNER JOIN  $db_output.AWT_level_values as l ON b.breakdown=l.breakdown and b.breakdown IN ('Provider','England','CCG - GP Practice or Residence')
 CROSS JOIN  $db_output.AWT_metric_values as m
 WHERE m.metric IN 
   (
     'EIP01a','EIP01b','EIP01c','EIP23aa','EIP23ab','EIP23ac','EIP23ba','EIP23bb','EIP23bc','EIP23ca','EIP23cb','EIP23cc','EIP23da','EIP23db','EIP23dc',
     'EIP23ea','EIP23eb','EIP23ec','EIP23fa','EIP23fb','EIP23fc','EIP23g','EIP23h','EIP23ia','EIP23ib','EIP23ic','EIP23ja','EIP23jb','EIP23jc',	'EIP32',
     'EIP63a','EIP63b','EIP63c','EIP64a','EIP64b','EIP64c','EIP65a',	'EIP65b','EIP65c','EIP66a','EIP66b','EIP66c','EIP67a','EIP67b',	'EIP67c','MHS32','ED32'
     )

 UNION ALL


 SELECT      b.breakdown, l.level, l.level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.AWT_breakdown_values as b
 INNER JOIN  $db_output.AWT_level_values as l ON b.breakdown=l.breakdown and b.breakdown IN ('Provider','England','CCG - GP Practice or Residence','England; Ethnicity','CCG - GP Practice or Residence; Ethnicity','Provider; Ethnicity')
 CROSS JOIN  $db_output.AWT_metric_values as m
 WHERE m.metric IN ('EIP01','EIP23a','EIP23b','EIP23c','EIP23d','EIP23e','EIP23f','EIP23i','EIP23j','EIP63','EIP64','EIP65','EIP66','EIP67')

 UNION ALL

 SELECT      b.breakdown, l.level, l.level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.AWT_breakdown_values as b
 INNER JOIN  $db_output.AWT_level_values as l ON b.breakdown=l.breakdown and b.breakdown IN ('Provider','England','CCG - GP Practice or Residence','STP - GP Practice or Residence','Commissioning Region')
 CROSS JOIN  $db_output.AWT_metric_values as m
 WHERE m.metric IN ('EIP68', 'EIP69a', 'EIP69b')
 ) x;

# COMMAND ----------

# DBTITLE 1,ASCOF
 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW ascof_possible_metrics AS 

 SELECT b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, l.third_level, m.metric, m.metric_name 
 FROM $db_output.ascof_breakdown_values as b 
 INNER JOIN $db_output.ascof_level_values as l ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.ascof_metric_values as m
