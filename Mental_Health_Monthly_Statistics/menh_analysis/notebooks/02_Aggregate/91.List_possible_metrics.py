# Databricks notebook source
 %md
 
 # List possible metrics for each product
 
 Create full list of providers and CCGs, and fill in unused metrics/breakdown/clusters.

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,1. List possible Main monthly metrics
 %sql
 --reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 
 -- status = dbutils.widgets.get("status")
 -- db_output = dbutils.widgets.get("db_output")
 -- month_id = dbutils.widgets.get("month_id")
 
 CREATE OR REPLACE GLOBAL TEMP VIEW Main_monthly_possible_metrics AS 
 SELECT b.breakdown,
 l.primary_level,
 l.primary_level_desc,
 l.secondary_level,
 l.secondary_level_desc,
 m.metric,
 m.metric_name 
 FROM $db_output.Main_monthly_breakdown_values as b 
 INNER JOIN $db_output.Main_monthly_level_values_1 as l 
 ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.Main_monthly_metric_values as m

# COMMAND ----------

# DBTITLE 1,1.1 List possible MHS26 in Main monthly (Delayed Discharge) metrics
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW DD_possible_metrics AS 
 SELECT b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name 
 FROM $db_output.DD_breakdown_values as b 
 INNER JOIN $db_output.DD_level_values as l 
 ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.DD_metric_values as m;

# COMMAND ----------

# DBTITLE 1,2. List possible AWT metrics - code excluded to prevent 0/* outputs in Rounded outputs
 %sql
 
 -- CREATE OR REPLACE GLOBAL TEMP VIEW AWT_possible_metrics AS
 
 -- SELECT      DISTINCT b.breakdown, l.level, l.level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 -- FROM        $db_output.AWT_breakdown_values as b
 -- INNER JOIN  $db_output.AWT_level_values as l ON b.breakdown=l.breakdown
 -- CROSS JOIN  $db_output.AWT_metric_values as m
 -- WHERE b.breakdown LIKE '%Ethnicity'
 --       AND m.metric IN ('EIP01', 'EIP23a', 'EIP23b', 'EIP23c', 'EIP23d', 'EIP23e', 'EIP23f', 'EIP23i', 'EIP23j', 'EIP63', 'EIP64', 'EIP65', 'EIP66', 'EIP67')
 
 -- UNION ALL
 
 -- SELECT      DISTINCT b.breakdown, l.level, l.level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 -- FROM        $db_output.AWT_breakdown_values as b
 -- INNER JOIN  $db_output.AWT_level_values as l ON b.breakdown=l.breakdown
 -- CROSS JOIN  $db_output.AWT_metric_values as m
 -- WHERE b.breakdown NOT LIKE '%Ethnicity'
       

# COMMAND ----------

# DBTITLE 1,3. List possible CYP 2nd contact metrics
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CYP_2nd_contact_possible_metrics AS
 SELECT      b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.CYP_2nd_contact_breakdown_values as b
 INNER JOIN  $db_output.CYP_2nd_contact_level_values as l ON b.breakdown = l.breakdown
 CROSS JOIN  $db_output.CYP_2nd_contact_metric_values as m;

# COMMAND ----------

# DBTITLE 1,4. List possible CAP metrics
 %sql
 --reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CaP_possible_metrics AS 
 SELECT b.breakdown, l.level, l.level_desc, c.cluster, m.metric, m.metric_name
 FROM $db_output.CaP_breakdown_values as b 
 INNER JOIN $db_output.CaP_level_values as l 
 ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.CaP_cluster_values as c 
 CROSS JOIN $db_output.CaP_metric_values as m

# COMMAND ----------

# DBTITLE 1,5. List possible CYP monthly metrics
 %sql
 --reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CYP_monthly_possible_metrics AS 
 SELECT distinct b.breakdown,l.primary_level,l.primary_level_desc,l.secondary_level,l.secondary_level_desc,m.metric,m.metric_name 
 FROM $db_output.CYP_monthly_breakdown_values as b 
 INNER JOIN $db_output.CYP_monthly_level_values as l 
 ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.CYP_monthly_metric_values as m

# COMMAND ----------

# DBTITLE 1,7. List  possible ASCOF metrics
 %sql
 --reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 CREATE OR REPLACE GLOBAL TEMP VIEW Ascof_possible_metrics AS 
 SELECT b.breakdown, l.level, l.level_desc, m.metric, m.metric_name 
 FROM $db_output.Ascof_breakdown_values as b 
 INNER JOIN $db_output.Ascof_level_values as l ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.Ascof_metric_values as m

# COMMAND ----------

# DBTITLE 1,8. Make list of possible metrics for FYFV
#only needs to run for quarterly (i.e. when month_id is divisible by 3 with no remainder) and data are Performance OR Final
status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output")
month_id = dbutils.widgets.get("month_id")

is_quarter = int(month_id) % 3 ==0

sql = "CREATE OR REPLACE GLOBAL TEMP VIEW FYFV_possible_metrics AS SELECT b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name FROM {db_output}.FYFV_Dashboard_breakdown_values as b INNER JOIN {db_output}.FYFV_Dashboard_level_values as l ON b.breakdown = l.breakdown CROSS JOIN {db_output}.FYFV_Dashboard_metric_values as m".format(db_output=db_output)

if is_quarter:
  print(is_quarter, status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(is_quarter, status)

# COMMAND ----------

