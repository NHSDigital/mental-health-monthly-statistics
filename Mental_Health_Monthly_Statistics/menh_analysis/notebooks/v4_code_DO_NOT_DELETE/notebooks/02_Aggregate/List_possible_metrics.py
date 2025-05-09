# Databricks notebook source
 %md

 # List possible metrics for each product

 Create full list of providers and CCGs, and fill in unused metrics/breakdown/clusters.

# COMMAND ----------

# DBTITLE 1,1. List possible Main monthly metrics
 %sql

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

# DBTITLE 1,2. List possible AWT metrics
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW AWT_possible_metrics AS
 -- User note: original code comented out
 -- SELECT      DISTINCT b.breakdown, l.level, l.level_desc, m.metric, m.metric_name
 SELECT      DISTINCT b.breakdown, l.level, l.level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name
 FROM        $db_output.AWT_breakdown_values as b
 INNER JOIN  $db_output.AWT_level_values as l ON b.breakdown=l.breakdown
 CROSS JOIN  $db_output.AWT_metric_values as m;

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

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CYP_monthly_possible_metrics AS 
 SELECT distinct b.breakdown,l.primary_level,l.primary_level_desc,l.secondary_level,l.secondary_level_desc,m.metric,m.metric_name 
 FROM $db_output.CYP_monthly_breakdown_values as b 
 INNER JOIN $db_output.CYP_monthly_level_values as l 
 ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.CYP_monthly_metric_values as m

# COMMAND ----------

# DBTITLE 1,7. List  possible ASCOF metrics
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW Ascof_possible_metrics AS 
 SELECT b.breakdown, l.level, l.level_desc, m.metric, m.metric_name 
 FROM $db_output.Ascof_breakdown_values as b 
 INNER JOIN $db_output.Ascof_level_values as l ON b.breakdown = l.breakdown 
 CROSS JOIN $db_output.Ascof_metric_values as m

# COMMAND ----------

# DBTITLE 1,8. Make list of possible metrics for FYFV
#only needs to run for quarterly (i.e. when month_id is divisible by 3 with no remainder) and data are final
status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output")
month_id = dbutils.widgets.get("month_id")

is_quarter = int(month_id) % 3 ==0

sql = "CREATE OR REPLACE GLOBAL TEMP VIEW FYFV_possible_metrics AS SELECT b.breakdown, l.primary_level, l.primary_level_desc, l.secondary_level, l.secondary_level_desc, m.metric, m.metric_name FROM {db_output}.FYFV_Dashboard_breakdown_values as b INNER JOIN {db_output}.FYFV_Dashboard_level_values as l ON b.breakdown = l.breakdown CROSS JOIN {db_output}.FYFV_Dashboard_metric_values as m".format(db_output=db_output)

if is_quarter and status != 'Provisional':
  print(is_quarter, status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(is_quarter, status)

# COMMAND ----------

