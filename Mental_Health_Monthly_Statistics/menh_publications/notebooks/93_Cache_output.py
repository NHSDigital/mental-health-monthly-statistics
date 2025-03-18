# Databricks notebook source
 %md

 # Collect and cache newly computed data in "all_products_cached"

# COMMAND ----------

db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source

# COMMAND ----------

 %sql

 -- global_temp.Main_monthly_expanded contains the England and Provider data for 72HOURS

 INSERT INTO $db_output.all_products_cached 
 SELECT 
 '$month_id' AS MONTH_ID,
 STATUS,
 2 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM 
 global_temp.Main_monthly_expanded


# COMMAND ----------

# DBTITLE 1,72HOURS
 %sql
 INSERT INTO $db_output.all_products_cached 
 SELECT 
 '$month_id' AS MONTH_ID,
 STATUS,
 2 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM global_temp.Main_monthly_expanded_72hrs

# COMMAND ----------

# DBTITLE 1,CYP_ED_WaitingTimes
 %sql
 INSERT INTO $db_output.all_products_cached 
 SELECT 
 '$month_id' AS MONTH_ID,
 STATUS,
 4 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM global_temp.cyp_ed_wt_expanded

# COMMAND ----------

# DBTITLE 1,MHA
#%sql
#MHA metrics moved to menh_bbrb
#INSERT INTO $db_output.all_products_cached 
#SELECT 
#'$month_id' AS MONTH_ID,
#STATUS,
#5 AS PRODUCT_NO, 
#REPORTING_PERIOD_START, 
#REPORTING_PERIOD_END, 
#BREAKDOWN, 
#PRIMARY_LEVEL, 
#PRIMARY_LEVEL_DESCRIPTION, 
#SECONDARY_LEVEL, 
#SECONDARY_LEVEL_DESCRIPTION, 
#METRIC, 
#METRIC_NAME, 
#METRIC_VALUE,
#SOURCE_DB
#FROM global_temp.mha_monthly_expanded

# COMMAND ----------

# DBTITLE 1,OUTCOME Measures - monthly
 %sql
 INSERT INTO $db_output.all_products_cached 
 SELECT 
 '$month_id' AS MONTH_ID,
 STATUS,
 6 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM global_temp.peri_monthly_expanded

# COMMAND ----------

# DBTITLE 1,OUTCOME Measures - rolling
 %sql
 INSERT INTO $db_output.all_products_cached 
 SELECT 
 '$month_id' AS MONTH_ID,
 STATUS,
 6 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM global_temp.peri_rolling_expanded

# COMMAND ----------

# DBTITLE 1,OUTCOME Measures - CCG & CCG;Provider
 %sql
 INSERT INTO $db_output.all_products_cached 
 SELECT 
 '$month_id' AS MONTH_ID,
 STATUS,
 6 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM global_temp.peri_monthly_expanded_ccg

# COMMAND ----------

# DBTITLE 1,Cache EIP - 3 monthly figures
 %sql

 INSERT INTO $db_output.all_products_cached
 SELECT
  '$month_id' AS MONTH_ID,
   STATUS,
   7 AS PRODUCT_NO,
   REPORTING_PERIOD_START,
   REPORTING_PERIOD_END,
   BREAKDOWN,
   PRIMARY_LEVEL,
   PRIMARY_LEVEL_DESCRIPTION,
   SECONDARY_LEVEL,
   SECONDARY_LEVEL_DESCRIPTION,
   METRIC,
   METRIC_NAME,
   METRIC_VALUE,
   SOURCE_DB
 FROM global_temp.EIP_expanded

# COMMAND ----------

# DBTITLE 1,EIP - Monthly Measures (EIP68,69a,69b)
 %sql
 INSERT INTO $db_output.all_products_cached
 SELECT
  '$month_id' AS MONTH_ID,
   STATUS,
   7 AS PRODUCT_NO,
   REPORTING_PERIOD_START,
   REPORTING_PERIOD_END,
   BREAKDOWN,
   PRIMARY_LEVEL,
   PRIMARY_LEVEL_DESCRIPTION,
   SECONDARY_LEVEL,
   SECONDARY_LEVEL_DESCRIPTION,
   METRIC,
   METRIC_NAME,
   METRIC_VALUE,
   SOURCE_DB
 FROM global_temp.EIP_expanded_Monthly



# COMMAND ----------

# DBTITLE 1,ASCOF - changed to accommodate a THIRD_LEVEL
 %sql

 INSERT INTO $db_output.third_level_products_cached 
 SELECT 
   '$month_id' AS MONTH_ID,
   STATUS,
   8 AS PRODUCT_NO,
   REPORTING_PERIOD_START,
   REPORTING_PERIOD_END,
   BREAKDOWN,
   PRIMARY_LEVEL,
   PRIMARY_LEVEL_DESCRIPTION,
   SECONDARY_LEVEL,
   SECONDARY_LEVEL_DESCRIPTION,
   THIRD_LEVEL,
   METRIC,
   METRIC_NAME,
   METRIC_VALUE,
   SOURCE_DB
 FROM global_temp.ascof_expanded