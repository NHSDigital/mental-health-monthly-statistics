# Databricks notebook source
 %md
 
 # Collect and cache newly computed data in "all_products_cached"

# COMMAND ----------

# DBTITLE 1,1. Cache Main Monthly
 %sql
 --GBT reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 INSERT INTO $db_output.all_products_cached 
 SELECT 1 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END, 
 STATUS, 
 BREAKDOWN, 
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE,
 SOURCE_DB
 FROM global_temp.Main_monthly_expanded

# COMMAND ----------

# DBTITLE 1,1.1 Cache Delayed Discharge (MHA26)
 %sql
 
 
 INSERT INTO $db_output.all_products_cached 
 SELECT 1 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END,
 STATUS, 
 BREAKDOWN, 
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE, 
 SOURCE_DB 
 FROM global_temp.DD_expanded

# COMMAND ----------

# DBTITLE 1,2.Cache AWT - code excluded to prevent 0/* outputs in Rounded outputs
 %sql
 
 -- INSERT INTO $db_output.all_products_cached
 -- SELECT
 --   2 AS PRODUCT_NO,
 --   REPORTING_PERIOD_START,
 --   REPORTING_PERIOD_END,
 --   STATUS,
 --   BREAKDOWN,
 --   PRIMARY_LEVEL,
 --   PRIMARY_LEVEL_DESCRIPTION,
 --   SECONDARY_LEVEL,
 --   SECONDARY_LEVEL_DESCRIPTION,
 --   METRIC,
 --   METRIC_NAME,
 --   METRIC_VALUE,
 --   SOURCE_DB
 --   --CURRENT_TIMESTAMP()
 -- FROM global_temp.AWT_expanded

# COMMAND ----------

# DBTITLE 1,3.Cache CYP 2nd Contact
 %sql
 
 INSERT INTO $db_output.all_products_cached
 SELECT
   3 AS PRODUCT_NO,
   REPORTING_PERIOD_START,
   REPORTING_PERIOD_END,
   STATUS,
   BREAKDOWN,
   PRIMARY_LEVEL,
   PRIMARY_LEVEL_DESCRIPTION,
   SECONDARY_LEVEL,
   SECONDARY_LEVEL_DESCRIPTION,
   METRIC,
   METRIC_NAME,
   METRIC_VALUE,
   SOURCE_DB
   --CURRENT_TIMESTAMP()
 FROM global_temp.CYP_2nd_contact_expanded

# COMMAND ----------

# DBTITLE 1,4.Cache CAP
 %sql
 --GBT reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 INSERT INTO $db_output.all_products_cached 
 SELECT 
 4 AS PRODUCT_NO, 
 REPORTING_PERIOD_START, 
 REPORTING_PERIOD_END,
 STATUS, BREAKDOWN, 
 LEVEL AS PRIMARY_LEVEL, 
 LEVEL_DESCRIPTION AS 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE, 
 SOURCE_DB 
 FROM global_temp.CAP_expanded

# COMMAND ----------

# DBTITLE 1,5.Cache CYP monthly
 %sql
 --GBT reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 INSERT INTO $db_output.all_products_cached 
 SELECT 5 AS PRODUCT_NO, 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS, 
 BREAKDOWN, 
 PRIMARY_LEVEL, 
 PRIMARY_LEVEL_DESCRIPTION, 
 SECONDARY_LEVEL, 
 SECONDARY_LEVEL_DESCRIPTION, 
 METRIC, 
 METRIC_NAME, 
 METRIC_VALUE, 
 SOURCE_DB
 FROM global_temp.CYP_monthly_expanded

# COMMAND ----------

# DBTITLE 1,8.Cache FYFV Quarterly
# only needs to run for quarterly (i.e. when month_id is divisible by 3 with no remainder) and data are final
status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output")
month_id = dbutils.widgets.get("month_id")

is_quarter = int(month_id) % 3 ==0

# original (no SOURCE_DB) code
# sql = "INSERT INTO {db_output}.all_products_cached SELECT 8 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_NAME, METRIC_VALUE FROM global_temp.FYFV_expanded".format(db_output=db_output)

# code below includes SOURCE_DB 
sql = "INSERT INTO {db_output}.all_products_cached SELECT 8 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_NAME, METRIC_VALUE, SOURCE_DB FROM global_temp.FYFV_expanded".format(db_output=db_output)

if is_quarter and status != 'Provisional':
  print(is_quarter, status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(is_quarter, status)

# COMMAND ----------

