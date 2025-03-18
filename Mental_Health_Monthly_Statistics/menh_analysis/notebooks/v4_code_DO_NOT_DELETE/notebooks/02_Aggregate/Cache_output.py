# Databricks notebook source
 %md

 # Collect and cache newly computed data in "all_products_cached"

# COMMAND ----------

# DBTITLE 1,1. Cache Main Monthly
# only needs to run if data are final
# amended to run when provisional too

status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output")
month_id = dbutils.widgets.get("month_id")



sql = "INSERT INTO {db_output}.all_products_cached SELECT 1 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_NAME, METRIC_VALUE, SOURCE_DB FROM global_temp.Main_monthly_expanded".format(db_output=db_output)

if status == 'Final':
  print(status)
  print(sql)
  spark.sql(sql)
else:
  print(status)
  # amended to run when provisional too
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,2.Cache AWT
 %sql

 INSERT INTO $db_output.all_products_cached
 SELECT
   2 AS PRODUCT_NO,
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
 FROM global_temp.AWT_expanded

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
# only needs to run if data are final
# amended to run when provisional too

sql = "INSERT INTO {db_output}.all_products_cached SELECT 4 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, LEVEL AS PRIMARY_LEVEL, LEVEL_DESCRIPTION AS PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_NAME, METRIC_VALUE, SOURCE_DB FROM global_temp.CAP_expanded".format(db_output=db_output)

if status == 'Final':
  print(status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(status)
  # amended to run when provisional too
  print(sql)
  spark.sql(sql).collect()

# COMMAND ----------

# DBTITLE 1,5.Cache CYP monthly
# only needs to run if data are final
# amended to run when provisional too

sql = "INSERT INTO {db_output}.all_products_cached SELECT 5 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_NAME, METRIC_VALUE, SOURCE_DB FROM global_temp.CYP_monthly_expanded".format(db_output=db_output)

if status == 'Final':
  print(status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(status)
  # amended to run when provisional too
  print(sql)
  spark.sql(sql).collect()

# COMMAND ----------

# DBTITLE 1,8.Cache FYFV Quarterly
# only needs to run for quarterly (i.e. when month_id is divisible by 3 with no remainder) and data are final
is_quarter = int(month_id) % 3 ==0

# code below includes SOURCE_DB 
sql = "INSERT INTO {db_output}.all_products_cached SELECT 8 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_NAME, METRIC_VALUE, SOURCE_DB FROM global_temp.FYFV_expanded".format(db_output=db_output)

if is_quarter and status != 'Provisional':
  print(is_quarter, status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(is_quarter, status)

# COMMAND ----------

# DBTITLE 1,9. CCGOIS - quarterly - doesn't need to be cached for rounding
