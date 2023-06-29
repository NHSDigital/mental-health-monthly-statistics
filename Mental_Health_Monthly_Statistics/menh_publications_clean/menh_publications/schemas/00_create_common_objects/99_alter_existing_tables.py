# Databricks notebook source
%md

## functions and lists to allow alterations to existing tables that cannot be dropped and recreated because they contain data that needs to be persisted

# COMMAND ----------

dbutils.widgets.text("db_output","menh_publications","Target Database")
db_output = dbutils.widgets.get("db_output")

# COMMAND ----------

# DBTITLE 1,Creating rpstartdate and rpenddate as tests for all_products_cached
dbutils.widgets.text("rp_startdate","","rp_startdate")
rp_startdate = dbutils.widgets.get("rp_startdate")
dbutils.widgets.text("rp_enddate","","rp_enddate")
rp_enddate = dbutils.widgets.get("rp_enddate")
dbutils.widgets.text("month_id","","month_id")
month_id = dbutils.widgets.get("month_id")


# COMMAND ----------

# DBTITLE 1,Creating rpstartdate and rpenddate as tests for all_products_cached (Previous)
#dbutils.widgets.text("rp_startdate","2022-04-01","rp_startdate")
#rp_startdate = dbutils.widgets.get("rp_startdate")
#dbutils.widgets.text("rp_enddate","2022-06-30","rp_enddate")
#rp_enddate = dbutils.widgets.get("rp_enddate")
#dbutils.widgets.text("month_id","1467","month_id")
#month_id = dbutils.widgets.get("month_id")


# COMMAND ----------

# DBTITLE 1,IsColumnInTable
# does datebase.table contain column? 1=yes, 0=no
def IsColumnInTable(database_name, table_name, column_name):
  try:
    df = spark.table(f"{database_name}.{table_name}")
    cols = df.columns
    if column_name in cols:
      return 1
    else:
      return 0
  except:
    return -1

# COMMAND ----------

# List of tables and column that needs adding to them
tableColumn = {'main_monthly_unformatted_new': 'SOURCE_DB', 'all_products_cached': 'SOURCE_DB', 'All_products_formatted': 'SOURCE_DB', 'rd_ccg_latest': 'original_ORG_CODE', 'rd_ccg_latest': 'original_NAME', 'audit_menh_publications': 'ADHOC_DESC'}

# list of tables and columns that need INT type columns adding
tableColumnINT = {'main_monthly_unformatted_new': 'PRODUCT_NO'}

# COMMAND ----------

# DBTITLE 1,Add STRING column to table if it doesn't exist
for table, column in tableColumn.items():
  print(table)
  print(column)
  exists = IsColumnInTable(db_output, table, column)
  if exists == 0:
    action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} STRING)""".format(db_output=db_output,table=table,column=column)
    print(action)
    spark.sql(action)

# COMMAND ----------

# DBTITLE 1,Add INT column to table if it doesn't exist
for table, column in tableColumnINT.items():
  print(table)
  print(column)
  print(db_output)
  exists = IsColumnInTable(db_output, table, column)
  print(exists)
  if exists == 0:
    action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} INT)""".format(db_output=db_output,table=table,column=column)
    print(action)
    spark.sql(action)

# COMMAND ----------

# DBTITLE 1,Roadmap for converting REPORTING DATE from STRING to DATE type
#Create another table (not temp view) "db_output.allproductscached2"-date column is date not string , then insert data from all products cached
#Drop all products, then re-create it, with date column as date not string, then transplant the data from "db_output.allproductscached2" into the re-created (And fixed) all_products cached
##Alternativley, after dropping all products cached, just rename it.


# COMMAND ----------

# DBTITLE 1,Make new version of all_products_cached with correct DATE format for reporting period, and rename.
%sql

DROP TABLE IF EXISTS $db_output.all_products_cached_v2;

Create TABLE IF NOT EXISTS $db_output.all_products_cached_v2 (
   MONTH_ID INT, 
   STATUS STRING,
   PRODUCT_NO INT, 
   REPORTING_PERIOD_START DATE, 
   REPORTING_PERIOD_END DATE, 
   BREAKDOWN STRING, 
   PRIMARY_LEVEL STRING, 
   PRIMARY_LEVEL_DESCRIPTION STRING, 
   SECONDARY_LEVEL STRING, 
   SECONDARY_LEVEL_DESCRIPTION STRING, 
   METRIC STRING, 
   METRIC_NAME STRING, 
   METRIC_VALUE STRING,
   SOURCE_DB string )
 USING DELTA
 PARTITIONED BY (MONTH_ID, STATUS)
  

# COMMAND ----------

# DBTITLE 1,Add data from all_products_cached ported into new version that holds data
%sql
INSERT INTO $db_output.all_products_cached_v2 
Select 
  MONTH_ID, 
  STATUS,
  PRODUCT_NO, 
  to_date(REPORTING_PERIOD_START,'yyyy-mm-dd'),
  to_date(REPORTING_PERIOD_END,'yyyy-mm-dd'),
  --last_day(to_date(REPORTING_PERIOD_END, 'yyyy-mm-dd')),
  BREAKDOWN, 
  PRIMARY_LEVEL, 
  PRIMARY_LEVEL_DESCRIPTION, 
  SECONDARY_LEVEL, 
  SECONDARY_LEVEL_DESCRIPTION, 
  METRIC, 
  METRIC_NAME, 
  METRIC_VALUE,
  SOURCE_DB  
  from $db_output.all_products_cached

# COMMAND ----------

# DBTITLE 1,DROP all_products_cached because it has obsolete STRING type column for reporting periods
#%sql
#--DROP TABLE $db_output.all_products_cached

# COMMAND ----------

# DBTITLE 1,Recreate table using correct date type for reporting period 
#%sql
#--Create TABLE IF NOT EXISTS $db_output.all_products_cached (
#--  MONTH_ID INT, 
#--  STATUS STRING,
#--   PRODUCT_NO INT, 
#--   REPORTING_PERIOD_START DATE, 
#--   REPORTING_PERIOD_END DATE, 
#--   BREAKDOWN STRING, 
#--   PRIMARY_LEVEL STRING, 
#--   PRIMARY_LEVEL_DESCRIPTION STRING, 
#--   SECONDARY_LEVEL STRING, 
#--   SECONDARY_LEVEL_DESCRIPTION STRING, 
#--   METRIC STRING, 
#--   METRIC_NAME STRING, 
#--   METRIC_VALUE STRING,
#--   SOURCE_DB string )
#-- USING DELTA
#-- PARTITIONED BY (MONTH_ID, STATUS)
 

# COMMAND ----------

# DBTITLE 1,INSERT data from the v2 version into the recreated all_products_cached
#%sql
#INSERT INTO $db_output.all_products_cached
#Select 
#MONTH_ID, 
#STATUS,
#PRODUCT_NO, 
 # to_date(REPORTING_PERIOD_START_UPDATED,'MMM-yy') as REPORTING_PERIOD,
 # last_day(to_date(REPORTING_PERIOD_END_UPDATED, 'MMM-yy')) as REPORTING_PERIOD_END,
#  REPORTING_PERIOD_START as REPORTING_PERIOD_START,
#  REPORTING_PERIOD_END AS REPORTING_PERIOD_END,
#  BREAKDOWN, 
#  PRIMARY_LEVEL, 
#  PRIMARY_LEVEL_DESCRIPTION, 
#  SECONDARY_LEVEL, 
 # SECONDARY_LEVEL_DESCRIPTION, 
#  METRIC, 
#  METRIC_NAME, 
#  METRIC_VALUE,
#  SOURCE_DB  
#  from $db_output.all_products_cached_v2 

# COMMAND ----------

#%sql
#--SELECT *
#--FROM $db_output.all_products_cached

# COMMAND ----------

#%sql
#--DROP TABLE $db_output.all_products_cached_v2 