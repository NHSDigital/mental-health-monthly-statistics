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

# DBTITLE 1,tidy up after BITC-2921 (can be removed after init_schemas has been run in PROD)
 %sql
 
 DROP TABLE IF EXISTS $db_output.all_products_cached_v2;