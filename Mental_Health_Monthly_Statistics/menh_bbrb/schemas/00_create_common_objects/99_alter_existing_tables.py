# Databricks notebook source
 %md

 ## functions and lists to allow alterations to existing tables that cannot be dropped and recreated because they contain data that needs to be persisted

# COMMAND ----------

# dbutils.widgets.text("db_output","menh_publications","Target Database")
db_output = dbutils.widgets.get("db_output")

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
#tableColumn = {'main_monthly_unformatted_new': 'SOURCE_DB', 'all_products_cached': 'SOURCE_DB', 'All_products_formatted': 'SOURCE_DB', 'rd_ccg_latest': 'original_ORG_CODE', 'rd_ccg_latest': 'original_NAME'}

# list of tables and columns that need INT type columns adding
#tableColumnINT = {'main_monthly_unformatted_new': 'PRODUCT_NO'}

# COMMAND ----------

tableColumn = {'bbrb_final': 'STATUS','los_raw':'STATUS', "audit_menh_bbrb": "NOTES"}

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
#for table, column in tableColumnINT.items():
  #print(table)
  #print(column)
#   exists = IsColumnInTable(db_output, table, column)
#   if exists == 0:
#     action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} INT)""".format(db_output=db_output,table=table,column=column)
#     print(action)
#     spark.sql(action)

# COMMAND ----------

# DBTITLE 1,Set SOURCE_DB to source database
# update only needs doing once
# for table, column in tableColumn.items():
#   action = """Update {db_output}.{table} SET {column} = '{mhsds_database}' where {column} is null""".format(db_output=db_output,table=table,column=column,mhsds_database=mhsds_database)
#   print(action)
#   spark.sql(action)

# COMMAND ----------

# DBTITLE 1,Set PRODUCT_NO to 72HOURS 
# update only needs doing once - DONE!
# for table, column in tableColumnINT.items():
#   action = """Update {db_output}.{table} SET {column} = 2 where {column} is null""".format(db_output=db_output,table=table,column=column)
#   print(action)
#   spark.sql(action)