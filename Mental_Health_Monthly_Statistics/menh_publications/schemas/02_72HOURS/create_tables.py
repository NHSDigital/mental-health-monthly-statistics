# Databricks notebook source
print("72 hours create tables")

db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output
mhsds_database = dbutils.widgets.get("mhsds_database")
print(mhsds_database)
assert mhsds_database


# COMMAND ----------

# DBTITLE 1,72hours_unrounded_stg
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.72hours_unrounded_stg(
   UniqMonthID int,
   Status string,
   ResponsibleProv string,
   CCG string,
   EligibleDischFlag int,
   ElgibleDischFlag_Modified int,
   FollowedUp3Days int,
   SOURCE_DB string 
 )
 USING DELTA
 PARTITIONED BY (UniqMonthID)

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
tableColumn = {'72hours_unrounded_stg': 'SOURCE_DB'}

# COMMAND ----------

# DBTITLE 1,Add column to table if it doesn't exist
for table, column in tableColumn.items():
  print(table)
  print(column)
  exists = IsColumnInTable(db_output, table, column)
  if exists == 0:
    action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} STRING)""".format(db_output=db_output,table=table,column=column)
    print(action)
    spark.sql(action)


# COMMAND ----------

# DBTITLE 1,Set SOURCE_DB to source database
# update only needs doing once
for table, column in tableColumn.items():
  action = """Update {db_output}.{table} SET {column} = '{mhsds_database}' where {column} is null""".format(db_output=db_output,table=table,column=column,mhsds_database=mhsds_database)
  print(action)
  spark.sql(action)