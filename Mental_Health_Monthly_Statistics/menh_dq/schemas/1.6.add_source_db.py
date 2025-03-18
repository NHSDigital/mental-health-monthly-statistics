# Databricks notebook source
db = dbutils.widgets.get("db")
print(db)
assert db

# User note: only needed for the addition of SOURCE_DB column to existing tables - no longer needed
# User note: commenting out as the new source db is menh_v5_pre_clear anyway (so if needed would need to change)

# $mhsds_db = dbutils.widgets.get("$mhsds_db")
# print($mhsds_db)
# assert $mhsds_db


# COMMAND ----------

# DBTITLE 1,dictionary structure as a schema for tables with new column - i.e. end state
# table names all in lower case
# code superseded by calls to IsColumnInTable - see below

table_list =  {
 'dq_inventory': {'UniqMonthID': 'int',
  'DimensionTypeId': 'tinyint',
  'MeasureId': 'tinyint',
  'MeasureTypeId': 'tinyint',
  'MetricTypeId': 'tinyint',
  'OrgIDProv': 'string',
  'Value': 'int',
  'SOURCE_DB': 'string'},
 'dq_coverage_monthly_csv': {'MONTH_ID': 'int',
  'REPORTING_PERIOD_START': 'string',
  'REPORTING_PERIOD_END': 'string',
  'STATUS': 'string',
  'ORGANISATION_CODE': 'string',
  'ORGANISATION_NAME': 'string',
  'TABLE_NAME': 'string',
  'COVERAGE_COUNT': 'bigint',
  'SOURCE_DB': 'string'},
 'dq_coverage_monthly_pbi': {'MONTH_ID': 'int',
  'STATUS': 'string',
  'ORGANISATION_CODE': 'string',
  'ORGANISATION_NAME': 'string',
  'TABLE_NAME': 'string',
  'SUM_OF_PERIOD_NAME': 'string',
  'X': 'string',
  'PERIOD_NAME': 'string',
  'PERIOD_END_DATE': 'string',
  'SOURCE_DB': 'string'},
 'DQ_VODIM_monthly_csv': {'Month_Id': 'int',
  'Reporting_Period': 'string',
  'Status': 'string',
  'Reporting_Level': 'string',
  'Provider_Code': 'string',
  'Provider_Name': 'string',
  'DQ_Measure': 'string',
  'DQ_Measure_Name': 'string',
  'DQ_Result': 'string',
  'DQ_Dataset_Metric_ID': 'string',
  'Unit': 'string',
  'Value': 'string',
  'SOURCE_DB': 'string'},
 'DQ_VODIM_monthly_pbi': {'Month_Id': 'int',
  'Reporting_Period': 'string',
  'Status': 'string',
  'Reporting_Level': 'string',
  'Provider_Code': 'string',
  'Provider_Name': 'string',
  'DQ_Measure': 'string',
  'DQ_Result': 'string',
  'DQ_Dataset_Metric_ID': 'string',
  'Count': 'int',
  'Percentage': 'string',
  'SOURCE_DB': 'string'},
 'DQMI_Monthly_Data': {'Month_ID': 'int',
  'Period': 'string',
  'Dataset': 'string',
  'Status': 'string',
  'DataProviderId': 'string',
  'DataProviderName': 'string',
  'DataItem': 'string',
  'CompleteNumerator': 'int',
  'CompleteDenominator': 'int',
  'ValidNumerator': 'int',
  'DefaultNumerator': 'int',
  'ValidDenominator': 'int',
  'SOURCE_DB': 'string'},
 'DQMI_Integrity': {'Month_ID': 'string',
  'Period': 'string',
  'Dataset': 'string',
  'Status': 'string',
  'OrgIdProv': 'string',
  'MeasureId': 'string',
  'MeasureName': 'string',
  'Numerator': 'int',
  'Denominator': 'int',
  'SOURCE_DB': 'string'}
}

# COMMAND ----------

# DBTITLE 1,Check actual against desired state - add column if doesn't exist
# look at schema_tests

# Function to get the schema for a table
# Function takes the database name and the table name and returns a dictionary of the columns and data types.

# code superseded by calls to IsColumnInTable - see below

def get_table_schema(dbase,table_name):

    desc_query = dbase+"."+table_name   
    df_tab_cols = spark.createDataFrame(spark.table(desc_query).dtypes)
    tab_cols_dict = dict(map(lambda row: (row[0],row[1]), df_tab_cols.collect())) 
    return tab_cols_dict

# This is the test, it cycles through the table_list dictionary 
# It takes the returned dictionary of columns and dtypes from the database 
# It then checks the returned columns and types match the dictionary of columns from tab_list.
# The try will pick up if there is a difference, print the table name, the columns from the list above
# and the list from the schema, it will then raise the error and fail the test at that point.

for tab in table_list:
  
    tab_cols_d = get_table_schema(db,tab)
     
    
    try: 
        assert tab_cols_d == table_list[tab] 
    except AssertionError as error:
        print(tab)
        print(tab_cols_d)
        action = """ALTER TABLE  {db}.{tab} ADD COLUMNS (SOURCE_DB STRING)""".format(db=db,tab=tab)
        spark.sql(action)
        print(action)
  
    
    

# COMMAND ----------

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
tableColumn = {'DQMI_Coverage': 'SOURCE_DB', 'dq_inventory': 'SOURCE_DB', 'dq_coverage_monthly_csv': 'SOURCE_DB', 'dq_coverage_monthly_pbi': 'SOURCE_DB', 'DQ_VODIM_monthly_csv': 'SOURCE_DB', 'DQ_VODIM_monthly_pbi': 'SOURCE_DB', 'DQMI_Monthly_Data': 'SOURCE_DB', 'DQMI_Integrity': 'SOURCE_DB'}
print(type(tableColumn))

# COMMAND ----------

# DBTITLE 1,Add column to table if it doesn't exist
for table, column in tableColumn.items():
  print(table)
  print(column)
  exists = IsColumnInTable(db, table, column)
  if exists == 0:
    action = """ALTER TABLE {db}.{table} ADD COLUMNS ({column} STRING)""".format(db=db,table=table,column=column)
    print(action)
    spark.sql(action)


# COMMAND ----------

# DBTITLE 1,Set SOURCE_DB to source database
# update only needs doing once - DONE
# for table, column in tableColumn.items():
#   action = """Update {db}.{table} SET {column} = '{$mhsds_db}' where {column} is null""".format(db=db,table=table,column=column,$mhsds_db=$mhsds_db)
#   print(action)
#   spark.sql(action)


# COMMAND ----------

