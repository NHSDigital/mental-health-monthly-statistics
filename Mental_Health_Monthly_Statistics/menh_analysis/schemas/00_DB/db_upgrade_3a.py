# Databricks notebook source
 %md
 
 this notebook is named db_upgrade_3a as it is mostly making changes to tables created in db_upgrade_3
 
 it must be run after db_upgrade_5 as it alters tables created there.
 
 the changes are the addition of the SOURCE_DB column to various tables

# COMMAND ----------

# dbutils.widgets.text("db_output","_demo_test2","Target Database")
db_output = dbutils.widgets.get("db_output")
assert db_output

# COMMAND ----------

# DBTITLE 1,dictionary structure as a schema for tables with new column - i.e. end state
# table names all in lower case

table_list =  {
 'main_monthly_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'awt_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'LEVEL': 'string',
  'LEVEL_DESCRIPTION': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string'},
 'cyp_2nd_contact_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'cap_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'LEVEL': 'string',
  'LEVEL_DESCRIPTION': 'string',
  'CLUSTER': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'cyp_monthly_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'ascof_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'LEVEL_ONE': 'string',
  'LEVEL_ONE_DESCRIPTION': 'string',
  'LEVEL_TWO': 'string',
  'LEVEL_TWO_DESCRIPTION': 'string',
  'LEVEL_THREE': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'all_products_cached': {'PRODUCT_NO': 'int',
  'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'METRIC': 'string',
  'METRIC_NAME': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'fyfv_unformatted': {'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'METRIC': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'ccgois_unformatted': {'REPORTING_PERIOD': 'string',
  'STATUS': 'string',
  'CCG': 'string',
  'AgeRepPeriodEnd': 'int',
  'Gender': 'string',
  'CLUSTER': 'string',
  'SUPER_CLUSTER': 'string',
  'EMPLOYMENT_STATUS': 'string',
  'METRIC_VALUE': 'float',
  'SOURCE_DB': 'string'},
 'all_products_formatted': {'PRODUCT_NO': 'string',
  'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'MEASURE_ID': 'string',
  'MEASURE_NAME': 'string',
  'MEASURE_VALUE': 'string',
  'DATETIME_INSERTED': 'timestamp',
  'SOURCE_DB': 'string'},
 'ascof_formatted': {'PRODUCT_NO': 'string',
  'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'STATUS': 'string',
  'BREAKDOWN': 'string',
  'PRIMARY_LEVEL': 'string',
  'PRIMARY_LEVEL_DESCRIPTION': 'string',
  'SECONDARY_LEVEL': 'string',
  'SECONDARY_LEVEL_DESCRIPTION': 'string',
  'THIRD_LEVEL': 'string',
  'THIRD_LEVEL_DESCRIPTION': 'string',
  'MEASURE_ID': 'string',
  'MEASURE_NAME': 'string',
  'MEASURE_VALUE': 'string',
  'DATETIME_INSERTED': 'timestamp',
  'SOURCE_DB': 'string'}
}

# COMMAND ----------

# DBTITLE 1,Check actual against desired state - add column if doesn't exist
# look at schema_tests

# Function to get the schema for a table
# Function takes the database name and the table name and returns a dictionary of the columns and data types.

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
  
    tab_cols_d = get_table_schema(db_output,tab)
     
    
    try: 
        assert tab_cols_d == table_list[tab] 
    except AssertionError as error:
        print(tab)
        action = """ALTER TABLE  {db_output}.{tab} ADD COLUMNS (SOURCE_DB STRING)""".format(db_output=db_output,tab=tab)
        print(action)
        spark.sql(action)
#         print("Predicted table: ",table_list[tab])
#         print("Actual Table: ", tab_cols_d)
#         raise
   
    
    

# COMMAND ----------

# DBTITLE 1,If different then add column
