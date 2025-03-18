# Databricks notebook source
 %md

 this notebook is named db_upgrade_11a as it is making changes to tables created in db_upgrade_11

 it must be run after db_upgrade_11 as it alters tables created there.

 the changes are the addition of the ADHOC_DESC column to audit_menh_analysis

# COMMAND ----------

# dbutils.widgets.text("db_output","User note_demo_test2","Target Database")
db_output = dbutils.widgets.get("db_output")
assert db_output

# COMMAND ----------

# DBTITLE 1,dictionary structure as a schema for tables with new column - i.e. end state
# table names all in lower case

table_list =  {
 'audit_menh_analysis': {'MONTH_ID': 'int',
  'STATUS': 'string',
  'REPORTING_PERIOD_START': 'date',
  'REPORTING_PERIOD_END': 'date',
  'SOURCE_DB': 'string',
  'RUN_START': 'timestamp',
  'RUN_END': 'timestamp',
  'ADHOC_DESC': 'string'}
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
    print(tab_cols_dict)
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
        action = """ALTER TABLE  {db_output}.{tab} ADD COLUMNS (ADHOC_DESC STRING)""".format(db_output=db_output,tab=tab)
        print(action)
        spark.sql(action)
#         print("Predicted table: ",table_list[tab])
#         print("Actual Table: ", tab_cols_d)
#         raise
   
    
    

# COMMAND ----------

# DBTITLE 1,If different then add column
