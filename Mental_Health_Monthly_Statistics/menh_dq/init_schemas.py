# Databricks notebook source
# dbutils.widgets.removeAll()


# COMMAND ----------

import os


dbutils.widgets.text("db","menh_dq","Target Database")
db = dbutils.widgets.get("db")
assert db

db_output = db
assert db_output

dbutils.widgets.text("$reference_data","$reference_data","Source Ref Database")
$reference_data = dbutils.widgets.get("$reference_data")
assert $reference_data

current_env = os.environ.get('env')
assert current_env

# User: only needed for the addition of SOURCE_DB column to existing tables - no longer needed
# User: commenting out as the new source db is menh_v5_pre_clear anyway (so if needed would need to change)

# dbutils.widgets.text("$mhsds", "", "Source database")
# $mhsds = dbutils.widgets.get("$mhsds")
# assert $mhsds

# User: removed all other parameters from this notebook as they are not needed for the set up of tables.

params = {'db' : db, 'db_output' : db_output, '$reference_data': $reference_data}

print(params)

# COMMAND ----------


dbutils.notebook.run('schemas/1.1.create_tables', 0, params)
dbutils.notebook.run('schemas/1.3.populate_tables', 0, params)
dbutils.notebook.run('schemas/1.4.populate_smh_service_category_code', 0, params)


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,create extract tables
dbutils.notebook.run('schemas/1.5.create_extract_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('schemas/1.6.add_source_db', 0, params)

# COMMAND ----------

dbutils.notebook.run('schemas/1.2.create_views', 0, params)


# COMMAND ----------

# DBTITLE 1,Create Common tables including Valid_Codes table
dbutils.notebook.run('schemas/00_create_common_objects/create_version_change_tables', 0, params)