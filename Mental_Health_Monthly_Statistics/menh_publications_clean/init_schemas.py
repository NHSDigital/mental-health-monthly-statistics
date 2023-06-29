# Databricks notebook source
reload_ref_data = True
update_metadata = True

product = dbutils.widgets.text("product", "", "product")
product = dbutils.widgets.get("product")

dbutils.widgets.text("db","menh_publications","Target Database")
db = dbutils.widgets.get("db")
assert db

# database reference data is included here because it is used to create views within /03_RestrictiveInterventions (e.g. ethnicity from reference data)
dbutils.widgets.text("database","database","Source Ref Database")
database = dbutils.widgets.get("database")
assert database

# For v5 Source_DB
# source_db is included here because one off updates to populate the new DB_SOURCE column were run from init_schemas
# ordinarily we would not expect to need any source data within the schema creation code
dbutils.widgets.text("$db_source", "$db_source", "v5 source database")
$db_source = dbutils.widgets.get("$db_source")
assert $db_source

# COMMAND ----------

params = {
  'db_output'       : db, 
  'product'         : product,
  'database'        : database,
  'update_metadata' : update_metadata,
  'reload_ref_data' : reload_ref_data,
  '$db_source'    : $db_source
}

print(params)

# COMMAND ----------

# DBTITLE 1,create and prep metadata
dbutils.notebook.run('schemas/metadata',0,params)

# COMMAND ----------


dbutils.notebook.run('schemas/schema_master', 0, params)
