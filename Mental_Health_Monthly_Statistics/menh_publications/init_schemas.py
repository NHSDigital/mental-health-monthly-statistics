# Databricks notebook source
reload_ref_data = True
update_metadata = True

product = dbutils.widgets.text("product", "", "product")
product = dbutils.widgets.get("product")

dbutils.widgets.text("db","menh_publications","Target Database")
db = dbutils.widgets.get("db")
assert db

# $reference_data reference data is included here because it is used to create views within /03_RestrictiveInterventions (e.g. ethnicity from reference data)
dbutils.widgets.text("$reference_data","$reference_data","Source Ref Database")
$reference_data = dbutils.widgets.get("$reference_data")
assert $reference_data

# For v5 Source_DB
# source_db is included here because one off updates to populate the new DB_SOURCE column were run from init_schemas
# ordinarily we would not expect to need any source data within the schema creation code
#dbutils.widgets.text("$mhsds", "$mhsds", "v5 source database")
dbutils.widgets.text("$mhsds", "$mhsds", "v6 source database")
$mhsds = dbutils.widgets.get("$mhsds")
assert $mhsds

# COMMAND ----------

params = {
  'db_output'       : db, 
  'product'         : product,
  '$reference_data'   : $reference_data,
  'update_metadata' : update_metadata,
  'reload_ref_data' : reload_ref_data,
  '$mhsds'    : $mhsds
}

print(params)

# COMMAND ----------

# DBTITLE 1,create and prep metadata
dbutils.notebook.run('schemas/metadata',0,params)

# COMMAND ----------


dbutils.notebook.run('schemas/schema_master', 0, params)