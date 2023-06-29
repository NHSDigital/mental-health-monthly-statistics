# Databricks notebook source
import os

dbutils.widgets.text("db","","Target Database")
db_output = dbutils.widgets.get("db")
#db = dbutils.widgets.get("db")
assert db_output

current_env = os.environ.get('env')
assert current_env

params = {'db_output' : db_output}

print(params)

# COMMAND ----------

# DBTITLE 1,create tables
dbutils.notebook.run('00_DB/db_upgrade_1', 0, params)
dbutils.notebook.run('00_DB/db_upgrade_3', 0, params) # monthly _unformatted tables created here, plus all_products_cached
dbutils.notebook.run('00_DB/db_upgrade_4', 0, params) # FYFV_unformatted only
dbutils.notebook.run('00_DB/db_upgrade_5', 0, params)
dbutils.notebook.run('00_DB/db_upgrade_6', 0, params)
dbutils.notebook.run('00_DB/db_upgrade_7', 0, params) # LDA_Counts
dbutils.notebook.run('00_DB/db_upgrade_8', 0, params)
dbutils.notebook.run('00_DB/db_upgrade_9', 0, params)

#dbutils.notebook.run('00_DB/db_upgrade_3a', 0, params) - BS 05/Feb - Commented this code as I moved them into original script
dbutils.notebook.run('00_DB/db_upgrade_3b', 0, params)

dbutils.notebook.run('00_DB/db_upgrade_10', 0, params) # contains validcodes table from v5 

# UKD 01/03/2022: BITC-3061 (menh_analysis: Create audit table)
dbutils.notebook.run('00_DB/db_upgrade_11', 0, params) # creates audit_menh_analysis
dbutils.notebook.run('00_DB/db_upgrade_11a', 0, params) # alters audit_menh_analysis to add ADHOC_DESC
dbutils.notebook.run('00_DB/db_upgrade_12', 0, params) #creates $db_output.tmp_mhmab_mhs001mpi_latest_month_data
