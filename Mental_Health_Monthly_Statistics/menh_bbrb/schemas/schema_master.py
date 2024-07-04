# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

# COMMAND ----------

params = { "db_output": db_output}
params

# COMMAND ----------

dbutils.notebook.run('00_create_common_objects/01_create_common_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('01_create_los_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('03_create_cmh_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('04_create_cyp_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('05_create_cyp_outcomes_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('06_create_ips_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('07_create_uec_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('08_create_oaps_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('00_create_common_objects/99_alter_existing_tables', 0, params)