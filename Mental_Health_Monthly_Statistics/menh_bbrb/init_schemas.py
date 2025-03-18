# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# # run this if you want to drop all tables in your chosen database
# db_output = dbutils.widgets.get("db")
# views_meta_df = spark.sql(f"show views in {db_output}")
# actual_views = [ r['viewName'] for r in views_meta_df.collect()]
# for exp_view in actual_views:
#   print(exp_view)
#   spark.sql(f"drop view {db_output}.{exp_view}")
# tables_meta_df = spark.sql(f"show tables in {db_output}")
# actual_tables = [ r['tableName'] for r in tables_meta_df.collect()]
# for exp_table in actual_tables:
#   print(exp_table)
#   spark.sql(f"drop table {db_output}.{exp_table}")

# COMMAND ----------

dbutils.widgets.text("db","menh_bbrb","Target Database")
db = dbutils.widgets.get("db")
assert db

# COMMAND ----------

params = {'db_output': db}
print('params:', params)

# COMMAND ----------

dbutils.notebook.run('schemas/schema_master', 0, params)