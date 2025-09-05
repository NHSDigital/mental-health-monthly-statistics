# Databricks notebook source
product = dbutils.widgets.get("product")
print(product)

db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

reference_data = dbutils.widgets.get("reference_data")
print(reference_data)
assert reference_data

reload_ref_data = dbutils.widgets.get("reload_ref_data")
print(reload_ref_data)
assert reload_ref_data

mhsds_database = dbutils.widgets.get("mhsds_database")
print(mhsds_database)
assert mhsds_database

# COMMAND ----------

params = {
  "db_output"      : db_output, 
  "product"        : product,
  "reference_data"  : reference_data,
  "reload_ref_data": reload_ref_data,
  "mhsds_database"   : mhsds_database
}
params

# COMMAND ----------

 %md
 always call all notebooks under create_common_notebooks 

# COMMAND ----------

# DBTITLE 1,create_common_objects
dbutils.notebook.run('00_create_common_objects/01_create_common_tables', 0, params)

# COMMAND ----------

# DBTITLE 1,create new tables for v5 restructure
dbutils.notebook.run('00_create_common_objects/5.0_create_version_change_tables', 0, params)

# COMMAND ----------

metadata_df = spark.sql(f"SELECT * FROM {db_output}.metadata")
display(metadata_df)

# COMMAND ----------

from pyspark.sql.functions import col,lit
group_by_seq =  metadata_df.select(col('seq'))\
  .groupBy(col('seq'))\
  .count()\
  .where(col('count') > 1)\
  .collect()

print(group_by_seq)

if group_by_seq:
  raise ValueError(f"multiple entries found for seq {group_by_seq}")
  
#check for ambigious metadata
group_by_products = metadata_df.select(col('product'),col('module'),col('stage'),col('notebook_path'))\
  .groupBy(col('product'),col('module'),col('stage'),col('notebook_path'))\
  .count()\
  .where(col('count') > 1)\
  .collect()

print(group_by_products)

if group_by_products:
  raise ValueError(f"multiple entries found product, module and stage columns")
#DEFAULT RUN - RUN ALL
if not product:
  print("processing default run")
  rows = metadata_df.select('notebook_path')\
    .where(col('module') == lit('init_schemas'))\
    .sort(col("seq"))\
    .collect()
  
  print(rows)
  for row in rows:
    path = row['notebook_path']
    print(path)
    dbutils.notebook.run(f'{path}', 0, params)
else:
  print(f"processing {product} product")
  rows = metadata_df.select('notebook_path').where((col('stage') == lit('create_schema')) & (col("product") == lit(product))).sort(col("seq")).collect()
  print(rows)
  if not rows:
    raise ValueError(f"invalid value {product} for product")
  
  for row in rows:
    path = row['notebook_path']
    print(path)
    dbutils.notebook.run(f'{path}', 0, params)
    
#   dbutils.notebook.run(f"{rows[0]['notebook_path']}", 0, params)

# COMMAND ----------

# DBTITLE 1,alter structure of existing tables - needs to be at the end so it can alter ANY existing tables
dbutils.notebook.run('00_create_common_objects/99_alter_existing_tables', 0, params)