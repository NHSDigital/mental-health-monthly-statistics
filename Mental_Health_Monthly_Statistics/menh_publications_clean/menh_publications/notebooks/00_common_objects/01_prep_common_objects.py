# Databricks notebook source
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output

month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id

db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source

status=dbutils.widgets.get("status")
print(status)
assert status

product=dbutils.widgets.get("product")
print(product)

# COMMAND ----------

# DBTITLE 1,Delete Existing Month Data - python equivalent
listoftables = {'All_products_formatted', 'main_monthly_unformatted_new', 'all_products_cached', 'third_level_products_cached', 'third_level_products_formatted'}

if product:
  prod_id = spark.sql("SELECT LEFT(seq,1) FROM {0}.metadata WHERE PRODUCT = '{1}'".format(db_output,product)).collect()[0][0]
  print(prod_id)

# COMMAND ----------

for table in listoftables:
  print(table)
  
  if not product:
    action = """DELETE FROM {db_output}.{table} WHERE MONTH_ID = {month_id} AND STATUS = '{status}' AND SOURCE_DB = '{db_source}'""".format(db_output=db_output,table=table,month_id=month_id,status=status,db_source=db_source)
    print(action)
    spark.sql(action)
    
  else:
    action = """DELETE FROM {db_output}.{table} WHERE MONTH_ID = {month_id} AND STATUS = '{status}' AND SOURCE_DB = '{db_source}' AND PRODUCT_NO = {prod_id}""".format(db_output=db_output,table=table,month_id=month_id,status=status,db_source=db_source,prod_id=prod_id)
    print(action)
    spark.sql(action)

# COMMAND ----------

# DBTITLE 1,Delete Existing Month Data - original - replaced by the code above
%sql


-- DELETE FROM $db_output.All_products_formatted
-- WHERE MONTH_ID = '$month_id'
-- AND STATUS = '$status'
-- AND SOURCE_DB = '$db_source';

-- DELETE FROM $db_output.main_monthly_unformatted_new
-- WHERE MONTH_ID = '$month_id'
-- AND STATUS = '$status'
-- AND SOURCE_DB = '$db_source';

-- DELETE FROM $db_output.all_products_cached
-- WHERE MONTH_ID = '$month_id'
-- AND STATUS = '$status'
-- AND SOURCE_DB = '$db_source';

-- DELETE FROM $db_output.third_level_products_cached
-- WHERE MONTH_ID = '$month_id'
-- AND STATUS = '$status'
-- AND SOURCE_DB = '$db_source';

-- DELETE FROM $db_output.third_level_products_formatted
-- WHERE MONTH_ID = '$month_id'
-- AND STATUS = '$status'
-- AND SOURCE_DB = '$db_source';

# COMMAND ----------

# DBTITLE 1,Vacuum formatted output tables for performance
%sql

VACUUM $db_output.all_products_cached RETAIN 8 HOURS;
VACUUM $db_output.All_products_formatted RETAIN 8 HOURS;
VACUUM $db_output.main_monthly_unformatted_new RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Optimise Tables
import os

db_output = dbutils.widgets.get("db_output")

spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='main_monthly_unformatted_new'))

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS001_CCG_LATEST'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS001_CCG_LATEST'))