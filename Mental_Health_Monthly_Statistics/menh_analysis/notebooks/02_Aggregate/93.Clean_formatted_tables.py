# Databricks notebook source
 %md

 # Clean FORMATTED output tables

 This is in case there already is left-over data for this month/status in these tables.

# COMMAND ----------

# DBTITLE 1,Delete Existing Month Data
 %sql

 DELETE FROM $db_output.All_products_formatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;

 DELETE FROM $db_output.Ascof_formatted
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;

 TRUNCATE TABLE $db_output.all_products_cached;

# COMMAND ----------

# DBTITLE 1,Vacuum formatted output tables for performance
 %sql

 VACUUM $db_output.all_products_cached RETAIN 8 HOURS;
 VACUUM $db_output.All_products_formatted RETAIN 8 HOURS;
 VACUUM $db_output.Ascof_formatted RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Optimize unformatted output tables for performance
 %python

 import os

 db_output = dbutils.widgets.get("db_output")
 month_id = dbutils.widgets.get("month_id")

 is_quarter = int(month_id) % 3 ==0

 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Main_monthly_unformatted'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_unformatted'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_2nd_contact_unformatted'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CAP_Unformatted'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_monthly_unformatted'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Ascof_unformatted'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='FYFV_unformatted'))
   
 print(is_quarter)

# COMMAND ----------

# DBTITLE 1,Optimise Quarterly Tables
 %python

 if is_quarter:
   ##only needs to run when month_id is divisible by 3 with no remainder

   import os

   db_output = dbutils.widgets.get("db_output")

   if os.environ['env'] == 'prod':
     spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='FYFV_unformatted'))