# Databricks notebook source
dbutils.widgets.text("db_output","menh_analysis","db_output")

db_output = dbutils.widgets.get("db_output")
print(db_output)

month_id = dbutils.widgets.get("month_id")
print(month_id)

rp_enddate = dbutils.widgets.get("rp_enddate")
print(rp_enddate)

assert db_output

# COMMAND ----------

'''updates RAW tables with ICB labelling for ICB months - so that data kept for posterity is correctly labelled'''

if int(month_id) > 1467:  # the last month of CCGs is June 2022 
  print('month_id requires updates of unrounded tables from CCG to sub ICB  (& STP to ICB) naming')
  
#   main monthly raw table
  sqlContext.sql(f"UPDATE {db_output}.Main_monthly_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 
  
#   CYP 2nd contact raw table
  sqlContext.sql(f"UPDATE {db_output}.CYP_2nd_contact_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 
  sqlContext.sql(f"UPDATE {db_output}.CYP_2nd_contact_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 

#   CAP raw table
  sqlContext.sql(f"UPDATE {db_output}.CAP_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 
  
#   CYP monthly raw table
  sqlContext.sql(f"UPDATE {db_output}.CYP_monthly_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 
  
#   FYFV raw table - this is only updated quarterly - there should be no updates occurring between quarters (but shouldn't fail)
  sqlContext.sql(f"UPDATE {db_output}.FYFV_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 
  sqlContext.sql(f"UPDATE {db_output}.FYFV_unformatted SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") 
  
else:
  print('no CCG or STP updates required for this month')