# Databricks notebook source
 %md
 
 # Round METRIC_VALUE and fill in 'NONE' wherever no value
 
 Suppression Rules are documented through inline comments in SQL below.

# COMMAND ----------

# DBTITLE 1,Numerator / Denominator proportions
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW NumeratorDenominatorProportions AS
 SELECT 'AMH13e%' AS METRIC,'AMH13e' AS Numerator,'AMH03e' AS Denominator UNION
 SELECT 'AMH14e%' AS METRIC,'AMH14e' AS Numerator,'AMH03e' AS Denominator UNION
 SELECT 'AMH16e%' AS METRIC,'AMH16e' AS Numerator,'AMH03e' AS Denominator UNION
 SELECT 'AMH17e%' AS METRIC,'AMH17e' AS Numerator,'AMH03e' AS Denominator UNION
 SELECT 'AMH15' AS METRIC,'AMH14' AS Numerator,'AMH03' AS Denominator UNION
 SELECT 'AMH18' AS METRIC,'AMH17' AS Numerator,'AMH03' AS Denominator UNION
 SELECT 'ACC37' AS METRIC, 'ACC36' AS Numerator, 'ACC33' AS Denominator UNION
 SELECT 'ACC53' AS METRIC, 'ACC02' AS Numerator, 'ACC33' AS Denominator UNION
 SELECT 'ACC62' AS METRIC, 'ACC54' AS Numerator, 'ACC33' AS Denominator UNION
 SELECT 'EIP23i' AS METRIC, 'EIP23b' AS Numerator, 'EIP23a' AS Denominator UNION
 SELECT 'EIP23ia' AS METRIC, 'EIP23ba' AS Numerator, 'EIP23aa' AS Denominator UNION
 SELECT 'EIP23ib' AS METRIC, 'EIP23bb' AS Numerator, 'EIP23ab' AS Denominator UNION
 SELECT 'EIP23ic' AS METRIC, 'EIP23bc' AS Numerator, 'EIP23ac' AS Denominator UNION
 SELECT 'EIP23j' AS METRIC, 'EIP23f' AS Numerator, 'EIP23d' AS Denominator UNION
 SELECT 'EIP23ja' AS METRIC, 'EIP23fa' AS Numerator, 'EIP23da' AS Denominator UNION
 SELECT 'EIP23jb' AS METRIC, 'EIP23fb' AS Numerator, 'EIP23db' AS Denominator UNION
 SELECT 'EIP23jc' AS METRIC, 'EIP23fc' AS Numerator, 'EIP23dc' AS Denominator UNION
 SELECT 'EIP67' AS METRIC, 'EIP66' AS Numerator, 'EIP64' AS Denominator UNION
 SELECT 'EIP67a' AS METRIC, 'EIP66a' AS Numerator, 'EIP64a' AS Denominator UNION
 SELECT 'EIP67b' AS METRIC, 'EIP66b' AS Numerator, 'EIP64b' AS Denominator UNION
 SELECT 'EIP67c' AS METRIC, 'EIP66c' AS Numerator, 'EIP64c' AS Denominator

# COMMAND ----------

# DBTITLE 1,1-5. Round and format most products (for England figures)
 %sql
 
 INSERT INTO $db_output.All_products_formatted
 SELECT 
     PRODUCT_NO,
     REPORTING_PERIOD_START,
     REPORTING_PERIOD_END,
     STATUS,
     BREAKDOWN,
     COALESCE(PRIMARY_LEVEL, 'NONE') AS PRIMARY_LEVEL,
     COALESCE(PRIMARY_LEVEL_DESCRIPTION, 'NONE') AS PRIMARY_LEVEL_DESCRIPTION,
     COALESCE(SECONDARY_LEVEL, 'NONE') AS SECONDARY_LEVEL,
     COALESCE(SECONDARY_LEVEL_DESCRIPTION, 'NONE') AS SECONDARY_LEVEL_DESCRIPTION,
     METRIC AS MEASURE_ID,
     METRIC_NAME AS MEASURE_NAME,
     COALESCE(METRIC_VALUE, '0') AS MEASURE_VALUE, -- England level stays unrounded
     CURRENT_TIMESTAMP() AS DATETIME_INSERTED,
     SOURCE_DB
 FROM $db_output.all_products_cached
 WHERE PRIMARY_LEVEL = 'England' -- WARNING: Restrict on PRIMARY_LEVEL, cos BREAKDOWN does not work for some reason

# COMMAND ----------

# DBTITLE 1,1-5. Round and format most products (for Sub-England figures)
 %sql
 
 INSERT INTO $db_output.All_products_formatted
 SELECT
     a.PRODUCT_NO,
     a.REPORTING_PERIOD_START,
     a.REPORTING_PERIOD_END,
     COALESCE(a.STATUS, '$status') AS STATUS,
     --a.BREAKDOWN, -- change case statement here to rename rounded products before output
 
     CASE
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence'  THEN 'Sub ICB - GP Practice or Residence'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; Provider'  THEN 'Sub ICB - GP Practice or Residence; Provider'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; ConsMechanismMH'  THEN 'Sub ICB - GP Practice or Residence; ConsMechanismMH'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; DNA Reason'  THEN 'Sub ICB - GP Practice or Residence; DNA Reason'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; Referral Source'  THEN 'Sub ICB - GP Practice or Residence; Referral Source'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; Delayed discharge attributable to'  THEN 'Sub ICB - GP Practice or Residence; Delayed discharge attributable to'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; Delayed discharge reason'  THEN 'Sub ICB - GP Practice or Residence; Delayed discharge reason'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'STP'  THEN 'ICB'
       ELSE a.BREAKDOWN
       END AS BREAKDOWN,
     COALESCE(a.PRIMARY_LEVEL, 'NONE') AS PRIMARY_LEVEL,
     COALESCE(a.PRIMARY_LEVEL_DESCRIPTION, 'NONE') AS PRIMARY_LEVEL_DESCRIPTION,
     COALESCE(a.SECONDARY_LEVEL, 'NONE') AS SECONDARY_LEVEL,
     COALESCE(a.SECONDARY_LEVEL_DESCRIPTION, 'NONE') AS SECONDARY_LEVEL_DESCRIPTION,
     a.METRIC AS MEASURE_ID,
     a.METRIC_NAME AS MEASURE_NAME,
     CASE 
       -- If present in NumeratorDenominatorProportions, when either denominator or numerator LESS than 5, then suppress result
       WHEN (b.Denominator IS NOT NULL and b.Numerator IS NOT NULL) AND (d.METRIC_VALUE < 5 or c.METRIC_VALUE < 5) THEN '*'
       
       -- If present in NumeratorDenominatorProportions, when either denominator and numerator MORE than 5, then round to nearest integer
       WHEN (b.Denominator IS NOT NULL and b.Numerator IS NOT NULL) AND (d.METRIC_VALUE >= 5 and c.METRIC_VALUE >= 5) THEN CAST(ROUND(CAST(a.METRIC_VALUE AS FLOAT), 0) AS INT)
       
       -- If present in NumeratorDenominatorProportions, when either denominator or numerator is NULL, then suppress result
       WHEN (b.Denominator IS NOT NULL And b.Numerator IS NOT NULL) AND (d.METRIC_VALUE IS NULL or c.METRIC_VALUE IS NULL) THEN '*'
 
       -- If NOT present in NumeratorDenominatorProportions, if value is LESS than 5, then suppress
       WHEN a.METRIC_VALUE < 5 THEN '*'
       
       -- If NOT present in NumeratorDenominatorProportions, if value is MORE than 5, round to nearest 5
       WHEN a.METRIC_VALUE >= 5 THEN CAST(ROUND(a.METRIC_VALUE / 5.0, 0) * 5 AS INT)
 
       -- If NOT present in NumeratorDenominatorProportions, if value is NULL, then suppress
       WHEN a.METRIC_VALUE IS NULL THEN '*'
 
       -- This impossible fallback proves that the WHEN clauses above are logically complete
       ELSE 'Error: No value returned, because rounding logic is logically incomplete!'
     END AS MEASURE_VALUE,
     CURRENT_TIMESTAMP() AS DATETIME_INSERTED,
     a.SOURCE_DB
 FROM $db_output.all_products_cached a
 left join global_temp.NumeratorDenominatorProportions b on a.METRIC = b.METRIC 
 left join $db_output.all_products_cached c on b.Numerator = c.METRIC and a.PRIMARY_LEVEL = c.PRIMARY_LEVEL and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL and a.STATUS = c.STATUS
 left join $db_output.all_products_cached d on b.Denominator = d.METRIC and a.PRIMARY_LEVEL = d.PRIMARY_LEVEL and a.SECONDARY_LEVEL = d.SECONDARY_LEVEL and a.STATUS = d.STATUS
 WHERE a.PRIMARY_LEVEL <> 'England'

# COMMAND ----------

# DBTITLE 1,7. Do not round Ascof, just fill in 'NONE' - If Provisional - don't run this...
# only needs to run if data are final

status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output")
rp_enddate = dbutils.widgets.get("rp_enddate")

# original (no SOURCE_DB) code
# sql = "INSERT INTO {db_output}.Ascof_formatted SELECT 7 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS,  BREAKDOWN,    COALESCE(PRIMARY_LEVEL, 'NONE') AS PRIMARY_LEVEL, COALESCE(PRIMARY_LEVEL_DESCRIPTION, 'NONE') AS PRIMARY_LEVEL_DESCRIPTION,    COALESCE(SECONDARY_LEVEL, 'NONE') AS SECONDARY_LEVEL, COALESCE(SECONDARY_LEVEL_DESCRIPTION, 'NONE') AS SECONDARY_LEVEL_DESCRIPTION,    COALESCE(THIRD_LEVEL, 'NONE') AS THIRD_LEVEL, COALESCE(THIRD_LEVEL_DESCRIPTION, 'NONE') AS THIRD_LEVEL_DESCRIPTION, METRIC AS MEASURE_ID,    METRIC_NAME AS MEASURE_NAME, COALESCE(METRIC_VALUE, '0') AS MEASURE_VALUE, CURRENT_TIMESTAMP() AS DATETIME_INSERTED FROM global_temp.Ascof_expanded".format(db_output=db_output)

# code below includes SOURCE_DB 
sql = "INSERT INTO {db_output}.Ascof_formatted SELECT 7 AS PRODUCT_NO, REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS,  BREAKDOWN,    COALESCE(PRIMARY_LEVEL, 'NONE') AS PRIMARY_LEVEL, COALESCE(PRIMARY_LEVEL_DESCRIPTION, 'NONE') AS PRIMARY_LEVEL_DESCRIPTION,    COALESCE(SECONDARY_LEVEL, 'NONE') AS SECONDARY_LEVEL, COALESCE(SECONDARY_LEVEL_DESCRIPTION, 'NONE') AS SECONDARY_LEVEL_DESCRIPTION,    COALESCE(THIRD_LEVEL, 'NONE') AS THIRD_LEVEL, COALESCE(THIRD_LEVEL_DESCRIPTION, 'NONE') AS THIRD_LEVEL_DESCRIPTION, METRIC AS MEASURE_ID,    METRIC_NAME AS MEASURE_NAME, COALESCE(METRIC_VALUE, '0') AS MEASURE_VALUE, CURRENT_TIMESTAMP() AS DATETIME_INSERTED, SOURCE_DB FROM global_temp.Ascof_expanded".format(db_output=db_output)

if status != 'Provisional':
  print(status)
  print(sql)
  spark.sql(sql).collect()
  spark.sql('OPTIMIZE {db_output}.{table} WHERE REPORTING_PERIOD_END = "{rp_enddate}" AND STATUS = "{status}"'.format(db_output=db_output, table='Ascof_formatted', rp_enddate=rp_enddate, status=status))
else:
  print(status)

# COMMAND ----------

# DBTITLE 1,Optimize formatted output tables (again) for performance
 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 status = dbutils.widgets.get("status")
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table} WHERE REPORTING_PERIOD_END = "{rp_enddate}" AND STATUS = "{status}"'.format(db_output=db_output, table='All_products_formatted', rp_enddate=rp_enddate, status=status))
   