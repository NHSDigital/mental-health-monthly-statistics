# Databricks notebook source
 %md
 
 # Round METRIC_VALUE and fill in 'NONE' wherever no value
 
 Suppression Rules are documented through inline comments in SQL below.

# COMMAND ----------

db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
status=dbutils.widgets.get("status")
print(status)
assert status
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source

# COMMAND ----------

# DBTITLE 1,Numerator / Denominator proportions
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW NumeratorDenominatorProportions AS
 SELECT 'MHS80' AS METRIC,'MHS79' AS Numerator,'MHS78' AS Denominator UNION
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
 SELECT 'EIP67c' AS METRIC, 'EIP66c' AS Numerator, 'EIP64c' AS Denominator UNION
 SELECT 'MHS94' AS METRIC, 'MHS92' AS Numerator, 'MHS93' AS Denominator
 
 -- Adding these in to resolve suppression issues with proportion measures
 UNION
 SELECT 'ED86e' AS METRIC, 'ED86a' AS Numerator, 'ED86' AS Denominator
 
 -- how can we get both ED87a and ED87b into this table as the Numerator without breaking everything???
 -- adding in as 2 separate rows doesn't work! - leads to 2 separate outputs for ED87e
 -- could we keep as 2 separate rows and then further process the output - RANK?
 -- this is the method used below... Further step introduced later in the code
 
 UNION
 SELECT 'ED87e' AS METRIC, 'ED87a' AS Numerator, 'ED87' AS Denominator
 UNION
 SELECT 'ED87e' AS METRIC, 'ED87b' AS Numerator, 'ED87' AS Denominator
 
 --ADD EXTRA ROWS FOR NEW PROPORTION METRICS
 
 -- the alternative of generating an unpublished measure to represent the Numerator as a single figure doesn't check both parts...
 -- UNION
 -- SELECT 'ED87e' AS METRIC, 'ED87xtra' AS Numerator, 'ED87' AS Denominator
 --  ED87e =  (ED87a + ED87b) / ED87
 -- ED87xtra = ED87a + ED87b
 
 --Additions for the new CYPED metrics
 UNION
 SELECT 'ED86e' AS METRIC, 'ED86a' AS Numerator, 'ED86' AS Denominator
 UNION
 SELECT 'ED86f' AS METRIC, 'ED86b' AS Numerator, 'ED86' AS Denominator
 UNION
 SELECT 'ED86g' AS METRIC, 'ED86c' AS Numerator, 'ED86' AS Denominator
 UNION
 SELECT 'ED86h' AS METRIC, 'ED86d' AS Numerator, 'ED86' AS Denominator
 UNION
 SELECT 'ED87f' AS METRIC, 'ED87a' AS Numerator, 'ED87' AS Denominator
 UNION
 SELECT 'ED87g' AS METRIC, 'ED87b' AS Numerator, 'ED87' AS Denominator
 UNION
 SELECT 'ED87h' AS METRIC, 'ED87c' AS Numerator, 'ED87' AS Denominator
 UNION
 SELECT 'ED87i' AS METRIC, 'ED87d' AS Numerator, 'ED87' AS Denominator
 UNION
 SELECT 'ED89e' AS METRIC, 'ED89a' AS Numerator, 'ED89' AS Denominator
 UNION
 SELECT 'ED89f' AS METRIC, 'ED89b' AS Numerator, 'ED89' AS Denominator
 UNION
 SELECT 'ED89g' AS METRIC, 'ED89c' AS Numerator, 'ED89' AS Denominator
 UNION
 SELECT 'ED89h' AS METRIC, 'ED89d' AS Numerator, 'ED89' AS Denominator
 UNION
 SELECT 'ED90e' AS METRIC, 'ED90a' AS Numerator, 'ED90' AS Denominator
 UNION
 SELECT 'ED90f' AS METRIC, 'ED90b' AS Numerator, 'ED90' AS Denominator
 UNION
 SELECT 'ED90g' AS METRIC, 'ED90c' AS Numerator, 'ED90' AS Denominator
 UNION
 SELECT 'ED90h' AS METRIC, 'ED90d' AS Numerator, 'ED90' AS Denominator

# COMMAND ----------

# DBTITLE 1,1-5. Format most products (for England figures)
 %sql
 
 INSERT INTO $db_output.All_products_formatted
 SELECT DISTINCT
     MONTH_ID,
     PRODUCT_NO,
     date_format(REPORTING_PERIOD_START, "y-M-dd") AS REPORTING_PERIOD_START, -- Spark 3 modification
     date_format(REPORTING_PERIOD_END, "y-M-dd") AS REPORTING_PERIOD_END, -- Spark 3 modification
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
 AND MONTH_ID = '$month_id'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,1-5. Round and format most products (for Sub-England figures)
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW All_products_formatted_prep3 AS
 SELECT DISTINCT
     a.MONTH_ID,
     a.PRODUCT_NO,
     a.REPORTING_PERIOD_START,
     a.REPORTING_PERIOD_END,
     a.STATUS, 
         CASE
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence'  THEN 'Sub ICB - GP Practice or Residence'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; Provider of Responsibility'  THEN 'Sub ICB - GP Practice or Residence; Provider of Responsibility'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'CCG - GP Practice or Residence; Ethnicity'  THEN 'Sub ICB - GP Practice or Residence; Ethnicity'
       WHEN a.REPORTING_PERIOD_END > '2022-07-01' AND a.BREAKDOWN = 'STP - GP Practice or Residence'  THEN 'ICB - GP Practice or Residence'
       ELSE a.BREAKDOWN
       END AS BREAKDOWN,
       
     a.PRIMARY_LEVEL,
     a.PRIMARY_LEVEL_DESCRIPTION,
     a.SECONDARY_LEVEL,
     a.SECONDARY_LEVEL_DESCRIPTION,
     a.METRIC AS MEASURE_ID,
     a.METRIC_NAME as MEASURE_NAME,
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
 left join $db_output.all_products_cached c 
   on b.Numerator = c.METRIC and a.PRIMARY_LEVEL = c.PRIMARY_LEVEL and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL and a.STATUS = c.STATUS and a.MONTH_ID = c.MONTH_ID 
   and a.SOURCE_DB = c.SOURCE_DB and a.BREAKDOWN = c.BREAKDOWN 
 left join $db_output.all_products_cached d 
   on b.Denominator = d.METRIC and a.PRIMARY_LEVEL = d.PRIMARY_LEVEL and a.SECONDARY_LEVEL = d.SECONDARY_LEVEL and a.STATUS = d.STATUS and a.MONTH_ID = d.MONTH_ID 
   and a.SOURCE_DB = d.SOURCE_DB and a.BREAKDOWN = d.BREAKDOWN 
 WHERE a.PRIMARY_LEVEL <> 'England'
 AND a.MONTH_ID = '$month_id'
 AND a.STATUS = '$status'
 AND a.SOURCE_DB = '$db_source'

# COMMAND ----------

 %sql
 
 --Re-introduced as there are still 2 rows coming out for ED87e in the rounded outputs
 
 CREATE OR REPLACE GLOBAL TEMP VIEW All_products_formatted_prep4 AS
 
 
 SELECT *,
 ROW_NUMBER () OVER(PARTITION BY BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, MEASURE_ID ORDER BY MEASURE_VALUE DESC) AS RANK
 FROM global_temp.All_products_formatted_prep3

# COMMAND ----------

 %sql
 
 INSERT INTO $db_output.All_products_formatted
 
 SELECT 
     MONTH_ID,
     PRODUCT_NO,
     REPORTING_PERIOD_START,
     REPORTING_PERIOD_END,
     STATUS,
     BREAKDOWN,
     PRIMARY_LEVEL,
     PRIMARY_LEVEL_DESCRIPTION,
     SECONDARY_LEVEL,
     SECONDARY_LEVEL_DESCRIPTION,
     MEASURE_ID,
     MEASURE_NAME,
     MEASURE_VALUE,
     DATETIME_INSERTED,
     SOURCE_DB
     
 FROM global_temp.All_products_formatted_prep4
 WHERE RANK = 1
 
 ORDER BY MEASURE_ID, BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL
     

# COMMAND ----------

# DBTITLE 1,Format products with THIRD_LEVEL breakdowns (for England figures)
 %sql
 
 INSERT INTO $db_output.third_level_products_formatted
 SELECT DISTINCT
     MONTH_ID,
     PRODUCT_NO,
     REPORTING_PERIOD_START,
     REPORTING_PERIOD_END,
     STATUS,
     BREAKDOWN,
     COALESCE(PRIMARY_LEVEL, 'NONE') AS PRIMARY_LEVEL,
     COALESCE(PRIMARY_LEVEL_DESCRIPTION, 'NONE') AS PRIMARY_LEVEL_DESCRIPTION,
     COALESCE(SECONDARY_LEVEL, 'NONE') AS SECONDARY_LEVEL,
     COALESCE(SECONDARY_LEVEL_DESCRIPTION, 'NONE') AS SECONDARY_LEVEL_DESCRIPTION,
     COALESCE(THIRD_LEVEL , 'NONE') AS THIRD_LEVEL,
     METRIC AS MEASURE_ID,
     METRIC_NAME AS MEASURE_NAME,
     COALESCE(METRIC_VALUE, '0') AS MEASURE_VALUE, -- England level stays unrounded
     CURRENT_TIMESTAMP() AS DATETIME_INSERTED,
     SOURCE_DB
 FROM $db_output.third_level_products_cached
 WHERE BREAKDOWN = 'England' 
 -- WARNING: Main code restricts on PRIMARY_LEVEL, cos BREAKDOWN does not work for some reason - trying BREAKDOWN here to determine if this is really a thing
 AND MONTH_ID = '$month_id'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,Round and format products with THIRD_LEVEL breakdowns (for Sub-England figures)
 %sql
 
 INSERT INTO $db_output.third_level_products_formatted
 
 SELECT DISTINCT
     a.MONTH_ID,
     a.PRODUCT_NO,
     a.REPORTING_PERIOD_START,
     a.REPORTING_PERIOD_END,
     a.STATUS,
     a.BREAKDOWN,
     a.PRIMARY_LEVEL,
     a.PRIMARY_LEVEL_DESCRIPTION,
     a.SECONDARY_LEVEL,
     a.SECONDARY_LEVEL_DESCRIPTION,
     a.THIRD_LEVEL,
     a.METRIC AS MEASURE_ID,
     a.METRIC_NAME as MEASURE_NAME,
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
     
 FROM $db_output.third_level_products_cached a
 left join global_temp.NumeratorDenominatorProportions b on a.METRIC = b.METRIC 
 left join $db_output.third_level_products_cached c 
   on b.Numerator = c.METRIC and a.PRIMARY_LEVEL = c.PRIMARY_LEVEL and a.SECONDARY_LEVEL = c.SECONDARY_LEVEL and a.THIRD_LEVEL = c.THIRD_LEVEL 
   and a.STATUS = c.STATUS and a.MONTH_ID = c.MONTH_ID and a.SOURCE_DB = c.SOURCE_DB
 left join $db_output.third_level_products_cached d 
   on b.Denominator = d.METRIC and a.PRIMARY_LEVEL = d.PRIMARY_LEVEL and a.SECONDARY_LEVEL = d.SECONDARY_LEVEL and a.THIRD_LEVEL = d.THIRD_LEVEL 
   and a.STATUS = d.STATUS and a.MONTH_ID = d.MONTH_ID and a.SOURCE_DB = d.SOURCE_DB
 WHERE a.BREAKDOWN <> 'England'
 AND a.MONTH_ID = '$month_id'
 AND a.STATUS = '$status'
 AND a.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,Special case: Renaming RAW ASCOF outputs for Social Care Team
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW ascof_formatted AS 
 SELECT DISTINCT
 
     REPORTING_PERIOD_START AS REPORTING_PERIOD,
     STATUS,
     BREAKDOWN,
     COALESCE(PRIMARY_LEVEL, 'NONE') AS LEVEL_ONE,
     COALESCE(PRIMARY_LEVEL_DESCRIPTION, 'NONE') AS LEVEL_ONE_DESCRIPTION,
     COALESCE(SECONDARY_LEVEL, 'NONE') AS LEVEL_TWO,
     COALESCE(SECONDARY_LEVEL_DESCRIPTION, 'NONE') AS LEVEL_TWO_DESCRIPTION,
     CASE 
       WHEN THIRD_LEVEL = "1" THEN "MALE"
       WHEN THIRD_LEVEL = "2" THEN "FEMALE"
       WHEN THIRD_LEVEL = "9" THEN "INDETERMINATE"
       WHEN THIRD_LEVEL = "X" THEN "NOT KNOWN"
        ELSE "NONE" 
     END AS LEVEL_THREE,
     CASE 
       WHEN METRIC = "AMH03e" THEN "DENOMINATOR"
       WHEN METRIC = "AMH14e" THEN "1H_NUMERATOR"
       WHEN METRIC = "AMH14e%" THEN "1H_OUTCOME"
       WHEN METRIC = "AMH17e" THEN "1F_NUMERATOR"
       WHEN METRIC = "AMH17e%" THEN "1F_OUTCOME"
       ELSE "PROBLEM"
     END AS METRIC,
     COALESCE(METRIC_VALUE, '0') AS METRIC_VALUE 
     
 FROM $db_output.third_level_products_cached
 WHERE MONTH_ID = '$month_id'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 AND PRODUCT_NO = 8