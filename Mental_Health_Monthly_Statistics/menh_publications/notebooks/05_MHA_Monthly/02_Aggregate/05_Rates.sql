-- Databricks notebook source
-- DBTITLE 1,Join MHS81 and MHS84 to population data
CREATE OR REPLACE GLOBAL TEMP VIEW MHA_POPULATIONS AS
SELECT mha.*
      ,pop.POPULATION_ID
      ,pop.BREAKDOWN as POP_BREAKDOWN
      ,pop.SOURCE_TABLE AS POP_SOURCE
      ,pop.PRIMARY_LEVEL AS POP_PRIMARY_LEVEL
      ,pop.PRIMARY_LEVEL_DESC AS POP_PRIMARY_LEVEL_DESC
      ,pop.SECONDARY_LEVEL AS POP_SECONDARY_LEVEL
      ,pop.SECONDARY_LEVEL_DESC AS POP_SECONDARY_LEVEL_DESC
      ,pop.METRIC_VALUE AS POPULATION
      ,CONCAT(METRIC, 'a') AS METRIC_RATE
      ,COALESCE(mha.METRIC_VALUE/pop.METRIC_VALUE, 0)*100000 AS METRIC_RATE_VALUE
FROM ( SELECT * FROM $db_output.mha_monthly_unformatted WHERE METRIC IN ('MHS81', 'MHS84') ) mha
LEFT JOIN $db_output.population_breakdowns pop ON mha.BREAKDOWN = pop.BREAKDOWN AND mha.PRIMARY_LEVEL = pop.PRIMARY_LEVEL and mha.SECONDARY_LEVEL = pop.SECONDARY_LEVEL

-- COMMAND ----------

-- DBTITLE 1,MHS81a and MHS84a (rate per 100,000)
INSERT INTO $db_output.mha_monthly_unformatted
SELECT
      UNIQMONTHID
      ,REPORTING_PERIOD_START
      ,REPORTING_PERIOD_END
      ,STATUS
      ,BREAKDOWN
      ,PRIMARY_LEVEL
      ,PRIMARY_LEVEL_DESCRIPTION
      ,SECONDARY_LEVEL
      ,SECONDARY_LEVEL_DESCRIPTION
      ,METRIC_RATE AS METRIC
      ,METRIC_RATE_VALUE AS METRIC_VALUE
      ,SOURCE_DB
FROM global_temp.MHA_POPULATIONS
WHERE 
(METRIC_RATE == 'MHS81a' AND BREAKDOWN IN ("England", "England; Age", "England; Ethnicity", "England; Gender", "England; IMD Decile", "CCG - GP Practice or Residence", "CCG - GP Practice or Residence; Age", "CCG - GP Practice or Residence; Ethnicity", 
"CCG - GP Practice or Residence; Gender", "CCG - GP Practice or Residence; IMD Decile"))
OR
(METRIC_RATE == 'MHS84a' AND BREAKDOWN IN ("England", "England; Age", "England; Ethnicity", "England; Gender", "England; IMD Decile"))