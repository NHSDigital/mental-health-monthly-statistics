-- Databricks notebook source
 %py
 db_output=dbutils.widgets.get("db_output")
 print(db_output)
 assert db_output
 db_source=dbutils.widgets.get("db_source")
 print(db_source)
 assert db_source
 $reference_data=dbutils.widgets.get("$reference_data")
 print($reference_data)
 assert $reference_data
 status=dbutils.widgets.get("status")
 print(status)
 assert status
 rp_startdate=dbutils.widgets.get("rp_startdate")
 print(rp_startdate)
 assert rp_startdate
 rp_enddate=dbutils.widgets.get("rp_enddate")
 print(rp_enddate)
 assert rp_enddate
 month_id=dbutils.widgets.get("month_id")
 print(month_id)
 assert month_id

-- COMMAND ----------

-- DBTITLE 1,MHS82 - National level
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'England' AS BREAKDOWN,
    'England' AS PRIMARY_LEVEL,
    'England' AS PRIMARY_LEVEL_DESCRIPTION,
    'NONE' AS SECONDARY_LEVEL,
    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS82' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Short_Term_Orders_monthly A
WHERE MHA_Logic_Cat_full in ('E','F')


-- COMMAND ----------

-- DBTITLE 1,MHS82 - Provider level
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'Provider' AS BREAKDOWN,
    OrgIDProv AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    'NONE' AS SECONDARY_LEVEL,
    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS82' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Short_Term_Orders_monthly A
WHERE MHA_Logic_Cat_full in ('E','F')
GROUP BY ORGIDPROV, NAME


-- COMMAND ----------

-- DBTITLE 1,MHS82 - CCG level
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'CCG - GP Practice or Residence' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
    'NONE' AS SECONDARY_LEVEL,
    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS82' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Short_Term_Orders_monthly A
WHERE MHA_Logic_Cat_full in ('E','F')
GROUP BY IC_REC_CCG, CCG_NAME