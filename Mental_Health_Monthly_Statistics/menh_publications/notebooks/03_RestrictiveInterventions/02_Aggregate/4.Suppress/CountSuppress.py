# Databricks notebook source
# DBTITLE 1,Restrictive Intervention Provider
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
status=dbutils.widgets.get("status")
print(status)
assert status
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source

# COMMAND ----------

# DBTITLE 1,Count suppression
 %sql

 Insert into $db_output.MHSRestrictiveInterventionCountSuppressed
 SELECT  REPORTING_PERIOD_START
        ,REPORTING_PERIOD_END
        ,Status
        ,BREAKDOWN
        ,PRIMARY_LEVEL
        ,PRIMARY_LEVEL_DESCRIPTION
        ,SECONDARY_LEVEL
        ,SECONDARY_LEVEL_DESCRIPTION
        ,TERTIARY_LEVEL
        ,TERTIARY_LEVEL_DESCRIPTION
        ,QUARTERNARY_LEVEL
        ,QUARTERNARY_LEVEL_DESCRIPTION
        ,MEASURE_ID
        ,MEASURE_NAME
        ,case WHEN PRIMARY_LEVEL = 'England' THEN CAST(MEASURE_VALUE AS STRING)
          WHEN BREAKDOWN = 'Provider type' OR BREAKDOWN = 'Provider type; Restrictive intervention type' THEN CAST(MEASURE_VALUE AS STRING)
          WHEN COALESCE(MEASURE_VALUE, 0) < 5 THEN '*'
          ELSE CAST(CAST(ROUND(MEASURE_VALUE / 5.0, 0) * 5 AS int) AS STRING)
          END AS MEASURE_VALUE
        ,UniqMonthID
        ,current_timestamp() as CreatedAt
        ,SOURCE_DB
   FROM $db_output.MHSRestrictiveInterventionCount
   WHERE UniqMonthID = '$month_id'
   AND STATUS = '$status'
   AND SOURCE_DB = '$db_source';
