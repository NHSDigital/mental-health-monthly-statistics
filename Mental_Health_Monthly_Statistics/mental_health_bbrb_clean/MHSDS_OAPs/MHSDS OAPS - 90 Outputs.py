# Databricks notebook source
 %md
 #MHSDS OAPS
 ##Outputs Notebook

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
status  = dbutils.widgets.get("status")

# COMMAND ----------

 %sql
 --GET ALL COMBINATIONS OF REPORTING PERIODS, BREAKDOWNS AND LEVEL VALUES AND METRICS TO CREATE COMPLETE LIST
 CREATE OR REPLACE TEMPORARY VIEW masterlist AS
 SELECT  a.REPORTING_PERIOD_START,
         a.REPORTING_PERIOD_END,
         a.STATUS,
         b.BREAKDOWN,
         b.LEVEL_ONE,
         b.LEVEL_ONE_DESCRIPTION,
         b.LEVEL_TWO,
         b.LEVEL_TWO_DESCRIPTION,
         b.LEVEL_THREE,
         b.LEVEL_THREE_DESCRIPTION,
         c.METRIC,
         c.METRIC_DESCRIPTION
 FROM
   (
   SELECT DISTINCT REPORTING_PERIOD_START,
                   REPORTING_PERIOD_END,
                   STATUS
   FROM $db_output.oaps_output
   ) AS a
 INNER JOIN
   (
   SELECT DISTINCT REPORTING_PERIOD_START,
                   BREAKDOWN,
                   LEVEL_ONE,
                   LEVEL_ONE_DESCRIPTION,
                   LEVEL_TWO,
                   LEVEL_TWO_DESCRIPTION,
                   LEVEL_THREE,
                   LEVEL_THREE_DESCRIPTION
   FROM $db_output.oaps_output
   ) AS b
    ON a.REPORTING_PERIOD_START = b.REPORTING_PERIOD_START
 CROSS JOIN
   (
   SELECT DISTINCT METRIC,
                   METRIC_DESCRIPTION
   FROM $db_output.oaps_output
   ) AS c

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_output_raw
 SELECT  a.REPORTING_PERIOD_START,
         a.REPORTING_PERIOD_END,
         a.STATUS,
         a.BREAKDOWN,
         a.LEVEL_ONE,
         a.LEVEL_ONE_DESCRIPTION,
         CASE WHEN a.LEVEL_TWO = 'NULL' THEN 'NONE' ELSE a.LEVEL_TWO END AS LEVEL_TWO,
         CASE WHEN a.LEVEL_TWO_DESCRIPTION = 'NULL' THEN 'NONE' ELSE a.LEVEL_TWO_DESCRIPTION END AS LEVEL_TWO_DESCRIPTION,
         a.METRIC,
         a.METRIC_DESCRIPTION,
         COALESCE(b.METRIC_VALUE,0) as METRIC_VALUE
 FROM masterlist AS a
 LEFT JOIN $db_output.oaps_output AS b
    ON a.REPORTING_PERIOD_START=b.REPORTING_PERIOD_START
   AND a.REPORTING_PERIOD_END=b.REPORTING_PERIOD_END
   AND a.BREAKDOWN=b.BREAKDOWN
   AND a.LEVEL_ONE=b.LEVEL_ONE
   AND a.LEVEL_TWO=b.LEVEL_TWO
   AND a.LEVEL_THREE=b.LEVEL_THREE
   AND a.METRIC=b.METRIC
 ORDER BY  a.METRIC,
           a.REPORTING_PERIOD_START DESC,
           CASE WHEN a.BREAKDOWN = 'England' THEN '0' ELSE a.BREAKDOWN END, 
           CASE WHEN a.LEVEL_ONE = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.LEVEL_ONE END,
           CASE WHEN a.LEVEL_TWO = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.LEVEL_TWO END

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_output_sup
 SELECT  a.REPORTING_PERIOD_START,
         a.REPORTING_PERIOD_END,
         a.STATUS,
         a.BREAKDOWN,
         a.PRIMARY_LEVEL,
         a.PRIMARY_LEVEL_DESCRIPTION,
         a.SECONDARY_LEVEL,
         a.SECONDARY_LEVEL_DESCRIPTION,
         a.METRIC,
         a.METRIC_DESCRIPTION,
         CASE WHEN CAST(METRIC_VALUE AS INT) < 5 THEN '*' ELSE ROUND(CAST(METRIC_VALUE AS INT)/5,0)*5 END as METRIC_VALUE
 FROM $db_output.oaps_output_raw AS a
 WHERE BREAKDOWN NOT IN ('England','Age Group','Ethnicity','Gender','IMD','Primary reason for referral','Bed Type')
 UNION ALL
 SELECT  a.REPORTING_PERIOD_START,
         a.REPORTING_PERIOD_END,
         a.STATUS,
         a.BREAKDOWN,
         a.PRIMARY_LEVEL,
         a.PRIMARY_LEVEL_DESCRIPTION,
         a.SECONDARY_LEVEL,
         a.SECONDARY_LEVEL_DESCRIPTION,
         a.METRIC,
         a.METRIC_DESCRIPTION,
         a.METRIC_VALUE
 FROM $db_output.oaps_output_raw AS a
 WHERE BREAKDOWN IN ('England','Age Group','Ethnicity','Gender','IMD','Primary reason for referral','Bed Type')
 ORDER BY  a.METRIC,
           a.REPORTING_PERIOD_START DESC,
           CASE WHEN a.BREAKDOWN = 'England' THEN '0' ELSE a.BREAKDOWN END, 
           CASE WHEN a.PRIMARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.PRIMARY_LEVEL END,
           CASE WHEN a.SECONDARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.SECONDARY_LEVEL END