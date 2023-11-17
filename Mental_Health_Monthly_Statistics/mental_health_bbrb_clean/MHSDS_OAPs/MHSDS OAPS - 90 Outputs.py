# Databricks notebook source
 %md
 #MHSDS OAPS
 ##Outputs Notebook

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $mhsds_database.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $mhsds_database.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $mhsds_database.mhs000header order by Uniqmonthid").collect()]

# dbutils.widgets.dropdown("rp_startdate_1m", "2021-05-01", startchoices)
# dbutils.widgets.dropdown("rp_enddate", "2021-05-31", endchoices)
# dbutils.widgets.dropdown("rp_startdate_qtr", "2021-03-01", startchoices)
# dbutils.widgets.dropdown("rp_startdate_12m", "2020-06-01", startchoices)
# dbutils.widgets.dropdown("start_month_id", "1454", monthid)
# dbutils.widgets.dropdown("end_month_id", "1454", monthid)
# dbutils.widgets.text("db_output","$user_id")
# dbutils.widgets.text("db_source","$db_source")
# dbutils.widgets.text("status","Final")

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
 CROSS JOIN
   (
   SELECT DISTINCT BREAKDOWN,
                   LEVEL_ONE,
                   LEVEL_ONE_DESCRIPTION,
                   LEVEL_TWO,
                   LEVEL_TWO_DESCRIPTION,
                   LEVEL_THREE,
                   LEVEL_THREE_DESCRIPTION
   FROM $db_output.oaps_output
   ) AS b
 --    ON a.REPORTING_PERIOD_START = b.REPORTING_PERIOD_START
 CROSS JOIN
   (
   SELECT DISTINCT METRIC,
                   METRIC_DESCRIPTION
   FROM $db_output.oaps_output
   ) AS c

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.oaps_csv_lookup;
 REFRESH TABLE $db_output.oaps_output;

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW oaps_output_raw_prep AS
 SELECT  a.REPORTING_PERIOD_START,
         a.REPORTING_PERIOD_END,
         a.STATUS,
         a.BREAKDOWN,
         a.PRIMARY_LEVEL,
         a.PRIMARY_LEVEL_DESCRIPTION,
         CASE WHEN a.SECONDARY_LEVEL = 'NULL' THEN 'NONE' ELSE a.SECONDARY_LEVEL END AS SECONDARY_LEVEL,
         CASE WHEN a.SECONDARY_LEVEL_DESCRIPTION = 'NULL' THEN 'NONE' ELSE a.SECONDARY_LEVEL_DESCRIPTION END AS SECONDARY_LEVEL_DESCRIPTION,
         a.METRIC,
         a.METRIC_DESCRIPTION,
         COALESCE(b.METRIC_VALUE,0) as METRIC_VALUE
         FROM $db_output.oaps_csv_lookup AS a
 LEFT JOIN $db_output.oaps_output AS b
    ON a.REPORTING_PERIOD_START=b.REPORTING_PERIOD_START
   AND a.REPORTING_PERIOD_END=b.REPORTING_PERIOD_END
   AND a.BREAKDOWN=b.BREAKDOWN
   AND a.PRIMARY_LEVEL=b.LEVEL_ONE
   AND a.SECONDARY_LEVEL=b.LEVEL_TWO
   AND a.METRIC=b.METRIC
 ORDER BY  a.METRIC,
           a.REPORTING_PERIOD_START DESC,
           CASE WHEN a.BREAKDOWN = 'England' THEN '0' ELSE a.BREAKDOWN END, 
           CASE WHEN a.PRIMARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.PRIMARY_LEVEL END,
           CASE WHEN a.SECONDARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.SECONDARY_LEVEL END

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_output_raw
 
 SELECT REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION,  SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_DESCRIPTION, SUM(METRIC_VALUE) AS METRIC_VALUE
 
 FROM global_temp.oaps_output_raw_prep
 
 GROUP BY REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN, PRIMARY_LEVEL, PRIMARY_LEVEL_DESCRIPTION, SECONDARY_LEVEL, SECONDARY_LEVEL_DESCRIPTION, METRIC, METRIC_DESCRIPTION

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
 --         CASE WHEN a.LEVEL_THREE = 'NULL' THEN 'NONE' ELSE a.LEVEL_THREE END AS LEVEL_THREE,
 --         CASE WHEN a.LEVEL_THREE_DESCRIPTION = 'NULL' THEN 'NONE' ELSE a.LEVEL_THREE_DESCRIPTION END AS LEVEL_THREE_DESCRIPTION,
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
 --         CASE WHEN a.LEVEL_THREE = 'NULL' THEN 'NONE' ELSE a.LEVEL_THREE END AS LEVEL_THREE,
 --         CASE WHEN a.LEVEL_THREE_DESCRIPTION = 'NULL' THEN 'NONE' ELSE a.LEVEL_THREE_DESCRIPTION END AS LEVEL_THREE_DESCRIPTION,
         a.METRIC,
         a.METRIC_DESCRIPTION,
         a.METRIC_VALUE
 FROM $db_output.oaps_output_raw AS a
 WHERE BREAKDOWN IN ('England','Age Group','Ethnicity','Gender','IMD','Primary reason for referral','Bed Type')
 ORDER BY  a.METRIC,
           a.REPORTING_PERIOD_START DESC,
           CASE WHEN a.BREAKDOWN = 'England' THEN '0' ELSE a.BREAKDOWN END, 
           CASE WHEN a.PRIMARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.PRIMARY_LEVEL END,
           CASE WHEN a.SECONDARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.SECONDARY_LEVEL END-- ,
 --           CASE WHEN a.LEVEL_THREE = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE a.LEVEL_THREE END