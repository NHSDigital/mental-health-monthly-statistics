# Databricks notebook source
 %md
 #MHSDS OAPS
 ##Master Notebook
 ###Update Widgets above and then Run All

# COMMAND ----------

# %sql
# SELECT DISTINCT Uniqmonthid, ReportingPeriodEndDate from $reference_data.mhs000header ORDER BY 1 DESC

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]

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

params = {'rp_startdate_1m': str(rp_startdate_1m),'rp_startdate_12m': str(rp_startdate_12m), 'rp_startdate_qtr': str(rp_startdate_qtr), 'rp_enddate': str(rp_enddate), 'start_month_id': start_month_id, 'end_month_id': end_month_id, 'db_output': db_output, 'db_source': db_source, 'status': status}
print(params)

# COMMAND ----------

 %md
 ##Generic Preparation

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.notebook.run('MHSDS OAPS - 1 Create Tables', 0, params)
dbutils.notebook.run('MHSDS OAPS - 2 Generic Prep', 0, params)

# COMMAND ----------

 %md
 ##Run Measures

# COMMAND ----------

dbutils.notebook.run('MHSDS OAPS - 3 Create Measures', 0, params)

# COMMAND ----------

 %md
 ##Generate Outputs

# COMMAND ----------

dbutils.notebook.run('MHSDS OAPS - 4 Create Master CSV List', 0, params)


# COMMAND ----------

dbutils.notebook.run('MHSDS OAPS - 90 Outputs', 0, params)

# COMMAND ----------

# DBTITLE 1,Download Raw Output from here
 %sql
 SELECT DISTINCT *
 FROM $db_output.oaps_output_raw
 ORDER BY  METRIC,
           REPORTING_PERIOD_START DESC,
           CASE WHEN BREAKDOWN = 'England' THEN '0' ELSE BREAKDOWN END, 
           CASE WHEN PRIMARY_LEVEL = 'UNKNOWN' OR PRIMARY_LEVEL = 'Unknown' THEN 'ZZZZZZZZ' ELSE PRIMARY_LEVEL END,
           CASE WHEN SECONDARY_LEVEL = 'UNKNOWN' OR SECONDARY_LEVEL = 'Unknown' THEN 'ZZZZZZZZ' ELSE SECONDARY_LEVEL END

# COMMAND ----------

# DBTITLE 1,Download Suppressed Output from here
 %sql
 SELECT DISTINCT *
 FROM $db_output.oaps_output_sup
 ORDER BY  METRIC,
           REPORTING_PERIOD_START DESC,
           CASE WHEN BREAKDOWN = 'England' THEN '0' ELSE BREAKDOWN END, 
           CASE WHEN PRIMARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE PRIMARY_LEVEL END,
           CASE WHEN SECONDARY_LEVEL = 'UNKNOWN' THEN 'ZZZZZZZZ' ELSE SECONDARY_LEVEL END

# COMMAND ----------

# %sql
# SELECT *
# FROM $db_output.oaps_output_raw
# WHERE Breakdown = 'Sending Provider'
# ORDER BY METRIC, REPORTING_PERIOD_START

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "oaps_output_raw",
  "suppressed_table": "oaps_output_sup"
}))