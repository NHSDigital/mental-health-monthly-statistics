# Databricks notebook source
 %md
 #MHSDS Monthly - Additional Breakdowns
 ##Master Notebook
 ###Update Widgets above and then Run All
 WARNING: This notebook has been adjusted so that it only produces MHS27a, MHS32d and MHS57c. To add in the other additional breakdowns, uncomment the dbutils lines in cmd 7 which are commented out and in cmd 11 and 12 remove the metric filters.

# COMMAND ----------

startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from mh_v5_pre_clear.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from mh_v5_pre_clear.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from mh_v5_pre_clear.mhs000header order by Uniqmonthid").collect()]

dbutils.widgets.dropdown("rp_startdate_1m", "2020-04-01", startchoices)
dbutils.widgets.dropdown("rp_startdate_12m", "2020-04-01", startchoices)
dbutils.widgets.dropdown("rp_startdate_qtr", "2021-03-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
dbutils.widgets.dropdown("start_month_id", "1441", monthid)
dbutils.widgets.dropdown("end_month_id", "1452", monthid)
dbutils.widgets.text("status","Provisional")
dbutils.widgets.text("db_output","")
dbutils.widgets.text("db_source","mh_pre_clear")

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
params = {'rp_startdate_1m': str(rp_startdate_1m),'rp_startdate_12m': str(rp_startdate_12m), 'rp_startdate_qtr': str(rp_startdate_qtr), 'rp_enddate': str(rp_enddate), 'start_month_id': start_month_id, 'end_month_id': end_month_id, 'db_output': db_output, 'db_source': db_source, 'status': status}
print(params)

# COMMAND ----------

 %md
 ##Generic Preparation

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Generic Prep', 0, params)
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Ref Data Load', 0, params)

# COMMAND ----------

 %md
 ##Run Measures

# COMMAND ----------

# DBTITLE 1,Measures
# MHS01
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS01', 0, params)

# MHS07
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS07', 0, params)

# MHS23 based measures - MHS23d&e
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS23', 0, params)

# MHS27 based measures - MHS27 Bed Type Breakdown
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS27', 0, params)

# MHS29 based measures - MHS29 and MHS29d&e, MHS29a
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS29', 0, params)

# MHS30 based measures - MHS30f,g,h&i, a
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS30', 0, params)

# MHS32 based measures - MHS32 and MHS32c&d
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS32', 0, params)

# MHS57 based measures - MHS57b&c
dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - MHS57', 0, params)


# COMMAND ----------

 %md
 ##Extract Output Data

# COMMAND ----------

dbutils.notebook.run('MHSDS Monthly - Additional Breakdowns - Outputs', 0, params)

# COMMAND ----------

# DBTITLE 1,This output should be saved as Monthly_File_Additional_MMM_YYYY_Status_RAW.csv
 %sql
 Select distinct * from $db_output.output_unsuppressed
 --WHERE measure_id in ('MHS57b')
 order by measure_id, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,This output should be saved as Monthly_File_Additional_MMM_YYYY_Status.csv
 %sql
 Select distinct * from $db_output.output_suppressed_final_1 
 --WHERE measure_id in ('MHS57b')
 order by measure_id, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END , primary_level, secondary_level

# COMMAND ----------

 %sql
 Select measure_id, Breakdown, SUM(Measure_Value) as Measure_Sum
 from $db_output.output_unsuppressed
 group by measure_id, breakdown
 order by measure_id, CASE WHEN breakdown = 'England' THEN '0' WHEN breakdown like 'England%' THEN '1' ELSE breakdown END-- , primary_level, secondary_level

# COMMAND ----------

# DBTITLE 1,Test for Automated Sense checks
import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "output_unsuppressed",
  "suppressed_table": "output_suppressed_final_1"
}))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))