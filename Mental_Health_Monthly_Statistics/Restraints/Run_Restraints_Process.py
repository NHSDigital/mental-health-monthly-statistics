# Databricks notebook source
 %run ./Restraints_Functions

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

import os

# dbutils.widgets.text("db_output", "personal_db")
db_output  = dbutils.widgets.get("db_output")
assert db_output

# bbrb_status = dbutils.widgets.get("status")

# dbutils.widgets.text("db_source", "mhsds_database")
db_source = dbutils.widgets.get("db_source")
assert db_source

db_source = dbutils.widgets.get("db_source")
statuses = ["Performance"] ###, "Provisional","Performance" "Final"]

# dbutils.widgets.text("end_month_id", "1478")
month_id = dbutils.widgets.get("end_month_id")
assert month_id

# dbutils.widgets.text("rp_enddate", "2023-05-31")
rp_enddate = dbutils.widgets.get("rp_enddate")
assert rp_enddate

# dbutils.widgets.text("rp_startdate", "2023-05-01")
rp_startdate = dbutils.widgets.get("rp_startdate") ##dbutils.widgets.get("rp_startdate_1m")
assert rp_startdate

params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate' : rp_enddate, 'rp_startdate': rp_startdate, 'month_id': month_id}

print(params)

# COMMAND ----------

 %sql
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate from $db_source.mhs000header order by uniqmonthid desc

# COMMAND ----------

# DBTITLE 1,Create Tables - Comment out when running multiple months
dbutils.notebook.run('Create_Restraints_Tables', 0, params)

# COMMAND ----------

# DBTITLE 1,Performance/Provisional Prep and Aggregation
for status in statuses:
  if status in ["Performance", "Final"]:
    run_params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate' : rp_enddate, 'rp_startdate': rp_startdate, 'month_id': month_id, "status": status}
    print(f"{status} Parameters: {run_params}")
  elif status == "Provisional":
    run_params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate' : last_day(add_months(rp_enddate, 1)), 'rp_startdate': add_months(rp_startdate, 1), 'month_id': str(int(month_id) + 1), "status": status}
    print(f"{status} Parameters: {run_params}")
  dbutils.notebook.run('Restraints_Prep', 0, run_params)    
  
  dbutils.notebook.run('Restraints_Published_CSV_Build/Average_Number_of_Restraints_per_Incident', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Incidents_Summary_Agg', 0, run_params)
  
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Summary_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Duration_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Demographics_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Provider_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Injury_Agg', 0, run_params)

  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Summary_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Duration_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Demographics_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Provider_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Injury_Agg', 0, run_params)

  dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Summary_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Duration_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Demographics_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Provider_Agg', 0, run_params)

  dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Summary_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Duration_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Demographics_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Provider_Agg', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Injury_Agg', 0, run_params)

  dbutils.notebook.run('Restraints_Published_CSV_Build/Perc_RIPeople_in_Hosp', 0, run_params)

  dbutils.notebook.run('Restraints_Published_CSV_Build/Average_Minutes_of_Restraint', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Maximum_Minutes_of_Restraint', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Median_Minutes_of_Restraint', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Maximum_Minutes_of_Incident', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Average_Minutes_of_Incident', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Median_Minutes_of_Incident', 0, run_params)
  
  dbutils.notebook.run('Restraints_Published_CSV_Build/Average_Days_of_Active_Seclusion', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Median_Days_of_Active_Seclusion', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Average_Days_of_Active_Segregation', 0, run_params)
  dbutils.notebook.run('Restraints_Published_CSV_Build/Median_Days_of_Active_Segregation', 0, run_params)
  
  if status in ["Performance", "Final"]:
    dbutils.notebook.run('Dash_Restraints_DQ_Coverage', 0, run_params)
#     dbutils.notebook.run('Restraints_Dashboard_CSV_Build_v3a', 0, run_params)
    dbutils.notebook.run('Restraints_Dashboard_CSV_Build_v4', 0, run_params)
  print(f"{status} Aggregation Complete")  

# COMMAND ----------

 %sql
 select distinct OrgIDProv from $db_output.RI_FINAL

# COMMAND ----------

# DBTITLE 1,Run Suppression Code
dbutils.notebook.run('Restraints_Suppression/Dashboard_Main_CSV_Suppression', 0, params)
dbutils.notebook.run('Restraints_Suppression/Dashboard_Perc_CSV_Suppression', 0, params)
dbutils.notebook.run('Restraints_Suppression/Published_CSV_Suppression', 0, params)
#Dashboard DQ CSV is already suppressed during process

# COMMAND ----------

# DBTITLE 1,Import Sense Check Functions
 %run Babbage_BBRB/MHSDS_Functions

# COMMAND ----------

# DBTITLE 1,Unsuppressed Tests
##LW Notes
##If this fails double check the tests to see if its site - if it is then its fine - just download the outputs
unsup_df = spark.table(f"{db_output}.restraints_final_output")

for status in statuses:
  print(status)
  #TEST: BREAKDOWN TOTALS GREATER OR EQUAL TO ENGLAND TOTAL FOR RESTRAINTS
  df_restr = unsup_df[(unsup_df["metric"] == "MHS77") & (unsup_df["status"] == status)]
  restr_breakdowns = df_restr.groupBy("metric", "breakdown").agg(sum("metric_value").alias("TOTAL"))
  test_breakdown_activity_count(restr_breakdowns)
  
  df_inc = unsup_df[(unsup_df["metric"] == "MHS136") & (unsup_df["status"] == status)]
  inc_breakdowns = df_inc.groupBy("metric", "breakdown").agg(sum("metric_value").alias("TOTAL"))
  test_breakdown_people_count(inc_breakdowns) #as we are breaking incidents by restraint type breakdown total will be greater than England total

  df_people = unsup_df[(unsup_df["metric"] == "MHS76") & (unsup_df["status"] == status)]
  people_breakdowns = df_people.groupBy("metric", "breakdown").agg(sum("metric_value").alias("TOTAL"))
  test_breakdown_people_count(people_breakdowns) ##can be test_breakdown_activity_count() when specialised service type ward flag is added

  #TEST: CHECK FOR DUPLICATE ROWS
colstotest = ["status", "breakdown", "level_one", "level_one_description", "level_two", "level_two_description", "level_three", "level_three_description", "level_four", "level_four_description", "level_five", "level_five_description", "level_six", "level_six_description", "metric"]
if unsup_df.count() > unsup_df.select(colstotest).dropDuplicates(colstotest).count():
  display(unsup_df.exceptAll(unsup_df.dropDuplicates(colstotest)).orderBy(colstotest))
  raise ValueError("Unsuppressed data has duplicates. Duplicate rows above")
else:
  print("test_unsuppressed_duplicates: PASSED")

# COMMAND ----------

 %py
 display(restr_breakdowns)

# COMMAND ----------

 %sql
 -- select UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID, count(distinct siteoftreat) from $db_output.RI_FINAL
 -- group by UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID
 -- having count(distinct siteoftreat) > 1

# COMMAND ----------

# DBTITLE 1,Suppressed Tests
sup_df = spark.table(f"{db_output}.supp_pub_csv_final")

#TEST: CHECK FOR DUPLICATE ROWS
colstotest = ["status", "breakdown", "level_one", "level_one_description", "level_two", "level_two_description", "level_three", "level_three_description", "level_four", "level_four_description", "level_five", "level_five_description", "level_six", "level_six_description", "metric"]
if sup_df.count() > sup_df.select(colstotest).dropDuplicates(colstotest).count():
  display(sup_df.exceptAll(sup_df.dropDuplicates(colstotest)).orderBy(colstotest))
  raise ValueError("Suppressed data has duplicates. Duplicate rows above")
else:
  print("test_suppressed_duplicates: PASSED")

# COMMAND ----------

 %sql
 select
 sum(days_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as avg_days_active_segregation,
 percentile(days_of_restraint, 0.5) as median_days_active_segregation
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Segregation" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'

# COMMAND ----------

 %sql
 select
 restrictiveintname,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as avg_days_active_segregation,
 percentile(minutes_of_restraint, 0.5) as median_days_active_segregation
 from $db_output.RI_FINAL
 where person_id is not null and RI_RecordNumber_Type = 1 and avg_min_flag_type = 'Y'
 group by restrictiveintname

# COMMAND ----------

# DBTITLE 1,Performance - Published CSV - Unsuppressed
 %sql
 select * from $db_output.restraints_final_output
 where status = "Performance"
 order by metric, breakdown, level_one, level_two, level_three, level_four, level_five, level_six

# COMMAND ----------

# DBTITLE 1,Provisional - Published CSV - Unsuppressed
 %sql
 select * from $db_output.restraints_final_output
 where status = "Provisional"
 order by metric, breakdown, level_one, level_two, level_three, level_four, level_five, level_six

# COMMAND ----------

# DBTITLE 1,Performance - Published CSV - Suppressed - download
 %sql
 select distinct * from $db_output.supp_pub_csv_final
 where status = "Performance"
 order by metric, breakdown, level_one, level_two, level_three, level_four, level_five, level_six

# COMMAND ----------

# DBTITLE 1,Provisional - Published CSV - Suppressed - download
 %sql
 select distinct * from $db_output.supp_pub_csv_final
 where status = "Provisional"
 order by metric, breakdown, level_one, level_two, level_three, level_four, level_five, level_six

# COMMAND ----------

# DBTITLE 1,Main Dashboard CSV - Suppressed
 %sql
 select * from $db_output.final_supp_table2 order by ReportingPeriodStartDate, region_code, orgidprov, specialised_service, restrictiveintcode

# COMMAND ----------

# DBTITLE 1,DQ Dashboard CSV - Suppressed
 %sql
 select * from $db_output.dq_coverage_restraints order by REPORTING_PERIOD_START

# COMMAND ----------

# DBTITLE 1,PercRI Dashboard CSV - Suppressed
 %sql
 select distinct * from $db_output.rest_dash_hosp_supp where perc_people <> '*' order by ReportingPeriodStartDate

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "restraints_final_output",
  "suppressed_table": "supp_pub_csv_final",
  "main_dash": "final_supp_table2",
  "dq_dash": "dq_coverage_restraints",
  "perc_dash": "rest_dash_hosp"
}))