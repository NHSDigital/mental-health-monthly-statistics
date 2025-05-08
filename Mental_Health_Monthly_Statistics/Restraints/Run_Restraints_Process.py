# Databricks notebook source
 %sql
 select distinct uniqmonthid, reportingperiodstartdate from $mhsds.mhs000header order by uniqmonthid desc

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

import os

dbutils.widgets.text("db_output", "")
db_output  = dbutils.widgets.get("db_output")
assert db_output

dbutils.widgets.text("db_source", "mh_cur_clear")
db_source = dbutils.widgets.get("db_source")
assert db_source

dbutils.widgets.text("month_id", "1446")
month_id = dbutils.widgets.get("month_id")
assert month_id

dbutils.widgets.text("rp_enddate", "2020-09-30")
rp_enddate = dbutils.widgets.get("rp_enddate")
assert rp_enddate

dbutils.widgets.text("rp_startdate", "2020-09-01")
rp_startdate = dbutils.widgets.get("rp_startdate")
assert rp_startdate

params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate' : str(rp_enddate), 'rp_startdate': str(rp_startdate), 'month_id': month_id}

print(params)

# COMMAND ----------

# DBTITLE 1,Create Tables - Comment out when running multiple months
dbutils.notebook.run('Create_Restraints_Tables', 0, params)

# COMMAND ----------

# DBTITLE 1,Insert Monthly Data into Dashboard Tables
#when checking against $mhsds data using mh_cur_clear, Submission Picker Cell needs editing in the below notebook - details in that cell
dbutils.notebook.run('Restraints_Prep', 0, params)

dbutils.notebook.run('Dash_Restraints_DQ_Coverage', 0, params)
dbutils.notebook.run('Restraints_Dashboard_CSV_Build_v2', 0, params)

# #change month_id, rp_startdate, rp_enddate for each month you want to run

# COMMAND ----------

 %sql
 select count(distinct mhs505uniqid), count(distinct person_id), sum(der_bed_days) from $db_output.RI_FINAL

# COMMAND ----------

# DBTITLE 1,Insert Monthly Data into Published CSV Table
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Summary_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Duration_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Demographics_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_Restraints_Provider_Agg', 0, params)

dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Summary_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Duration_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Demographics_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Number_of_People_Provider_Agg', 0, params)

dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Summary_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Duration_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Demographics_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Bed_Days_Provider_Agg', 0, params)

dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Summary_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Duration_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Demographics_Agg', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Restr_per_1000BD_Provider_Agg', 0, params)

dbutils.notebook.run('Restraints_Published_CSV_Build/Perc_RIPeople_in_Hosp', 0, params)

dbutils.notebook.run('Restraints_Published_CSV_Build/Average_Minutes_of_Restraint', 0, params)
dbutils.notebook.run('Restraints_Published_CSV_Build/Maximum_Minutes_of_Restraint', 0, params)

# COMMAND ----------

# DBTITLE 1,Run Suppression Code
dbutils.notebook.run('Restraints_Suppression/Dashboard_Main_CSV_Suppression', 0, params)
dbutils.notebook.run('Restraints_Suppression/Dashboard_Perc_CSV_Suppression', 0, params)
dbutils.notebook.run('Restraints_Suppression/Published_CSV_Suppression', 0, params)
#Dashboard DQ CSV is already suppressed during process

# COMMAND ----------

# DBTITLE 1,Published CSV - Unsuppressed
 %sql
  select * from $db_output.restraints_final_output1
  order by metric, breakdown, level_one, level_two, level_three, level_four, level_five, level_six

# COMMAND ----------

# DBTITLE 1,Published CSV - Suppressed - download
 %sql
 select distinct * from $db_output.supp_pub_csv_final
 order by metric, breakdown, level_one, level_two, level_three, level_four, level_five, level_six

# COMMAND ----------

# DBTITLE 1,Main Dashboard CSV - Suppressed
 %sql
 select * from $db_output.final_supp_table2 order by ReportingPeriodStartDate

# COMMAND ----------

# DBTITLE 1,DQ Dashboard CSV - Suppressed
 %sql
 select * from $db_output.dq_coverage_restraints order by REPORTING_PERIOD_START

# COMMAND ----------

# DBTITLE 1,PercRI Dashboard CSV - Suppressed
 %sql
 select distinct * from global_temp.pre2_86 where perc_people <> '*' order by ReportingPeriodStartDate