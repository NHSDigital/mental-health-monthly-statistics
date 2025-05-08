# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Dashboard CSV - Percentage of People in Hospital subject to a restrictive intervention
 %sql
 DROP TABLE IF EXISTS $db_output.rest_dash_hosp;
 CREATE TABLE IF NOT EXISTS $db_output.rest_dash_hosp(
 ReportingPeriodStartDate string,
 ReportingPeriodEndDate string,
 orgidprov string,
 org_name string,
 specialised_service string,
 ri_people int,
 people_in_hospital int,
 perc_people float
 ) USING DELTA

# COMMAND ----------

# DBTITLE 1,Unsuppressed Published CSV Final Table
 %sql
 DROP TABLE IF EXISTS $db_output.restraints_final_output1;
 CREATE TABLE IF NOT EXISTS $db_output.restraints_final_output1(
 ReportingPeriodStartDate string,
 ReportingPeriodEndDate string,
 breakdown string,
 level_one string,
 level_one_description string,
 level_two string,
 level_two_description string,
 level_three string,
 level_three_description string,
 level_four string,
 level_four_description string,
 level_five string,
 level_five_description string,
 level_six string,
 level_six_description string,
 metric string,
 metric_value float
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.supp_pub_csv_final;
 CREATE TABLE IF NOT EXISTS $db_output.supp_pub_csv_final(
 REPORTING_PERIOD_START string,
 REPORTING_PERIOD_END string,
 STATUS string,
 BREAKDOWN string,
 LEVEL_ONE string,
 LEVEL_ONE_DESCRIPTION string,
 LEVEL_TWO string,
 LEVEL_TWO_DESCRIPTION string,
 LEVEL_THREE string,
 LEVEL_THREE_DESCRIPTION string,
 LEVEL_FOUR string,
 LEVEL_FOUR_DESCRIPTION string,
 LEVEL_FIVE string,
 LEVEL_FIVE_DESCRIPTION string,
 LEVEL_SIX string,
 LEVEL_SIX_DESCRIPTION string,
 METRIC string,
 METRIC_VALUE string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.final_table;
 CREATE TABLE IF NOT EXISTS $db_output.final_table(
 ReportingPeriodStartDate string,
 ReportingPeriodEndDate string,
 region_code string,
 region_name string,
 orgidprov string,
 orgidname string,
 specialised_service string,
 restrictiveintcode string,
 restrictiveintname string,
 variabletype string,
 variable string,
 restraints int,
 people int,
 bed_days int,
 ri_per_1000_bed_days float,
 avg_min_of_restraint float,
 max_min_of_restraint int
 ) USING DELTA

# COMMAND ----------

 %sql

 DROP TABLE IF EXISTS $db_output.dq_coverage_restraints;
 CREATE TABLE $db_output.dq_coverage_restraints
 (
 REPORTING_PERIOD_START STRING,
 REPORTING_PERIOD_END STRING,
 STATUS STRING,
 ORGANISATION_CODE STRING,
 ORGANISATION_NAME STRING,
 TABLE_NAME STRING,
 COVERAGE_COUNT STRING
 ) USING DELTA