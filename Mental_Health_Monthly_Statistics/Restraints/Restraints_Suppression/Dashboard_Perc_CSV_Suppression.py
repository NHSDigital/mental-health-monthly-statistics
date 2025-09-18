# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW pre_86 AS
 select
 ReportingPeriodStartDate, 
 ReportingPeriodEndDate,
 orgidprov,
 org_name,
 specialised_service,
 CASE WHEN ri_people < 5 THEN '*' ELSE ROUND(ri_people/5, 0)*5 END as ri_people,
 CASE WHEN people_in_hospital < 5 THEN '*' ELSE ROUND(people_in_hospital/5, 0)*5 END as people_in_hospital,
 perc_people
 from $db_output.rest_dash_hosp

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.rest_dash_hosp_supp;
 CREATE TABLE IF NOT EXISTS $db_output.rest_dash_hosp_supp AS
 select
 ReportingPeriodStartDate, 
 ReportingPeriodEndDate,
 orgidprov,
 org_name,
 specialised_service,
 CASE WHEN ri_people = '*' THEN '*' WHEN people_in_hospital = '*' THEN '*' ELSE CAST(ROUND(perc_people,0) as INT) END AS perc_people
 from global_temp.pre_86