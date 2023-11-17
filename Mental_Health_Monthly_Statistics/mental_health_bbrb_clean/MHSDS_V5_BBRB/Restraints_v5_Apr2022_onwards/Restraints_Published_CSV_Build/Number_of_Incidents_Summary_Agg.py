# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS136 - England
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 'NULL' as level_two,
 'NULL' as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS136' as metric,
 count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null

# COMMAND ----------

# DBTITLE 1,MHS136 - Region
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 'NULL' as level_two,
 'NULL' as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS136' as metric,
 count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name

# COMMAND ----------

# DBTITLE 1,MHS136 - Provider
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 'NULL' as level_two,
 'NULL' as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS136' as metric,
 count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname

# COMMAND ----------

# DBTITLE 1,MHS136 - England; Incident Reason
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Incident Reason' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 RestrictiveIntReasonCode as level_two,
 RestrictiveIntReasonName as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS136' as metric,
 count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null                         -------------------------WS flag changes-------
 group by RestrictiveIntReasonCode, RestrictiveIntReasonName

# COMMAND ----------

# DBTITLE 1,MHS136 - Region; Incident Reason
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Incident Reason' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 RestrictiveIntReasonCode as level_two,
 RestrictiveIntReasonName as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS136' as metric,
 count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null                         -------------------------WS flag changes-------
 group by region_code, region_name, RestrictiveIntReasonCode, RestrictiveIntReasonName

# COMMAND ----------

# DBTITLE 1,MHS136 - England; Restrictive Intervention Type; Incident Reason
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Incident Reason' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 RestrictiveIntReasonCode as level_three,
 RestrictiveIntReasonName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS136' as metric,
 count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null                         -------------------------WS flag changes-------
 group by restrictiveintcode, restrictiveintname, RestrictiveIntReasonCode, RestrictiveIntReasonName