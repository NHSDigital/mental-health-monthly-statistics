# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS138 - England
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
 'MHS138' as metric,
 sum(minutes_of_incident)/count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_inc = 'Y' and RI_RecordNumber_Inc =1

# COMMAND ----------

# DBTITLE 1,MHS138 - Region
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
 'MHS138' as metric,
 sum(minutes_of_incident)/count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_inc = 'Y' and RI_RecordNumber_Inc = 1
 group by region_code, region_name

# COMMAND ----------

# DBTITLE 1,MHS138 - Provider
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
 'MHS138' as metric,
 sum(minutes_of_incident)/count(distinct UniqRestrictiveIntIncID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_inc = 'Y' and RI_RecordNumber_Inc = 1
 group by orgidprov,orgidname