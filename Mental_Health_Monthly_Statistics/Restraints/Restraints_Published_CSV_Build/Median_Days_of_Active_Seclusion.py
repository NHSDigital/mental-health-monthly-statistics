# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS146 - England
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
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'

# COMMAND ----------

# DBTITLE 1,MHS146 - Region
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
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by region_code, region_name

# COMMAND ----------

# DBTITLE 1,MHS146 - Provider
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
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by orgidprov,orgidname

# COMMAND ----------

# DBTITLE 1,MHS146 - Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
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
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by specialised_service

# COMMAND ----------

# DBTITLE 1,MHS146 - Region; Provider
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by region_code, region_name, orgidprov, orgidname

# COMMAND ----------

 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by region_code, region_name, specialised_service

# COMMAND ----------

# DBTITLE 1,MHS146 - Region; Provider; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by region_code, region_name, orgidprov, orgidname, specialised_service

# COMMAND ----------

# DBTITLE 1,MHS146 - Provider; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS146a' as metric,
 percentile(days_of_restraint, 0.5) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and restrictiveintname = "Seclusion" and RI_RecordNumber_Type = 1 and enddaterestrictiveintType is null and active_avg_min_flag_type = 'Y'
 group by orgidprov, orgidname, specialised_service

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output