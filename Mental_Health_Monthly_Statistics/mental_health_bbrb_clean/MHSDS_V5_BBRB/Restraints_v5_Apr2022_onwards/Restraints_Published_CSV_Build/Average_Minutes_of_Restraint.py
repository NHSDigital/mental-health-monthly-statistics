# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS98 - England; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type =1
 group by restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS98 - Region; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by region_code, region_name, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS98 - Provider; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by orgidprov,orgidname, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS98 - Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS98 - Region; Provider; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname

# COMMAND ----------

 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS98 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS98 - Provider; Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS98' as metric,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and avg_min_flag_type = 'Y' and RI_RecordNumber_Type = 1
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output