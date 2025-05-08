# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output1

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'England; Restrictive Intervention Type; Length of Restraint' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 length_of_restraint as level_three,
 length_of_restraint as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Region; Restrictive Intervention Type; Length of Restraint' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 length_of_restraint as level_three,
 length_of_restraint as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Provider; Restrictive Intervention Type; Length of Restraint' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 length_of_restraint as level_three,
 length_of_restraint as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 length_of_restraint as level_three,
 length_of_restraint as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Region; Provider; Restrictive Intervention Type; Length of Restraint' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 length_of_restraint as level_four,
 length_of_restraint as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 length_of_restraint as level_four,
 length_of_restraint as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 length_of_restraint as level_five,
 length_of_restraint as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Length of Restraint' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 length_of_restraint as level_four,
 length_of_restraint as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output1