# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output1

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Provider; Restrictive Intervention Type; Provider Site' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 siteidoftreat as level_three,
 site_name as level_three_description,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, siteidoftreat, site_name

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Region; Provider; Restrictive Intervention Type; Provider Site' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 siteidoftreat as level_four,
 site_name as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, siteidoftreat, site_name

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Provider Site' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 siteidoftreat as level_five,
 site_name as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, siteidoftreat, site_name

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output1
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Provider Site' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 siteidoftreat as level_four,
 site_name as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, siteidoftreat, site_name

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output1