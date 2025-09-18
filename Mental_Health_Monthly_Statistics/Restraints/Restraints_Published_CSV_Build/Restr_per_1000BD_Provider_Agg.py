# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS116 - Provider; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
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
 'MHS117' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.WS_bed_days_pub_csv b
 on a.orgidprov = b.bd_orgidprov 
 and a.orgidname = b.bd_orgidname 
 and a.siteidoftreat = b.bd_siteidoftreat 
 and a.site_name = b.bd_site_name 
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_gendercode = 'NULL'
 and b.bd_gendername = 'NULL'
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_specialised_service = 'NULL'
 and b.bd_provider_type = 'NULL'
 --and b.bd_length_of_restraint = 'NULL'
 where person_id is not null  and ss_type_ward_Rank = 1  and a.specialised_service <>  'No associated Ward Stay'                            -------------------------WS flag changes-------
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, siteidoftreat, site_name, b.bed_days 

# COMMAND ----------

# DBTITLE 1,MHS116 - Region; Provider; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
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
 'MHS117' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.WS_bed_days_pub_csv b
 on a.orgidprov = b.bd_orgidprov 
 and a.orgidname = b.bd_orgidname 
 and a.region_code = b.bd_region_code
 and a.region_name = b.bd_region_name
 and a.siteidoftreat = b.bd_siteidoftreat 
 and a.site_name = b.bd_site_name 
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_gendercode = 'NULL'
 and b.bd_gendername = 'NULL'
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_specialised_service = 'NULL'
 and b.bd_provider_type = 'NULL'
 --and b.bd_length_of_restraint = 'NULL'
 where person_id is not null and ss_type_ward_Rank = 1 and a.specialised_service <>  'No associated Ward Stay'                       -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, siteidoftreat, site_name, b.bed_days

# COMMAND ----------

# DBTITLE 1,MHS116 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
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
 'MHS117' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.WS_bed_days_pub_csv b
 on a.orgidprov = b.bd_orgidprov 
 and a.orgidname = b.bd_orgidname 
 and a.region_code = b.bd_region_code
 and a.region_name = b.bd_region_name
 and a.specialised_service = b.bd_specialised_service
 and a.siteidoftreat = b.bd_siteidoftreat 
 and a.site_name = b.bd_site_name 
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_gendercode = 'NULL'
 and b.bd_gendername = 'NULL'
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_provider_type = 'NULL'
 --and b.bd_length_of_restraint = 'NULL'
 where person_id is not null  and ss_type_ward_Rank = 1 and a.specialised_service <>  'No associated Ward Stay'                         -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, siteidoftreat, site_name, b.bed_days

# COMMAND ----------

# DBTITLE 1,MHS116 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Provider Site
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
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
 'MHS117' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.WS_bed_days_pub_csv b
 on a.orgidprov = b.bd_orgidprov 
 and a.orgidname = b.bd_orgidname 
 and a.specialised_service = b.bd_specialised_service
 and a.siteidoftreat = b.bd_siteidoftreat 
 and a.site_name = b.bd_site_name 
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_gendercode = 'NULL'
 and b.bd_gendername = 'NULL'
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_provider_type = 'NULL'
 --and b.bd_length_of_restraint = 'NULL'
 where person_id is not null and ss_type_ward_Rank = 1 and a.specialised_service <>  'No associated Ward Stay'                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, siteidoftreat, site_name, b.bed_days

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output