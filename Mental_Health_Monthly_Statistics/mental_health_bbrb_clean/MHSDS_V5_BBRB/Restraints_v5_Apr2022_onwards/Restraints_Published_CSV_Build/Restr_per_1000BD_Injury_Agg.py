# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 patient_injury as level_two,
 patient_injury as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS96' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.der_bed_days) * 1000 as metric_value
 from (select *, 'a' as tag from $db_output.RI_FINAL where person_id is not null) a
 cross join (select sum(der_bed_days) as der_bed_days, 'a' as tag from $db_output.RI_FINAL) b on a.tag = b.tag
 GROUP BY 
 patient_injury, b.der_bed_days

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Restrictive Intervention Type; Age Category; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Age Category; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 patient_injury as level_four,
 patient_injury as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS96' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.bed_days_pub_csv b
 on a.age_category = b.bd_age_category
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_gendercode = 'NULL' 
 and b.bd_gendername = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_orgidprov = 'NULL'
 and b.bd_orgidname = 'NULL'
 -- and b.bd_specialised_service = 'NULL' 
 -- and b.bd_siteidoftreat = 'NULL'
 -- and b.bd_site_name = 'NULL'
 and b.bd_provider_type = 'NULL'
 and b.bd_length_of_restraint = 'NULL'
 and b.bd_bame_group = 'NULL'
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, age_category, patient_injury, b.bed_days

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Restrictive Intervention Type; Gender; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Gender; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
 patient_injury as level_four,
 patient_injury as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS96' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.bed_days_pub_csv b
 on a.gendercode = b.bd_gendercode
 and a.gendername = b.bd_gendername
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_orgidprov = 'NULL'
 and b.bd_orgidname = 'NULL'
 -- and b.bd_specialised_service = 'NULL' 
 -- and b.bd_siteidoftreat = 'NULL'
 -- and b.bd_site_name = 'NULL'
 and b.bd_provider_type = 'NULL'
 and b.bd_length_of_restraint = 'NULL'
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, gendercode, gendername, patient_injury, b.bed_days

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Restrictive Intervention Type; BAME Group; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; BAME Group; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 patient_injury as level_four,
 patient_injury as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS96' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.bed_days_pub_csv b
 on a.bame_group = b.bd_bame_group
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_gendercode = 'NULL' 
 and b.bd_gendername = 'NULL'
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_orgidprov = 'NULL'
 and b.bd_orgidname = 'NULL'
 -- and b.bd_specialised_service = 'NULL' 
 -- and b.bd_siteidoftreat = 'NULL'
 -- and b.bd_site_name = 'NULL'
 and b.bd_provider_type = 'NULL'
 and b.bd_length_of_restraint = 'NULL'
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, bame_group, patient_injury, b.bed_days

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Restrictive Intervention Type; Upper Ethnicity; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Upper Ethnicity; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 upperethnicitycode as level_three,
 upperethnicityname as level_three_description,
 patient_injury as level_four,
 patient_injury as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS96' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a
 left join $db_output.bed_days_pub_csv b
 on a.upperethnicitycode = b.bd_upperethnicitycode
 and a.upperethnicityname = b.bd_upperethnicityname
 and b.bd_gendercode = 'NULL'
 and b.bd_gendername = 'NULL'
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_orgidprov = 'NULL'
 and b.bd_orgidname = 'NULL'
 -- and b.bd_specialised_service = 'NULL' 
 -- and b.bd_siteidoftreat = 'NULL'
 -- and b.bd_site_name = 'NULL'
 and b.bd_provider_type = 'NULL'
 and b.bd_length_of_restraint = 'NULL'
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname, patient_injury, b.bed_days

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Restrictive Intervention Type; Length of Restraint; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Length of Restraint; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 length_of_restraint as level_three,
 length_of_restraint as level_three_description,
 patient_injury as level_four,
 patient_injury as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS96' as metric,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/b.bed_days) * 1000 as metric_value
 from $db_output.RI_FINAL a 
 left join $db_output.bed_days_pub_csv b
 on a.length_of_restraint = b.bd_length_of_restraint
 and b.bd_upperethnicitycode = 'NULL'
 and b.bd_upperethnicityname = 'NULL'
 and b.bd_gendercode = 'NULL'
 and b.bd_gendername = 'NULL'
 and b.bd_bame_group = 'NULL'
 and b.bd_age_group = 'NULL'
 and b.bd_age_category = 'NULL'
 and b.bd_region_code = 'NULL'
 and b.bd_region_name = 'NULL'
 and b.bd_lowerethnicitycode = 'NULL'
 and b.bd_lowerethnicity = 'NULL'
 and b.bd_orgidprov = 'NULL'
 and b.bd_orgidname = 'NULL'
 -- and b.bd_specialised_service = 'NULL' 
 -- and b.bd_siteidoftreat = 'NULL'
 -- and b.bd_site_name = 'NULL'
 and b.bd_provider_type = 'NULL'
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, length_of_restraint, patient_injury, b.bed_days

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output