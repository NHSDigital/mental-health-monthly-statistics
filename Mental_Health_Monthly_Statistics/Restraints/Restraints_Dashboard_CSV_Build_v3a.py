# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

 %sql
 refresh table $db_output.RI_FINAL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_bed_days_total_test AS
 select 
 'Total' as variable,
 sum(der_bed_days) as hosp_bed_days,
 sum(der_bed_days) as ward_bed_days
 from $db_output.RI_FINAL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_bed_days_restr_test AS
 select 
 'Restrictive Intervention Type' as variable,
 sum(der_bed_days) as hosp_bed_days,
 sum(der_bed_days_ws) as ward_bed_days
 from $db_output.RI_FINAL

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.dash_hosp_bed_days_pub_csv;
 CREATE TABLE IF NOT EXISTS $db_output.dash_hosp_bed_days_pub_csv AS
 select 
 coalesce(age_group, 'England') as age_group, 
 coalesce(gendercode, 'England') as gendercode, 
 coalesce(gendername, 'England') as gendername, 
 coalesce(upperethnicitycode, 'England') as upperethnicitycode, 
 coalesce(upperethnicityname, 'England') as upperethnicityname, 
 coalesce(lowerethnicitycode, 'England') as lowerethnicitycode, 
 coalesce(lowerethnicity, 'England') as lowerethnicity, 
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname, 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,
 coalesce(provider_type, 'England') as provider_type,
 coalesce(length_of_restraint, 'England') as length_of_restraint,
 coalesce(age_category, 'England') as age_category,
 coalesce(bame_group, 'England') as bame_group,
 'England' as specialised_service,
 'England' as siteidoftreat,
 'England' as site_name,
 sum(Der_bed_days) as bed_days
 from $db_output.RI_FINAL 
 group by grouping sets (
 (age_group),
 (gendercode, gendername),
 (upperethnicitycode, upperethnicityname),
 (lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname),
 (region_code, region_name),
 (provider_type),
 (length_of_restraint),
 (age_category),
 (bame_group),
 (region_code, region_name, age_group),
 (region_code, region_name, gendercode, gendername),
 (region_code, region_name, upperethnicitycode, upperethnicityname),
 (region_code, region_name, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, provider_type),
 (region_code, region_name, length_of_restraint),
 (region_code, region_name, age_category),
 (region_code, region_name, bame_group),
 (orgidprov, orgidname, age_group),
 (orgidprov, orgidname, gendercode, gendername),
 (orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, provider_type),
 (orgidprov, orgidname, length_of_restraint),
 (orgidprov, orgidname, age_category),
 (orgidprov, orgidname, bame_group),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, orgidprov, orgidname, age_group),
 (region_code, region_name, orgidprov, orgidname, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, provider_type),
 (region_code, region_name, orgidprov, orgidname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, age_category),
 (region_code, region_name, orgidprov, orgidname, bame_group),
 (gendercode, gendername, upperethnicitycode, upperethnicityname), ---NEW BREAKDOWN
 (age_group, upperethnicitycode, upperethnicityname), ---NEW BREAKDOWN
 (gendercode, gendername, lowerethnicitycode, lowerethnicity) ---NEW BREAKDOWN
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.dash_ward_bed_days_pub_csv;
 CREATE TABLE IF NOT EXISTS $db_output.dash_ward_bed_days_pub_csv AS
 select 
 coalesce(age_group, 'England') as age_group, 
 coalesce(gendercode, 'England') as gendercode, 
 coalesce(gendername, 'England') as gendername, 
 coalesce(upperethnicitycode, 'England') as upperethnicitycode, 
 coalesce(upperethnicityname, 'England') as upperethnicityname, 
 coalesce(lowerethnicitycode, 'England') as lowerethnicitycode, 
 coalesce(lowerethnicity, 'England') as lowerethnicity, 
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname, 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name, 
 coalesce(specialised_service_bd, 'England') as specialised_service, 
 coalesce(siteidoftreat_bd, 'England') as siteidoftreat,
 coalesce(site_name_bd, 'England') as site_name,
 coalesce(provider_type, 'England') as provider_type,
 coalesce(age_category, 'England') as age_category,
 coalesce(bame_group, 'England') as bame_group,
 sum(der_bed_days_ws) as bed_days
 from $db_output.RI_FINAL
 group by grouping sets (
 (specialised_service_bd),
 (siteidoftreat_bd, site_name_bd),
 (region_code, region_name, siteidoftreat_bd, site_name_bd),
 (orgidprov, orgidname, siteidoftreat_bd, site_name_bd),
 (specialised_service_bd, age_group),
 (specialised_service_bd, gendercode, gendername),
 (specialised_service_bd, upperethnicitycode, upperethnicityname),
 (specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (specialised_service_bd, provider_type),
 (specialised_service_bd, age_category),
 (specialised_service_bd, bame_group),
 (region_code, region_name, orgidprov, orgidname, siteidoftreat_bd, site_name_bd),
 (region_code, region_name, specialised_service_bd),
 (region_code, region_name, specialised_service_bd, age_group),
 (region_code, region_name, specialised_service_bd, gendercode, gendername),
 (region_code, region_name, specialised_service_bd, upperethnicitycode, upperethnicityname),
 (region_code, region_name, specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (region_code, region_name, specialised_service_bd, provider_type),
 (region_code, region_name, specialised_service_bd, age_category),
 (region_code, region_name, specialised_service_bd, bame_group),
 (orgidprov, orgidname, specialised_service_bd),
 (orgidprov, orgidname, specialised_service_bd, age_group),
 (orgidprov, orgidname, specialised_service_bd, gendercode, gendername),
 (orgidprov, orgidname, specialised_service_bd, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (orgidprov, orgidname, specialised_service_bd, provider_type),
 (orgidprov, orgidname, specialised_service_bd, age_category),
 (orgidprov, orgidname, specialised_service_bd, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, bame_group)
 )

# COMMAND ----------

# %sql
# CREATE OR REPLACE GLOBAL TEMP VIEW bed_days_total_test AS
# select 
# coalesce(age_group, 'England') as age_group, 
# coalesce(gendercode, 'England') as gendercode, 
# coalesce(gendername, 'England') as gendername, 
# coalesce(upperethnicitycode, 'England') as upperethnicitycode, 
# coalesce(upperethnicityname, 'England') as upperethnicityname, 
# coalesce(lowerethnicitycode, 'England') as lowerethnicitycode, 
# coalesce(lowerethnicity, 'England') as lowerethnicity, 
# coalesce(orgidprov, 'England') as orgidprov, 
# coalesce(orgidname, 'England') as orgidname, 
# coalesce(region_code, 'England') as region_code, 
# coalesce(region_name, 'England') as region_name, 
# coalesce(specialised_service_bd, 'England') as specialised_service, 
# coalesce(siteidoftreat_bd, 'England') as siteidoftreat,
# coalesce(site_name_bd, 'England') as site_name,
# coalesce(provider_type, 'England') as provider_type,
# coalesce(length_of_restraint, 'England') as length_of_restraint,
# coalesce(age_category, 'England') as age_category,
# coalesce(bame_group, 'England') as bame_group,
# sum(Der_bed_days) as bed_days
# from $db_output.RI_FINAL
# group by grouping sets (
# (age_group),
# (gendercode, gendername),
# (upperethnicitycode, upperethnicityname),
# (lowerethnicitycode, lowerethnicity),
# (orgidprov, orgidname),
# (region_code, region_name),
# (specialised_service_bd),
# (siteidoftreat_bd, site_name_bd),
# (provider_type),
# (length_of_restraint),
# (age_category),
# (bame_group),
# (region_code, region_name, age_group),
# (region_code, region_name, gendercode, gendername),
# (region_code, region_name, upperethnicitycode, upperethnicityname),
# (region_code, region_name, lowerethnicitycode, lowerethnicity),
# (region_code, region_name, siteidoftreat_bd, site_name_bd),
# (region_code, region_name, provider_type),
# (region_code, region_name, length_of_restraint),
# (region_code, region_name, age_category),
# (region_code, region_name, bame_group),
# (orgidprov, orgidname, age_group),
# (orgidprov, orgidname, gendercode, gendername),
# (orgidprov, orgidname, upperethnicitycode, upperethnicityname),
# (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
# (orgidprov, orgidname, siteidoftreat_bd, site_name_bd),
# (orgidprov, orgidname, provider_type),
# (orgidprov, orgidname, length_of_restraint),
# (orgidprov, orgidname, age_category),
# (orgidprov, orgidname, bame_group),
# (specialised_service_bd, age_group),
# (specialised_service_bd, gendercode, gendername),
# (specialised_service_bd, upperethnicitycode, upperethnicityname),
# (specialised_service_bd, lowerethnicitycode, lowerethnicity),
# (specialised_service_bd, siteidoftreat_bd, site_name_bd),
# (specialised_service_bd, provider_type),
# (specialised_service_bd, length_of_restraint),
# (specialised_service_bd, age_category),
# (specialised_service_bd, bame_group),
# (region_code, region_name, orgidprov, orgidname),
# (region_code, region_name, orgidprov, orgidname, age_group),
# (region_code, region_name, orgidprov, orgidname, gendercode, gendername),
# (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),
# (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
# (region_code, region_name, orgidprov, orgidname, siteidoftreat_bd, site_name_bd),
# (region_code, region_name, orgidprov, orgidname, provider_type),
# (region_code, region_name, orgidprov, orgidname, length_of_restraint),
# (region_code, region_name, orgidprov, orgidname, age_category),
# (region_code, region_name, orgidprov, orgidname, bame_group),
# (region_code, region_name, specialised_service_bd),
# (region_code, region_name, specialised_service_bd, age_group),
# (region_code, region_name, specialised_service_bd, gendercode, gendername),
# (region_code, region_name, specialised_service_bd, upperethnicitycode, upperethnicityname),
# (region_code, region_name, specialised_service_bd, lowerethnicitycode, lowerethnicity),
# (region_code, region_name, specialised_service_bd, siteidoftreat_bd, site_name_bd),
# (region_code, region_name, specialised_service_bd, provider_type),
# (region_code, region_name, specialised_service_bd, length_of_restraint),
# (region_code, region_name, specialised_service_bd, age_category),
# (region_code, region_name, specialised_service_bd, bame_group),
# (orgidprov, orgidname, specialised_service_bd),
# (orgidprov, orgidname, specialised_service_bd, age_group),
# (orgidprov, orgidname, specialised_service_bd, gendercode, gendername),
# (orgidprov, orgidname, specialised_service_bd, upperethnicitycode, upperethnicityname),
# (orgidprov, orgidname, specialised_service_bd, lowerethnicitycode, lowerethnicity),
# (orgidprov, orgidname, specialised_service_bd, siteidoftreat_bd, site_name_bd),
# (orgidprov, orgidname, specialised_service_bd, provider_type),
# (orgidprov, orgidname, specialised_service_bd, length_of_restraint),
# (orgidprov, orgidname, specialised_service_bd, age_category),
# (orgidprov, orgidname, specialised_service_bd, bame_group),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_group),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, gendercode, gendername),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, upperethnicitycode, upperethnicityname),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, lowerethnicitycode, lowerethnicity),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, siteidoftreat_bd, site_name_bd),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, provider_type),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, length_of_restraint),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_category),
# (region_code, region_name, orgidprov, orgidname, specialised_service_bd, bame_group)
# )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_restr_type_breakdowns_test AS 
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 restrictiveintcode as restrictiveintcode,
 restrictiveintname as restrictiveintname,
 'England' as patient_injury,
 'Restrictive Intervention Type' as variabletype, 
 'Restrictive Intervention Type' as variable, 
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints, count(distinct UniqRestrictiveIntIncID) as incidents, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Total' as variabletype,
 'Total' as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL 
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by grouping sets (
 (region_code, region_name),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, specialised_service),
 (region_code, region_name, restrictiveintcode, restrictiveintname),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname),
 (region_code, region_name, orgidprov, orgidname, specialised_service),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname),
 (orgidprov, orgidname, specialised_service),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (specialised_service),
 (specialised_service, restrictiveintcode, restrictiveintname),
 (restrictiveintcode, restrictiveintname),
 (patient_injury),
 (specialised_service, patient_injury),
 (restrictiveintcode, restrictiveintname, patient_injury)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Total' as variabletype,
 'Total' as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL 
 where person_id is not null  and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by grouping sets (
 (region_code, region_name),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, specialised_service),
 (region_code, region_name, restrictiveintcode, restrictiveintname),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname),
 (region_code, region_name, orgidprov, orgidname, specialised_service),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname),
 (orgidprov, orgidname, specialised_service),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (specialised_service),
 (specialised_service, restrictiveintcode, restrictiveintname),
 (restrictiveintcode, restrictiveintname)
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Total' as variabletype,
 'Total' as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null and ss_type_ward_Rank = 1)
 where patient_injury_max = 1
 group by grouping sets (
 (patient_injury),
 (specialised_service, patient_injury)
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 restrictiveintcode,
 restrictiveintname,
 patient_injury,
 'Total' as variabletype,
 'Total' as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id, restrictiveintcode order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null and ss_type_ward_Rank = 1)
 where patient_injury_max = 1
 group by restrictiveintcode, restrictiveintname, patient_injury

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_time_test_type AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Total' as variabletype,
 'Total' as variable,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as avg_min_of_restraint,
 max(minutes_of_restraint) as max_min_of_restraint
 from $db_output.RI_FINAL
 where avg_min_flag_type = 'Y' and person_id is not null and RI_RecordNumber_Type =1
 group by grouping sets (
 (region_code, region_name),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, specialised_service),
 (region_code, region_name, restrictiveintcode, restrictiveintname),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname),
 (region_code, region_name, orgidprov, orgidname, specialised_service),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname),
 (orgidprov, orgidname, specialised_service),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (specialised_service),
 (specialised_service, restrictiveintcode, restrictiveintname),
 (restrictiveintcode, restrictiveintname)
 )
  
 UNION
  
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 'England' as patient_injury,
 'England' as variabletype,
 'England' as variable,
 sum(minutes_of_restraint)/count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as avg_min_of_restraint,
 max(minutes_of_restraint) as max_min_of_restraint
 from $db_output.RI_FINAL
 where avg_min_flag_type = 'Y' and person_id is not null and RI_RecordNumber_Type =1

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_time_test_inc AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 'England' as patient_injury,
 'Total' as variabletype,
 'Total' as variable,
 sum(minutes_of_incident)/count(distinct UniqRestrictiveIntIncID) as avg_min_of_incident,
 max(minutes_of_incident) as max_min_of_incident
 from $db_output.RI_FINAL
 where avg_min_flag_inc = 'Y' and person_id is not null and RI_RecordNumber_Inc =1
 group by grouping sets (
 (region_code, region_name),
 (region_code, region_name, orgidprov, orgidname),
 -- (region_code, region_name, specialised_service),
 -- (region_code, region_name, restrictiveintcode, restrictiveintname),
 -- (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname),
 -- (region_code, region_name, orgidprov, orgidname, specialised_service),
 -- (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 -- (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 (orgidprov, orgidname)
 -- (orgidprov, orgidname, specialised_service),
 -- (orgidprov, orgidname, restrictiveintcode, restrictiveintname),
 -- (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname),
 -- (specialised_service),
 -- (specialised_service, restrictiveintcode, restrictiveintname),
 -- (restrictiveintcode, restrictiveintname)
 )
  
 UNION
  
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 'England' as patient_injury,
 'England' as variabletype,
 'England' as variable,
 sum(minutes_of_incident)/count(distinct UniqRestrictiveIntIncID) as avg_min_of_incident,
 max(minutes_of_incident) as max_min_of_incident
 from $db_output.RI_FINAL
 where avg_min_flag_inc = 'Y' and person_id is not null and RI_RecordNumber_Inc =1

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_time_test AS 
 select 
 a.region_code, 
 a.region_name,  
 a.orgidprov, 
 a.orgidname,
 a.specialised_service,
 a.restrictiveintcode,
 a.restrictiveintname,
 a.patient_injury,
 a.variabletype,
 a.variable,
 a.avg_min_of_restraint,
 a.max_min_of_restraint,
 coalesce(b.avg_min_of_incident, 0) as avg_min_of_incident,
 coalesce(b.max_min_of_incident, 0) as max_min_of_incident
 from global_temp.total_breakdowns_time_test_type a
 LEFT JOIN global_temp.total_breakdowns_time_test_inc b
 ON 
 a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.restrictiveintcode = b.restrictiveintcode
 and a.restrictiveintname = b.restrictiveintname
 and a.patient_injury = b.patient_injury
 and a.variabletype = b.variabletype
 and a.variable = b.variable

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_group_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Age Group' as variabletype,
 age_group as variable, 
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (age_group),
 (region_code, region_name, age_group),
 (region_code, region_name, orgidprov, orgidname, age_group),
 (region_code, region_name, specialised_service, age_group),
 (region_code, region_name, restrictiveintcode, restrictiveintname, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_group),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_group),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (orgidprov, orgidname, age_group),
 (orgidprov, orgidname, specialised_service, age_group),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_group),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (specialised_service, age_group),
 (specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (restrictiveintcode, restrictiveintname, age_group),
 (patient_injury, age_group),
 (restrictiveintcode, restrictiveintname, patient_injury, age_group)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_group_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Age Group' as variabletype,
 age_group as variable, 
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (age_group),
 (region_code, region_name, age_group),
 (region_code, region_name, orgidprov, orgidname, age_group),
 (region_code, region_name, specialised_service, age_group),
 (region_code, region_name, restrictiveintcode, restrictiveintname, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_group),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_group),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (orgidprov, orgidname, age_group),
 (orgidprov, orgidname, specialised_service, age_group),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_group),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (specialised_service, age_group),
 (specialised_service, restrictiveintcode, restrictiveintname, age_group),
 (restrictiveintcode, restrictiveintname, age_group)
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Age Group' as variabletype,
 age_group as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 group by grouping sets (
 (patient_injury, age_group)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW upper_eth_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Upper Ethnicity' as variabletype,
 upperethnicityname as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (upperethnicitycode, upperethnicityname),
 (region_code, region_name, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, specialised_service, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (specialised_service, upperethnicitycode, upperethnicityname),--
 (specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (patient_injury, upperethnicitycode, upperethnicityname),
 (restrictiveintcode, restrictiveintname, patient_injury, upperethnicitycode, upperethnicityname)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW upper_eth_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Upper Ethnicity' as variabletype,
 upperethnicityname as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (upperethnicitycode, upperethnicityname),
 (region_code, region_name, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, specialised_service, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (specialised_service, upperethnicitycode, upperethnicityname),--
 (specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname),--
 (restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname)--
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Upper Ethnicity' as variabletype,
 upperethnicityname as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 group by grouping sets (
 (patient_injury, upperethnicitycode, upperethnicityname)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW lower_eth_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Lower Ethnicity' as variabletype,
 lowerethnicity as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (lowerethnicitycode, lowerethnicity),
 (region_code, region_name, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (specialised_service, lowerethnicitycode, lowerethnicity),
 (specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (patient_injury, lowerethnicitycode, lowerethnicity),
 (restrictiveintcode, restrictiveintname, patient_injury, lowerethnicitycode, lowerethnicity)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW lower_eth_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Lower Ethnicity' as variabletype,
 lowerethnicity as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (lowerethnicitycode, lowerethnicity),
 (region_code, region_name, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (specialised_service, lowerethnicitycode, lowerethnicity),
 (specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity),
 (restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW site_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Provider Site' as variabletype,
 site_name as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (region_code, region_name, orgidprov, orgidname, site_name),
 (region_code, region_name, orgidprov, orgidname, specialised_service, site_name),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, site_name),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, site_name),
 (orgidprov, orgidname, site_name),
 (orgidprov, orgidname, specialised_service, site_name),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, site_name),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, site_name)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW site_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Provider Site' as variabletype,
 site_name as variable,
 count(distinct person_id) as people 
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (region_code, region_name, orgidprov, orgidname, site_name),
 (region_code, region_name, orgidprov, orgidname, specialised_service, site_name),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, site_name),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, site_name),
 (orgidprov, orgidname, site_name),
 (orgidprov, orgidname, specialised_service, site_name),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, site_name),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, site_name)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW prov_type_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Provider Type' as variabletype,
 provider_type as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (provider_type),
 (region_code, region_name, provider_type),
 (region_code, region_name, orgidprov, orgidname, provider_type),
 (region_code, region_name, specialised_service, provider_type),
 (region_code, region_name, restrictiveintcode, restrictiveintname, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service, provider_type),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, provider_type),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (orgidprov, orgidname, provider_type),
 (orgidprov, orgidname, specialised_service, provider_type),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, provider_type),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (specialised_service, provider_type),
 (specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (restrictiveintcode, restrictiveintname, provider_type)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW prov_type_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Provider Type' as variabletype,
 provider_type as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (provider_type),
 (region_code, region_name, provider_type),
 (region_code, region_name, orgidprov, orgidname, provider_type),
 (region_code, region_name, specialised_service, provider_type),
 (region_code, region_name, restrictiveintcode, restrictiveintname, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service, provider_type),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, provider_type),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (orgidprov, orgidname, provider_type),
 (orgidprov, orgidname, specialised_service, provider_type),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, provider_type),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (specialised_service, provider_type),
 (specialised_service, restrictiveintcode, restrictiveintname, provider_type),
 (restrictiveintcode, restrictiveintname, provider_type)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW gender_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Gender' as variabletype,
 gendername as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (gendername),
 (region_code, region_name, gendername),
 (region_code, region_name, orgidprov, orgidname, gendername),
 (region_code, region_name, specialised_service, gendername),
 (region_code, region_name, restrictiveintcode, restrictiveintname, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service, gendername),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, gendername),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (orgidprov, orgidname, gendername),
 (orgidprov, orgidname, specialised_service, gendername),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, gendername),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (specialised_service, gendername),
 (specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (restrictiveintcode, restrictiveintname, gendername),
 (patient_injury, gendername),
 (restrictiveintcode, restrictiveintname, patient_injury, gendername)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW gender_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Gender' as variabletype,
 gendername as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (gendername),
 (region_code, region_name, gendername),
 (region_code, region_name, orgidprov, orgidname, gendername),
 (region_code, region_name, specialised_service, gendername),
 (region_code, region_name, restrictiveintcode, restrictiveintname, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service, gendername),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, gendername),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (orgidprov, orgidname, gendername),
 (orgidprov, orgidname, specialised_service, gendername),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, gendername),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (specialised_service, gendername),
 (specialised_service, restrictiveintcode, restrictiveintname, gendername),
 (restrictiveintcode, restrictiveintname, gendername),
 (restrictiveintcode, restrictiveintname, patient_injury, gendername)
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Gender' as variabletype,
 gendername as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 group by grouping sets (
 (patient_injury, gendername)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW restr_length_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Restraint Length' as variabletype,
 length_of_restraint as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (region_code, region_name, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (restrictiveintcode, restrictiveintname, length_of_restraint),
 (patient_injury, length_of_restraint),
 (restrictiveintcode, restrictiveintname, patient_injury, length_of_restraint)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW restr_length_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Restraint Length' as variabletype,
 length_of_restraint as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (region_code, region_name, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (restrictiveintcode, restrictiveintname, length_of_restraint),
 (patient_injury, length_of_restraint)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_category_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Age Category' as variabletype,
 age_category as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (age_category),
 (region_code, region_name, age_category),
 (region_code, region_name, orgidprov, orgidname, age_category),
 (region_code, region_name, specialised_service, age_category),
 (region_code, region_name, restrictiveintcode, restrictiveintname, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_category),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_category),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (orgidprov, orgidname, age_category),
 (orgidprov, orgidname, specialised_service, age_category),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_category),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (specialised_service, age_category),
 (specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (restrictiveintcode, restrictiveintname, age_category),
 (patient_injury, age_category),
 (restrictiveintcode, restrictiveintname, patient_injury, age_category)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_category_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Age Category' as variabletype,
 age_category as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (age_category),
 (region_code, region_name, age_category),
 (region_code, region_name, orgidprov, orgidname, age_category),
 (region_code, region_name, specialised_service, age_category),
 (region_code, region_name, restrictiveintcode, restrictiveintname, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_category),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_category),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (orgidprov, orgidname, age_category),
 (orgidprov, orgidname, specialised_service, age_category),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_category),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (specialised_service, age_category),
 (specialised_service, restrictiveintcode, restrictiveintname, age_category),
 (restrictiveintcode, restrictiveintname, age_category)
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'Age Category' as variabletype,
 age_category as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 group by grouping sets (
 (patient_injury, age_category)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bame_group_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'BAME Group' as variabletype,
 bame_group as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (bame_group),
 (region_code, region_name, bame_group),
 (region_code, region_name, orgidprov, orgidname, bame_group),
 (region_code, region_name, specialised_service, bame_group),
 (region_code, region_name, restrictiveintcode, restrictiveintname, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (orgidprov, orgidname, bame_group),
 (orgidprov, orgidname, specialised_service, bame_group),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (specialised_service, bame_group),
 (specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (restrictiveintcode, restrictiveintname, bame_group),
 (patient_injury, bame_group),
 (restrictiveintcode, restrictiveintname, patient_injury, bame_group)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bame_group_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'BAME Group' as variabletype,
 bame_group as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (bame_group),
 (region_code, region_name, bame_group),
 (region_code, region_name, orgidprov, orgidname, bame_group),
 (region_code, region_name, specialised_service, bame_group),
 (region_code, region_name, restrictiveintcode, restrictiveintname, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (orgidprov, orgidname, bame_group),
 (orgidprov, orgidname, specialised_service, bame_group),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (specialised_service, bame_group),
 (specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (restrictiveintcode, restrictiveintname, bame_group)
 )
 union
 select 
 'England' as region_code, 
 'England' as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 'England' as restrictiveintcode,
 'England' as restrictiveintname,
 coalesce(patient_injury, 'England') as patient_injury,
 'BAME Group' as variabletype,
 bame_group as variable,
 count(distinct person_id) as people
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 group by grouping sets (
 (patient_injury, bame_group)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW inc_reason_breakdowns_restr AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Incident Reason' as variabletype,
 RestrictiveIntReasonName as variable,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (RestrictiveIntReasonName),
 (region_code, region_name, RestrictiveIntReasonName),
 -- (region_code, region_name, orgidprov, orgidname, bame_group),
 -- (region_code, region_name, specialised_service, bame_group),
 -- (region_code, region_name, restrictiveintcode, restrictiveintname, bame_group),
 -- (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group),
 -- (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 -- (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 -- (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 -- (orgidprov, orgidname, bame_group),
 -- (orgidprov, orgidname, specialised_service, bame_group),
 -- (orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 -- (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 -- (specialised_service, bame_group),
 -- (specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (restrictiveintcode, restrictiveintname, RestrictiveIntReasonName)
 -- (patient_injury, bame_group),
 -- (restrictiveintcode, restrictiveintname, patient_injury, bame_group)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW inc_reason_breakdowns_people AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 'England' as orgidprov, 
 'England' as orgidname,
 'England' as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'England' as patient_injury,
 'Incident Reason' as variabletype,
 RestrictiveIntReasonName as variable,
 count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by grouping sets (
 (RestrictiveIntReasonName),
 (region_code, region_name, RestrictiveIntReasonName),
 -- (region_code, region_name, orgidprov, orgidname, bame_group),
 -- (region_code, region_name, specialised_service, bame_group),
 -- (region_code, region_name, restrictiveintcode, restrictiveintname, bame_group),
 -- (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group),
 -- (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 -- (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 -- (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 -- (orgidprov, orgidname, bame_group),
 -- (orgidprov, orgidname, specialised_service, bame_group),
 -- (orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group),
 -- (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 -- (specialised_service, bame_group),
 -- (specialised_service, restrictiveintcode, restrictiveintname, bame_group),
 (restrictiveintcode, restrictiveintname, RestrictiveIntReasonName)
 -- (patient_injury, bame_group),
 -- (restrictiveintcode, restrictiveintname, patient_injury, bame_group)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_restr_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury, a.variabletype, a.variable, a.restraints, a.incidents, a.people, b.hosp_bed_days, 
 (a.restraints/b.hosp_bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.eng_restr_type_breakdowns_test a
 left join global_temp.eng_bed_days_restr_test b
 on a.variable = b.variable

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury, a.variabletype, a.variable, a.restraints, a.incidents, 
 coalesce(p.people, 0) as people, 
 coalesce(hb.bed_days, wb.bed_days, eb.hosp_bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days, eb.hosp_bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.total_breakdowns_restr a
 left join global_temp.total_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service
 and hb.age_group = 'England'
 and hb.gendercode = 'England'
 and hb.gendername = 'England'
 and hb.upperethnicitycode = 'England'
 and hb.upperethnicityname = 'England'
 and hb.lowerethnicitycode = 'England'
 and hb.lowerethnicity = 'England'
 and hb.provider_type = 'England'
 and hb.siteidoftreat = 'England' ---removed for ward_bed_days
 and hb.site_name = 'England' ---removed for ward_bed_days
 and hb.length_of_restraint = 'England'
 and hb.age_category = 'England'
 and hb.bame_group = 'England'
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service ----added for ward_bed_days
 and wb.age_group = 'England'
 and wb.gendercode = 'England'
 and wb.gendername = 'England'
 and wb.upperethnicitycode = 'England'
 and wb.upperethnicityname = 'England'
 and wb.lowerethnicitycode = 'England'
 and wb.lowerethnicity = 'England'
 and wb.provider_type = 'England'
 and wb.siteidoftreat = 'England'
 and wb.site_name = 'England'
 -- and wb.length_of_restraint = 'England'
 and wb.age_category = 'England'
 and wb.bame_group = 'England'
 left join global_temp.eng_bed_days_total_test eb
 on a.variable = eb.variable
 and a.region_code = 'England'
 and a.region_name = 'England'
 and a.orgidprov = 'England'
 and a.orgidname = 'England'
 and a.specialised_service = 'England'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_group_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury, 
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.age_group_breakdowns_restr a
 left join global_temp.age_group_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  ---removed for ward_bed_days
 and a.variable = hb.age_group
 and hb.upperethnicitycode = 'England' ---added to due to duplication
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.age_group
 and wb.upperethnicitycode = 'England' ---added to due to duplication
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW upper_eth_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.upper_eth_breakdowns_restr a
 left join global_temp.upper_eth_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  --removed for ward_bed_days
 and a.variable = hb.upperethnicityname
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.upperethnicityname
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW lower_eth_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.lower_eth_breakdowns_restr a
 left join global_temp.lower_eth_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  ---removed for ward_bed_days
 and a.variable = hb.lowerethnicity
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.lowerethnicity
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW site_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, wb.bed_days, (a.restraints/wb.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.site_breakdowns_restr a
 left join global_temp.site_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_ward_bed_days_pub_csv wb --site_level uses ward stay bed days exclusively
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.site_name
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW prov_type_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.prov_type_breakdowns_restr a
 left join global_temp.prov_type_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  --removed for ward_bed_days
 and a.variable = hb.provider_type
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.provider_type
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW gender_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.gender_breakdowns_restr a
 left join global_temp.gender_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  ---removed for ward_bed_days
 and a.variable = hb.gendername
 and hb.upperethnicitycode = 'England' ---added to due to duplication
 and hb.lowerethnicitycode = 'England' ---added to due to duplication
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.gendername
 and wb.upperethnicitycode = 'England' ---added to due to duplication
 and wb.lowerethnicitycode = 'England' ---added to due to duplication
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW restr_length_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, hb.bed_days as bed_days, (a.restraints/hb.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.restr_length_breakdowns_restr a
 left join global_temp.restr_length_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  ---removed for ward_bed_days
 and a.variable = hb.length_of_restraint
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_category_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.age_category_breakdowns_restr a
 left join global_temp.age_category_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  --removed for ward_bed_days
 and a.variable = hb.age_category
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.age_category
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bame_group_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, coalesce(hb.bed_days, wb.bed_days) as bed_days, (a.restraints/coalesce(hb.bed_days, wb.bed_days)) * 1000 as ri_per_1000_bed_days
 from global_temp.bame_group_breakdowns_restr a
 left join global_temp.bame_group_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable
 left join $db_output.dash_hosp_bed_days_pub_csv hb
 on a.region_code = hb.region_code
 and a.region_name = hb.region_name
 and a.orgidprov = hb.orgidprov
 and a.orgidname = hb.orgidname
 and a.specialised_service = hb.specialised_service  ---removed for ward_bed_days
 and a.variable = hb.bame_group
 left join $db_output.dash_ward_bed_days_pub_csv wb
 on a.region_code = wb.region_code
 and a.region_name = wb.region_name
 and a.orgidprov = wb.orgidprov
 and a.orgidname = wb.orgidname
 and a.specialised_service = wb.specialised_service  --added for ward_bed_days
 and a.variable = wb.bame_group
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW inc_reason_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, 
 a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.patient_injury,
 a.variabletype, a.variable, 
 a.restraints, a.incidents, coalesce(p.people, 0) as people, 9999 as bed_days, 9999 as ri_per_1000_bed_days
 from global_temp.inc_reason_breakdowns_restr a
 left join global_temp.inc_reason_breakdowns_people p
 on a.region_code = p.region_code
 and a.region_name = p.region_name
 and a.orgidprov = p.orgidprov
 and a.orgidname = p.orgidname
 and a.specialised_service = p.specialised_service 
 and a.restrictiveintcode = p.restrictiveintcode
 and a.restrictiveintname = p.restrictiveintname
 and a.patient_injury = p.patient_injury
 and a.variabletype = p.variabletype
 and a.variable = p.variable

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW pre_final_table AS
 -- insert into $db_output.pre_final_table
 Select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'England' as region_code, 
 'England' as region_name, 
 'England' as orgidprov, 
 'England' as orgidname, 
 'England' as specialised_service, 
 'England' as restrictiveintcode, 
 'England' as restrictiveintname, 
 'England' as patient_injury,
 'England' as variabletype,
 'England' as variable, 
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints,
 count(distinct UniqRestrictiveIntIncID) as incidents,
 count(distinct person_id) as people, 
 sum(Der_bed_days) as bed_days,
 (count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID)/sum(Der_bed_days)) * 1000 as ri_per_1000_bed_days
 from $db_output.RI_FINAL 
 union all
 select * from global_temp.eng_restr_breakdowns
 union all
 select * from global_temp.total_breakdowns
 union all
 select * from global_temp.age_group_breakdowns
 union all
 select * from global_temp.upper_eth_breakdowns
 union all
 select * from global_temp.lower_eth_breakdowns
 union all
 select * from global_temp.site_breakdowns
 union all
 select * from global_temp.prov_type_breakdowns
 union all
 select * from global_temp.gender_breakdowns
 union all
 select * from global_temp.restr_length_breakdowns
 union all
 select * from global_temp.age_category_breakdowns
 union all
 select * from global_temp.bame_group_breakdowns
 union all
 select * from global_temp.inc_reason_breakdowns

# COMMAND ----------

 %sql
 -- CREATE OR REPLACE GLOBAL TEMP VIEW final_table AS
 DROP TABLE IF EXISTS $db_output.final_table_prep;
 CREATE TABLE IF NOT EXISTS $db_output.final_table_prep AS

 -- DROP TABLE IF EXISTS $db_output.final_table;
 -- CREATE TABLE $db_output.final_table AS

 select 
 date_format(a.ReportingPeriodStartDate, "dd/MM/yyyy") as ReportingPeriodStartDate, 
 date_format(a.ReportingPeriodEndDate, "dd/MM/yyyy") as ReportingPeriodEndDate,
 a.region_code, 
 a.region_name, 
 a.orgidprov, 
 a.orgidname, 
 a.specialised_service, 
 a.restrictiveintcode, 
 a.restrictiveintname, 
 a.patient_injury,
 a.variabletype,
 a.variable,
 a.restraints,
 a.incidents,
 CASE WHEN (a.incidents > 0 and a.restraints > 0) THEN a.restraints / a.incidents ELSE 0 END as avg_restraints_per_incident,
 a.people,
 a.bed_days,
 a.ri_per_1000_bed_days,
 COALESCE(b.avg_min_of_restraint, 0) as avg_min_of_restraint,
 COALESCE(b.max_min_of_restraint, 0) as max_min_of_restraint,
 COALESCE(b.avg_min_of_incident, 0) as avg_min_of_incident,
 COALESCE(b.max_min_of_incident, 0) as max_min_of_incident
 from global_temp.pre_final_table a
 left join global_temp.total_breakdowns_time_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.restrictiveintcode = b.restrictiveintcode
 and a.restrictiveintname = b.restrictiveintname
 and a.patient_injury = b.patient_injury
 and a.variabletype = b.variabletype
 and a.variable = b.variable;

 OPTIMIZE $db_output.final_table_prep

# COMMAND ----------

 %sql
 -- insert overwrite table $db_output.final_table
 DROP TABLE IF EXISTS $db_output.final_table;
 CREATE TABLE IF NOT EXISTS $db_output.final_table AS

 SELECT
 ReportingPeriodStartDate
 ,ReportingPeriodEndDate
 ,region_code
 ,region_name
 ,orgidprov
 ,orgidname
 ,specialised_service
 ,restrictiveintcode
 ,restrictiveintname
 ,patient_injury
 ,variabletype
 ,variable
 ,restraints
 ,incidents
 ,avg_restraints_per_incident
 ,people
 ,bed_days
 ,ri_per_1000_bed_days
 ,avg_min_of_restraint
 ,max_min_of_restraint
 ,avg_min_of_incident	
 ,max_min_of_incident

 FROM

 (SELECT ReportingPeriodStartDate
 ,ReportingPeriodEndDate
 ,region_code
 ,region_name
 ,orgidprov
 ,orgidname
 ,specialised_service
 ,restrictiveintcode
 ,restrictiveintname
 ,patient_injury
 ,variabletype
 ,variable
 ,restraints
 ,incidents
 ,avg_restraints_per_incident
 ,people
 ,bed_days
 ,ri_per_1000_bed_days
 ,avg_min_of_restraint
 ,max_min_of_restraint
 ,avg_min_of_incident	
 ,max_min_of_incident
 ,count(*)
 FROM $db_output.final_table_prep
 GROUP BY
 ReportingPeriodStartDate
 ,ReportingPeriodEndDate
 ,region_code
 ,region_name
 ,orgidprov
 ,orgidname
 ,specialised_service
 ,restrictiveintcode
 ,restrictiveintname
 ,patient_injury
 ,variabletype
 ,variable
 ,restraints
 ,incidents
 ,avg_restraints_per_incident
 ,people
 ,bed_days
 ,ri_per_1000_bed_days
 ,avg_min_of_restraint
 ,max_min_of_restraint
 ,avg_min_of_incident	
 ,max_min_of_incident) T1

 --OPTIMIZE $db_output.final_table

# COMMAND ----------

 %sql
 insert into $db_output.rest_dash_hosp
 select 
 date_format('$rp_startdate', "dd/MM/yyyy") as reporting_period_start,
 date_format('$rp_enddate', "dd/MM/yyyy") as reporting_period_end,
 ri.orgidprov,
 orgidname,
 specialised_service,
 count(distinct ri.person_id) as restraint_people,
 hos.people as hosp_spell_people,
 (count(distinct ri.person_id) / hos.people) * 100 as perc_people
 from $db_output.RI_FINAL ri
 inner join $db_output.RI_HOSPITAL_PERC hos on ri.orgidprov = hos.orgidprov
 where ss_type_ward_Rank = 1
 group by ri.orgidprov, orgidname, specialised_service, hos.people
 having count(distinct ri.person_id) > 0
 order by ri.orgidprov, orgidname, hos.people

# COMMAND ----------

 %sql
 insert into $db_output.rest_dash_hosp
 select 
 date_format('$rp_startdate', "dd/MM/yyyy") as reporting_period_start,
 date_format('$rp_enddate', "dd/MM/yyyy") as reporting_period_end,
 ri.orgidprov,
 orgidname,
 'England' as specialised_service,
 count(distinct ri.person_id) as restraint_people,
 hos.people as hosp_spell_people,
 (count(distinct ri.person_id) / hos.people) * 100 as perc_people
 from $db_output.RI_FINAL ri
 inner join $db_output.RI_HOSPITAL_PERC hos on ri.orgidprov = hos.orgidprov
 group by ri.orgidprov, orgidname, hos.people
 having count(distinct ri.person_id) > 0
 order by ri.orgidprov, orgidname, hos.people;

 OPTIMIZE $db_output.rest_dash_hosp