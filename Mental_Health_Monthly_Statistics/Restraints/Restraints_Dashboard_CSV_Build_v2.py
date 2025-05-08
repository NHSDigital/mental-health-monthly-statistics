# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_bed_days_total_test AS
 select 
 'Total' as variable,
 sum(Der_bed_days) as bed_days
 from $db_output.RI_FINAL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_bed_days_restr_test AS
 select 
 'Restrictive Intervention Type' as variable,
 sum(Der_bed_days) as bed_days
 from $db_output.RI_FINAL

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bed_days_total_test AS
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
 coalesce(specialised_service, 'England') as specialised_service, 
 coalesce(siteidoftreat, 'England') as siteidoftreat,
 coalesce(site_name, 'England') as site_name,
 coalesce(provider_type, 'England') as provider_type,
 coalesce(length_of_restraint, 'England') as length_of_restraint,
 coalesce(age_category, 'England') as age_category,
 coalesce(bame_group, 'England') as bame_group,
 sum(Der_bed_days) as bed_days
 from $db_output.RI_FINAL
 group by grouping sets (
 (age_group),
 (gendercode, gendername),
 (upperethnicitycode, upperethnicityname),
 (lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname),
 (region_code, region_name),
 (specialised_service),
 (siteidoftreat, site_name),
 (provider_type),
 (length_of_restraint),
 (age_category),
 (bame_group),
 (region_code, region_name, age_group),
 (region_code, region_name, gendercode, gendername),
 (region_code, region_name, upperethnicitycode, upperethnicityname),
 (region_code, region_name, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, siteidoftreat, site_name),
 (region_code, region_name, provider_type),
 (region_code, region_name, length_of_restraint),
 (region_code, region_name, age_category),
 (region_code, region_name, bame_group),
 (orgidprov, orgidname, age_group),
 (orgidprov, orgidname, gendercode, gendername),
 (orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, siteidoftreat, site_name),
 (orgidprov, orgidname, provider_type),
 (orgidprov, orgidname, length_of_restraint),
 (orgidprov, orgidname, age_category),
 (orgidprov, orgidname, bame_group),
 (specialised_service, age_group),
 (specialised_service, gendercode, gendername),
 (specialised_service, upperethnicitycode, upperethnicityname),
 (specialised_service, lowerethnicitycode, lowerethnicity),
 (specialised_service, siteidoftreat, site_name),
 (specialised_service, provider_type),
 (specialised_service, length_of_restraint),
 (specialised_service, age_category),
 (specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, orgidprov, orgidname, age_group),
 (region_code, region_name, orgidprov, orgidname, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, siteidoftreat, site_name),
 (region_code, region_name, orgidprov, orgidname, provider_type),
 (region_code, region_name, orgidprov, orgidname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, age_category),
 (region_code, region_name, orgidprov, orgidname, bame_group),
 (region_code, region_name, specialised_service),
 (region_code, region_name, specialised_service, age_group),
 (region_code, region_name, specialised_service, gendercode, gendername),
 (region_code, region_name, specialised_service, upperethnicitycode, upperethnicityname),
 (region_code, region_name, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service, siteidoftreat, site_name),
 (region_code, region_name, specialised_service, provider_type),
 (region_code, region_name, specialised_service, length_of_restraint),
 (region_code, region_name, specialised_service, age_category),
 (region_code, region_name, specialised_service, bame_group),
 (orgidprov, orgidname, specialised_service),
 (orgidprov, orgidname, specialised_service, age_group),
 (orgidprov, orgidname, specialised_service, gendercode, gendername),
 (orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service, siteidoftreat, site_name),
 (orgidprov, orgidname, specialised_service, provider_type),
 (orgidprov, orgidname, specialised_service, length_of_restraint),
 (orgidprov, orgidname, specialised_service, age_category),
 (orgidprov, orgidname, specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service, siteidoftreat, site_name),
 (region_code, region_name, orgidprov, orgidname, specialised_service, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group)
 )

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
 'Restrictive Intervention Type' as variabletype, 
 'Restrictive Intervention Type' as variable, 
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Total' as variabletype,
 'Total' as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL 
 where person_id is not null
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

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns_time_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Total' as variabletype,
 'Total' as variable,
 sum(minutes_of_restraint)/count(distinct mhs505uniqid) as avg_min_of_restraint,
 max(minutes_of_restraint) as max_min_of_restraint
 from $db_output.RI_FINAL
 where avg_min_flag = 'Y' and person_id is not null
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

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_group_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Age Group' as variabletype,
 age_group as variable, 
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW upper_eth_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Upper Ethnicity' as variabletype,
 upperethnicityname as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW lower_eth_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Lower Ethnicity' as variabletype,
 lowerethnicity as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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
 CREATE OR REPLACE GLOBAL TEMP VIEW site_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Provider Site' as variabletype,
 site_name as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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
 CREATE OR REPLACE GLOBAL TEMP VIEW prov_type_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Provider Type' as variabletype,
 provider_type as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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
 CREATE OR REPLACE GLOBAL TEMP VIEW gender_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Gender' as variabletype,
 gendername as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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
 (restrictiveintcode, restrictiveintname, gendername)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW restr_length_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Restraint Length' as variabletype,
 length_of_restraint as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
 group by grouping sets (
 (region_code, region_name, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (orgidprov, orgidname, restrictiveintcode, restrictiveintname, length_of_restraint),
 (orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (specialised_service, restrictiveintcode, restrictiveintname, length_of_restraint),
 (restrictiveintcode, restrictiveintname, length_of_restraint)
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_category_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'Age Category' as variabletype,
 age_category as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bame_group_breakdowns_test AS 
 select 
 coalesce(region_code, 'England') as region_code, 
 coalesce(region_name, 'England') as region_name,  
 coalesce(orgidprov, 'England') as orgidprov, 
 coalesce(orgidname, 'England') as orgidname,
 coalesce(specialised_service , 'England') as specialised_service,
 coalesce(restrictiveintcode, 'England') as restrictiveintcode,
 coalesce(restrictiveintname, 'England') as restrictiveintname,
 'BAME Group' as variabletype,
 bame_group as variable,
 count(distinct mhs505uniqid) as restraints, count(distinct person_id) as people
 from $db_output.RI_FINAL
 where person_id is not null
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

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW eng_restr_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.eng_restr_type_breakdowns_test a
 left join global_temp.eng_bed_days_restr_test b
 on a.variable = b.variable

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW total_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.total_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and age_group = 'England'
 and gendercode = 'England'
 and gendername = 'England'
 and upperethnicitycode = 'England'
 and upperethnicityname = 'England'
 and lowerethnicitycode = 'England'
 and lowerethnicity = 'England'
 and provider_type = 'England'
 and siteidoftreat = 'England'
 and site_name = 'England'
 and length_of_restraint = 'England'
 and age_category = 'England'
 and bame_group = 'England'
 UNION ALL
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.total_breakdowns_test a
 left join global_temp.eng_bed_days_total_test b
 on a.variable = b.variable
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
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.age_group_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.age_group
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW upper_eth_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.upper_eth_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.upperethnicityname
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW lower_eth_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.lower_eth_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.lowerethnicity
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW site_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.site_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.site_name
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW prov_type_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.prov_type_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.provider_type
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW gender_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.gender_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.gendername
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW restr_length_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.restr_length_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.length_of_restraint
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW age_category_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.age_category_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.age_category
 order by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bame_group_breakdowns AS
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 a.region_code, a.region_name, a.orgidprov, a.orgidname, a.specialised_service, a.restrictiveintcode, a.restrictiveintname, a.variabletype, a.variable, a.restraints, a.people, b.bed_days, 
 (a.restraints/b.bed_days) * 1000 as ri_per_1000_bed_days
 from global_temp.bame_group_breakdowns_test a
 left join global_temp.bed_days_total_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.variable = b.bame_group
 order by a.orgidprov

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
 'England' as variabletype,
 'England' as variable, 
 count(distinct mhs505uniqid) as restraints, 
 count(distinct person_id) as people, 
 sum(Der_bed_days) as bed_days,
 (count(distinct mhs505uniqid)/sum(Der_bed_days)) * 1000 as ri_per_1000_bed_days
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

# COMMAND ----------

 %sql
 -- CREATE OR REPLACE GLOBAL TEMP VIEW final_table AS
 insert into $db_output.final_table
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
 a.variabletype,
 a.variable,
 a.restraints, 
 a.people,
 a.bed_days,
 a.ri_per_1000_bed_days,
 COALESCE(b.avg_min_of_restraint, 0) as avg_min_of_restraint,
 COALESCE(b.max_min_of_restraint, 0) as max_min_of_restraint
 from global_temp.pre_final_table a
 left join global_temp.total_breakdowns_time_test b
 on a.region_code = b.region_code
 and a.region_name = b.region_name
 and a.orgidprov = b.orgidprov
 and a.orgidname = b.orgidname
 and a.specialised_service = b.specialised_service
 and a.restrictiveintcode = b.restrictiveintcode
 and a.restrictiveintname = b.restrictiveintname
 and a.variable = b.variable

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
 order by ri.orgidprov, orgidname, hos.people

# COMMAND ----------

