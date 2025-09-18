# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS76 - England
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null

# COMMAND ----------

# DBTITLE 1,MHS76 - Region
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by orgidprov, orgidname

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider Type' as breakdown,
 provider_type as level_one,
 provider_type as level_one_description,
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - England; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; BAME Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 bame_group as level_two,
 bame_group as level_two_description,
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
 group by bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Upper Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 UpperEthnicityCode as level_two,
 UpperEthnicityName as level_two_description,
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
 group by UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Lower Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 LowerEthnicityCode as level_two,
 lowerethnicity as level_two_description,
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
 group by LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Age Category' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 age_category as level_two,
 age_category as level_two_description,
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
 group by age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Age Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 age_group as level_two,
 age_group as level_two_description,
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
 group by age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 provider_type as level_two,
 provider_type as level_two_description,
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
 group by region_code, region_name, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 bame_group as level_two,
 bame_group as level_two_description,
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
 group by region_code, region_name, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 UpperEthnicityCode as level_two,
 UpperEthnicityName as level_two_description,
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
 group by region_code, region_name, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 LowerEthnicityCode as level_two,
 lowerethnicity as level_two_description,
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
 group by region_code, region_name, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 age_category as level_two,
 age_category as level_two_description,
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
 group by region_code, region_name, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 age_group as level_two,
 age_group as level_two_description,
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
 group by region_code, region_name, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Provider Type' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 provider_type as level_two,
 provider_type as level_two_description,
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
 group by orgidprov, orgidname, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; BAME Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 bame_group as level_two,
 bame_group as level_two_description,
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
 group by orgidprov, orgidname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Upper Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 UpperEthnicityCode as level_two,
 UpperEthnicityName as level_two_description,
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
 group by orgidprov, orgidname, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Lower Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 LowerEthnicityCode as level_two,
 lowerethnicity as level_two_description,
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
 group by orgidprov, orgidname, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Age Category' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 age_category as level_two,
 age_category as level_two_description,
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
 group by orgidprov, orgidname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Age Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 age_group as level_two,
 age_group as level_two_description,
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
 group by orgidprov, orgidname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Provider Type' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; BAME Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Age Category' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Age Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by specialised_service, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Provider Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
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
 group by region_code, region_name, orgidprov, orgidname, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
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
 group by region_code, region_name, orgidprov, orgidname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
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
 group by region_code, region_name, orgidprov, orgidname, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
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
 group by region_code, region_name, orgidprov, orgidname, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
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
 group by region_code, region_name, orgidprov, orgidname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
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
 group by region_code, region_name, orgidprov, orgidname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Provider Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Provider Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 provider_type as level_four,
 provider_type as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 bame_group as level_four,
 bame_group as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 UpperEthnicityCode as level_four,
 UpperEthnicityName as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 LowerEthnicityCode as level_four,
 lowerethnicity as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 age_category as level_four,
 age_category as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 age_group as level_four,
 age_group as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Provider Type' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, provider_type

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; BAME Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Age Category' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Age Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Incident Reason
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by RestrictiveIntReasonCode, RestrictiveIntReasonName

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Incident Reason
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, RestrictiveIntReasonCode, RestrictiveIntReasonName

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output