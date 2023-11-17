# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Age Category' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Age Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; BAME Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Gender' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
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
 group by restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 upperethnicitycode as level_three,
 upperethnicityname as level_three_description,
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
 group by restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 lowerethnicitycode as level_three,
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
 group by restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Incident Reason
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Incident Reason' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 RestrictiveIntReasonCode as level_three,
 RestrictiveIntReasonName as level_three_description,
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
 group by restrictiveintcode, restrictiveintname, RestrictiveIntReasonCode, RestrictiveIntReasonName

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by region_code, region_name, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by region_code, region_name, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by region_code, region_name, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type; Gender' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
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
 group by region_code, region_name, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 upperethnicitycode as level_three,
 upperethnicityname as level_three_description,
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
 group by region_code, region_name, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 lowerethnicitycode as level_three,
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
 group by region_code, region_name, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type; Age Category' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type; Age Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type; BAME Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type; Gender' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 upperethnicitycode as level_three,
 upperethnicityname as level_three_description,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 lowerethnicitycode as level_three,
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
 group by orgidprov, orgidname, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type; Age Category' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by specialised_service, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type; Age Group' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by specialised_service, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type; BAME Group' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
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
 group by specialised_service, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type; Gender' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
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
 group by specialised_service, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 upperethnicitycode as level_three,
 upperethnicityname as level_three_description,
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
 group by specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 specialised_service as level_one,
 specialised_service as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 lowerethnicitycode as level_three,
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
 group by specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 age_category as level_four,
 age_category as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null 
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 age_group as level_four,
 age_group as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null 
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 bame_group as level_four,
 bame_group as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type; Gender' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 gendercode as level_four,
 gendername as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 upperethnicitycode as level_four,
 upperethnicityname as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 lowerethnicitycode as level_four,
 lowerethnicity as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by region_code, region_name, orgidprov, orgidname, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
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
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
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
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
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
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; Gender' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 gendercode as level_four,
 gendername as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 upperethnicitycode as level_four,
 upperethnicityname as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 lowerethnicitycode as level_four,
 lowerethnicity as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 age_category as level_five,
 age_category as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 age_group as level_five,
 age_group as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 bame_group as level_five,
 bame_group as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Gender' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 gendercode as level_five,
 gendername as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 upperethnicitycode as level_five,
 upperethnicityname as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Provider; Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 orgidprov as level_two,
 orgidname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 restrictiveintcode as level_four,
 restrictiveintname as level_four_description,
 lowerethnicitycode as level_five,
 lowerethnicity as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by region_code, region_name, orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Category' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
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
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_category

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Age Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
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
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, age_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; BAME Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
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
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, bame_group

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Gender
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Gender' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 gendercode as level_four,
 gendername as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, gendercode, gendername

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Upper Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 upperethnicitycode as level_four,
 upperethnicityname as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - Provider; Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Restrictive Intervention Type; Lower Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 restrictiveintcode as level_three,
 restrictiveintname as level_three_description,
 lowerethnicitycode as level_four,
 lowerethnicity as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 group by orgidprov, orgidname, specialised_service, restrictiveintcode, restrictiveintname, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Gender; Upper Ethnicity - TO CHECK
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Gender; Upper Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
 upperethnicitycode as level_four,
 upperethnicityname as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname, gendercode, gendername, upperethnicitycode, upperethnicityname

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Gender; Lower Ethnicity - TO CHECK
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Gender; Lower Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 gendercode as level_three,
 gendername as level_three_description,
 lowerethnicitycode as level_four,
 lowerethnicity as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname, gendercode, gendername, lowerethnicitycode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Age Group; Upper Ethnicity - TO CHECK
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Age Group; Upper Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 upperethnicitycode as level_four,
 upperethnicityname as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 group by restrictiveintcode, restrictiveintname, age_group, upperethnicitycode, upperethnicityname

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output