# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Injury Status
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
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Age Category; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Age Category; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 age_category as level_two,
 age_category as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 age_category, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Age Group; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Age Group; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 age_group as level_two,
 age_group as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 age_group, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Gender; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Gender; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 gendercode as level_two,
 gendername as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 gendercode, gendername, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; BAME Group; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; BAME Group; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 bame_group as level_two,
 bame_group as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 bame_group, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Upper Ethnicity; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Upper Ethnicity; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 upperethnicitycode as level_two,
 upperethnicityname as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 upperethnicitycode, upperethnicityname, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Specialised Commissioning Service; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service as level_two,
 specialised_service as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null and ss_type_ward_Rank = 1)      -----WS flag changes----
 where patient_injury_max = 1
 GROUP BY 
 specialised_service, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS76 - England; Restrictive Intervention Type; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 patient_injury as level_three,
 patient_injury as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS76' as metric,
 count(distinct person_id) as metric_value
 from (select *, dense_rank() over (partition by person_id, restrictiveintcode order by patient_injury_binary) as patient_injury_max from $db_output.RI_FINAL where person_id is not null)
 where patient_injury_max = 1
 GROUP BY 
 restrictiveintcode, restrictiveintname, patient_injury

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output