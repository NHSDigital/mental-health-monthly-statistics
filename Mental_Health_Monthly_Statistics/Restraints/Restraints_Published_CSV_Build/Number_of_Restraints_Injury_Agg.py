# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

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
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 GROUP BY 
 patient_injury

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Restrictive Intervention Type; Age Category; Injury Status
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
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, age_category, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Restrictive Intervention Type; Gender; Injury Status
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
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, gendercode, gendername, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Restrictive Intervention Type; BAME Group; Injury Status
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
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, bame_group, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Restrictive Intervention Type; Upper Ethnicity; Injury Status
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
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, upperethnicitycode, upperethnicityname, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Restrictive Intervention Type; Length of Restraint; Injury Status
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
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null
 GROUP BY 
 restrictiveintcode, restrictiveintname, length_of_restraint, patient_injury

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Restrictive Intervention Type; Specialised Commissioning Service; Injury Status
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Restrictive Intervention Type; Specialised Commissioning Service; Injury Status' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 restrictiveintcode as level_two,
 restrictiveintname as level_two_description,
 specialised_service as level_three,
 specialised_service as level_three_description,
 patient_injury as level_four,
 patient_injury as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHS77' as metric,
 count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as metric_value
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1                          -------------------------WS flag changes-------
 GROUP BY 
 restrictiveintcode, restrictiveintname, specialised_service, patient_injury

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output