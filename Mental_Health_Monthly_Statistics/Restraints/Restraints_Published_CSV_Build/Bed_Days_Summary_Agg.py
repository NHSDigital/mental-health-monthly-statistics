# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output

# COMMAND ----------

 %md
 WS Flag changes: MHSXX calculation using WardStay bed days for specialised_service & Provider site breakdowns

# COMMAND ----------

# DBTITLE 1,MHS96 - England
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL

# COMMAND ----------

# DBTITLE 1,MHS96 - Region
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,                                                            -----WS flag changes
 specialised_service_bd as level_two_description,                                                -----WS flag changes
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,                                                                           -----WS flag changes                                        
 specialised_service_bd as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd                                                     -----WS flag changes

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service
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
 specialised_service_bd as level_three,                                                                      -----WS flag changes
 specialised_service_bd as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd 

# COMMAND ----------

# DBTITLE 1,MHSXXa- Provider; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,                                        -----WS flag changes
 specialised_service_bd as level_two_description,
 'NULL' as level_three,
 'NULL' as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd         

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider Type
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by provider_type
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - England; BAME Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by bame_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Upper Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by UpperEthnicityCode, UpperEthnicityName
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Lower Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by LowerEthnicityCode, lowerethnicity
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Age Category
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by age_category
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - England; Age Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by age_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider Type
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, provider_type
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; BAME Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, bame_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Upper Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, UpperEthnicityCode, UpperEthnicityName
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Lower Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, LowerEthnicityCode, lowerethnicity
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Age Category
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, age_category
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Age Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, age_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider; Provider Type
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname, provider_type
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider; BAME Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname, bame_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider; Upper Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname, UpperEthnicityCode, UpperEthnicityName
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider; Lower Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname, LowerEthnicityCode, lowerethnicity
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider; Age Category
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname, age_category
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Provider; Age Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by orgidprov, orgidname, age_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Provider Type' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,                                                         -----WS flag changes
 specialised_service_bd as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd, provider_type                                              -----WS flag changes

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; BAME Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd, bame_group

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Age Category' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd, age_category

# COMMAND ----------

# DBTITLE 1,MHSXXa - England; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'England; Specialised Commissioning Service; Age Group' as breakdown,
 'England' as level_one,
 'England' as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by specialised_service_bd, age_group

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider; Provider Type
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname, provider_type
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider; BAME Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname, bame_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider; Upper Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname, UpperEthnicityCode, UpperEthnicityName
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider; Lower Ethnicity
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname, LowerEthnicityCode, lowerethnicity
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider; Age Category
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname, age_category
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHS96 - Region; Provider; Age Group
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
 'MHSXX' as metric,
 sum(der_bed_days) as metric_value
 from $db_output.RI_FINAL
 group by region_code, region_name, orgidprov, orgidname, age_group
 having count(distinct mhs515uniqid) > 0

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Provider Type' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd, provider_type

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; BAME Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd, bame_group

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Age Category' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd, age_category

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Region; Specialised Commissioning Service; Age Group' as breakdown,
 region_code as level_one,
 region_name as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, specialised_service_bd, age_group

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service; Provider Type
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
 specialised_service_bd as level_three,
 specialised_service_bd as level_three_description,
 provider_type as level_four,
 provider_type as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd, provider_type

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service; BAME Group
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
 specialised_service_bd as level_three,
 specialised_service_bd as level_three_description,
 bame_group as level_four,
 bame_group as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd, bame_group

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service; Upper Ethnicity
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
 specialised_service_bd as level_three,
 specialised_service_bd as level_three_description,
 UpperEthnicityCode as level_four,
 UpperEthnicityName as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service; Lower Ethnicity
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
 specialised_service_bd as level_three,
 specialised_service_bd as level_three_description,
 LowerEthnicityCode as level_four,
 lowerethnicity as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service; Age Category
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
 specialised_service_bd as level_three,
 specialised_service_bd as level_three_description,
 age_category as level_four,
 age_category as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_category

# COMMAND ----------

# DBTITLE 1,MHSXXa - Region; Provider; Specialised Commissioning Service; Age Group
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
 specialised_service_bd as level_three,
 specialised_service_bd as level_three_description,
 age_group as level_four,
 age_group as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_group

# COMMAND ----------

# DBTITLE 1,MHSXXa - Provider; Specialised Commissioning Service; Provider Type
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Provider Type' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 provider_type as level_three,
 provider_type as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd, provider_type

# COMMAND ----------

# DBTITLE 1,MHSXXa - Provider; Specialised Commissioning Service; BAME Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; BAME Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 bame_group as level_three,
 bame_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd, bame_group

# COMMAND ----------

# DBTITLE 1,MHSXXa - Provider; Specialised Commissioning Service; Upper Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Upper Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 UpperEthnicityCode as level_three,
 UpperEthnicityName as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd, UpperEthnicityCode, UpperEthnicityName

# COMMAND ----------

# DBTITLE 1,MHSXXa - Provider; Specialised Commissioning Service; Lower Ethnicity
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Lower Ethnicity' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 LowerEthnicityCode as level_three,
 lowerethnicity as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd, LowerEthnicityCode, lowerethnicity

# COMMAND ----------

# DBTITLE 1,MHSXXa - Provider; Specialised Commissioning Service; Age Category
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Age Category' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 age_category as level_three,
 age_category as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd, age_category

# COMMAND ----------

# DBTITLE 1,MHSXXa - Provider; Specialised Commissioning Service; Age Group
 %sql
 insert into $db_output.restraints_final_output
 select
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
 'Provider; Specialised Commissioning Service; Age Group' as breakdown,
 orgidprov as level_one,
 orgidname as level_one_description,
 specialised_service_bd as level_two,
 specialised_service_bd as level_two_description,
 age_group as level_three,
 age_group as level_three_description,
 'NULL' as level_four,
 'NULL' as level_four_description,
 'NULL' as level_five,
 'NULL' as level_five_description,
 'NULL' as level_six,
 'NULL' as level_six_description,
 'MHSXXa' as metric,
 sum(der_bed_days_ws) as metric_value
 from $db_output.RI_FINAL 
 where specialised_service_bd  <>  'No associated Ward Stay' 
 group by orgidprov, orgidname, specialised_service_bd, age_group

# COMMAND ----------

# DBTITLE 1,MHS77 - England; Gender (To add in future?)
# %sql
# insert into $db_output.restraints_final_output
# select
# '$rp_startdate' as ReportingPeriodStartDate,
# '$rp_enddate' as ReportingPeriodEndDate, '$status' as status,
# 'England; Gender' as breakdown,
# 'England' as level_one,
# 'England' as level_one_description,
# gendercode as level_two,
# gendername as level_two_description,
# 'NULL' as level_three,
# 'NULL' as level_three_description,
# 'NULL' as level_four,
# 'NULL' as level_four_description,
# 'NULL' as level_five,
# 'NULL' as level_five_description,
# 'NULL' as level_six,
# 'NULL' as level_six_description,
# 'MHSXX' as metric,
# sum(der_bed_days) as metric_value
# from $db_output.RI_FINAL

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output