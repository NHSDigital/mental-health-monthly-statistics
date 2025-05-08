# Databricks notebook source
 %sql
 REFRESH TABLE $db_output.restraints_final_output1

# COMMAND ----------

# DBTITLE 1,MHS97 - Provider
 %sql
 insert into $db_output.restraints_final_output1
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Provider' as breakdown,
 ri.orgidprov as level_one,
 ri.orgidname as level_one_description,
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
 'MHS97' as metric,
 (count(distinct ri.person_id) / hos.people) * 100 as metric_value
 from $db_output.RI_FINAL ri
 inner join $db_output.RI_HOSPITAL_PERC hos on ri.orgidprov = hos.orgidprov
 group by ri.orgidprov, ri.orgidname, hos.people
 having count(distinct ri.person_id) > 0

# COMMAND ----------

# DBTITLE 1,MHS97 - Provider; Specialised Commissioning Service
 %sql
 insert into $db_output.restraints_final_output1
 select 
 '$rp_startdate' as ReportingPeriodStartDate,
 '$rp_enddate' as ReportingPeriodEndDate,
 'Provider; Specialised Commissioning Service' as breakdown,
 ri.orgidprov as level_one,
 ri.orgidname as level_one_description,
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
 'MHS97' as metric,
 (count(distinct ri.person_id) / hos.people) * 100 as metric_value
 from $db_output.RI_FINAL ri
 inner join $db_output.RI_HOSPITAL_PERC hos on ri.orgidprov = hos.orgidprov
 group by ri.orgidprov, ri.orgidname, ri.specialised_service, hos.people
 having count(distinct ri.person_id) > 0

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.restraints_final_output1