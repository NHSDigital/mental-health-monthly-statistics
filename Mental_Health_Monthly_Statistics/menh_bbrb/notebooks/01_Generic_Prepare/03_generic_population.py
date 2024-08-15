# Databricks notebook source
 %sql
 INSERT OVERWRITE TABLE $db_output.age_band_pop
 select a.Age_Group_IPS, a.Age_Group_OAPs,
 SUM(POPULATION_COUNT) as POPULATION_COUNT    
 from $reference_data.ons_population_v2 p
 left join $db_output.age_band_desc a on p.AGE_LOWER = a.AgeRepPeriodEnd
 where year_of_count = (select max(year_of_count) from $reference_data.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38') ---looks at latest data rather than depending on what month is being ran
 and GEOGRAPHIC_GROUP_CODE = 'E38'
 and trim(RECORD_TYPE) = 'E'
 group by a.Age_Group_IPS, a.Age_Group_OAPs

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.gender_pop
 select 
 case when GENDER = "M" then 1
      when GENDER = "F" then 2
      end as Der_Gender,
 SUM(POPULATION_COUNT) as POPULATION_COUNT    
 from $reference_data.ons_population_v2
 where year_of_count = (select max(year_of_count) from $reference_data.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38') ---looks at latest data rather than depending on what month is being ran $$$
 and GEOGRAPHIC_GROUP_CODE = 'E38'
 and trim(RECORD_TYPE) = 'E'
 group by 
 case when GENDER = "M" then 1
      when GENDER = "F" then 2
      end

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.higher_eth_pop
 select 
 case 
      when Ethnic_group = "White" then "White or White British"
      when Ethnic_group = "Black" then "Black or Black British"
      when Ethnic_group = "Asian" then "Asian or Asian British"
      when Ethnic_group = "Other" then "Other Ethnic Groups"
      else Ethnic_group end as UpperEthnicity,
 SUM(EthPop_value) as POPULATION_COUNT
 from $reference_data.ccg_ethpop
 where Ethnic_group != "All" and Period = (select max(Period) from $reference_data.ccg_ethpop) ---looks at latest data rather than depending on what month is being ran $$$
 group by 
 case 
      when Ethnic_group = "White" then "White or White British"
      when Ethnic_group = "Black" then "Black or Black British"
      when Ethnic_group = "Asian" then "Asian or Asian British"
      when Ethnic_group = "Other" then "Other Ethnic Groups"
      else Ethnic_group end

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.imd_pop
 ----IAPT contains IMD ref data --is this hard-coded?
 VALUES
 ('10 Least deprived', 5413926),
 ('09 Less deprived', 5487156),
 ('08 Less deprived', 5554096),
 ('07 Less deprived', 5563598),
 ('06 Less deprived', 5733511),
 ('05 More deprived', 5692582),
 ('04 More deprived', 5766656),
 ('03 More deprived', 5809906),
 ('02 More deprived', 5677439),
 ('01 Most deprived', 5588091)

# COMMAND ----------

 %sql
 /************************************************
  
 The extract counts differ from the below hardcoded values and an analyst must verify that we agree with these numbers!
  
 should we parameterise:
  
 1. pop_v2.year_of_count = '2020'
 2. successorDetails.startDate > '2021-03-31'
 3. CCG_2020_CODE, CCG_2020_name,  CCG_2021_CODE, CCG_2021_name - the Names to reflect the year selected above..
  
  
 ************************************************/
  
 with reference_list_rn as
 (
 select 
       age_lower
 --       predecessor ID
      ,geo.DH_GEOGRAPHY_CODE
 --       successor ID
       ,SuccessorDetails.TargetOrganisationID
      --predecessor ID
       ,SuccessorDetails.OrganisationID
       ,row_number() over (partition by age_lower, geo.DH_GEOGRAPHY_CODE order by geo.year_of_change desc) as RN
 ,sum(pop_v2.population_count) as all_ages
 from $reference_data.ONS_POPULATION_V2 pop_v2
 inner join $reference_data.ONS_CHD_GEO_EQUIVALENTS geo
 on pop_v2.GEOGRAPHIC_SUBGROUP_CODE = geo.geography_code 
 Left join $reference_data.odsapisuccessordetails successorDetails 
 on geo.DH_GEOGRAPHY_CODE = successorDetails.Organisationid and successorDetails.Type = 'Successor' and successorDetails.startDate > '2021-03-31'--and geo.IS_CURRENT =1
 where pop_v2.GEOGRAPHIC_GROUP_CODE = 'E38'
 and geo.ENTITY_CODE = 'E38'
 -- and geo.IS_CURRENT =1
 and pop_v2.year_of_count = $year_of_count
 and pop_v2.RECORD_TYPE = 'E'
 -- and DH_GEOGRAPHY_CODE = '99M'
 group by age_lower, geo.DH_GEOGRAPHY_CODE,successorDetails.TargetOrganisationID,successorDetails.OrganisationID,geo.year_of_change
 order by geo.DH_GEOGRAPHY_CODE
  
 ),
 reference_list as 
 (
 select * from reference_list_rn where RN = 1
 )
 --when successorDetails.Organisationid is not null then we do have a successor organisation after 31 Mar 2021
 --likewise when successorDetails.OrganisationID is null then predecessor and successor orgs are the same (ie no successor)
 ,no_successor as
 (select 
         case when (age_lower > 17 and age_lower < 65) then "18-64"
              when age_lower > 64 then "65+"
              when age_lower < 18 then "0-17" end as AGE_GROUP,
         org1.OrganisationId as ccg_successor_code,
         org1.name as ccg_successor_name,
         org1.name as ccg_predecessor_name, 
         org1.OrganisationId as ccg_predecessor_code,
         $year_of_count as ONS_POPULATION_V2_year_of_count,
         sum(ref.All_Ages) as POPULATION_COUNT
 from reference_list ref
 inner join $reference_data.ODSAPIOrganisationDetails org1
 on ref.DH_GEOGRAPHY_CODE = org1.OrganisationId
 where ref.OrganisationID is null
 and org1.EndDate is null
 AND org1.DateType = 'Operational'
 group by case when (age_lower > 17 and age_lower < 65) then "18-64"
              when age_lower > 64 then "65+"
              when age_lower < 18 then "0-17" end,
         org1.OrganisationId,
         org1.name,
         org1.name, 
         org1.OrganisationId
 order by ccg_predecessor_code   
 )
  
 ,with_successor_this_one_predecessor as
 (select 
         case when (age_lower > 17 and age_lower < 65) then "18-64"
              when age_lower > 64 then "65+"
              when age_lower < 18 then "0-17" end as AGE_GROUP,
         ref2.OrganisationID as id,
         ref2.DH_GEOGRAPHY_CODE,
         org2.name as name, 
         org2.OrganisationId as Org_CODE,
         sum(ref2.All_Ages) as POPULATION_COUNT
 from reference_list ref2
 inner join $reference_data.ODSAPIOrganisationDetails org2
 on ref2.DH_GEOGRAPHY_CODE = org2.OrganisationId
 where org2.DateType = 'Operational'
 group by case when (age_lower > 17 and age_lower < 65) then "18-64"
              when age_lower > 64 then "65+"
              when age_lower < 18 then "0-17" end,
         ref2.OrganisationID,
         ref2.DH_GEOGRAPHY_CODE,
         org2.name, 
         org2.OrganisationId
 )
 ,with_successor_this_one_successor as
 (select    ref3.OrganisationID as id,
         org3.name as name, 
         ref3.TargetOrganisationId as Org_CODE
 from reference_list ref3
 inner join $reference_data.ODSAPIOrganisationDetails org3
 on ref3.TargetOrganisationId = org3.OrganisationId
  
 where org3.DateType = 'Operational'
 )
 , with_successor as
 (select  
       org_predecessor.AGE_GROUP,
       org_successor.org_code as ccg_successor_code,
       org_successor.Name as ccg_successor_name,
       org_predecessor.Name as ccg_predecessor_name,
       org_predecessor.org_code as ccg_predecessor_code,
       $year_of_count as ONS_POPULATION_V2_year_of_count,
       org_predecessor.POPULATION_COUNT as POPULATION_COUNT
 from with_successor_this_one_predecessor org_predecessor
 inner join with_successor_this_one_successor org_successor
 on org_predecessor.id = org_successor.id
 order by ccg_predecessor_code
 )
 INSERT OVERWRITE TABLE $db_output.ccg_pop
 (
 select * from no_successor
 union
 select * from with_successor
 )

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.eng_pop
 SELECT 
 case when (age_lower > 17 and age_lower < 65) then "18-64"
      when age_lower > 64 then "65+"
      when age_lower < 18 then "0-17" end as AGE_GROUP,
 SUM(POPULATION_COUNT) as POPULATION_COUNT
 from $reference_data.ons_population_v2 where year_of_count = (select max(year_of_count) from $reference_data.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38') and GEOGRAPHIC_GROUP_CODE = 'E38' and trim(RECORD_TYPE) = 'E'
 group by case when (age_lower > 17 and age_lower < 65) then "18-64"
      when age_lower > 64 then "65+"
      when age_lower < 18 then "0-17" end