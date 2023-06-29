# Databricks notebook source
 %md
 ### Geographic Reference data

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhsds_org_daily;
 
 CREATE TABLE IF NOT EXISTS $db_output.mhsds_org_daily USING DELTA AS
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $ref_database.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate'               

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhsds_org_relationship_daily;
 CREATE TABLE $db_output.mhsds_org_relationship_daily USING DELTA AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $ref_database.org_relationship_daily
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhsds_stp_mapping;
 CREATE TABLE $db_output.mhsds_stp_mapping USING DELTA AS 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.mhsds_org_daily A
 LEFT JOIN $db_output.mhsds_org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.mhsds_org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.mhsds_org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.mhsds_org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST' 
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.rd_ccg_latest; 
 CREATE TABLE IF NOT EXISTS $db_output.rd_ccg_latest
 ( 
   original_ORG_CODE string,
   original_NAME string,
   ORG_CODE string,
   NAME string
 ) USING DELTA

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW mappedCCGs
  
 AS SELECT DISTINCT od.ORG_CODE as original_ORG_CODE,
                 od.NAME as original_NAME,
                 COALESCE(odssd.TargetOrganisationID, od.ORG_CODE) as ORG_CODE
         FROM $ref_database.org_daily od
  
          
         LEFT JOIN $ref_database.ODSAPISuccessorDetails as odssd
         ON od.ORG_CODE = odssd.OrganisationID and odssd.Type = 'Successor' and odssd.StartDate <= '$rp_enddate'
         
            WHERE (od.BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(od.BUSINESS_END_DATE))
                 AND od.BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
                 AND od.ORG_TYPE_CODE = 'CC'
                 AND (od.ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(od.ORG_CLOSE_DATE))
                 AND od.ORG_OPEN_DATE <= '$rp_enddate'
                 AND od.NAME NOT LIKE '%HUB'
                 AND od.NAME NOT LIKE '%NATIONAL%';
 
  
 INSERT INTO TABLE $db_output.rd_ccg_latest
  
 SELECT mappedCCGs.*,
       od1.NAME as NAME
     FROM mappedCCGs
     LEFT JOIN (
             SELECT
             row_number() over (partition by ORG_CODE order by case when ORG_CLOSE_DATE is null then 1 else 0 end desc, case when BUSINESS_END_DATE is null then 1 else 0 end desc, ORG_CLOSE_DATE desc) as RN,
             ORG_CODE,
             NAME
             FROM $ref_database.org_daily
                     
             WHERE ORG_OPEN_DATE <= '$rp_enddate'
             ) od1
             ON mappedCCGs.ORG_CODE = od1.ORG_CODE
             AND od1.RN = 1
           
 ORDER BY original_ORG_CODE

# COMMAND ----------

 %md
 ### Population Reference data

# COMMAND ----------

 %sql
 drop table if exists $db_output.ips_age_pop;   
 create table if not exists $db_output.ips_age_pop as
 select 
 case when AGE_LOWER between 0 and 19 then '0 to 19'
      when AGE_LOWER between 20 and 29 then '20 to 29'
      when AGE_LOWER between 30 and 39 then '30 to 39'
      when AGE_LOWER between 40 and 49 then '40 to 49'
      when AGE_LOWER between 50 and 59 then '50 to 59'
      when AGE_LOWER between 60 and 69 then '60 to 69'
      else '70 and over' end as Age_Band,
 SUM(POPULATION_COUNT) as POPULATION_COUNT    
 from $ref_database.ons_population_v2
 where year_of_count = (select max(year_of_count) from $ref_database.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38')
 and GEOGRAPHIC_GROUP_CODE = 'E38'
 and trim(RECORD_TYPE) = 'E'
 group by 
 case when AGE_LOWER between 0 and 19 then '0 to 19'
      when AGE_LOWER between 20 and 29 then '20 to 29'
      when AGE_LOWER between 30 and 39 then '30 to 39'
      when AGE_LOWER between 40 and 49 then '40 to 49'
      when AGE_LOWER between 50 and 59 then '50 to 59'
      when AGE_LOWER between 60 and 69 then '60 to 69'
      else '70 and over' end

# COMMAND ----------

 %sql
 drop table if exists $db_output.ips_gender_pop;   
 create table if not exists $db_output.ips_gender_pop as
 select 
 case when GENDER = "M" then 1
      when GENDER = "F" then 2
      end as Der_Gender,
 SUM(POPULATION_COUNT) as POPULATION_COUNT    
 from $ref_database.ons_population_v2
 where year_of_count = (select max(year_of_count) from $ref_database.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38')
 and GEOGRAPHIC_GROUP_CODE = 'E38'
 and trim(RECORD_TYPE) = 'E'
 group by case when GENDER = "M" then 1
      when GENDER = "F" then 2
      end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_higher_eth_pop;
 CREATE TABLE IF NOT EXISTS $db_output.ips_higher_eth_pop AS
 select 
 case when Ethnic_group = "Black" then "Black or Black British"
      when Ethnic_group = "Asian" then "Asian or Asian British"
      when Ethnic_group = "Other" then "Other Ethnic Groups"
      else Ethnic_group end as UpperEthnicity,
 SUM(EthPop_value) as POPULATION_COUNT
 from $ref_database.ccg_ethpop
 where Ethnic_group != "All"
 group by case when Ethnic_group = "Black" then "Black or Black British"
      when Ethnic_group = "Asian" then "Asian or Asian British"
      when Ethnic_group = "Other" then "Other Ethnic Groups"
      else Ethnic_group end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_imd_pop;
 CREATE TABLE IF NOT EXISTS $db_output.ips_imd_pop
 (      
        IMD_Decile                STRING,
        POPULATION_COUNT                     INT
 )
  using delta

# COMMAND ----------

import pandas as pd
import io

db_output = dbutils.widgets.get("db_output")

data = """
IMD_Decile,POPULATION_COUNT
10 Least deprived,5413926
09 Less deprived,5487156
08 Less deprived,5554096
07 Less deprived,5563598
06 Less deprived,5733511
05 More deprived,5692582
04 More deprived,5766656
03 More deprived,5809906
02 More deprived,5677439
01 Most deprived,5588091
"""
df = pd.read_csv(io.StringIO(data), header=0, delimiter=',').astype(str)
spark.createDataFrame(df).write.insertInto(f"{db_output}.ips_imd_pop")

# COMMAND ----------

 %sql
 ----SUB ICB Population
 create or replace temp view reference_list_rn as
 select 
      geo.DH_GEOGRAPHY_CODE
       ,SuccessorDetails.TargetOrganisationID
       ,SuccessorDetails.OrganisationID
       ,row_number() over (partition by geo.DH_GEOGRAPHY_CODE order by geo.year_of_change desc) as RN
 ,sum(pop_v2.population_count) as all_ages
 ,sum(case when (pop_v2.age_lower > 17 and pop_v2.age_lower < 65) then pop_v2.population_count else 0 end) as Aged_18_to_64
 ,sum(case when (pop_v2.age_lower > 64) then pop_v2.population_count else 0 end) as Aged_65_OVER
 ,sum(case when (pop_v2.age_lower < 18) then pop_v2.population_count else 0 end) as Aged_0_to_17
 
 from $ref_database.ONS_POPULATION_V2 pop_v2
 inner join  $ref_database.ONS_CHD_GEO_EQUIVALENTS geo
 on pop_v2.GEOGRAPHIC_SUBGROUP_CODE = geo.geography_code 
 Left join $ref_database.odsapisuccessordetails successorDetails 
 on geo.DH_GEOGRAPHY_CODE = successorDetails.Organisationid and successorDetails.Type = 'Successor' and successorDetails.startDate > '2021-03-31'--and geo.IS_CURRENT =1
 where pop_v2.GEOGRAPHIC_GROUP_CODE = 'E38'
 and geo.ENTITY_CODE = 'E38'
 and pop_v2.year_of_count = (select max(year_of_count) from $ref_database.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38')
 and trim(pop_v2.RECORD_TYPE) = 'E'
 group by geo.DH_GEOGRAPHY_CODE,successorDetails.TargetOrganisationID,successorDetails.OrganisationID,geo.year_of_change
 order by geo.DH_GEOGRAPHY_CODE

# COMMAND ----------

 %sql
 create or replace temp view reference_list_rn1 as
 select * from reference_list_rn where RN = 1

# COMMAND ----------

 %sql
 create or replace temp view no_successor as
 
 select 
 --ref.OrganisationID as id, 
         org1.org_code as CCG_2020_CODE,
         org1.name as CCG_2020_name, 
         org1.org_code as CCG_2021_CODE,
         org1.name as CCG_2021_name,
         ref.All_Ages,
         ref.Aged_18_to_64,
         ref.Aged_65_OVER,
         ref.Aged_0_to_17
 from reference_list_rn1 ref
 inner join $ref_database.org_daily org1
 on ref.DH_GEOGRAPHY_CODE = org1.org_code
 where ref.OrganisationID is null
 and org1.Business_end_date is null
 order by CCG_2020_CODE 

# COMMAND ----------

 %sql
 create or replace temp view with_successor_this_one_predecessor as
 select 
         ref.OrganisationID as id,
         org2.name as name, 
         org2.org_code as Org_CODE,
         ref.All_Ages,
         ref.Aged_18_to_64,
         ref.Aged_65_OVER,
         ref.Aged_0_to_17
 from reference_list_rn1 ref
 inner join $ref_database.org_daily org2
 on ref.DH_GEOGRAPHY_CODE = org2.org_code
 where ref.OrganisationID is not null
 and org2.Business_end_date is null

# COMMAND ----------

 %sql
 create or replace temp view with_successor_this_one_successor as
 select 
       ref.OrganisationID as id,
       org3.name ,
       org3.org_code
 from reference_list_rn1 ref
 inner join $ref_database.org_daily org3
 on ref.TargetOrganisationID = org3.org_code
 where ref.OrganisationID is not null
 and org3.Business_end_date is null 

# COMMAND ----------

 %sql
 create or replace temp view with_successor as
 select  
       org_predecessor.org_code as CCG_2020_CODE,
       org_predecessor.Name as CCG_2020_NAME,
       org_successor.org_code as CCG_2021_CODE,
       org_successor.Name as CCG_2021_NAME,
       org_predecessor.All_Ages,
       org_predecessor.Aged_18_to_64,
       org_predecessor.Aged_65_OVER,
       org_predecessor.Aged_0_to_17
 from with_successor_this_one_predecessor org_predecessor
 inner join with_successor_this_one_successor org_successor
 on org_predecessor.id = org_successor.id
 order by CCG_2020_CODE

# COMMAND ----------

 %sql
 drop table if exists $db_output.ips_ccg_pop;   
 create table if not exists $db_output.ips_ccg_pop as
 
 select 
 o.CCG_2021_CODE as CCG_Code,
 sum(All_ages) as POPULATION_COUNT
 from no_successor o
 group by o.CCG_2021_CODE
 union
 select 
 o.CCG_2021_CODE as CCG_Code,
 sum(All_ages) as POPULATION_COUNT
 from with_successor o
 group by o.CCG_2021_CODE

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_master_pop;
 CREATE TABLE IF NOT EXISTS $db_output.ips_master_pop as
 select
 "England" as breakdown,
 "England" as level,
 SUM(POPULATION_COUNT) as POPULATION_COUNT    
 from $ref_database.ons_population_v2 where year_of_count = (select max(year_of_count) from $ref_database.ons_population_v2 where GEOGRAPHIC_GROUP_CODE = 'E38') and GEOGRAPHIC_GROUP_CODE = 'E38' and trim(RECORD_TYPE) = 'E'
 UNION
 select
 "England; Age Band" as breakdown,
 Age_Band as level,
 SUM(POPULATION_COUNT) as POPULATION_COUNT
 from $db_output.ips_age_pop
 group by Age_Band
 UNION
 select
 "England; Gender" as breakdown,
 Der_Gender as level,
 SUM(POPULATION_COUNT) as POPULATION_COUNT
 from $db_output.ips_gender_pop
 group by Der_Gender
 UNION
 select
 "England; Upper Ethnicity" as breakdown,
 UpperEthnicity as level,
 ROUND(SUM(POPULATION_COUNT), 0) as POPULATION_COUNT
 from $db_output.ips_higher_eth_pop
 group by UpperEthnicity
 UNION
 select
 "England; IMD Decile" as breakdown,
 IMD_Decile as level,
 SUM(POPULATION_COUNT) as POPULATION_COUNT
 from $db_output.ips_imd_pop
 group by IMD_Decile
 UNION
 select
 "CCG of Residence" as breakdown,
 CCG_Code as level,
 SUM(POPULATION_COUNT) as POPULATION_COUNT
 from $db_output.ips_ccg_pop
 group by CCG_Code

# COMMAND ----------

 %md
 ### Final Prep Tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_master;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_master
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber string,
 Der_FY string,
 UniqMonthID string,
 ReportingPeriodStartDate date,
 OrgIDProv string,
 Provider_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 RefLength int,
 AtLeast7days int,
 TotalContacts int,
 AttendedContacts int,
 Contact1 date,
 Contact2 date,
 Assessment_SR int,
 Paired_SR int,
 Improvement_SR int,
 NoChange_SR int,
 Deter_SR int,
 Assessment_PR int,
 Paired_PR int,
 Improvement_PR int,
 NoChange_PR int,
 Deter_PR int,
 Assessment_CR int,
 Paired_CR int,
 Improvement_CR int,
 NoChange_CR int,
 Deter_CR int,
 Assessment_ANY int,
 Paired_ANY int
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_master;
 CREATE TABLE IF NOT EXISTS $db_output.ips_master
 (
 UniqServReqID string,
 Person_ID  string,
 UniqMonthID string,
 RecordNumber string,
 ReportingPeriodStartDate string,
 ReportingPeriodEndDate date,
 Der_FY string, 
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Age_Band string,
 Der_Gender string,
 Der_Gender_Desc string,
 UpperEthnicity string,
 IMD_Decile string,
 OrgIDProv string,
 Provider_Name string,
 OrgIDCCGRes string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 AccessFlag int,
 FYAccessFlag int,
 AccessDate date,
 Contacts int
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ips_master_rates;
 CREATE TABLE IF NOT EXISTS $db_output.ips_master_rates
 (
 breakdown string,
 level_code string,
 level_name string,
 REF_COUNT int,
 POPULATION_COUNT float ----float due to ccg_ethpop figures being estimates
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.community_crisis_master;
 CREATE TABLE IF NOT EXISTS $db_output.community_crisis_master
 (
 UniqServReqID STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 OrgIDProv STRING,
 Provider_Name STRING,
 ClinRespPriorityType STRING,
 ReferralRequestReceivedDate DATE,
 ReferralRequestReceivedTime TIMESTAMP,
 ReferralRequestReceivedDateTime TIMESTAMP,
 AGE_GROUP STRING,        
 SourceOfReferralMH STRING,
 ServTeamTypeRefToMH STRING,
 UniqCareContID STRING,
 AttendOrDNACode STRING,
 ConsMechanismMH STRING,
 CareContDate DATE,
 CareContTime TIMESTAMP,
 CareContDateTime TIMESTAMP,
 minutes_between_ref_and_first_cont FLOAT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.liason_psychiatry_master;
 CREATE TABLE IF NOT EXISTS $db_output.liason_psychiatry_master
 (
 UniqServReqID STRING,
 CCG_Code STRING,
 CCG_Name STRING,
 OrgIDProv STRING,
 Provider_Name STRING,
 ClinRespPriorityType STRING,
 ReferralRequestReceivedDate DATE,
 ReferralRequestReceivedTime TIMESTAMP,
 ReferralRequestReceivedDateTime TIMESTAMP,
 AGE_GROUP STRING,        
 SourceOfReferralMH STRING,
 ServTeamTypeRefToMH STRING,
 UniqCareContID STRING,
 AttendOrDNACode STRING,
 ConsMechanismMH STRING,
 CareContDate DATE,
 CareContTime TIMESTAMP,
 CareContDateTime TIMESTAMP,
 minutes_between_ref_and_first_cont FLOAT
 ) USING DELTA

# COMMAND ----------

 %md
 ### VaildCodes Table

# COMMAND ----------

 %sql
  
 -- introduced for MHSDS v5
  
 DROP TABLE IF EXISTS $db_output.validcodes; 
 CREATE TABLE IF NOT EXISTS $db_output.validcodes
 (
   Table string,
   Field string,
   Measure string,
   Type string,
   ValidValue string,
   FirstMonth int,
   LastMonth int
 )
  
 USING DELTA
 PARTITIONED BY (Table)

# COMMAND ----------

 %sql
  
 -- Values correspondto the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth
  
 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 
  
 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements
  
 INSERT INTO $db_output.validcodes
  
 VALUES ('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '1', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '2', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'ED87_90', 'include', '3', 1390, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'CCR70_72', 'include', '1', 1429, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CCR70_72', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '3', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '1', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '2', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '3', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'A1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'A2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'A3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'B1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'B2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'C1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'C2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'D1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E6', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'F1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'F2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'F3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'H1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'H2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'I1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'I2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'L1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'L2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M6', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M7', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M9', 1459, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'N3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'P1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'Q1', 1459, null)

# COMMAND ----------

 %sql
  
 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 
  
 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements
  
 INSERT INTO $db_output.validcodes
 VALUES ('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A03', 1429, 1458)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A04', 1429, 1458)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A05', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A06', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A07', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A08', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A09', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A10', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A11', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A12', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A13', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A14', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A15', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A16', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A17', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A18', 1429, null) 
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A21', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A22', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A23', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A24', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A25', 1429, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'B01', 1429, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C04', 1429, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C08', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C10', 1429, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D03', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D04', 1429, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D06', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D07', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D08', 1429, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F01', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F02', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F03', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F04', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F05', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F06', 1459, null)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z02', 1429, null)
  
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A03', 1429, 1458)
  
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A03', 1429, 1458)

# COMMAND ----------

 %sql
  
 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 
  
 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements
  
 INSERT INTO $db_output.validcodes
  
 VALUES ('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '01', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '11', 1459, null)
  
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '01', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '04', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '11', 1459, null)
  
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '05', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '06', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '09', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '10', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '12', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '13', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '98', 1459, null)
  
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '01', 1390, null) -- 01_Prepare/2.AWT_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '04', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '11', 1459, null)
  
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '01', 1390, null) -- 01_Prepare/3.CYP_2nd_contact_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '04', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '11', 1459, null)
  
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '05', 1390, null) -- 01_Prepare/3.CYP_2nd_contact_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '06', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '09', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '10', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '13', 1459, null)

# COMMAND ----------

 %md
 ### Output Tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.menh_output_unsuppressed;
 CREATE TABLE IF NOT EXISTS $db_output.menh_output_unsuppressed
 ( 
   REPORTING_PERIOD_START DATE,
   REPORTING_PERIOD_END DATE,
   STATUS STRING,
   BREAKDOWN STRING,
   PRIMARY_LEVEL STRING,
   PRIMARY_LEVEL_DESCRIPTION STRING,
   SECONDARY_LEVEL STRING,
   SECONDARY_LEVEL_DESCRIPTION STRING,
   MEASURE_ID STRING,
   MEASURE_NAME STRING,
   MEASURE_VALUE FLOAT
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.menh_output_suppressed;
 CREATE TABLE IF NOT EXISTS $db_output.menh_output_suppressed
 ( 
   REPORTING_PERIOD_START DATE,
   REPORTING_PERIOD_END DATE,
   STATUS STRING,
   BREAKDOWN STRING,
   PRIMARY_LEVEL STRING,
   PRIMARY_LEVEL_DESCRIPTION STRING,
   SECONDARY_LEVEL STRING,
   SECONDARY_LEVEL_DESCRIPTION STRING,
   MEASURE_ID STRING,
   MEASURE_NAME STRING,
   MEASURE_VALUE STRING
 )