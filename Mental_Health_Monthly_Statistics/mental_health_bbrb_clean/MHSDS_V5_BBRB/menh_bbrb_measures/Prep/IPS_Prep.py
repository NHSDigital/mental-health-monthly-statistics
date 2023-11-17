# Databricks notebook source
# DBTITLE 1,Get MPI Table for Rolling 12 month period
 %sql
 DROP TABLE IF EXISTS $db_output.ips_mpi;
 CREATE TABLE $db_output.ips_mpi AS 
 SELECT 
 MPI.uniqmonthid
 ,MPI.person_id
 ,MPI.OrgIDProv
 ,OD.Name as OrgIDProvName
 ,MPI.recordnumber
 ,MPI.PatMRecInRP
 ,MPI.NHSDEthnicity
            ,CASE WHEN MPI.NHSDEthnicity = 'A' THEN 'A'
                  WHEN MPI.NHSDEthnicity = 'B' THEN 'B'
                  WHEN MPI.NHSDEthnicity = 'C' THEN 'C'
                  WHEN MPI.NHSDEthnicity = 'D' THEN 'D'
                  WHEN MPI.NHSDEthnicity = 'E' THEN 'E'
                  WHEN MPI.NHSDEthnicity = 'F' THEN 'F'
                  WHEN MPI.NHSDEthnicity = 'G' THEN 'G'
                  WHEN MPI.NHSDEthnicity = 'H' THEN 'H'
                  WHEN MPI.NHSDEthnicity = 'J' THEN 'J'
                  WHEN MPI.NHSDEthnicity = 'K' THEN 'K'
                  WHEN MPI.NHSDEthnicity = 'L' THEN 'L'
                  WHEN MPI.NHSDEthnicity = 'M' THEN 'M'
                  WHEN MPI.NHSDEthnicity = 'N' THEN 'N'
                  WHEN MPI.NHSDEthnicity = 'P' THEN 'P'
                  WHEN MPI.NHSDEthnicity = 'R' THEN 'R'
                  WHEN MPI.NHSDEthnicity = 'S' THEN 'S'
                  WHEN MPI.NHSDEthnicity = 'Z' THEN 'Z'
                  WHEN MPI.NHSDEthnicity = '99' THEN '99'
                  ELSE 'UNKNOWN' END AS LowerEthnicity 
             ,CASE WHEN MPI.NHSDEthnicity = 'A' THEN 'British'
                   WHEN MPI.NHSDEthnicity = 'B' THEN 'Irish'
                   WHEN MPI.NHSDEthnicity = 'C' THEN 'Any Other White Background'
                   WHEN MPI.NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
                   WHEN MPI.NHSDEthnicity = 'E' THEN 'White and Black African'
                   WHEN MPI.NHSDEthnicity = 'F' THEN 'White and Asian'
                   WHEN MPI.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
                   WHEN MPI.NHSDEthnicity = 'H' THEN 'Indian'
                   WHEN MPI.NHSDEthnicity = 'J' THEN 'Pakistani'
                   WHEN MPI.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                   WHEN MPI.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
                   WHEN MPI.NHSDEthnicity = 'M' THEN 'Caribbean'
                   WHEN MPI.NHSDEthnicity = 'N' THEN 'African'
                   WHEN MPI.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
                   WHEN MPI.NHSDEthnicity = 'R' THEN 'Chinese'
                   WHEN MPI.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                   WHEN MPI.NHSDEthnicity = 'Z' THEN 'Not Stated'
                   WHEN MPI.NHSDEthnicity = '99' THEN 'Not Known' 
                   ELSE 'UNKNOWN' END AS LowerEthnicity_Desc    
                  
                  ,CASE WHEN MPI.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN MPI.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN MPI.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN MPI.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN MPI.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN MPI.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN MPI.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'UNKNOWN' END AS UpperEthnicity
                                     
                   ,MPI.Gender
            ,MPI.GenderIDCode
            ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode 
                  WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender 
                  ELSE 'UNKNOWN' END AS Der_Gender          
                  ,MPI.AgeRepPeriodEnd
                  ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 19 THEN '0 to 19'
                  WHEN MPI.AgeRepPeriodEnd BETWEEN 20 AND 29 THEN '20 to 29'
                  WHEN MPI.AgeRepPeriodEnd BETWEEN 30 AND 39 THEN '30 to 39'
                  WHEN MPI.AgeRepPeriodEnd BETWEEN 40 AND 49 THEN '40 to 49'
                  WHEN MPI.AgeRepPeriodEnd BETWEEN 50 AND 59 THEN '50 to 59'
                  WHEN MPI.AgeRepPeriodEnd BETWEEN 60 AND 69 THEN '60 to 69'
                  WHEN MPI.AgeRepPeriodEnd >= 70 THEN '70 and over' ELSE 'UNKNOWN' END AS Age_Band
  
                      ,CASE
                 WHEN IMD.DECI_IMD = 10 THEN '10 Least deprived'
                 WHEN IMD.DECI_IMD = 9 THEN '09 Less deprived'
                 WHEN IMD.DECI_IMD = 8 THEN '08 Less deprived'
                 WHEN IMD.DECI_IMD = 7 THEN '07 Less deprived'
                 WHEN IMD.DECI_IMD = 6 THEN '06 Less deprived'
                 WHEN IMD.DECI_IMD = 5 THEN '05 More deprived'
                 WHEN IMD.DECI_IMD = 4 THEN '04 More deprived'
                 WHEN IMD.DECI_IMD = 3 THEN '03 More deprived'
                 WHEN IMD.DECI_IMD = 2 THEN '02 More deprived'
                 WHEN IMD.DECI_IMD = 1 THEN '01 Most deprived'
                 ELSE 'UNKNOWN' -- updated to UNKNOWN tbc
                 END AS IMD_Decile
                 FROM $db_source.MHS001MPI MPI
 LEFT JOIN $reference_data.ENGLISH_INDICES_OF_DEP_V02 IMD 
                     on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
                     and IMD.imd_year = '2019'
 LEFT JOIN $db_output.bbrb_org_daily_past_12_months_mhsds_providers OD on MPI.OrgIDProv = OD.ORG_CODE
 WHERE MPI.UniqMonthID between '$start_month_id' and '$end_month_id'

# COMMAND ----------

# DBTITLE 1,Get Referrals to IPS Team Types (ServTeamTypeRefToMH = 'D05')
 %sql
 DROP TABLE IF EXISTS $db_output.ips_referrals;
 CREATE TABLE         $db_output.ips_referrals AS
 SELECT 
     r.UniqServReqID 
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,'ServTeamTypeRefToMH' AS Identifier   
  
 FROM $db_output.NHSE_Pre_Proc_Referral r
 WHERE r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND  r.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id'
 AND r.ServTeamTypeRefToMH = 'D05' 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth ='')

# COMMAND ----------

# DBTITLE 1,Get SNOMED Interventions for IPS (All Providers)
 %sql
 DROP TABLE IF EXISTS $db_output.ips_interventions;
 CREATE TABLE         $db_output.ips_interventions AS
  
 SELECT
     a.UniqMonthID,
     a.OrgIDProv,
     a.RecordNumber, 
     a.UniqServReqID,
     a.Der_SNoMEDProcQual --ADDED IN TO TEST
  
 FROM $db_output.NHSE_Pre_Proc_Interventions a
 LEFT JOIN $db_output.NHSE_Pre_Proc_Activity p ON a.UniqMonthID = p.UniqMonthID and a.OrgIDProv = p.OrgIDProv AND a.RecordNumber = p.RecordNumber AND a.UniqCareContID = p.UniqCareContID
  
 WHERE (a.Der_SNoMEDProcCode IN ('1082621000000104', '772822000') OR regexp_replace(a.Der_SNoMEDProcCode, "\n|\r", "")=1082621000000104 or regexp_replace(a.Der_SNoMEDProcCode, "\n|\r", "")=772822000)
 AND ((a.Der_SNoMEDProcQual != '443390004' OR a.Der_SNoMEDProcQual IS NULL) OR regexp_replace(a.Der_SNoMEDProcQual, "\n|\r", "")!= 443390004)
 AND (a.Der_SNoMEDProcQual != 443390004 OR a.Der_SNoMEDProcQual IS NULL)--to make sure 443390004 is filtered out and not removing nulls, POSSIBLE use case statement to handle this?
 AND p.Der_DirectContact = 1 -- and only bring in direct contacts that are F2F, video, telephone or other, codes need to be linked to snomed code 
  
 GROUP BY a.UniqMonthID, a.OrgIDProv, a.RecordNumber, a.UniqServReqID, a.Der_SNoMEDProcQual

# COMMAND ----------

# DBTITLE 1,Insert Referral Records for IPS SNOMED activity (instead of ServTeamTypeRefToMH) into ips_referrals
 %sql 
 INSERT INTO $db_output.ips_referrals
 SELECT 
     r.UniqServReqID
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,'SNoMED' AS Identifier
     
 FROM $db_output.NHSE_Pre_Proc_Referral r
 INNER JOIN $db_output.ips_interventions a 
 ON a.UniqMonthID = r.UniqMonthID 
 AND a.OrgIDProv = r.OrgIDProv 
 AND a.RecordNumber = r.RecordNumber 
 AND a.UniqServReqID = r.UniqServReqID -- Select records for referrals that have an IPS SNOMED intervention recorded
  
 WHERE r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND  r.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id'
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth ='')

# COMMAND ----------

# DBTITLE 1,Get IPS Care Contacts for Referrals to IPS Team Types (ServTeamTypeRefToMH = 'D05')
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity;
 CREATE TABLE         $db_output.ips_activity AS
  
 SELECT
     r.UniqMonthID
     ,r.Person_ID
     ,r.RecordNumber
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,r.UniqServReqID
     ,r.Identifier
     ,c.UniqCareContID
     ,c.Der_ContactDate
     ,c.Der_FY
  
 FROM $db_output.ips_referrals r
  
 INNER JOIN $db_output.NHSE_Pre_Proc_Activity c ON r.RecordNumber = c.RecordNumber 
        AND r.UniqServReqID = c.UniqServReqID 
        AND c.Der_DirectContact=1
        AND  c.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id'
  
 WHERE r.Identifier = 'ServTeamTypeRefToMH' --bring through care contacts for IPS referrals identified via Team Type code 

# COMMAND ----------

# DBTITLE 1,Insert IPS Care Contacts Referral Records for IPS SNOMED activity (instead of ServTeamTypeRefToMH) into ips_activity
 %sql
 INSERT INTO $db_output.ips_activity
  
 SELECT
      r.UniqMonthID
     ,r.Person_ID
     ,r.RecordNumber
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,r.UniqServReqID
     ,r.Identifier
     ,c.UniqCareContID
     ,c.Der_ContactDate
     ,c.Der_FY
  
  FROM $db_output.ips_referrals r
  
  INNER JOIN $db_output.NHSE_Pre_Proc_Activity c ON r.RecordNumber = c.RecordNumber 
     AND r.UniqServReqID = c.UniqServReqID 
     AND r.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id' 
   
  INNER JOIN $db_output.NHSE_Pre_Proc_Interventions i ON r.RecordNumber = i.RecordNumber 
     AND r.UniqServReqID = i.UniqServReqID 
     AND c.UniqCareContID = i.UniqCareContID
  
 WHERE (i.Der_SNoMEDProcCode IN ('1082621000000104', '772822000') OR regexp_replace(i.Der_SNoMEDProcCode, "\n|\r", "")=1082621000000104 or regexp_replace(i.Der_SNoMEDProcCode, "\n|\r", "")=772822000)
 AND ((i.Der_SNoMEDProcQual != '443390004' OR i.Der_SNoMEDProcQual IS NULL) OR regexp_replace(i.Der_SNoMEDProcQual, "\n|\r", "")!= 443390004)
 AND (i.Der_SNoMEDProcQual != 443390004 OR i.Der_SNoMEDProcQual IS NULL) --to make sure 443390004 is filtered out POSSIBLE use case statement to handle this?
 AND c.Der_DirectContact=1-- and only bring in direct contacts that are F2F, video, telephone or other, codes need to be linked to snomed code 
  AND r.Identifier = 'SNoMED' ---bring through contacts for IPS referrals identified via SNoMED codes

# COMMAND ----------

# DBTITLE 1,Select distinct IPS Referrals to get a single referral for referrals which flowed under ServTeamTypeRefToMH and SNOMED
 %sql
 DROP TABLE IF EXISTS $db_output.ips_referrals_distinct;
 CREATE TABLE         $db_output.ips_referrals_distinct AS
  
 SELECT DISTINCT
     r.UniqServReqID
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
 
 FROM $db_output.ips_referrals r

# COMMAND ----------

# DBTITLE 1,Select distinct IPS Care Contacts to get a single referral for referrals which flowed under ServTeamTypeRefToMH and SNOMED
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_distinct;
 CREATE TABLE         $db_output.ips_activity_distinct AS
  
 SELECT DISTINCT
      a.UniqMonthID
     ,a.Person_ID
     ,a.RecordNumber
     ,a.OrgIDProv
     ,a.OrgIDCCGRes
     ,a.UniqServReqID
     ,a.UniqCareContID
     ,a.Der_ContactDate
     ,a.Der_FY
  
 FROM $db_output.ips_activity a

# COMMAND ----------

# DBTITLE 1,Partition Care Contacts to flag first contact per referral (ever and within financial year)
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_order;
 CREATE TABLE $db_output.ips_activity_order AS 
  
 SELECT
     a.RecordNumber
     ,a.UniqMonthID
     ,a.Person_ID
     ,a.UniqServReqID
     ,ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID ORDER BY a.Der_ContactDate ASC) AS AccessFlag 
     ,ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID, a.Der_FY ORDER BY a.Der_ContactDate ASC) AS FYAccessFlag 
     ,a.Der_ContactDate
  
 FROM $db_output.ips_activity_distinct a

# COMMAND ----------

# DBTITLE 1,Aggregate by referral per month
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_agg;
 CREATE TABLE $db_output.ips_activity_agg AS 
 SELECT
 	a.RecordNumber
 	,a.UniqMonthID 
 	,a.Person_ID
 	,a.UniqServReqID 
 	,MIN(a.AccessFlag) AS AccessFlag
 	,MIN(a.FYAccessFlag) AS FYAccessFlag
 	,MIN(a.Der_ContactDate) AS AccessDate
 	,COUNT(*) AS TotalContacts
 
 FROM $db_output.ips_activity_order a
 
 GROUP BY a.RecordNumber, a.UniqMonthID, a.Person_ID, a.UniqServReqID  

# COMMAND ----------

# DBTITLE 1,Final Prep Table for Counts
 %sql
 INSERT INTO $db_output.ips_master
 
 SELECT 
 	r.UniqServReqID
     ,r.Person_ID
 	,r.UniqMonthID
 	,r.RecordNumber
 	,r.ReportingPeriodStartDate
 	,r.ReportingPeriodEndDate
 	,r.Der_FY   
 	,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,COALESCE(m.Age_Band, "UNKNOWN") as Age_Band
     ,CASE WHEN Der_Gender = "1" THEN '1'
           WHEN Der_Gender = "2" THEN '2'
           WHEN Der_Gender = "3" THEN '3'
           WHEN Der_Gender = "4" THEN '4'
           WHEN Der_Gender = "9" THEN '9'
           ELSE 'UNKNOWN' END AS Der_Gender
     ,CASE WHEN Der_Gender = "1" THEN 'Male'
           WHEN Der_Gender = "2" THEN 'Female'
           WHEN Der_Gender = "3" THEN 'Non-binary'
           WHEN Der_Gender = "4" THEN 'Other (not listed)'
           WHEN Der_Gender = "9" THEN 'Indeterminate'
           WHEN Der_Gender ="" THEN 'UNKNOWN'
           ELSE 'UNKNOWN' END AS Der_Gender_Desc
     ,COALESCE(m.UpperEthnicity, "UNKNOWN") as UpperEthnicity
     ,COALESCE(m.IMD_Decile, "UNKNOWN") as IMD_Decile
 	,r.OrgIDProv
     ,m.OrgIDProvName as Provider_Name
 	,r.OrgIDCCGRes
     ,COALESCE(i.ccg_code,'UNKNOWN') AS CCG_Code
     ,COALESCE(i.ccg_name,'UNKNOWN') AS CCG_Name
     ,COALESCE(i.STP_CODE, 'UNKNOWN') AS STP_Code
     ,COALESCE(i.STP_NAME, 'UNKNOWN') AS STP_Name
     ,COALESCE(i.REGION_CODE, 'UNKNOWN') AS Region_Code
     ,COALESCE(i.REGION_NAME, 'UNKNOWN') AS Region_Name
 	,CASE WHEN a.AccessFlag = 1 THEN 1 ELSE 0 END AS AccessFlag 
 	,CASE WHEN a.FYAccessFlag = 1 THEN 1 ELSE 0 END AS FYAccessFlag
 	,a.AccessDate
     ,TotalContacts AS Contacts
 
 FROM $db_output.ips_referrals_distinct r 
 LEFT JOIN $db_output.ips_activity_agg a ON r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber
 LEFT JOIN $db_output.mhsds_stp_mapping i ON r.OrgIDCCGRes = i.ccg_code --- double check with AT  should it be OrgIDProv? 
 INNER JOIN $db_output.ips_mpi m ON a.Person_ID = m.Person_ID and a.RecordNumber = m.RecordNumber and a.AccessFlag = 1 ---joining on record number to get demographics submitted by provider at first contact

# COMMAND ----------

# DBTITLE 1,Aggregate IPS Counts for Rates
 %sql
 DROP TABLE IF EXISTS $db_output.ips_master_count;
 CREATE TABLE IF NOT EXISTS $db_output.ips_master_count as
 select
 "England" as breakdown,
 "England" as level_code,
 "England" as level_name,
 COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID END) AS REF_COUNT
 from $db_output.ips_master
 UNION
 select
 "England; Age Band" as breakdown,
 Age_Band as level_code,
 Age_Band as level_name,
 COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID END) AS REF_COUNT
 from $db_output.ips_master
 group by Age_Band
 UNION
 select
 "England; Gender" as breakdown,
 Der_Gender as level_code,
 Der_Gender_Desc as level_code,
 COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID END) AS REF_COUNT
 from $db_output.ips_master
 group by Der_Gender, Der_Gender_Desc
 UNION
 select
 "England; Upper Ethnicity" as breakdown,
 UpperEthnicity as level_code,
 UpperEthnicity as level_name,
 COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID END) AS REF_COUNT
 from $db_output.ips_master
 group by UpperEthnicity
 UNION
 select
 "England; IMD Decile" as breakdown,
 IMD_Decile as level_code,
 IMD_Decile as level_name,
 COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID END) AS REF_COUNT
 from $db_output.ips_master
 group by IMD_Decile
 UNION
 select
 "CCG of Residence" as breakdown,
 CCG_Code as level_code,
 CCG_Name as level_name,
 COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID END) AS REF_COUNT
 from $db_output.ips_master
 group by CCG_Code, CCG_Name

# COMMAND ----------

# DBTITLE 1,Combine with Population data created in Reference_Tables and insert into $db_output.ips_master_rates
 %sql
 INSERT INTO $db_output.ips_master_rates
 select
 CASE WHEN c.breakdown = 'England; Age Band' THEN 'England; Age Group' ELSE c.breakdown END,
 c.level_code,
 c.level_name,
 c.REF_COUNT,
 p.POPULATION_COUNT as POPULATION_COUNT
 from $db_output.ips_master_count c
 left join $db_output.ips_master_pop p on c.breakdown = p.breakdown and c.level_code = p.level