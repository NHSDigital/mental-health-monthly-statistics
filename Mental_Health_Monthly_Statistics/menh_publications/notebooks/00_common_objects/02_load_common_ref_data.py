# Databricks notebook source
 %md

 # Generic Prep assets used throughout Mental health menh_publications
 -- NP: only cells which have been added for CYP_ED_WT have been noted here
 -- User note: updated to include all tables prepared in this notebook

 - RD_CCG_LATEST - TABLE
 - CCG_PRAC - view
 - CCG_prep_3months [User note changed from ed_CCG_prep as the tables are the same] - view
 - CCG_PREP - view
 - CCG - TABLE
 - RD_ORG_DAILY_LATEST - TABLE
 - Provider_list - TABLE
 - Main_monthly_metric_values - TABLE
 - Main_monthly_breakdown_values - TABLE
 - Main_monthly_level_values - TABLE
 - Org_Daily - view
 - org_relationship_daily - view
 - STP_Region_mapping_post_2020 - TABLE
 - MHS101Referral_LATEST - view
 - MHS001_CCG_LATEST - [User note changed from ED_CCG_LATEST] - TABLE
 - MHS001_PATMRECINRP_201819_F_M - view
 - MHS001MPI_PATMRECINRP_FIX - view

 - REFS - view
 - TEAMTYPE - view
 - ward_type_list - view
 - ward_stay_cats - view
 - referral_list - view
 - referral_cats - view
 - MHS101Referral_service_area - TABLE
 - MHS101Referral_open_end_rp - TABLE

 - accommodation_latest - TABLE
 - employment_latest - TABLE

 - CASSR_mapping
 - GenderCodes

 - Ref_GenderCodes - TABLE
 - Ref_EthnicGroup - TABLE
 - Ref_AgeBand - TABLE
 - Ref_IMD_Decile - TABLE

# COMMAND ----------

# DBTITLE 1,RD_CCG_LATEST - *new - phase 1* using ODSAPI tables AND org_daily - commented out
# %sql

# TRUNCATE TABLE $db_output.RD_CCG_LATEST;

# with mappedCCGs as

# (SELECT DISTINCT od.ORG_CODE as original_ORG_CODE,
#                 od.NAME as original_NAME,
#                 COALESCE(odssd.TargetOrganisationID, od.ORG_CODE) as ORG_CODE
#         FROM $reference_data.org_daily od

         
#         LEFT JOIN $reference_data.ODSAPISuccessorDetails as odssd
#         ON od.ORG_CODE = odssd.OrganisationID and odssd.Type = 'Successor' and odssd.StartDate <= '$rp_enddate'
        
#            WHERE (od.BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(od.BUSINESS_END_DATE))
#                 AND od.BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
#                 AND od.ORG_TYPE_CODE = 'CC'
#                 AND (od.ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(od.ORG_CLOSE_DATE))
#                 AND od.ORG_OPEN_DATE <= '$rp_enddate'
#                 AND od.NAME NOT LIKE '%HUB'
#                 AND od.NAME NOT LIKE '%NATIONAL%' 
# )

# INSERT INTO TABLE $db_output.RD_CCG_LATEST

# SELECT mappedCCGs.*,
#       od1.NAME as NAME
#     FROM mappedCCGs
#     LEFT JOIN (
#             SELECT
#             row_number() over (partition by ORG_CODE order by case when ORG_CLOSE_DATE is null then 1 else 0 end desc, case when BUSINESS_END_DATE is null then 1 else 0 end desc, ORG_CLOSE_DATE desc) as RN,
#             ORG_CODE,
#             NAME
#             FROM $reference_data.org_daily
                    
#             WHERE ORG_OPEN_DATE <= '$rp_enddate'
#             ) od1
#             ON mappedCCGs.ORG_CODE = od1.ORG_CODE
#             AND od1.RN = 1
          
# ORDER BY original_ORG_CODE


# COMMAND ----------

# DBTITLE 1,RD_CCG_LATEST - *new* using only ODSAPI tables
 %sql

 -- The code below can be used to replace the cell above once the issue with missing '13T' has been resolved - see DMS001-1132
 -- Removed the need for this fix (which is never going to happen) by introducing another row_number on DateType

 TRUNCATE TABLE $db_output.RD_CCG_LATEST;

 with mappedCCGs as

 (
 SELECT original_ORG_CODE, original_NAME, ORG_CODE 
 FROM
 (SELECT rd.OrganisationID as original_ORG_CODE, 
                 Name as original_NAME,
 --  This will select 'Legal' dates over 'Operational' dates because of "order by rd.DateType"
 --  If it's decided we need 'Operational' dates as preference then add desc                         
                 row_number() over (partition by rd.OrganisationId order by rd.DateType ) as RN1,
                 COALESCE(odssd.TargetOrganisationID, rd.OrganisationID) as ORG_CODE
                 
         FROM $reference_data.ODSAPIRoleDetails rd
         
         LEFT JOIN $reference_data.ODSAPIOrganisationDetails od
         ON rd.OrganisationID = od.OrganisationID and rd.DateType = od.DateType
         
         LEFT JOIN $reference_data.ODSAPISuccessorDetails as odssd
         ON rd.OrganisationID = odssd.OrganisationID and odssd.Type = 'Successor' and odssd.StartDate <= '$rp_enddate'
         
         WHERE 
 -- a Primary RoleID of 'RO98' is a CCG
 -- a non-Primary RoleID of 'RO319' is a Sub-ICB
          CASE WHEN $month_id > 1467 THEN (rd.RoleId = 'RO319' and rd.PrimaryRole = 0)  
             WHEN $month_id <= 1467 THEN (rd.RoleId = 'RO98' and rd.PrimaryRole = 1)
             END
         AND (rd.EndDate >= add_months('$rp_enddate', 1) OR ISNULL(rd.EndDate))
         AND rd.StartDate <= add_months('$rp_enddate', 1)
         AND Name NOT LIKE '%HUB'
         AND Name NOT LIKE '%NATIONAL%' 
         AND Name NOT LIKE '%ENTITY%' 
 )
 WHERE RN1 = 1
 )

 INSERT INTO TABLE $db_output.RD_CCG_LATEST

 SELECT 
       mappedCCGs.original_ORG_CODE,
       mappedCCGs.original_NAME,
       mappedCCGs.ORG_CODE,
       od1.NAME as NAME
     FROM mappedCCGs
     LEFT JOIN (
             SELECT
             row_number() over (partition by rd.OrganisationId order by case when rd.EndDate is null then 1 else 0 end desc, rd.EndDate desc) as RN,
             rd.OrganisationId as ORG_CODE,
             Name as NAME

         FROM $reference_data.ODSAPIRoleDetails rd
         LEFT JOIN $reference_data.ODSAPIOrganisationDetails od
         ON rd.OrganisationID = od.OrganisationID and rd.DateType = od.DateType
         
        WHERE 
          CASE WHEN $month_id > 1467 THEN (rd.RoleId = 'RO319' and rd.PrimaryRole = 0)  
             WHEN $month_id <= 1467 THEN (rd.RoleId = 'RO98' and rd.PrimaryRole = 1)
             END
             
         AND (rd.EndDate >= add_months('$rp_enddate', 1) OR ISNULL(rd.EndDate))
         AND Name NOT LIKE '%HUB'
         AND Name NOT LIKE '%NATIONAL%' 
         AND Name NOT LIKE '%ENTITY%' 
         AND rd.StartDate <= '$rp_enddate'
             ) od1
             ON mappedCCGs.ORG_CODE = od1.ORG_CODE
             AND od1.RN = 1
           
 ORDER BY original_ORG_CODE


# COMMAND ----------

# DBTITLE 1,CCG_PRAC - used by MHA_Monthly
 %sql

 CREATE OR REPLACE TEMPORARY VIEW CCG_PRAC AS
 SELECT GP.Person_ID,
       GP.OrgIDCCGGPPractice,
       GP.OrgIDSubICBLocGP,
       GP.RecordNumber
  FROM $db_source.MHS002GP GP
       INNER JOIN 
                  (
                    SELECT Person_ID, 
                           MAX(RecordNumber) as RecordNumber
                      FROM $db_source.MHS002GP
                     WHERE UniqMonthID = $month_id
                           AND GMPReg NOT IN ('V81999','V81998','V81997')
                           --AND OrgIDGPPrac <> '-1' --CCG methodology change removed clause, would have previously removed GP records that didnt pass a valid value in the validation. See publication notes around methodology for more detail. 
                           AND EndDateGMPRegistration is NULL
                  GROUP BY Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GP.UniqMonthID = $month_id
        AND GMPReg NOT IN ('V81999','V81998','V81997')
        AND EndDateGMPRegistration is null

# COMMAND ----------

# DBTITLE 1,CCG_PREP_12months
 %sql
 --used by CYP_ED_WaitingTimes & EIP
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_prep_12months AS
 SELECT DISTINCT    a.Person_ID,
                    max(a.RecordNumber) as recordnumber                
 FROM               $db_source.MHS001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null                
 LEFT JOIN          $db_output.RD_CCG_LATEST c on a.OrgIDCCGRes = c.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST c1 on a.OrgIDSubICBLocResidence = c1.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e1 on b.OrgIDSubICBLocGP = e1.original_ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or e1.ORG_CODE is not null or c1.ORG_CODE is not null)
                    and a.uniqmonthid between $month_id - 11 AND $month_id            
 GROUP BY           a.Person_ID;

# COMMAND ----------

# DBTITLE 1,CCG_PREP_3months
 %sql
 --used by CYP_ED_WaitingTimes & EIP
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_prep_3months AS
 SELECT DISTINCT    a.Person_ID,
                    max(a.RecordNumber) as recordnumber                
 FROM               $db_source.MHS001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null                
 LEFT JOIN          $db_output.RD_CCG_LATEST c on a.OrgIDCCGRes = c.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST c1 on a.OrgIDSubICBLocResidence = c1.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e1 on b.OrgIDSubICBLocGP = e1.original_ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or e1.ORG_CODE is not null or c1.ORG_CODE is not null)
                    and a.uniqmonthid between $month_id - 2 AND $month_id            
 GROUP BY           a.Person_ID;

# COMMAND ----------

# DBTITLE 1,CCG_PREP - single month
 %sql
 --used by 72hours
 CREATE OR REPLACE TEMPORARY VIEW CCG_PREP AS
 SELECT a.Person_ID,
       CASE 
            WHEN UniqMonthID <= 1467 AND c.OrgIDCCGGPPractice IS not null then c.OrgIDCCGGPPractice
            WHEN UniqMonthID > 1467 AND c.OrgIDSubICBLocGP IS not null then c.OrgIDSubICBLocGP
            WHEN UniqMonthID <= 1467 AND a.OrgIDCCGRes IS not null then a.OrgIDCCGRes
            WHEN UniqMonthID > 1467 AND a.OrgIDSubICBLocResidence IS not null then a.OrgIDSubICBLocResidence
            ELSE 'UNKNOWN' end as IC_Rec_CCG
 FROM $db_source.MHS001MPI a
 LEFT JOIN CCG_PRAC c 
           ON a.Person_ID = c.Person_ID 
           AND a.RecordNumber = c.RecordNumber
     WHERE a.UniqMonthID = $month_id 
           AND a.PatMRecInRP = true

# COMMAND ----------

# DBTITLE 1,CCG - INSERT OVERWRITE
 %sql

 INSERT OVERWRITE TABLE $db_output.CCG

  FROM CCG_PREP a
  LEFT JOIN $db_output.RD_CCG_LATEST b 
            ON a.IC_Rec_CCG = b.original_ORG_CODE
            
     SELECT Person_ID,
            CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS IC_Rec_CCG,
            CASE WHEN NAME IS null THEN 'UNKNOWN' ELSE NAME END AS NAME

# COMMAND ----------

# DBTITLE 1,RD_ORG_DAILY_LATEST - INSERT OVERWRITE
 %sql
 --used 02_Expand_output
 /* Org daily latest */

 INSERT OVERWRITE TABLE $db_output.RD_ORG_DAILY_LATEST

            FROM $reference_data.org_daily
          
           SELECT DISTINCT ORG_CODE, 
                 NAME
                 
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN')

# COMMAND ----------

# DBTITLE 1,Provider_list (in month)
 %sql

 TRUNCATE TABLE $db_output.Provider_list;

 INSERT INTO TABLE $db_output.Provider_list
 SELECT DISTINCT OrgIDProvider as ORG_CODE, x.NAME as NAME
 FROM $db_source.MHS000Header as Z
 LEFT OUTER JOIN $db_output.RD_ORG_DAILY_LATEST AS X
     ON Z.OrgIDProvider = X.ORG_CODE
 WHERE Z.Uniqmonthid = $month_id 


# COMMAND ----------

# DBTITLE 1,Provider list (rolling quarter)
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* providers_between_rp_start_end_dates: This is a view used in a lot of the the Provider breakdowns        

   This returns the OrgIDProvider codes between the reporting period dates from MHS000Header
                                                                                  */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 TRUNCATE TABLE $db_output.providers_between_rp_start_end_dates;
 INSERT INTO $db_output.providers_between_rp_start_end_dates  
 SELECT DISTINCT OrgIDProvider as ORG_CODE, x.NAME as NAME
            FROM $db_source.MHS000Header as Z
                 LEFT OUTER JOIN $db_output.RD_ORG_DAILY_LATEST AS X
 					ON Z.OrgIDProvider = X.ORG_CODE
              WHERE	UniqMonthID BETWEEN '$month_id' -2 AND '$month_id'
             

# COMMAND ----------

# DBTITLE 1,Provider list (rolling 12 months)
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* providers_between_rp_start_end_dates: This is a view used in a lot of the the Provider breakdowns        

   This returns the OrgIDProvider codes between the reporting period dates from MHS000Header
                                                                                  */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 TRUNCATE TABLE $db_output.providers_between_rp_start_end_dates_12m;
 INSERT INTO $db_output.providers_between_rp_start_end_dates_12m  
 SELECT DISTINCT OrgIDProvider as ORG_CODE, x.NAME as NAME
            FROM $db_source.MHS000Header as Z
                 LEFT OUTER JOIN $db_output.RD_ORG_DAILY_LATEST AS X
 					ON Z.OrgIDProvider = X.ORG_CODE
              WHERE	UniqMonthID BETWEEN '$month_id' -11 AND '$month_id'
             

# COMMAND ----------

# DBTITLE 1,1. Main monthly
 %sql
 -- Main_monthly_metric_values is also truncated and populated - with 1 rows - in 01_detoc_prepare
 TRUNCATE TABLE $db_output.Main_monthly_metric_values;

 INSERT INTO $db_output.Main_monthly_metric_values VALUES 
 ('MHS78', 'Discharges from adult acute beds eligible for 72 hour follow up in the reporting period'),
 ('MHS79', 'Discharges from adult acute beds followed up within 72 hours in the reporting period'),
 ('MHS80', 'Proportion of discharges from adult acute beds eligible for 72 hour follow up - followed up in the reporting period');

 -- Main_monthly_breakdown_values is also truncated and populated - with 5 rows - in 01_detoc_prepare
 TRUNCATE TABLE $db_output.Main_monthly_breakdown_values;
 INSERT INTO $db_output.Main_monthly_breakdown_values VALUES
   ('England'),
   ('CCG - GP Practice or Residence'),
   ('Provider of Responsibility'),
   ('CASSR'),
   ('CASSR; Provider'),
   ('CCG - GP Practice or Residence; Provider of Responsibility');

 TRUNCATE TABLE $db_output.Main_monthly_level_values_1;

 INSERT INTO $db_output.Main_monthly_level_values_1
 SELECT DISTINCT 
  IC_Rec_CCG as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'CCG - GP Practice or Residence' as breakdown 
 FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 union all
 SELECT DISTINCT
  ORG_CODE as primary_level, 
  COALESCE(NAME, "UNKNOWN") as primary_level_desc,
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'Provider of Responsibility' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 union all
 SELECT DISTINCT
  'England' as primary_level, 
  'England' as primary_level_desc,
  'NONE' as secondary_level,
  'NONE' as secondary_level_desc,
  'England' as breakdown



# COMMAND ----------

# DBTITLE 1,Org_Daily temporary view 
 %sql
 --This view has been copied from menh_analysis\notebooks\01_prepare\0.Generic_prep

 /** added as part of the change in STP mapping/derivation for v4.1 **/

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW org_daily AS
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate';

# COMMAND ----------

# DBTITLE 1, ORG_RELATIONSHIP_DAILY temporary view
 %sql
 --NP This view has been copied from menh_analysis\notebooks\01_prepare\0.Generic_prep
 /** added as part of the change in STP mapping/derivation for v4.1 **/


 CREATE OR REPLACE GLOBAL TEMPORARY VIEW org_relationship_daily AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $reference_data.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate';



# COMMAND ----------

# DBTITLE 1,STP/Region breakdowns April 2020 onwards (NP: copied from menh_analysis\notebooks\01_prep_common_objects\0.Insert_lookup_data)
 %sql
 --This code has been copied from in menh_analysis\notebooks\01_Prepare\0.Insert_lookup_data

 TRUNCATE TABLE $db_output.STP_Region_mapping_post_2020;
 INSERT INTO $db_output.STP_Region_mapping_post_2020 

 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_DESCRIPTION, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_DESCRIPTION,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_DESCRIPTION
 FROM 
 global_temp.org_daily A
 LEFT JOIN global_temp.org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN global_temp.org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN global_temp.org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN global_temp.org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null

 --Added this bit in to get the UNKNOWN row displaying in the output
 UNION ALL
 SELECT DISTINCT

 'UNKNOWN' as STP_CODE, 
 'UNKNOWN' as STP_DESCRIPTION, 
 'UNKNOWN' as CCG_CODE, 
 'UNKNOWN' as CCG_DESCRIPTION,
 'UNKNOWN' as REGION_CODE,
 'UNKNOWN' as REGION_DESCRIPTION
  
 ORDER BY 1




# COMMAND ----------

# DBTITLE 1,EIP CCG Methodology Prep update - commented out
# %sql
# --NP: copied from menh_anaysis\notebooks\common_objects\01_prepare\2.awt_prep

# CREATE OR REPLACE GLOBAL TEMP VIEW CCG_prep_EIP AS
# SElECT DISTINCT    a.Person_ID
# 				   ,max(a.RecordNumber) as recordnumber	
# FROM               $db_source.MHS001MPI a
# LEFT JOIN          $db_source.MHS002GP b 
# 		           on a.Person_ID = b.Person_ID 
#                    and a.UniqMonthID = b.UniqMonthID
# 		           and a.recordnumber = b.recordnumber
# 		           and b.GMPReg NOT IN ('V81999','V81998','V81997')
# 		           --and b.OrgIDGPPrac <> '-1' 
# 		           and b.EndDateGMPRegistration is null
# LEFT JOIN          $db_output.RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
# LEFT JOIN          $db_output.RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
# WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
#                    and a.uniqmonthid between '$month_id' - 2 AND '$month_id'
# GROUP BY           a.Person_ID

# COMMAND ----------

# DBTITLE 1,MHS101Referral_LATEST (EIP)
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* MHS101Referral_LATEST:  This is a view used in a lot of EIP metrics breakdowns . This provides the latest 
   records between the reporting period dates from MHS101Referral*/
                                                           
   /* ---------------------------------------------------------------------------------------------------------*/
 /* Latest Referrals */

 CREATE OR REPLACE GLOBAL TEMP VIEW MHS101Referral_LATEST AS
     SELECT DISTINCT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.ReferralRequestReceivedDate,
            r.PrimReasonReferralMH,
            r.ServDischDate,
            r.AgeServReferRecDate,
            r.ClinRespPriorityType,
            r.RecordNumber
       FROM $db_source.MHS101Referral AS r
       WHERE ((r.RecordEndDate IS NULL OR r.RecordEndDate >= '$rp_enddate') AND r.RecordStartDate <= '$rp_enddate');

# COMMAND ----------

# DBTITLE 1,V6_Changes (ServiceTeamType from both 101 and 102)
 %sql
 DROP TABLE IF EXISTS $db_output.ServiceTeamType;
 CREATE TABLE IF NOT EXISTS $db_output.ServiceTeamType AS
 ----MHS102 All
 SELECT
 s.UniqMonthID,
 s.OrgIDProv,
 s.Person_ID,
 s.UniqServReqID,
 COALESCE(s.UniqCareProfTeamID, s.UniqOtherCareProfTeamLocalID) as UniqCareProfTeamID,
 s.ServTeamTypeRefToMH,
 s.ServTeamIntAgeGroup,
 s.ReferClosureDate,
 s.ReferClosReason,
 s.ReferRejectionDate,
 s.ReferRejectReason,
 s.RecordNumber,
 s.RecordStartDate,
 s.RecordEndDate
 from $db_source.mhs102otherservicetype s
 UNION ALL
 ----MHS101 v6
 SELECT
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.UniqServReqID,
 r.UniqCareProfTeamLocalID as UniqCareProfTeamID, 
 r.ServTeamType as ServTeamTypeRefToMH,
 r.ServTeamIntAgeGroup,
 r.ServDischDate as ReferClosureDate,
 r.ReferClosReason,
 r.ReferRejectionDate,
 r.ReferRejectReason,
 r.RecordNumber,
 r.RecordStartDate,
 r.RecordEndDate
 from $db_source.mhs101referral r
 where UniqMonthID > 1488

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS102ServiceTypeReferredTo_LATEST AS
     SELECT DISTINCT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.ServTeamTypeRefToMH,
            r.ReferClosureDate,
            r.ReferRejectionDate
       FROM $db_output.ServiceTeamType AS r                                                     ----V6_Changes
       WHERE ((r.RecordEndDate IS NULL OR r.RecordEndDate >= '$rp_enddate') AND r.RecordStartDate <= '$rp_enddate');

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS501HospProvSpell_LATEST AS
     SELECT DISTINCT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.StartDateHospProvSpell,
            r.DischDateHospProvSpell,
            r.UniqHospProvSpellID
       FROM $db_source.MHS501HospProvSpell AS r
       WHERE ((r.RecordEndDate IS NULL OR r.RecordEndDate >= '$rp_enddate') AND r.RecordStartDate <= '$rp_enddate');

# COMMAND ----------

# DBTITLE 1,CCG Methodology update
 %sql
 --27-01-2022: renaming from ED_CCG_LATEST to MHS001_CCG_LATEST throughout as this table is used in multiple places and contains the same data!

 TRUNCATE TABLE $db_output.MHS001_CCG_LATEST;

 INSERT INTO TABLE $db_output.MHS001_CCG_LATEST
 select distinct    a.Person_ID,
 				   CASE WHEN b.UniqMonthID <= 1467 AND b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
 					    WHEN b.UniqMonthID > 1467 AND b.OrgIDSubICBLocGP IS NOT NULL and e1.ORG_CODE is not null THEN b.OrgIDSubICBLocGP
 					    WHEN a.UniqMonthID <= 1467 AND A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
                         WHEN a.UniqMonthID > 1467 AND A.OrgIDSubICBLocResidence IS NOT NULL and c1.ORG_CODE is not null THEN A.OrgIDSubICBLocResidence
 						ELSE 'UNKNOWN' END AS IC_Rec_CCG		
 FROM               $db_source.mhs001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    --and b.OrgIDGPPrac <> '-1' 
                    and b.EndDateGMPRegistration is null
 INNER JOIN         global_temp.CCG_prep_3months ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.RD_CCG_LATEST c on a.OrgIDCCGRes = c.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST c1 on a.OrgIDSubICBLocResidence = c1.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e1 on b.OrgIDSubICBLocGP = e1.original_ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or e1.ORG_CODE is not null or c1.ORG_CODE is not null)
                    and a.uniqmonthid between $month_id - 2 AND $month_id

# COMMAND ----------

# DBTITLE 1,CCG Methodology update 12m
 %sql
 --27-01-2022: renaming from ED_CCG_LATEST to MHS001_CCG_LATEST throughout as this table is used in multiple places and contains the same data!

 TRUNCATE TABLE $db_output.MHS001_CCG_LATEST_12m;

 INSERT INTO TABLE $db_output.MHS001_CCG_LATEST_12m
 select distinct    a.Person_ID,
 				   CASE WHEN b.UniqMonthID <= 1467 AND b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
 					    WHEN b.UniqMonthID > 1467 AND b.OrgIDSubICBLocGP IS NOT NULL and e1.ORG_CODE is not null THEN b.OrgIDSubICBLocGP
 					    WHEN a.UniqMonthID <= 1467 AND A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
                         WHEN a.UniqMonthID > 1467 AND A.OrgIDSubICBLocResidence IS NOT NULL and c1.ORG_CODE is not null THEN A.OrgIDSubICBLocResidence
 						ELSE 'UNKNOWN' END AS IC_Rec_CCG		
 FROM               $db_source.mhs001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPReg NOT IN ('V81999','V81998','V81997')
                    --and b.OrgIDGPPrac <> '-1' 
                    and b.EndDateGMPRegistration is null
 INNER JOIN         global_temp.CCG_prep_12months ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.RD_CCG_LATEST c on a.OrgIDCCGRes = c.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST c1 on a.OrgIDSubICBLocResidence = c1.original_ORG_CODE
 LEFT JOIN          $db_output.RD_CCG_LATEST e1 on b.OrgIDSubICBLocGP = e1.original_ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or e1.ORG_CODE is not null or c1.ORG_CODE is not null)
                    and a.uniqmonthid between $month_id - 11 AND $month_id

# COMMAND ----------

# DBTITLE 1,MHS001_PATMRECINRP_201819_F_M
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS001_PATMRECINRP_201819_F_M AS

 SELECT MPI.Person_ID,
        MPI.UniqSubmissionID,
        MPI.UniqMonthID,
        CASE WHEN x.Person_ID IS NULL THEN False ELSE True END AS  PatMRecInRP_temp
        

 FROM   $db_source.mhs001MPI MPI
 LEFT JOIN
 (
 SELECT Person_ID,
        UniqMonthID,
        MAX (UniqSubmissionID) AS UniqSubmissionID
        
 FROM   $db_source.mhs001MPI

 WHERE  UniqMonthID IN ('1427', '1428')

 GROUP BY Person_ID, UniqMonthID
 ) AS x
 ON MPI.Person_ID = x.Person_ID AND MPI.UniqSubmissionID = x.UniqSubmissionID AND MPI.UniqMonthID = x.UniqMonthID

 WHERE MPI.UniqMonthID IN ('1427', '1428');

# COMMAND ----------

# DBTITLE 1,MHS001MPI_PATMRECINRP_FIX
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW MHS001MPI_PATMRECINRP_FIX AS

 SELECT MPI.*,
        CASE WHEN FIX.Person_ID IS NULL THEN MPI.PatMRecInRP ELSE FIX.PatMRecInRP_temp END AS PatMRecInRP_FIX

 FROM $db_source.mhs001MPI MPI

 LEFT JOIN global_temp.MHS001_PATMRECINRP_201819_F_M FIX
 ON MPI.Person_ID = FIX.Person_ID AND MPI.UniqSubmissionID = FIX.UniqSubmissionID and MPI.UniqMonthID = FIX.UniqMonthID;

# COMMAND ----------

# DBTITLE 1,MHS001MPI_latest_month_data - MPI Table with latest month and latest patient info, with CCG data attached
 %sql

 TRUNCATE table $db_output.MHS001MPI_latest_month_data;

 INSERT INTO $db_output.MHS001MPI_latest_month_data 
     (SELECT MPI.AgeDeath 
            ,MPI.AgeRepPeriodEnd 
            ,MPI.AgeRepPeriodStart 
            ,MPI.CCGGPRes 
            ,MPI.County 
            ,MPI.DefaultPostcode 
            ,MPI.ElectoralWard 
            ,MPI.EthnicCategory 
            ,MPI.Gender 
            ,MPI.IMDQuart 
            ,MPI.LADistrictAuth 
            ,MPI.LDAFlag 
            ,"" AS LSOA 
            ,MPI.LSOA2011 
            ,MPI.LanguageCodePreferred 
            ,MPI.LocalPatientId 
            ,MPI.MHS001UniqID 
            ,MPI.MaritalStatus 
            ,MPI.NHSDEthnicity 
            ,MPI.NHSNumber 
            ,MPI.NHSNumberStatus 
            ,MPI.OrgIDCCGRes 
            ,MPI.OrgIDEduEstab 
            ,MPI.OrgIDLocalPatientId 
            ,MPI.OrgIDProv 
            ,MPI.PatMRecInRP 
            ,MPI.Person_ID 
            ,MPI.PostcodeDistrict 
            ,MPI.RecordEndDate 
            ,MPI.RecordNumber 
            ,MPI.RecordStartDate 
            ,MPI.RowNumber 
            ,MPI.UniqMonthID 
            ,MPI.UniqSubmissionID
            ,CCG.IC_Rec_CCG
            ,CCG.NAME
       FROM $db_source.MHS001MPI MPI
  LEFT JOIN $db_output.CCG CCG
            ON MPI.Person_ID = CCG.Person_ID
      WHERE UniqMonthID = $month_id
            AND MPI.PatMRecInRP = true) --PatMRecInRP selects latest version of MPI for each person.
            
 -- changed temporary view to table insert to materialise query called 69 times in the remaining code.         

# COMMAND ----------

# DBTITLE 1,REFS
 %sql

 /** Added to support code needed for v4.1 when CAMHSTier removed **/

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW REFS AS 

 SELECT 
 DISTINCT 
 a.UniqServReqID,
 a.Person_ID,
 CASE 
             WHEN AgeServReferRecDate BETWEEN 3 AND 17 THEN 'Under 18'  
             WHEN AgeServReferRecDate >=18 THEN '18 or over'
             ELSE 'Unknown'
 END AS AgeCat,
 s.ServTeamTypeRefToMH AS TeamType,
 s.UniqCareProfTeamID
 FROM $db_source.MHS101REFERRAL A 
 LEFT JOIN $db_output.ServiceTeamType S ON A.UNIQSERVREQID = S.UNIQSERVREQID AND A.PERSON_ID = S.PERSON_ID AND S.UNIQMONTHID = $month_id
 WHERE A.UniqMonthID = $month_id


# COMMAND ----------

# DBTITLE 1,TEAMTYPE
 %sql

 /** Added to support code needed for v4.1 when CAMHSTier removed **/

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW TEAMTYPE AS 

 SELECT
 r.UniqCareProfTeamID,
 SUM(CASE WHEN r.UniqServReqID IS NOT NULL THEN 1 ELSE 0 END) AS TotalRefs,
 SUM(CASE WHEN r.AgeCat = 'Under 18' THEN 1 ELSE 0 END) AS TotalU18Refs,
 (SUM(CASE WHEN r.AgeCat = 'Under 18' THEN 1 ELSE 0 END) / SUM(CASE WHEN r.UniqServReqID IS NOT NULL THEN 1 ELSE 0 END)) *100 AS PRCNT_U18
             
 FROM global_temp.REFS r

 GROUP BY r.UniqCareProfTeamID


# COMMAND ----------

# DBTITLE 1,ward_type_list
 %sql

 /**CODE TO SPLIT THE DIFFERENT SERVICE AREAS FOR WARD STAYS**/
 /** Updated for v4.1 when CAMHSTier removed **/

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list AS
     SELECT  DISTINCT CASE --WHEN CAMHSTier IN ('4','9') THEN 'Y' -- Removed when CAMHSTier was discontinued
 				--	      WHEN WardIntendedClinCareMH IN ('61', '62', '63') THEN NULL
                           WHEN WardIntendedClinCareMH IN ('61', '62', '63') THEN NULL  
                           WHEN TreatFuncCodeMH = '700' THEN NULL
                           WHEN WardType = '05' THEN NULL 
                           
                           WHEN TreatFuncCodeMH = '711' THEN 'Y'
 						  WHEN WardType IN ('01', '02') THEN 'Y'
 						  WHEN WardAge IN ('10','11','12') THEN 'Y'
                           
                           WHEN PRCNT_U18 > 50 THEN 'Y'  ---prior v6 
                           WHEN ServTeamIntAgeGroup = '02' THEN 'Y' ---v6
                           
 						  ELSE NULL END AS CAMHS
                           
 			,CASE WHEN WardIntendedClinCareMH IN ('61', '62', '63') THEN 'Y'
 				  WHEN TreatFuncCodeMH = '700' THEN 'Y'
 				  WHEN WardType = '05' THEN 'Y'
 				  ELSE NULL END AS LD
                   
 			,CASE WHEN WardType IN ('01', '02', '05') THEN NULL 
 				  WHEN WardIntendedClinCareMH in ('61', '62', '63') THEN NULL
 				  --WHEN CAMHSTier IN ('4','9') THEN NULL -- Removed when CAMHSTier was discontinued
 				  WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
 				  WHEN WardAge IN ('10', '11', '12') THEN NULL
                   
                   WHEN D.ServTeamIntAgeGroup = '02' THEN NULL ---v6
                   WHEN PRCNT_U18 > 50 THEN NULL
                   
 				  WHEN WardAge IN ('13', '14', '15') THEN 'Y'
 				  WHEN WardIntendedClinCareMH IN ('51', '52', '53') THEN 'Y'
 				  WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
 				  WHEN WardType IN ('03', '04', '06') THEN 'Y'
                   
                   WHEN PRCNT_U18 <= 50 THEN 'Y'
                   
 				  ELSE 'Y' END AS MH
                   
 			,UniqWardStayID
             
       FROM	$db_source.MHS001MPI AS A
 INNER JOIN  $db_source.MHS502WardStay AS B
 			ON A.Person_ID = B.Person_ID 
             AND B.UniqMonthID = $month_id  
  LEFT JOIN  $db_source.MHS503AssignedCareProf AS C
 			ON B.UniqHospProvSpellID = C.UniqHospProvSpellID 
             AND C.UniqMonthID = $month_id 
             AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf > '$rp_enddate')
  LEFT JOIN  $db_output.ServiceTeamType AS D
 			ON B.UniqServReqID = D.UniqServReqID 
             AND D.UniqMonthID = $month_id 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (D.ReferRejectionDate IS NULL OR D.ReferRejectionDate > '$rp_enddate'))
             
  LEFT JOIN global_temp.TEAMTYPE AS Z 
 			ON D.UniqCareProfTeamID = Z.UniqCareProfTeamID

      WHERE	A.UniqMonthID = $month_id
 			AND A.PatMRecInRP = true
 			AND (EndDateWardStay IS NULL OR EndDateWardStay > '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,ward_stay_cats
 %sql

 --CREATES A DISTINCT VERSION OF THE SERVICE AREA BREAKDOWNS
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_stay_cats AS
 SELECT  DISTINCT UniqWardStayID
                  ,MIN (LD) AS LD
                  ,MIN (CAMHS) AS CAMHS
                  ,MIN (MH) AS MH
   FROM  global_temp.ward_type_list
 GROUP BY	UniqWardStayID

# COMMAND ----------

# DBTITLE 1,referral_list
 %sql

 /**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/
 /** updated for v4.1 when CAMHSTier removed **/

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list AS
     SELECT	CASE WHEN F.LD = 'Y' THEN 'Y'
 				 WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN 'Y'
 				 ELSE NULL END AS LD
                  
 			,CASE WHEN F.CAMHS = 'Y' THEN 'Y'            
                   WHEN F.LD = 'Y' THEN NULL
                   WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN NULL
 				  --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN 'Y'  -- Removed when CAMHSTier was discontinued
 				  WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07') THEN 'Y'
                   WHEN PRCNT_U18 > 50 THEN 'Y'
 				  ELSE NULL END AS CAMHS
                   
 			,CASE WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01', 'C05', 'C06', 'C07') THEN NULL 
                   WHEN F.LD = 'Y' THEN NULL 
                   WHEN F.CAMHS = 'Y' THEN NULL 
                   WHEN F.MH = 'Y' THEN 'Y'
 				 
                   WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
 				  --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN NULL -- Removed when CAMHSTier was discontinued
                  
                   WHEN PRCNT_U18 > 50 THEN NULL
                   WHEN E.ServTeamIntAgeGroup = '02' THEN NULL ---v6
                   
                   WHEN vc.ValidValue IS NOT NULL THEN 'Y'				  
                   WHEN PRCNT_U18 <= 50 THEN 'Y'
 				  ELSE 'Y' END AS MH
                   
 			,D.UniqServReqID
             
       FROM	$db_source.MHS101Referral AS D
                    
  LEFT JOIN  $db_source.MHS001MPI AS A
 			ON A.Person_ID = D.Person_ID 
             AND A.PatMRecInRP = 'Y'
             AND A.UniqMonthID = $month_id
  LEFT JOIN  $db_source.MHS501HospProvSpell AS G
 			ON G.UniqServReqID = D.UniqServReqID 
             AND G.UniqMonthID = $month_id
  LEFT JOIN  $db_source.MHS502WardStay AS B
 			ON G.UniqHospProvSpellID = B.UniqHospProvSpellID 
             AND B.UniqMonthID = $month_id 
  LEFT JOIN  global_temp.ward_stay_cats AS F
 			ON B.UniqWardStayID = F.UniqWardStayID
  LEFT JOIN  $db_output.ServiceTeamType AS E
 			ON D.UniqServReqID = E.UniqServReqID 
             AND E.UniqMonthID = $month_id 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (E.ReferRejectionDate IS NULL OR E.ReferRejectionDate > '$rp_enddate'))
  
   LEFT JOIN $db_output.validcodes as vc
             ON vc.tablename = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'referral_list' and vc.type = 'include' and E.ServTeamTypeRefToMH = vc.ValidValue 
              and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
              
   LEFT JOIN global_temp.TEAMTYPE AS Z 
 			ON E.UniqCareProfTeamID = Z.UniqCareProfTeamID

      WHERE	(D.ServDischDate IS NULL OR D.ServDischDate >'$rp_enddate')
 		    AND D.UniqMonthID = $month_id

# COMMAND ----------

# DBTITLE 1,referral_cats
 %sql

 --CREATES A DISTINCT VERSION OF THE SERVICE AREA BREAKDOWNS

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_cats AS
 SELECT  DISTINCT UniqServReqID
         ,MIN (LD) AS LD
         ,MIN (CAMHS) AS CAMHS
         ,MIN (MH) AS MH
   FROM	global_temp.referral_list
 GROUP BY	UniqServReqID

# COMMAND ----------

# DBTITLE 1,MHS101Referral_service_area
 %sql TRUNCATE TABLE $db_output.MHS101Referral_service_area;
 INSERT INTO
   $db_output.MHS101Referral_service_area
 SELECT
 --  A.AMHServiceRefEndRP,
   A.AgeServReferDischDate,
   A.AgeServReferRecDate,
 -- A.CYPServiceRefEndRP,
 --  A.CYPServiceRefStartRP,
   A.ClinRespPriorityType,
   A.CountAttendedCareContactsInFinancialYearPersonWas017AtTimeOfContact,
   A.CountOfAttendedCareContacts,
   A.CountOfAttendedCareContactsInFinancialYear,
   A.DecisionToTreatDate,
   A.DecisionToTreatTime,
   --A.DischLetterIssDate,
   A.DischPlanCreationDate,
   A.DischPlanCreationTime,
   A.DischPlanLastUpdatedDate,
   A.DischPlanLastUpdatedTime,
   A.FirstAttendedContactInFinancialYearDate,
   A.FirstAttendedContactInRPDate,
   A.FirstContactEverDate,
   A.FirstContactEverWhereAgeAtContactUnder18Date,
   A.InactTimeRef,
 --  A.LDAServiceRefEndRP,
   A.LocalPatientId,
   A.MHS101UniqID,
   A.NHSServAgreeLineID,
   A.OrgIDComm,
   A.OrgIDProv,
   A.OrgIDReferringOrg,
   A.Person_ID,
   A.PrimReasonReferralMH,
   A.ReasonOAT,
   A.RecordEndDate,
   A.RecordNumber,
   A.RecordStartDate,
   A.ReferralRequestReceivedDate,
   A.ReferralRequestReceivedTime,
 --  A.ReferralServiceAreasOpenEndRPLDA,
 --  A.ReferralServiceAreasStartingInRPLDA,
   A.ReferringCareProfessionalType,
   A.RowNumber,
   A.SecondAttendedContactEverDate,
   A.SecondAttendedContactInFinancialYearDate,
   A.SecondContactEverWhereAgeAtContactUnder18Date,
   A.ServDischDate,
   A.ServDischTime,
   A.ServiceRequestId,
   A.SourceOfReferralMH,
   A.SpecialisedMHServiceCode,
   A.UniqMonthID,
   A.UniqServReqID,
   A.UniqSubmissionID,
   B.CAMHS,
   B.LD,
   B.MH
 FROM
   $db_source.MHS101Referral AS A
   LEFT JOIN global_temp.referral_cats AS B ON A.UniqServReqID = B.UniqServReqID
 WHERE
   (
     ServDischDate IS NULL
     OR ServDischDate > '$rp_enddate'
   )
   AND A.UniqMonthID = $month_id

# COMMAND ----------

# DBTITLE 1,MHS101Referral_open_end_rp - Referral table, latest month and open end RP
 %sql

 -- Replacing ref.* here with a list of all columns to allow the _v sourced menhprimary_refresh and menh_point_in_time tables to be used as source data too.

 TRUNCATE TABLE $db_output.MHS101Referral_open_end_rp;

 INSERT INTO $db_output.MHS101Referral_open_end_rp
     SELECT 
     --        ref.AMHServiceRefEndRP
             ref.AgeServReferDischDate
            ,ref.AgeServReferRecDate
    --        ,ref.CYPServiceRefEndRP
    --        ,ref.CYPServiceRefStartRP
            ,ref.ClinRespPriorityType
            ,ref.DischPlanCreationDate
            ,ref.DischPlanCreationTime
            ,ref.DischPlanLastUpdatedDate
            ,ref.DischPlanLastUpdatedTime
            ,ref.InactTimeRef
    --        ,ref.LDAServiceRefEndRP
            ,ref.LocalPatientId
            ,ref.MHS101UniqID
            ,ref.NHSServAgreeLineID     
            ,ref.OrgIDComm
            ,ref.OrgIDProv
            ,ref.OrgIDReferringOrg
            ,ref.Person_ID     
            ,ref.PrimReasonReferralMH    
            ,ref.ReasonOAT     
            ,ref.RecordEndDate
            ,ref.RecordNumber    
            ,ref.RecordStartDate
            ,ref.ReferralRequestReceivedDate
            ,ref.ReferralRequestReceivedTime
            ,ref.ReferringCareProfessionalType
            ,ref.RowNumber       
            ,ref.ServDischDate
            ,ref.ServDischTime
            ,ref.ServiceRequestId
            ,ref.SourceOfReferralMH
            ,ref.SpecialisedMHServiceCode
            ,ref.UniqMonthID     
            ,ref.UniqServReqID 
            ,ref.UniqSubmissionID
            ,case when refa.CAMHS = 'Y' then TRUE else FALSE end AS CYPServiceRefEndRP_temp
            ,case when refa.LD = 'Y' then TRUE else FALSE end AS LDAServiceRefEndRP_temp
            ,case when refa.MH = 'Y' then TRUE else FALSE end AS AMHServiceRefEndRP_temp
       FROM $db_source.MHS101Referral AS ref
 INNER JOIN $db_output.MHS101Referral_service_area AS refa
            ON ref.UniqServReqID = refa.UniqServReqID
      WHERE ref.UniqMonthID = $month_id
            AND (ref.ServDischDate IS NULL OR ref.ServDischDate > '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,accommodation_latest
 %sql

 TRUNCATE TABLE $db_output.accommodation_latest;

 INSERT INTO $db_output.accommodation_latest
     SELECT  AccommodationType,
             AccommodationTypeDate,     
             AccommodationTypeEndDate,     
             AccommodationTypeStartDate,     
             AgeAccomTypeDate,          
             LocalPatientId,   
             MHS003UniqID,   
             OrgIDProv,   
             Person_ID,   
             RecordNumber,   
             RowNumber,   
             SCHPlacementType,   
             SettledAccommodationInd,   
             UniqMonthID,   
             UniqSubmissionID       
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY AccommodationTypeDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY AccommodationTypeDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS003AccommStatus
      WHERE  AccommodationTypeDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
             AND  AccommodationType IS NOT NULL
             AND UniqMonthID <= $month_id
   ORDER BY  Person_ID

# COMMAND ----------

# DBTITLE 1,Employment_Latest
 %sql
 TRUNCATE TABLE $db_output.employment_latest;

 INSERT INTO $db_output.employment_latest
     SELECT  EmployStatus   
             ,EmployStatusEndDate     
             ,EmployStatusRecDate     
             ,EmployStatusStartDate     
             ,LocalPatientId   
             ,MHS004UniqID 
             ,OrgIDProv 
             ,patprimempconttypemh
             ,Person_ID   
             ,RecordNumber   
             ,RowNumber
             ,UniqMonthID 
             ,UniqSubmissionID 
             ,WeekHoursWorked 
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY EmployStatusRecDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY EmployStatusRecDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS004EmpStatus
       WHERE EmployStatusRecDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
       AND EmployStatus IS NOT NULL
             AND EmployStatus IS NOT NULL
             AND UniqMonthID <= $month_id
   ORDER BY  Person_ID

# COMMAND ----------

# DBTITLE 1,CASSR_mapping
 %sql

 --USING ENTITY_CODE THIS SELECTS ALL UNITARY AUTHORITIES, NON-METROPOLITAN DISTRICTS, METROPOLITAN DISTRICTS, AND LONDON BOROUGHS. NON-METROPOLITAN DISTICTS ARE THEN 
 --REPLACED BY THE RELEVANT HIGHER TIER LOCAL AUTHORITY
 --ONE WELSH CODE IS LET THROUGH TO CREATE AN 'UNKNOWN' RECORD


 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CASSR_mapping AS
 SELECT case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_CODE END as LADistrictAuth
       ,case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_NAME END as LADistrictAuthName
 	  ,CASE WHEN a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN"
             WHEN a.ENTITY_CODE in ('E07')  THEN b.GEOGRAPHY_CODE
             ELSE a.GEOGRAPHY_CODE END as CASSR
       ,CASE WHEN a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN"
             WHEN a.ENTITY_CODE in ('E07')  THEN b.GEOGRAPHY_NAME
             ELSE a.GEOGRAPHY_NAME END as CASSR_description
               
 FROM  $reference_data.ONS_CHD_GEO_LISTINGS as a
       INNER JOIN $reference_data.ONS_CHD_GEO_LISTINGS as b
         ON b.GEOGRAPHY_CODE = a.PARENT_GEOGRAPHY_CODE
         AND a.ENTITY_CODE IN ('E06', 'E07', 'E08', 'E09','W04')

 WHERE ((a.DATE_OF_TERMINATION >= '$rp_enddate' OR ISNULL(a.DATE_OF_TERMINATION))
                 AND a.DATE_OF_OPERATION <= '$rp_enddate')
       AND ((b.DATE_OF_TERMINATION >= '$rp_enddate' OR ISNULL(b.DATE_OF_TERMINATION))
                 AND b.DATE_OF_OPERATION <= '$rp_enddate')
       AND (a.GEOGRAPHY_CODE LIKE "E%" OR B.GEOGRAPHY_CODE LIKE "E%" OR a.GEOGRAPHY_CODE = "W04000869")

 ORDER BY CASSR_description

# COMMAND ----------

# DBTITLE 1,GenderCodes
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW GenderCodes AS

 select PrimaryCode, Description from $reference_data.DataDictionaryCodes
 where ItemName ='PERSON_STATED_GENDER_CODE'
 and (BusinessEndDate is null OR BusinessEndDate > '$rp_enddate')
 -- all values for BusinessStartDate in this table are null for these items - so no startdate filter is included at present...
 order by PrimaryCode

# COMMAND ----------

# DBTITLE 1,GenderCodes (new methodology)
 %sql

 TRUNCATE TABLE $db_output.Ref_GenderCodes;

 INSERT INTO $db_output.Ref_GenderCodes
 VALUES
     ('1','Male')
     ,('2','Female')
     ,('3','Non-binary')
     ,('4','Other (not listed)')
     ,('9','Indeterminate')
     ,('Unknown','Unknown')

# COMMAND ----------

# DBTITLE 1,AgeBand
 %sql
 TRUNCATE TABLE $db_output.Ref_AgeBand;
 INSERT INTO $db_output.Ref_AgeBand
 VALUES
   ('Under 18')
   ,('18 to 19')
   ,('20 to 24')
   ,('25 to 29')
   ,('30 to 34')
   ,('35 to 39')
   ,('40 to 44')
   ,('45 to 49')
   ,('50 to 54')
   ,('55 to 59')
   ,('60 to 64')
   ,('65 to 69')
   ,('70 to 74')
   ,('75 to 79')
   ,('80 to 84')
   ,('85 to 89')
   ,('90 or over')
   ,('Unknown')

# COMMAND ----------

# DBTITLE 1,EthnicGroup
 %sql
 TRUNCATE TABLE $db_output.Ref_EthnicGroup;
 INSERT INTO $db_output.Ref_EthnicGroup
 VALUES
   ('White')
   ,('Mixed')
   ,('Asian')
   ,('Black')
   ,('Other')
   ,('Not Stated')
   ,('Unknown')

# COMMAND ----------

# DBTITLE 1,IMDDecileCode
 %sql
 TRUNCATE TABLE $db_output.Ref_IMD_Decile;
 INSERT INTO $db_output.Ref_IMD_Decile
 VALUES
   ('01 Most deprived')
   ,('02 More deprived')
   ,('03 More deprived')
   ,('04 More deprived')
   ,('05 More deprived')
   ,('06 Less deprived')
   ,('07 Less deprived')
   ,('08 Less deprived')
   ,('09 Less deprived')
   ,('10 Least deprived')
   ,('Unknown')