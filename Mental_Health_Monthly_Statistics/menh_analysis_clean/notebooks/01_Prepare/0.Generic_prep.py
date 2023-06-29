# Databricks notebook source
 %md
 
 # Generic Prep assets used throughout Mental health
 -- This list has been checked. All cells are listed here IN ORDER
 
 - RD_CCG_LATEST
 - CCG_PRAC
 - CCG_PREP
 - CCG
 - RD_ORG_DAILY_LATEST
 - Provider_list
 - Provider_list_AWT
 - REFS
 - TEAMTYPE
 - ward_type_list
 - ward_stay_cats
 - referral_list
 - referral_cats
 - MHS502WardStay_service_area
 - MHS101Referral_service_area
 - MHS001MPI_latest_month_data -- Materialised
 - maybe need to add in a new 001mpi table
 - MHS101Referral_open_end_rp -- Materialised
 - MHS102ServiceTypeReferredTo
 - MHS502WardStay_open_end_rp
 - MHS501HospProvSpell_open_end_rp
 - MHS701CPACareEpisode_latest -- Materialised
 - Accomodation_latest
 - Employment_Latest
 - mhs001mpi for MHS23 AND MHS01 Added Breakdowns
 - bed_types
 - unique_bed_types
 - ward_list_in_rp
 - ward_stay_cats_in_rp
 - MHS502WardStay_service_area_discharges
 - bed_types_in_rp
 - unique_bed_types_in_rp
 - MHS401MHActPeriod_open_end_rp
 - BED_DAYS_IN_RP -- Materialised
 - BED_DAYS_IN_RP_PROV
 - HOME_LEAVE_IN_RP_PROV
 - HOME_LEAVE_IN_RP
 - LOA_IN_RP
 - LOA_IN_RP_PROV
 - ref_list_in_rp -- commented out
 - ref_cats_in_rp -- commented out
 - ward_type_list_RPstart - for testing - can use derivation
 - ward_stay_cats_RPstart - for testing - can use derivation
 - referral_list_RPstart - for testing - can use derivation
 - referral_cats_RPstart - for testing - can use derivation
 - MHS502WardStay_service_area_RPstart - for testing - can use derivation
 - MHS101Referral_service_area_RPstart - for testing - can use derivation
 - CASSR_mapping
 - ResponsibleLA_mapping
 - DelayedDischDim 
 - mhs26_ranking
 - org_daily
 - org_relationship_daily

# COMMAND ----------

# DBTITLE 1,RD_CCG_LATEST - original - commented out - didn't take into account successor orgs - retained for info
# %sql
# TRUNCATE TABLE $db_output.RD_CCG_LATEST;

# INSERT INTO TABLE $db_output.RD_CCG_LATEST
# SELECT DISTINCT ORG_CODE,
#                 NAME
#            FROM $db_source.org_daily
#           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
#                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
#                 AND ORG_TYPE_CODE = 'CC'
#                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
#                 AND ORG_OPEN_DATE <= '$rp_enddate'
#                 AND NAME NOT LIKE '%HUB'
#                 AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

# DBTITLE 1,RD_CCG_LATEST - *new - phase 1* using ODSAPI tables AND org_daily
 %sql
 
 TRUNCATE TABLE $db_output.RD_CCG_LATEST;
 
 with mappedCCGs as
 
 (SELECT DISTINCT od.ORG_CODE as original_ORG_CODE,
                 od.NAME as original_NAME,
                 COALESCE(odssd.TargetOrganisationID, od.ORG_CODE) as ORG_CODE
         FROM $db_source.org_daily od
 
          
         LEFT JOIN $db.source.ODSAPISuccessorDetails as odssd
         ON od.ORG_CODE = odssd.OrganisationID and odssd.Type = 'Successor' and odssd.StartDate <= '$rp_enddate'
         
            WHERE (od.BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(od.BUSINESS_END_DATE))
                 AND od.BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
                 AND od.ORG_TYPE_CODE = 'CC'
                 AND (od.ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(od.ORG_CLOSE_DATE))
                 AND od.ORG_OPEN_DATE <= '$rp_enddate'
                 AND od.NAME NOT LIKE '%HUB'
                 AND od.NAME NOT LIKE '%NATIONAL%' 
 )
 
 INSERT INTO TABLE $db_output.RD_CCG_LATEST
 
 SELECT mappedCCGs.*,
       od1.NAME as NAME
     FROM mappedCCGs
     LEFT JOIN (
             SELECT
             row_number() over (partition by ORG_CODE order by case when ORG_CLOSE_DATE is null then 1 else 0 end desc, case when BUSINESS_END_DATE is null then 1 else 0 end desc, ORG_CLOSE_DATE desc) as RN,
             ORG_CODE,
             NAME
             FROM $db_source.org_daily
                     
             WHERE ORG_OPEN_DATE <= '$rp_enddate'
             ) od1
             ON mappedCCGs.ORG_CODE = od1.ORG_CODE
             AND od1.RN = 1
           
 ORDER BY original_ORG_CODE

# COMMAND ----------

# DBTITLE 1,RD_CCG_LATEST - *new* using only ODSAPI tables - DO NOT DELETE - ready for correction in db_source tables
# %sql

# --    the code below can be used to replace the cell above once the issue with missing '13T' has been resolved - see DMS001-1132

# TRUNCATE TABLE $db_output.RD_CCG_LATEST;

# with mappedCCGs as

# (SELECT DISTINCT rd.OrganisationID as original_ORG_CODE,
#                 Name as original_NAME,
#                 COALESCE(odssd.TargetOrganisationID, rd.OrganisationID) as ORG_CODE
                
#         FROM $db_source.ODSAPIRoleDetails rd
        
#         LEFT JOIN $db_source.ODSAPIOrganisationDetails od
#         ON rd.OrganisationID = od.OrganisationID and rd.DateType = od.DateType
        
#         LEFT JOIN $db_source.ODSAPISuccessorDetails as odssd
#         ON rd.OrganisationID = odssd.OrganisationID and odssd.Type = 'Successor' and odssd.StartDate <= '$rp_enddate'
        
#         WHERE rd.RoleId = 'RO98'
#         AND rd.DateType = 'Legal'
#         AND (rd.EndDate >= add_months('$rp_enddate', 1) OR ISNULL(rd.EndDate))
#         AND rd.StartDate <= add_months('$rp_enddate', 1)
#         AND Name NOT LIKE '%HUB'
#         AND Name NOT LIKE '%NATIONAL%' 
# )

# INSERT INTO TABLE $db_output.RD_CCG_LATEST

# SELECT mappedCCGs.*,
#       od1.NAME as NAME
#     FROM mappedCCGs
#     LEFT JOIN (
#             SELECT
#             row_number() over (partition by rd.OrganisationId order by case when rd.EndDate is null then 1 else 0 end desc, rd.EndDate desc) as RN,
#             rd.OrganisationId as ORG_CODE,
#             Name as NAME

#         FROM $db_source.ODSAPIRoleDetails rd
#         LEFT JOIN $db_source.ODSAPIOrganisationDetails od
#         ON rd.OrganisationID = od.OrganisationID and rd.DateType = od.DateType
        
#         WHERE rd.RoleId = 'RO98'
#         AND rd.DateType = 'Legal'
#         AND (rd.EndDate >= add_months('$rp_enddate', 1) OR ISNULL(rd.EndDate))
#         AND Name NOT LIKE '%HUB'
#         AND Name NOT LIKE '%NATIONAL%' 
#         AND rd.StartDate <= '$rp_enddate'
#             ) od1
#             ON mappedCCGs.ORG_CODE = od1.ORG_CODE
#             AND od1.RN = 1
          
# ORDER BY original_ORG_CODE


# COMMAND ----------

spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='RD_CCG_LATEST'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='RD_CCG_LATEST'))

# COMMAND ----------

# DBTITLE 1,CCG_PRAC - have changed to person_id - SH 2019-04-29
 %sql
 --This table returns rows, carry forward
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PRAC AS
 
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
                     WHERE UniqMonthID = '$month_id'
                           AND GMPCodeReg NOT IN ('V81999','V81998','V81997')
                           --AND OrgIDGPPrac <> '-1' --CCG methodology change 
                           AND EndDateGMPRegistration is NULL
                  GROUP BY Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GP.UniqMonthID = '$month_id'
        AND GMPCodeReg NOT IN ('V81999','V81998','V81997')
        AND EndDateGMPRegistration is null

# COMMAND ----------

# DBTITLE 1,CCG_PREP
 %sql
 --Under month_id = 1472 this table returns rows
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PREP AS
      SELECT a.Person_ID,
             CASE
               WHEN UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice
               WHEN UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP 
               WHEN UNIQMONTHID <= 1467 then OrgIDCCGRes 
               WHEN UNIQMONTHID > 1467 then OrgIDSubICBLocResidence
               ELSE 'ERROR'
               END as IC_Rec_CCG ---> this column name is replaced by IC_REC_GP_RESC see code below, but this name is persisted for now as this column name is used in many group by clauses 
               --END AS IC_REC_GP_RES
        FROM $db_source.MHS001MPI a
   LEFT JOIN global_temp.CCG_PRAC c 
             ON a.Person_ID = c.Person_ID 
             AND a.RecordNumber = c.RecordNumber
       WHERE a.UniqMonthID = '$month_id' 
             AND a.PatMRecInRP = true

# COMMAND ----------

# DBTITLE 1,CCG
 %sql
 --now this table is populated using month_id as 1472
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG AS
 
 SELECT Person_ID
 ,CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS IC_Rec_CCG
 ,CASE WHEN NAME IS null THEN 'UNKNOWN' ELSE NAME END AS NAME
 
 FROM global_temp.CCG_PREP a
 
 LEFT JOIN $db_output.RD_CCG_LATEST b 
         ON a.IC_Rec_CCG = b.original_ORG_CODE
            
    

# COMMAND ----------

# DBTITLE 1,RD_ORG_DAILY_LATEST 
 %sql
 
 /* Org daily latest */
 --20th Sep - #COPIED_EIP
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_ORG_DAILY_LATEST AS
 SELECT DISTINCT ORG_CODE, 
                 NAME
            FROM $db_sourse.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

# COMMAND ----------

# DBTITLE 1,List of providers
 %sql
 /* List of providers (taken from EIP script) */
 
 TRUNCATE TABLE $db_output.Provider_list;
 
 INSERT INTO TABLE $db_output.Provider_list
 SELECT DISTINCT OrgIDProvider as ORG_CODE, x.NAME as NAME
           FROM $db_source.MHS000Header as Z
           LEFT OUTER JOIN global_temp.RD_ORG_DAILY_LATEST AS X
               ON Z.OrgIDProvider = X.ORG_CODE
           WHERE	Z.UniqMonthID = '$month_id'

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Provider_list'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Provider_list'))

# COMMAND ----------

# DBTITLE 1,List of providers - EIP
 %sql
 /* List of providers (taken from EIP script) */
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Provider_list_AWT AS
 SELECT DISTINCT OrgIDProvider as ORG_CODE, x.NAME as NAME
           FROM $db_source.MHS000Header as Z
           LEFT OUTER JOIN global_temp.RD_ORG_DAILY_LATEST AS X
               ON Z.OrgIDProvider = X.ORG_CODE
           WHERE	Z.UniqMonthID between '$month_id'-2 and '$month_id'

# COMMAND ----------

# DBTITLE 1,REFS - to replace CAMHSTier
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
 FROM
 $db_source.MHS101REFERRAL A 
 LEFT JOIN $db_source.MHS102SERVICETYPEREFERREDTO S ON A.UNIQSERVREQID = S.UNIQSERVREQID AND A.PERSON_ID = S.PERSON_ID AND S.UNIQMONTHID = '$month_id'
 WHERE 
 A.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,TEAMTYPE - to replace CAMHSTier
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

# DBTITLE 1,Service area end RP
 %sql
 
 /**CODE TO SPLIT THE DIFFERENT SERVICE AREAS FOR WARD STAYS**/
 /** Updated for v4.1 when CAMHSTier removed **/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list AS
     SELECT  DISTINCT CASE --WHEN CAMHSTier IN ('4','9') THEN 'Y'
 					      WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN NULL
                           WHEN TreatFuncCodeMH = '700' THEN NULL
                           WHEN WardType = '05' THEN NULL 
                           
                           WHEN TreatFuncCodeMH = '711' THEN 'Y'
 						  WHEN WardType IN ('01', '02') THEN 'Y'
 						  WHEN WardAge IN ('10','11','12') THEN 'Y'
                           
                           WHEN PRCNT_U18 > 50 THEN 'Y'
                           
 						  ELSE NULL END AS CAMHS
                           
 			,CASE WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
 				  WHEN TreatFuncCodeMH = '700' THEN 'Y'
 				  WHEN WardType = '05' THEN 'Y'
 				  ELSE NULL END AS LD
                   
 			,CASE WHEN WardType IN ('01', '02', '05') THEN NULL 
 				  WHEN IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
 				  --WHEN CAMHSTier IN ('4','9') THEN NULL 
 				  WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
 				  WHEN WardAge IN ('10', '11', '12') THEN NULL
                   
                   WHEN PRCNT_U18 > 50 THEN NULL
                   
 				  WHEN WardAge IN ('13', '14', '15') THEN 'Y'
 				  WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53') THEN 'Y'
 				  WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
 				  WHEN WardType IN ('03', '04', '06') THEN 'Y'
                   
                   WHEN PRCNT_U18 <= 50 THEN 'Y'
                   
 				  ELSE 'Y' END AS MH
                   
 			,UniqWardStayID
             
       FROM	$db_source.MHS001MPI AS A
 INNER JOIN  $db_source.MHS502WardStay AS B
 			ON A.Person_ID = B.Person_ID 
             AND B.UniqMonthID = '$month_id'  
  LEFT JOIN  $db_source.MHS503AssignedCareProf AS C
 			ON B.UniqHospProvSpellID = C.UniqHospProvSpellID 
             AND C.UniqMonthID = '$month_id' 
             AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf > '$rp_enddate')
  LEFT JOIN  $db_source.MHS102ServiceTypeReferredTo AS D
 			ON B.UniqServReqID = D.UniqServReqID 
             AND D.UniqMonthID = '$month_id' 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate'))
             
  LEFT JOIN global_temp.TEAMTYPE AS Z 
 			ON D.UniqCareProfTeamID = Z.UniqCareProfTeamID
 
      WHERE	A.UniqMonthID = '$month_id'
 			AND A.PatMRecInRP = true
 			AND (EndDateWardStay IS NULL OR EndDateWardStay > '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,Ward Stay Categories
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

# DBTITLE 1,Service area end RP
 %sql
 
 /**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/
 /** Updated for v4.1 when CAMHSTier removed **/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list AS
     SELECT	CASE WHEN F.LD = 'Y' THEN 'Y'
 				 WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN 'Y'
 				 ELSE NULL END AS LD
                  
 			,CASE WHEN F.CAMHS = 'Y' THEN 'Y'
             
                   WHEN F.LD = 'Y' THEN NULL
                   WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN NULL
 
 				  --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN 'Y'
 				  WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07') THEN 'Y'
                   
                   WHEN PRCNT_U18 > 50 THEN 'Y'
                   
 				  ELSE NULL END AS CAMHS
                   
 			,CASE WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01', 'C05', 'C06', 'C07') THEN NULL 
                   WHEN F.LD = 'Y' THEN NULL 
                   WHEN F.CAMHS = 'Y' THEN NULL 
                   WHEN F.MH = 'Y' THEN 'Y'
 				 
                   WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
 				  --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN NULL
                   
                   WHEN PRCNT_U18 > 50 THEN NULL
                   
 -- 				  WHEN ServTeamTypeRefToMH IN vc.ValidValue THEN 'Y'
                   WHEN vc.ValidValue IS NOT NULL THEN 'Y'
 				  
                   WHEN PRCNT_U18 <= 50 THEN 'Y'
 				  ELSE 'Y' END AS MH
                   
 			,D.UniqServReqID
             
       FROM	$db_source.MHS101Referral AS D
                    
  LEFT JOIN  $db_source.MHS001MPI AS A
 			ON A.Person_ID = D.Person_ID 
             AND A.PatMRecInRP = 'Y'
             AND A.UniqMonthID = '$month_id'
  LEFT JOIN  $db_source.MHS501HospProvSpell AS G
 			ON G.UniqServReqID = D.UniqServReqID 
             AND G.UniqMonthID = '$month_id'
  LEFT JOIN  $db_source.MHS502WardStay AS B
 			ON G.UniqHospProvSpellID = B.UniqHospProvSpellID 
             AND B.UniqMonthID = '$month_id' 
  LEFT JOIN  global_temp.ward_stay_cats AS F
 			ON B.UniqWardStayID = F.UniqWardStayID
  LEFT JOIN  $db_source.MHS102ServiceTypeReferredTo AS E
 			ON D.UniqServReqID = E.UniqServReqID 
             AND E.UniqMonthID = '$month_id' 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate'))
  
   LEFT JOIN $db_output.validcodes as vc
             ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'referral_list' and vc.type = 'include' and E.ServTeamTypeRefToMH = vc.ValidValue 
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
              
   LEFT JOIN global_temp.TEAMTYPE AS Z 
 			ON E.UniqCareProfTeamID = Z.UniqCareProfTeamID
 
      WHERE	(D.ServDischDate IS NULL OR D.ServDischDate >'$rp_enddate')
 		    AND D.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,Service area end RP
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

# DBTITLE 1,Service area end RP
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS502WardStay_service_area AS
 SELECT	A.*
         ,B.CAMHS
         ,B.LD
         ,B.MH 
   FROM	$db_source.MHS502WardStay AS A
 LEFT JOIN  global_temp.ward_stay_cats AS B
         ON A.UniqWardStayID = B.UniqWardStayID
  WHERE	(EndDateWardStay IS NULL OR EndDateWardStay > '$rp_enddate')
         AND A.UniqMonthID = '$month_id'   

# COMMAND ----------

# DBTITLE 1,Service area end RP
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS101Referral_service_area AS
 SELECT  A.*
         ,B.CAMHS
         ,B.LD
         ,B.MH
   FROM	$db_source.MHS101Referral AS A
 LEFT JOIN  global_temp.referral_cats AS B
         ON A.UniqServReqID = B.UniqServReqID
  WHERE	(ServDischDate IS NULL OR ServDischDate > '$rp_enddate')
         AND A.UniqMonthID = '$month_id'

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
            ,MPI.MPSConfidence 
            ,MPI.MaritalStatus 
            ,MPI.NHSDEthnicity 
            ,MPI.NHSNumber 
            ,MPI.NHSNumberStatus 
            ,MPI.OrgIDCCGRes --SubICB Residence isnt included because it would mean a changeto table structure and not used in the notebook.
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
  LEFT JOIN global_temp.CCG CCG
            ON MPI.Person_ID = CCG.Person_ID
      WHERE UniqMonthID = '$month_id'
            AND MPI.PatMRecInRP = true) 
            
 -- changed temporary view to table insert to materialise query called 69 times in the remaining code.          

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS001MPI_latest_month_data'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS001MPI_latest_month_data'))

# COMMAND ----------

# DBTITLE 1,MHS101Referral_open_end_rp - Referral table, latest month and open end RP
 %sql
 
 -- replacing ref.* here with a list of all columns to allow the _v sourced menhprimary_refresh and menh_point_in_time tables to be used as source data too.
 
 TRUNCATE TABLE $db_output.MHS101Referral_open_end_rp;
 
 INSERT INTO $db_output.MHS101Referral_open_end_rp
     SELECT 
 --     ref.*
             ref.AMHServiceRefEndRP
            ,ref.AgeServReferDischDate
            ,ref.AgeServReferRecDate
            ,ref.CYPServiceRefEndRP
            ,ref.CYPServiceRefStartRP
            ,ref.ClinRespPriorityType
            ,ref.DischPlanCreationDate
            ,ref.DischPlanCreationTime
            ,ref.DischPlanLastUpdatedDate
            ,ref.DischPlanLastUpdatedTime
            ,ref.InactTimeRef
            ,ref.LDAServiceRefEndRP
            ,ref.LocalPatientId
            ,ref.MHS101UniqID
            ,ref.NHSServAgreeLineNum     
            ,ref.OrgIDComm
            ,ref.OrgIDProv
            ,ref.OrgIDReferring
            ,ref.Person_ID     
            ,ref.PrimReasonReferralMH    
            ,ref.ReasonOAT     
            ,ref.RecordEndDate
            ,ref.RecordNumber    
            ,ref.RecordStartDate
            ,ref.ReferralRequestReceivedDate
            ,ref.ReferralRequestReceivedTime
            ,ref.ReferringCareProfessionalStaffGroup
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
 INNER JOIN global_temp.MHS101Referral_service_area AS refa
            ON ref.UniqServReqID = refa.UniqServReqID
      WHERE ref.UniqMonthID = '$month_id'
            AND (ref.ServDischDate IS NULL OR ref.ServDischDate > '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS101Referral_open_end_rp'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS101Referral_open_end_rp'))

# COMMAND ----------

# DBTITLE 1,MHS102ServiceTypeReferredTo
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS102ServiceTypeReferredTo AS
       SELECT SRV.*              
         FROM $db_source.MHS102ServiceTypeReferredTo AS SRV
        WHERE SRV.UniqMonthID = '$month_id'
              AND ((((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate > '$rp_enddate') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate > '$rp_enddate'))) OR SRV.ReferClosureDate <= '$rp_enddate' OR        SRV.ReferRejectionDate <= '$rp_enddate')             

# COMMAND ----------

 %sql
 DESCRIBE $db_source.MHS102ServiceTypeReferredTo

# COMMAND ----------

# DBTITLE 1,MHS502WardStay_open_end_rp
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS502WardStay_open_end_rp AS
     SELECT WRD.*
            ,case when WRDa.CAMHS = 'Y' then 'TRUE' else 'FALSE' end AS CYPServiceWSEndRP_temp
            ,case when WRDa.LD = 'Y' then 'TRUE' else 'FALSE' end AS LDAServiceWSEndRP_temp
            ,case when WRDa.MH = 'Y' then 'TRUE' else 'FALSE' end AS AMHServiceWSEndRP_temp
       FROM $db_source.MHS502WardStay WRD
 INNER JOIN global_temp.MHS502WardStay_service_area as WRDa
            ON WRD.UniqWardStayID = WRDa.UniqWardStayID
      WHERE WRD.UniqMonthID = '$month_id' 
 		   AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay > '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,MHS501HospProvSpell_open_end_rp
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS501HospProvSpell_open_end_rp AS
     SELECT HSP.*
       FROM $db_source.MHS501HospProvSpell HSP
      WHERE HSP.UniqMonthID = '$month_id'
            AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell > '$rp_enddate') 
            
  --          DROP table IF EXISTS $db_output.tmp_mhmab_mhs501hospprovspell_latest_month_data;
  
 --This table is exactly the same as the anaylyst version used in DAE labelled as $db_output.tmp_mhmab_mhs501hospprovspell_latest_month_data
            

# COMMAND ----------

# DBTITLE 1,MHS701CPACareEpisode_latest - CPA Intermediate Table
 %sql
 
 TRUNCATE TABLE $db_output.MHS701CPACareEpisode_latest;
 
 INSERT INTO TABLE $db_output.MHS701CPACareEpisode_latest
   SELECT CPAEpisodeId
          ,EndDateCPA
          ,LocalPatientId
          ,MHS701UniqID
          ,OrgIDProv
          ,Person_ID
          ,RecordEndDate
          ,RecordNumber
          ,RecordStartDate
          ,RowNumber
          ,StartDateCPA
          ,UniqCPAEpisodeID
          ,UniqMonthID
          ,UniqSubmissionID
     FROM $db_source.MHS701CPACareEpisode
    WHERE UniqMonthID = '$month_id'
          AND (EndDateCPA IS NULL OR EndDateCPA > '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS701CPACareEpisode_latest'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS701CPACareEpisode_latest'))

# COMMAND ----------

# DBTITLE 1,Accomodation_latest
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Accomodation_Latest AS
     SELECT  *
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY AccommodationTypeDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY AccommodationTypeDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS003AccommStatus
      WHERE  
             AccommodationTypeDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
             AND  AccommodationType IS NOT NULL
             AND UniqMonthID <= '$month_id'
   ORDER BY  Person_ID

# COMMAND ----------

 %sql
 SELECT 
 RIGHT(global_temp.Accomodation_Latest.AccommodationType,2) AS AccommodationType
 ,dictionary_accom.PrimaryCode as Accomodation_code
 ,dictionary_accom.Description as Accomodation_Desc
 from global_temp.Accomodation_Latest
 LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'ACCOMMODATION_TYPE') dictionary_accom
 ON 
 RIGHT(global_temp.Accomodation_Latest.AccommodationType,2) = dictionary_accom.PrimaryCode

# COMMAND ----------

# DBTITLE 1,Employment_Latest
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Employment_Latest AS
     SELECT  *
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY EmployStatusRecDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY EmployStatusRecDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS004EmpStatus
      WHERE  --EmployStatusRecDate BETWEEN DATEADD(MM,-12,dateadd(DD,1,'$rp_enddate')) AND '$rp_enddate'
             EmployStatusRecDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
             AND EmployStatus IS NOT NULL
             AND UniqMonthID <= '$month_id'
   ORDER BY  Person_ID

# COMMAND ----------

# DBTITLE 1,DELETE LATER--The Columns that appear for Employment_Latest
# %sql 
# --EmployStatus from Employment_Latest
# SELECT 
# EmployStatus AS EmploymentType
# ,dss_emp.PrimaryCode AS Employment_Code
# ,dss_emp.Description as Emplyment_Desc
# FROM global_temp.Employment_Latest
# LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'EMPLOYMENT_STATUS') dss_emp
# ON Employment_Latest.EmployStatus = dss_emp.PrimaryCode;

# --Select DISTINCT EmployStatus 
# --from Employment_Latest
# --ORDER BY EmployStatus
# --WHERE EmployStatus NOT IN ('01','02','03','04','05','06','07','08','ZZ')---RETURNS NO RESULTS.Therefore within Employment_Latest View, there are no codes under the column EmployStatus that are not the valid list 


# COMMAND ----------

# DBTITLE 1,DELETE LATER-What JOINS may work for Ethnicty breadowns?
# %sql
# --ETHNIC_CATEGORY_CODE
# --Whilst the code 99 appears in the $db_source.MHS001MPI.NHSDEthnicity it doesn't appear in the dss_corop data base as it identifies it as NULL. This is consistent with the CASE WHEN which dictates 99 to be 'NOT KNOWN' code.
# SELECT 
# $db_source.MHS001MPI.NHSDEthnicity
# ,dss_ethnic.PrimaryCode as Ethnicity_Code
# ,dss_ethnic.Description as Ethnicity_Desc
# FROM $db_source.MHS001MPI  
# LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'ETHNIC_CATEGORY_CODE') dss_ethnic
# ON $db_source.MHS001MPI.NHSDEthnicity = dss_ethnic.PrimaryCode
# LIMIT 10
# --WHERE $db_source.MHS001MPI.NHSDEthnicity = 99

# COMMAND ----------

# DBTITLE 1,DELETE LATER-What Joins may work for Disability Breakdowns?
# %sql
# SELECT 
# $db_source.MHS007DisabilityType.Disabcode as DisabilityType
# ,dss_disab.PrimaryCode as Disability_Code
# ,dss_disab.Description as Disability_Desc
# FROM $db_source.MHS007DisabilityType
# LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'DISABILITY_CODE') dss_disab
# ON $db_source.MHS007DisabilityType.Disabcode = dss_disab.PrimaryCode
# --WHERE dss_disab.PrimaryCode IS NULL

# COMMAND ----------

# DBTITLE 1,DELETE LATER-JUST TESTING MHS001SOC PREP
# %sql
# SELECT 
# PERSON_ID
# ,ORGIDPROV
# ,SocPerCircumstance
# ,SocPerCircumstanceRecTimestamp
# ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY uniqmonthid desc, SocPerCircumstanceRecTimestamp DESC, CASE WHEN SocPerCircumstance in ('699042003','440583007') THEN 2 else 1 end asc) AS RANK
# ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY uniqmonthid desc, SocPerCircumstanceRecTimestamp DESC) AS PROV_RANK
# FROM $db_source.MHS011SocPerCircumstances A




# COMMAND ----------

# DBTITLE 1,Creating temp view for mhs011socpercircircumstance_latest_month_data
 %sql
 
 
 CREATE OR REPLACE GLOBAL TEMP VIEW tmp_mhmab_mhs011socpercircircumstance_latest_month_data AS
 (
 SELECT 
 PERSON_ID
 ,ORGIDPROV
 ,SocPerCircumstance
 ,SocPerCircumstanceRecTimestamp
 ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY uniqmonthid desc, SocPerCircumstanceRecTimestamp DESC, CASE WHEN SocPerCircumstance in ('699042003','440583007') THEN 2 else 1 end asc) AS RANK
 ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY uniqmonthid desc, SocPerCircumstanceRecTimestamp DESC) AS PROV_RANK
 FROM $db_source.MHS011SocPerCircumstances A
 INNER JOIN $db_source.SNOMED_SCT2_REFSET_FULL  B ON a.SocPerCircumstance = ReferencedComponentID and ACTIVE = '1' AND REFSETID = '999003081000000103' 
 INNER JOIN 
 (SELECT REFERENCEDCOMPONENTID
 ,MAX(EFFECTIVETIME) AS EFFECTIVETIME 
 FROM $db_source.SNOMED_SCT2_REFSET_FULL 
 WHERE REFSETID = '999003081000000103' 
 GROUP BY REFERENCEDCOMPONENTID) C 
 ON B.EFFECTIVETIME = C.EFFECTIVETIME
 WHERE UNIQMONTHID <= '$month_id') 

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW MPI001_prep AS
 SELECT 
             $db_source.MHS001MPI.AgeDeath 
            ,$db_source.MHS001MPI.AgeRepPeriodEnd
            ,$db_source.MHS001MPI.AgeRepPeriodStart            
            , $db_source.MHS001MPI.County 
            , $db_source.MHS001MPI.DefaultPostcode 
            , $db_source.MHS001MPI.ElectoralWard 
            , $db_source.MHS001MPI.EthnicCategory 
            , $db_source.MHS001MPI.Gender
            , $db_source.MHS001MPI.GenderIDCode      
            , $db_source.MHS001MPI.IMDQuart 
            , $db_source.MHS001MPI.LADistrictAuth 
            , $db_source.MHS001MPI.LDAFlag 
            ,"" AS LSOA 
            ,$db_source.MHS001MPI.LSOA2011 
            , $db_source.MHS001MPI.LanguageCodePreferred 
            , $db_source.MHS001MPI.LocalPatientId 
            , $db_source.MHS001MPI.MHS001UniqID 
            , $db_source.MHS001MPI.MPSConfidence 
            , $db_source.MHS001MPI.MaritalStatus 
            , $db_source.MHS001MPI.NHSDEthnicity
            , $db_source.MHS001MPI.NHSNumber 
            , $db_source.MHS001MPI.NHSNumberStatus 
            , $db_source.MHS001MPI.OrgIDCCGRes 
            , $db_source.MHS001MPI.OrgIDEduEstab 
            , $db_source.MHS001MPI.OrgIDLocalPatientId 
            , $db_source.MHS001MPI.OrgIDProv 
            , $db_source.MHS001MPI.OrgIDResidenceResp 
            , $db_source.MHS001MPI.PatMRecInRP 
            , $db_source.MHS001MPI.Person_ID 
            , $db_source.MHS001MPI.PostcodeDistrict 
            , $db_source.MHS001MPI.RecordEndDate 
            , $db_source.MHS001MPI.RecordNumber 
            , $db_source.MHS001MPI.RecordStartDate 
            , $db_source.MHS001MPI.RowNumber 
            , $db_source.MHS001MPI.UniqMonthID 
            , $db_source.MHS001MPI.UniqSubmissionID  
       ,dss_ethnic.PrimaryCode as Ethnicity_Code
       ,dss_ethnic.Description as Ethnicity_Desc
      FROM $db_source.MHS001MPI  
      LEFT JOIN (SELECT PrimaryCode, Description FROM $db_source.datadictionarycodes WHERE ItemName = 'ETHNIC_CATEGORY_CODE') dss_ethnic
      ON $db_source.MHS001MPI.NHSDEthnicity = dss_ethnic.PrimaryCode

# COMMAND ----------

# DBTITLE 1,Used in the third JOIN within $db_output.tmp_mhmab_mhs001mpi_latest_month_data
 %sql
 --WITH ACC AS
 --(
 CREATE OR REPLACE temp view ACC001_prep as
 SELECT 
       global_temp.Accomodation_Latest.RANK,
       global_temp.Accomodation_Latest.Person_ID,
       RIGHT(global_temp.Accomodation_Latest.AccommodationType,2) AS AccommodationType
      ,dictionary_accom.PrimaryCode as Accomodation_Code
      ,dictionary_accom.Description as Accomodation_Desc
       From global_temp.Accomodation_Latest
       LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'ACCOMMODATION_TYPE') dictionary_accom
       ON RIGHT(global_temp.Accomodation_Latest.AccommodationType,2) = dictionary_accom.PrimaryCode
       --)

# COMMAND ----------

# DBTITLE 1,CTE for EMP
 %sql
 --WITH EMP AS (
 CREATE OR REPLACE temp view EMP001_prep as
             SELECT 
               global_temp.Employment_Latest.Person_ID as Person_ID
              ,global_temp.Employment_Latest.RANK as RANK
              ,global_temp.Employment_Latest.EmployStatus AS EmploymentType
              ,dss_emp.PrimaryCode AS Employment_Code
              ,dss_emp.Description as Employment_Desc
              FROM global_temp.Employment_Latest
              LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'EMPLOYMENT_STATUS') dss_emp
              ON Employment_Latest.EmployStatus = dss_emp.PrimaryCode
  --            ) 
 --select * from EMP

# COMMAND ----------

 %sql
 --WITH DIS AS (
 CREATE OR REPLACE temp view DIS001_prep as
 SELECT 
      $db_source.MHS007DisabilityType.Disabcode
      ,$db_source.MHS007DisabilityType.Person_ID
      ,$db_source.MHS007DisabilityType.UniqMonthID
      ,$db_source.MHS007DisabilityType.OrgIDProv
      ,dss_disab.PrimaryCode as Disability_Code
      ,dss_disab.Description as Disability_Desc
        FROM $db_source.MHS007DisabilityType
            LEFT JOIN (SELECT * FROM $db_source.datadictionarycodes WHERE ItemName = 'DISABILITY_CODE') dss_disab
                   ON $db_source.MHS007DisabilityType.Disabcode = dss_disab.PrimaryCode
                   --)
 --SELECT * FROM DIS

# COMMAND ----------

 %sql
 SELECT * FROM $db_source.datadictionarycodes WHERE ItemName LIKE '%SEXUAL_ORIENTATION%'

# COMMAND ----------

 %sql
 
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs001mpi_latest_month_data 

# COMMAND ----------

# DBTITLE 1, $db_output.tmp_mhmab_mhs001mpi_latest_month_data
 %sql
 
 
 INSERT INTO $db_output.tmp_mhmab_mhs001mpi_latest_month_data (
 SELECT 
             MPI.AgeDeath 
            ,MPI.AgeRepPeriodEnd
            ,CASE WHEN MPI.AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                      when MPI.AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                      when MPI.AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                      when MPI.AgeRepPeriodEnd = 16 then '16'
                      when MPI.AgeRepPeriodEnd = 17 then '17'
                      when MPI.AgeRepPeriodEnd = 18 then '18'
                      when MPI.AgeRepPeriodEnd = 19 then '19'
                      when MPI.AgeRepPeriodEnd between 20 and 24 then '20 to 24'
                      when MPI.AgeRepPeriodEnd between 25 and 29 then '25 to 29'
                      when MPI.AgeRepPeriodEnd between 30 and 34 then '30 to 34'
                      when MPI.AgeRepPeriodEnd between 35 and 39 then '35 to 39'
                      when MPI.AgeRepPeriodEnd between 40 and 44 then '40 to 44'
                      when MPI.AgeRepPeriodEnd between 45 and 49 then '45 to 49'
                      when MPI.AgeRepPeriodEnd between 50 and 54 then '50 to 54'
                      when MPI.AgeRepPeriodEnd between 55 and 59 then '55 to 59'
                      when MPI.AgeRepPeriodEnd between 60 and 64 then '60 to 64'
                      when MPI.AgeRepPeriodEnd between 65 and 69 then '65 to 69'
                      when MPI.AgeRepPeriodEnd between 70 and 74 then '70 to 74'
                      when MPI.AgeRepPeriodEnd between 75 and 79 then '75 to 79'
                      when MPI.AgeRepPeriodEnd between 80 and 84 then '80 to 84'
                      when MPI.AgeRepPeriodEnd between 85 and 89 then '85 to 89'
                      when MPI.AgeRepPeriodEnd >= '90' then '90 or over' else 'UNKNOWN' end AS Age_Band --updated to UNKNOWN tbc
            ,MPI.AgeRepPeriodStart            
            ,MPI.County 
            ,MPI.DefaultPostcode 
            ,MPI.ElectoralWard 
            ,MPI.EthnicCategory 
            ,MPI.Gender
            ,MPI.GenderIDCode
            ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode
                  WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender
                  ELSE 'UNKNOWN' END AS Der_Gender 
            ,MPI.IMDQuart 
            ,MPI.LADistrictAuth 
            ,MPI.LDAFlag 
            ,"" AS LSOA 
            ,MPI.LSOA2011 
            ,MPI.LanguageCodePreferred 
            ,MPI.LocalPatientId 
            ,MPI.MHS001UniqID 
           -- ,MPI.MPSConfidence --commented out for now
           -- ,MPI.MaritalStatus -- commented out for now, commented out in schemas db 12 both columns
            ,MPI.NHSDEthnicity
       ,CASE WHEN MPI.NHSDEthnicity IS NULL THEN 'UNKNOWN' ELSE MPI.NHSDEthnicity END AS LowerEthnicity        
       ,CASE WHEN MPI.Ethnicity_Desc IS NULL THEN 'UNKNOWN' ELSE MPI.Ethnicity_Desc end as LowerEthnicity_Desc
            ,MPI.NHSNumber 
            ,MPI.NHSNumberStatus 
            ,MPI.OrgIDCCGRes 
            ,MPI.OrgIDEduEstab 
            ,MPI.OrgIDLocalPatientId 
            ,MPI.OrgIDProv 
            ,MPI.OrgIDResidenceResp 
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
            ,CASE WHEN IMD.DECI_IMD = 10 THEN '10 Least deprived'
                 WHEN IMD.DECI_IMD = 9 THEN '09 Less deprived'
                 WHEN IMD.DECI_IMD = 8 THEN '08 Less deprived'
                 WHEN IMD.DECI_IMD = 7 THEN '07 Less deprived'
                 WHEN IMD.DECI_IMD = 6 THEN '06 Less deprived'
                 WHEN IMD.DECI_IMD = 5 THEN '05 More deprived'
                 WHEN IMD.DECI_IMD = 4 THEN '04 More deprived'
                 WHEN IMD.DECI_IMD = 3 THEN '03 More deprived'
                 WHEN IMD.DECI_IMD = 2 THEN '02 More deprived'
                 WHEN IMD.DECI_IMD = 1 THEN '01 Most deprived'
                 ELSE 'UNKNOWN' 
                 END AS IMD_Decile      
      --won't be in DD but likely to be in another db_source table - searc
      -----------------------------------------------------------------------------------------------------------------------------------
       ,CASE WHEN ACC.Accomodation_Code IS NULL AND ACC.AccommodationType IS NOT NULL then 'UNKNOWN' ELSE ACC.AccommodationType END AS AccommodationType 
       ,CASE WHEN ACC.Accomodation_Desc IS NULL AND ACC.AccommodationType IS NOT NULL THEN 'UNKOWN' ELSE ACC.Accomodation_Desc END AS AccommodationType_Desc 
        -----------------------------------------------------------------------------------------------------------------------------------
      ,CASE WHEN EMP.Employment_Code IS NULL AND EMP.EmploymentType IS NOT NULL THEN 'UNKOWN' ELSE EMP.EmploymentType END AS EmployStatus
     ---------------------------------------------------------------------------------------------------------------------------------------
       ,CASE WHEN EMP.Employment_Desc IS NULL AND EMP.EmploymentType IS NOT NULL THEN 'UNKOWN' ELSE EMP.Employment_Desc END AS EmployStatus_Desc
    ---------------------------------------------------------------------------------------------------------------------------------------
       ,CASE WHEN DIS.Disability_Code IS NULL AND DIS.DisabCode IS NOT NULL THEN 'UNKNOWN' ELSE DIS.DisabCode END AS DisabCode           
        ,CASE WHEN DIS.Disability_Desc is NULL AND DIS.DisabCode IS NOT NULL THEN 'UNKOWN' ELSE DIS.Disability_Desc END as DisabCode_Desc
    ---------------------------------------------------------------------------------------------------------------------------------------
            ,CASE WHEN SOC.SocPerCircumstance = '765288000' THEN 'Asexual (not sexually attracted to either gender)'
                  WHEN SOC.SocPerCircumstance = '20430005' THEN 'Heterosexual or Straight'
                  WHEN SOC.SocPerCircumstance = '1064711000000108' THEN 'Person asked and does not know or is not sure'
                  WHEN SOC.SocPerCircumstance IN ('729951000000104','699042003') THEN 'Not Stated (Person asked but declined to provide a response)'
                  WHEN SOC.SocPerCircumstance = '440583007' THEN 'Not known (not recorded)'
                  WHEN SOC.SocPerCircumstance = '89217008' THEN 'Gay or Lesbian'
                  WHEN SOC.SocPerCircumstance = '76102007' THEN 'Gay or Lesbian'
                  WHEN SOC.SocPerCircumstance = '42035005' THEN 'Bisexual'
                  WHEN SOC.SocPerCircumstance = '472985009' THEN 'Asexual (not sexually attracted to either gender)'
                  ELSE 'UNKNOWN' END AS Sex_Orient 
 --First Join-----------------------------------------------------------------------------------------------------------------
 FROM MPI001_prep MPI 
 LEFT JOIN global_temp.CCG
 ON MPI.Person_ID = CCG.Person_ID
 
 --Second Join
 LEFT JOIN $db_source.ENGLISH_INDICES_OF_DEP_V02 IMD 
 on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
 and IMD.imd_year = '2019'
 -----------------------------------------------------------------------------------------------------------------------------     
      
 --THIRD LEFT JOIN MPI AND ACC
 
 LEFT JOIN ACC001_prep ACC
 --on MPI.Person_ID = ACC001_prep.Person_ID and MPI.PatMRecInRP = TRUE and ACC001_prep.RANK = 1
 ON MPI.Person_ID = ACC.Person_ID 
 and MPI.PatMRecInRP = TRUE 
 and ACC.RANK = 1
 -----------------------------------------------------------------------------------------------------------------------------     
 --FOURTH LEFT JOIN MPI AND EMP
 
 LEFT JOIN EMP001_prep EMP
            on MPI.Person_ID = EMP.Person_ID and MPI.PatMRecInRP = TRUE and EMP.RANK = 1
 -----------------------------------------------------------------------------------------------------------------------------
 --- Fifth Join MPI AND DIS
 
 LEFT JOIN DIS001_prep DIS
    on MPI.Person_ID = DIS.Person_ID 
    --and DIS.UniqMonthID = '$end_month_id'
    and DIS.UniqMonthID = '$month_id'
    and MPI.OrgIDProv = DIS.OrgIDProv
 --Sixth Join
 ---MPI AND SOC
 
 LEFT JOIN global_temp.tmp_mhmab_mhs011socpercircircumstance_latest_month_data SOC 
   on MPI.Person_ID = SOC.Person_ID and MPI.PatMRecInRP = TRUE and SOC.RANK = 1
 
 --WHERE MPI.UniqMonthID = '$end_month_id'
 WHERE MPI.UniqMonthID = '$month_id'
 AND MPI.PatMRecInRP = true   
 )

# COMMAND ----------

# DBTITLE 1,bed_types
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW bed_types AS
     SELECT	CASE WHEN WardSecLevel IN ('1', '2', '3') THEN 1 --Specialist MH Servcies
 				 WHEN IntendClinCareIntenCodeMH = '53' THEN 3 --Rehab and older adults organic
 				 WHEN TreatFuncCodeMH IN ('724', '720') THEN 1 --Specialist MH Servcies
 				 WHEN WardType = '06' AND TreatFuncCodeMH = '715' THEN 3 --Rehab and older adults organic
 				 WHEN TreatFuncCodeMH IN ('715', '725', '727') THEN 3 --Rehab and older adults organic
 				 WHEN TreatFuncCodeMH IN ('710', '712', '723') THEN 2 --Adult Acute
 				 WHEN WardType IN ('03', '06') AND IntendClinCareIntenCodeMH IN ('51', '52') THEN 2 --Adult Acute
 				 ELSE 4 --Unknown
 				 END AS Bed_type
 			,UniqWardStayID
       FROM	global_temp.MHS502WardStay_open_end_rp AS A
  LEFT JOIN  $db_source.MHS503AssignedCareProf AS B
 			ON A.UniqServReqID = B.UniqServReqID 
             AND B.UniqMonthID = '$month_id' 
             AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf > '$rp_enddate')
      WHERE  AMHServiceWSEndRP_temp = True		    

# COMMAND ----------

# DBTITLE 1,unique_bed_types
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW unique_bed_types AS
      SELECT UniqWardStayID
 			,MIN (Bed_type) AS Bed_type
       FROM	global_temp.bed_types	
   GROUP BY	UniqWardStayID

# COMMAND ----------

# DBTITLE 1,ward_list_in_rp
 %sql
 
 /** updated for v4.1 when CAMHSTier removed **/
 
        CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_list_in_rp AS
    SELECT DISTINCT CASE --WHEN SRV.CAMHSTier IN ('4','9') THEN 'Y'
    
                         WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN NULL
                         WHEN TreatFuncCodeMH = '700' THEN NULL
                         WHEN WardType = '05' THEN NULL
 
 						WHEN PROF.TreatFuncCodeMH = '711' THEN 'Y'
 						WHEN WRD.WardType IN ('01', '02') THEN 'Y'
 						WHEN WRD.WardAge IN ('10','11','12') THEN 'Y'
                         
                         WHEN PRCNT_U18 > 50 THEN 'Y'
                         
 						ELSE NULL END	AS CAMHS
                         
 				 ,CASE WHEN WRD.IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
 						WHEN PROF.TreatFuncCodeMH = '700' THEN 'Y'
 						WHEN WRD.WardType = '05' THEN 'Y'
 						ELSE NULL END	AS LD
                         
 				 ,CASE WHEN WRD.WardType IN ('01', '02', '05') THEN NULL 
 						WHEN WRD.IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
 						--WHEN SRV.CAMHSTier IN ('4','9') THEN NULL 
 						WHEN PROF.TreatFuncCodeMH IN ('700', '711') THEN NULL
 						WHEN WRD.WardAge IN ('10', '11', '12') THEN NULL
                         
                         WHEN PRCNT_U18 > 50 THEN NULL
                         
 						WHEN WRD.WardAge IN ('13', '14', '15') THEN 'Y'
 						WHEN WRD.IntendClinCareIntenCodeMH IN ('51', '52', '53') THEN 'Y'
 						WHEN PROF.TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
 						WHEN WRD.WardType IN ('03', '04', '06') THEN 'Y'
 						ELSE 'Y' END AS MH
                         
 				,WRD.UniqWardStayID
            FROM $db_output.MHS001MPI_latest_month_data AS MPI
 	 INNER JOIN $db_source.MHS502WardStay AS WRD 
 	 			ON MPI.Person_ID = WRD.Person_ID 
                 AND WRD.UniqMonthID = '$month_id'
                 AND WRD.EndDateWardStay BETWEEN '$rp_startdate' AND '$rp_enddate'
       LEFT JOIN $db_source.MHS503AssignedCareProf	AS PROF
 				ON WRD.UniqHospProvSpellID = PROF.UniqHospProvSpellID 
 				AND PROF.UniqMonthID = '$month_id' 
 				AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf >= wrd.EndDateWardStay)
       LEFT JOIN $db_source.MHS102ServiceTypeReferredTo AS SRV
 				ON WRD.UniqServReqID = SRV.UniqServReqID 
                 AND SRV.UniqMonthID = '$month_id'
                 AND ((ReferClosureDate IS NULL OR ReferClosureDate >= EndDateWardStay) AND (ReferRejectionDate IS NULL OR ReferRejectionDate >= EndDateWardStay))
               
       LEFT JOIN global_temp.TEAMTYPE AS Z 
 			    ON SRV.UniqCareProfTeamID = Z.UniqCareProfTeamID

# COMMAND ----------

# DBTITLE 1,ward_stay_cats_in_rp
 %sql
 
        CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_stay_cats_in_rp AS 
           SELECT UniqWardStayID
 				,MIN (LD) AS LD
 				,MIN (CAMHS) AS CAMHS
 				,MIN (MH) AS MH
            FROM global_temp.ward_list_in_rp
        GROUP BY UniqWardStayID;

# COMMAND ----------

# DBTITLE 1,MHS502WardStay_service_area_discharges
 %sql
 
         CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS502WardStay_service_area_discharges AS
              SELECT WRD.*
                     ,CATS.CAMHS
                     ,CATS.LD
                     ,CATS.MH 
                FROM $db_source.MHS502WardStay AS WRD
          INNER JOIN global_temp.ward_stay_cats_in_rp	AS CATS
                     ON WRD.UniqWardStayID = CATS.UniqWardStayID
               WHERE WRD.UniqMonthID = '$month_id' 

# COMMAND ----------

# DBTITLE 1,bed_types_in_rp
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW bed_types_in_rp AS
     SELECT	CASE WHEN WardSecLevel IN ('1', '2', '3') THEN 1 --Specialist MH Servcies
 				 WHEN IntendClinCareIntenCodeMH = '53' THEN 3 --Rehab and older adults organic
 				 WHEN TreatFuncCodeMH IN ('724', '720') THEN 1 --Specialist MH Servcies
 				 WHEN WardType = '06' AND TreatFuncCodeMH = '715' THEN 3 --Rehab and older adults organic
 				 WHEN TreatFuncCodeMH IN ('715', '725', '727') THEN 3 --Rehab and older adults organic
 				 WHEN TreatFuncCodeMH IN ('710', '712', '723') THEN 2 --Adult Acute
 				 WHEN WardType IN ('03', '06') AND IntendClinCareIntenCodeMH IN ('51', '52') THEN 2 --Adult Acute
 				 ELSE 4 --Unknown
 				 END AS Bed_type
 			,UniqWardStayID
       FROM	global_temp.MHS502WardStay_service_area_discharges AS A
  LEFT JOIN  $db_source.MHS503AssignedCareProf AS B
 			ON A.UniqServReqID = B.UniqServReqID 
             AND B.UniqMonthID = '$month_id' 
             AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf >= A.EndDateWardStay)
      WHERE  EndDateWardStay >= '$rp_startdate'
             AND EndDateWardStay <= '$rp_enddate'
             AND MH = 'Y'

# COMMAND ----------

# DBTITLE 1,unique_bed_types_in_rp
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW unique_bed_types_in_rp AS
      SELECT UniqWardStayID
 			,MIN (Bed_type) AS Bed_type
       FROM	global_temp.bed_types_in_rp
   GROUP BY	UniqWardStayID

# COMMAND ----------

# DBTITLE 1,MHS401MHActPeriod_STO_open_end_rp (to be used for MHA measures)
 %sql
 --21/05/2019 - view renamed from MHS401MHActPeriod_open_end_rp to MHS401MHActPeriod_STO_open_end_rp
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS401MHActPeriod_STO_open_end_rp AS
     SELECT	*
       FROM	$db_source.MHS401MHActPeriod AS STO
      WHERE  STO.UniqMonthID = '$month_id' 
             AND STO.legalstatuscode IN ('04', '05', '06', '19', '20')
      	    AND (STO.EndDateMHActLegalStatusClass IS NULL OR STO.EndDateMHActLegalStatusClass > '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,BED_DAYS_IN_RP
 %sql
 
         TRUNCATE TABLE $db_output.BED_DAYS_IN_RP;
 
         INSERT INTO TABLE $db_output.BED_DAYS_IN_RP
              SELECT 
                     'England' AS LEVEL
                     ,MPI.IC_Rec_CCG	
                     ,MPI.NAME
                     ,MPI.AgeRepPeriodEnd
                     ,WRD.WardType
                     ,WRD.StartDateWardStay
                     ,WRD.EndDateWardStay
                     ,SUM(DATEDIFF(CASE WHEN WRD.EndDateWardStay IS NULL
 											THEN DATE_ADD ('$rp_enddate',1)
 											ELSE WRD.EndDateWardStay
 											END
                                  ,
                                   CASE WHEN WRD.StartDateWardStay < '$rp_startdate'
 											THEN '$rp_startdate'
 											ELSE WRD.StartDateWardStay
 											END							
                                   )
                         ) AS METRIC_VALUE	
               FROM (SELECT DISTINCT StartDateWardStay
                                    ,EndDateWardStay
                                    ,RecordNumber
                                    ,UniqWardStayID
                                    ,Person_ID
                                    ,WardType
                                FROM $db_source.MHS502WardStay
                               WHERE UniqMonthID = '$month_id' 
                     ) AS WRD 
 		 LEFT JOIN $db_output.MHS001MPI_latest_month_data AS MPI
 				   ON WRD.Person_ID = MPI.Person_ID
           GROUP BY MPI.IC_Rec_CCG	
                    ,MPI.NAME
                    ,MPI.AgeRepPeriodEnd
                    ,WRD.WardType
                    ,WRD.StartDateWardStay
                    ,WRD.EndDateWardStay

# COMMAND ----------

spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='BED_DAYS_IN_RP'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='BED_DAYS_IN_RP'))

# COMMAND ----------

# DBTITLE 1,BED_DAYS_IN_RP_PROV
 %sql
 
         CREATE OR REPLACE GLOBAL TEMPORARY VIEW BED_DAYS_IN_RP_PROV AS
              SELECT  MPI.OrgIDProv
                     ,MPI.AgeRepPeriodEnd
                     ,WRD.StartDateWardStay
                     ,WRD.EndDateWardStay
                     ,WRD.WardType
                     ,SUM(DATEDIFF(CASE WHEN WRD.EndDateWardStay IS NULL
 											THEN DATE_ADD ('$rp_enddate',1)
 											ELSE WRD.EndDateWardStay
 											END
                                  ,
                                   CASE WHEN WRD.StartDateWardStay < '$rp_startdate'
 											THEN '$rp_startdate'
 											ELSE WRD.StartDateWardStay
 											END							
                                   )
                         ) AS METRIC_VALUE	
               FROM (SELECT DISTINCT StartDateWardStay
                                    ,EndDateWardStay
                                    ,RecordNumber
                                    ,UniqWardStayID
                                    ,Person_ID
                                    ,OrgIDProv
                                    ,WardType
                                FROM $db_source.MHS502WardStay
                               WHERE UniqMonthID = '$month_id' 
                     ) AS WRD 
 		 LEFT JOIN $db_source.MHS001MPI AS MPI
 				   ON WRD.Person_ID = MPI.Person_ID
                    AND WRD.OrgIDProv = MPI.OrgIDProv
                    AND MPI.UniqMonthID = '$month_id'
           GROUP BY  MPI.OrgIDProv
                    ,MPI.AgeRepPeriodEnd
                    ,WRD.StartDateWardStay
                    ,WRD.EndDateWardStay
                    ,WRD.WardType

# COMMAND ----------

# DBTITLE 1,HOME_LEAVE_IN_RPmhs25
 %sql
 
         CREATE OR REPLACE GLOBAL TEMPORARY VIEW HOME_LEAVE_IN_RP AS
              SELECT 'England' AS LEVEL
                     ,MPI.IC_Rec_CCG			
                     ,MPI.NAME
                     ,SUM(DATEDIFF(CASE	WHEN HLV.EndDateHomeLeave IS NULL
 										THEN DATE_ADD ('$rp_enddate',1)
 										ELSE HLV.EndDateHomeLeave
 										END
 								 ,CASE	WHEN HLV.StartDateHomeLeave < '$rp_startdate'
 										THEN '$rp_startdate'
 										ELSE HLV.StartDateHomeLeave
 										END
 								 )
                         ) AS METRIC_VALUE
               FROM (SELECT DISTINCT StartDateWardStay
                                    ,EndDateWardStay
                                    ,RecordNumber
                                    ,UniqWardStayID
                                    ,Person_ID
                                    ,OrgIDProv
                                    ,WardType
                                FROM $db_source.MHS502WardStay
                               WHERE UniqMonthID = '$month_id' 
                     ) AS WRD 
            INNER JOIN $db_source.MHS509HomeLeave AS HLV
                      ON WRD.UniqWardStayID = HLV.UniqWardStayID 
                      AND HLV.UniqMonthID = '$month_id' 
            LEFT JOIN $db_output.MHS001MPI_latest_month_data AS MPI
                      ON WRD.Person_ID = MPI.Person_ID		 				
             GROUP BY MPI.IC_Rec_CCG			
                      ,MPI.NAME

# COMMAND ----------

# DBTITLE 1,HOME_LEAVE_IN_RP_PROV
 %sql
         CREATE OR REPLACE GLOBAL TEMPORARY VIEW HOME_LEAVE_IN_RP_PROV AS
              SELECT WRD.OrgIDProv
                     ,SUM(DATEDIFF(CASE	WHEN HLV.EndDateHomeLeave IS NULL
 										THEN DATE_ADD ('$rp_enddate',1)
 										ELSE HLV.EndDateHomeLeave
 										END
 								 ,CASE	WHEN HLV.StartDateHomeLeave < '$rp_startdate'
 										THEN '$rp_startdate'
 										ELSE HLV.StartDateHomeLeave
 										END
 								 )
                         ) AS METRIC_VALUE
               FROM (SELECT DISTINCT StartDateWardStay
                                    ,EndDateWardStay
                                    ,RecordNumber
                                    ,UniqWardStayID
                                    ,Person_ID
                                    ,OrgIDProv
                                    ,WardType
                                FROM $db_source.MHS502WardStay
                               WHERE UniqMonthID = '$month_id' 
                     ) AS WRD 
            INNER JOIN $db_source.MHS509HomeLeave AS HLV
                      ON WRD.UniqWardStayID = HLV.UniqWardStayID 
                      AND WRD.OrgIDProv = HLV.OrgIDProv
                      AND HLV.UniqMonthID = '$month_id' 
 		 LEFT JOIN $db_source.MHS001MPI AS MPI
 				   ON WRD.Person_ID = MPI.Person_ID
                    AND WRD.OrgIDProv = MPI.OrgIDProv
                    AND MPI.UniqMonthID = '$month_id'	 				
             GROUP BY WRD.OrgIDProv

# COMMAND ----------

# DBTITLE 1,LOA_IN_RP
 %sql
 
         CREATE OR REPLACE GLOBAL TEMPORARY VIEW LOA_IN_RP AS
              SELECT 'England' AS LEVEL
                     ,MPI.IC_Rec_CCG			
                     ,MPI.NAME
                     ,SUM(DATEDIFF(CASE	WHEN LOA.EndDateMHLeaveAbs IS NULL
 										THEN DATE_ADD ('$rp_enddate',1)
 										ELSE LOA.EndDateMHLeaveAbs
                                         END
 								 ,CASE	WHEN LOA.StartDateMHLeaveAbs < '$rp_startdate'
 										THEN '$rp_startdate'
 										ELSE LOA.StartDateMHLeaveAbs
 										END
 								 )
 					    ) AS METRIC_VALUE
               FROM (SELECT DISTINCT StartDateWardStay
                                    ,EndDateWardStay
                                    ,RecordNumber
                                    ,UniqWardStayID
                                    ,Person_ID
                                    ,OrgIDProv
                                    ,WardType
                                FROM $db_source.MHS502WardStay
                               WHERE UniqMonthID = '$month_id' 
                     ) AS WRD 
           INNER JOIN $db_source.MHS510LeaveOfAbsence AS LOA
 				    ON WRD.UniqWardStayID = LOA.UniqWardStayID 
                     AND LOA.UniqMonthID = '$month_id'  
 	  	  LEFT JOIN $db_output.MHS001MPI_latest_month_data AS MPI
 				    ON WRD.Person_ID = MPI.Person_ID 					
            GROUP BY MPI.IC_Rec_CCG			
                     ,MPI.NAME

# COMMAND ----------

# DBTITLE 1,LOA_IN_RP_PROV
 %sql
         CREATE OR REPLACE GLOBAL TEMPORARY VIEW LOA_IN_RP_PROV AS
              SELECT WRD.OrgIDProv
                     ,SUM(DATEDIFF(CASE	WHEN LOA.EndDateMHLeaveAbs IS NULL
 										THEN DATE_ADD ('$rp_enddate',1)
 										ELSE LOA.EndDateMHLeaveAbs
                                         END
 								 ,CASE	WHEN LOA.StartDateMHLeaveAbs < '$rp_startdate'
 										THEN '$rp_startdate'
 										ELSE LOA.StartDateMHLeaveAbs
 										END
 								 )
 					    ) AS METRIC_VALUE
               FROM (SELECT DISTINCT StartDateWardStay
                                    ,EndDateWardStay
                                    ,RecordNumber
                                    ,UniqWardStayID
                                    ,Person_ID
                                    ,OrgIDProv
                                    ,WardType
                                FROM $db_source.MHS502WardStay
                               WHERE UniqMonthID = '$month_id' 
                     ) AS WRD 
           INNER JOIN $db_source.MHS510LeaveOfAbsence AS LOA
 				    ON WRD.UniqWardStayID = LOA.UniqWardStayID 
                     AND WRD.OrgIDProv = LOA.OrgIDProv 
                     AND LOA.UniqMonthID = '$month_id'  
 	  	  LEFT JOIN $db_output.MHS001MPI_latest_month_data AS MPI
 				    ON WRD.Person_ID = MPI.Person_ID
                     AND WRD.OrgIDProv = MPI.OrgIDProv
            GROUP BY WRD.OrgIDProv

# COMMAND ----------

# DBTITLE 1,ref_list_in_rp - commented out
 %sql
 
 --       CREATE OR REPLACE GLOBAL TEMPORARY VIEW ref_list_in_rp AS
 --            SELECT CASE WHEN WRDRP.LD = 'Y'
 --						THEN 'Y'
 --				     	WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'B02', 'C01')
 --						THEN 'Y'
 --						ELSE NULL
 --					END AS LD
 --                  ,CASE	WHEN WRDRP.CAMHS = 'Y'
 --						THEN 'Y'
 --                        WHEN CAMHSTier IN ('1', '2', '3', '4','9')
 --						THEN 'Y'
 --                        WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07','C09')
 --						THEN 'Y'
 --						ELSE NULL
 --					END	AS CAMHS
 --                  ,CASE	WHEN WRDRP.MH = 'Y'
 --						THEN 'Y'
 --                        WHEN ReasonOAT IN ('10','11','12','13','14','15')
 --						THEN 'Y'
 --                        WHEN CAMHSTier IN ('1', '2', '3', '4','9')
 --						THEN NULL
 --                        WHEN ServTeamTypeRefToMH IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'B01', 'C02', 'C03', 'C04', 'C08', 'D01',                                                          'D02', 'D03', 'D04', 'Z01', 'Z02')
 --						THEN 'Y'
 --                        WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'B02', 'C01', 'C05', 'C06', 'C07','C09')
 --						THEN NULL
 --						ELSE 'Y'
 --					END	AS MH
 --                   ,REF.UniqServReqID
 --
 --                FROM $db_source.MHS101Referral AS REF
 --          INNER JOIN $db_output.MHS001MPI_latest_month_data AS MPI
 --                     ON MPI.Person_ID = REF.Person_ID  
 --          INNER JOIN $db_source.MHS501HospProvSpell AS HOSP 
 --                     ON HOSP.UniqServReqID = REF.UniqServReqID 
 --                     AND HOSP.UniqMonthID = '$month_id' 
 --          INNER JOIN $db_source.MHS502WardStay AS WRD
 --                     ON HOSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID 
 --                     AND WRD.UniqMonthID = '$month_id' 
 --          INNER JOIN global_temp.ward_list_in_rp AS WRDRP 
 --                     ON WRD.UniqWardStayID = WRDRP.UniqWardStayID
 --          INNER JOIN $db_source.MHS102ServiceTypeReferredTo SRV 
 --                     ON REF.UniqServReqID = SRV.UniqServReqID 
 --                     AND SRV.UniqMonthID = '$month_id' 
 --              WHERE (ReferralRequestReceivedDate >= '$rp_startdate' and ReferralRequestReceivedDate  <= '$rp_enddate') 
 --                    AND REF.UniqMonthID = '$month_id' 

# COMMAND ----------

# DBTITLE 1,ref_cats_in_rp - commented out
#%sql

#--CREATE OR REPLACE GLOBAL TEMPORARY VIEW ref_cats_in_rp AS 
#--SELECT DISTINCT	UniqServReqID
 #--       ,MIN (LD) AS LD
 #--       ,MIN (CAMHS) AS CAMHS
 #--       ,MIN (MH) AS MH
# --  FROM global_temp.ref_list_in_rp
#--GROUP BY UniqServReqID;

# COMMAND ----------

# DBTITLE 1,MHS101Referral_service_area_RPstart - commented out
 %sql
 
 --    CREATE GLOBAL TEMPORARY VIEW MHS101Referral_service_area_RPstart AS
 --         SELECT REF.*
 --                ,CATS.CAMHS
 --                ,CATS.LD
 --                ,CATS.MH
 --           FROM $db_source.MHS101Referral AS REF
 --     INNER JOIN ref_cats_in_rp AS CATS on REF.UniqServReqID = CATS.UniqServReqID
 --          WHERE REF.UniqMonthID = '$month_id';

# COMMAND ----------

# DBTITLE 1,ward_type_list_RPstart - for testing - can use derivation
 %sql
 
 /** GBT: updated for v4.1 when CAMHSTier removed **/
 
  CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list_RPstart AS
  SELECT DISTINCT CASE --WHEN CAMHSTier IN ('4','9') THEN 'Y'
  
      WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN NULL
      WHEN TreatFuncCodeMH = '700' THEN NULL
      WHEN WardType = '05' THEN NULL
 
      WHEN TreatFuncCodeMH = '711' THEN 'Y'
      WHEN WardType IN ('01', '02') THEN 'Y'
      WHEN WardAge IN ('10','11','12') THEN 'Y'
    
      WHEN PRCNT_U18 > 50 THEN 'Y'
    
      ELSE NULL END AS CAMHS
 
 ,CASE	
     WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
     WHEN TreatFuncCodeMH = '700' THEN 'Y'
     WHEN WardType = '05' THEN 'Y'
 	ELSE NULL END AS LD
     
  ,CASE	
      WHEN WardType IN ('01', '02', '05') THEN NULL 
      WHEN IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
      --WHEN CAMHSTier IN ('4','9') THEN NULL 
      WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
      WHEN WardAge IN ('10', '11', '12') THEN NULL
      
      WHEN PRCNT_U18 > 50 THEN NULL
      
      WHEN WardAge IN ('13', '14', '15') THEN 'Y'
      WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53') THEN 'Y'
      WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
      WHEN WardType IN ('03', '04', '06') THEN 'Y'
       
      WHEN PRCNT_U18 <= 50 THEN 'Y'
       
 	 ELSE 'Y' END AS MH,
     
  TreatFuncCodeMH, 
   IntendClinCareIntenCodeMH, 
   WardType, 
   CAMHSTier, 
   UniqWardStayID
   from $db_output.MHS001MPI_latest_month_data a
  inner join $db_source.MHS502WardStay b 
    on a.Person_ID = b.Person_ID 
     and b.UniqMonthID = '$month_id'
   left join $db_source.MHS503AssignedCareProf c 
    on b.UniqHospProvSpellID = c.UniqHospProvSpellID 
   and c.UniqMonthID = '$month_id'
   and c.StartDateAssCareProf = b.StartDateWardStay -- ADDED TO ENSURE THE CARE PROFFESSIONAL IS ASSIGNED ON THE SAME DAY AS THE WARD DAY STARTS
  left join $db_source.MHS102ServiceTypeReferredTo d 
   on b.UniqServReqID = d.UniqServReqID 
   and d.UniqMonthID = '$month_id'
   
   LEFT JOIN global_temp.TEAMTYPE AS Z 
 			ON d.UniqCareProfTeamID = Z.UniqCareProfTeamID
 
   left join $db_source.MHS101Referral as r 
   on r.UniqServReqID = b.UniqServReqID 
    and r.UniqMonthID = '$month_id'
    where (r.ReferralRequestReceivedDate >= '$rp_startdate' and r.ReferralRequestReceivedDate <= '$rp_enddate')
   and (b.StartDateWardStay >= '$rp_startdate' and b.StartDateWardStay <= '$rp_enddate')
   and (r.ReferralRequestReceivedDate = b.StartDateWardStay) -- ADDED TO ENSURE THAT THE WARD STAY STARTS ON THE SAME DAY AS THE REFERRAL

# COMMAND ----------

# DBTITLE 1,ward_type_list_RPstart - for testing - can use derivation
 %sql
 
  CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_stay_cats_RPstart AS
   select distinct UniqWardStayID, 
        MIN(LD) as LD,
      MIN(CAMHS) as CAMHS,
    MIN(MH) as MH
   from global_temp.ward_type_list_RPstart
  group by UniqWardStayID

# COMMAND ----------

# DBTITLE 1,referral_list_RPstart - for testing - can use derivation
 %sql
 
 /**  updated for v4.1 when CAMHSTier removed **/
 
  CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list_RPstart AS
  select Case WHEN F.LD = 'Y' THEN 'Y'
 		 WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01') THEN 'Y'
 		 ELSE NULL END AS LD
          
   ,CASE	 WHEN F.CAMHS = 'Y' THEN 'Y'
   
 		 WHEN F.LD = 'Y' THEN NULL
          WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN NULL
 
 --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN 'Y'
 		 WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07') THEN 'Y'
           
          WHEN PRCNT_U18 > 50 THEN 'Y'
          
 		 ELSE NULL END AS CAMHS
          
   ,CASE	 
         WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01', 'C05', 'C06', 'C07') THEN NULL 
         WHEN F.LD = 'Y' THEN NULL 
         WHEN F.CAMHS = 'Y' THEN NULL 
         
         WHEN F.MH = 'Y' THEN 'Y'
 		WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
 		-- WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN NULL
         
         WHEN PRCNT_U18 > 50 THEN NULL
         
         WHEN vc.ValidValue IS NOT NULL THEN 'Y'
          
 		WHEN PRCNT_U18 <= 50 THEN 'Y'
 		
 		ELSE 'Y' END AS MH,
          
          d.UniqServReqID 
          
  from $db_source.MHS101Referral d 
   
  left join $db_output.MHS001MPI_latest_month_data a 
            on a.Person_ID = d.Person_ID 
  left join $db_source.MHS501HospProvSpell g on g.UniqServReqID = d.UniqServReqID 
            and g.UniqMonthID = '$month_id' 
  left join $db_source.MHS502WardStay b on g.UniqHospProvSpellID = b.UniqHospProvSpellID 
            and b.UniqMonthID = '$month_id' 
  left join global_temp.ward_stay_cats_RPstart f on b.UniqWardStayID = f.UniqWardStayID
  left join global_temp.MHS102ServiceTypeReferredTo e on d.UniqServReqID = e.UniqServReqID 
            and e.UniqMonthID = '$month_id'
            
  LEFT JOIN $db_output.validcodes as vc
  ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'referral_list' and vc.type = 'include' and e.ServTeamTypeRefToMH = vc.ValidValue
   and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)          
           
  LEFT JOIN global_temp.TEAMTYPE AS Z 
            ON e.UniqCareProfTeamID = Z.UniqCareProfTeamID
 
    where (ReferralRequestReceivedDate >= '$rp_startdate' and ReferralRequestReceivedDate  <= '$rp_enddate') 
         and d.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,referral_cats_RPstart - for testing - can use derivation
 %sql
 
   CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_cats_RPstart AS
    select UniqServReqID, 
           MIN(LD) as LD,
           MIN(CAMHS) as CAMHS,
           MIN(MH) as MH
      from global_temp.referral_list_RPstart
   group by UniqServReqID

# COMMAND ----------

# DBTITLE 1,MHS502WardStay_service_area_RPstart - for testing - can use derivation
 %sql
 
   CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS502WardStay_service_area_RPstart AS
     select a.*,
           case when b.CAMHS = 'Y' then TRUE else FALSE end as CYPServiceWSStartRP,
           case when b.LD = 'Y' then TRUE else FALSE end as LDAServiceWSStartRP,
          case when b.MH = 'Y' then TRUE else FALSE end as AMHServiceWSStartRP 
      from $db_source.MHS502WardStay a
   left join global_temp.ward_stay_cats_RPstart b on a.UniqWardStayID = b.UniqWardStayID
      where (StartDateWardStay >= '$rp_startdate' and StartDateWardStay <= '$rp_enddate') 
           and a.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,MHS101Referral_service_area_RPstart - for testing - can use derivation
 %sql
 
   CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS101Referral_service_area_RPstart AS
     select a.*,
            --true as CYPServiceRefStartRP,
            --true as LDAServiceRefStartRP, 
            --true as AMHServiceRefStartRP
            case when b.CAMHS  = 'Y' then TRUE else FALSE end as CYPServiceRefStartRP_temp,
            case when b.LD = 'Y' then TRUE else FALSE end as LDAServiceRefStartRP_temp, 
            case when b.MH = 'Y'  then TRUE else FALSE end as AMHServiceRefStartRP_temp
       from $db_source.MHS101Referral a
   left join global_temp.referral_cats_RPstart as b 
             on a.uniqservreqid = b.uniqservreqid
      where (ReferralRequestReceivedDate >= '$rp_startdate' and ReferralRequestReceivedDate  <= '$rp_enddate') 
            and a.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,CASSR_mapping
 %sql
 
 --USING ENTITY_CODE THIS SELECTS ALL UNITARY AUTHORITIES, NON-METROPOLITAN DISTRICTS, METROPOLITAN DISTRICTS, AND LONDON BOROUGHS. NON-METROPOLITAN DISTICTS ARE THEN 
 --REPLACED BY THE RELEVANT HIGHER TIER LOCAL AUTHORITY
 --ONE RANDOM WELSH CODE IS LET THROUGH TO CREATE AN 'UNKNOWN' RECORD
 --UPDATED to remove the additional month used in cases where START and END dates of Orgs are not ACTUAL (they are ACTUAL in these fields in this table)
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CASSR_mapping AS
 SELECT case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_CODE END as LADistrictAuth
       ,case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_NAME END as LADistrictAuthName
 	  ,CASE WHEN a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN"
             WHEN a.ENTITY_CODE in ('E07')  THEN b.GEOGRAPHY_CODE
             ELSE a.GEOGRAPHY_CODE END as CASSR
       ,CASE WHEN a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN"
             WHEN a.ENTITY_CODE in ('E07')  THEN b.GEOGRAPHY_NAME
             ELSE a.GEOGRAPHY_NAME END as CASSR_description
               
 FROM  $db_source.ONS_CHD_GEO_LISTINGS as a
       INNER JOIN $db_source.ONS_CHD_GEO_LISTINGS as b
         ON b.GEOGRAPHY_CODE = a.PARENT_GEOGRAPHY_CODE
         AND a.ENTITY_CODE IN ('E06', 'E07', 'E08', 'E09','W04')
 
 WHERE ((a.DATE_OF_TERMINATION >= '$rp_enddate' OR ISNULL(a.DATE_OF_TERMINATION))
                 AND a.DATE_OF_OPERATION <= '$rp_enddate')
       AND ((b.DATE_OF_TERMINATION >= '$rp_enddate' OR ISNULL(b.DATE_OF_TERMINATION))
                 AND b.DATE_OF_OPERATION <= '$rp_enddate')
       AND (a.GEOGRAPHY_CODE LIKE "E%" OR B.GEOGRAPHY_CODE LIKE "E%" OR a.GEOGRAPHY_CODE = "W04000869")
 
 ORDER BY CASSR_description

# COMMAND ----------

# DBTITLE 1,ResponsibleLA_mapping
 %sql
 --added in at the request of analysts to allow full list of LAs to be pulled through
 --ONE RANDOM WELSH CODE IS LET THROUGH TO CREATE AN 'UNKNOWN' RECORD
 --UPDATED to remove the additional month used in cases where START and END dates of Orgs are not ACTUAL (they are ACTUAL in these fields in this table).
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ResponsibleLA_mapping AS
 SELECT case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_CODE END as LADistrictAuth
       ,case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_NAME END as LADistrictAuthName
 	
 FROM  $db_source.ONS_CHD_GEO_LISTINGS as a
       INNER JOIN $db_source.ONS_CHD_GEO_LISTINGS as b
         ON b.GEOGRAPHY_CODE = a.PARENT_GEOGRAPHY_CODE
         AND a.ENTITY_CODE IN ('E06','E07','E08','E09','W04','E10','E11')
 
 WHERE ((a.DATE_OF_TERMINATION >= '$rp_enddate' OR ISNULL(a.DATE_OF_TERMINATION))
                 AND a.DATE_OF_OPERATION <= '$rp_enddate')
       AND ((b.DATE_OF_TERMINATION >= '$rp_enddate' OR ISNULL(b.DATE_OF_TERMINATION))
                 AND b.DATE_OF_OPERATION <= '$rp_enddate')
       AND (a.GEOGRAPHY_CODE LIKE "E%" OR B.GEOGRAPHY_CODE LIKE "E%" OR a.GEOGRAPHY_CODE = "W04000869")
 
 ORDER BY a.GEOGRAPHY_NAME

# COMMAND ----------

# DBTITLE 1,DelayedDischDim - Interim table until ref table exists - moved from main_monthly_prep as needed earlier
 %sql
 Truncate table $db_output.delayeddischdim;
 INSERT INTO $db_output.delayeddischdim VALUES
  ('att','04','NHS, excluding housing', 1429, null),
  ('att','05','Social Care, excluding housing', 1429, null),
  ('att','06','Both (NHS and Social Care), excluding housing', 1429, null),
  ('att','07','Housing (including supported/specialist housing)', 1429, null),
  
  ('att','UNKNOWN','UNKNOWN', 1429, null),
  ('reason','UNKNOWN','UNKNOWN', 1429, null),
  
  ('reason','A2','Awaiting care coordinator allocation', 1429, null),
  ('reason','B1','Awaiting public funding', 1429, null),
  ('reason','C1','Awaiting further non-acute (including community and mental health) NHS care (including intermediate care, rehabilitation services etc)', 1429, null),
  ('reason','D1','Awaiting Care Home Without Nursing placement or availability', 1429, null),
  ('reason','D2','Awaiting Care Home With Nursing placement or availability', 1429, null),
  ('reason','E1','Awaiting care package in own home', 1429, null),
  ('reason','F2','Awaiting community equipment, telecare and/or adaptations', 1429, null),
  ('reason','G2','Patient or Family choice (reason not stated by patient or family)', 1429, null),
  ('reason','G3','Patient or Family choice - Non-acute (including community and mental health) NHS care (including intermediate care, rehabilitation services etc)', 1429, null),
  ('reason','G4','Patient or Family choice - Care Home Without Nursing placement', 1429, null),
  ('reason','G5','Patient or Family choice - Care Home With Nursing placement', 1429, null),
  ('reason','G6','Patient or Family choice - Care package in own home', 1429, null),
  ('reason','G7','Patient or Family choice - Community equipment, telecare and/or adaptations', 1429, null),
  ('reason','G8','Patient or Family Choice - general needs housing/private landlord acceptance as patient NOT covered by Housing Act/Care Act', 1429, null),
  ('reason','G9','Patient or Family choice - Supported accommodation', 1429, null),
  ('reason','G10','Patient or Family choice - Emergency accommodation from the Local Authority under the Housing Act', 1429, null),
  ('reason','G11','Patient or Family choice - Child or young person awaiting social care or family placement', 1429, null),
  ('reason','G12','Patient or Family choice - Ministry of Justice agreement/permission of proposed placement', 1429, null),
  ('reason','H1','Disputes', 1429, null),
  ('reason','I2','Housing - Awaiting availability of general needs housing/private landlord accommodation acceptance as patient NOT covered by Housing Act and/or Care Act', 1429, null),
  ('reason','I3','Housing - Single homeless patients or asylum seekers NOT covered by Care Act', 1429, null),
  ('reason','J2','Housing - Awaiting supported accommodation', 1429, null),
  ('reason','K2','Housing - Awaiting emergency accommodation from the Local Authority under the Housing Act', 1429, null),
  ('reason','L1','Child or young person awaiting social care or family placement', 1429, null),
  ('reason','M1','Awaiting Ministry of Justice agreement/permission of proposed placement', 1429, null),
  ('reason','N1','Awaiting outcome of legal requirements (mental capacity/mental health legislation)', 1429, null),
  ('reason','P1','Awaiting residential special school or college placement or availability', 1459, null),
  ('reason','Q1','Lack of local education support', 1459, null),
  ('reason','R1','Public safety concern unrelated to clinical treatment need (care team)', 1459, null),
  ('reason','R2','Public safety concern unrelated to clinical treatment need (Ministry of Justice)', 1459, null),
  ('reason','S1','No lawful community care package available', 1459, null),
  ('reason','T1','Lack of health care service provision', 1459, null),
  ('reason','T2','Lack of social care support', 1459, null),
  ('reason','98','No reason given', 1459, null)

# COMMAND ----------

# DBTITLE 1,MHS26 Preparation - National/CCG/Provider/LA-CSSR (used by 0.Insert_lookup_data)
 %sql
 
 /** added to support revised (more detailed) DTOC measure **/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS26_ranking AS
 SELECT      mhs504.orgidprov
   
   --  attempting to sort out missing UNKNOWNs
   
             ,coalesce(att.code, 'UNKNOWN') AS AttribToIndic
             ,coalesce(reas.code, 'UNKNOWN') AS DelayDischReason
 
 
 --             ,AttribToIndic
 --             ,DelayDischReason
             ,mhs504.UniqHospProvSpellID
             ,StartDateDelayDisch
             ,EndDateDelayDisch
             ,MHS504UniqID
             ,IC_Rec_CCG
             ,case
             
             --top line of where clause changed following discussions with analysts
                   when geo.geography_code is null and mhs504.OrgIDRespLADelayDisch is not null then 'UNKNOWN'
                   when geo.geography_code is null and mpi.ladistrictauth = '' then 'UNKNOWN'
                   else coalesce(geo.geography_code, mpigeo.geography_code, 'UNKNOWN') end as ResponsibleLA
          
             ,case 
             
              --top line of where clause changed following discussions with analysts
                  when geo.geography_name is null and mhs504.OrgIDRespLADelayDisch is not null then 'UNKNOWN'
                  when geo.geography_name is null and mpi.ladistrictauth = '' then 'UNKNOWN'
                  else coalesce(geo.geography_name, mpigeo.geography_name, 'UNKNOWN') end as ResponsibleLA_Name
             ,ROW_NUMBER() OVER (PARTITION BY mhs504.UniqHospProvSpellID ORDER BY StartDateDelayDisch ASC, MHS504UniqID ASC) as rnk
             ,LEAD(Enddatedelaydisch,-1) OVER (PARTITION BY MHS504.UniqHospProvSpellID ORDER BY startdatedelaydisch ASC, MHS504uniqid ASC) as lastenddate
             
 from        $db_source.mhs504delayeddischarge as mhs504
   -- attempting to sort out missing UNKNOWNs
 LEFT JOIN $db_output.DelayedDischDim att ON mhs504.AttribToIndic = att.code and att.key = 'att'
 
 LEFT JOIN $db_output.DelayedDischDim reas ON mhs504.DelayDischReason = reas.code and reas.key = 'reason'
 -- up to here
 
 LEFT JOIN   global_temp.ccg as ccg on mhs504.person_id = ccg.person_id
 LEFT JOIN   $db_source.mhs001mpi mpi on mhs504.person_id = mpi.person_id
             and mhs504.uniqmonthid = mpi.uniqmonthid
             and MPI.PatMRecInRP = true
 LEFT JOIN   $db_source.ONS_CHD_GEO_EQUIVALENTS geo on mhs504.OrgIDRespLADelayDisch = geo.dh_geography_code and (geo.DATE_OF_TERMINATION IS NULL or geo.DATE_OF_TERMINATION > '$rp_enddate') and geo.ENTITY_CODE IN ('E06','E07','E08','E09','E10','E11')
 LEFT JOIN   $db_source.ONS_CHD_GEO_EQUIVALENTS mpigeo on mpi.ladistrictauth = mpigeo.geography_code and (mpigeo.DATE_OF_TERMINATION IS NULL or mpigeo.DATE_OF_TERMINATION > '$rp_enddate') and mpigeo.ENTITY_CODE IN ('E06','E07','E08','E09','E10','E11')
 WHERE       mhs504.uniqmonthid = '$month_id'
 AND         MHS504UniqID not in (select distinct MHS504UniqID from $db_source.mhs504delayeddischarge where uniqmonthid = '$month_id' and StartDateDelayDisch = EndDateDelayDisch)
 GROUP BY    mhs504.orgidprov
             ,mhs504.UniqHospProvSpellID
             
               -- attempting to sort out missing UNKNOWNs
               
             ,coalesce(att.code, 'UNKNOWN')
             ,coalesce(reas.code, 'UNKNOWN')
             
 --             ,AttribToIndic
 --             ,DelayDischReason
             ,StartDateDelayDisch
             ,EndDateDelayDisch
             ,MHS504UniqID
             ,IC_Rec_CCG
             ,case 
             
               --top line of where clause changed following discussions with analysts
                  when geo.geography_code is null and mhs504.OrgIDRespLADelayDisch is not null then 'UNKNOWN'
                  when geo.geography_code is null and mpi.ladistrictauth = '' then 'UNKNOWN'
                  else coalesce(geo.geography_code, mpigeo.geography_code, 'UNKNOWN') end
             ,case 
             
             --top line of where clause changed following discussions with analysts
                  when geo.geography_name is null and mhs504.OrgIDRespLADelayDisch is not null then 'UNKNOWN'
                  when geo.geography_name is null and mpi.ladistrictauth = '' then 'UNKNOWN'
                  else coalesce(geo.geography_name, mpigeo.geography_name, 'UNKNOWN') end 

# COMMAND ----------

# DBTITLE 1,Org_Daily temporary view 
 %sql
 --This view has been copied to menh_publications\notebooks\common_objects\02_load_common_ref_data
 
 /** added as part of the change in STP mapping/derivation for v4.1 **/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW org_daily AS
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $db_source.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
               --  AND ORG_TYPE_CODE = 'ST'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate';

# COMMAND ----------

# DBTITLE 1, ORG_RELATIONSHIP_DAILY temporary view
 %sql
 -- This view has been copied to menh_publications\notebooks\common_objects\02_load_common_ref_data
 /** added as part of the change in STP mapping/derivation for v4.1 **/
 
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW org_relationship_daily AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $db_source.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate';

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_rd_ccg_latest;
 
 INSERT INTO $db_output.tmp_mhmab_rd_ccg_latest
 SELECT DISTINCT ORG_CODE,
                 NAME
            FROM $db_source.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%';
               
 OPTIMIZE $db_output.tmp_mhmab_rd_ccg_latest;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_ccg;
 
 INSERT INTO $db_output.tmp_mhmab_ccg
     SELECT a.Person_ID,
            CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS IC_REC_GP_RES,
            CASE WHEN b.NAME IS null THEN 'UNKNOWN' ELSE b.NAME END AS NAME
       FROM global_temp.CCG_PREP a
  LEFT JOIN $db_output.tmp_MHMAB_RD_CCG_LATEST b 
            ON IC_Rec_CCG = b.ORG_CODE;
            
 OPTIMIZE $db_output.tmp_mhmab_ccg;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs30f_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs30f_prep
 SELECT
   CC.UniqCareContID, CCG.NAME, CCG.IC_REC_GP_RES, CC.OrgIDProv, CC.AttendOrDNACode, CC.Person_ID, CC.UniqServReqID, CC.ConsMechanismMH, CC.CareContCancelDate, CC.CareContDate,
       CASE 
         WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
         WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
         WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
         WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
       ELSE 'N/A' END AS DNA_Reason,
      COALESCE(CMU_DIM.Code,'INVALID') ConsMedUsed,
      COALESCE(CMU_DIM.Description,'INVALID') as CMU,
     CASE 
       WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
       WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
       WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
     ELSE 'UNKNOWN' END as AgeGroup,
     CASE 
       WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
       WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
       WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
       ELSE 'UNKNOWN' END as AgeGroupName         
 FROM $db_source.MHS201CareContact AS CC
 INNER JOIN $db_source.MHS101Referral AS REF ON CC.Person_ID = REF.Person_ID AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
 INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI ON MPI.Person_ID = CC.Person_ID 
 INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID AND CC.UniqCareProfTeamID = SERV.UniqCareProfTeamID AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
 INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG ON CC.Person_ID = CCG.Person_ID
 INNER JOIN $db_output.ConsMechanismMH_dim as CMU_DIM ON CC.ConsMechanismMH = CMU_DIM.Code and CC.UniqMonthID >= CMU_DIM.FirstMonth and CC.UniqMonthID <= coalesce(LastMonth,9999)
 INNER JOIN $db_output.validcodes as vc on SERV.ServTeamTypeRefToMH = vc.ValidValue and vc.Measure = 'CAMHS' and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 WHERE CC.UniqMonthID = '$month_id' AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');
 
 OPTIMIZE $db_output.tmp_mhmab_mhs30f_prep;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs30f_prep_prov;
 INSERT INTO $db_output.tmp_mhmab_mhs30f_prep_prov
 SELECT
   CC.UniqCareContID, CCG.NAME, CCG.IC_REC_GP_RES, CC.OrgIDProv, CC.AttendOrDNACode, CC.Person_ID, CC.UniqServReqID, CC.ConsMechanismMH, CC.CareContCancelDate, CC.CareContDate,
   CASE 
     WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
     WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
     WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
     WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
    ELSE 'N/A' END AS DNA_Reason,
   COALESCE(CMU_DIM.Code,'INVALID') ConsMedUsed,
   COALESCE(CMU_DIM.Description,'INVALID') as CMU,
  CASE 
    WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
    WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
    WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
  ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName  
  FROM  $db_source.MHS201CareContact AS CC
  INNER JOIN $db_source.MHS101Referral AS REF ON CC.Person_ID = REF.Person_ID AND REF.UNIQSERVREQID = CC.UNIQSERVREQID 
  INNER JOIN $db_source.MHS001MPI AS MPI ON MPI.Person_ID = CC.Person_ID AND CC.OrgIDPRov = MPI.OrgIDProv AND MPI.UniqMonthID = CC.UniqMonthID
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO AS SERV ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID AND CC.UniqCareProfTeamID = SERV.UniqCareProfTeamID AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
  INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG ON CC.Person_ID = CCG.Person_ID
  INNER JOIN $db_output.ConsMechanismMH_dim as CMU_DIM ON CC.ConsMechanismMH = CMU_DIM.Code and CC.UniqMonthID >= CMU_DIM.FirstMonth and CC.UniqMonthID <= coalesce(LastMonth,9999)
  INNER JOIN $db_output.validcodes as vc on SERV.ServTeamTypeRefToMH = vc.ValidValue and vc.Measure = 'CAMHS' and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
  WHERE CC.UniqMonthID = '$month_id' AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');
  
  OPTIMIZE $db_output.tmp_mhmab_mhs30f_prep_prov;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs30h_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs30h_prep
 SELECT
 CC.UniqCareContID, CCG.NAME, CCG.IC_REC_GP_RES, CC.OrgIDProv, CC.AttendOrDNACode, CC.Person_ID, CC.UniqServReqID, CC.ConsMechanismMH, CC.CareContCancelDate, CC.CareContDate,
 CASE 
   WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
   WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
   WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
   WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
 ELSE 'N/A' END AS DNA_Reason,
 COALESCE(CMU_DIM.Code,'INVALID') AS ConsMedUsed, COALESCE(CMU_DIM.Description,'INVALID') as CMU,
 CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
 ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName                     
 FROM $db_source.MHS201CareContact AS CC INNER JOIN $db_source.MHS101Referral AS REF 
 ON CC.Person_ID = REF.Person_ID AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
 INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI ON MPI.Person_ID = CC.Person_ID 
 INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO AS SERV ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
 AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
 INNER JOIN $db_output.tmp_MHMAB_CCG AS CCG ON CC.Person_ID = CCG.Person_ID
 INNER JOIN $db_output.ConsMechanismMH_dim as CMU_DIM ON CC.ConsMechanismMH = CMU_DIM.Code 
 and CC.UniqMonthID >= CMU_DIM.FirstMonth and CC.UniqMonthID <= coalesce(LastMonth,9999)
 WHERE CC.UniqMonthID = '$month_id' AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');
 
 OPTIMIZE $db_output.tmp_mhmab_mhs30h_prep;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_MHMAB_MHS30h_prep_prov;
 INSERT INTO $db_output.tmp_MHMAB_MHS30h_prep_prov
 SELECT
 CC.UniqCareContID, CCG.NAME, CCG.IC_REC_GP_RES, CC.OrgIDProv, CC.AttendOrDNACode, CC.Person_ID, CC.UniqServReqID, 
 CC.ConsMechanismMH,CC.CareContCancelDate, CC.CareContDate,
 CASE 
   WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
   WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
   WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
   WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
 ELSE 'N/A' END AS DNA_Reason,
 COALESCE(CMU_DIM.Code,'INVALID') AS ConsMedUsed, COALESCE(CMU_DIM.Description,'INVALID') as CMU,
 CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
 ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName                     
 FROM $db_source.MHS201CareContact AS CC INNER JOIN $db_source.MHS101Referral AS REF
 ON CC.Person_ID = REF.Person_ID AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
 INNER JOIN $db_source.MHS001MPI AS MPI ON MPI.Person_ID = CC.Person_ID
 AND CC.OrgIDPRov = MPI.OrgIDProv AND MPI.UniqMonthID = CC.UniqMonthID
 INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO AS SERV
 ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
 INNER JOIN $db_output.tmp_mhmab_ccg AS CCG ON CC.Person_ID = CCG.Person_ID
 INNER JOIN $db_output.ConsMechanismMH_dim as CMU_DIM ON CC.ConsMechanismMH = CMU_DIM.Code 
 and CC.UniqMonthID >= CMU_DIM.FirstMonth and CC.UniqMonthID <= coalesce(LastMonth,9999)
 WHERE CC.UniqMonthID = '$month_id' AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');
 
  OPTIMIZE $db_output.tmp_mhmab_mhs30h_prep_prov;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs32c_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs32c_prep
   SELECT   REF.UniqServReqID 
            ,REF.SourceOfReferralMH
            ,MPI.IC_REC_CCG
            ,MPI.NAME
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
                  WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
                  WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
                  WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
                  WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
                  WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
                  WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
                  WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
                  WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
                  WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
                  WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
                  WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
                  WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
                  WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                  ELSE 'Invalid' 
            END AS Referral_Source 
            ,CASE WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
                 WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
                 WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
                 WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
                 WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
                 WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
                 WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
                 WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
                 WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
                 WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
                 WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
                 WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
                 WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
                 WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
                 ELSE 'Invalid' 
             END AS Referral_Description
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
        FROM $db_output.tmp_mhmab_mhs001mpi_latest_month_data AS MPI
  INNER JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$month_id'
 INNER JOIN $db_output.validcodes as vc on SERV.ServTeamTypeRefToMH = vc.ValidValue and vc.Measure = 'CAMHS' and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
       WHERE REF.UniqMonthID = '$month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate';
 --        AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs32_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs32_prep
   SELECT    REF.UniqServReqID 
            ,REF.Person_ID
            ,COALESCE(MPI.Age_Band, "UNKNOWN") as Age_Band
            ,COALESCE(MPI.Der_Gender, "UNKNOWN") as Der_Gender
            ,COALESCE(MPI.LowerEthnicity, "UNKNOWN") as LowerEthnicity
            ,COALESCE(MPI.LowerEthnicity_Desc, "UNKNOWN") as LowerEthnicity_Desc
            ,COALESCE(MPI.IMD_Decile, "UNKNOWN") as IMD_Decile
            ,COALESCE(MPI.AccommodationType, "UNKNOWN") as AccommodationType
            ,COALESCE(MPI.AccommodationType_Desc, "UNKNOWN") as AccommodationType_Desc
            ,COALESCE(MPI.EmployStatus, "UNKNOWN") as EmployStatus
            ,COALESCE(MPI.EmployStatus_Desc, "UNKNOWN") as EmployStatus_Desc
            ,COALESCE(MPI.DisabCode, "UNKNOWN") as DisabCode
            ,COALESCE(MPI.DisabCode_Desc, "UNKNOWN") as DisabCode_Desc
            ,COALESCE(MPI.Sex_Orient, "UNKNOWN") as Sex_Orient
 FROM $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 LEFT JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND MPI.PatMRecInRP = TRUE
 WHERE REF.UniqMonthID = '$month_id' 
 AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate';

# COMMAND ----------

 %sql
 INSERT INTO $db_output.tmp_mhmab_mhs32c_prep_prov
 SELECT 
 REF.UniqServReqID,REF.SourceOfReferralMH,REF.OrgIDProv,
 CASE 
   WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
   WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
   WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
   WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
   WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
   WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
   WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
   WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
   WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
   WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
   WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
   WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
   WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
   WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing'
 ELSE 'Invalid' END AS Referral_Source,
 CASE 
   WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
   WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
   WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
   WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
   WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
   WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
   WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
   WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
   WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
   WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
   WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
   WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
   WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
   WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
  ELSE 'Invalid' END AS Referral_Description, 
 CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
 ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
 FROM $db_source.MHS101Referral AS REF INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
 ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
 INNER JOIN $db_source.MHS001MPI AS MPI ON MPI.Person_ID = REF.Person_ID  AND MPI.OrgIDProv = REF.OrgIDProv AND MPI.UniqMonthID = REF.UniqMonthID
 INNER JOIN $db_output.validcodes as vc on Serv.ServTeamTypeRefToMH = vc.ValidValue and vc.Measure = 'CAMHS' and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 WHERE REF.UniqMonthID = '$month_id' AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate';
 --AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs32d_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs32d_prep
 SELECT   
 REF.UniqServReqID,REF.SourceOfReferralMH,MPI.IC_REC_CCG,MPI.NAME,
 CASE 
    WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
    WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
    WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
    WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
    WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
    WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
    WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
    WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
    WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
    WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
    WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
    WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
    WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
    WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
 ELSE 'Invalid' END AS Referral_Source,
 CASE 
   WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
   WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
   WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
   WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
   WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
   WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
   WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
   WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
   WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust'
   WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
   WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
   WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
   WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
   WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
 ELSE 'Invalid' END AS Referral_Description,
 CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
 ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName 
 FROM $db_output.tmp_mhmab_mhs001mpi_latest_month_data AS MPI INNER JOIN $db_source.MHS101Referral AS REF ON MPI.Person_ID = REF.Person_ID 
 INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO AS SERV ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
 WHERE REF.UniqMonthID = '$month_id' AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate' AND SERV.ServTeamTypeRefToMH in ('C02');

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs32d_prep_prov;
 INSERT INTO $db_output.tmp_mhmab_mhs32d_prep_prov
 SELECT
 REF.UniqServReqID,REF.SourceOfReferralMH,REF.OrgIDProv,
 CASE 
    WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'A' 
    WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'B' 
    WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'C' 
    WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'D' 
    WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'E' 
    WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'F' 
    WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'G' 
    WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'H' 
    WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'I' 
    WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'M' 
    WHEN REF.SourceOfReferralMH = 'N3' THEN 'N' 
    WHEN REF.SourceOfReferralMH = 'P1' THEN 'P' 
    WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Q' 
    WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
 ELSE 'Invalid' END AS Referral_Source, 
 CASE 
     WHEN REF.SourceOfReferralMH IN ('A1','A2','A3','A4') THEN 'Primary Health Care' 
     WHEN REF.SourceOfReferralMH IN ('B1','B2') THEN 'Self Referral' 
     WHEN REF.SourceOfReferralMH IN ('C1','C2','C3') THEN 'Local Authority Services' 
     WHEN REF.SourceOfReferralMH IN ('D1','D2') THEN 'Employer' 
     WHEN REF.SourceOfReferralMH IN ('E1','E2','E3','E4','E5','E6') THEN 'Justice System' 
     WHEN REF.SourceOfReferralMH IN ('F1','F2','F3') THEN 'Child Health' 
     WHEN REF.SourceOfReferralMH IN ('G1','G2','G3','G4') THEN 'Independent/Voluntary Sector' 
     WHEN REF.SourceOfReferralMH IN ('H1','H2') THEN 'Acute Secondary Care' 
     WHEN REF.SourceOfReferralMH IN ('I1','I2') THEN 'Other Mental Health NHS Trust' 
     WHEN REF.SourceOfReferralMH IN ('M1','M2','M3','M4','M5','M6','M7','M9') THEN 'Other' 
     WHEN REF.SourceOfReferralMH = 'N3' THEN 'Improving access to psychological  therapies' 
     WHEN REF.SourceOfReferralMH = 'P1' THEN 'Internal' 
     WHEN REF.SourceOfReferralMH = 'Q1' THEN 'Drop In Service' 
     WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing' 
 ELSE 'Invalid' END AS Referral_Description,
 CASE 
     WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
     WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
     WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
     ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
 FROM $db_source.MHS101Referral AS REF INNER JOIN $db_source.MHS102SERVICETYPEREFERREDTO AS SERV
 ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID AND REF.PERSON_ID = SERV.PERSON_ID AND SERV.UNIQMONTHID = '$month_id'
 INNER JOIN $db_source.MHS001MPI AS MPI ON MPI.Person_ID = REF.Person_ID AND MPI.OrgIDProv = REF.OrgIDProv AND MPI.UniqMonthID = REF.UniqMonthID
 WHERE REF.UniqMonthID = '$month_id' AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate' AND SERV.ServTeamTypeRefToMH in ('C02');

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs57b_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs57b_prep
     SELECT   MPI.Person_ID
             ,MPI.IC_REC_CCG
             ,MPI.NAME
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
 FROM    $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = '$month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID
             AND Serv.UniqMonthID = '$month_id' 
 INNER JOIN $db_output.validcodes as vc on Serv.ServTeamTypeRefToMH = vc.ValidValue and vc.Measure = 'CAMHS' and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 WHERE       REF.ServDischDate between '$rp_startdate' AND '$rp_enddate';
 --  AND       Serv.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs57b_prep_prov;
 INSERT INTO $db_output.tmp_mhmab_mhs57b_prep_prov
     SELECT   MPI.Person_ID
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
     FROM    $db_source.MHS101Referral AS REF
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = MPI.UniqMonthID
             AND MPI.OrgIDProv = REF.OrgIDProv
             AND REF.UniqMonthID = '$month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID
             AND Serv.UniqMonthID = '$month_id'
 INNER JOIN $db_output.validcodes as vc on Serv.ServTeamTypeRefToMH = vc.ValidValue and vc.Measure = 'CAMHS' and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 WHERE       REF.ServDischDate between '$rp_startdate' AND '$rp_enddate';
 --  AND       Serv.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs57c_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs57c_prep
     SELECT   MPI.Person_ID
             ,MPI.IC_REC_CCG
             ,MPI.NAME
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
     FROM    $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = '$month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID
             AND Serv.UniqMonthID = '$month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate' AND '$rp_enddate'
   AND       Serv.ServTeamTypeRefToMH in ('C02');

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs57c_prep_prov; 
 INSERT INTO $db_output.tmp_mhmab_mhs57c_prep_prov
     SELECT   MPI.Person_ID
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 18 THEN '0-18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup,
  CASE 
   WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
   WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
   ELSE 'UNKNOWN' END as AgeGroupName
     FROM    $db_source.MHS101Referral AS REF
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = MPI.UniqMonthID
             AND MPI.OrgIDProv = REF.OrgIDProv
             AND REF.UniqMonthID = '$month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID
             AND Serv.UniqMonthID = '$month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate' AND '$rp_enddate'
   AND       Serv.ServTeamTypeRefToMH in ('C02');

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.tmp_mhmab_mhs01_prep;
 INSERT INTO $db_output.tmp_mhmab_mhs01_prep
 SELECT      REF.UniqServReqID 
            ,REF.Person_ID
            ,COALESCE(MPI.Age_Band, "UNKNOWN") as Age_Band
            ,COALESCE(MPI.Der_Gender, "UNKNOWN") as Der_Gender
            ,COALESCE(MPI.LowerEthnicity, "UNKNOWN") as LowerEthnicity
            ,COALESCE(MPI.LowerEthnicity_Desc, "UNKNOWN") as LowerEthnicity_Desc
            ,COALESCE(MPI.IMD_Decile, "UNKNOWN") as IMD_Decile
            ,COALESCE(MPI.AccommodationType, "UNKNOWN") as AccommodationType
            ,COALESCE(MPI.AccommodationType_Desc, "UNKNOWN") as AccommodationType_Desc
            ,COALESCE(MPI.EmployStatus, "UNKNOWN") as EmployStatus
            ,COALESCE(MPI.EmployStatus_Desc, "UNKNOWN") as EmployStatus_Desc
            ,COALESCE(MPI.DisabCode, "UNKNOWN") as DisabCode
            ,COALESCE(MPI.DisabCode_Desc, "UNKNOWN") as DisabCode_Desc
            ,COALESCE(MPI.Sex_Orient, "UNKNOWN") as Sex_Orient
 FROM $db_output.mhs101referral_open_end_rp AS REF
 LEFT JOIN $db_output.tmp_MHMAB_mhs001mpi_latest_month_data AS MPI
             ON REF.Person_ID = MPI.Person_ID 
             AND MPI.PatMRecInRP = TRUE;

# COMMAND ----------

