# Databricks notebook source
 %md
 
 # Generic Prep assets used throughout Mental health
 -- check that all cells are listed here appropriately IN ORDER and are all needed
 
 - RD_CCG_LATEST
 - CCG_PRAC
 - CCG_PREP
 - CCG
 - RD_ORG_DAILY_LATEST
 - Provider_list
 - Provider_list_AWT
 - ward_type_list
 - ward_stay_cats
 - referral_list
 - referral_cats
 - MHS502WardStay_service_area
 - MHS101Referral_service_area
 - MHS001MPI_latest_month_data  -- Materialised
 - MHS101Referral_open_end_rp   -- Materialised
 - MHS102ServiceTypeReferredTo
 - MHS502WardStay_open_end_rp
 - MHS501HospProvSpell_open_end_rp
 - MHS701CPACareEpisode_latest
 - Accomodation_latest
 - Employment_Latest
 - bed_types
 - unique_bed_types
 - ward_list_in_rp
 - ward_stay_cats_in_rp
 - MHS502WardStay_service_area_discharges
 - bed_types_in_rp
 - unique_bed_types_in_rp
 - MHS401MHActPeriod_open_end_rp
 - BED_DAYS_IN_RP   -- Materialised
 - BED_DAYS_IN_RP_PROV
 - HOME_LEAVE_IN_RP
 - LOA_IN_RP
 - LOA_IN_RP_PROV
 - ref_list_in_rp -- commented out
 - ref_cats_in_rp -- commented out
 - ward_type_list_Rpstart - for testing - can use derivation
 - ward_stay_cats_Rpstart - for testing - can use derivation
 - referral_list_Rpstart - for testing - can use derivation
 - referral_cats_Rpstart - for testing - can use derivation
 - MHS502WardStay_service_area_Rpstart - for testing - can use derivation
 - MHS101Referral_service_area_Rpstart - for testing - can use derivation
 - CASSR_mapping

# COMMAND ----------

# DBTITLE 1,RD_CCG_LATEST
 %sql
 TRUNCATE TABLE $db_output.RD_CCG_LATEST;
 
 INSERT INTO TABLE $db_output.RD_CCG_LATEST
 SELECT DISTINCT ORG_CODE,
                 NAME
            FROM reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='RD_CCG_LATEST'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='RD_CCG_LATEST'))

# COMMAND ----------

# DBTITLE 1,CCG_PRAC - have changed to person_id - SH 2019-04-29
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PRAC AS
 SELECT GP.Person_ID,
       GP.OrgIDCCGGPPractice,
       GP.RecordNumber
  FROM $db_source.MHS002GP GP
       INNER JOIN 
                  (
                    SELECT Person_ID, 
                           MAX(RecordNumber) as RecordNumber
                      FROM $db_source.MHS002GP
                     WHERE UniqMonthID = '$month_id'
                           AND GMPCodeReg NOT IN ('V81999','V81998','V81997')
                           --AND OrgIDGPPrac <> '-1' --CCG methodology change from TP - change made by DC
                           AND EndDateGMPRegistration is NULL
                  GROUP BY Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GP.UniqMonthID = '$month_id'
        --AND MPI.PatMRecInRP = TRUE 
        AND GMPCodeReg NOT IN ('V81999','V81998','V81997')
        --AND OrgIDGPPrac <> '-1' 
        AND EndDateGMPRegistration is null

# COMMAND ----------

# DBTITLE 1,CCG_PREP
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PREP AS
      SELECT a.Person_ID,
             CASE 
                  WHEN c.OrgIDCCGGPPractice IS not null then c.OrgIDCCGGPPractice
                  ELSE a.OrgIDCCGRes end as IC_Rec_CCG
        FROM $db_source.MHS001MPI a
   LEFT JOIN global_temp.CCG_PRAC c 
             ON a.Person_ID = c.Person_ID 
             AND a.RecordNumber = c.RecordNumber
       WHERE a.UniqMonthID = '$month_id' 
             AND a.PatMRecInRP = true

# COMMAND ----------

# DBTITLE 1,CCG
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG AS
     SELECT Person_ID,
            CASE WHEN b.ORG_CODE IS null THEN 'UNKNOWN' ELSE b.ORG_CODE END AS IC_Rec_CCG,
            CASE WHEN NAME IS null THEN 'UNKNOWN' ELSE NAME END AS NAME
       FROM global_temp.CCG_PREP a
  LEFT JOIN $db_output.RD_CCG_LATEST b 
            ON a.IC_Rec_CCG = b.ORG_CODE;

# COMMAND ----------

# DBTITLE 1,RD_ORG_DAILY_LATEST
 %sql
 
 /* Org daily latest */
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_ORG_DAILY_LATEST AS
 SELECT DISTINCT ORG_CODE, 
                 NAME
            FROM reference_data.org_daily
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
                           --ReportingPeriodStartDate >= '$rp_startdate'
                           --AND ReportingPeriodEndDate <= '$rp_enddate';

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
                           --ReportingPeriodStartDate >= '$rp_startdate'
                           --AND ReportingPeriodEndDate <= '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,Service area end RP - For testing only - derivation to be created
 %sql
 
 /**CODE TO SPLIT THE DIFFERENT SERVICE AREAS FOR WARD STAYS**/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list AS
     SELECT  DISTINCT CASE WHEN CAMHSTier IN ('4','9') THEN 'Y'
 					      WHEN TreatFuncCodeMH = '711' THEN 'Y'
 						  WHEN WardType IN ('01', '02') THEN 'Y'
 						  WHEN WardAge IN ('10','11','12') THEN 'Y'
 						  ELSE NULL END AS CAMHS
 			,CASE WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
 				  WHEN TreatFuncCodeMH = '700' THEN 'Y'
 				  WHEN WardType = '05' THEN 'Y'
 				  ELSE NULL END AS LD
 			,CASE WHEN WardType IN ('01', '02', '05') THEN NULL 
 				  WHEN IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
 				  WHEN CAMHSTier IN ('4','9') THEN NULL 
 				  WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
 				  WHEN WardAge IN ('10', '11', '12') THEN NULL
 				  WHEN WardAge IN ('13', '14', '15') THEN 'Y'
 				  WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53') THEN 'Y'
 				  WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
 				  WHEN WardType IN ('03', '04', '06') THEN 'Y'
 				  ELSE 'Y' END AS MH
 			,UniqWardStayID
       FROM	$db_source.MHS001MPI AS A
 INNER JOIN  $db_source.MHS502WardStay AS B
 			ON A.Person_ID = B.Person_ID 
             AND B.UniqMonthID = '$month_id'  
  LEFT JOIN  $db_source.MHS503AssignedCareProf AS C
 			ON B.UniqHospProvSpellNum = C.UniqHospProvSpellNum 
             AND C.UniqMonthID = '$month_id' 
             AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf > '$rp_enddate')
  LEFT JOIN  $db_source.MHS102ServiceTypeReferredTo AS D
 			ON B.UniqServReqID = D.UniqServReqID 
             AND D.UniqMonthID = '$month_id' 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate'))
      WHERE	A.UniqMonthID = '$month_id'
 			AND A.PatMRecInRP = true
 			AND (EndDateWardStay IS NULL OR EndDateWardStay > '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,Service area end RP - For testing only - derivation to be created
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

# DBTITLE 1,Service area end RP - For testing only - derivation to be created
 %sql
 
 /**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list AS
     SELECT	CASE WHEN F.LD = 'Y' THEN 'Y'
 				 WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN 'Y'
 				 ELSE NULL END AS LD
 			,CASE WHEN F.CAMHS = 'Y' THEN 'Y'
 				  WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN 'Y'
 				  WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07','C09') THEN 'Y'
 				  ELSE NULL END AS CAMHS
 			,CASE WHEN F.MH = 'Y' THEN 'Y'
 				  WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
 				  WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN NULL
 				  WHEN ServTeamTypeRefToMH IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'B01', 'C02', 'C03', 'C04', 'C08', 'D01', 'D02', 'D03', 'D04', 'Z01', 'Z02') THEN 'Y'
 				  WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01', 'C05', 'C06', 'C07','C09') THEN NULL
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
 			ON G.UniqHospProvSpellNum = B.UniqHospProvSpellNum 
             AND B.UniqMonthID = '$month_id' 
  LEFT JOIN  global_temp.ward_stay_cats AS F
 			ON B.UniqWardStayID = F.UniqWardStayID
  LEFT JOIN  $db_source.MHS102ServiceTypeReferredTo AS E
 			ON D.UniqServReqID = E.UniqServReqID 
             AND E.UniqMonthID = '$month_id' 
             AND ((ReferClosureDate IS NULL OR ReferClosureDate > '$rp_enddate') 
             AND (ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate'))
      WHERE	(D.ServDischDate IS NULL OR D.ServDischDate >'$rp_enddate')
 		    AND D.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,Service area end RP - For testing only - derivation to be created
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

# DBTITLE 1,Service area end RP - For testing only - derivation to be created
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

# DBTITLE 1,Service area end RP - For testing only - derivation to be created
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
 
 -- MPI
 
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
       FROM $db_source.MHS001MPI MPI
  LEFT JOIN global_temp.CCG CCG
            ON MPI.Person_ID = CCG.Person_ID
      WHERE UniqMonthID = '$month_id'
            AND MPI.PatMRecInRP = true) -- IC_NAT_MRecent_IN_RP_FLAG is renamed as PatMRecInRP in TOS :TODO
            
 -- changed temporary view to table insert to materialise query called 69 times in the remaining code.  Jonathan Bliss.           

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS001MPI_latest_month_data'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS001MPI_latest_month_data'))

# COMMAND ----------

# DBTITLE 1,MHS101Referral_open_end_rp - Referral table, latest month and open end RP
 %sql
 
 --  replacing ref.* here with a list of all columns to allow the _v sourced menhprimary_refresh and menh_point_in_time tables to be used as source data too.
 
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
            ,ref.DischLetterIssDate
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
          --and ic_use_submission_flag = 'Y' -- need in hue

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS701CPACareEpisode_latest'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS701CPACareEpisode_latest'))

# COMMAND ----------

# DBTITLE 1,Accomodation_latest
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Accomodation_latest AS
     SELECT  *
             ,dense_rank() OVER (PARTITION BY Person_ID ORDER BY AccommodationStatusDate DESC, UniqMonthID DESC, RecordNumber DESC) AS RANK
             ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY AccommodationStatusDate DESC, UniqMonthID DESC, RecordNumber DESC) AS PROV_RANK
       FROM  $db_source.MHS003AccommStatus
      WHERE  --AccommodationStatusDate BETWEEN DATEADD(MM,-12,dateadd(DD,1,'$rp_enddate')) AND '$rp_enddate'
             AccommodationStatusDate BETWEEN add_months(date_add('$rp_enddate',1),-12) AND '$rp_enddate'
             AND  AccommodationStatusCode IS NOT NULL
             AND UniqMonthID <= '$month_id'
   ORDER BY  Person_ID

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
 
        CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_list_in_rp AS
    SELECT DISTINCT CASE WHEN SRV.CAMHSTier IN ('4','9') THEN 'Y'
 						WHEN PROF.TreatFuncCodeMH = '711' THEN 'Y'
 						WHEN WRD.WardType IN ('01', '02') THEN 'Y'
 						WHEN WRD.WardAge IN ('10','11','12') THEN 'Y'
 						ELSE NULL END	AS CAMHS
 				 ,CASE WHEN WRD.IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
 						WHEN PROF.TreatFuncCodeMH = '700' THEN 'Y'
 						WHEN WRD.WardType = '05' THEN 'Y'
 						ELSE NULL END	AS LD
 				 ,CASE WHEN WRD.WardType IN ('01', '02', '05') THEN NULL 
 						WHEN WRD.IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
 						WHEN SRV.CAMHSTier IN ('4','9') THEN NULL 
 						WHEN PROF.TreatFuncCodeMH IN ('700', '711') THEN NULL
 						WHEN WRD.WardAge IN ('10', '11', '12') THEN NULL
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
 				ON WRD.UniqHospProvSpellNum = PROF.UniqHospProvSpellNum 
 				AND PROF.UniqMonthID = '$month_id' 
 				AND (EndDateAssCareProf IS NULL OR EndDateAssCareProf >= wrd.EndDateWardStay)
       LEFT JOIN $db_source.MHS102ServiceTypeReferredTo AS SRV
 				ON WRD.UniqServReqID = SRV.UniqServReqID 
                 AND SRV.UniqMonthID = '$month_id'
                 AND ((ReferClosureDate IS NULL OR ReferClosureDate >= EndDateWardStay) AND (ReferRejectionDate IS NULL OR ReferRejectionDate >= EndDateWardStay))

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
 --                     ON HOSP.UniqHospProvSpellNum = WRD.UniqHospProvSpellNum 
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

# DBTITLE 1,ward_type_list_RPstart - for testing - can use derivation
 %sql
 
  CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list_RPstart AS
  select distinct case WHEN CAMHSTier IN ('4','9') THEN 'Y'
  WHEN TreatFuncCodeMH = '711' THEN 'Y'
  WHEN WardType IN ('01', '02') THEN 'Y'
  WHEN WardAge IN ('10','11','12') THEN 'Y'
 ELSE NULL END AS CAMHS
 ,CASE	WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') THEN 'Y'
    WHEN TreatFuncCodeMH = '700' THEN 'Y'
     WHEN WardType = '05' THEN 'Y'
 	ELSE NULL END AS LD
  ,CASE	WHEN WardType IN ('01', '02', '05') THEN NULL 
    WHEN IntendClinCareIntenCodeMH in ('61', '62', '63') THEN NULL
    WHEN CAMHSTier IN ('4','9') THEN NULL 
    WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
    WHEN WardAge IN ('10', '11', '12') THEN NULL
    WHEN WardAge IN ('13', '14', '15') THEN 'Y'
    WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53') THEN 'Y'
   WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
   WHEN WardType IN ('03', '04', '06') THEN 'Y'
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
    on b.UniqHospProvSpellNum = c.UniqHospProvSpellNum 
   and c.UniqMonthID = '$month_id'
   and c.StartDateAssCareProf = b.StartDateWardStay -- ADDED TO ENSURE THE CARE PROFFESSIONAL IS ASSIGNED ON THE SAME DAY AS THE WARD DAY STARTS
  left join $db_source.MHS102ServiceTypeReferredTo d 
   on b.UniqServReqID = d.UniqServReqID 
   and d.UniqMonthID = '$month_id'
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
 
  CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list_RPstart AS
  select Case WHEN F.LD = 'Y' THEN 'Y'
 		 WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01') THEN 'Y'
 		 ELSE NULL END AS LD
   ,CASE	 WHEN F.CAMHS = 'Y' THEN 'Y'
 		 WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN 'Y'
 		 WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07','C09') THEN 'Y'
 		 ELSE NULL END AS CAMHS
   ,CASE	 WHEN F.MH = 'Y' THEN 'Y'
 		 WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
 		 WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN NULL
 		 WHEN ServTeamTypeRefToMH IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'B01', 'C02', 'C03', 'C04', 'C08', 'D01', 'D02', 'D03', 'D04', 'Z01', 'Z02') THEN 'Y'
 		 WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03','E04', 'B02', 'C01', 'C05', 'C06', 'C07','C09') THEN NULL
 		 ELSE 'Y' END AS MH,
          d.UniqServReqID 
   from $db_source.MHS101Referral d 
  left join $db_output.MHS001MPI_latest_month_data a 
            on a.Person_ID = d.Person_ID 
  left join $db_source.MHS501HospProvSpell g on g.UniqServReqID = d.UniqServReqID 
            and g.UniqMonthID = '$month_id' 
  left join $db_source.MHS502WardStay b on g.UniqHospProvSpellNum = b.UniqHospProvSpellNum 
            and b.UniqMonthID = '$month_id' 
  left join global_temp.ward_stay_cats_RPstart f on b.UniqWardStayID = f.UniqWardStayID
  left join global_temp.MHS102ServiceTypeReferredTo e on d.UniqServReqID = e.UniqServReqID 
           and e.UniqMonthID = '$month_id'
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
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CASSR_mapping AS
 SELECT case when a.GEOGRAPHY_CODE = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_CODE END as LADistrictAuth
       ,case when a.GEOGRAPHY_NAME = "W04000869" THEN "UNKNOWN" ELSE a.GEOGRAPHY_NAME END as LADistrictAuthName
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
 
 ORDER BY a.GEOGRAPHY_NAME