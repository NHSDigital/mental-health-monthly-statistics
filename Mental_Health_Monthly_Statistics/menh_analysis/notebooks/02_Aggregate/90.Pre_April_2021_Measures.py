# Databricks notebook source
# DBTITLE 1,1. Main monthly
 %sql

 -- Outpatient-Other measures
 INSERT INTO $db_output.Main_monthly_metric_values VALUES 

   ('MHS02', 'People on CPA  at the end of the reporting period')
   , ('AMH02', 'People in contact with adult mental health services on CPA  at the end of the reporting period')
   , ('AMH03', 'People on CPA aged 18 to 69  at the end of the reporting period (adult mental health services only)')
   , ('AMH04', 'People in contact with adult mental health services CPA at the end of the reporting period with HoNOS recorded')
   , ('AMH05', 'People on CPA for 12 months at the end of the reporting period (adult mental health services only)')
   , ('AMH06', 'People on CPA for 12 months with review at the end of the reporting period (adult mental health services only)')
   , ('AMH14', 'People aged 18 to 69 on CPA at the end of the reporting period  in settled accommodation (adult mental health services)')
   , ('AMH15', 'Proportion of people aged 18 to 69 on CPA at the end of the reporting period in settled accommodation (adult mental health services)')
   , ('AMH17', 'People aged 18 to 69 on CPA (adult mental health services) at the end of the reporting period in employment')
   , ('AMH18', 'Proportion of people aged 18 to 69 on CPA (adult mental health services) at the end of the reporting period  in employment')

# COMMAND ----------

# DBTITLE 1,4. CaP
 %sql

 INSERT INTO $db_output.CaP_metric_values VALUES
   ('ACC02', 'People on CPA at the end of the Reporting Period')
   , ('ACC53', 'Proportion of people at the end of the Reporting Period who are on CPA')

# COMMAND ----------

# DBTITLE 1,5. CYP monthly
 %sql

 -- Outpatient Other
 INSERT INTO $db_output.CYP_monthly_metric_values VALUES 

     ('CYP02', 'People in contact with children and young people''s mental health services on CPA at the end of the reporting period')

# COMMAND ----------

# DBTITLE 1,ACC02
# %sql
# /**ACC02 - People on CPA at the end of the Reporting Period - FINAL**/

# INSERT INTO $db_output.CAP_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#         ,'$rp_enddate' AS REPORTING_PERIOD_END
#         ,'$status' AS STATUS
#         ,BREAKDOWN
#         ,CAST (LEVEL AS STRING) AS LEVEL
#         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
#         ,CLUSTER
#         ,METRIC
#         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
#         ,'$db_source' AS SOURCE_DB
        
#   FROM	global_temp.ACC02

# COMMAND ----------

# DBTITLE 1,ACC02 - Provider
# %sql
# /**ACC02 - People on CPA at the end of the Reporting Period, PROVIDER - INTERMEDIATE**/

# INSERT INTO $db_output.CAP_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#         ,'$rp_enddate' AS REPORTING_PERIOD_END
#         ,'$status' AS STATUS
#         ,BREAKDOWN
#         ,CAST (LEVEL AS STRING) AS LEVEL
#         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
#         ,CLUSTER
#         ,METRIC
#         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
#         ,'$db_source' AS SOURCE_DB
        
#   FROM	global_temp.ACC02_PROVIDER

# COMMAND ----------

# DBTITLE 1,ACC02 - CCG
# %sql
# /**ACC02 - People on CPA at the end of the Reporting Period, CCG - INTERMEDIATE**/

# INSERT INTO $db_output.CAP_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
# 			,BREAKDOWN
# 			,CAST (LEVEL AS STRING) AS LEVEL
# 			,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
# 			,CLUSTER
# 			,METRIC
# 			,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM	global_temp.ACC02_CCG

# COMMAND ----------

# DBTITLE 1,ACC53
# %sql
# /**ACC53 - Proportion of people at the end of the Reporting Period who are on CPA, ENGLAND - FINAL**/

# INSERT INTO $db_output.CAP_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,ACC33.BREAKDOWN
# 			,CAST (ACC33.LEVEL AS STRING) AS LEVEL
# 			,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
# 			,ACC33.CLUSTER
# 			,'ACC53' AS METRIC
# 			,CAST (CASE	WHEN COALESCE (ACC02.METRIC_VALUE, 0) = 0 THEN 0
# 					    ELSE COALESCE (ACC02.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0) *100
# 						END AS STRING) AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM	global_temp.ACC33 AS ACC33
#  LEFT JOIN   global_temp.ACC02 AS ACC02
# 			ON ACC33.LEVEL = ACC02.LEVEL 
#             AND ACC33.CLUSTER = ACC02.CLUSTER 
#             AND ACC33.BREAKDOWN = ACC02.BREAKDOWN

# COMMAND ----------

# DBTITLE 1,ACC53 - Provider
# %sql
# /**ACC53 - Proportion of people at the end of the Reporting Period who are on CPA, PROVIDER - FINAL**/

# INSERT INTO $db_output.CAP_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
# 			,ACC33.BREAKDOWN
# 			,CAST (ACC33.LEVEL AS STRING) AS LEVEL
# 			,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
# 			,ACC33.CLUSTER
# 			,'ACC53' AS METRIC
# 			,CAST (CASE	WHEN COALESCE (ACC02.METRIC_VALUE, 0) = 0 THEN 0
# 					    ELSE COALESCE (ACC02.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0) *100
# 						END AS STRING) AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM	global_temp.ACC33_PROVIDER AS ACC33
#  LEFT JOIN   global_temp.ACC02_PROVIDER AS ACC02
# 			ON ACC33.LEVEL = ACC02.LEVEL 
#             AND ACC33.CLUSTER = ACC02.CLUSTER 
#             AND ACC33.BREAKDOWN = ACC02.BREAKDOWN

# COMMAND ----------

# DBTITLE 1,ACC53 - CCG
# %sql
# /**ACC62 - Proportion of people at the end of the Reporting Period who are on CPA, ENGLAND - FINAL**/

# --CREATE OR REPLACE GLOBAL TEMP VIEW ACC53_ccg_v AS -- removed 20/8/19 and replaced with insert into statement (believe this was done in error in the first place)
# INSERT INTO $db_output.CAP_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
# 			,ACC33.BREAKDOWN
# 			,CAST (ACC33.LEVEL AS STRING) AS LEVEL
# 			,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
# 			,ACC33.CLUSTER
# 			,'ACC53' AS METRIC
# 			,CAST (CASE	WHEN COALESCE (ACC02.METRIC_VALUE, 0) = 0 THEN 0
# 					    ELSE COALESCE (ACC02.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0) *100
# 						END AS STRING) AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM	global_temp.ACC33_CCG AS ACC33
#  LEFT JOIN   global_temp.ACC02_CCG AS ACC02
# 			ON ACC33.LEVEL = ACC02.LEVEL 
#             AND ACC33.CLUSTER = ACC02.CLUSTER 
#             AND ACC33.BREAKDOWN = ACC02.BREAKDOWN

# COMMAND ----------

# DBTITLE 1,CYP02 - CCG
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	   ,IC_Rec_CCG AS PRIMARY_LEVEL
     	   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	   ,'NONE' AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CYP02' AS METRIC
            ,CAST(COALESCE(CAST(COUNT (DISTINCT Person_ID)AS INT) , 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CYP02_prep
   GROUP BY IC_Rec_CCG
            ,NAME;

# COMMAND ----------

# DBTITLE 1,CYP02 - Provider
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,MPI.OrgIDProv	AS PRIMARY_LEVEL
 			,RD.NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'CYP02' AS METRIC
             ,CAST(COALESCE(CAST(COUNT (DISTINCT MPI.Person_ID)AS INT) , 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_source.MHS001MPI MPI
  LEFT JOIN $db_output.MHS101Referral_open_end_rp AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND MPI.OrgIDProv=REF.OrgIDProv
  LEFT JOIN $db_output.MHS701CPACareEpisode_latest AS CPAE
             ON MPI.Person_ID = CPAE.Person_ID 
             AND MPI.OrgIDProv = CPAE.OrgIDProv
              --AND CPAE.ic_use_submission_flag = 'Y' -- need in hue
  INNER JOIN global_temp.rd_org_daily_latest RD
             ON MPI.OrgIDProv = RD.ORG_CODE
       WHERE REF.CYPServiceRefEndRP_temp = true
             AND MPI.UniqMonthID = '$month_id' 
             AND CPAE.Person_ID is not null
             AND REF.Person_ID is not null
             --AND MPI.ic_use_submission_flag = 'Y' -- need in hue
    GROUP BY MPI.OrgIDProv, RD.NAME;

# COMMAND ----------

# DBTITLE 1,CYP02 - England
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CYP02' AS METRIC
            ,CAST(COALESCE(CAST(COUNT (DISTINCT Person_ID)AS INT) , 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CYP02_prep;

# COMMAND ----------

# DBTITLE 1,MHS02 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'England' AS BREAKDOWN,
 			'England' AS PRIMARY_LEVEL,
 			'England' AS PRIMARY_LEVEL_DESCRIPTION,
 			'NONE' AS SECONDARY_LEVEL,
 	        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			'MHS02' AS METRIC,
             CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID

# COMMAND ----------

# DBTITLE 1,AMH02 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'England' AS BREAKDOWN,
             'England' AS PRIMARY_LEVEL,
             'England' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH02' AS METRIC,
             CAST (Coalesce (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
            '$db_source' AS SOURCE_DB
            
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
       WHERE AMHServiceRefEndRP_temp = TRUE 

# COMMAND ----------

# DBTITLE 1,AMH03 National
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'England' AS BREAKDOWN,
 			 'England' AS PRIMARY_LEVEL,
 			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
              '$db_source' AS SOURCE_DB
              
         FROM $db_output.AMH03_prep

# COMMAND ----------

# DBTITLE 1,AMH04
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
              ,'$rp_enddate' AS REPORTING_PERIOD_END
 		     ,'$status' AS STATUS
 			 ,'England' AS BREAKDOWN
 			 ,'England' AS PRIMARY_LEVEL
 			 ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			 ,'NONE' AS SECONDARY_LEVEL
 			 ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			 ,'AMH04' AS METRIC
              ,CAST(COALESCE(cast(COUNT(DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
              ,'$db_source' AS SOURCE_DB
              
        FROM global_temp.AMH04_prep;

# COMMAND ----------

# DBTITLE 1,AMH05 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE'	AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'AMH05' AS METRIC
             ,CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp	AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON REF.Person_ID = CPAE.Person_ID 
       WHERE REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD((ADD_MONTHS('$rp_enddate', -12)),1);

# COMMAND ----------

# DBTITLE 1,AMH06 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 		   ,'$status' AS STATUS
 		   ,'England' AS BREAKDOWN
 		   ,'England' AS PRIMARY_LEVEL
 		   ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 		   ,'NONE'	AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		   ,'AMH06' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM $db_output.MHS101Referral_open_end_rp	AS REF
 INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
            ON REF.Person_ID = CPAE.Person_ID 
 INNER JOIN $db_source.MHS702CPAReview AS CPAR
            ON CPAE.UniqCPAEpisodeID = CPAR.UniqCPAEpisodeID 
            AND CPAR.UniqMonthID <= '$month_id' 
      WHERE REF.AMHServiceRefEndRP_temp = true
 		   AND CPAE.StartDateCPA < DATE_ADD((ADD_MONTHS('$rp_enddate', -12)),1)
 		   AND CPAR.CPAReviewDate >= DATE_ADD((ADD_MONTHS('$rp_enddate', -12)),1)
 		   AND CPAR.CPAReviewDate <= '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,AMH15
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  $db_output.AMH03_prep AS AMH15
  LEFT JOIN  $db_output.AMH14_prep AS AMH14
            ON AMH14.Person_ID = AMH15.Person_ID;
          

# COMMAND ----------

# DBTITLE 1,AMH18
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  $db_output.AMH03_prep AS AMH18
  LEFT JOIN $db_output.AMH17_prep AS AMH17
  ON AMH17.Person_ID = AMH18.Person_ID

# COMMAND ----------

# DBTITLE 1,MHS02 CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'CCG - GP Practice or Residence' AS BREAKDOWN,
 			IC_REC_CCG AS PRIMARY_LEVEL,
 			NAME AS PRIMARY_LEVEL_DESCRIPTION,
 			'NONE' AS SECONDARY_LEVEL,
 	        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			'MHS02' AS METRIC,
              CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
  INNER JOIN $db_output.MHS001MPI_latest_month_data AS CCG 
             ON REF.Person_ID = CCG.Person_ID
    GROUP BY IC_REC_CCG, NAME 

# COMMAND ----------

# DBTITLE 1,AMH02 CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'CCG - GP Practice or Residence' AS BREAKDOWN,
             IC_REC_CCG AS PRIMARY_LEVEL,
             NAME AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH02' AS METRIC,
             CAST (coalesce (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
  INNER JOIN $db_output.MHS001MPI_latest_month_data AS CCG 
             ON REF.Person_ID = CCG.Person_ID 
       WHERE AMHServiceRefEndRP_temp = TRUE 
    GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,AMH03 CCG
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'CCG - GP Practice or Residence' AS BREAKDOWN,
 			 IC_REC_CCG AS PRIMARY_LEVEL,
 			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (coalesce (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
              '$db_source' AS SOURCE_DB
              
         FROM $db_output.AMH03_prep 
     GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,AMH04
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'CCG - GP Practice or Residence' AS BREAKDOWN,
 			 IC_REC_CCG AS PRIMARY_LEVEL,
 			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH04' AS METRIC,
 			 CAST(COALESCE(CAST(COUNT(DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
              '$db_source' AS SOURCE_DB
              
         FROM global_temp.AMH04_prep
     GROUP BY IC_REC_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH05
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG	AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH05' AS METRIC
 			,CAST (COALESCE (CAST(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE    
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS001MPI_latest_month_data AS MPI
  INNER JOIN $db_output.MHS101Referral_open_end_rp	AS REF
 			ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON MPI.Person_ID = CPAE.Person_ID
             AND CPAE.UniqMonthID = '$month_id' 
       WHERE REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH06
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG	AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH06' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
             
        FROM	$db_output.MHS001MPI_latest_month_data AS MPI
  INNER JOIN $db_output.MHS101Referral_open_end_rp	AS REF
 			ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON MPI.Person_ID = CPAE.Person_ID             
  INNER JOIN $db_source.MHS702CPAReview AS CPAR
 		    ON CPAE.UniqCPAEpisodeID = CPAR.UniqCPAEpisodeID 
             AND CPAR.UniqMonthID <= '$month_id' 
       WHERE	REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			AND CPAR.CPAReviewDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			AND CPAR.CPAReviewDate <= '$rp_enddate'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH15
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'CCG - GP Practice or Residence' AS BREAKDOWN
             ,AMH15.IC_Rec_CCG AS PRIMARY_LEVEL
             ,AMH15.NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  $db_output.AMH03_prep AS AMH15
  LEFT JOIN  $db_output.AMH14_prep AS AMH14
            ON AMH14.Person_ID = AMH15.Person_ID
   GROUP BY AMH15.IC_Rec_CCG, AMH15.NAME;

# COMMAND ----------

# DBTITLE 1,AMH18
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			, AMH18.IC_Rec_CCG AS PRIMARY_LEVEL
 			, AMH18.NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100 AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH03_prep AS AMH18
   LEFT JOIN $db_output.AMH17_prep AS AMH17
             ON AMH17.Person_ID = AMH18.Person_ID
    GROUP BY AMH18.IC_Rec_CCG, AMH18.NAME;

# COMMAND ----------

# DBTITLE 1,MHS02 Provider
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
                     '$status' AS STATUS,
                     'Provider' AS BREAKDOWN,
                      REF.OrgIDProv AS PRIMARY_LEVEL,
                     'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
                     'NONE' AS SECONDARY_LEVEL,
                     'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
                     'MHS02'  AS METRIC,
                     CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
                FROM $db_output.MHS101Referral_open_end_rp AS REF
          INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
                  ON REF.Person_ID = CPA.Person_ID
                 AND REF.OrgIDProv = CPA.OrgIDProv
            GROUP BY REF.OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH02 Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             REF.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH02'  AS METRIC,
             CAST (coalesce( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
             AND REF.OrgIDProv = CPA.OrgIDProv
             AND REF.uniqmonthid = CPA.uniqmonthid
       WHERE AMHServiceRefEndRP_temp = TRUE
             AND REF.uniqmonthid = '${month_id}'
    GROUP BY REF.OrgIDProv   

# COMMAND ----------

# DBTITLE 1,AMH03 Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'Provider' AS BREAKDOWN,
 			 OrgIDProv AS PRIMARY_LEVEL,
 			 'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (coalesce (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH03_prep_prov
     GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH04
 %sql
      INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 		        '$status' AS STATUS,
 		        'Provider' AS BREAKDOWN,
 			    MPI.OrgIDProv AS PRIMARY_LEVEL,
 			    'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
 			    'NONE' AS SECONDARY_LEVEL,
 			    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			    'AMH04' AS METRIC,
 			    CAST(COALESCE(CAST(COUNT(DISTINCT MPI.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
            FROM $db_source.MHS001MPI AS MPI
 	 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 				ON MPI.Person_ID = REF.Person_ID 
 				AND REF.UniqMonthID = '$month_id' 
                 AND REF.OrgIDProv = MPI.OrgIDProv
 	 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 				ON MPI.Person_ID = CPAE.Person_ID 
 				AND CPAE.UniqMonthID = '$month_id' 
                 AND CPAE.OrgIDProv = MPI.OrgIDProv
 	 LEFT JOIN $db_source.MHS801ClusterTool AS CCT
 				ON MPI.Person_ID = CCT.Person_ID 
 				AND CCT.UniqMonthID <= '$month_id' 
 				AND CCT.AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				AND CCT.AssToolCompDate <= '$rp_enddate'
                 AND CCT.OrgIDProv = MPI.OrgIDProv
 	 LEFT JOIN	(	 SELECT UniqClustID
 					        ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					   FROM $db_source.MHS802ClusterAssess
 					  WHERE UniqMonthID <= '$month_id'
 					    AND CodedAssToolType IN ('979641000000103', '979651000000100', '979661000000102', '979671000000109', '979681000000106', '979691000000108', '979701000000108', '979711000000105', '979721000000104',                                                      '979731000000102', '979741000000106', '979751000000109')
 					    AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				   GROUP BY UniqClustID
 				) AS CCAHONOS
 				ON CCT.UniqClustID = CCAHONOS.UniqClustID
 	LEFT JOIN   (	SELECT UniqClustID
 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					 FROM $db_source.MHS802ClusterAssess
 					WHERE UniqMonthID <= '$month_id'
 						  AND CodedAssToolType IN ('985071000000105', '985081000000107', '985111000000104', '985121000000105', '985131000000107', '985141000000103')
 						  AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				 GROUP BY UniqClustID
 				) AS CCACT
 				ON CCT.UniqClustID = CCACT.UniqClustID
 	LEFT JOIN $db_output.MHS101Referral_open_end_rp
 					AS REFYR
 					ON MPI.Person_ID = REFYR.Person_ID 
 					AND REFYR.UniqMonthID <= '$month_id' 
                     AND REFYR.OrgIDProv = MPI.OrgIDProv
 	LEFT JOIN $db_source.MHS201CareContact AS CCONT
 			  ON REFYR.UniqServReqID = CCONT.UniqServReqID 
 			  AND CCONT.UniqMonthID <= '$month_id'
 			  AND CCONT.CareContDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			  AND CCONT.CareContDate <= '$rp_enddate'
 	LEFT JOIN $db_source.MHS202CareActivity AS CACT
 			  ON CCONT.UniqCareContID = CACT.UniqCareContID 
 			  AND CACT.UniqMonthID <= '$month_id'
     LEFT JOIN (    SELECT UniqCareActID
 						  ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					FROM $db_source.MHS607CodedScoreAssessmentAct
 				   WHERE UniqMonthID <= '$month_id'
 						 AND CodedAssToolType IN ('979641000000103', '979651000000100', '979661000000102', '979671000000109', '979681000000106', '979691000000108', '979701000000108', '979711000000105', '979721000000104',                                                     '979731000000102', '979741000000106', '979751000000109')
 						 AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				GROUP BY UniqCareActID
 		      )	AS CSACHONOS
 			  ON CACT.UniqCareActID = CSACHONOS.UniqCareActID
     LEFT JOIN (	   SELECT UniqCareActID
 						  ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					FROM $db_source.MHS607CodedScoreAssessmentAct
 				   WHERE UniqMonthID <= '$month_id'
 				   		 AND CodedAssToolType IN ('980761000000107', '980771000000100', '980781000000103', '980791000000101', '980801000000102', '980811000000100', '980821000000106', '980831000000108', '980841000000104',                                                     '980851000000101', '980861000000103', '980871000000105')
 					     AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 			    GROUP BY UniqCareActID
 			  )	AS CSACHONOS65
 			  ON CACT.UniqCareActID = CSACHONOS65.UniqCareActID
 	LEFT JOIN (   SELECT UniqCareActID
 					     ,COUNT (DISTINCT CodedAssToolType)	AS CodedAssToolTypeNum
 				    FROM $db_source.MHS607CodedScoreAssessmentAct
 			       WHERE UniqMonthID <= '$month_id'
 						 AND CodedAssToolType IN ('989881000000104', '989931000000107', '989941000000103', '989951000000100', '989961000000102', '989971000000109', '989981000000106', '989991000000108', '990001000000107',                                                     '989891000000102', '989901000000101', '989911000000104', '989921000000105')
 						 AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				GROUP BY UniqCareActID
 			  )	AS CSACHONOSCA
 			  ON CACT.UniqCareActID = CSACHONOSCA.UniqCareActID
 	LEFT JOIN (   SELECT UniqCareActID
 						 ,COUNT (DISTINCT CodedAssToolType)	AS CodedAssToolTypeNum
 				    FROM $db_source.MHS607CodedScoreAssessmentAct
 				   WHERE UniqMonthID <= '$month_id'
 						 AND CodedAssToolType IN ('989751000000102', '989801000000109', '989811000000106', '989821000000100', '989831000000103', '989841000000107', '989851000000105', '989861000000108', '989871000000101',                                                     '989761000000104', '989771000000106', '989781000000108', '989791000000105')
 						 AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				GROUP BY UniqCareActID
 			  )	AS CSACHONOSCACR
 			  ON CACT.UniqCareActID = CSACHONOSCACR.UniqCareActID
 	LEFT JOIN (	 SELECT UniqCareActID
 						,COUNT (DISTINCT CodedAssToolType)AS CodedAssToolTypeNum
 				   FROM $db_source.MHS607CodedScoreAssessmentAct
 			      WHERE UniqMonthID <= '$month_id'
 						AND CodedAssToolType IN ('989621000000101', '989671000000102', '989681000000100', '989691000000103', '989701000000103', '989711000000101', '989721000000107', '989731000000109', '989741000000100',                                                      '989631000000104', '989641000000108', '989651000000106', '989661000000109')
 						AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 			   GROUP BY	UniqCareActID
 			  )	AS CSACHONOSCASR
 			  ON CACT.UniqCareActID = CSACHONOSCASR.UniqCareActID
 	LEFT JOIN (	  SELECT UniqCareActID
 						 ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					FROM $db_source.MHS607CodedScoreAssessmentAct
 			       WHERE UniqMonthID <= '$month_id'
 						 AND CodedAssToolType IN ('981391000000108', '981401000000106', '981411000000108', '981421000000102', '981431000000100', '981441000000109', '981451000000107')
 					     AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 			    GROUP BY UniqCareActID
 			  )	AS CSACHONOSSEC
 			  ON CACT.UniqCareActID = CSACHONOSSEC.UniqCareActID
 	LEFT JOIN (   SELECT UniqServReqID
 					     ,AssToolCompTimestamp
 						 ,COUNT (DISTINCT CodedAssToolType)	AS CodedAssToolTypeNum
 				    FROM $db_source.MHS606CodedScoreAssessmentRefer
 				   WHERE  UniqMonthID <= '$month_id'
 						  AND AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 						  AND AssToolCompTimestamp <= '$rp_enddate'
 						  AND CodedAssToolType IN ('979641000000103', '979651000000100', '979661000000102', '979671000000109', '979681000000106', '979691000000108', '979701000000108', '979711000000105', '979721000000104',                                                      '979731000000102', '979741000000106', '979751000000109')
 						  AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				 GROUP BY UniqServReqID
 						  ,AssToolCompTimestamp
 			  )	AS CSARHONOS
 			  ON REFYR.UniqServReqID = CSARHONOS.UniqServReqID
 	LEFT JOIN (    SELECT UniqServReqID
 						  ,AssToolCompTimestamp
 						  ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					FROM  $db_source.MHS606CodedScoreAssessmentRefer
 					WHERE UniqMonthID <= '$month_id'
 						  AND AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 						  AND AssToolCompTimestamp <= '$rp_enddate'
 						  AND CodedAssToolType IN ('980761000000107', '980771000000100', '980781000000103', '980791000000101', '980801000000102', '980811000000100', '980821000000106', '980831000000108', '980841000000104',                                                      '980851000000101', '980861000000103', '980871000000105')
 						  AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				GROUP BY  UniqServReqID
 						  ,AssToolCompTimestamp
 			   ) AS CSARHONOS65
 			   ON REFYR.UniqServReqID = CSARHONOS65.UniqServReqID
 	LEFT JOIN (  	SELECT UniqServReqID
 					       ,AssToolCompTimestamp
 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
 				     WHERE UniqMonthID <= '$month_id'
 						   AND AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 						   AND AssToolCompTimestamp <= '$rp_enddate'
 						   AND CodedAssToolType IN ('989881000000104', '989931000000107', '989941000000103', '989951000000100', '989961000000102', '989971000000109', '989981000000106', '989991000000108', '990001000000107',                                                     '989891000000102', '989901000000101', '989911000000104', '989921000000105')
 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				  GROUP BY UniqServReqID
 					       ,AssToolCompTimestamp
 			  )	AS CSARHONOSCA
 			  ON REFYR.UniqServReqID = CSARHONOSCA.UniqServReqID
 	LEFT JOIN (  	SELECT UniqServReqID
 					       ,AssToolCompTimestamp
 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
 				     WHERE UniqMonthID <= '$month_id'
 						   AND AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 						   AND AssToolCompTimestamp <= '$rp_enddate'
 						   AND CodedAssToolType IN ('989751000000102', '989801000000109', '989811000000106', '989821000000100', '989831000000103', '989841000000107', '989851000000105', '989861000000108', '989871000000101',                                                     '989761000000104', '989771000000106', '989781000000108', '989791000000105')
 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				  GROUP BY UniqServReqID
 					       ,AssToolCompTimestamp
 		   	   ) AS CSARHONOSCACR
 			   ON REFYR.UniqServReqID = CSARHONOSCACR.UniqServReqID
 	LEFT JOIN (  	SELECT UniqServReqID
 					       ,AssToolCompTimestamp
 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
 				     WHERE UniqMonthID <= '$month_id'
 						   AND AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 						   AND AssToolCompTimestamp <= '$rp_enddate'
 						   AND CodedAssToolType IN ('989621000000101', '989671000000102', '989681000000100', '989691000000103', '989701000000103', '989711000000101', '989721000000107', '989731000000109', '989741000000100',                                                     '989631000000104', '989641000000108', '989651000000106', '989661000000109')
 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				 GROUP BY  UniqServReqID
 				     	   ,AssToolCompTimestamp
 		       ) AS CSARHONOSCASR
 			   ON REFYR.UniqServReqID = CSARHONOSCASR.UniqServReqID
 	LEFT JOIN (  	SELECT UniqServReqID
 					       ,AssToolCompTimestamp
 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
 				     WHERE UniqMonthID <= '$month_id'
 						   AND AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 						   AND AssToolCompTimestamp <= '$rp_enddate'
 						   AND CodedAssToolType IN ('981391000000108', '981401000000106', '981411000000108', '981421000000102', '981431000000100', '981441000000109', '981451000000107')
 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				  GROUP BY UniqServReqID
 						   ,AssToolCompTimestamp
 			   ) AS CSARHONOSSEC
 			   ON REFYR.UniqServReqID = CSARHONOSSEC.UniqServReqID					
          WHERE MPI.UniqMonthID = '$month_id'
          AND (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
                AND REF.AMHServiceRefEndRP_temp = True
                AND REFYR.AMHServiceRefEndRP_temp = True
                AND (CCAHONOS.CodedAssToolTypeNum >= 9 
                      OR CCACT.CodedAssToolTypeNum >= 6 
                      OR CSACHONOS.CodedAssToolTypeNum >= 9 
                      OR CSACHONOS65.CodedAssToolTypeNum >= 9 
                      OR CSACHONOSCA.CodedAssToolTypeNum = 13 
                      OR CSACHONOSCACR.CodedAssToolTypeNum = 13 
                      OR CSACHONOSCASR.CodedAssToolTypeNum = 13 
                      AND CSACHONOSSEC.CodedAssToolTypeNum = 7 
                      OR CSARHONOS.CodedAssToolTypeNum >= 9 
                      OR CSARHONOS65.CodedAssToolTypeNum >= 9 
                      OR CSARHONOSCA.CodedAssToolTypeNum = 13 
                      OR CSARHONOSCACR.CodedAssToolTypeNum = 13 
                      OR CSARHONOSCASR.CodedAssToolTypeNum = 13
                      AND CSARHONOSSEC.CodedAssToolTypeNum = 7
                     )
      GROUP BY MPI.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH05
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'Provider'	AS BREAKDOWN
 			,REF.OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH05' AS METRIC
 			,CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.MHS101Referral_open_end_rp	AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON REF.Person_ID = CPAE.Person_ID
             AND CPAE.UniqMonthID = '$month_id'
             AND REF.OrgIDProv = CPAE.OrgIDProv
       WHERE REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
       GROUP BY REF.OrgIDProv;	

# COMMAND ----------

# DBTITLE 1,AMH06
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider'	AS BREAKDOWN
 			,REF.OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH06'AS METRIC
 			,CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE		
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPAE
             ON REF.Person_ID = CPAE.Person_ID 
             AND REF.OrgIDProv = CPAE.OrgIDProv       
  INNER JOIN $db_source.MHS702CPAReview	AS CPAR
 			ON CPAE.UniqCPAEpisodeID = CPAR.UniqCPAEpisodeID 
             AND CPAE.OrgIDProv = CPAR.OrgIDProv 
             AND CPAR.UniqMonthID <= '$month_id' 
 	  WHERE REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			AND CPAR.CPAReviewDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			AND CPAR.CPAReviewDate <= '$rp_enddate'
 	  GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH15
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,AMH15.OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  $db_output.AMH03_prep_prov AS AMH15
  LEFT JOIN  $db_output.AMH14_prep_prov AS AMH14
            ON AMH14.Person_ID = AMH15.Person_ID
            AND AMH14.OrgIDProv = AMH15.OrgIDProv
   GROUP BY AMH15.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH18
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			, AMH18.OrgIDProv AS PRIMARY_LEVEL
 			, 'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100 AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH03_prep_prov AS AMH18
   LEFT JOIN $db_output.AMH17_prep_prov AS AMH17
             ON AMH17.Person_ID = AMH18.Person_ID
             AND AMH17.OrgIDProv = AMH18.OrgIDProv
    GROUP BY AMH18.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH03 CASSR
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'CASSR' AS BREAKDOWN,
 			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
 			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (coalesce (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
              '$db_source' AS SOURCE_DB
              
         FROM $db_output.AMH03_prep
     GROUP BY COALESCE(CASSR,"UNKNOWN"), COALESCE(CASSR_description,"UNKNOWN")

# COMMAND ----------

# DBTITLE 1,AMH03 CASSR;Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'CASSR; Provider' AS BREAKDOWN,
 			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
 			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
 			 OrgIDProv AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (coalesce (CAST(COUNT (DISTINCT C.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
              '$db_source' AS SOURCE_DB
              
         FROM $db_output.AMH03_prep AS C
 INNER JOIN global_temp.AMH15_prep_prov AS P
 ON C.Person_ID = P.Person_ID
     GROUP BY COALESCE(CASSR,"UNKNOWN"), COALESCE(CASSR_description,"UNKNOWN"), OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH15 CASSR
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR' AS BREAKDOWN
 			,COALESCE(AMH15.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(AMH15.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH03_prep AS AMH15
   LEFT JOIN  $db_output.AMH14_prep AS AMH14
              ON AMH14.Person_ID = AMH15.Person_ID
    GROUP BY  COALESCE(AMH15.CASSR,"UNKNOWN")
             ,COALESCE(AMH15.CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,AMH15 CASSR;Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR; Provider' AS BREAKDOWN
 			,COALESCE(AMH15.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(AMH15.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,AMH15.OrgIDProv AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  
                 (SELECT DISTINCT CASSR, CASSR_description, C.Person_ID, P.OrgIDProv
                            FROM $db_output.AMH03_prep AS C
                      INNER JOIN global_temp.AMH15_prep_prov AS P
                                 ON C.Person_ID = P.Person_ID) AS AMH15 
        LEFT JOIN                         
                (SELECT DISTINCT CASSR, C.Person_ID, P.OrgIDProv
                            FROM $db_output.AMH14_prep AS C
                      INNER JOIN $db_output.AMH14_prep_prov AS P
                                 ON C.Person_ID = P.Person_ID) as AMH14
                                           
                                     ON AMH15.Person_ID = AMH14.Person_ID
                                     AND AMH15.CASSR = AMH14.CASSR
                                     AND AMH15.OrgIDProv = AMH14.OrgIDProv
    GROUP BY  COALESCE(AMH15.CASSR,"UNKNOWN")
             ,COALESCE(AMH15.CASSR_description,"UNKNOWN")
             ,AMH15.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH18 CASSR
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR' AS BREAKDOWN
 			,COALESCE(AMH18.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(AMH18.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  $db_output.AMH03_prep AS AMH18
  LEFT JOIN $db_output.AMH17_prep AS AMH17
  ON AMH17.Person_ID = AMH18.Person_ID
 GROUP BY COALESCE(AMH18.CASSR,"UNKNOWN"), COALESCE(AMH18.CASSR_description,"UNKNOWN")

# COMMAND ----------

# DBTITLE 1,AMH18 CASSR;Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR; Provider' AS BREAKDOWN
 			,COALESCE(AMH18.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(AMH18.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,AMH18.OrgIDProv AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  
               (SELECT DISTINCT CASSR
                               ,CASSR_description
                               ,C.Person_ID
                               ,P.OrgIDProv 
                           FROM $db_output.AMH03_prep AS C
                     INNER JOIN global_temp.AMH18_prep_prov AS P
                                ON C.Person_ID = P.Person_ID) AS AMH18     
        LEFT JOIN                             
                (SELECT DISTINCT CASSR
                               ,C.Person_ID
                               ,P.OrgIDProv 
                          FROM $db_output.AMH17_prep AS C
                    INNER JOIN $db_output.AMH17_prep_prov AS P
                               ON C.Person_ID = P.Person_ID) AS AMH17
                 
                                   ON AMH18.Person_ID = AMH17.Person_ID
                                   AND AMH18.CASSR = AMH17.CASSR
                                   AND AMH18.OrgIDProv = AMH17.OrgIDProv
     GROUP BY COALESCE(AMH18.CASSR,"UNKNOWN")
             ,COALESCE(AMH18.CASSR_description,"UNKNOWN")
             ,AMH18.OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH14 - National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH14_prep;

# COMMAND ----------

# DBTITLE 1,AMH17 - National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			AND EmployStatusRecDate <= '${rp_enddate}';

# COMMAND ----------

# DBTITLE 1,AMH14 - CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'CCG - GP Practice or Residence' AS BREAKDOWN
             ,IC_Rec_CCG AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH14_prep
    GROUP BY IC_Rec_CCG,NAME;

# COMMAND ----------

# DBTITLE 1,AMH17 - CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			, IC_Rec_CCG AS PRIMARY_LEVEL
 			, NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			AND EmployStatusRecDate <= '${rp_enddate}'
       GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH14 - Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH14_prep_prov
    GROUP BY OrgIDProv; 

# COMMAND ----------

# DBTITLE 1,AMH17 - Provider
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			, OrgIDProv AS PRIMARY_LEVEL
 			, 'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep_prov
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			AND EmployStatusRecDate <= '${rp_enddate}'
       GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH14 CASSR
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH14_prep
    GROUP BY  COALESCE(CASSR,"UNKNOWN")
             ,COALESCE(CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,AMH14 CASSR - Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR; Provider' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,OrgIDProv AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH14_prep AS C
   INNER JOIN $db_output.AMH14_prep_prov AS P
              ON C.PERSON_ID = P.PERSON_ID
     GROUP BY COALESCE(CASSR,"UNKNOWN")
             ,COALESCE(CASSR_description,"UNKNOWN")
             ,OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH17 CASSR
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 	 		 AND EmployStatusRecDate <= '${rp_enddate}'
     GROUP BY COALESCE(CASSR,"UNKNOWN")
 			,COALESCE(CASSR_description,"UNKNOWN") ;

# COMMAND ----------

# DBTITLE 1,AMH17 CASSR - Provider
 %sql

 INSERT INTO $db_output.Main_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR; Provider' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
 			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
 			,OrgIDProv AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep AS C
   INNER JOIN $db_output.AMH17_prep_prov AS P
              ON C.Person_ID = P.Person_ID
              AND C.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
  	  		 AND C.EmployStatusRecDate <= '${rp_enddate}'
              AND P.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 		 	 AND P.EmployStatusRecDate <= '${rp_enddate}'
     GROUP BY COALESCE(CASSR,"UNKNOWN")
 			,COALESCE(CASSR_description,"UNKNOWN") 
             ,OrgIDProv;