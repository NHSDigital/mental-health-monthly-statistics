# Databricks notebook source
 %md
 # CaP Prep assets:
 - CaP_possible_metrics
 - MHS803CareCluster_common --Materialised
 - StartDateCareClustMax
 - MHS803UniqIDMAX
 - StartDateCareClustMaxProv
 - MHS803UniqIDMAXProv
 - ACC33
 - ACC36
 - ACC33_PROVIDER
 - ACC36_PROVIDER
 - ACC33_CCG
 - ACC36_CCG
 - ACC02
 - ACC54
 - ACC02_PROVIDER
 - ACC54_PROVIDER
 - ACC02_CCG
 - ACC54_CCG


# COMMAND ----------

# DBTITLE 1,MHS803CareCluster_common
 %sql

 TRUNCATE table $db_output.MHS803CareCluster_common;

 INSERT INTO $db_output.MHS803CareCluster_common
     select UniqClustID,
            OrgIDProv, 
            UniqMonthID,
            MHS803UniqID,
            AMHCareClustCodeFin,
            startdatecareclust,
            CASE	WHEN CC.AMHCareClustCodeFin IN ('0', '00') THEN '0' 
                 WHEN CC.AMHCareClustCodeFin IN ('01', '1') THEN '1'
                 WHEN CC.AMHCareClustCodeFin IN ('02', '2') THEN '2'
                 WHEN CC.AMHCareClustCodeFin IN ('03', '3') THEN '3'
                 WHEN CC.AMHCareClustCodeFin IN ('04', '4') THEN '4'
                 WHEN CC.AMHCareClustCodeFin IN ('05', '5') THEN '5'
                 WHEN CC.AMHCareClustCodeFin IN ('06', '6') THEN '6'
                 WHEN CC.AMHCareClustCodeFin IN ('07', '7') THEN '7'
                 WHEN CC.AMHCareClustCodeFin IN ('08', '8') THEN '8'
                 WHEN CC.AMHCareClustCodeFin IN ('10') THEN '10'
                 WHEN CC.AMHCareClustCodeFin IN ('11') THEN '11'
                 WHEN CC.AMHCareClustCodeFin IN ('12') THEN '12'
                 WHEN CC.AMHCareClustCodeFin IN ('13') THEN '13'
                 WHEN CC.AMHCareClustCodeFin IN ('14') THEN '14'
                 WHEN CC.AMHCareClustCodeFin IN ('15') THEN '15'
                 WHEN CC.AMHCareClustCodeFin IN ('16') THEN '16'
                 WHEN CC.AMHCareClustCodeFin IN ('17') THEN '17'
                 WHEN CC.AMHCareClustCodeFin IN ('18') THEN '18'
                 WHEN CC.AMHCareClustCodeFin IN ('19') THEN '19'
                 WHEN CC.AMHCareClustCodeFin IN ('20') THEN '20'
                 WHEN CC.AMHCareClustCodeFin IN ('21') THEN '21'
                 ELSE 'Unknown'
                 END AS CLUSTER
      from $db_source.MHS803CareCluster CC
     where (CC.EndDateCareClust IS NULL OR CC.EndDateCareClust > '$rp_enddate')
           AND CC.AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')
           AND CC.UniqMonthID = '$month_id'

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS803CareCluster_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS803CareCluster_common'))

# COMMAND ----------

# DBTITLE 1,StartDateCareClustMax
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW StartDateCareClustMax AS 
     SELECT CCT.Person_ID
 		   ,MAX(CC.StartDateCareClust) AS StartDateCareClustMAX
       FROM $db_source.MHS801ClusterTool AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
 		   ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON CCT.Person_ID = REF.Person_ID
      WHERE CCT.UniqMonthID = '$month_id'
   GROUP BY CCT.Person_ID

# COMMAND ----------

# DBTITLE 1,MHS803UniqIDMAX
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS803UniqIDMAX AS 
     SELECT CCT.Person_ID
 		   ,MAX (CC.MHS803UniqID) AS MHS803UniqIDMAX		
       FROM $db_source.MHS801ClusterTool AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
 		   ON CCT.UniqClustID = CC.UniqClustID
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON CCT.Person_ID = REF.Person_ID  
 INNER JOIN global_temp.StartDateCareClustMax AS SDCCMAX
 		   ON CC.StartDateCareClust = SDCCMAX.StartDateCareClustMAX 
            AND CCT.Person_ID = SDCCMAX.Person_ID
      WHERE CCT.UniqMonthID = '$month_id'
   GROUP BY CCT.Person_ID

# COMMAND ----------

# DBTITLE 1,StartDateCareClustMaxProv
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW StartDateCareClustMaxProv AS 
     SELECT REF.Person_ID
 		   ,REF.OrgIDProv
 		   ,MAX (CC.StartDateCareClust) AS StartDateCareClustMAX
       FROM $db_source.MHS801ClusterTool AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
 		   ON CCT.UniqClustID = CC.UniqClustID
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON CCT.Person_ID = REF.Person_ID 
            AND REF.OrgIDProv = CCT.OrgIDProv
      WHERE CCT.UniqMonthID = '$month_id'
   GROUp BY REF.Person_ID
 		   ,REF.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS803UniqIDMAXProv
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS803UniqIDMAXProv AS 
     SELECT CCT.Person_ID
 		   ,CCT.OrgIDProv
 		   ,MAX (CC.MHS803UniqID) AS MHS803UniqIDMAX			
       FROM $db_source.MHS801ClusterTool	AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
 		   ON CCT.UniqClustID = CC.UniqClustID
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON CCT.Person_ID = REF.Person_ID
            AND CCT.OrgIDProv = REF.OrgIDProv
 INNER JOIN global_temp.StartDateCareClustMaxProv AS SDCCMAX
 		   ON CC.StartDateCareClust = SDCCMAX.StartDateCareClustMAX 
            AND CCT.Person_ID = SDCCMAX.Person_ID 
            AND CCT.OrgIDProv = SDCCMAX.OrgIDProv
      WHERE CCT.UniqMonthID = '$month_id'
   GROUP BY CCT.Person_ID
 		   ,CCT.OrgIDProv

# COMMAND ----------

# DBTITLE 1,ACC33
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC33 AS 
     SELECT 'England' AS BREAKDOWN
 		   ,'England' AS LEVEL
 		   ,'England' AS LEVEL_DESCRIPTION
 		   ,Cluster AS CLUSTER
 		   --,NULL AS CLUSTER_DESCRIPTION
 		   ,'ACC33' AS METRIC
 		   ,COALESCE (COUNT (DISTINCT CCT.Person_ID), 0) AS METRIC_VALUE
       FROM $db_source.MHS801ClusterTool AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
 		   ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON CCT.Person_ID = REF.Person_ID 
 INNER JOIN global_temp.MHS803UniqIDMAX AS MHS803MAX
 		   ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE CCT.UniqMonthID = '$month_id'
   GROUP BY Cluster

# COMMAND ----------

# DBTITLE 1,ACC36
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC36 AS 
     SELECT	'England' AS BREAKDOWN
 			,'England' AS LEVEL
 			,'England' AS LEVEL_DESCRIPTION
 			,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC36' AS METRIC
 			,COALESCE (COUNT (DISTINCT CCT.Person_ID), 0) AS METRIC_VALUE
       FROM	$db_source.MHS801ClusterTool AS CCT
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF
 			ON CCT.Person_ID = REF.Person_ID 
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE	CCT.UniqMonthID = '$month_id'
 			AND CASE WHEN CC.AMHCareClustCodeFin IN ('0', '00', '03', '3', '04', '4', '05', '5', '06', '6', '16', '17', '19', '20', '21')
                                    THEN ADD_MONTHS (CCT.AssToolCompDate,6)
                                    WHEN CC.AMHCareClustCodeFin IN ('01', '1')
                                    THEN DATE_ADD (CCT.AssToolCompDate,84)
                                    WHEN CC.AMHCareClustCodeFin IN ('02', '2')
                                    THEN DATE_ADD (CCT.AssToolCompDate,105)
                                    WHEN CC.AMHCareClustCodeFin IN ('07', '7', '08', '8', '10', '11', '12', '13', '18')
                                    THEN ADD_MONTHS (CCT.AssToolCompDate,12)
                                    WHEN CC.AMHCareClustCodeFin IN ('14', '15')
                                    THEN DATE_ADD (CCT.AssToolCompDate,28)
                                    ELSE NULL
                                    END > '$rp_enddate'
   GROUP BY	Cluster

# COMMAND ----------

# DBTITLE 1,ACC33_PROVIDER
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC33_PROVIDER AS 
     SELECT 'Provider' AS BREAKDOWN
            ,CCT.OrgIDProv as LEVEL
            ,'NONE' as LEVEL_DESCRIPTION
            ,Cluster AS CLUSTER
            --,NULL AS CLUSTER_DESCRIPTION
            ,'ACC33' AS METRIC
            ,COALESCE (COUNT (DISTINCT CCT.Person_ID), 0) AS METRIC_VALUE
       FROM $db_source.MHS801ClusterTool AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
            ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
            ON CCT.Person_ID = REF.Person_ID 
            AND CCT.OrgIDProv = REF.OrgIDProv
 INNER JOIN global_temp.MHS803UniqIDMAXProv AS MHS803MAX
            ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE CCT.UniqMonthID = '$month_id'
   GROUP BY Cluster
            ,CCT.OrgIDProv

# COMMAND ----------

# DBTITLE 1,ACC36_PROVIDER
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC36_PROVIDER AS 
 SELECT 'Provider' AS BREAKDOWN
       ,CCT.OrgIDProv AS LEVEL
       ,'NONE' as LEVEL_DESCRIPTION
       ,Cluster AS CLUSTER
       --,NULL AS CLUSTER_DESCRIPTION
       ,'ACC36' AS METRIC
       ,coalesce (COUNT (DISTINCT CCT.Person_ID), 0) AS METRIC_VALUE
  FROM $db_source.MHS801ClusterTool AS CCT
 INNER JOIN $db_output.MHS803CareCluster_common AS CC
       ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
       ON CCT.Person_ID = REF.Person_ID 
       AND CCT.OrgIDProv = REF.OrgIDProv
 INNER JOIN global_temp.MHS803UniqIDMAXProv AS MHS803MAX
       ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
 WHERE CCT.UniqMonthID = '$month_id'
       AND CASE WHEN CC.AMHCareClustCodeFin IN ('0', '00', '03', '3', '04', '4', '05', '5', '06', '6', '16', '17', '19', '20', '21')
                THEN ADD_MONTHS (CCT.AssToolCompDate,6)
                WHEN CC.AMHCareClustCodeFin IN ('01', '1')
                THEN DATE_ADD (CCT.AssToolCompDate,84)
                WHEN CC.AMHCareClustCodeFin IN ('02', '2')
                THEN DATE_ADD (CCT.AssToolCompDate,105)
                WHEN CC.AMHCareClustCodeFin IN ('07', '7', '08', '8', '10', '11', '12', '13', '18')
                THEN ADD_MONTHS (CCT.AssToolCompDate,12)
                WHEN CC.AMHCareClustCodeFin IN ('14', '15')
                THEN DATE_ADD (CCT.AssToolCompDate,28)
                ELSE NULL
                END > '$rp_enddate'
 GROUP BY Cluster
         ,CCT.OrgIDProv

# COMMAND ----------

# DBTITLE 1,ACC33_CCG
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC33_CCG AS 
     SELECT	'CCG - GP Practice or Residence' AS BREAKDOWN
 			,CAST (CCG.IC_Rec_CCG AS STRING) AS LEVEL
 			,CCG.NAME AS LEVEL_DESCRIPTION
 			,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC33' AS METRIC
 			,COALESCE (COUNT (DISTINCT CCT.Person_ID), 0) AS METRIC_VALUE
       FROM	$db_source.MHS801ClusterTool AS CCT
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF
 			ON CCT.Person_ID = REF.Person_ID 
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
 INNER JOIN  $db_output.MHS001MPI_latest_month_data AS ccg
             ON CCT.Person_ID = ccg.Person_ID
      WHERE	CCT.UniqMonthID = '$month_id'
   GROUP BY	Cluster
 			,CAST (CCG.IC_Rec_CCG AS STRING)
             ,CCG.NAME

# COMMAND ----------

# DBTITLE 1,ACC36_CCG
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC36_CCG AS 
     SELECT	'CCG - GP Practice or Residence' AS BREAKDOWN
 			,CAST (CCG.IC_Rec_CCG AS STRING) AS LEVEL
 			,CCG.NAME AS LEVEL_DESCRIPTION
 			,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC36' AS METRIC
 			,COALESCE (COUNT (DISTINCT CCT.Person_ID), 0) AS METRIC_VALUE
       FROM	$db_source.MHS801ClusterTool AS CCT
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF
 			ON CCT.Person_ID = REF.Person_ID 
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
  LEFT JOIN  $db_output.MHS001MPI_latest_month_data ccg 
             ON CCT.Person_ID = ccg.Person_ID
      WHERE	CCT.UniqMonthID = '$month_id'
 			AND CASE WHEN CC.AMHCareClustCodeFin IN ('0', '00', '03', '3', '04', '4', '05', '5', '06', '6', '16', '17', '19', '20', '21')
                                    THEN ADD_MONTHS (CCT.AssToolCompDate,6)
                                    WHEN CC.AMHCareClustCodeFin IN ('01', '1')
                                    THEN DATE_ADD (CCT.AssToolCompDate,84)
                                    WHEN CC.AMHCareClustCodeFin IN ('02', '2')
                                    THEN DATE_ADD (CCT.AssToolCompDate,105)
                                    WHEN CC.AMHCareClustCodeFin IN ('07', '7', '08', '8', '10', '11', '12', '13', '18')
                                    THEN ADD_MONTHS (CCT.AssToolCompDate,12)
                                    WHEN CC.AMHCareClustCodeFin IN ('14', '15')
                                    THEN DATE_ADD (CCT.AssToolCompDate,28)
                                    ELSE NULL
                                    END > '$rp_enddate'
    GROUP BY	Cluster
 			,CAST (CCG.IC_Rec_CCG AS STRING)
             ,CCG.NAME

# COMMAND ----------

# DBTITLE 1,ACC02
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC02 AS
     SELECT	'England' AS BREAKDOWN
 			,'England' AS LEVEL
 		    ,'England' AS LEVEL_DESCRIPTION
 		    ,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC02' AS METRIC
 			,COALESCE (COUNT (DISTINCT REF.Person_ID), 0) AS METRIC_VALUE
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS701CPACareEpisode AS CPAE
 			ON REF.Person_ID = CPAE.Person_ID 
             AND CPAE.UniqMonthID = '$month_id'
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT
 			ON REF.Person_ID = CCT.Person_ID 
             AND CCT.UniqMonthID = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE	(CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
   GROUP BY	Cluster
     

# COMMAND ----------

# DBTITLE 1,ACC54
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC54 AS
     SELECT	'England' AS BREAKDOWN
 			,'England' AS LEVEL
 			,'England' AS LEVEL_DESCRIPTION
 			,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC54' AS METRIC
 			,CAST (COALESCE (COUNT (DISTINCT REF.Person_ID), 0) AS STRING) AS METRIC_VALUE
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS003AccommStatus AS ACC
 			ON REF.Person_ID = ACC.Person_ID 
             AND ACC.UniqMonthID <= '$month_id'
             AND ACC.SettledAccommodationInd = 'Y'
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT
 			ON REF.Person_ID = CCT.Person_ID 
             AND CCT.UniqMonthID = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE	ACC.AccommodationStatusDate >= DATE_ADD (ADD_MONTHS ('$rp_enddate',-12),1)
 			AND ACC.AccommodationStatusDate <= '$rp_enddate'
   GROUP BY	Cluster
     

# COMMAND ----------

# DBTITLE 1,ACC02_PROVIDER
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC02_PROVIDER AS
 SELECT 'Provider' AS BREAKDOWN
       ,REF.OrgIDProv AS LEVEL
       ,'NONE' AS LEVEL_DESCRIPTION
       ,Cluster AS CLUSTER
       --,NULL AS CLUSTER_DESCRIPTION
       ,'ACC02' AS METRIC
       ,COALESCE (COUNT (DISTINCT REF.Person_ID), 0) AS METRIC_VALUE
 FROM $db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS701CPACareEpisode AS CPAE
       ON REF.Person_ID = CPAE.Person_ID 
       AND CPAE.UniqMonthID = '$month_id'
       AND REF.OrgIDProv = CPAE.OrgIDProv
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT
       ON REF.Person_ID = CCT.Person_ID 
       AND CCT.UniqMonthID = '$month_id'
       AND REF.OrgIDProv = CCT.OrgIDProv
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
       ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
       ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
 WHERE	(CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
 GROUP BY	Cluster
       ,REF.OrgIDProv

# COMMAND ----------

# DBTITLE 1,ACC54_PROVIDER
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC54_PROVIDER AS
                   SELECT	'Provider' AS BREAKDOWN
 				            ,REF.OrgIDProv as LEVEL
                             ,'NONE' AS LEVEL_DESCRIPTION
 				            ,Cluster AS CLUSTER
                             --,NULL AS CLUSTER_DESCRIPTION
                             ,'ACC54' AS METRIC
                             ,CAST (COALESCE (COUNT (DISTINCT REF.Person_ID), 0) AS STRING) AS METRIC_VALUE
                       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 				INNER JOIN  $db_source.MHS003AccommStatus AS ACC
 					        ON REF.Person_ID = ACC.Person_ID 
                             AND ACC.UniqMonthID <= '$month_id'
                             AND ACC.SettledAccommodationInd = 'Y' 
                             AND REF.OrgIDProv = ACC.OrgIDProv
 				INNER JOIN  $db_source.MHS801ClusterTool AS CCT
 					        ON REF.Person_ID = CCT.Person_ID 
                             AND CCT.UniqMonthID = '$month_id'
                             AND REF.OrgIDProv = CCT.OrgIDProv
 				INNER JOIN  $db_output.MHS803CareCluster_common AS CC
                             ON CCT.UniqClustID = CC.UniqClustID 
 				INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
                             ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
                      WHERE	ACC.AccommodationStatusDate >= DATE_ADD (ADD_MONTHS ('$rp_enddate',-12),1)
                             AND ACC.AccommodationStatusDate <= '$rp_enddate'
                   GROUP BY	Cluster
 					        ,REF.OrgIDProv

# COMMAND ----------

# DBTITLE 1,ACC02_CCG
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC02_CCG AS
     SELECT	'CCG - GP Practice or Residence' AS BREAKDOWN
 			,PRSN.IC_Rec_CCG AS LEVEL
 			,PRSN.NAME AS LEVEL_DESCRIPTION
 			,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC02' AS METRIC
 			,COALESCE (COUNT (DISTINCT PRSN.Person_ID), 0) AS METRIC_VALUE
       FROM	$db_output.MHS001MPI_latest_month_data AS PRSN
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF
 			ON PRSN.Person_ID = REF.Person_ID 
 INNER JOIN  $db_source.MHS701CPACareEpisode AS CPAE
 			ON PRSN.Person_ID = CPAE.Person_ID 
             AND CPAE.UniqMonthID = '$month_id'
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT
 			ON PRSN.Person_ID = CCT.Person_ID 
             AND CCT.UniqMonthID = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE	(CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
   GROUP BY	Cluster
 			,PRSN.IC_Rec_CCG
             ,PRSN.NAME

# COMMAND ----------

# DBTITLE 1,ACC54_CCG
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ACC54_CCG AS
     SELECT	'CCG - GP Practice or Residence' AS BREAKDOWN
 			,PRSN.IC_Rec_CCG AS LEVEL
 			,PRSN.NAME AS LEVEL_DESCRIPTION
 			,Cluster AS CLUSTER
 			--,NULL AS CLUSTER_DESCRIPTION
 			,'ACC54' AS METRIC
 			,CAST (COALESCE (COUNT (DISTINCT PRSN.Person_ID), 0) AS STRING) AS METRIC_VALUE
       FROM	$db_output.MHS001MPI_latest_month_data AS PRSN
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF
 			ON PRSN.Person_ID = REF.Person_ID 
 INNER JOIN  $db_source.MHS003AccommStatus AS ACC
 			ON PRSN.Person_ID = ACC.Person_ID 
             AND ACC.UniqMonthID <= '$month_id'  
             AND ACC.SettledAccommodationInd = 'Y'
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT
 			ON PRSN.Person_ID = CCT.Person_ID 
             AND CCT.UniqMonthID = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC
 			ON CCT.UniqClustID = CC.UniqClustID  
 INNER JOIN  global_temp.MHS803UniqIDMAX AS MHS803MAX
 			ON CC.MHS803UniqID = MHS803MAX.MHS803UniqIDMAX
      WHERE	ACC.AccommodationStatusDate >= DATE_ADD (ADD_MONTHS ('$rp_enddate',-12),1)
 			AND ACC.AccommodationStatusDate <= '$rp_enddate'			
   GROUP BY	Cluster
 			,PRSN.IC_Rec_CCG
             ,PRSN.NAME