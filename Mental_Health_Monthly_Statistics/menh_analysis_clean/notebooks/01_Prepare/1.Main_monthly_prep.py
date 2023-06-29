# Databricks notebook source
 %md
 ##To Fix:
 - MHS29/30 Intermediate Prov
 - AMH15_prep_prov
 - AMH14_prep_prov
 - MH29_Prov_prep

# COMMAND ----------

dbutils.widgets.text("db_souce","db_souce","db_souce")

# COMMAND ----------

 
 %md
 # Main Monthly prep tables:
 - MH01_prep
 - MH01_Prov_prep
 - AMH04_prep
 - MHS401MHActPeriod_Intermediate
 - MHS11_INTERMEDIATE --Materialised
 - MHS10_INTERMEDIATE --Materialised
 - MHS09_INTERMEDIATE  --Materialised
 - MHS11Prov_INTERMEDIATE  --Materialised
 - MHS10Prov_INTERMEDIATE  --Materialised
 - MHS09Prov_INTERMEDIATE  --Materialised
 - MHS07_prep
 - MHS07_Prov_prep
 - AMH15_prep
 - AMH15_prep_prov
 - AMH14_prep
 - AMH14_prep_prov
 - AMH17_prep
 - AMH18_prep
 - MHS21_prep  -- Materialised
 - MHS21_Prov_prep  -- Materialised
 - MHS23_prep
 - MHS23abc_prep
 - MHS23d_prep
 - MHS23d_prep_prov
 - MHS29d_prep
 - MHS29f_prep
 - MHS23
 - MHS01_prep
 - MH29_prep
 - MH29_Prov_prep
 - MH26_ranking
 - AMH59_prep
 - AMH59_prep_prov
 - CCR7071_prep
 - CCR7071_prep_prov
 - CCR7273_prep
 - CCR7273_prep_prov

# COMMAND ----------

# DBTITLE 1,MH01 Intermediate National/CCG
 %sql
 -- MH01 National and CCG prep table
 --/**MH01 - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD**/
 --/**MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18**/
 --/**MH01b - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 19-64**/
 --/**MH01c - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 --DROP VIEW IF EXISTS   $db.MH01_prep;
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MH01_prep AS	  
 SELECT				  MPI.Person_ID
 					  ,IC_Rec_CCG
 					  ,NAME
 					  ,CASE WHEN AgeRepPeriodEnd <= 18 THEN '00-18'
 					  		WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64'
 							WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
 							END AS AGE_GROUP
 FROM				  $db_output.MHS001MPI_latest_month_data AS MPI -- table in generic prep
 INNER JOIN 			  $db_output.MHS101Referral_open_end_rp AS REF -- table in generic prep
 					  ON MPI.Person_ID = REF.Person_ID
 WHERE				  (REF.CYPServiceRefEndRP_temp = TRUE OR REF.AMHServiceRefEndRP_temp = TRUE)
                       

# COMMAND ----------

# DBTITLE 1,MH01 Intermediate Provider
 %sql
 
 -- MH01 Provider prep tables
 -- MH01 - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD
 -- MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18
 -- MH01b - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 19-64
 -- MH01c - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 65 AND OVER
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MH01_Prov_prep AS
 SELECT				   
 MPI.Person_ID
 ,REF.OrgIDProv
 ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 18 THEN '00-18'
 WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64'
 WHEN AgeRepPeriodEnd >=65 THEN '65-120' END AS AGE_GROUP
 FROM  $db_source.MHS001MPI AS MPI 
 INNER JOIN 			  $db_output.MHS101Referral_open_end_rp AS REF  
 					  ON MPI.Person_ID = REF.Person_ID
                       AND MPI.OrgIDProv = REF.OrgIDProv
 WHERE				  (REF.CYPServiceRefEndRP_temp = TRUE OR REF.AMHServiceRefEndRP_temp = TRUE)
                       AND MPI.UniqMonthID = '$month_id'

# COMMAND ----------

 %sql
 SELECT CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 18 THEN '00-18'
 WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64'
 WHEN AgeRepPeriodEnd >=65 THEN '65-120' END AS AGE_GROUP FROM $db_source.MHS001MPI
 limit 1

# COMMAND ----------

# DBTITLE 1,AMH03 National, CCG, CASSR
 %sql
 
 TRUNCATE TABLE $db_output.AMH03_prep;
 
 WITH 
 REFER AS (SELECT Person_id, ROW_NUMBER() OVER (PARTITION BY Person_id ORDER BY Person_id) as rnkr
       FROM $db_output.MHS101Referral_open_end_rp
       WHERE AMHServiceRefEndRP_temp = TRUE),
 
 CPA AS (SELECT Person_id, ROW_NUMBER() OVER (PARTITION BY Person_id ORDER BY Person_Id) as rnkc
        FROM $db_output.MHS701CPACareEpisode_latest)      
 
 INSERT INTO TABLE $db_output.AMH03_prep
 SELECT   PRSN.Person_ID
                 ,CASE 
                    WHEN GENDER = '1' THEN 'MALE' 
                    WHEN GENDER = '2' THEN 'FEMALE' 
                    WHEN GENDER = '9' THEN 'INDETERMINATE' 
                      ELSE 'NOT KNOWN' END 
                        AS GENDER
                 ,PRSN.IC_Rec_CCG
                 ,PRSN.NAME
                 ,CASSR
                 ,CASSR_description
            FROM $db_output.MHS001MPI_latest_month_data AS PRSN
           INNER JOIN REFER ON PRSN.Person_ID = REFER.Person_ID
                 AND REFER.rnkr=1
           INNER JOIN CPA ON CPA.Person_ID = PRSN.Person_ID
                 AND rnkc=1          
           LEFT JOIN global_temp.CASSR_mapping AS CASSR --------------->global_temp.CASSR_mapping is in Generic Prep as a temp view
                 ON PRSN.LADistrictAuth = CASSR.LADistrictAuth
           WHERE PRSN.AgeRepPeriodEnd >= 18
 			AND PRSN.AgeRepPeriodEnd <= 69

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH03_prep'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH03_prep'))

# COMMAND ----------

# DBTITLE 1,AMH03 Provider
 %sql
 
 TRUNCATE TABLE $db_output.AMH03_prep_prov;
 
 INSERT INTO TABLE $db_output.AMH03_prep_prov
 
         SELECT   PRSN.Person_ID
                 ,CASE 
                    WHEN GENDER = '1' THEN 'MALE' 
                    WHEN GENDER = '2' THEN 'FEMALE' 
                    WHEN GENDER = '9' THEN 'INDETERMINATE' 
                      ELSE 'NOT KNOWN' END 
                        AS GENDER
                 ,REF.OrgIDProv
            FROM $db_source.MHS001MPI AS PRSN 
      INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
                 ON PRSN.Person_ID = REF.Person_ID 
                 AND PRSN.OrgIDProv=REF.OrgIDProv
      INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
                 ON REF.Person_ID = CPA.Person_ID
                 AND REF.OrgIDProv=CPA.OrgIDProv              
           WHERE PRSN.AgeRepPeriodEnd >= 18
 			    AND PRSN.AgeRepPeriodEnd <= 69
                 AND AMHServiceRefEndRP_temp = true
                 AND PRSN.UniqMonthID = '$month_id'

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH03_prep_prov'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH03_prep_prov'))

# COMMAND ----------

# DBTITLE 1,AMH04 National and CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH04_prep AS
 
          SELECT MPI.Person_ID
 				,MPI.IC_Rec_CCG
 				,MPI.NAME
 
 		   FROM $db_output.MHS001MPI_latest_month_data AS MPI
 	 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 				ON MPI.Person_ID = REF.Person_ID 
 	 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 				ON MPI.Person_ID = CPAE.Person_ID 
 				AND CPAE.UniqMonthID = '$month_id' 
 	  LEFT JOIN $db_source.MHS801ClusterTool AS CCT
 				ON MPI.Person_ID = CCT.Person_ID 
 				AND CCT.UniqMonthID <= '$month_id' 
 				AND CCT.AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
                 AND CCT.AssToolCompDate <= '$rp_enddate'
                   
 	  LEFT JOIN	(	SELECT	UniqClustID
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
 						  
                           AND   -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
                             AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				
                           AND   
                               (AssToolCompTimestamp is not null AND AssToolCompTimestamp <= '$rp_enddate')
                             
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
 						  
                           AND   -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
                             AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				
                           AND   
                             (AssToolCompTimestamp is not null AND AssToolCompTimestamp <= '$rp_enddate')
                             
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
 						   
                           AND   -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
                             AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				
                           AND  
                             (AssToolCompTimestamp is not null AND AssToolCompTimestamp <= '$rp_enddate')
                             
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
 						   
                           AND   -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
                             AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				
                           AND   
                             (AssToolCompTimestamp is not null AND AssToolCompTimestamp <= '$rp_enddate')
                             
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
 						   
                           AND   -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
                             AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				
                           AND   
                             (AssToolCompTimestamp is not null AND AssToolCompTimestamp <= '$rp_enddate')
                             
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
 						   
                           AND   -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
                             AssToolCompTimestamp >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 				
                           AND  
                              (AssToolCompTimestamp is not null AND AssToolCompTimestamp <= '$rp_enddate')
                             
 						   AND CodedAssToolType IN ('981391000000108', '981401000000106', '981411000000108', '981421000000102', '981431000000100', '981441000000109', '981451000000107')
 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
 				  GROUP BY UniqServReqID
 						   ,AssToolCompTimestamp
 			   ) AS CSARHONOSSEC
 			   ON REFYR.UniqServReqID = CSARHONOSSEC.UniqServReqID
          WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
                AND REF.AMHServiceRefEndRP_temp = true
                AND REFYR.AMHServiceRefEndRP_temp = true
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
                      AND CSARHONOSSEC.CodedAssToolTypeNum = 7)
                     

# COMMAND ----------

# DBTITLE 1,MHS401MHActPeriod_GRD_open_end_rp
 %sql
 
 TRUNCATE TABLE $db_output.MHS401MHActPeriod_GRD_open_end_rp;
 
 INSERT INTO TABLE $db_output.MHS401MHActPeriod_GRD_open_end_rp 
     SELECT	 EndDateMHActLegalStatusClass
             ,EndTimeMHActLegalStatusClass
             ,ExpiryDateMHActLegalStatusClass
             ,ExpiryTimeMHActLegalStatusClass
             ,InactTimeMHAPeriod
             ,LegalStatusClassPeriodEndReason
             ,LegalStatusClassPeriodStartReason
             ,LegalStatusCode
             ,LocalPatientId
             ,MHActLegalStatusClassPeriodId
             ,MHS401UniqID
             ,MentalCat
             ,NHSDLegalStatus
             ,OrgIDProv
             ,Person_ID
             ,RecordEndDate
             ,RecordNumber
             ,RecordStartDate
             ,RowNumber
             ,StartDateMHActLegalStatusClass
             ,StartTimeMHActLegalStatusClass
             ,UniqMHActEpisodeID
             ,UniqMonthID
             ,UniqSubmissionID
 
       FROM	$db_source.MHS401MHActPeriod
      WHERE  UniqMonthID = '$month_id' 
             AND legalstatuscode IN ('35', '36')
      	    AND (EndDateMHActLegalStatusClass IS NULL OR EndDateMHActLegalStatusClass > '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS401MHActPeriod_GRD_open_end_rp'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS401MHActPeriod_GRD_open_end_rp'))

# COMMAND ----------

# DBTITLE 1,MHS11_INTERMEDIATE
 %sql
 /**MHS11 - PEOPLE SUBJECT TO A SHORT TERM ORDER AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 TRUNCATE TABLE $db_output.MHS11_INTERMEDIATE;
 
 INSERT INTO $db_output.MHS11_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
             ,AMHServiceRefEndRP_temp
             ,CYPServiceRefEndRP_temp
             ,LDAServiceRefEndRP_temp
             ,AgeRepPeriodEnd
             ,IC_REC_CCG
             ,NAME
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON REF.Person_ID = STO.Person_ID
 INNER JOIN  $db_output.MHS001MPI_latest_month_data MPI
             ON REF.Person_ID = MPI.Person_ID

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS11_INTERMEDIATE'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS11_INTERMEDIATE'))

# COMMAND ----------

# DBTITLE 1,MHS10_INTERMEDIATE
 %sql
 /**MHS10 - PEOPLE SUBJECT TO A COMMUNITY TREATMENT ORDER OR ON CONDITIONAL DISCHARGE AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 TRUNCATE TABLE $db_output.MHS10_INTERMEDIATE;
 
 INSERT INTO $db_output.MHS10_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
             ,AMHServiceRefEndRP_temp
             ,CYPServiceRefEndRP_temp
             ,LDAServiceRefEndRP_temp
             ,AgeRepPeriodEnd
             ,IC_REC_CCG
             ,NAME
       FROM  $db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND MHA.UniqMonthID = '$month_id'
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 			ON MHA.UniqMHActEpisodeID = CTO.UniqMHActEpisodeID 
             AND CTO.UniqMonthID = '$month_id' 
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 		    ON MHA.UniqMHActEpisodeID = CTOR.UniqMHActEpisodeID 
             AND CTOR.UniqMonthID = '$month_id'  
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.UniqMHActEpisodeID = CD.UniqMHActEpisodeID 
             AND CD.UniqMonthID = '$month_id'
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber
 INNER JOIN  $db_output.MHS001MPI_latest_month_data MPI
             ON REF.Person_ID = MPI.Person_ID
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NOT NULL OR CTOR.Person_ID IS NOT NULL OR CD.UniqMHActEpisodeID IS NOT NULL)
 			AND STO.RecordNumber IS NULL

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS10_INTERMEDIATE'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS10_INTERMEDIATE'))

# COMMAND ----------

# DBTITLE 1,MHS09_INTERMEDIATE
 %sql
 /**MHS09 - PEOPLE SUBJECT TO DETENTION IN HOSPITAL AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 TRUNCATE TABLE $db_output.MHS09_INTERMEDIATE;
 
 INSERT INTO $db_output.MHS09_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
             ,AMHServiceWSEndRP_temp
             ,CYPServiceWSEndRP_temp
             ,LDAServiceWSEndRP_temp            
             ,AgeRepPeriodEnd
             ,IC_REC_CCG
             ,NAME
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND MHA.UniqMonthID = '${month_id}' 
             AND MHA.legalstatuscode IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38')
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 			ON MHA.RecordNumber = CTO.RecordNumber 
             AND CTO.UniqMonthID = '${month_id}'  
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '${rp_enddate}')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 			ON MHA.RecordNumber = CTOR.RecordNumber 
             AND CTOR.UniqMonthID = '${month_id}' 
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '${rp_enddate}')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.RecordNumber = CD.RecordNumber 
             AND CD.UniqMonthID = '${month_id}'  
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '${rp_enddate}')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber             
 INNER JOIN  global_temp.MHS501HospProvSpell_open_end_rp AS HSP 
 			ON REF.UniqServReqID = HSP.UniqServReqID 
 INNER JOIN  $db_output.MHS001MPI_latest_month_data MPI
             ON REF.Person_ID = MPI.Person_ID 
   LEFT JOIN global_temp.MHS502WardStay_Open_End_RP AS WRD
 	        ON HSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '${rp_enddate}')
 			AND (CTO.Person_ID IS NULL AND CTOR.Person_ID IS NULL AND CD.UniqMHActEpisodeID IS NULL)
 			AND STO.RecordNumber IS NULL
             AND MHA.Person_ID IS NOT NULL

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS09_INTERMEDIATE'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS09_INTERMEDIATE'))

# COMMAND ----------

# DBTITLE 1,MHS11Prov_INTERMEDIATE
 %sql
 /**MHS11 - PEOPLE SUBJECT TO A SHORT TERM ORDER AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 TRUNCATE TABLE $db_output.MHS11Prov_INTERMEDIATE;
 
 INSERT INTO $db_output.MHS11Prov_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 			,REF.OrgIDProv
             ,AMHServiceRefEndRP_temp
             ,CYPServiceRefEndRP_temp
             ,LDAServiceRefEndRP_temp
             ,AgeRepPeriodEnd
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON REF.Person_ID = STO.Person_ID 
             AND REF.OrgIDProv = STO.OrgIDProv 
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON REF.Person_ID = MPI.Person_ID
             AND REF.OrgIDProv = MPI.OrgIDProv 
             AND  MPI.UniqMonthID = '$month_id' 

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS11Prov_INTERMEDIATE'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS11Prov_INTERMEDIATE'))

# COMMAND ----------

# DBTITLE 1,MHS10Prov_INTERMEDIATE
 %sql
 /**MHS10 - PEOPLE SUBJECT TO A COMMUNITY TREATMENT ORDER OR ON CONDITIONAL DISCHARGE AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 TRUNCATE table $db_output.MHS10Prov_INTERMEDIATE;
 
 INSERT INTO $db_output.MHS10Prov_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 			,REF.OrgIDProv
             ,AMHServiceRefEndRP_temp
             ,CYPServiceRefEndRP_temp
             ,LDAServiceRefEndRP_temp
             ,AgeRepPeriodEnd
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND REF.OrgIDProv = MHA.OrgIDProv 
             AND MHA.UniqMonthID = '$month_id'
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 			ON MHA.UniqMHActEpisodeID = CTO.UniqMHActEpisodeID 
             AND MHA.OrgIDProv = CTO.OrgIDProv 
             AND CTO.UniqMonthID = '$month_id' 
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 			ON MHA.UniqMHActEpisodeID = CTOR.UniqMHActEpisodeID 
             AND MHA.OrgIDProv = CTOR.OrgIDProv 
             AND CTOR.UniqMonthID = '$month_id'
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.UniqMHActEpisodeID = CD.UniqMHActEpisodeID 
             AND MHA.OrgIDProv = CD.OrgIDProv 
             AND CD.UniqMonthID = '$month_id'
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON REF.Person_ID = MPI.Person_ID
             AND REF.OrgIDProv = MPI.OrgIDProv 
             AND  MPI.UniqMonthID = '$month_id' 
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NOT NULL OR CTOR.Person_ID IS NOT NULL OR CD.UniqMHActEpisodeID IS NOT NULL)
 			AND STO.RecordNumber IS NULL

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS10Prov_INTERMEDIATE'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS10Prov_INTERMEDIATE'))

# COMMAND ----------

# DBTITLE 1,MHS09Prov_INTERMEDIATE
 %sql
 /**MHS09 - PEOPLE SUBJECT TO DETENTION IN HOSPITAL AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 TRUNCATE table $db_output.MHS09Prov_INTERMEDIATE;
 
 INSERT INTO $db_output.MHS09Prov_INTERMEDIATE
     SELECT	DISTINCT REF.Person_ID
 			,REF.RecordNumber
 			,REF.OrgIDProv
             ,CYPServiceWSEndRP_temp
             ,LDAServiceWSEndRP_temp
             ,AMHServiceWSEndRP_temp
             ,AMHServiceRefEndRP_temp
             ,AgeRepPeriodEnd
       FROM	$db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN  $db_source.MHS401MHActPeriod AS MHA
 			ON REF.Person_ID = MHA.Person_ID 
             AND REF.OrgIDProv = MHA.OrgIDProv 
             AND MHA.UniqMonthID = '$month_id'
             AND MHA.legalstatuscode IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38') 
  LEFT JOIN  $db_source.MHS404CommTreatOrder AS CTO
 		    ON MHA.RecordNumber = CTO.RecordNumber 
             AND MHA.OrgIDProv = CTO.OrgIDProv 
             AND CTO.UniqMonthID = '$month_id'
             AND (CTO.EndDateCommTreatOrd IS NULL OR CTO.EndDateCommTreatOrd > '$rp_enddate')
  LEFT JOIN  $db_source.MHS405CommTreatOrderRecall AS CTOR
 			ON MHA.RecordNumber = CTOR.RecordNumber 
             AND MHA.OrgIDProv = CTOR.OrgIDProv 
             AND CTOR.UniqMonthID = '$month_id'
             AND (CTOR.EndDateCommTreatOrdRecall IS NULL OR CTOR.EndDateCommTreatOrdRecall > '$rp_enddate')
  LEFT JOIN  $db_source.MHS403ConditionalDischarge AS CD
 			ON MHA.RecordNumber = CD.RecordNumber 
             AND MHA.OrgIDProv = CD.OrgIDProv 
             AND CD.UniqMonthID = '$month_id'
             AND (CD.EndDateMHCondDisch IS NULL OR CD.EndDateMHCondDisch > '$rp_enddate')
  LEFT JOIN  global_temp.MHS401MHActPeriod_STO_open_end_rp AS STO
 			ON MHA.RecordNumber = STO.RecordNumber 
             AND MHA.OrgIDProv = STO.OrgIDProv 
 INNER JOIN  $db_source.MHS001MPI MPI
             ON REF.Person_ID = MPI.Person_ID
             AND REF.OrgIDProv = MPI.OrgIDProv
             AND MPI.UniqMonthID = '$month_id'
 INNER JOIN  global_temp.MHS501HospProvSpell_open_end_rp AS HSP
 			ON REF.UniqServReqID = HSP.UniqServReqID 
             AND REF.OrgIDProv = HSP.OrgIDProv 
 LEFT JOIN global_temp.MHS502WardStay_Open_End_RP AS WRD
 	   ON HSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID
      WHERE	(MHA.EndDateMHActLegalStatusClass IS NULL OR MHA.EndDateMHActLegalStatusClass > '$rp_enddate')
 			AND (CTO.Person_ID IS NULL AND CTOR.Person_ID IS NULL AND CD.UniqMHActEpisodeID IS NULL)
 			AND STO.RecordNumber IS NULL

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS09Prov_INTERMEDIATE'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS09Prov_INTERMEDIATE'))

# COMMAND ----------

# DBTITLE 1,MHS07 intermediate (CCG and National)
 %sql
 /**MHS07 - PEOPLE WITH A HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS07_prep AS
 SELECT DISTINCT  MPI.Person_ID
                  ,IC_Rec_CCG
                  ,NAME
                  ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 18 THEN '00-18'
                        WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64' 
                        WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
                        END AS AGE_GROUP
 FROM             $db_output.MHS001MPI_latest_month_data AS MPI -- table in generic prep
 INNER JOIN       $db_output.MHS101Referral_open_end_rp AS REF -- table in generic prep
                  ON MPI.Person_ID = REF.Person_ID                 
 INNER JOIN       global_temp.MHS501HospProvSpell_open_end_rp AS HSP -- table in generic prep
                  ON REF.UniqServReqID = HSP.UniqServReqID

# COMMAND ----------

# DBTITLE 1,MHS07 intermediate (Provider)
 %sql
 /**MHS07 - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, PROVIDER**/
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS07_Prov_prep AS
 SELECT DISTINCT   MPI.Person_ID
                   ,MPI.ORGIDProv
                   ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 18 THEN '00-18'
                         WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64' 
                         WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
                         END AS AGE_GROUP
 FROM               $db_source.MHS001MPI AS MPI 
 INNER JOIN         $db_output.MHS101Referral_open_end_rp AS REF -- table in generic prep
                    ON MPI.Person_ID = REF.Person_ID  
                    AND MPI.OrgIDProv = REF.OrgIDProv 
 INNER JOIN         global_temp.MHS501HospProvSpell_open_end_rp AS HSP -- table in generic prep
                    ON REF.UniqServReqID = HSP.UniqServReqID     
                    AND REF.OrgIDProv = HSP.OrgIDProv
                    AND MPI.UniqMonthID = '$month_id'

# COMMAND ----------

# DBTITLE 1,MHS13 National CCG Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS13_Prep AS
 SELECT      MPI.Person_ID
             ,IC_Rec_CCG
             ,NAME
 FROM		$db_output.MHS001MPI_latest_month_data AS MPI-- prep table in generic prep folder
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF-- prep table in generic prep folder
             ON MPI.Person_ID = REF.Person_ID
 INNER JOIN  global_temp.Accomodation_latest AS ACC -- prep table in generic prep folder
             ON REF.Person_ID = ACC.Person_ID 
 WHERE       REF.Person_ID is not NULL
             --AND ACC.Person_ID is not NULL -- not sure if this is necessa

# COMMAND ----------

# DBTITLE 1,AMH15 National, CCG and CASSR
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH15_prep AS
     SELECT MPI.Person_ID
            ,CASE 
                WHEN GENDER = '1' THEN 'MALE' 
                WHEN GENDER = '2' THEN 'FEMALE' 
                WHEN GENDER = '9' THEN 'INDETERMINATE' 
                  ELSE 'NOT KNOWN' END 
                    AS GENDER
            ,MPI.IC_Rec_CCG
            ,MPI.NAME
            ,COALESCE(CASSR,'UNKNOWN') as CASSR
            ,COALESCE(CASSR_description ,'UNKNOWN') as CASSR_description
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 		   ON MPI.Person_ID = CPAE.Person_ID
            AND CPAE.UniqMonthID  = '$month_id'
 LEFT JOIN global_temp.CASSR_mapping AS CASSR
            ON COALESCE(MPI.LADistrictAuth,'UNKNOWN') = CASSR.LADistrictAuth
      WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
 		   AND REF.AMHServiceRefEndRP_temp = true
            AND MPI.AgeRepPeriodEnd >= 18
 		   AND MPI.AgeRepPeriodEnd <= 69		 

# COMMAND ----------

# DBTITLE 1,AMH15 Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH15_prep_prov AS
     SELECT MPI.Person_ID
            ,CASE 
                WHEN GENDER = '1' THEN 'MALE' 
                WHEN GENDER = '2' THEN 'FEMALE' 
                WHEN GENDER = '9' THEN 'INDETERMINATE' 
                  ELSE 'NOT KNOWN' END 
                    AS GENDER
            ,MPI.OrgIDProv            
       FROM $db_source.MHS001MPI AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
            AND MPI.OrgIDProv = REF.OrgIDProv       
 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 		   ON MPI.Person_ID = CPAE.Person_ID
            AND MPI.OrgIDProv = CPAE.OrgIDProv
            AND CPAE.UniqMonthID  = '$month_id'
      WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
 		   AND REF.AMHServiceRefEndRP_temp = true
            AND MPI.AgeRepPeriodEnd >= 18
 		   AND MPI.AgeRepPeriodEnd <= 69
            AND MPI.UniqMonthID  = '$month_id'

# COMMAND ----------

# DBTITLE 1,AMH14 National, CCG and CASSR
 %sql
 
 TRUNCATE TABLE $db_output.AMH14_prep;
 
 INSERT INTO TABLE $db_output.AMH14_prep
         
     SELECT PREP.Person_ID 
            ,GENDER
            ,PREP.IC_Rec_CCG
            ,PREP.NAME  
            ,CASSR
            ,CASSR_description
            ,AccommodationTypeDate
       FROM global_temp.AMH15_prep AS PREP
 INNER JOIN global_temp.Accomodation_latest AS ACC
 		   ON PREP.Person_ID = ACC.Person_ID
            AND ACC.SettledAccommodationInd = 'Y' 
            AND ACC.RANK = '1'
      WHERE ACC.AccommodationTypeDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 		   AND ACC.AccommodationTypeDate <= '$rp_enddate'

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH14_prep'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH14_prep'))

# COMMAND ----------

# DBTITLE 1,AMH14 Provider
 %sql
 
 TRUNCATE TABLE $db_output.AMH14_prep_prov;
 
 INSERT INTO TABLE $db_output.AMH14_prep_prov
         
     SELECT PREP.Person_ID 
            ,GENDER
            ,PREP.OrgIDProv  
       FROM global_temp.AMH15_prep_prov AS PREP
 INNER JOIN global_temp.Accomodation_latest AS ACC
 		   ON PREP.Person_ID = ACC.Person_ID
            AND PREP.OrgIDProv = ACC.OrgIDProv
            AND ACC.SettledAccommodationInd = 'Y' 
            AND ACC.PROV_RANK = '1' 
      WHERE ACC.AccommodationTypeDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 		   AND ACC.AccommodationTypeDate <= '$rp_enddate'

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH14_prep_prov'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH14_prep_prov'))

# COMMAND ----------

# DBTITLE 1,AMH17
 %sql
 
 TRUNCATE TABLE $db_output.AMH17_prep;
 
 INSERT INTO TABLE $db_output.AMH17_prep
         
     SELECT MPI.Person_ID
            ,CASE 
                WHEN GENDER = '1' THEN 'MALE' 
                WHEN GENDER = '2' THEN 'FEMALE' 
                WHEN GENDER = '9' THEN 'INDETERMINATE' 
                  ELSE 'NOT KNOWN' 
                    END AS GENDER
            ,MPI.IC_Rec_CCG
            ,MPI.NAME
            ,EMP.EmployStatusRecDate
            ,COALESCE(CASSR,'UNKNOWN') as CASSR
            ,COALESCE(CASSR_description ,'UNKNOWN') as CASSR_description
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 		   ON MPI.Person_ID = CPAE.Person_ID
            AND CPAE.UniqMonthID  = '${month_id}'
 INNER JOIN global_temp.EMPLOYMENT_LATEST AS EMP
 		   ON MPI.Person_ID = EMP.Person_ID
            AND (EMP.EmployStatus = '01' OR EMP.EmployStatus = '1') 
            AND EMP.RANK = '1'
 LEFT JOIN  global_temp.CASSR_mapping AS CASSR
            ON COALESCE(MPI.LADistrictAuth,'UNKNOWN') = CASSR.LADistrictAuth
      WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '${rp_enddate}')
 		   AND REF.AMHServiceRefEndRP_temp = TRUE
 		   AND MPI.AgeRepPeriodEnd >= 18
 		   AND MPI.AgeRepPeriodEnd <= 69

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH17_prep'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH17_prep'))

# COMMAND ----------

# DBTITLE 1,AMH17 Provider
 %sql
 
 TRUNCATE TABLE $db_output.AMH17_prep_prov;
 
 INSERT INTO TABLE $db_output.AMH17_prep_prov
     SELECT MPI.Person_ID
            ,CASE 
                WHEN GENDER = '1' THEN 'MALE' 
                WHEN GENDER = '2' THEN 'FEMALE' 
                WHEN GENDER = '9' THEN 'INDETERMINATE' 
                  ELSE 'NOT KNOWN' 
                    END AS GENDER
            ,MPI.OrgIDProv
            ,EMP.EmployStatusRecDate
       FROM $db_source.MHS001MPI AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
            AND MPI.OrgIDProv = REF.OrgIDProv
 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 		   ON MPI.Person_ID = CPAE.Person_ID
            AND MPI.OrgIDProv = CPAE.OrgIDProv
            AND MPI.UniqMonthID = CPAE.UniqMonthID
 INNER JOIN global_temp.EMPLOYMENT_LATEST AS EMP
 		   ON MPI.Person_ID = EMP.Person_ID
            AND (EMP.EmployStatus = '01' OR EMP.EmployStatus = '1') 
            AND MPI.OrgIDProv = EMP.OrgIDProv
            AND EMP.PROV_RANK = '1'
      WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '${rp_enddate}')
 		   AND REF.AMHServiceRefEndRP_temp = TRUE
 		   AND MPI.AgeRepPeriodEnd >= 18
 		   AND MPI.AgeRepPeriodEnd <= 69
            AND MPI.UniqMonthID = '${month_id}'

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH17_prep_prov'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH17_prep_prov'))

# COMMAND ----------

# DBTITLE 1,AMH18
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH18_prep AS
     SELECT MPI.Person_ID
            ,MPI.IC_Rec_CCG
            ,MPI.NAME
            ,COALESCE(CASSR,'UNKNOWN') as CASSR
            ,COALESCE(CASSR_description ,'UNKNOWN') as CASSR_description 
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 		   ON MPI.Person_ID = CPAE.Person_ID
            AND CPAE.UniqMonthID  = '${month_id}'
  LEFT JOIN global_temp.CASSR_mapping AS CASSR
            ON COALESCE(MPI.LADistrictAuth,'UNKNOWN') = CASSR.LADistrictAuth
      WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '${rp_enddate}')
 		   AND REF.AMHServiceRefEndRP_temp = TRUE
 		   AND MPI.AgeRepPeriodEnd >= 18
 		   AND MPI.AgeRepPeriodEnd <= 69

# COMMAND ----------

# DBTITLE 1,AMH18 Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH18_prep_prov AS
     SELECT MPI.Person_ID
            ,MPI.OrgIDProv
       FROM $db_source.MHS001MPI AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID
            AND MPI.OrgIDProv = REF.OrgIDProv
 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
 		   ON MPI.Person_ID = CPAE.Person_ID
            AND MPI.OrgIDProv = CPAE.OrgIDProv
            AND MPI.UniqMonthID = CPAE.UniqMonthID
      WHERE (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '${rp_enddate}')
 		   AND REF.AMHServiceRefEndRP_temp = TRUE
 		   AND MPI.AgeRepPeriodEnd >= 18
 		   AND MPI.AgeRepPeriodEnd <= 69
            AND MPI.UniqMonthID = '${month_id}'

# COMMAND ----------

# DBTITLE 1,MHS21/22 Intermediate - Used for AMH21/22 and CYP21
 %sql
 /**MHS21 - OPEN WARD STAYS AT END OF REPORTING PERIOD**/
 -- cyp21 has scripts for both monthly and camhs monthly output tables 
 
 TRUNCATE table $db_output.MHS21_prep;
 
 INSERT INTO $db_output.MHS21_prep
 SELECT DISTINCT	    WRD.UniqWardStayID
                     ,WRD.Person_ID
                     ,IC_Rec_CCG 
                     ,NAME 
                     ,Bed_Type
                     ,WardLocDistanceHome 
                     ,CYPServiceWSEndRP_temp 
                     ,AMHServiceWSEndRP_temp
                     ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 18 THEN '00-18'
                           WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64' 
                           WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
                           END AS AGE_GROUP
 FROM                global_temp.MHS502WardStay_open_end_rp AS WRD -- table in generic prep
 INNER JOIN          $db_output.MHS001MPI_latest_month_data AS MPI -- table in generic prep
                     ON WRD.Person_ID = MPI.Person_ID
 LEFT JOIN           global_temp.unique_bed_types AS BED -- table in generic prep
                     ON WRD.UniqWardStayID = BED.UniqWardStayID

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS21_prep'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS21_prep'))

# COMMAND ----------

# DBTITLE 1,MHS21/22 Intermediate Prov - Used for AMH21/22 and CYP21
 %sql
 /**MHS21 - OPEN WARD STAYS AT END OF REPORTING PERIOD**/
 -- cyp21/mhs21a has scripts for both monthly and camhs monthly output tables - only done monthly output for now
 
 TRUNCATE TABLE $db_output.MHS21_Prov_prep;
 
 INSERT INTO $db_output.MHS21_Prov_prep
 SELECT              WRD.Person_ID
                     ,WRD.OrgIDProv
                     ,WRD.UniqWardStayID
                     ,Bed_Type
                     ,CYPServiceWSEndRP_temp
                     ,AMHServiceWSEndRP_temp
                     ,WardLocDistanceHome
                     ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 18 THEN '00-18'
                           WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64' 
                           WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
                           END AS AGE_GROUP
 FROM                global_temp.MHS502WardStay_open_end_rp AS WRD -- table in generic prep
 INNER JOIN          $db_source.MHS001MPI AS MPI -- table in generic prep
                     ON WRD.Person_ID = MPI.Person_ID
                     AND WRD.OrgIDProv = MPI.OrgIDProv
                     AND MPI.UniqMonthID = '$month_id' 
 LEFT JOIN           global_temp.unique_bed_types AS BED -- table in generic prep
                     ON WRD.UniqWardStayID = BED.UniqWardStayID				

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='MHS21_Prov_prep'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='MHS21_Prov_prep'))

# COMMAND ----------

# DBTITLE 1,MHS23
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS23_prep AS
       SELECT MPI.IC_Rec_CCG
             ,MPI.NAME
             ,REF.UniqServReqID
             ,REF.AMHServiceRefEndRP_temp
             ,REF.CYPServiceRefEndRP_temp
             ,REF.LDAServiceRefEndRP_temp
        FROM $db_output.MHS101Referral_open_end_rp	AS REF	
  INNER JOIN $db_output.MHS001MPI_latest_month_data	AS MPI
 		    ON REF.Person_ID = MPI.Person_ID;	

# COMMAND ----------

# DBTITLE 1,MHS23abc_prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS23abc_prep AS
       SELECT PREP.IC_Rec_CCG
             ,PREP.NAME
             ,SRV.ServTeamTypeRefToMH
             ,PREP.UniqServReqID
        FROM  global_temp.MHS23_prep	AS PREP	
   INNER JOIN global_temp.MHS102ServiceTypeReferredTo AS SRV	
              ON PREP.UniqServReqID = SRV.UniqServReqID
              AND SRV.Uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,MHS23d_prep
 %sql
 --Copied from Main_monthly_prep_TJ
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS23d_prep AS
       SELECT 
       MPI.NAME
 
       ,MPI.IC_Rec_CCG
 
             ,REF.UniqServReqID
             ,REF.AMHServiceRefEndRP_temp
             ,REF.CYPServiceRefEndRP_temp
             ,REF.LDAServiceRefEndRP_temp
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup              
             ,SERV.ServTeamTypeRefToMH
 
  FROM $db_output.MHS101Referral_open_end_rp    AS REF  
 ---------------------------------------------------------------------------------------------------------------  
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data    AS MPI
 --INNER JOIN global_temp.tmp_MHMAB_MHS001MPI_latest_month_data as MPI
             ON REF.Person_ID = MPI.Person_ID
 --INNER JOIN $db_output.tmp_MHMAB_MHS102ServiceTypeReferredTo   AS SERV
 INNER JOIN global_temp.MHS102ServiceTypeReferredTo   AS SERV --from generic prep
 --------------------------------------------------------------------------------------------------------------- 
             ON REF.UniqServReqID = SERV.UniqServReqID
  WHERE      SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');    

# COMMAND ----------

 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS23d_prep_prov AS
       SELECT REF.UniqServReqID
             ,REF.AMHServiceRefEndRP_temp
             ,REF.CYPServiceRefEndRP_temp
             ,REF.LDAServiceRefEndRP_temp
             ,REF.OrgIDProv
              ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                    WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                    ELSE 'UNKNOWN' END as AgeGroup 
        FROM $db_output.MHS101Referral_open_end_rp   AS REF    
   INNER JOIN $db_source.MHS001MPI    AS MPI
              ON REF.Person_ID = MPI.Person_ID
             AND MPI.OrgIDProv = REF.OrgIDProv
             AND MPI.UniqMonthID = REF.UniqMonthID
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo AS SERV
              ON REF.UniqServReqID = SERV.UniqServReqID
   WHERE      SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')    
  
  --OPTIMIZE $db_output.tmp_MHMAB_MHS23d_prep_prov 

# COMMAND ----------

# DBTITLE 1,MHS01_prep


# COMMAND ----------

# DBTITLE 1,MHS07_prep


# COMMAND ----------

# DBTITLE 1,MHS29 Prep - National/CCG/Provider
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS29_prep AS
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_CCG
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'Missing'
                             ELSE COALESCE(CM.Code, 'Invalid')
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'Missing'
                             ELSE COALESCE(CM.Description, 'Invalid')
                             END AS CMU
 FROM                  $db_source.MHS201CareContact AS CC
 INNER JOIN            global_temp.CCG AS CCG 
                       ON CC.Person_ID = CCG.Person_ID
 LEFT JOIN             $db_output.ConsMechanismMH_dim as CM
                       ON CC.ConsMechanismMH = CM.Code
                       and CC.UniqMonthID >= CM.FirstMonth and (CM.LastMonth is null or CC.UniqMonthID <= CM.LastMonth)
 WHERE		   		  CC.UniqMonthID = '$month_id' 
 					  AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,MHS29abc Prep - National and CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS29abc_prep AS
 SELECT                CC.UniqCareContID
                       ,SRV.ServTeamTypeRefToMH
                       ,MPI.PatMRecInRP
                       ,CCG.NAME
 					  ,CCG.IC_Rec_CCG
                       ,CC.attendordnacode
                       ,CC.person_id
 FROM                  $db_source.MHS201CareContact AS CC
 INNER JOIN            $db_source.MHS102ServiceTypeReferredTo AS SRV
                       ON CC.UniqServReqID = SRV.UniqServReqID
                       AND CC.UniqCareProfTeamID = SRV.UniqCareProfTeamID
                       AND SRV.UniqMonthID = '$month_id' 
 INNER JOIN            $db_source.MHS001MPI AS MPI
                       ON CC.Person_ID = MPI.Person_ID
                       AND MPI.UniqMonthID = '$month_id'
 INNER JOIN            global_temp.CCG AS CCG 
                       ON MPI.Person_ID = CCG.Person_ID
 WHERE		   		  CC.UniqMonthID = '$month_id' 
 					  AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,MHS29abc Prep - Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS29abc_prov_prep AS
 SELECT                CC.UniqCareContID
                       ,SRV.ServTeamTypeRefToMH
                       ,CC.attendordnacode
                       ,CC.OrgIDProv
 FROM                  $db_source.MHS201CareContact AS CC
 INNER JOIN            $db_source.MHS102ServiceTypeReferredTo AS SRV
                       ON CC.UniqServReqID = SRV.UniqServReqID
                       AND CC.UniqCareProfTeamID = SRV.UniqCareProfTeamID
                       AND SRV.UniqMonthID = '$month_id' 
 WHERE		   		  CC.UniqMonthID = '$month_id' 
 					  AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate')

# COMMAND ----------

 %md
 - Main_monthly_prep/global_temp.CC001_prep-> Main_monthly_prep/(MHS29d_prep + MHS29f_prep) -> Outpatient_Measures(MHS29d breakdowns + MHS29fbreakdowns)

# COMMAND ----------

# DBTITLE 1,CC0011_PREP added to clean up analyst code that makes the MHS29d_prep, MHS29d_prov_prep,MHS29_f
 
 %sql
 --In the original analyst code, they use the $db_source.MHS201CareContact table itself as primary source table, therefore a long hardcoded list is added to make up the AttendOrDNACode.
 CREATE OR REPLACE GLOBAL TEMP VIEW CC001_Prep as
   SELECT   
   CC.UniqMonthID
  ,CC.MHS201UniqID
  ,CC.UniqCareContID
  ,CC.UniqCareProfTeamID
  ,CC.CareContDate
  ,CC.AttendOrDNACode
  ,dss_corp_ATT.PrimaryCode AS ATT_PrimaryCode
  ,dss_corp_ATT.Description AS ATT_Desc
  ,CC.ConsMechanismMH
  ,dss_corp_CONS.PrimaryCode AS CONSMECH_PrimaryCode
  ,dss_corp_CONS.Description AS CONSMECH_Desc
                       ,CC.Person_ID
                       ,CC.UniqServReqID 
                       ,CC.CareContCancelDate
                       ,CC.OrgIDProv
 FROM $db_source.MHS201CareContact AS CC
 LEFT JOIN (SELECT * from $db_source.datadictionarycodes WHERE ItemName = 'ATTENDED_OR_DID_NOT_ATTEND') dss_corp_ATT
  ON CC.AttendOrDNACode = dss_corp_ATT.PrimaryCode
 LEFT JOIN (SELECT * from $db_source.datadictionarycodes WHERE ItemName = 'CONSULTATION_MEDIUM_USED') dss_corp_CONS
 ON CC.ConsMechanismMH = dss_corp_CONS.PrimaryCode

# COMMAND ----------

# DBTITLE 1,MHS29d_prep
 %sql
 --MHS29d_prep (from CC001_prep) Commented Out-Comment back in after MHS29 is Insert_Lookup_data
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS29d_prep AS
 SELECT               
                       CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_CCG 
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode IN ('2','3','4','7') THEN CC.ATT_Desc ELSE 'N/A' END AS DNA_Reason
                -------------------------------------------------------------------------------------------------------------      
                      ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed 
                  -------------------------------------------------------------------------------------------------------------            
                   
                      ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN' 
                      WHEN CC.CONSMECH_PrimaryCode IS NOT NULL THEN CC.CONSMECH_Desc 
                      WHEN CC.ConsMechanismMH IS NOT NULL AND CC.CONSMECH_PrimaryCode IS NULL THEN 'Invalid' END AS CMU
                    -------------------------------------------------------------------------------------------------------------      
                   ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup       
                   ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'People aged under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN 'People aged 18 to 64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN 'People aged 65 or over'
                             ELSE 'UNKNOWN' END as AgeGroupName       
             ,CASE WHEN CC.ATT_PrimaryCode IS NOT NULL THEN CC.AttendOrDNACode
                      WHEN CC.AttendOrDNACode IS NULL THEN 'UNKOWN'
                      WHEN CC.AttendOrDNACode IS NOT NULL AND CC.ATT_PrimaryCode IS NULL THEN 'Invalid' END AS Attend_Code 
         -------------------------------------------------------------------------------------------------------------                     
            ,CASE WHEN CC.ATT_PrimaryCode IS NOT NULL THEN CC.ATT_Desc ELSE 'Invalid' END AS Attend_Name
             -------------------------------------------------------------------------------------------------------------           
  
  FROM      global_temp.CC001_prep AS CC
  
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
             ON MPI.Person_ID = CC.Person_ID
             
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo as SERV
  --$db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
             AND CC.UniqCareProfTeamID = SERV.UniqCareProfTeamID 
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$month_id'
            
  INNER JOIN global_temp.CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$month_id' 
            AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate')
            AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');

# COMMAND ----------

# DBTITLE 1,MHS29d_prep_prov
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS29d_prep_prov AS
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_CCG
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode IN ('2','3','4','7') THEN CC.ATT_Desc ELSE 'N/A' END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                             
                      ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN' 
                      WHEN CC.CONSMECH_PrimaryCode IS NOT NULL THEN CC.CONSMECH_Desc 
                      WHEN CC.ConsMechanismMH IS NOT NULL AND CC.CONSMECH_PrimaryCode IS NULL THEN 'Invalid' END AS CMU
                       
                    -------------------------------------------------------------------------------------------------------------      
                   ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                             ELSE 'UNKNOWN' END as AgeGroup       
                 ----------------------------------------------------------------------------------------------------------------   
                       ,CASE WHEN CC.ATT_PrimaryCode IS NOT NULL THEN CC.AttendOrDNACode
                      WHEN CC.AttendOrDNACode IS NULL THEN 'UNKOWN'
                      WHEN CC.AttendOrDNACode IS NOT NULL AND CC.ATT_PrimaryCode IS NULL THEN 'Invalid' END AS Attend_Code 
                       ,CASE WHEN CC.ATT_PrimaryCode IS NOT NULL THEN CC.ATT_Desc ELSE 'Invalid' END AS Attend_Name
 
 --new correcred joins for the prep_prov
 FROM      global_temp.CC001_prep AS CC
  
  INNER JOIN $db_source.MHS101Referral AS REF
             ON CC.Person_ID = REF.Person_ID 
            AND REF.UNIQSERVREQID = CC.UNIQSERVREQID
  
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
             ON MPI.Person_ID = CC.Person_ID
 			AND CC.OrgIDPRov = MPI.OrgIDProv    ---added to the prep views
 			AND MPI.UniqMonthID = CC.UniqMonthID -----added to the prep views after a mismatch was identified with the analyst's outputs
 			
             
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo as SERV
  --$db_source.MHS102SERVICETYPEREFERREDTO  AS SERV
             ON REF.UNIQSERVREQID = SERV.UNIQSERVREQID
             AND CC.UniqCareProfTeamID = SERV.UniqCareProfTeamID 
            AND REF.PERSON_ID = SERV.PERSON_ID
            AND SERV.UNIQMONTHID = '$month_id'
            
  INNER JOIN global_temp.CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$month_id' 
            AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate')
            AND SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');
 ------------------------------------------------------------------------------------------------------Old
 -- FROM       global_temp.CC001_prep AS CC
 --  INNER JOIN global_temp.CCG AS CCG 
 --             ON CC.Person_ID = CCG.Person_ID
 --  WHERE      CC.UniqMonthID = '$month_id' 
 --            AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');
 
 
 
 -- CREATE OR REPLACE GLOBAL TEMP VIEW MHS29d_prep_prov AS
 -- SELECT                CC.UniqCareContID
 --                       ,CCG.NAME
 --                       ,CCG.IC_REC_CCG
 --                       ,CC.OrgIDProv
 --                       ,CC.AttendOrDNACode
 --                       ,CC.Person_ID
 --                       ,CC.UniqServReqID
 --                       ,CC.ConsMechanismMH
 --                       ,CC.CareContCancelDate
 --                       ,CC.CareContDate
 --                       ,CASE WHEN CC.AttendOrDNACode IN ('2','3','4','7') THEN CC.ATT_Desc ELSE 'N/A' END AS DNA_Reason
 --                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
 --                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
 --                             ELSE CC.ConsMechanismMH
 --                             END AS ConsMedUsed
 --                     ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
 --                             WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
 --                             WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
 --                             ELSE 'UNKNOWN' END as AgeGroup     
                             
 --                      ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN' 
 --                      WHEN CC.CONSMECH_PrimaryCode IS NOT NULL THEN CC.CONSMECH_Desc 
 --                      WHEN CC.ConsMechanismMH IS NOT NULL AND CC.CONSMECH_PrimaryCode IS NULL THEN 'Invalid' END AS CMU
                       
 --                       ,CASE WHEN CC.ATT_PrimaryCode IS NOT NULL THEN CC.AttendOrDNACode
 --                      WHEN CC.AttendOrDNACode IS NULL THEN 'UNKOWN'
 --                      WHEN CC.AttendOrDNACode IS NOT NULL AND CC.ATT_PrimaryCode IS NULL THEN 'Invalid' END AS Attend_Code 
 --                       ,CASE WHEN CC.ATT_PrimaryCode IS NOT NULL THEN CC.ATT_Desc ELSE 'Invalid' END AS Attend_Name
 
 -- FROM       global_temp.CC001_prep AS CC
 -- INNER JOIN global_temp.CCG AS CCG 
 --             ON CC.Person_ID = CCG.Person_ID
 -- INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 --             ON MPI.Person_ID = CC.Person_ID
 --  WHERE      CC.UniqMonthID = '$month_id' 
 --            AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');

# COMMAND ----------

# DBTITLE 1,MHS29f_prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS29f_prep AS
 SELECT                CC.UniqCareContID
                       ,CCG.NAME
                       ,CCG.IC_REC_CCG
                       ,CC.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,CC.UniqServReqID
                       ,CC.ConsMechanismMH
                       ,CC.CareContCancelDate
                       ,CC.CareContDate
                       ,CASE WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             ELSE 'N/A'
                             END AS DNA_Reason
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH NOT IN ('01','02','04','05','09','10','11','12','13','98') THEN 'Invalid'
                             ELSE CC.ConsMechanismMH
                             END AS ConsMedUsed
                       ,CASE WHEN CC.ConsMechanismMH IS NULL THEN 'UNKNOWN'
                             WHEN CC.ConsMechanismMH = '01' THEN 'Face to face communication'
                             WHEN CC.ConsMechanismMH = '02' THEN 'Telephone'
                             WHEN CC.ConsMechanismMH = '04' THEN 'Talk type for a person unable to speak'
                             WHEN CC.ConsMechanismMH = '05' THEN 'Email'
                             WHEN CC.ConsMechanismMH = '09' THEN 'Text message (asynchronous)'
                             WHEN CC.ConsMechanismMH = '10' THEN 'Instant messaging (synchronous)'
                             WHEN CC.ConsMechanismMH = '11' THEN 'Video consultation'
                             WHEN CC.ConsMechanismMH = '12' THEN 'Message board (asynchronous)'
                             WHEN CC.ConsMechanismMH = '13' THEN 'Chat room (synchronous)'
                             WHEN CC.ConsMechanismMH = '98' THEN 'Other'
                             ELSE 'Invalid'
                             END AS CMU 
                        ,CASE WHEN CC.AttendOrDNACode = 5 THEN 5
                             WHEN CC.AttendOrDNACode = 6 THEN 6
                             WHEN CC.AttendOrDNACode = 2 THEN 2
                             WHEN CC.AttendOrDNACode = 3 THEN 3
                             WHEN CC.AttendOrDNACode = 4 THEN 4
                             WHEN CC.AttendOrDNACode = 7 THEN 7
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Code
                       ,CASE WHEN CC.AttendOrDNACode = 5 THEN 'Attended on time or, if late, before the relevant professional was ready to see the patient'
                             WHEN CC.AttendOrDNACode = 6 THEN 'Arrived late, after the relevant professional was ready to see the patient, but was seen'
                             WHEN CC.AttendOrDNACode = 2 THEN 'Appointment cancelled by, or on behalf of the patient'
                             WHEN CC.AttendOrDNACode = 3 THEN 'Did not attend, no advance warning given'
                             WHEN CC.AttendOrDNACode = 4 THEN 'Appointment cancelled or postponed by the health care provider'
                             WHEN CC.AttendOrDNACode = 7 THEN 'Patient arrived late and could not be seen'
                             WHEN CC.AttendOrDNACode is null THEN 'UNKNOWN'
                             ELSE 'Invalid'
                             END AS Attend_Name
  FROM       $db_source.MHS201CareContact AS CC
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
             ON MPI.Person_ID = CC.Person_ID
  INNER JOIN global_temp.CCG AS CCG 
             ON CC.Person_ID = CCG.Person_ID
  WHERE      CC.UniqMonthID = '$month_id' 
            AND (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate');

# COMMAND ----------

# DBTITLE 1,MHS58 Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS58_Prep AS
 SELECT      A.UniqCareContID
             ,A.NAME
             ,A.IC_REC_CCG
             ,A.OrgIDProv
 FROM        global_temp.MHS29_prep AS A
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON REF.UniqServReqID = A.UniqServReqID
             AND REF.UniqMonthID = '$month_id'
 WHERE		A.AttendOrDNACode IN ('2', '3', '4', '7')

# COMMAND ----------

# DBTITLE 1,AMH59 Intermediate National and CCG 
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH59_prep AS
              SELECT WRD.UniqWardStayID
                    ,CASE WHEN WardLocDistanceHome BETWEEN 0 and 19 THEN '<20'
                          WHEN WardLocDistanceHome BETWEEN 20 AND 49 THEN '20-49' 
                          WHEN WardLocDistanceHome BETWEEN 50 AND 99 THEN '50-99'
                          WHEN WardLocDistanceHome  >=100 THEN '100 or Over' 
                          END AS WardDistanceHome
                     ,MPI.IC_Rec_CCG
                     ,MPI.NAME
                FROM global_temp.MHS502WardStay_open_end_rp 	AS WRD
 	     INNER JOIN global_temp.unique_bed_types AS BED
                     ON WRD.UniqWardStayID = BED.UniqWardStayID
          INNER JOIN $db_output.MHS001MPI_latest_month_data AS MPI
                     ON MPI.Person_ID = WRD.Person_ID 			
              WHERE  WRD.AMHServiceWSEndRP_temp = true
                     --AND BedTypeAdultEndRP = 2 --Official derivation - using analyst version
                     AND Bed_Type = 2

# COMMAND ----------

# DBTITLE 1,AMH59 Intermediate Prov
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW AMH59_prep_prov AS
             SELECT WRD.UniqWardStayID,
                    WRD.OrgIDProv
                    ,CASE WHEN WardLocDistanceHome BETWEEN 0 and 19 THEN '<20'
                        WHEN WardLocDistanceHome BETWEEN 20 AND 49 THEN '20-49' 
                        WHEN WardLocDistanceHome BETWEEN 50 AND 99 THEN '50-99'
                        WHEN WardLocDistanceHome  >=100 THEN '100 or Over'
                        END AS WardDistanceHome				
               FROM global_temp.MHS502WardStay_open_end_rp AS WRD
         INNER JOIN global_temp.unique_bed_types AS BED 
                    ON WRD.UniqWardStayID = BED.UniqWardStayID                  			
              WHERE WRD.AMHServiceWSEndRP_temp= true 
                    --AND BedTypeAdultEndRP = 2 --Official derivation - using analyst version
                    AND Bed_Type = 2

# COMMAND ----------

# DBTITLE 1,CCR70/CCR71 Intermediate National/CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CCR7071_prep AS
              SELECT MPI.IC_Rec_CCG 
                     ,MPI.NAME
                     ,REF.ClinRespPriorityType
                     ,CASE WHEN REF.AgeServReferRecDate BETWEEN 0 and 17 THEN '0-17'
                           WHEN REF.AgeServReferRecDate >=18 THEN '18 and over' 
                           END AS AGE_GROUP
                      ,REF.UniqServReqID
                 FROM $db_output.MHS001MPI_latest_month_data AS MPI
           INNER JOIN $db_source.MHS101Referral AS REF 
                      ON MPI.Person_ID = REF.Person_ID
                      AND REF.UniqMonthID = '$month_id' 
           INNER JOIN $db_source.MHS102ServiceTypeReferredTo AS REFTO
                      ON REF.UniqServReqID = REFTO.UniqServReqID 
                      AND REFTO.UniqMonthID = '$month_id' 
                      
           INNER JOIN $db_output.validcodes as vc
                      ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'CCR7071_prep' and vc.type = 'include' and REFTO.ServTeamTypeRefToMH = vc.ValidValue 
                       and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
                       
                WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
 --                      AND REFTO.ServTeamTypeRefToMH IN ('A02','A03');

# COMMAND ----------

# DBTITLE 1,CCR70/CCR71 Intermediate Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CCR7071_prep_prov AS
              SELECT REF.OrgIDProv
                     ,REF.ClinRespPriorityType
                     ,CASE WHEN REF.AgeServReferRecDate BETWEEN 0 and 17 THEN '0-17'
                           WHEN REF.AgeServReferRecDate >=18 THEN '18 and over' 
                           END AS AGE_GROUP
                      ,REF.UniqServReqID
                 FROM $db_source.MHS101Referral AS REF 
           
           INNER JOIN $db_source.MHS102ServiceTypeReferredTo AS REFTO
                      ON REF.UniqServReqID = REFTO.UniqServReqID 
                      AND REFTO.UniqMonthID = '$month_id' 
                      AND REF.OrgIDProv = REFTO.OrgIDProv
                      
          INNER JOIN $db_output.validcodes as vc
                      ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'CCR7071_prep' and vc.type = 'include' and REFTO.ServTeamTypeRefToMH = vc.ValidValue 
                       and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
                       
                WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
 --                      AND REFTO.ServTeamTypeRefToMH IN ('A02','A03')
                      AND REF.UniqMonthID = '$month_id' ;
                      

# COMMAND ----------

# DBTITLE 1,CCR72/CCR73 Intermediate National/CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CCR7273_prep AS
              SELECT CCR.IC_Rec_CCG 
                     ,CCR.NAME
                     ,CCR.ClinRespPriorityType
                     ,CCR.AGE_GROUP
                     ,CCR.UniqServReqID
                 FROM global_temp.CCR7071_prep AS CCR
           INNER JOIN $db_source.MHS201CareContact CON 
                      ON CCR.UniqServReqID = CON.UniqServReqID 
                      AND CON.UniqMonthID = '$month_id'
 
                WHERE CON.CareContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
                      AND CON.AttendOrDNACode in ('5','6')
                      AND CON.ConsMechanismMH = '01';                  

# COMMAND ----------

# DBTITLE 1,CCR72/CCR73 Intermediate Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CCR7273_prep_prov AS 
              SELECT CCR.OrgIDProv
                     ,CCR.ClinRespPriorityType
                     ,CCR.AGE_GROUP
                     ,CCR.UniqServReqID
                 FROM global_temp.CCR7071_prep_prov AS CCR
           INNER JOIN $db_source.MHS201CareContact CON 
                      ON CCR.UniqServReqID = CON.UniqServReqID 
                      AND CON.UniqMonthID = '$month_id'
                      AND CCR.OrgIDProv = CON.OrgIDProv 
                WHERE CON.CareContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
                      AND CON.AttendOrDNACode in ('5','6')
                      AND CON.ConsMechanismMH = '01';      

# COMMAND ----------

