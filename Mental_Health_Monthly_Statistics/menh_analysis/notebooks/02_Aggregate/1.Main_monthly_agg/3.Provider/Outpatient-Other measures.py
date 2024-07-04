# Databricks notebook source
# DBTITLE 1,MHS01 Provider
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 		    '$status' AS STATUS,
 		    'Provider' AS BREAKDOWN,
             OrgIDProv	AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
 		    'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS01'	AS METRIC,
        	    CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as int), 0) AS STRING) AS METRIC_VALUE,
             --try_cast(COALESCE (cast(COUNT (DISTINCT Person_ID) as int), 0) AS STRING) as METRIC_VALUE
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp 
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS02 Provider - commented out
# %sql
# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#             '$rp_enddate' AS REPORTING_PERIOD_END,
#                     '$status' AS STATUS,
#                     'Provider' AS BREAKDOWN,
#                      REF.OrgIDProv AS PRIMARY_LEVEL,
#                     'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
#                     'NONE' AS SECONDARY_LEVEL,
#                     'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
#                     'MHS02'  AS METRIC,
#                     CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
#             '$db_source' AS SOURCE_DB
            
#                FROM $db_output.MHS101Referral_open_end_rp AS REF
#          INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
#                  ON REF.Person_ID = CPA.Person_ID
#                 AND REF.OrgIDProv = CPA.OrgIDProv
#            GROUP BY REF.OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH01
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 		    '$status' AS STATUS,
 		    'Provider' AS BREAKDOWN,
             OrgIDProv	AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
 		    'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH01'	AS METRIC,
        	    CAST (COALESCE (cast(COUNT (DISTINCT REF.Person_ID) as int), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF
       WHERE REF.AMHServiceRefEndRP_temp = TRUE
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH02 Provider - commented out
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#             '$rp_enddate' AS REPORTING_PERIOD_END,
#             '$status' AS STATUS,
#             'Provider' AS BREAKDOWN,
#             REF.OrgIDProv AS PRIMARY_LEVEL,
#             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
#             'NONE' AS SECONDARY_LEVEL,
#             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
#             'AMH02'  AS METRIC,
#             CAST (coalesce( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
#             '$db_source' AS SOURCE_DB
            
#        FROM $db_output.MHS101Referral_open_end_rp AS REF
#  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
#             ON REF.Person_ID = CPA.Person_ID
#             AND REF.OrgIDProv = CPA.OrgIDProv
#             AND REF.uniqmonthid = CPA.uniqmonthid
#       WHERE AMHServiceRefEndRP_temp = TRUE
#             AND REF.uniqmonthid = '${month_id}'
#    GROUP BY REF.OrgIDProv   

# COMMAND ----------

# DBTITLE 1,AMH03 Provider - commented out
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#             '$rp_enddate' AS REPORTING_PERIOD_END,
# 		     '$status' AS STATUS,
# 			 'Provider' AS BREAKDOWN,
# 			 OrgIDProv AS PRIMARY_LEVEL,
# 			 'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
# 			 'NONE' AS SECONDARY_LEVEL,
# 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
# 			 'AMH03' AS METRIC,
# 			 CAST (coalesce (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
#             '$db_source' AS SOURCE_DB
            
#         FROM $db_output.AMH03_prep_prov
#     GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH04 - commented out
# %sql
#      INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#             '$rp_enddate' AS REPORTING_PERIOD_END,
# 		        '$status' AS STATUS,
# 		        'Provider' AS BREAKDOWN,
# 			    MPI.OrgIDProv AS PRIMARY_LEVEL,
# 			    'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
# 			    'NONE' AS SECONDARY_LEVEL,
# 			    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
# 			    'AMH04' AS METRIC,
# 			    CAST(COALESCE(CAST(COUNT(DISTINCT MPI.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
#             '$db_source' AS SOURCE_DB
            
#            FROM $db_source.MHS001MPI AS MPI
# 	 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
# 				ON MPI.Person_ID = REF.Person_ID 
# 				AND REF.UniqMonthID = '$month_id' 
#                 AND REF.OrgIDProv = MPI.OrgIDProv
# 	 INNER JOIN $db_source.MHS701CPACareEpisode AS CPAE
# 				ON MPI.Person_ID = CPAE.Person_ID 
# 				AND CPAE.UniqMonthID = '$month_id' 
#                 AND CPAE.OrgIDProv = MPI.OrgIDProv
# 	 LEFT JOIN $db_source.MHS801ClusterTool AS CCT
# 				ON MPI.Person_ID = CCT.Person_ID 
# 				AND CCT.UniqMonthID <= '$month_id' 
# 				AND CCT.AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 				AND CCT.AssToolCompDate <= '$rp_enddate'
#                 AND CCT.OrgIDProv = MPI.OrgIDProv
# 	 LEFT JOIN	(	 SELECT UniqClustID
# 					        ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					   FROM $db_source.MHS802ClusterAssess
# 					  WHERE UniqMonthID <= '$month_id'
# 					    AND CodedAssToolType IN ('979641000000103', '979651000000100', '979661000000102', '979671000000109', '979681000000106', '979691000000108', '979701000000108', '979711000000105', '979721000000104',                                                      '979731000000102', '979741000000106', '979751000000109')
# 					    AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				   GROUP BY UniqClustID
# 				) AS CCAHONOS
# 				ON CCT.UniqClustID = CCAHONOS.UniqClustID
# 	LEFT JOIN   (	SELECT UniqClustID
# 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					 FROM $db_source.MHS802ClusterAssess
# 					WHERE UniqMonthID <= '$month_id'
# 						  AND CodedAssToolType IN ('985071000000105', '985081000000107', '985111000000104', '985121000000105', '985131000000107', '985141000000103')
# 						  AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				 GROUP BY UniqClustID
# 				) AS CCACT
# 				ON CCT.UniqClustID = CCACT.UniqClustID
# 	LEFT JOIN $db_output.MHS101Referral_open_end_rp
# 					AS REFYR
# 					ON MPI.Person_ID = REFYR.Person_ID 
# 					AND REFYR.UniqMonthID <= '$month_id' 
#                     AND REFYR.OrgIDProv = MPI.OrgIDProv
# 	LEFT JOIN $db_source.MHS201CareContact AS CCONT
# 			  ON REFYR.UniqServReqID = CCONT.UniqServReqID 
# 			  AND CCONT.UniqMonthID <= '$month_id'
# 			  AND CCONT.CareContDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 			  AND CCONT.CareContDate <= '$rp_enddate'
# 	LEFT JOIN $db_source.MHS202CareActivity AS CACT
# 			  ON CCONT.UniqCareContID = CACT.UniqCareContID 
# 			  AND CACT.UniqMonthID <= '$month_id'
#     LEFT JOIN (    SELECT UniqCareActID
# 						  ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					FROM $db_source.MHS607CodedScoreAssessmentAct
# 				   WHERE UniqMonthID <= '$month_id'
# 						 AND CodedAssToolType IN ('979641000000103', '979651000000100', '979661000000102', '979671000000109', '979681000000106', '979691000000108', '979701000000108', '979711000000105', '979721000000104',                                                     '979731000000102', '979741000000106', '979751000000109')
# 						 AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				GROUP BY UniqCareActID
# 		      )	AS CSACHONOS
# 			  ON CACT.UniqCareActID = CSACHONOS.UniqCareActID
#     LEFT JOIN (	   SELECT UniqCareActID
# 						  ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					FROM $db_source.MHS607CodedScoreAssessmentAct
# 				   WHERE UniqMonthID <= '$month_id'
# 				   		 AND CodedAssToolType IN ('980761000000107', '980771000000100', '980781000000103', '980791000000101', '980801000000102', '980811000000100', '980821000000106', '980831000000108', '980841000000104',                                                     '980851000000101', '980861000000103', '980871000000105')
# 					     AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 			    GROUP BY UniqCareActID
# 			  )	AS CSACHONOS65
# 			  ON CACT.UniqCareActID = CSACHONOS65.UniqCareActID
# 	LEFT JOIN (   SELECT UniqCareActID
# 					     ,COUNT (DISTINCT CodedAssToolType)	AS CodedAssToolTypeNum
# 				    FROM $db_source.MHS607CodedScoreAssessmentAct
# 			       WHERE UniqMonthID <= '$month_id'
# 						 AND CodedAssToolType IN ('989881000000104', '989931000000107', '989941000000103', '989951000000100', '989961000000102', '989971000000109', '989981000000106', '989991000000108', '990001000000107',                                                     '989891000000102', '989901000000101', '989911000000104', '989921000000105')
# 						 AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				GROUP BY UniqCareActID
# 			  )	AS CSACHONOSCA
# 			  ON CACT.UniqCareActID = CSACHONOSCA.UniqCareActID
# 	LEFT JOIN (   SELECT UniqCareActID
# 						 ,COUNT (DISTINCT CodedAssToolType)	AS CodedAssToolTypeNum
# 				    FROM $db_source.MHS607CodedScoreAssessmentAct
# 				   WHERE UniqMonthID <= '$month_id'
# 						 AND CodedAssToolType IN ('989751000000102', '989801000000109', '989811000000106', '989821000000100', '989831000000103', '989841000000107', '989851000000105', '989861000000108', '989871000000101',                                                     '989761000000104', '989771000000106', '989781000000108', '989791000000105')
# 						 AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				GROUP BY UniqCareActID
# 			  )	AS CSACHONOSCACR
# 			  ON CACT.UniqCareActID = CSACHONOSCACR.UniqCareActID
# 	LEFT JOIN (	 SELECT UniqCareActID
# 						,COUNT (DISTINCT CodedAssToolType)AS CodedAssToolTypeNum
# 				   FROM $db_source.MHS607CodedScoreAssessmentAct
# 			      WHERE UniqMonthID <= '$month_id'
# 						AND CodedAssToolType IN ('989621000000101', '989671000000102', '989681000000100', '989691000000103', '989701000000103', '989711000000101', '989721000000107', '989731000000109', '989741000000100',                                                      '989631000000104', '989641000000108', '989651000000106', '989661000000109')
# 						AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 			   GROUP BY	UniqCareActID
# 			  )	AS CSACHONOSCASR
# 			  ON CACT.UniqCareActID = CSACHONOSCASR.UniqCareActID
# 	LEFT JOIN (	  SELECT UniqCareActID
# 						 ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					FROM $db_source.MHS607CodedScoreAssessmentAct
# 			       WHERE UniqMonthID <= '$month_id'
# 						 AND CodedAssToolType IN ('981391000000108', '981401000000106', '981411000000108', '981421000000102', '981431000000100', '981441000000109', '981451000000107')
# 					     AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 			    GROUP BY UniqCareActID
# 			  )	AS CSACHONOSSEC
# 			  ON CACT.UniqCareActID = CSACHONOSSEC.UniqCareActID
# 	LEFT JOIN (   SELECT UniqServReqID
# 					     ,AssToolCompDate
# 						 ,COUNT (DISTINCT CodedAssToolType)	AS CodedAssToolTypeNum
# 				    FROM $db_source.MHS606CodedScoreAssessmentRefer
# 				   WHERE  UniqMonthID <= '$month_id'
# 						  AND AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 						  AND AssToolCompDate <= '$rp_enddate'
# 						  AND CodedAssToolType IN ('979641000000103', '979651000000100', '979661000000102', '979671000000109', '979681000000106', '979691000000108', '979701000000108', '979711000000105', '979721000000104',                                                      '979731000000102', '979741000000106', '979751000000109')
# 						  AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				 GROUP BY UniqServReqID
# 						  ,AssToolCompDate
# 			  )	AS CSARHONOS
# 			  ON REFYR.UniqServReqID = CSARHONOS.UniqServReqID
# 	LEFT JOIN (    SELECT UniqServReqID
# 						  ,AssToolCompDate
# 						  ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					FROM  $db_source.MHS606CodedScoreAssessmentRefer
# 					WHERE UniqMonthID <= '$month_id'
# 						  AND AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 						  AND AssToolCompDate <= '$rp_enddate'
# 						  AND CodedAssToolType IN ('980761000000107', '980771000000100', '980781000000103', '980791000000101', '980801000000102', '980811000000100', '980821000000106', '980831000000108', '980841000000104',                                                      '980851000000101', '980861000000103', '980871000000105')
# 						  AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				GROUP BY  UniqServReqID
# 						  ,AssToolCompDate
# 			   ) AS CSARHONOS65
# 			   ON REFYR.UniqServReqID = CSARHONOS65.UniqServReqID
# 	LEFT JOIN (  	SELECT UniqServReqID
# 					       ,AssToolCompDate
# 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
# 				     WHERE UniqMonthID <= '$month_id'
# 						   AND AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 						   AND AssToolCompDate <= '$rp_enddate'
# 						   AND CodedAssToolType IN ('989881000000104', '989931000000107', '989941000000103', '989951000000100', '989961000000102', '989971000000109', '989981000000106', '989991000000108', '990001000000107',                                                     '989891000000102', '989901000000101', '989911000000104', '989921000000105')
# 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				  GROUP BY UniqServReqID
# 					       ,AssToolCompDate
# 			  )	AS CSARHONOSCA
# 			  ON REFYR.UniqServReqID = CSARHONOSCA.UniqServReqID
# 	LEFT JOIN (  	SELECT UniqServReqID
# 					       ,AssToolCompDate
# 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
# 				     WHERE UniqMonthID <= '$month_id'
# 						   AND AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 						   AND AssToolCompDate <= '$rp_enddate'
# 						   AND CodedAssToolType IN ('989751000000102', '989801000000109', '989811000000106', '989821000000100', '989831000000103', '989841000000107', '989851000000105', '989861000000108', '989871000000101',                                                     '989761000000104', '989771000000106', '989781000000108', '989791000000105')
# 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				  GROUP BY UniqServReqID
# 					       ,AssToolCompDate
# 		   	   ) AS CSARHONOSCACR
# 			   ON REFYR.UniqServReqID = CSARHONOSCACR.UniqServReqID
# 	LEFT JOIN (  	SELECT UniqServReqID
# 					       ,AssToolCompDate
# 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
# 				     WHERE UniqMonthID <= '$month_id'
# 						   AND AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 						   AND AssToolCompDate <= '$rp_enddate'
# 						   AND CodedAssToolType IN ('989621000000101', '989671000000102', '989681000000100', '989691000000103', '989701000000103', '989711000000101', '989721000000107', '989731000000109', '989741000000100',                                                     '989631000000104', '989641000000108', '989651000000106', '989661000000109')
# 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				 GROUP BY  UniqServReqID
# 				     	   ,AssToolCompDate
# 		       ) AS CSARHONOSCASR
# 			   ON REFYR.UniqServReqID = CSARHONOSCASR.UniqServReqID
# 	LEFT JOIN (  	SELECT UniqServReqID
# 					       ,AssToolCompDate
# 						   ,COUNT (DISTINCT CodedAssToolType) AS CodedAssToolTypeNum
# 					  FROM $db_source.MHS606CodedScoreAssessmentRefer
# 				     WHERE UniqMonthID <= '$month_id'
# 						   AND AssToolCompDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 						   AND AssToolCompDate <= '$rp_enddate'
# 						   AND CodedAssToolType IN ('981391000000108', '981401000000106', '981411000000108', '981421000000102', '981431000000100', '981441000000109', '981451000000107')
# 						   AND PersScore IN ('0', '1', '2', '3', '4', '00', '01', '02', '03', '04')
# 				  GROUP BY UniqServReqID
# 						   ,AssToolCompDate
# 			   ) AS CSARHONOSSEC
# 			   ON REFYR.UniqServReqID = CSARHONOSSEC.UniqServReqID					
#          WHERE MPI.UniqMonthID = '$month_id'
#          AND (CPAE.EndDateCPA IS NULL OR CPAE.EndDateCPA > '$rp_enddate')
#                AND REF.AMHServiceRefEndRP_temp = True
#                AND REFYR.AMHServiceRefEndRP_temp = True
#                AND (CCAHONOS.CodedAssToolTypeNum >= 9 
#                      OR CCACT.CodedAssToolTypeNum >= 6 
#                      OR CSACHONOS.CodedAssToolTypeNum >= 9 
#                      OR CSACHONOS65.CodedAssToolTypeNum >= 9 
#                      OR CSACHONOSCA.CodedAssToolTypeNum = 13 
#                      OR CSACHONOSCACR.CodedAssToolTypeNum = 13 
#                      OR CSACHONOSCASR.CodedAssToolTypeNum = 13 
#                      AND CSACHONOSSEC.CodedAssToolTypeNum = 7 
#                      OR CSARHONOS.CodedAssToolTypeNum >= 9 
#                      OR CSARHONOS65.CodedAssToolTypeNum >= 9 
#                      OR CSARHONOSCA.CodedAssToolTypeNum = 13 
#                      OR CSARHONOSCACR.CodedAssToolTypeNum = 13 
#                      OR CSARHONOSCASR.CodedAssToolTypeNum = 13
#                      AND CSARHONOSSEC.CodedAssToolTypeNum = 7
#                     )
#      GROUP BY MPI.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,CYP01
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 		    '$status' AS STATUS,
 		    'Provider' AS BREAKDOWN,
             OrgIDProv	AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
 		    'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'CYP01'	AS METRIC,
        	    CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM  $db_output.MHS101Referral_open_end_rp AS REF
        WHERE REF.CYPServiceRefEndRP_temp = true
     GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MH01 Provider
 %sql
 --/**MH01 - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'Provider' AS BREAKDOWN
     	    ,OrgIDProv AS PRIMARY_LEVEL
     	    ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MH01_Prov_prep -- prep table in main monthly prep folder
 GROUP BY	OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MH01a Provider
 %sql
 --/**MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18, PROVIDER**/
 -- in both monthly and cahms monthly outputs
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'Provider' AS BREAKDOWN
     	    ,OrgIDProv AS PRIMARY_LEVEL
     	    ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01a' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MH01_Prov_prep -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '00-18'
 GROUP BY	OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH01b Provider
 %sql
 --/**MH01b - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 19-64, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'Provider' AS BREAKDOWN
     	    ,OrgIDProv AS PRIMARY_LEVEL
     	    ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01b' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MH01_Prov_prep -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '19-64'
 GROUP BY	OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH01c Provider
 %sql
 --/**MH01c - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 65 AND OVER, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'Provider' AS BREAKDOWN
     	    ,OrgIDProv AS PRIMARY_LEVEL
     	    ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01c' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MH01_Prov_prep -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '65-120'
 GROUP BY	OrgIDProv

# COMMAND ----------

# DBTITLE 1,LDA01
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 		    '$status' AS STATUS,
 		    'Provider' AS BREAKDOWN,
             OrgIDProv	AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
 		    'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'LDA01'	AS METRIC,
        	    CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM  $db_output.MHS101Referral_open_end_rp AS REF
        WHERE REF.LDAServiceRefEndRP_temp = true
     GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH05 - commented out
# %sql
# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 		    ,'$status' AS STATUS
# 			,'Provider'	AS BREAKDOWN
# 			,REF.OrgIDProv	AS PRIMARY_LEVEL
# 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
# 			,'NONE'	AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH05' AS METRIC
# 			,CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM $db_output.MHS101Referral_open_end_rp	AS REF
#  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
# 			ON REF.Person_ID = CPAE.Person_ID
#             AND CPAE.UniqMonthID = '$month_id'
#             AND REF.OrgIDProv = CPAE.OrgIDProv
#       WHERE REF.AMHServiceRefEndRP_temp = true
# 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
#       GROUP BY REF.OrgIDProv;	

# COMMAND ----------

# DBTITLE 1,AMH06 - commented out
# %sql
# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'Provider'	AS BREAKDOWN
# 			,REF.OrgIDProv	AS PRIMARY_LEVEL
# 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
# 			,'NONE' AS SECONDARY_LEVEL
# 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH06'AS METRIC
# 			,CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE		
#             ,'$db_source' AS SOURCE_DB
            
#        FROM $db_output.MHS101Referral_open_end_rp AS REF
#  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPAE
#             ON REF.Person_ID = CPAE.Person_ID 
#             AND REF.OrgIDProv = CPAE.OrgIDProv       
#  INNER JOIN $db_source.MHS702CPAReview	AS CPAR
# 			ON CPAE.UniqCPAEpisodeID = CPAR.UniqCPAEpisodeID 
#             AND CPAE.OrgIDProv = CPAR.OrgIDProv 
#             AND CPAR.UniqMonthID <= '$month_id' 
# 	  WHERE REF.AMHServiceRefEndRP_temp = true
# 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 			AND CPAR.CPAReviewDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
# 			AND CPAR.CPAReviewDate <= '$rp_enddate'
# 	  GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS13 - Provider
 %sql
 /**MHS13 - PEOPLE IN CONTACT WITH SERVICES AT END OF REPORTING PERIOD WITH ACCOMODATION STATUS RECORDED, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,MPI.OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS13' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT MPI.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		$db_source.mhs001mpi AS MPI
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF
             ON MPI.Person_ID = REF.Person_ID AND MPI.OrgIDProv = REF.OrgIDProv 
 INNER JOIN  global_temp.accomodation_latest AS ACC 
             ON REF.Person_ID = ACC.Person_ID AND MPI.OrgIDProv = ACC.OrgIDProv 
 WHERE       MPI.UniqMonthID = '$month_id'
             AND REF.Person_ID is not NULL
             AND ACC.AccommodationTypeDate <= '$rp_enddate'
             AND ACC.Person_ID is not NULL -- not sure if this is necessary
 GROUP BY	MPI.OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH14
#%sql

#INSERT INTO $db_output.Main_monthly_unformatted

#    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#			,'$status' AS STATUS
#			,'Provider' AS BREAKDOWN
#			,OrgIDProv AS PRIMARY_LEVEL
#			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#			,'NONE' AS SECONDARY_LEVEL
#			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#			,'AMH14' AS METRIC
 #           ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
  #          ,'$db_source' AS SOURCE_DB
            
#       FROM $db_output.AMH14_prep_prov
#   GROUP BY OrgIDProv;          

# COMMAND ----------

# DBTITLE 1,AMH15 - commented out
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted

#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'Provider' AS BREAKDOWN
# 			,AMH15.OrgIDProv AS PRIMARY_LEVEL
# 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
# 			,'NONE' AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH15' AS METRIC
#             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM  $db_output.AMH03_prep_prov AS AMH15
#  LEFT JOIN  $db_output.AMH14_prep_prov AS AMH14
#            ON AMH14.Person_ID = AMH15.Person_ID
#            AND AMH14.OrgIDProv = AMH15.OrgIDProv
#   GROUP BY AMH15.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH17
#%sql
#INSERT INTO $db_output.Main_monthly_unformatted

#    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#			,'$status' AS STATUS
#			,'Provider' AS BREAKDOWN
#			, OrgIDProv AS PRIMARY_LEVEL
#			, 'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#			,'NONE' AS SECONDARY_LEVEL
#			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#			,'AMH17' AS METRIC
 #           ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
  #          ,'$db_source' AS SOURCE_DB
            
   #     FROM $db_output.AMH17_prep_prov
    #   WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
	#		AND EmployStatusRecDate <= '${rp_enddate}'
     # GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH18 - commented out
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted

#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'Provider' AS BREAKDOWN
# 			, AMH18.OrgIDProv AS PRIMARY_LEVEL
# 			, 'NONE' AS PRIMARY_LEVEL_DESCRIPTION
# 			,'NONE' AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH18' AS METRIC
#             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100 AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#        FROM $db_output.AMH03_prep_prov AS AMH18
#   LEFT JOIN $db_output.AMH17_prep_prov AS AMH17
#             ON AMH17.Person_ID = AMH18.Person_ID
#             AND AMH17.OrgIDProv = AMH18.OrgIDProv
#    GROUP BY AMH18.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS16
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
 	       ,'MHS16'	AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 
       FROM $db_output.MHS101Referral_open_end_rp AS REF 
 INNER JOIN $db_source.MHS004EmpStatus AS EMP
 		   ON REF.Person_ID = EMP.Person_ID 
            AND EMP.UniqMonthID <= '$month_id' 
            AND REF.OrgIDProv = EMP.OrgIDProv 
      WHERE EMP.EmployStatusRecDate <= '$rp_enddate'
 		   AND EMP.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
   GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS19
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
 	        ,'MHS19'	AS METRIC
 		    ,CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp AS REF
   LEFT JOIN $db_source.MHS008CarePlanType AS CRS
             ON REF.Person_ID = CRS.Person_ID
             AND CRS.UniqMonthID <= '${month_id}' 
             AND REF.OrgIDProv = CRS.OrgIDProv
             
  --Commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 --  LEFT JOIN $db_source.MHS008CrisisPlan AS CRSold
 -- 		    ON REF.Person_ID = CRSold.Person_ID 
 --             AND CRSold.UniqMonthID <= '${month_id}' 
 --             AND REF.OrgIDProv = CRSold.OrgIDProv
       WHERE 
 --Commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 --(
                      (CarePlanTypeMH = '12' AND CRS.Person_ID IS NOT NULL 
                      AND ((CRS.CarePlanCreatDate <= '${rp_enddate}' 
                      AND CRS.CarePlanCreatDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}',-12),1))
                      OR (CRS.CarePlanLastUpdateDate <= '${rp_enddate}' 
                      AND CRS.CarePlanLastUpdateDate >= DATE_ADD(ADD_MONTHS( '${rp_enddate}', -12),1))))
 --Commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 -- OR 
 --                      (CRSold.Person_ID IS NOT NULL 
 --                      AND ((CRSold.MHCrisisPlanCreatDate <= '${rp_enddate}' 
 --                      AND CRSold.MHCrisisPlanCreatDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)) 
 --                      OR (CRSold.MHCrisisPlanLastUpdateDate <= '${rp_enddate}' 
 --                      AND CRSold.MHCrisisPlanLastUpdateDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1))))
 --             )   
    GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS20
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider'    AS BREAKDOWN
             ,REF.OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE'    AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS20'    AS METRIC
            ,CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING)    AS METRIC_VALUE       
             ,'$db_source' AS SOURCE_DB    
            
      FROM $db_output.MHS101Referral_open_end_rp AS REF
 LEFT JOIN $db_source.MHS604PrimDiag AS PDGN
           ON REF.UniqServReqID = PDGN.UniqServReqID
           AND REF.OrgIDProv = PDGN.OrgIDProv 
           AND ((PDGN.RecordEndDate is null or PDGN.RecordEndDate >= '$rp_enddate') AND PDGN.RecordStartDate <= '$rp_enddate')
 LEFT JOIN $db_source.MHS603ProvDiag  AS PVDGN
           ON REF.UniqServReqID = PVDGN.UniqServReqID 
           AND REF.OrgIDProv = PVDGN.OrgIDProv 
           AND ((PVDGN.RecordEndDate is null or PVDGN.RecordEndDate >= '$rp_enddate') AND PVDGN.RecordStartDate <= '$rp_enddate')
 LEFT JOIN $db_source.MHS605SecDiag AS SDGN
           ON REF.UniqServReqID = SDGN.UniqServReqID 
           AND REF.OrgIDProv = SDGN.OrgIDProv 
           AND ((SDGN.RecordEndDate IS null OR SDGN.RecordEndDate >= '$rp_enddate') AND SDGN.RecordStartDate <= '$rp_enddate')
     LEFT JOIN $db_source.mhs609prescomp AS PCDGN
            ON REF.UniqServReqID = PCDGN.UniqServReqID 
            AND ((PCDGN.RecordEndDate is null or PCDGN.RecordEndDate >= '$rp_enddate') AND PCDGN.RecordStartDate <= '$rp_enddate')
      WHERE (PDGN.Person_ID IS NOT NULL OR PVDGN.Person_ID IS NOT NULL OR SDGN.Person_ID IS NOT NULL OR PCDGN.Person_ID IS NOT NULL)
  GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS23
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 		    ,'Provider' AS BREAKDOWN
             ,OrgIDProv	AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 		    ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH23
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 		    ,'Provider' AS BREAKDOWN
             ,OrgIDProv	AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 		    ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'AMH23' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp
       WHERE AMHServiceRefEndRP_temp = true
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,CYP23
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 		    ,'Provider' AS BREAKDOWN
             ,OrgIDProv	AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 		    ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'CYP23' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.MHS101Referral_open_end_rp
       WHERE CYPServiceRefEndRP_temp = true
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS23a
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,REF.OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23a' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT REF.UniqServReqID) as INT), 0) AS STRING)    AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.MHS101Referral_open_end_rp AS REF
   INNER JOIN $db_output.ServiceTeamType AS SRV    
              ON REF.UniqServReqID = SRV.UniqServReqID
        WHERE SRV.ServTeamTypeRefToMH = 'C02'
              AND SRV.Uniqmonthid = '$month_id'
     GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS23b
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,REF.OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23b' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT REF.UniqServReqID) as INT), 0) AS STRING)    AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.MHS101Referral_open_end_rp AS REF
  
   INNER JOIN $db_output.ServiceTeamType AS SRV    
              ON REF.UniqServReqID = SRV.UniqServReqID
              AND REF.OrgIDProv = SRV.OrgIDProv      
       
   INNER JOIN $db_output.validcodes as vc
             ON vc.tablename = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'crisis_resolution' and vc.type = 'include' and SRV.ServTeamTypeRefToMH = vc.ValidValue  
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
              
     WHERE SRV.Uniqmonthid = '$month_id'
     GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS23c
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,REF.OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23c' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT REF.UniqServReqID) as INT), 0) AS STRING)    AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.MHS101Referral_open_end_rp AS REF
   INNER JOIN $db_output.ServiceTeamType AS SRV    
              ON REF.UniqServReqID = SRV.UniqServReqID
              AND REF.OrgIDProv = SRV.OrgIDProv 
        WHERE SRV.ServTeamTypeRefToMH = 'A17'
              AND SRV.Uniqmonthid = '$month_id'
     GROUP BY REF.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS01 Provider
# %sql
# --MHS01 Provider
# INSERT INTO $db_output.Main_monthly_unformatted_exp
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#            ,'$status' AS STATUS
#             ,'Provider' AS BREAKDOWN
#             ,OrgIDProv AS PRIMARY_LEVEL
#             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#             ,'NONE' AS SECONDARY_LEVEL
#             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS01' AS MEASURE_ID
#           --  ,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE --taken from Provider code for 
#             ,,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
      
# FROM global_temp.MHS01_prep_prov
# GROUP BY OrgIDProv ;


# COMMAND ----------

# DBTITLE 1,MHS07 Provider
# %sql
# --MHS07 Provider
# INSERT INTO $db_output.Main_monthly_unformatted--_exp
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#            ,'$status' AS STATUS
#             ,'Provider' AS BREAKDOWN
#             ,OrgIDProv AS PRIMARY_LEVEL
#             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#             ,'NONE' AS SECONDARY_LEVEL
#             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS07' AS MEASURE_ID
#           --  ,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE --taken from Provider code for 
#             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
      
# FROM global_temp.MHS07_general_prep_prov
# GROUP BY OrgIDProv ;


# COMMAND ----------

# DBTITLE 1,MHS23d Provider
 %sql
 /*** MHS23d ***/
  
 -- Provider
  
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             
             ,'$db_source' AS SOURCE_DB
             
    FROM global_temp.MHS23d_prep_prov
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS23d Provider Age Group
 %sql
 /*** MHS23d ***/
  
 -- Provider; Age Group
  
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
          -- ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING) AS METRIC_VALUE ---? this try_CAST works for the previous group by 
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.MHS23d_prep_prov
    GROUP BY OrgIDProv, AgeGroup,AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS29 Provider
 %sql
 --/**MHS29 - CONTACTS IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29_prep -- prep table in main monthly prep folder
 GROUP BY	OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS29a Provider
 %sql
 --/**MHS29a - CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'C02'
 GROUP BY    OrgIDProv

# COMMAND ----------

 %sql
 --/**MHS29a - CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider; Attendance' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,Attend_Code AS SECONDARY_LEVEL
 			,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'C02'
 GROUP BY    OrgIDProv, Attend_Code, Attend_Name

# COMMAND ----------

# DBTITLE 1,MHS29b Provider
 %sql
 --/**MHS29b - CONTACTS WITH CRISIS RESOLUTION SERVICE OR HOME TREATMENT TEAM IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep REF -- prep table in main monthly prep folder
 INNER JOIN $db_output.validcodes as vc
             ON vc.tablename = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'crisis_resolution' and vc.type = 'include' and REF.ServTeamTypeRefToMH = vc.ValidValue  
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 -- WHERE		ServTeamTypeRefToMH IN ('A02', 'A03')
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS29c Provider
 %sql
 --/**MHS29c - CONTACTS WITH MEMORY SERVICES TEAM IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'A17'
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS29d Provider
 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.Main_monthly_unformatted
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
            ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
            
             ,'$db_source' AS SOURCE_DB
             
 -- FROM        global_temp.MHS29d_prep_prov -- the provider prep is taken out and replaced with MHS29d_prep
 FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    OrgIDProv

# COMMAND ----------

 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.Main_monthly_unformatted
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Attendance' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
            ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
            
             ,'$db_source' AS SOURCE_DB
             
 -- FROM        global_temp.MHS29d_prep_prov -- the provider prep is taken out and replaced with MHS29d_prep
 FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    OrgIDProv, Attend_Code, Attend_Name

# COMMAND ----------

# DBTITLE 1,MHS29d_prep_prov outputs test-to be deleted
# %sql
# --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER**/
# INSERT INTO $db_output.Main_monthly_unformatted_exp
 
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,'Provider' AS BREAKDOWN
#             ,OrgIDProv AS PRIMARY_LEVEL
#             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#             ,'NONE' AS SECONDARY_LEVEL
#             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS29d_using_MHS29dprep_prov' AS MEASURE_ID
#            ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
           
#             ,'$db_source' AS SOURCE_DB
            
# FROM        global_temp.MHS29d_prep_prov -- testing again using the MHS29d_prep_prov view to see the difference in the outputs
# -- FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder
# GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS29d Provider; Age Group
 %sql
 --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER, Age Group**/
  
 INSERT INTO $db_output.Main_monthly_unformatted
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,try_cast (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
              ,'$db_source' AS SOURCE_DB
             
 -- FROM        global_temp.MHS29d_prep_prov -- prep table in main monthly prep folder
 FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    OrgIDProv
             ,AgeGroup
             ,AgeGroupName

# COMMAND ----------

# DBTITLE 0,MHS29d Provider; Age Group test using MHS29d_prep_prov TEST to be deleted in final
# %sql
# --/**MHS29d - CONTACTS IN REPORTING PERIOD, PROVIDER, Age Group**/
 
# INSERT INTO $db_output.Main_monthly_unformatted_exp
 
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
#             ,'$status' AS STATUS
#             ,'Provider; Age Group' AS BREAKDOWN
#             ,OrgIDProv AS PRIMARY_LEVEL
#             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
#             ,AgeGroup AS SECONDARY_LEVEL
#             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
#             ,'MHS29d_using_MHS29dprep_prov' AS MEASURE_ID
#             ,try_cast (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
#              ,'$db_source' AS SOURCE_DB
            
# FROM        global_temp.MHS29d_prep_prov -- prep table in main monthly prep folder
# -- FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder
# GROUP BY    OrgIDProv
#             ,AgeGroup

# COMMAND ----------

# DBTITLE 1,MHS29f
 %sql
 --/**MHS29f - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.Main_monthly_unformatted
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Attendance' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29f' AS MEASURE_ID
             ,try_cast (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS29f_prep
 GROUP BY    OrgIDProv
             ,Attend_Code
             ,Attend_Name

# COMMAND ----------

# DBTITLE 1,MHS30 Provider
 %sql
 --/**MHS30 - ATTENDED CONTACTS IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29_prep -- prep table in main monthly prep folder
 WHERE		AttendStatus IN ('5', '6')
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS30a Provider
 %sql
 --/**MHS30a - ATTENDED CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep -- prep table in main monthly prep folder
 WHERE		AttendStatus IN ('5', '6')
 			AND ServTeamTypeRefToMH = 'C02'
 GROUP BY    OrgIDProv

# COMMAND ----------

 %sql
 --/**MHS30a - ATTENDED CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider; ConsMechanismMH' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,ConsMedUsed AS SECONDARY_LEVEL
 			,CMU AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep -- prep table in main monthly prep folder
 WHERE		AttendStatus IN ('5', '6')
 			AND ServTeamTypeRefToMH = 'C02'
 GROUP BY    OrgIDProv, ConsMedUsed, CMU

# COMMAND ----------

# DBTITLE 1,MHS30b Provider
 %sql
 --/**MHS30b - ATTENDED CONTACTS WITH CRISIS RESOLUTION SERVICE OR HOME TREATMENT TEAM IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep REF -- prep table in main monthly prep folder
 INNER JOIN $db_output.validcodes as vc
             ON vc.tablename = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'crisis_resolution' and vc.type = 'include' and REF.ServTeamTypeRefToMH = vc.ValidValue  
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 WHERE		AttendStatus IN ('5', '6')
 -- 			AND ServTeamTypeRefToMH IN ('A02', 'A03')
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS30c Provider
 %sql
 --/**MHS30c - ATTENDED CONTACTS WITH MEMORY SERVICES TEAM TEAM IN REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS29abc_prov_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'A17'
 			AND AttendStatus IN ('5', '6')
 GROUP BY	OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS30f Provider
 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER**/
  
 INSERT INTO $db_output.Main_monthly_unformatted 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB           
 FROM        $db_output.tmp_mhmab_mhs30f_prep_prov -- prep table in main monthly prep folder
 WHERE       AttendStatus IN ('5', '6')
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS30f Provider; Age Group
 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER, Age Group**/
  
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB          
 FROM        $db_output.tmp_mhmab_mhs30f_prep_prov -- prep table in main monthly prep folder
 WHERE       AttendStatus IN ('5', '6')
 GROUP BY    OrgIDProv
             ,AgeGroup,AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS30f Provider; ConsMechanismMH
 %sql
 --/**MHS30f - CONTACTS IN REPORTING PERIOD, PROVIDER, Consultant Mechanism **/
  
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT 
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; ConsMechanismMH' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB        
 FROM        $db_output.tmp_mhmab_mhs30f_prep_prov -- prep table in main monthly prep folder
 WHERE       AttendStatus IN ('5', '6')
 GROUP BY    OrgIDProv,ConsMedUsed,CMU

# COMMAND ----------

# DBTITLE 1,MHS30h Provider; ConsMechanismMH
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; ConsMechanismMH' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30h' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB            
 FROM        $db_output.tmp_mhmab_mhs30h_prep_prov -- prep table in main monthly prep folder
 WHERE        AttendStatus IN ('5', '6')
 GROUP BY    OrgIDProv
             ,ConsMedUsed
             ,CMU

# COMMAND ----------

# DBTITLE 1,MHS32
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_source.MHS101Referral           
       WHERE UniqMonthID = '$month_id' 
             AND ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate'
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS32c Provider
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider'    AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE'    AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE'    AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32c' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_mhmab_mhs32c_prep_prov
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS32c Provider; Age Group
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group'    AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE'    AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup    AS SECONDARY_LEVEL
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32c' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_mhmab_mhs32c_prep_prov
   GROUP BY OrgIDProv, AgeGroup, AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS32d Provider
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider'    AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE'    AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE'    AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS32d' AS MEASURE_ID
             ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_mhmab_mhs32d_prep_prov
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS33 Provider
 %sql
 /**MHS33 - PEOPLE ASSIGNED TO A CARE CLUSTER AT END OF REPORTING PERIOD, PROVIDER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,MPI.OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS33' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT MPI.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		$db_source.MHS001MPI AS MPI --prep table in Generic prep folder
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT 
             ON MPI.Person_ID = CCT.Person_ID
             AND MPI.OrgIDProv = CCT.OrgIDProv
             AND CCT.uniqmonthid = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC --prep table in CaP Prep folder
 			ON CCT.UniqClustID = CC.UniqClustID 
             AND CCT.OrgIDProv = CC.OrgIDProv
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF --prep table in Generic prep folder
 			ON MPI.Person_ID = REF.Person_ID     
             AND MPI.OrgIDProv = REF.OrgIDProv
    WHERE    MPI.uniqmonthid = '$month_id'
 GROUP BY    MPI.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS57 Provider
 %sql
 /**MHS57 - NUMBER OF PEOPLE DISCHARGED IN THE RP**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS57' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		$db_source.MHS101Referral AS REF	
 WHERE		(ServDischDate >= '$rp_startdate' AND ServDischDate <= '$rp_enddate')
 			AND uniqmonthid = '$month_id'	
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS57b Provider
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
             ,'MHS57b' AS MEASURE_ID 
             ,CAST(COALESCE(COUNT(DISTINCT(Person_ID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB            
 FROM        $db_output.tmp_mhmab_mhs57b_prep_prov
 GROUP BY     OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS57b Provider; Age Group
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL 
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL 
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST(COALESCE(COUNT(DISTINCT(Person_ID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB            
 FROM        $db_output.tmp_mhmab_mhs57b_prep_prov
 GROUP BY     OrgIDProv
             ,AgeGroup, AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS57c Provider
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
             ,'MHS57c' AS MEASURE_ID 
             ,CAST(COALESCE(COUNT(DISTINCT(Person_ID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB            
 FROM        $db_output.tmp_MHMAB_MHS57c_prep_prov
 GROUP BY     OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS58 Provider
 %sql
 /**MHS58 - NUMBER OF MISSED CARE CONTACTS IN THE RP**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS58' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS58_Prep
 GROUP BY    OrgIDProv