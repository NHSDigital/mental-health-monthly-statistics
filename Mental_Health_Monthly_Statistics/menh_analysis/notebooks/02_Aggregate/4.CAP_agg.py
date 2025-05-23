# Databricks notebook source
# DBTITLE 1,ACC33
 %sql
 /**ACC33 - PEOPLE ASSIGNED TO A CARE CLUSTER BY CLUSTER AT END OF REPORTING PERIOD - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC33

# COMMAND ----------

# DBTITLE 1,ACC36
 %sql
 /**ACC36 - PEOPLE ASSIGNED TO A CARE CLUSTER WITHIN CLUSTER REVIEW PERIOD BY CLUSTER AT END OF REPORTING PERIOD - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,BREAKDOWN
 			,CAST (LEVEL AS STRING) AS LEVEL
 			,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
 			,CLUSTER 
 			,METRIC
 			,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM	global_temp.ACC36

# COMMAND ----------

# DBTITLE 1,ACC37
 %sql
 /**ACC37 - PROPORTION OF PEOPLE ASSIGNED TO A CARE CLUSTER WITHIN CLUSTER REVIEW PERIOD BY CLUSTER AT END OF REPORTING PERIOD**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,ACC33.BREAKDOWN
         ,CAST (ACC33.LEVEL AS STRING) AS LEVEL
         ,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,ACC33.CLUSTER
         ,'ACC37' AS METRIC
         ,CAST (CASE	WHEN COALESCE (ACC33.METRIC_VALUE, 0) = 0 THEN 0
                     ELSE COALESCE(ACC36.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0)*100
                     END AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC33 AS ACC33
 LEFT JOIN  global_temp.ACC36 AS ACC36
         ON ACC33.LEVEL = ACC36.LEVEL 
         AND ACC33.CLUSTER = ACC36.CLUSTER 
         AND ACC33.BREAKDOWN = ACC36.BREAKDOWN

# COMMAND ----------

# DBTITLE 1,ACC33 - Provider
 %sql
 /**ACC33 - PEOPLE ASSIGNED TO A CARE CLUSTER BY CLUSTER AT END OF REPORTING PERIOD, PROVIDER - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC33_PROVIDER

# COMMAND ----------

# DBTITLE 1,ACC36 - Provider
 %sql
 /**ACC36 - PEOPLE ASSIGNED TO A CARE CLUSTER WITHIN CLUSTER REVIEW PERIOD BY CLUSTER AT END OF REPORTING PERIOD, PROVIDER - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END 
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC36_PROVIDER

# COMMAND ----------

# DBTITLE 1,ACC37 - Provider
 %sql
 /**ACC37 - PROPORTION OF PEOPLE ASSIGNED TO A CARE CLUSTER WITHIN CLUSTER REVIEW PERIOD BY CLUSTER AT END OF REPORTING PERIOD, PROVIDER - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,ACC33.BREAKDOWN
         ,CAST (ACC33.LEVEL AS STRING) AS LEVEL
         ,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,ACC33.CLUSTER
         ,'ACC37' AS METRIC
         ,CAST (CASE	WHEN COALESCE (ACC33.METRIC_VALUE, 0) = 0 THEN 0
                     ELSE COALESCE(ACC36.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0)*100
                     END AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC33_PROVIDER AS ACC33
 LEFT JOIN  global_temp.ACC36_PROVIDER AS ACC36
         ON ACC33.LEVEL = ACC36.LEVEL 
         AND ACC33.CLUSTER = ACC36.CLUSTER 
         AND ACC33.BREAKDOWN = ACC36.BREAKDOWN
     

# COMMAND ----------

# DBTITLE 1,ACC33 - CCG
 %sql
 /**ACC33 - PEOPLE ASSIGNED TO A CARE CLUSTER BY CLUSTER AT END OF REPORTING PERIOD, CCG - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM  global_temp.ACC33_CCG

# COMMAND ----------

# DBTITLE 1,ACC36 - CCG
 %sql
 /**ACC36 - PEOPLE ASSIGNED TO A CARE CLUSTER WITHIN CLUSTER REVIEW PERIOD BY CLUSTER AT END OF REPORTING PERIOD, CCG - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC36_CCG

# COMMAND ----------

# DBTITLE 1,ACC37 - CCG
 %sql
 /**ACC37 - PROPORTION OF PEOPLE ASSIGNED TO A CARE CLUSTER WITHIN CLUSTER REVIEW PERIOD BY CLUSTER AT END OF REPORTING PERIOD, CCG**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END 
         ,'$status' AS STATUS
         ,ACC33.BREAKDOWN
         ,CAST (ACC33.LEVEL AS STRING) AS LEVEL
         ,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,ACC33.CLUSTER
         ,'ACC37' AS METRIC
         ,CAST (CASE	WHEN COALESCE (ACC33.METRIC_VALUE, 0) = 0 THEN 0
                     ELSE COALESCE(ACC36.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0)*100
                     END AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC33_CCG AS ACC33
 LEFT JOIN   global_temp.ACC36_CCG AS ACC36
         ON ACC33.LEVEL = ACC36.LEVEL 
         AND ACC33.CLUSTER = ACC36.CLUSTER 
         AND ACC33.BREAKDOWN = ACC36.BREAKDOWN

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

# DBTITLE 1,ACC54
 %sql
 /**ACC54 - People at the end of the RP in settled accommodation - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,BREAKDOWN
 			,CAST (LEVEL AS STRING) AS LEVEL
 			,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
 			,CLUSTER
 			,METRIC
 			,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM	global_temp.ACC54

# COMMAND ----------

# DBTITLE 1,ACC54 - Provider
 %sql
 /**ACC54 - People at the end of the RP in settled accommodation, PROVIDER - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC54_PROVIDER

# COMMAND ----------

# DBTITLE 1,ACC54 - CCG
 %sql
 /**ACC54 - People at the end of the RP in settled accommodation, CCG - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'$status' AS STATUS
         ,BREAKDOWN
         ,CAST (LEVEL AS STRING) AS LEVEL
         ,CAST (LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
         ,CLUSTER
         ,METRIC
         ,CAST (METRIC_VALUE AS STRING) AS METRIC_VALUE
         ,'$db_source' AS SOURCE_DB
         
   FROM	global_temp.ACC54_CCG

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

# DBTITLE 1,ACC62
 %sql
 /**ACC62 - Proportion of people at the end of the Reporting Period who are on CPA, ENGLAND - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,ACC33.BREAKDOWN
 			,CAST (ACC33.LEVEL AS STRING) AS LEVEL
 			,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
 			,ACC33.CLUSTER
 			,'ACC62' AS METRIC
 			,CAST (CASE	WHEN COALESCE (ACC54.METRIC_VALUE, 0) = 0 THEN 0
 					    ELSE COALESCE (ACC54.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0) *100
 						END AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM	global_temp.ACC33 AS ACC33
  LEFT JOIN   global_temp.ACC54 AS ACC54
 			ON ACC33.LEVEL = ACC54.LEVEL 
             AND ACC33.CLUSTER = ACC54.CLUSTER 
             AND ACC33.BREAKDOWN = ACC54.BREAKDOWN

# COMMAND ----------

# DBTITLE 1,ACC62 - CCG
 %sql
 /**ACC62 - Proportion of people at the end of the Reporting Period who are on CPA, CCG - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,ACC33.BREAKDOWN
 			,CAST (ACC33.LEVEL AS STRING) AS LEVEL
 			,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
 			,ACC33.CLUSTER
 			,'ACC62' AS METRIC
 			,CAST (CASE	WHEN COALESCE (ACC54.METRIC_VALUE, 0) = 0 THEN 0
 					    ELSE COALESCE (ACC54.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0) *100
 						END AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM	global_temp.ACC33_CCG AS ACC33
  LEFT JOIN   global_temp.ACC54_CCG AS ACC54
 			ON ACC33.LEVEL = ACC54.LEVEL 
             AND ACC33.CLUSTER = ACC54.CLUSTER 
             AND ACC33.BREAKDOWN = ACC54.BREAKDOWN

# COMMAND ----------

# DBTITLE 1,ACC62 - Provider
 %sql
 /**ACC62 - Proportion of people at the end of the Reporting Period who are on CPA, PROVIDER - FINAL**/

 INSERT INTO $db_output.CAP_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,ACC33.BREAKDOWN
 			,CAST (ACC33.LEVEL AS STRING) AS LEVEL
 			,CAST (ACC33.LEVEL_DESCRIPTION AS STRING) AS LEVEL_DESCRIPTION
 			,ACC33.CLUSTER
 			,'ACC62' AS METRIC
 			,CAST (CASE	WHEN COALESCE (ACC54.METRIC_VALUE, 0) = 0 THEN 0
 					    ELSE COALESCE (ACC54.METRIC_VALUE, 0)  / COALESCE (ACC33.METRIC_VALUE, 0) *100
 						END AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM	global_temp.ACC33_PROVIDER AS ACC33
  LEFT JOIN   global_temp.ACC54_PROVIDER AS ACC54
 			ON ACC33.LEVEL = ACC54.LEVEL 
             AND ACC33.CLUSTER = ACC54.CLUSTER 
             AND ACC33.BREAKDOWN = ACC54.BREAKDOWN