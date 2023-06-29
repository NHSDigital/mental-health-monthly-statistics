# Databricks notebook source
# DBTITLE 1,AMH03 CASSR
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#              '$rp_enddate' AS REPORTING_PERIOD_END,
# 		     '$status' AS STATUS,
# 			 'CASSR' AS BREAKDOWN,
# 			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
# 			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
# 			 'NONE' AS SECONDARY_LEVEL,
# 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
# 			 'AMH03' AS METRIC,
# 			 CAST (coalesce (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
#              '$db_source' AS SOURCE_DB
             
#         FROM $db_output.AMH03_prep
#     GROUP BY COALESCE(CASSR,"UNKNOWN"), COALESCE(CASSR_description,"UNKNOWN")

# COMMAND ----------

# DBTITLE 1,AMH03 CASSR;Provider
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted
#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#              '$rp_enddate' AS REPORTING_PERIOD_END,
# 		     '$status' AS STATUS,
# 			 'CASSR; Provider' AS BREAKDOWN,
# 			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
# 			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
# 			 OrgIDProv AS SECONDARY_LEVEL,
# 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
# 			 'AMH03' AS METRIC,
# 			 CAST (coalesce (CAST(COUNT (DISTINCT C.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
#              '$db_source' AS SOURCE_DB
             
#         FROM $db_output.AMH03_prep AS C
# INNER JOIN global_temp.AMH15_prep_prov AS P
# ON C.Person_ID = P.Person_ID
#     GROUP BY COALESCE(CASSR,"UNKNOWN"), COALESCE(CASSR_description,"UNKNOWN"), OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH14 CASSR
#%sql

#INSERT INTO $db_output.Main_monthly_unformatted

#    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#			,'$status' AS STATUS
#			,'CASSR' AS BREAKDOWN
#			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
#			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
#			,'NONE' AS SECONDARY_LEVEL
#			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#			,'AMH14' AS METRIC
#            ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
#            ,'$db_source' AS SOURCE_DB
            
#       FROM  $db_output.AMH14_prep
#   GROUP BY  COALESCE(CASSR,"UNKNOWN")
#            ,COALESCE(CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,AMH14 CASSR;Provider
#%sql

#INSERT INTO $db_output.Main_monthly_unformatted

#    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#			,'$status' AS STATUS
#			,'CASSR; Provider' AS BREAKDOWN
#			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
#			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
#			,OrgIDProv AS SECONDARY_LEVEL
#			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#			,'AMH14' AS METRIC
#            ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
#            ,'$db_source' AS SOURCE_DB
            
#       FROM  $db_output.AMH14_prep AS C
#  INNER JOIN $db_output.AMH14_prep_prov AS P
#             ON C.PERSON_ID = P.PERSON_ID
#    GROUP BY COALESCE(CASSR,"UNKNOWN")
#            ,COALESCE(CASSR_description,"UNKNOWN")
#            ,OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH15 CASSR
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted

#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'CASSR' AS BREAKDOWN
# 			,COALESCE(AMH15.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
# 			,COALESCE(AMH15.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
# 			,'NONE' AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH15' AS METRIC
#             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#        FROM  $db_output.AMH03_prep AS AMH15
#   LEFT JOIN  $db_output.AMH14_prep AS AMH14
#              ON AMH14.Person_ID = AMH15.Person_ID
#    GROUP BY  COALESCE(AMH15.CASSR,"UNKNOWN")
#             ,COALESCE(AMH15.CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,AMH15 CASSR;Provider
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted

#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'CASSR; Provider' AS BREAKDOWN
# 			,COALESCE(AMH15.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
# 			,COALESCE(AMH15.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
# 			,AMH15.OrgIDProv AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH15' AS METRIC
#             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#        FROM  
#                 (SELECT DISTINCT CASSR, CASSR_description, C.Person_ID, P.OrgIDProv
#                            FROM $db_output.AMH03_prep AS C
#                      INNER JOIN global_temp.AMH15_prep_prov AS P
#                                 ON C.Person_ID = P.Person_ID) AS AMH15 
#        LEFT JOIN                         
#                (SELECT DISTINCT CASSR, C.Person_ID, P.OrgIDProv
#                            FROM $db_output.AMH14_prep AS C
#                      INNER JOIN $db_output.AMH14_prep_prov AS P
#                                 ON C.Person_ID = P.Person_ID) as AMH14
                                          
#                                     ON AMH15.Person_ID = AMH14.Person_ID
#                                     AND AMH15.CASSR = AMH14.CASSR
#                                     AND AMH15.OrgIDProv = AMH14.OrgIDProv
#    GROUP BY  COALESCE(AMH15.CASSR,"UNKNOWN")
#             ,COALESCE(AMH15.CASSR_description,"UNKNOWN")
#             ,AMH15.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH17 CASSR
#%sql

#INSERT INTO $db_output.Main_monthly_unformatted

#    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#			,'$status' AS STATUS
#			,'CASSR' AS BREAKDOWN
#			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
#			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
#			,'NONE' AS SECONDARY_LEVEL
#			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#			,'AMH17' AS METRIC
#            ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
#            ,'$db_source' AS SOURCE_DB
            
#        FROM $db_output.AMH17_prep
#       WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
#	 		 AND EmployStatusRecDate <= '${rp_enddate}'
#    GROUP BY COALESCE(CASSR,"UNKNOWN")
#			,COALESCE(CASSR_description,"UNKNOWN") ;

# COMMAND ----------

# DBTITLE 1,AMH17 CASSR;Provider
#%sql

#INSERT INTO $db_output.Main_monthly_unformatted

#    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#            ,'$rp_enddate' AS REPORTING_PERIOD_END
#			,'$status' AS STATUS
#			,'CASSR; Provider' AS BREAKDOWN
#			,COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL
#			,COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
#			,OrgIDProv AS SECONDARY_LEVEL
#			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
#			,'AMH17' AS METRIC
#            ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
#            ,'$db_source' AS SOURCE_DB
            
#        FROM $db_output.AMH17_prep AS C
#  INNER JOIN $db_output.AMH17_prep_prov AS P
#             ON C.Person_ID = P.Person_ID
#             AND C.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
# 	  		 AND C.EmployStatusRecDate <= '${rp_enddate}'
#             AND P.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
#		 	 AND P.EmployStatusRecDate <= '${rp_enddate}'
#    GROUP BY COALESCE(CASSR,"UNKNOWN")
#			,COALESCE(CASSR_description,"UNKNOWN") 
#            ,OrgIDProv;

# COMMAND ----------

# DBTITLE 1,AMH18 CASSR
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted

#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'CASSR' AS BREAKDOWN
# 			,COALESCE(AMH18.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
# 			,COALESCE(AMH18.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
# 			,'NONE' AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH18' AS METRIC
#             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100	AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM  $db_output.AMH03_prep AS AMH18
#  LEFT JOIN $db_output.AMH17_prep AS AMH17
#  ON AMH17.Person_ID = AMH18.Person_ID
# GROUP BY COALESCE(AMH18.CASSR,"UNKNOWN"), COALESCE(AMH18.CASSR_description,"UNKNOWN")

# COMMAND ----------

# DBTITLE 1,AMH18 CASSR;Provider
# %sql

# INSERT INTO $db_output.Main_monthly_unformatted

#     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
#             ,'$rp_enddate' AS REPORTING_PERIOD_END
# 			,'$status' AS STATUS
# 			,'CASSR; Provider' AS BREAKDOWN
# 			,COALESCE(AMH18.CASSR,"UNKNOWN") AS PRIMARY_LEVEL
# 			,COALESCE(AMH18.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION
# 			,AMH18.OrgIDProv AS SECONDARY_LEVEL
# 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
# 			,'AMH18' AS METRIC
#             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100	AS METRIC_VALUE
#             ,'$db_source' AS SOURCE_DB
            
#       FROM  
#               (SELECT DISTINCT CASSR
#                               ,CASSR_description
#                               ,C.Person_ID
#                               ,P.OrgIDProv 
#                           FROM $db_output.AMH03_prep AS C
#                     INNER JOIN global_temp.AMH18_prep_prov AS P
#                                ON C.Person_ID = P.Person_ID) AS AMH18     
#        LEFT JOIN                             
#                (SELECT DISTINCT CASSR
#                               ,C.Person_ID
#                               ,P.OrgIDProv 
#                          FROM $db_output.AMH17_prep AS C
#                    INNER JOIN $db_output.AMH17_prep_prov AS P
#                               ON C.Person_ID = P.Person_ID) AS AMH17
                
#                                   ON AMH18.Person_ID = AMH17.Person_ID
#                                   AND AMH18.CASSR = AMH17.CASSR
#                                   AND AMH18.OrgIDProv = AMH17.OrgIDProv
#     GROUP BY COALESCE(AMH18.CASSR,"UNKNOWN")
#             ,COALESCE(AMH18.CASSR_description,"UNKNOWN")
#             ,AMH18.OrgIDProv
