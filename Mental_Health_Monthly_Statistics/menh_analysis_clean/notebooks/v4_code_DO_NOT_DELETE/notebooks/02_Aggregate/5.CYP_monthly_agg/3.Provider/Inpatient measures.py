# Databricks notebook source
# DBTITLE 1,MHS07a Provider - commented out
 %sql
 
 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly
 
 --MHS07a - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 0-18
 
 -- in both monthly and cahms monthly output
 
 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --            ,'Provider'AS BREAKDOWN
 -- 		   ,OrgIDProv AS PRIMARY_LEVEL
 -- 		   ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 -- 		   ,'NONE' AS SECONDARY_LEVEL
 -- 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 -- 		   ,'MHS07a' AS METRIC
 -- 		   ,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
 --       FROM global_temp.MHS07_Prov_prep  -- prep table in main monthly prep folder
 --      WHERE AGE_GROUP = '00-18'
 --   GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,CYP21 Provider - commented out
 %sql
 --CYP21 - OPEN WARD STAYS (CHILDREN AND YOUNG PEOPLE'S MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD, PROVIDER
 
 -- in both monthly and cahms monthly output
 
 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly
 
 
 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --             ,'Provider' AS BREAKDOWN
 --             ,OrgIDProv AS PRIMARY_LEVEL
 -- 			,'None' AS PRIMARY_LEVEL_DESCRIPTION
 -- 			,'NONE' AS SECONDARY_LEVEL
 -- 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 -- 			,'CYP21' AS METRIC
 -- 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
 -- FROM        $db_output.MHS21_Prov_prep -- prep table in main monthly prep folder
 -- WHERE       CYPServicewsEndRP_temp = true
 -- GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS21a Provider - commented out
 %sql
 
 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly
 
 --MHS21a - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 0-18, PROVIDER
 
 -- in both monthly and cahms monthly output
 
 
 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --             ,'Provider' AS BREAKDOWN
 --             ,OrgIDProv AS PRIMARY_LEVEL
 -- 			,'None' AS PRIMARY_LEVEL_DESCRIPTION
 -- 			,'NONE' AS SECONDARY_LEVEL
 -- 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 -- 			,'MHS21a' AS METRIC
 -- 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
 -- FROM        $db_output.MHS21_Prov_prep  -- prep table in main monthly prep folder
 -- WHERE       AGE_GROUP = '00-18'
 -- GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS24a Provider
 %sql
 --MHS24a - AGED UNDER 16 BED DAYS ON ADULT WARDS IN REPORTING PERIOD
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT  '$rp_startdate' AS REPORTING_PERIOD_START,
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'Provider' AS BREAKDOWN,
           OrgIDProv AS PRIMARY_LEVEL,
           'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
           'NONE' AS SECONDARY_LEVEL,
           'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
           'MHS24a' AS METRIC,
           SUM(METRIC_VALUE) AS METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
         FROM global_temp.BED_DAYS_IN_RP_PROV AS BedDays
        WHERE AgeRepPeriodEnd < 16
          AND WardType IN ('03','06')
     GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS24b Provider
 %sql
 --MHS24b - AGED 16 BED DAYS ON ADULT WARDS IN REPORTING PERIOD
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
              '$status' AS STATUS,
              'Provider' AS BREAKDOWN,
              OrgIDProv AS PRIMARY_LEVEL,
              'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
              'NONE' AS SECONDARY_LEVEL,
              'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
              'MHS24b' AS METRIC,
 	         SUM(METRIC_VALUE) AS METRIC_VALUE
              ,'$db_source' AS SOURCE_DB
         FROM global_temp.BED_DAYS_IN_RP_PROV AS BedDays
        WHERE AgeRepPeriodEnd = 16
          AND WardType IN ('03','06')       
     GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS24c Provider
 %sql
 --MHS24c - AGED 17 BED DAYS ON ADULT WARDS IN REPORTING PERIOD
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
              '$status' AS STATUS,
              'Provider' AS BREAKDOWN,
              OrgIDProv AS PRIMARY_LEVEL,
              'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
              'NONE' AS SECONDARY_LEVEL,
              'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
              'MHS24c' AS METRIC,
 	         SUM(METRIC_VALUE) AS METRIC_VALUE
              ,'$db_source' AS SOURCE_DB
         FROM global_temp.BED_DAYS_IN_RP_PROV AS BedDays
        WHERE AgeRepPeriodEnd = 17
          AND WardType IN ('03','06')
     GROUP BY OrgIDProv