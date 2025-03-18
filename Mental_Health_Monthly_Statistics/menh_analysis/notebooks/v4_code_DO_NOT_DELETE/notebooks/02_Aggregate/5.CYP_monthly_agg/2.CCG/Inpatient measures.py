# Databricks notebook source
# DBTITLE 1,MHS07a CCG - commented out
 %sql

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly

 --MHS07a - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 0-18

 -- in both monthly and camhs monthly tables

 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --            ,'$rp_enddate' AS REPORTING_PERIOD_END
 --            ,'$status' AS STATUS
 --            ,'CCG - GP Practice or Residence' AS BREAKDOWN
 -- 		   ,IC_Rec_CCG AS PRIMARY_LEVEL
 -- 		   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 --            ,'NONE' AS SECONDARY_LEVEL
 --            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 --            ,'MHS07a' AS METRIC
 -- 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
 -- FROM       global_temp.MHS07_prep  -- prep table in main monthly prep folder
 -- WHERE      AGE_GROUP = '00-18'
 -- GROUP BY   IC_Rec_CCG
 --            ,NAME  

# COMMAND ----------

# DBTITLE 1,CYP21 CCG - commented out
 %sql
 --CYP21 - OPEN WARD STAYS (CHILDREN AND YOUNG PEOPLE'S MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD

 -- in both monthly and cahms monthly tables

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly


 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --             ,'CCG - GP Practice or Residence' AS BREAKDOWN
 --             ,IC_Rec_CCG AS PRIMARY_LEVEL
 --             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 --             ,'NONE' AS SECONDARY_LEVEL
 --             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 --             ,'CYP21' AS METRIC
 --             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
 -- FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 -- WHERE       CYPServicewsEndRP_temp = true
 -- GROUP BY    IC_Rec_CCG
 --             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS21a CCG - commented out
 %sql

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly

 --MHS21a - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 0-18

 -- in both monthly and camhs monthly tables

 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 -- 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 -- 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 -- 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 -- 	        ,'NONE' AS SECONDARY_LEVEL
 -- 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 -- 	        ,'MHS21a' AS METRIC
 -- 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
 -- FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 -- WHERE		AGE_GROUP = '00-18'
 -- GROUP BY    IC_Rec_CCG
 --             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS24a CCG
 %sql
 --MHS24a - AGED UNDER 16 BED DAYS ON ADULT WARDS IN REPORTING PERIOD
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
          '$rp_enddate' AS REPORTING_PERIOD_END,
          '$status' AS STATUS,
          'CCG - GP Practice or Residence' AS BREAKDOWN,
          IC_REC_CCG AS PRIMARY_LEVEL,
          NAME AS PRIMARY_LEVEL_DESCRIPTION,
          'NONE' AS SECONDARY_LEVEL,
          'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
          'MHS24a' AS METRIC,
 	     SUM(METRIC_VALUE) AS METRIC_VALUE
          ,'$db_source' AS SOURCE_DB
     FROM $db_output.BED_DAYS_IN_RP AS BedDays      
    WHERE AgeRepPeriodEnd < 16
      AND WardType IN ('03','06')
 GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,MHS24b CCG
 %sql
 --MHS24b - AGED 16 BED DAYS ON ADULT WARDS IN REPORTING PERIOD
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
          '$rp_enddate' AS REPORTING_PERIOD_END,
          '$status' AS STATUS,
          'CCG - GP Practice or Residence' AS BREAKDOWN,
          IC_REC_CCG AS PRIMARY_LEVEL,
          NAME AS PRIMARY_LEVEL_DESCRIPTION,
          'NONE' AS SECONDARY_LEVEL,
          'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
          'MHS24b' AS METRIC,
 	     SUM(METRIC_VALUE) AS METRIC_VALUE
          ,'$db_source' AS SOURCE_DB
     FROM $db_output.BED_DAYS_IN_RP AS BedDays      
    WHERE AgeRepPeriodEnd = 16
      AND WardType IN ('03','06')
 GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,MHS24c CCG
 %sql
 --MHS24c - AGED 17 BED DAYS ON ADULT WARDS IN REPORTING PERIOD
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
          '$rp_enddate' AS REPORTING_PERIOD_END,
          '$status' AS STATUS,
          'CCG - GP Practice or Residence' AS BREAKDOWN,
          IC_REC_CCG AS PRIMARY_LEVEL,
          NAME AS PRIMARY_LEVEL_DESCRIPTION,
          'NONE' AS SECONDARY_LEVEL,
          'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
          'MHS24c' AS METRIC,
 	     SUM(METRIC_VALUE) AS METRIC_VALUE
          ,'$db_source' AS SOURCE_DB
     FROM $db_output.BED_DAYS_IN_RP AS BedDays      
    WHERE AgeRepPeriodEnd = 17
      AND WardType IN ('03','06')
 GROUP BY IC_REC_CCG, NAME