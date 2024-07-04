# Databricks notebook source
# DBTITLE 1,MHS07 CCG All MHS07 Should be moved to Outpatient
 %sql
 
 /**MHS07 - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'CCG - GP Practice or Residence' AS BREAKDOWN
 		   ,IC_Rec_CCG AS PRIMARY_LEVEL
 		   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS METRIC
 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder
   GROUP BY IC_Rec_CCG
            ,NAME

# COMMAND ----------

# DBTITLE 1,MHS07a CCG
 %sql
 /**MHS07a - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 0-18**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'CCG - GP Practice or Residence' AS BREAKDOWN
 		   ,IC_Rec_CCG AS PRIMARY_LEVEL
 		   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07a' AS METRIC
 		   ,CAST (COALESCE ( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
            
       FROM global_temp.MHS07_prep  -- prep table in main monthly prep folder
      WHERE AGE_GROUP = '00-18'
   GROUP BY IC_Rec_CCG
            ,NAME  

# COMMAND ----------

# DBTITLE 1,MHS07b CCG
 %sql
 /**MHS07b - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'CCG - GP Practice or Residence' AS BREAKDOWN
 		   ,IC_Rec_CCG AS PRIMARY_LEVEL
 		   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07b' AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder
      WHERE AGE_GROUP = '19-64'
   GROUP BY IC_Rec_CCG
            ,NAME

# COMMAND ----------

# DBTITLE 1,MHS07c CCG
 %sql
 /**MHS07c - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'CCG - GP Practice or Residence' AS BREAKDOWN
 		   ,IC_Rec_CCG AS PRIMARY_LEVEL
 		   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07c' AS METRIC
 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder
      WHERE AGE_GROUP = '65-120'
   GROUP BY IC_Rec_CCG
            ,NAME

# COMMAND ----------

# DBTITLE 1,MHS21 CCG
 %sql
 /**MHS21 - OPEN WARD STAYS AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'CCG - GP Practice or Residence' AS BREAKDOWN
             ,IC_Rec_CCG AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS21' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS21a CCG
 %sql
 /**MHS21a - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 0-18**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS21a' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '00-18'
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS21b CCG
 %sql
 /**MHS21b - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 19-64**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS21b' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '19-64'
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS21c CCG
 %sql
 /**MHS21c - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS21c' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '65-120'
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,AMH21 CCG
 %sql
 -- /**AMH21 - OPEN WARD STAYS (ADULT MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH21' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep 
 WHERE       AMHServiceWSEndRP_temp = TRUE
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,CYP21 CCG
 %sql
 /**CYP21 - OPEN WARD STAYS (CHILDREN AND YOUNG PEOPLE'S MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD**/
 -- in both monthly and cahms monthly tables
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'CCG - GP Practice or Residence' AS BREAKDOWN
             ,IC_Rec_CCG AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'CYP21' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE       CYPServiceWSEndRP_temp = TRUE
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,AMH21a CCG
 %sql
 /**AMH21a - OPEN WARD STAYS, ADULT ACUTE MH CARE, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH21a' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep  -- prep table in main monthly prep folder
 WHERE       Bed_Type = 2 
             AND AMHServiceWSEndRP_temp = TRUE
 GROUP BY    IC_Rec_CCG
             ,NAME	

# COMMAND ----------

# DBTITLE 1,AMH21b CCG
 %sql
 /**AMH21b - OPEN WARD STAYS, SPECIALIST ADULT MH SERVICES, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH21b' AS METRIC
 	        ,CAST (IFNULL (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep  -- prep table in main monthly prep folder
 WHERE       Bed_Type = 1 
             AND AMHServiceWSEndRP_temp = TRUE
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS22 CCG
 %sql
 
 /**MHS22 - OPEN WARD STAYS DISTANCE >= 50KM AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep  -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS22a CCG
 %sql
 
 /**MHS22a - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD, AGED 0-18**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22a' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep  -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
             AND Age_group = '00-18'
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS22b CCG
 %sql
 
 /**MHS22b - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22b' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
             AND Age_group = '19-64'
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS22c CCG
 %sql
 
 /**MHS22c - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22c' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
             AND Age_group = '65-120'
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,AMH22a CCG
 %sql
 /**AMH22a - DISTANCE TO TREATMENT >= 50KM OPEN WARD STAYS, ADULT ACUTE MH CARE, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	        ,IC_Rec_CCG AS PRIMARY_LEVEL
 	        ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH22a' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE       Bed_Type = 2 
             AND WardLocDistanceHome >= 50  
             AND AMHServiceWSEndRP_temp = TRUE
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,AMH22b CCG
 %sql
 /**AMH22b - DISTANCE TO TREATMENT >= 50KM OPEN WARD STAYS, SPECIALIST ADULT MH SERVICES, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	       ,IC_Rec_CCG AS PRIMARY_LEVEL
 	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'AMH22b' AS METRIC
 	       ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
            ,'$db_source' AS SOURCE_DB
            
 FROM       $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE      Bed_Type = 1 
            AND WardLocDistanceHome >= 50   
            AND AMHServiceWSEndRP_temp = TRUE 
 GROUP BY   IC_Rec_CCG
            ,NAME

# COMMAND ----------

# DBTITLE 1,MHS24 CCG
 %sql
 --MHS24 - BED DAYS IN REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'CCG - GP Practice or Residence' AS BREAKDOWN,
             IC_REC_CCG AS PRIMARY_LEVEL,
             NAME AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS24' AS METRIC,
 			SUM(METRIC_VALUE) as METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.BED_DAYS_IN_RP AS BedDays      
    GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,MHS25
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 	        '$status' AS STATUS,
 	        'CCG - GP Practice or Residence' AS BREAKDOWN,
 	        WRD.IC_Rec_CCG AS PRIMARY_LEVEL,
 	        WRD.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 	        'NONE' AS SECONDARY_LEVEL,
 	        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 		    'MHS25' AS METRIC,
 		    CAST (COALESCE (WRD.METRIC_VALUE, 0) - COALESCE (HLV.METRIC_VALUE, 0) - COALESCE (LOA.METRIC_VALUE, 0) AS STRING)AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM 
              (SELECT IC_Rec_CCG, NAME
                      ,SUM(METRIC_VALUE) AS METRIC_VALUE 
                 FROM $db_output.BED_DAYS_IN_RP 
             GROUP BY IC_Rec_CCG, NAME) AS WRD
    LEFT JOIN 
              (SELECT IC_Rec_CCG
                      ,SUM(METRIC_VALUE) AS METRIC_VALUE 
                 FROM global_temp.HOME_LEAVE_IN_RP 
             GROUP BY IC_Rec_CCG) AS HLV
                        ON WRD.IC_Rec_CCG = HLV.IC_Rec_CCG
    LEFT JOIN  
              (SELECT IC_Rec_CCG
                      ,SUM(METRIC_VALUE) AS METRIC_VALUE 
                 FROM global_temp.LOA_IN_RP 
             GROUP BY IC_Rec_CCG) AS LOA
                        ON WRD.IC_Rec_CCG = LOA.IC_Rec_CCG;

# COMMAND ----------

# DBTITLE 1,MHS26 CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'CCG - GP Practice or Residence' AS BREAKDOWN
             ,IC_Rec_CCG AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS METRIC
             ,CAST( COALESCE( sum(DateDIFF(CASE  WHEN EndDateDelayDisch IS NULL
                                             THEN DATE_ADD ('$rp_enddate',1)
                                             ELSE EndDateDelayDisch
                                             END
                                             ,case when StartDateDelayDisch < '$rp_startdate' then '$rp_startdate'
                                                   when rnk = 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate'
                                                      then date_add(StartDateDelayDisch,1)
                                                   when rnk <> 1 and startdatedelaydisch  between '$rp_startdate' and '$rp_enddate'
                                                                 and startdatedelaydisch > date_add(lastenddate,1) 
                                                      then date_add(startdatedelaydisch,1)   
                                                   when rnk <> 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate' 
                                                      then StartDateDelayDisch  
                                                   else cast('01/01/1900' as date)
                                              end
                                          )),0) as STRING) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM global_temp.MHS26_ranking ranking
 group by IC_Rec_CCG

# COMMAND ----------

# DBTITLE 1,MHS26 CCG by AttribToIndic
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'CCG - GP Practice or Residence; Delayed discharge attributable to' AS BREAKDOWN
             ,IC_Rec_CCG AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AttribToIndic AS SECONDARY_LEVEL
             ,COALESCE(ref.Description,'UNKNOWN') AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS METRIC
             ,CAST( COALESCE( sum(DateDIFF(CASE  WHEN EndDateDelayDisch IS NULL
                                             THEN DATE_ADD ('$rp_enddate',1)
                                             ELSE EndDateDelayDisch
                                             END
                                             ,case when StartDateDelayDisch < '$rp_startdate' then '$rp_startdate'
                                                   when rnk = 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate'
                                                      then date_add(StartDateDelayDisch,1)
                                                   when rnk <> 1 and startdatedelaydisch  between '$rp_startdate' and '$rp_enddate'
                                                                 and startdatedelaydisch > date_add(lastenddate,1) 
                                                      then date_add(startdatedelaydisch,1)   
                                                   when rnk <> 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate' 
                                                      then StartDateDelayDisch  
                                                   else cast('01/01/1900' as date)
                                              end
                                          )),0) as STRING) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM global_temp.MHS26_ranking ranking
 left outer join $db_output.DelayedDischDim ref
 on ranking.AttribToIndic = ref.Code and ref.key = 'att'
 and '$month_id' >= ref.FirstMonth and (ref.LastMonth is null or '$month_id' <= ref.LastMonth)
 
 group by ranking.IC_Rec_CCG,AttribToIndic,ref.Description

# COMMAND ----------

# DBTITLE 1,MHS26 CCG by DelayDischReason
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'CCG - GP Practice or Residence; Delayed discharge reason' AS BREAKDOWN
             ,IC_Rec_CCG AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,DelayDischReason AS SECONDARY_LEVEL
             ,COALESCE(ref.Description,'UNKNOWN') AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS METRIC
             ,CAST( COALESCE( sum(DateDIFF(CASE  WHEN EndDateDelayDisch IS NULL
                                             THEN DATE_ADD ('$rp_enddate',1)
                                             ELSE EndDateDelayDisch
                                             END
                                             ,case when StartDateDelayDisch < '$rp_startdate' then '$rp_startdate'
                                                   when rnk = 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate'
                                                      then date_add(StartDateDelayDisch,1)
                                                   when rnk <> 1 and startdatedelaydisch  between '$rp_startdate' and '$rp_enddate'
                                                                 and startdatedelaydisch > date_add(lastenddate,1) 
                                                      then date_add(startdatedelaydisch,1)   
                                                   when rnk <> 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate' 
                                                      then StartDateDelayDisch  
                                                   else cast('01/01/1900' as date)
                                              end
                                          )),0) as STRING) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM global_temp.MHS26_ranking ranking
 left outer join $db_output.DelayedDischDim ref
 on ranking.DelayDischReason = ref.Code and ref.key = 'reason'
 and '$month_id' >= ref.FirstMonth and (ref.LastMonth is null or '$month_id' <= ref.LastMonth)
 
 group by ranking.IC_Rec_CCG,DelayDischReason,ref.Description

# COMMAND ----------

# DBTITLE 1,MHS27
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	       ,MPI.IC_Rec_CCG AS PRIMARY_LEVEL
 	       ,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		   ,'MHS27' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (UniqHospProvSpellID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
          FROM $db_source.MHS501HospProvSpell AS HSP
 INNER JOIN $db_output.MHS001MPI_latest_month_data AS MPI
 		   ON HSP.Person_ID = MPI.Person_ID									
      WHERE HSP.UniqMonthID = '$month_id'
            AND HSP.StartDateHospProvSpell >= '$rp_startdate' 
            AND HSP.StartDateHospProvSpell <= '$rp_enddate'
   GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'CCG - GP Practice or Residence; Bed Type' AS BREAKDOWN
             ,IC_REC_CCG AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,CASE WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('10') THEN 'Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('11') THEN 'Older Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('23','24') THEN 'CYP Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                WHEN MHAdmittedPatientClass in ('200', '202') THEN 'Adult Acute'
                WHEN MHAdmittedPatientClass in ('201') THEN 'Older Adult Acute'
                WHEN MHAdmittedPatientClass in ('203','204','205','206','207','208','209','212','213') THEN 'Adult Specialist'
                WHEN MHAdmittedPatientClass in ('300','301') THEN 'CYP Acute'
                WHEN MHAdmittedPatientClass in ('302','303','304','305','306','307','308','309','310','311') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END AS SECONDARY_LEVEL
             ,CASE WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('10') THEN 'Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('11') THEN 'Older Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('23','24') THEN 'CYP Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                WHEN MHAdmittedPatientClass in ('200', '202') THEN 'Adult Acute'
                WHEN MHAdmittedPatientClass in ('201') THEN 'Older Adult Acute'
                WHEN MHAdmittedPatientClass in ('203','204','205','206','207','208','209','212','213') THEN 'Adult Specialist'
                WHEN MHAdmittedPatientClass in ('300','301') THEN 'CYP Acute'
                WHEN MHAdmittedPatientClass in ('302','303','304','305','306','307','308','309','310','311') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS27a' AS METRIC
             ,CAST (IFNULL (cast(COUNT (a.UniqHospProvSpellID) as INT), 0) AS STRING)    AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_source.MHS501HospProvSpell a
         LEFT JOIN $db_source.MHS502WardStay b on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.StartDateHospProvSpell = b.StartDateWardStay and ((a.starttimehospprovspell = b.starttimewardstay) or a.starttimehospprovspell is null) and b.UniqMonthID = '$month_id'
         LEFT JOIN $db_output.MHS001MPI_latest_month_data c on a.person_id = c.person_id
       WHERE a.UniqMonthID = '$month_id'
             AND StartDateHospProvSpell >= '$rp_startdate' 
             AND StartDateHospProvSpell <= '$rp_enddate'
       GROUP BY 
       IC_REC_CCG,
       NAME,
       CASE WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('10') THEN 'Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('11') THEN 'Older Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('23','24') THEN 'CYP Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                WHEN MHAdmittedPatientClass in ('200', '202') THEN 'Adult Acute'
                WHEN MHAdmittedPatientClass in ('201') THEN 'Older Adult Acute'
                WHEN MHAdmittedPatientClass in ('203','204','205','206','207','208','209','212','213') THEN 'Adult Specialist'
                WHEN MHAdmittedPatientClass in ('300','301') THEN 'CYP Acute'
                WHEN MHAdmittedPatientClass in ('302','303','304','305','306','307','308','309','310','311') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END
             ,CASE WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('10') THEN 'Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('11') THEN 'Older Adult Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('23','24') THEN 'CYP Acute'
                WHEN a.UniqMonthID <= 1488 AND MHAdmittedPatientClass in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                WHEN MHAdmittedPatientClass in ('200', '202') THEN 'Adult Acute'
                WHEN MHAdmittedPatientClass in ('201') THEN 'Older Adult Acute'
                WHEN MHAdmittedPatientClass in ('203','204','205','206','207','208','209','212','213') THEN 'Adult Specialist'
                WHEN MHAdmittedPatientClass in ('300','301') THEN 'CYP Acute'
                WHEN MHAdmittedPatientClass in ('302','303','304','305','306','307','308','309','310','311') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END

# COMMAND ----------

# DBTITLE 1,MHS28
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	       ,MPI.IC_Rec_CCG AS PRIMARY_LEVEL
 	       ,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		   ,'MHS28' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (UniqHospProvSpellID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM $db_source.MHS501HospProvSpell AS HSP
 INNER JOIN $db_output.MHS001MPI_latest_month_data AS MPI
 		   ON HSP.Person_ID = MPI.Person_ID									
      WHERE HSP.UniqMonthID = '$month_id'
            AND HSP.DischDateHospProvSpell >= '$rp_startdate' 
            AND HSP.DischDateHospProvSpell <= '$rp_enddate'           
   GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

# DBTITLE 1,MHS31 CCG
 %sql
 --MHS31 - AWOL EPISODES IN REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'CCG - GP Practice or Residence' AS BREAKDOWN,
            IC_REC_CCG AS PRIMARY_LEVEL, 
            NAME AS PRIMARY_LEVEL_DESCRIPTION,
            'NONE' AS SECONDARY_LEVEL,
            'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'MHS31' AS METRIC,
 	       CAST (COALESCE(cast(COUNT (DISTINCT MHS511UniqID) as INT), 0) AS STRING) AS METRIC_VALUE,
            '$db_source' AS SOURCE_DB
            
       FROM $db_source.MHS511AbsenceWithoutLeave AS MHS511
 INNER JOIN $db_output.MHS001MPI_latest_month_data AS PRSN
            ON MHS511.Person_ID = PRSN.Person_ID
      WHERE MHS511.UniqMonthID = '$month_id'
            AND (EndDateMHAbsWOLeave IS NULL OR EndDateMHAbsWOLeave > StartDateMHAbsWOLeave)
            AND StartDateMHAbsWOLeave >= '$rp_startdate'
   GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,AMH48a
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	       ,MPI.IC_Rec_CCG AS PRIMARY_LEVEL
 	       ,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		   ,'AMH48a' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT WRD.UniqWardStayID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
        FROM global_temp.MHS502WardStay_service_area_discharges AS WRD		
  --INNER JOIN global_temp.unique_bed_types_in_rp AS BED			
 			--ON WRD.UniqWardStayID = BED.UniqWardStayID							
  INNER JOIN $db_output.MHS001MPI_latest_month_data AS MPI
             ON WRD.Person_ID = MPI.Person_ID 
       WHERE WRD.EndDateWardStay >= '$rp_startdate'
             AND WRD.EndDateWardStay <= '$rp_enddate'	
             --and BED.Bed_type = 2
             AND UniqWardStayID in (SELECT UniqWardStayID FROM global_temp.unique_bed_types_in_rp where Bed_type = 2)
   GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

# DBTITLE 1,AMH59a
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
            ,'AMH59a' AS METRIC
            ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '<20'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH59b
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
            ,'AMH59b' AS METRIC
            ,CAST (IFNULL (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '20-49'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH59c
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
            ,'AMH59c' AS METRIC
            ,CAST (IFNULL (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '50-99'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH59d
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
            ,'AMH59d' AS METRIC
            ,CAST (IFNULL (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '100 or Over'
   GROUP BY IC_Rec_CCG, NAME;