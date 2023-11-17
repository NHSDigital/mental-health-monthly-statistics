# Databricks notebook source
# DBTITLE 1,MHS07 National
 %sql
 /**MHS07 - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted--_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS METRIC
 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder  

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Accommodation Type' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,AccommodationType AS SECONDARY_LEVEL
            ,AccommodationType_Desc AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  AccommodationType, AccommodationType_Desc;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,Age_Band AS SECONDARY_LEVEL
            ,Age_Band AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  Age_Band;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Disability' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,DisabCode AS SECONDARY_LEVEL
            ,DisabCode_Desc AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  DisabCode, DisabCode_Desc;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Employment Status' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,EmployStatus AS SECONDARY_LEVEL
            ,EmployStatus_Desc AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  EmployStatus, EmployStatus_Desc;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Ethnicity' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,CASE WHEN LowerEthnicity = '-1' THEN 'UNKNOWN' ELSE LowerEthnicity END AS SECONDARY_LEVEL
            ,LowerEthnicity_Desc AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  LowerEthnicity, LowerEthnicity_Desc;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Gender' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,Der_Gender AS SECONDARY_LEVEL
            ,Der_GenderName AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  Der_Gender, Der_GenderName;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; IMD Decile' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,IMD_Decile AS SECONDARY_LEVEL
            ,IMD_Decile AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  IMD_Decile;

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Sexual Orientation' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,Sex_Orient AS SECONDARY_LEVEL
            ,Sex_Orient AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs07_prep
  GROUP BY  Sex_Orient;

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,MHS07a National
 %sql
 
 /**MHS07a - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 0-18**/
 
 
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07a' AS METRIC
 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder 
      WHERE AGE_GROUP = '00-18'

# COMMAND ----------

# DBTITLE 1,MHS07b National
 %sql
 /**MHS07b - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07b' AS METRIC
 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder 
      WHERE AGE_GROUP = '19-64'

# COMMAND ----------

# DBTITLE 1,MHS07c National
 %sql
 /**MHS07c - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS07c' AS METRIC
 		   ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS07_prep -- prep table in main monthly prep folder 
      WHERE AGE_GROUP = '65-120'

# COMMAND ----------

# DBTITLE 1,MHS21 National
 %sql
 /**MHS21 - OPEN WARD STAYS AT END OF REPORTING PERIOD**/
 
 INSERT INTO  $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
              ,'$rp_enddate' AS REPORTING_PERIOD_END
              ,'$status' AS STATUS
              ,'England' AS BREAKDOWN
              ,'England' AS PRIMARY_LEVEL
              ,'England' AS PRIMARY_LEVEL_DESCRIPTION
              ,'NONE' AS SECONDARY_LEVEL
              ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
              ,'MHS21' AS METRIC
              ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
              ,'$db_source' AS SOURCE_DB
              
 FROM         $db_output.MHS21_prep    -- prep table in main monthly prep folder 

# COMMAND ----------

# DBTITLE 1,MHS21a National
 %sql
 /**MHS21a - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 0-18**/
 --in both monthly and camhs monthly output tables 
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS21a' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE       AGE_GROUP = '00-18'

# COMMAND ----------

# DBTITLE 1,MHS21b National
 %sql
 /**MHS21b - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS21b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE		AGE_GROUP = '19-64'

# COMMAND ----------

# DBTITLE 1,MHS21c National
 %sql
 /**MHS21c - OPEN WARD STAYS DISTANCE AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS21c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE		AGE_GROUP = '65-120'

# COMMAND ----------

# DBTITLE 1,AMH21 National
 %sql
 -- /**AMH21 - OPEN WARD STAYS (ADULT MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
 	       ,'England' AS BREAKDOWN
 	       ,'England' AS PRIMARY_LEVEL
 	       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'AMH21' AS METRIC
 	       ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
 FROM       $db_output.MHS21_prep  -- prep table in main monthly prep folder 
 WHERE      AMHServiceWSEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,CYP21 National
 %sql
 /**CYP21 - OPEN WARD STAYS (CHILDREN AND YOUNG PEOPLE'S MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD**/
 --for both monthly and camhs monthly output tables 
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'CYP21' AS METRIC
            ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE     
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE       CYPServiceWSEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,AMH21a National
 %sql
 /**AMH21a - OPEN WARD STAYS, ADULT ACUTE MH CARE, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH21a' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE       Bed_Type = 2 
             AND AMHServiceWSEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,AMH21b National
 %sql
 /**AMH21b - OPEN WARD STAYS, SPECIALIST ADULT MH SERVICES, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH21b' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE       Bed_Type = 1 
             AND AMHServiceWSEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,MHS22 National
 %sql
 /**MHS22 - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder 
 WHERE		WardLocDistanceHome >= 50

# COMMAND ----------

# DBTITLE 1,MHS22a National
 %sql
 /**MHS22a - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD, AGED 0-18**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
             AND Age_group = '00-18'

# COMMAND ----------

# DBTITLE 1,MHS22b National
 %sql
 
 /**MHS22b - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22b' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
             AND Age_group = '19-64'

# COMMAND ----------

# DBTITLE 1,MHS22c National
 %sql
 
 /**MHS22c - OPEN WARD STAYS DISTANCE > 50KM AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
 	        ,'England' AS BREAKDOWN
 	        ,'England' AS PRIMARY_LEVEL
 	        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'MHS22c' AS METRIC
   	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE		WardLocDistanceHome >= 50
             AND Age_group = '65-120'

# COMMAND ----------

# DBTITLE 1,AMH22a National
 %sql
 /**AMH22a - DISTANCE TO TREATMENT >= 50KM OPEN WARD STAYS, ADULT ACUTE MH CARE, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	        ,'NONE' AS SECONDARY_LEVEL
 	        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	        ,'AMH22a' AS METRIC
 	        ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE  
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE       Bed_Type = 2 
             AND WardLocDistanceHome >= 50 
             AND AMHServiceWSEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,AMH22b National
 %sql
 
 /**AMH22b - DISTANCE TO TREATMENT >= 50KM OPEN WARD STAYS, SPECIALIST ADULT MH SERVICES, AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'AMH22b' AS METRIC
 	       ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) AS INT), 0) AS STRING) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
            
 FROM       $db_output.MHS21_prep -- prep table in main monthly prep folder
 WHERE      Bed_Type = 1 
            AND WardLocDistanceHome >= 50  
            AND AMHServiceWSEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,MHS24 National
 %sql
 --MHS24 - BED DAYS IN REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'England' AS BREAKDOWN,
             'England' AS PRIMARY_LEVEL,
             'England' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS24' AS METRIC,
 		    SUM(METRIC_VALUE) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.BED_DAYS_IN_RP AS BedDays      

# COMMAND ----------

# DBTITLE 1,MHS25
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS25' AS METRIC
 			,CAST (COALESCE (WRD.METRIC_VALUE, 0) - COALESCE (HLV.METRIC_VALUE, 0) - COALESCE (LOA.METRIC_VALUE, 0) AS STRING)AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM 
               (SELECT LEVEL
                       ,SUM(METRIC_VALUE) AS METRIC_VALUE 
                  FROM $db_output.BED_DAYS_IN_RP 
                  GROUP BY LEVEL) AS WRD
    LEFT JOIN 
               (SELECT LEVEL
                       ,SUM(METRIC_VALUE) AS METRIC_VALUE 
                  FROM global_temp.HOME_LEAVE_IN_RP 
              GROUP BY LEVEL) AS HLV
                        ON WRD.LEVEL = HLV.LEVEL
    LEFT JOIN  
               (SELECT LEVEL
                       ,SUM(METRIC_VALUE) AS METRIC_VALUE 
                  FROM global_temp.LOA_IN_RP 
              GROUP BY LEVEL) AS LOA
                        ON WRD.LEVEL = LOA.LEVEL;

# COMMAND ----------

# DBTITLE 1,MHS26 National
 %sql
  INSERT INTO $db_output.Main_monthly_unformatted
  SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
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
             
 FROM global_temp.MHS26_ranking

# COMMAND ----------

# DBTITLE 1,MHS26 National by AttribToIndic
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'England; Delayed discharge attributable to' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AttribToIndic AS SECONDARY_LEVEL
             ,coalesce(ref.Description,'Not a valid code') AS SECONDARY_LEVEL_DESCRIPTION
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
 on ranking.AttribToIndic = ref.code and ref.key = 'att'
 and '$month_id' >= ref.FirstMonth and (ref.LastMonth is null or '$month_id' <= ref.LastMonth)
 
 group by AttribToIndic,ref.Description

# COMMAND ----------

# DBTITLE 1,MHS26 National by DelayDischReason
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'England; Delayed discharge reason' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,DelayDischReason   AS SECONDARY_LEVEL
             ,coalesce(ref.Description,'Not a valid code') AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS Measure_ID
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
 on ranking.DelayDischReason = ref.code and ref.key = 'reason'
 and '$month_id' >= ref.FirstMonth and (ref.LastMonth is null or '$month_id' <= ref.LastMonth)
 
 group by DelayDischReason,ref.Description

# COMMAND ----------

# DBTITLE 1,MHS27
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status'AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS27' AS METRIC
 			,CAST (IFNULL (cast(COUNT (UniqHospProvSpellID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_source.MHS501HospProvSpell
        
       WHERE UniqMonthID = '$month_id'
             AND StartDateHospProvSpell >= '$rp_startdate' 
             AND StartDateHospProvSpell <= '$rp_enddate';

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status'AS STATUS
 			,'England; Bed Type' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
                WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
                WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
                WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END AS SECONDARY_LEVEL
             ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
                WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
                WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
                WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS27a' AS METRIC
 			,CAST (IFNULL (cast(COUNT (a.UniqHospProvSpellID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_source.MHS501HospProvSpell a
         LEFT JOIN $db_source.MHS502WardStay b on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.StartDateHospProvSpell = b.StartDateWardStay and ((a.starttimehospprovspell = b.starttimewardstay) or a.starttimehospprovspell is null) and b.UniqMonthID = '$month_id'
       WHERE a.UniqMonthID = '$month_id'
             AND StartDateHospProvSpell >= '$rp_startdate' 
             AND StartDateHospProvSpell <= '$rp_enddate'
       GROUP BY 
       CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
                WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
                WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
                WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END
             ,CASE WHEN HospitalBedTypeMH in ('10') THEN 'Adult Acute'
                WHEN HospitalBedTypeMH in ('11') THEN 'Older Adult Acute'
                WHEN HospitalBedTypeMH in ('12','13','14','15','16','17','18','19','20','21','22') THEN 'Adult Specialist'
                WHEN HospitalBedTypeMH in ('23','24') THEN 'CYP Acute'
                WHEN HospitalBedTypeMH in ('25','26','27','28','29','30','31','32','33','34') THEN 'CYP Specialist'
                ELSE 'UNKNOWN'
                END

# COMMAND ----------

# DBTITLE 1,MHS28
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status'AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS28' AS METRIC
 			,CAST (IFNULL (cast(COUNT (UniqHospProvSpellID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_source.MHS501HospProvSpell
       WHERE UniqMonthID = '$month_id'
             AND DischDateHospProvSpell >= '$rp_startdate' 
             AND DischDateHospProvSpell <= '$rp_enddate';    

# COMMAND ----------

# DBTITLE 1,MHS31 National
 %sql
 --MHS31 - AWOL EPISODES IN REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'England' AS BREAKDOWN,
        'England' AS PRIMARY_LEVEL,
        'England' AS PRIMARY_LEVEL_DESCRIPTION,
        'NONE' AS SECONDARY_LEVEL,
        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
        'MHS31' AS METRIC,
 	   CAST (COALESCE(cast(COUNT (DISTINCT MHS511UniqID) as INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_source.MHS511AbsenceWithoutLeave
  WHERE UniqMonthID = '$month_id'
    AND (EndDateMHAbsWOLeave IS NULL OR EndDateMHAbsWOLeave > StartDateMHAbsWOLeave)
    AND StartDateMHAbsWOLeave >= '$rp_startdate'

# COMMAND ----------

# DBTITLE 1,AMH48a
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
 			,'AMH48a'AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT WRD.UniqWardStayID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.MHS502WardStay_service_area_discharges AS WRD		
  --INNER JOIN global_temp.unique_bed_types_in_rp AS BED			
 			--ON WRD.UniqWardStayID = BED.UniqWardStayID		
     WHERE	WRD.EndDateWardStay >= '$rp_startdate'
             AND WRD.EndDateWardStay <= '$rp_enddate'	
             --and BED.Bed_type = 2
             AND UniqWardStayID in (SELECT UniqWardStayID FROM global_temp.unique_bed_types_in_rp where Bed_type = 2)

# COMMAND ----------

# DBTITLE 1,AMH59a
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
            ,'AMH59a' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '<20';

# COMMAND ----------

# DBTITLE 1,AMH59b
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
            ,'AMH59b' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '20-49';

# COMMAND ----------

# DBTITLE 1,AMH59c
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
            ,'AMH59c' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '50-99';

# COMMAND ----------

# DBTITLE 1,AMH59d
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
            ,'AMH59d' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqWardStayID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.AMH59_prep
      WHERE WardDistanceHome = '100 or Over';
      