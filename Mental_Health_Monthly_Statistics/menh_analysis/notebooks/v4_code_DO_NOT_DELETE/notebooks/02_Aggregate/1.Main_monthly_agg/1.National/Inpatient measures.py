# Databricks notebook source
# DBTITLE 1,MHS07 National
 %sql

 /**MHS07 - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD**/

 INSERT INTO $db_output.Main_monthly_unformatted
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
 		    SUM(METRIC_VALUE) AS METRIC_VALUE ,
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

# DBTITLE 1,MHS26
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
 			,'MHS26' AS METRIC
 			,CAST (COALESCE (SUM(DATEDIFF(CASE	WHEN EndDateDelayDisch IS NULL
 											THEN DATE_ADD ('$rp_enddate',1)
 											ELSE EndDateDelayDisch
 											END
                                       ,CASE	WHEN StartDateDelayDisch < '$rp_startdate'
 											THEN '$rp_startdate'
 											ELSE StartDateDelayDisch
 											END
 									  )), 0) AS STRING)	AS METRIC_VALUE 
               ,'$db_source' AS SOURCE_DB
               
        FROM $db_source.MHS504DelayedDischarge							
       WHERE UniqMonthID = '$month_id';

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
 			,CAST (IFNULL (cast(COUNT (UniqHospProvSpellNum) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_source.MHS501HospProvSpell			
       WHERE UniqMonthID = '$month_id'
             AND StartDateHospProvSpell >= '$rp_startdate' 
             AND StartDateHospProvSpell <= '$rp_enddate';

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
 			,CAST (IFNULL (cast(COUNT (UniqHospProvSpellNum) as INT), 0) AS STRING)	AS METRIC_VALUE 
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
 	   CAST (COALESCE(cast(COUNT (DISTINCT MHS511UniqID) as INT), 0) AS STRING) AS METRIC_VALUE ,
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
      