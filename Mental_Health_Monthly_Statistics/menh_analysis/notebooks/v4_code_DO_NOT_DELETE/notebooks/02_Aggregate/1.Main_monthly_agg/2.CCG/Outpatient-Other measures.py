# Databricks notebook source
# DBTITLE 1,MHS01 CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
 		   '$status' AS STATUS,
            'CCG - GP Practice or Residence'	AS BREAKDOWN,
            IC_Rec_CCG AS PRIMARY_LEVEL,
 		   NAME AS PRIMARY_LEVEL_DESCRIPTION,
 		   'NONE' AS SECONDARY_LEVEL,
 		   'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'MHS01' AS METRIC,
            CAST (COALESCE (cast(COUNT (DISTINCT REF.Person_ID) as int), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN $db_output.MHS001MPI_latest_month_data  AS MPI 
            ON REF.Person_ID = MPI.Person_ID        
      GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS02 CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'CCG - GP Practice or Residence' AS BREAKDOWN,
 			IC_REC_CCG AS PRIMARY_LEVEL,
 			NAME AS PRIMARY_LEVEL_DESCRIPTION,
 			'NONE' AS SECONDARY_LEVEL,
 	        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			'MHS02' AS METRIC,
              CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE ,
              '$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
  INNER JOIN $db_output.MHS001MPI_latest_month_data AS CCG 
             ON REF.Person_ID = CCG.Person_ID
    GROUP BY IC_REC_CCG, NAME 

# COMMAND ----------

# DBTITLE 1,AMH01
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
 		   '$status' AS STATUS,
            'CCG - GP Practice or Residence'	AS BREAKDOWN,
             IC_Rec_CCG AS PRIMARY_LEVEL,
 		    NAME AS PRIMARY_LEVEL_DESCRIPTION,
 		   'NONE' AS SECONDARY_LEVEL,
 		   'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'AMH01' AS METRIC,
 	       CAST (COALESCE(cast(COUNT (DISTINCT REF.Person_ID) as int), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN $db_output.MHS001MPI_latest_month_data  AS MPI 
            ON REF.Person_ID = MPI.Person_ID 
     WHERE  REF.AMHServiceRefEndRP_temp = TRUE                
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH02 CCG
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'CCG - GP Practice or Residence' AS BREAKDOWN,
             IC_REC_CCG AS PRIMARY_LEVEL,
             NAME AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH02' AS METRIC,
             CAST (coalesce (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
  INNER JOIN $db_output.MHS001MPI_latest_month_data AS CCG 
             ON REF.Person_ID = CCG.Person_ID 
       WHERE AMHServiceRefEndRP_temp = TRUE 
    GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,AMH03 CCG
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'CCG - GP Practice or Residence' AS BREAKDOWN,
 			 IC_REC_CCG AS PRIMARY_LEVEL,
 			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (coalesce (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE ,
              '$db_source' AS SOURCE_DB
         FROM $db_output.AMH03_prep 
     GROUP BY IC_REC_CCG, NAME

# COMMAND ----------

# DBTITLE 1,AMH04
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'CCG - GP Practice or Residence' AS BREAKDOWN,
 			 IC_REC_CCG AS PRIMARY_LEVEL,
 			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH04' AS METRIC,
 			 CAST(COALESCE(CAST(COUNT(DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE ,
              '$db_source' AS SOURCE_DB
         FROM global_temp.AMH04_prep
     GROUP BY IC_REC_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CYP01
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
 		   '$status' AS STATUS,
            'CCG - GP Practice or Residence'	AS BREAKDOWN,
             IC_Rec_CCG AS PRIMARY_LEVEL,
 		    NAME AS PRIMARY_LEVEL_DESCRIPTION,
 		   'NONE' AS SECONDARY_LEVEL,
 		   'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'CYP01' AS METRIC,
            CAST (IFNULL (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS001MPI_latest_month_data  AS MPI
             ON REF.Person_ID = MPI.Person_ID        
       WHERE REF.CYPServiceRefEndRP_temp = 'Y'             
    GROUP BY IC_Rec_CCG
             ,NAME;

# COMMAND ----------

# DBTITLE 1,MH01 CCG
 %sql
 
 --/**MH01 - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder
 GROUP BY	IC_Rec_CCG
 			,NAME;

# COMMAND ----------

# DBTITLE 1,MH01aCCG
 %sql
 --/**MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18**/
 -- in both monthly and cahms montly outputs
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01a' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '00-18'
 GROUP BY	IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,MH01b CCG
 %sql
 --/**MH01b - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01b' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '19-64'
 GROUP BY	IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,MH01c CCG
 %sql
 --/**MH01c - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01c' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder
 WHERE		AGE_GROUP = '65-120'
 GROUP BY	IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,LDA01
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
 		   '$status' AS STATUS,
            'CCG - GP Practice or Residence'	AS BREAKDOWN,
             IC_Rec_CCG AS PRIMARY_LEVEL,
 		    NAME AS PRIMARY_LEVEL_DESCRIPTION,
 		   'NONE' AS SECONDARY_LEVEL,
 		   'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'LDA01' AS METRIC,
 	       CAST (IFNULL (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
      FROM  $db_output.MHS101Referral_open_end_rp AS REF
 INNER JOIN $db_output.MHS001MPI_latest_month_data  AS MPI
            ON REF.Person_ID = MPI.Person_ID
       WHERE REF.LDAServiceRefEndRP_temp = true       
   GROUP BY IC_Rec_CCG
            ,NAME;

# COMMAND ----------

# DBTITLE 1,AMH05
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG	AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH05' AS METRIC
 			,CAST (COALESCE (CAST(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB    
        FROM $db_output.MHS001MPI_latest_month_data AS MPI
  INNER JOIN $db_output.MHS101Referral_open_end_rp	AS REF
 			ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON MPI.Person_ID = CPAE.Person_ID
             AND CPAE.UniqMonthID = '$month_id' 
       WHERE REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH06
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG	AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH06' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB   
        FROM	$db_output.MHS001MPI_latest_month_data AS MPI
  INNER JOIN $db_output.MHS101Referral_open_end_rp	AS REF
 			ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON MPI.Person_ID = CPAE.Person_ID             
  INNER JOIN $db_source.MHS702CPAReview AS CPAR
 		    ON CPAE.UniqCPAEpisodeID = CPAR.UniqCPAEpisodeID 
             AND CPAR.UniqMonthID <= '$month_id' 
       WHERE	REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			AND CPAR.CPAReviewDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 			AND CPAR.CPAReviewDate <= '$rp_enddate'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS13 - CCG
 %sql
 
 --/**MHS13 - PEOPLE IN CONTACT WITH SERVICES AT END OF REPORTING PERIOD WITH ACCOMODATION STATUS RECORDED**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS13' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS13_prep
 GROUP BY	IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,AMH14
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
             ,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.AMH14_prep
    GROUP BY IC_Rec_CCG,NAME;

# COMMAND ----------

# DBTITLE 1,AMH15
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'CCG - GP Practice or Residence' AS BREAKDOWN
             ,AMH15.IC_Rec_CCG AS PRIMARY_LEVEL
             ,AMH15.NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
       FROM  $db_output.AMH03_prep AS AMH15
  LEFT JOIN  $db_output.AMH14_prep AS AMH14
            ON AMH14.Person_ID = AMH15.Person_ID
   GROUP BY AMH15.IC_Rec_CCG, AMH15.NAME;

# COMMAND ----------

# DBTITLE 1,AMH17
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			, IC_Rec_CCG AS PRIMARY_LEVEL
 			, NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			AND EmployStatusRecDate <= '${rp_enddate}'
       GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH18
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			, AMH18.IC_Rec_CCG AS PRIMARY_LEVEL
 			, AMH18.NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100 AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.AMH03_prep AS AMH18
   LEFT JOIN $db_output.AMH17_prep AS AMH17
             ON AMH17.Person_ID = AMH18.Person_ID
    GROUP BY AMH18.IC_Rec_CCG, AMH18.NAME;

# COMMAND ----------

# DBTITLE 1,AMH14
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	       ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 	       ,AMH14.IC_Rec_CCG AS PRIMARY_LEVEL
 	       ,AMH14.NAME AS PRIMARY_LEVEL_DESCRIPTION
 	       ,'NONE' AS SECONDARY_LEVEL
 	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		   ,'AMH14' AS METRIC
            ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM $db_output.AMH14_prep AS AMH14
      WHERE AccommodationStatusDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
 		   AND AccommodationStatusDate <= '$rp_enddate'
   GROUP BY AMH14.IC_Rec_CCG, AMH14.NAME;

# COMMAND ----------

# DBTITLE 1,MHS16
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,MPI.IC_Rec_CCG	AS PRIMARY_LEVEL
 			,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'MHS16'	AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
 
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
 INNER JOIN $db_source.MHS004EmpStatus AS EMP
 		   ON MPI.Person_ID = EMP.Person_ID 
            AND EMP.UniqMonthID <= '$month_id' 
      WHERE EMP.EmployStatusRecDate <= '$rp_enddate'
 		   AND EMP.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
   GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

# DBTITLE 1,MHS19
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,MPI.IC_Rec_CCG	AS PRIMARY_LEVEL
 			,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'MHS19'	AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
            
            
         FROM $db_output.MHS001MPI_latest_month_data MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
            ON MPI.Person_ID = REF.Person_ID
 LEFT JOIN $db_source.MHS008CarePlanType AS CRS
            ON MPI.Person_ID = CRS.Person_ID
            AND CRS.UniqMonthID <= '$month_id' 
 
  --commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data           
 -- LEFT JOIN $db_source.MHS008CrisisPlan AS CRSold
 -- 		   ON MPI.Person_ID = CRSold.Person_ID 
 --            AND CRSold.UniqMonthID <= '$month_id' 
 
     WHERE 
     
     --commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
     --(
                      (CarePlanTypeMH = '12' AND CRS.Person_ID IS NOT NULL 
                      AND ((CRS.CarePlanCreatDate <= '$rp_enddate' 
                      AND CRS.CarePlanCreatDate >= DATE_ADD(ADD_MONTHS('$rp_enddate',-12),1))
                      OR (CRS.CarePlanLastUpdateDate <= '$rp_enddate' 
                      AND CRS.CarePlanLastUpdateDate >= DATE_ADD(ADD_MONTHS( '$rp_enddate', -12),1))))
   
     --commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 --     OR 
 --                      (CRSold.Person_ID IS NOT NULL 
 --                      AND ((CRSold.MHCrisisPlanCreatDate <= '$rp_enddate' 
 --                      AND CRSold.MHCrisisPlanCreatDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)) 
 --                      OR (CRSold.MHCrisisPlanLastUpdateDate <= '$rp_enddate' 
 --                      AND CRSold.MHCrisisPlanLastUpdateDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1))))
 --           )
         
  GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

# DBTITLE 1,MHS20
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,MPI.IC_Rec_CCG	AS PRIMARY_LEVEL
 			,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'MHS20'	AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB           
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
 		   ON MPI.Person_ID = REF.Person_ID 
  LEFT JOIN $db_source.MHS604PrimDiag AS PDGN
            ON REF.UniqServReqID = PDGN.UniqServReqID 
            AND ((PDGN.RecordEndDate is null or PDGN.RecordEndDate >= '$rp_enddate') AND PDGN.RecordStartDate <= '$rp_enddate')
  LEFT JOIN $db_source.MHS603ProvDiag  AS PVDGN
            ON REF.UniqServReqID = PVDGN.UniqServReqID 
            AND ((PVDGN.RecordEndDate is null or PVDGN.RecordEndDate >= '$rp_enddate') AND PVDGN.RecordStartDate <= '$rp_enddate')
  LEFT JOIN $db_source.MHS605SecDiag AS SDGN
            ON REF.UniqServReqID = SDGN.UniqServReqID 
            AND ((SDGN.RecordEndDate IS null OR SDGN.RecordEndDate >= '$rp_enddate') AND SDGN.RecordStartDate <= '$rp_enddate')
      WHERE (PDGN.Person_ID IS NOT NULL OR PVDGN.Person_ID IS NOT NULL OR SDGN.Person_ID IS NOT NULL)
   GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

# DBTITLE 1,MHS23
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
 		   ,'MHS23' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS23_prep
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,AMH23
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
 		   ,'AMH23' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS23_prep	
      WHERE AMHServiceRefEndRP_temp = true
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CYP23
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
 		   ,'CYP23' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS23_prep
      WHERE CYPServiceRefEndRP_temp = true
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS23a
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
 		   ,'MHS23a' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS23abc_prep
      WHERE ServTeamTypeRefToMH = 'C02'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS23b
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
 		   ,'MHS23b' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS23abc_prep
      WHERE ServTeamTypeRefToMH IN ('A02', 'A03') 
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS23c
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
 		   ,'MHS23c' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS23abc_prep
      WHERE ServTeamTypeRefToMH = 'A17' 
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS29 CCG
 %sql
 --/**MHS29 - CONTACTS IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29_prep -- prep table in main monthly prep folder
 
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS29a CCG
 %sql
 --/**MHS29a - CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'C02'
         --    AND IC_Use_Submission_Flag = 'Y' -- needed to be correct in hue
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS29b CCG
 %sql
 --/**MHS29b - CONTACTS WITH CRISIS RESOLUTION SERVICE OR HOME TREATMENT TEAM IN REPORTING PERIOD, CCG**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH IN ('A02', 'A03')
         --    AND IC_Use_Submission_Flag = 'Y' -- needed to be correct in hue
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS29c CCG
 %sql
 --/**MHS29c - CONTACTS WITH MEMORY SERVICES TEAM IN REPORTING PERIOD, CCG**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'A17'
         --    AND IC_Use_Submission_Flag = 'Y' -- needed to be correct in hue
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS30 CCG
 %sql
 --/**MHS30 - ATTENDED CONTACTS IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29_prep -- prep table in main monthly prep folder
 WHERE		AttendOrDNACode IN ('5', '6')
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS30a CCG
 %sql
 --/**MHS30a - ATTENDED CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD, CCG**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'C02'
 			AND AttendOrDNACode IN ('5', '6')
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS30b CCG
 %sql
 --/**MHS30b - ATTENDED CONTACTS WITH CRISIS RESOLUTION SERVICE OR HOME TREATMENT TEAM IN REPORTING PERIOD, CCG**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH IN ('A02', 'A03')
 			AND AttendOrDNACode IN ('5', '6')
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS30c CCG
 %sql
 --/**MHS30c - ATTENDED CONTACTS WITH MEMORY SERVICES TEAM TEAM IN REPORTING PERIOD, CCG**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'A17'
 			AND AttendOrDNACode IN ('5', '6')
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS32
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,MPI.IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32' AS METRIC
             ,COUNT (DISTINCT REF.UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.MHS001MPI_latest_month_data AS MPI -- prep table in generic prep folder
  INNER JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID=REF.Person_ID 
       WHERE REF.UniqMonthID = '$month_id' 
             AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS33 CCG
 %sql 
 /**MHS33 - PEOPLE ASSIGNED TO A CARE CLUSTER AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS33' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		$db_output.MHS001MPI_latest_month_data AS MPI  -- prep table in generic prep folder
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT
             ON MPI.Person_ID = CCT.Person_ID 
             AND CCT.uniqmonthid = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC  -- prep table in CaP prep folder
 			ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF  -- prep table in generic prep folder
 			ON MPI.Person_ID = REF.Person_ID
 GROUP BY    IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,MHS57 CCG
 %sql
 /**MHS57 - NUMBER OF PEOPLE DISCHARGED IN THE RP**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS57' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		$db_source.MHS101Referral AS REF
 INNER JOIN  $db_output.MHS001MPI_latest_month_data AS MPI
 			ON REF.Person_ID = MPI.Person_ID  	
 WHERE		(ServDischDate >= '$rp_startdate' AND ServDischDate <= '$rp_enddate')
 			AND REF.uniqmonthid = '$month_id'			
 GROUP BY    IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,MHS58 CCG
 %sql
 /**MHS58 - NUMBER OF MISSED CARE CONTACTS IN THE RP**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS58' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS58_prep
 GROUP BY    IC_Rec_CCG
             ,NAME

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,CCR70
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
 		   ,'CCR70' AS METRIC
 		   ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM  global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '1'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CCR70a
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
 		   ,'CCR70a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '18 and over'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CCR70b
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
 		   ,'CCR70b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '0-17'
    GROUP BY IC_Rec_CCG, NAME;  
           

# COMMAND ----------

# DBTITLE 1,CCR71
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
 		   ,'CCR71' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '2'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CCR71a
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
 		   ,'CCR71a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '18 and over'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CCR71b
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
 		   ,'CCR71b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '0-17'
    GROUP BY IC_Rec_CCG, NAME;      

# COMMAND ----------

# DBTITLE 1,CCR72
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
 		   ,'CCR72' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '1'
    GROUP BY IC_Rec_CCG, NAME;    

# COMMAND ----------

# DBTITLE 1,CCR72a
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
 		   ,'CCR72a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '18 and over'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CCR72b
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
 		   ,'CCR72b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '0-17'
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CCR73
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
 		   ,'CCR73' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2'
    GROUP BY IC_Rec_CCG, NAME;    

# COMMAND ----------

# DBTITLE 1,CCR73a
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
 		   ,'CCR73a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '18 and over'
    GROUP BY IC_Rec_CCG, NAME;    

# COMMAND ----------

# DBTITLE 1,CCR73b
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
 		   ,'CCR73b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '0-17'
    GROUP BY IC_Rec_CCG, NAME;     