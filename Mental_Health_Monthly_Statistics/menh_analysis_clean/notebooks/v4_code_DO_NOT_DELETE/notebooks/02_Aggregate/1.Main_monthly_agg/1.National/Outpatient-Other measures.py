# Databricks notebook source
# DBTITLE 1,MHS01 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'England' AS BREAKDOWN,
            'England' AS PRIMARY_LEVEL,
            'England' AS PRIMARY_LEVEL_DESCRIPTION,
            'NONE' AS SECONDARY_LEVEL,
            'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'MHS01' AS METRIC,
            CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp 

# COMMAND ----------

# DBTITLE 1,MHS02 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'England' AS BREAKDOWN,
 			'England' AS PRIMARY_LEVEL,
 			'England' AS PRIMARY_LEVEL_DESCRIPTION,
 			'NONE' AS SECONDARY_LEVEL,
 	        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			'MHS02' AS METRIC,
             CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp AS REF 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID

# COMMAND ----------

# DBTITLE 1,AMH01 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'England' AS BREAKDOWN,
            'England' AS PRIMARY_LEVEL,
            'England' AS PRIMARY_LEVEL_DESCRIPTION,
            'NONE' AS SECONDARY_LEVEL,
            'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'AMH01' AS METRIC,
            CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp AS REF		
      WHERE REF.AMHServiceRefEndRP_temp = TRUE

# COMMAND ----------

# DBTITLE 1,AMH02 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'England' AS BREAKDOWN,
             'England' AS PRIMARY_LEVEL,
             'England' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH02' AS METRIC,
             CAST (Coalesce (CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPA
             ON REF.Person_ID = CPA.Person_ID
       WHERE AMHServiceRefEndRP_temp = TRUE 

# COMMAND ----------

# DBTITLE 1,AMH03 National
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
              '$rp_enddate' AS REPORTING_PERIOD_END,
 		     '$status' AS STATUS,
 			 'England' AS BREAKDOWN,
 			 'England' AS PRIMARY_LEVEL,
 			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 			 'NONE' AS SECONDARY_LEVEL,
 			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 			 'AMH03' AS METRIC,
 			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE ,
              '$db_source' AS SOURCE_DB
         FROM $db_output.AMH03_prep

# COMMAND ----------

# DBTITLE 1,AMH04
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
 			 ,'AMH04' AS METRIC
              ,CAST(COALESCE(cast(COUNT(DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
              '$db_source' AS SOURCE_DB
        FROM global_temp.AMH04_prep;

# COMMAND ----------

# DBTITLE 1,CYP01 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'England' AS BREAKDOWN,
            'England' AS PRIMARY_LEVEL,
            'England' AS PRIMARY_LEVEL_DESCRIPTION,
            'NONE' AS SECONDARY_LEVEL,
            'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'CYP01' AS METRIC,
            CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp AS REF
      WHERE REF.CYPServiceRefEndRP_temp = true;

# COMMAND ----------

# DBTITLE 1,MH01 National
 %sql
 --/**MH01 - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status' AS STATUS
     	    ,'England' AS BREAKDOWN
     	    ,'England' AS PRIMARY_LEVEL
     	    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep  

# COMMAND ----------

# DBTITLE 1,MH01a National
 %sql
 --/**MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18**/
 -- has both monthly and camhs monthly outputs
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'England' AS BREAKDOWN
     	    ,'England' AS PRIMARY_LEVEL
     	    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01a' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder 
 WHERE  		AGE_GROUP = '00-18'

# COMMAND ----------

# DBTITLE 1,MH01b National
 %sql
 --/**MH01b - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 19-64**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'England' AS BREAKDOWN
     	    ,'England' AS PRIMARY_LEVEL
     	    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01b' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder
 WHERE  		AGE_GROUP = '19-64'

# COMMAND ----------

# DBTITLE 1,MH01c National
 %sql
 --/**MH01c - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 65 AND OVER**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 	        ,'$status'	AS STATUS
     	    ,'England' AS BREAKDOWN
     	    ,'England' AS PRIMARY_LEVEL
     	    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MH01c' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder
 WHERE  		AGE_GROUP = '65-120'

# COMMAND ----------

# DBTITLE 1,LDA01 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'England' AS BREAKDOWN,
            'England' AS PRIMARY_LEVEL,
            'England' AS PRIMARY_LEVEL_DESCRIPTION,
            'NONE' AS SECONDARY_LEVEL,
            'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'LDA01' AS METRIC,
            CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
            '$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp AS REF
      WHERE REF.LDAServiceRefEndRP_temp = true;

# COMMAND ----------

# DBTITLE 1,AMH05 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 		    ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE'	AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'AMH05' AS METRIC
             ,CAST (COALESCE (CAST(COUNT (DISTINCT REF.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp	AS REF
  INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
 			ON REF.Person_ID = CPAE.Person_ID 
       WHERE REF.AMHServiceRefEndRP_temp = true
 			AND CPAE.StartDateCPA < DATE_ADD((ADD_MONTHS('$rp_enddate', -12)),1);

# COMMAND ----------

# DBTITLE 1,AMH06 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 		   ,'$status' AS STATUS
 		   ,'England' AS BREAKDOWN
 		   ,'England' AS PRIMARY_LEVEL
 		   ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 		   ,'NONE'	AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		   ,'AMH06' AS METRIC
 		   ,CAST (COALESCE (cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM $db_output.MHS101Referral_open_end_rp	AS REF
 INNER JOIN $db_output.MHS701CPACareEpisode_latest	AS CPAE
            ON REF.Person_ID = CPAE.Person_ID 
 INNER JOIN $db_source.MHS702CPAReview AS CPAR
            ON CPAE.UniqCPAEpisodeID = CPAR.UniqCPAEpisodeID 
            AND CPAR.UniqMonthID <= '$month_id' 
      WHERE REF.AMHServiceRefEndRP_temp = true
 		   AND CPAE.StartDateCPA < DATE_ADD((ADD_MONTHS('$rp_enddate', -12)),1)
 		   AND CPAR.CPAReviewDate >= DATE_ADD((ADD_MONTHS('$rp_enddate', -12)),1)
 		   AND CPAR.CPAReviewDate <= '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,MHS13 National
 %sql
 
 --/**MHS13 - PEOPLE IN CONTACT WITH SERVICES AT END OF REPORTING PERIOD WITH ACCOMODATION STATUS RECORDED**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS13' AS METRIC
 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS13_Prep
                  			

# COMMAND ----------

# DBTITLE 1,AMH14
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
 			,'AMH14' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
         FROM $db_output.AMH14_prep;

# COMMAND ----------

# DBTITLE 1,AMH15
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
 			,'AMH15' AS METRIC
             ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH15.Person_ID) as INT))*100	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
       FROM  $db_output.AMH03_prep AS AMH15
  LEFT JOIN  $db_output.AMH14_prep AS AMH14
            ON AMH14.Person_ID = AMH15.Person_ID;
          

# COMMAND ----------

# DBTITLE 1,MHS16
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
 			,'MHS16' AS METRIC
             ,CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB     
    
       FROM $db_output.MHS101Referral_open_end_rp AS REF		 
 INNER JOIN $db_source.MHS004EmpStatus AS EMP
 		   ON REF.Person_ID = EMP.Person_ID 
            AND EMP.UniqMonthID <= '$month_id' 
      WHERE EMP.EmployStatusRecDate <= '$rp_enddate'
 		   AND EMP.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)

# COMMAND ----------

# DBTITLE 1,AMH17
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
 			,'AMH17' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			AND EmployStatusRecDate <= '${rp_enddate}';

# COMMAND ----------

# DBTITLE 1,AMH18
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
 			,'AMH18' AS METRIC
             ,(cast(count(distinct AMH17.Person_ID) as INT) / cast(count(distinct AMH18.Person_ID) as INT))*100	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
       FROM  $db_output.AMH03_prep AS AMH18
  LEFT JOIN $db_output.AMH17_prep AS AMH17
  ON AMH17.Person_ID = AMH18.Person_ID

# COMMAND ----------

# DBTITLE 1,MHS19
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 		   ,'$status' AS STATUS
 		   ,'England' AS BREAKDOWN
 		   ,'England' AS PRIMARY_LEVEL
 		   ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 		   ,'NONE'	AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'MHS19'	AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
            
            
       FROM $db_output.MHS001MPI_latest_month_data MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
            ON MPI.Person_ID = REF.Person_ID
 LEFT JOIN $db_source.MHS008CarePlanType AS CRS
            ON MPI.Person_ID = CRS.Person_ID
            AND CRS.UniqMonthID <= '$month_id' 
 
     --GBT: commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 -- LEFT JOIN $db_source.MHS008CrisisPlan AS CRSold
 -- 		   ON MPI.Person_ID = CRSold.Person_ID 
 --            AND CRSold.UniqMonthID <= '$month_id' 
 
     WHERE 
     
     --GBT: commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
     --(
                      (CarePlanTypeMH = '12' AND CRS.Person_ID IS NOT NULL 
                      AND ((CRS.CarePlanCreatDate <= '$rp_enddate' 
                      AND CRS.CarePlanCreatDate >= DATE_ADD(ADD_MONTHS('$rp_enddate',-12),1))
                      OR (CRS.CarePlanLastUpdateDate <= '$rp_enddate' 
                      AND CRS.CarePlanLastUpdateDate >= DATE_ADD(ADD_MONTHS( '$rp_enddate', -12),1))))
                      
    --GBT: commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 --                 OR 
 --                      (CRSold.Person_ID IS NOT NULL 
 --                      AND ((CRSold.MHCrisisPlanCreatDate <= '$rp_enddate' 
 --                      AND CRSold.MHCrisisPlanCreatDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)) 
 --                      OR (CRSold.MHCrisisPlanLastUpdateDate <= '$rp_enddate' 
 --                      AND CRSold.MHCrisisPlanLastUpdateDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1))))
 --           );

# COMMAND ----------

# DBTITLE 1,MHS20
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
 		   ,'$status' AS STATUS
 		   ,'England' AS BREAKDOWN
 		   ,'England' AS PRIMARY_LEVEL
 		   ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 		   ,'NONE'	AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 	       ,'MHS20'	AS METRIC
 		   ,CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
            
            
      FROM $db_output.MHS101Referral_open_end_rp AS REF
 LEFT JOIN $db_source.MHS604PrimDiag AS PDGN
           ON REF.UniqServReqID = PDGN.UniqServReqID 
           AND ((PDGN.RecordEndDate is null or PDGN.recordenddate >= '$rp_enddate') AND PDGN.recordstartdate <= '$rp_enddate')
 LEFT JOIN $db_source.MHS603ProvDiag  AS PVDGN
           ON REF.UniqServReqID = PVDGN.UniqServReqID 
           AND ((PVDGN.RecordEndDate is null or PVDGN.recordenddate >= '$rp_enddate') AND PVDGN.recordstartdate <= '$rp_enddate')
 LEFT JOIN $db_source.MHS605SecDiag AS SDGN
           ON REF.UniqServReqID = SDGN.UniqServReqID 
           AND ((SDGN.RecordEndDate IS null OR SDGN.recordenddate >= '$rp_enddate') AND SDGN.recordstartdate <= '$rp_enddate')
     WHERE (PDGN.Person_ID IS NOT NULL OR PVDGN.Person_ID IS NOT NULL OR SDGN.Person_ID IS NOT NULL)

# COMMAND ----------

# DBTITLE 1,MHS23 National
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
 	        ,'MHS23' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp;

# COMMAND ----------

# DBTITLE 1,AMH23 National
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
 	        ,'AMH23' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp
       WHERE AMHServiceRefEndRP_temp = true;

# COMMAND ----------

# DBTITLE 1,CYP23 National
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
 	        ,'CYP23' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM $db_output.MHS101Referral_open_end_rp
       WHERE CYPServiceRefEndRP_temp = 'Y';

# COMMAND ----------

# DBTITLE 1,MHS23a National
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
 	        ,'MHS23a' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
         FROM global_temp.MHS23abc_prep
        WHERE ServTeamTypeRefToMH = 'C02';

# COMMAND ----------

# DBTITLE 1,MHS23b National
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
 	        ,'MHS23b' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
         FROM global_temp.MHS23abc_prep
        WHERE ServTeamTypeRefToMH IN ('A02', 'A03');

# COMMAND ----------

# DBTITLE 1,MHS23c National
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
 	        ,'MHS23c' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
         FROM global_temp.MHS23abc_prep
        WHERE ServTeamTypeRefToMH = 'A17';

# COMMAND ----------

# DBTITLE 1,MHS29 National
 %sql
 --MHS29 - CONTACTS IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29_prep -- prep table in main monthly prep folder

# COMMAND ----------

# DBTITLE 1,MHS29a National
 %sql
 --/**MHS29a - CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'C02'

# COMMAND ----------

# DBTITLE 1,MHS29b National
 %sql
 --/**MHS29b - CONTACTS WITH CRISIS RESOLUTION SERVICE OR HOME TREATMENT TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH IN ('A02', 'A03')

# COMMAND ----------

# DBTITLE 1,MHS29c National
 %sql
 --/**MHS29c - CONTACTS WITH MEMORY SERVICES TEAM IN REPORTING PERIOD**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS29c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		ServTeamTypeRefToMH = 'A17'

# COMMAND ----------

# DBTITLE 1,MHS30 National
 %sql
 --/**MHS30 - ATTENDED CONTACTS IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29_prep -- prep table in main monthly prep folder
 WHERE		AttendOrDNACode IN ('5', '6')

# COMMAND ----------

# DBTITLE 1,MHS30a National
 %sql
 --/**MHS30a - ATTENDED CONTACTS WITH PERINATAL MENTAL HEALTH TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30a' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		AttendOrDNACode IN ('5', '6')
 			AND ServTeamTypeRefToMH = 'C02'

# COMMAND ----------

# DBTITLE 1,MHS30b National
 %sql
 
 --/**MHS30b - ATTENDED CONTACTS WITH CRISIS RESOLUTION SERVICE OR HOME TREATMENT TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30b' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		AttendOrDNACode IN ('5', '6')
 			AND ServTeamTypeRefToMH IN ('A02', 'A03')

# COMMAND ----------

# DBTITLE 1,MHS30c National
 %sql
 --/**MHS30c - ATTENDED CONTACTS WITH MEMORY SERVICES TEAM TEAM IN REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30c' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS29abc_prep -- prep table in main monthly prep folder
 WHERE		AttendOrDNACode IN ('5', '6')
 			AND ServTeamTypeRefToMH = 'A17'

# COMMAND ----------

# DBTITLE 1,MHS32
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
            ,'MHS32' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE 
            ,'$db_source' AS SOURCE_DB
       FROM $db_source.MHS101Referral
      WHERE UniqMonthID = '$month_id' 
             AND ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate'            

# COMMAND ----------

# DBTITLE 1,MHS33 National
 %sql
 /**MHS33 - PEOPLE ASSIGNED TO A CARE CLUSTER AT END OF REPORTING PERIOD**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS33' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT MPI.Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		$db_output.MHS001MPI_latest_month_data AS MPI -- prep table in generic prep folder
 INNER JOIN  $db_source.MHS801ClusterTool AS CCT 
             ON MPI.Person_ID = CCT.Person_ID 
             AND CCT.uniqmonthid = '$month_id'
 INNER JOIN  $db_output.MHS803CareCluster_common AS CC -- prep table in CaP prep folder
 			ON CCT.UniqClustID = CC.UniqClustID 
 INNER JOIN  $db_output.MHS101Referral_open_end_rp AS REF -- prep table in generic prep folder
 			ON MPI.Person_ID = REF.Person_ID 

# COMMAND ----------

# DBTITLE 1,MHS57 National
 %sql
 /**MHS57 - NUMBER OF PEOPLE DISCHARGED IN THE RP**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS57' AS METRIC
 			,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		$db_source.MHS101Referral
 
 WHERE		(ServDischDate >= '$rp_startdate' AND ServDischDate <= '$rp_enddate')
 			AND uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,MHS58 National
 %sql
 /**MHS58 - NUMBER OF MISSED CARE CONTACTS IN THE RP**/
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS58' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) AS INT), 0) AS STRING) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS58_prep -- prep table in main monthly prep folder

# COMMAND ----------

# DBTITLE 1,CCR70 National
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
 			,'CCR70' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '1';

# COMMAND ----------

# DBTITLE 1,CCR70a National
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
 			,'CCR70a' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '18 and over';

# COMMAND ----------

# DBTITLE 1,CCR70b National
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
 			,'CCR70b' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '0-17';

# COMMAND ----------

# DBTITLE 1,CCR71 National
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
 			,'CCR71' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '2';

# COMMAND ----------

# DBTITLE 1,CCR71a National
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
 			,'CCR71a' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '18 and over';
             

# COMMAND ----------

# DBTITLE 1,CCR71b National
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
 			,'CCR71b' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7071_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '0-17';

# COMMAND ----------

# DBTITLE 1,CCR72 National
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
 			,'CCR72' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '1'

# COMMAND ----------

# DBTITLE 1,CCR72a National
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
 			,'CCR72a' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '18 and over';

# COMMAND ----------

# DBTITLE 1,CCR72b National
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
 			,'CCR72b' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '1'
             AND AGE_GROUP = '0-17';

# COMMAND ----------

# DBTITLE 1,CCR73 National
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
 			,'CCR73' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2';

# COMMAND ----------

# DBTITLE 1,CCR73a National
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
 			,'CCR73a' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '18 and over';

# COMMAND ----------

# DBTITLE 1,CCR73b National
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
 			,'CCR73b' AS METRIC
             ,COUNT(DISTINCT UniqServReqID) AS METRIC_VALUE 
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '0-17';