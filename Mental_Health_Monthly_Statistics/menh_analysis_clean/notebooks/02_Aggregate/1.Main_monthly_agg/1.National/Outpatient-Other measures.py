# Databricks notebook source
 %md
 
 ## Measures currently in this notebook 
 ### these measures are all listed in $db_output.Main_monthly_metric_values (populated within notebooks/01_Prepare/0.Insert_lookup_data)
 
 MHS01
 AMH01
 CYP01
 MH01
 MH01a,b,c (group into a single piece of code and CASE WHEN on age groups??)
 LDA01
 
 MHS13
 MHS16
 MHS19
 MHS20
 MHS23
 AMH23
 CYP23
 MHS23a,b,c
 MHS23d (outputs going into $db_output.Main_monthly_unformatted_exp)
 
 MHS29
 MHS29a,b,c
 MHS29d,f (outputs going into $db_output.Main_monthly_unformatted_exp)
 
 MHS30
 MHS30a,b,c,f(partial)
 
 MHS32
 MHS33
 
 MHS57
 MHS58
 
 move this block into their own notebook: Outpatient-CCR
 CCR70
 CCR70a,b
 CCR71
 CCR71a,b
 CCR72
 CCR72a,b
 CCR73
 CCR73a,b

# COMMAND ----------

# DBTITLE 1,MHS01 National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted--_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'England' AS BREAKDOWN,
            'England' AS PRIMARY_LEVEL,
            'England' AS PRIMARY_LEVEL_DESCRIPTION,
            'NONE' AS SECONDARY_LEVEL,
            'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
            'MHS01' AS METRIC,
            CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
       FROM $db_output.MHS101Referral_open_end_rp 

# COMMAND ----------

# DBTITLE 1,MHS01 National; Accommodation Type
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Accommodation Type' AS BREAKDOWN
            ,AccommodationType AS PRIMARY_LEVEL
            ,AccommodationType_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  AccommodationType, AccommodationType_Desc;

# COMMAND ----------

# DBTITLE 1,MHS01 National; Age Band
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age Group' AS BREAKDOWN
            ,Age_Band AS PRIMARY_LEVEL
            ,Age_Band AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  Age_Band;

# COMMAND ----------

# DBTITLE 1,MHS01 National; Disability Code
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Disability' AS BREAKDOWN
            ,DisabCode AS PRIMARY_LEVEL
            ,DisabCode_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  DisabCode, DisabCode_Desc;

# COMMAND ----------

# DBTITLE 1,MHS01 National; Employment Status
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Employment Status' AS BREAKDOWN
            ,EmployStatus AS PRIMARY_LEVEL
            ,EmployStatus_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  EmployStatus, EmployStatus_Desc;

# COMMAND ----------

# DBTITLE 1,MHS01 National; Lower Ethnicity
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Ethnicity' AS BREAKDOWN
            ,LowerEthnicity AS PRIMARY_LEVEL
            ,LowerEthnicity_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB          
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  LowerEthnicity, LowerEthnicity_Desc;

# COMMAND ----------

# DBTITLE 1,MHS01 National; Der Gender
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Gender' AS BREAKDOWN
            ,Der_Gender AS PRIMARY_LEVEL
            ,Der_Gender AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  Der_Gender;

# COMMAND ----------

# DBTITLE 1,National; IMD Decile
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; IMD Decile' AS BREAKDOWN
            ,IMD_Decile AS PRIMARY_LEVEL
            ,IMD_Decile AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  IMD_Decile;

# COMMAND ----------

# DBTITLE 1,MHS01 National; Sexual Orientation
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Sexual Orientation' AS BREAKDOWN
            ,Sex_Orient AS PRIMARY_LEVEL
            ,Sex_Orient AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB          
      FROM  $db_output.tmp_mhmab_mhs01_prep
  GROUP BY  Sex_Orient;

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
            CAST (COALESCE( cast(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
            '$db_source' AS SOURCE_DB
            
       FROM $db_output.MHS101Referral_open_end_rp AS REF		
      WHERE REF.AMHServiceRefEndRP_temp = TRUE

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
            CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
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
            CAST (COALESCE( CAST(COUNT (DISTINCT REF.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
            '$db_source' AS SOURCE_DB
            
       FROM $db_output.MHS101Referral_open_end_rp AS REF
      WHERE REF.LDAServiceRefEndRP_temp = true;

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
            
 --Commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 -- LEFT JOIN $db_source.MHS008CrisisPlan AS CRSold
 -- 		   ON MPI.Person_ID = CRSold.Person_ID 
 --            AND CRSold.UniqMonthID <= '$month_id' 
 
     WHERE 
 --Commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 --(
                      (CarePlanTypeMH = '12' AND CRS.Person_ID IS NOT NULL 
                      AND ((CRS.CarePlanCreatDate <= '$rp_enddate' 
                      AND CRS.CarePlanCreatDate >= DATE_ADD(ADD_MONTHS('$rp_enddate',-12),1))
                      OR (CRS.CarePlanLastUpdateDate <= '$rp_enddate' 
                      AND CRS.CarePlanLastUpdateDate >= DATE_ADD(ADD_MONTHS( '$rp_enddate', -12),1))))
                      
 --Commenting this section of code out to exclude the need for table MHS008CrisisPlan in the source data
 -- OR 
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
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM global_temp.MHS23abc_prep D
         INNER JOIN $db_output.validcodes as vc
             ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'crisis_resolution' and vc.type = 'include' and D.ServTeamTypeRefToMH = vc.ValidValue  
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
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
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM global_temp.MHS23abc_prep
        WHERE ServTeamTypeRefToMH = 'A17';

# COMMAND ----------

# DBTITLE 1,MHS23d National 
  %sql
  INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             
             ,'$db_source' AS SOURCE_DB            
        FROM global_temp.MHS23d_prep ; 

# COMMAND ----------

# DBTITLE 1,MHS23d National -Grouped by Age Group
 
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
    
  SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.MHS23d_prep
    GROUP BY AgeGroup;

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
             
 FROM		global_temp.MHS29abc_prep D -- prep table in main monthly prep folder
 INNER JOIN $db_output.validcodes as vc
             ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'crisis_resolution' and vc.type = 'include' and D.ServTeamTypeRefToMH = vc.ValidValue  
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 -- WHERE		ServTeamTypeRefToMH IN ('A02', 'A03')

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

# DBTITLE 1,MHS29d-National/England
 %sql
 --MHS29d - CONTACTS IN REPORTING PERIOD, National**/
  
 INSERT INTO $db_output.Main_monthly_unformatted_exp
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder

# COMMAND ----------

 %sql
 --MHS29d - CONTACTS IN REPORTING PERIOD, National, Age Group**/
  
 INSERT INTO $db_output.Main_monthly_unformatted_exp
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29d' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS29d_prep -- prep table in main monthly prep folder
 GROUP BY    AgeGroup, AgeGroupName

# COMMAND ----------

 %sql
 --MHS29f National Attendance --only breakdown needed at the national level for MHS29f
 INSERT INTO $db_output.Main_monthly_unformatted_exp
  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Attendance' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,Attend_Code AS SECONDARY_LEVEL
             ,Attend_Name AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS29f' AS MEASURE_ID
             ,try_CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)    AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM       global_temp.MHS29f_prep -- prep table in main monthly prep folder
 GROUP BY    Attend_Code, Attend_Name

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
             
 FROM		global_temp.MHS29abc_prep D -- prep table in main monthly prep folder
 INNER JOIN $db_output.validcodes as vc
             ON vc.table = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'crisis_resolution' and vc.type = 'include' and D.ServTeamTypeRefToMH = vc.ValidValue  
              and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 WHERE		AttendOrDNACode IN ('5', '6')
 -- 			AND ServTeamTypeRefToMH IN ('A02', 'A03')

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

# DBTITLE 1,MHS30f National
 %sql
 --MHS30f - CONTACTS IN REPORTING PERIOD, National**/
 INSERT INTO $db_output.Main_monthly_unformatted_exp  
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB           
 FROM        $db_output.tmp_mhmab_mhs30f_prep -- prep table in main monthly prep folder
 WHERE       AttendOrDNACode IN ('5', '6');

# COMMAND ----------

# DBTITLE 1,MHS30f National; Age Band
 %sql
 --MHS30f - CONTACTS IN REPORTING PERIOD, National; Age Band**/
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB           
 FROM        $db_output.tmp_mhmab_mhs30f_prep -- prep table in main monthly prep folder
 WHERE       AttendOrDNACode IN ('5', '6')
 GROUP BY    AgeGroup,AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS30f National; ConsMechanismMH
 %sql
 --MHS30f - CONTACTS IN REPORTING PERIOD, National, CMU **/
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT 
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; ConsMechanismMH' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30f' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB        
 FROM        $db_output.tmp_mhmab_mhs30f_prep -- prep table in main monthly prep folder
 WHERE       AttendOrDNACode IN ('5', '6')
 GROUP BY    ConsMedUsed,CMU

# COMMAND ----------

# DBTITLE 1,MHS30h National; ConsMechanismMH
 %sql
  
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; ConsMechanismMH' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,ConsMedUsed AS SECONDARY_LEVEL
             ,CMU AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS30h' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(UniqCareContID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB            
 FROM        $db_output.tmp_mhmab_mhs30h_prep
 WHERE        AttendOrDNACode IN ('5', '6')
 GROUP BY    ConsMedUsed
             ,CMU

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

# DBTITLE 1,MHS32 National; Accommodation Type
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Accommodation Type' AS BREAKDOWN
            ,AccommodationType AS PRIMARY_LEVEL
            ,AccommodationType_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  AccommodationType, AccommodationType_Desc;

# COMMAND ----------

# DBTITLE 1,MHS32 National; Age Band
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age' AS BREAKDOWN
            ,Age_Band AS PRIMARY_LEVEL
            ,Age_Band AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  Age_Band

# COMMAND ----------

# DBTITLE 1,MHS32 National; Disability
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Disability' AS BREAKDOWN
            ,DisabCode AS PRIMARY_LEVEL
            ,DisabCode_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT(DISTINCT(UniqServReqID)) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB           
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  DisabCode, DisabCode_Desc

# COMMAND ----------

# DBTITLE 1,MHS32 National; Employment Status
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Employment Status' AS BREAKDOWN
            ,EmployStatus AS PRIMARY_LEVEL
            ,EmployStatus_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  EmployStatus, EmployStatus_Desc

# COMMAND ----------

# DBTITLE 1,MHS32 National; Lower Ethnicity
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Ethnicity' AS BREAKDOWN
            ,LowerEthnicity AS PRIMARY_LEVEL
            ,LowerEthnicity_Desc AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  LowerEthnicity, LowerEthnicity_Desc

# COMMAND ----------

# DBTITLE 1,MHS32 National; Der_Gender
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Gender' AS BREAKDOWN
            ,Der_Gender AS PRIMARY_LEVEL
            ,Der_Gender AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  Der_Gender

# COMMAND ----------

# DBTITLE 1,MHS32 National; IMD Decile
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; IMD Decile' AS BREAKDOWN
            ,IMD_Decile AS PRIMARY_LEVEL
            ,IMD_Decile AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  IMD_Decile

# COMMAND ----------

# DBTITLE 1,MHS32 National; Sexual Orientation
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Sexual Orientation' AS BREAKDOWN
            ,Sex_Orient AS PRIMARY_LEVEL
            ,Sex_Orient AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32_prep
  GROUP BY  Sex_Orient

# COMMAND ----------

# DBTITLE 1,MHS32c National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32c' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32c_prep;

# COMMAND ----------

# DBTITLE 1,MHS32c National; Age Group
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age Group' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,AgeGroup AS SECONDARY_LEVEL
            ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32c' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32c_prep
  GROUP BY  AgeGroup,AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS32d National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32d' AS MEASURE_ID
            ,COUNT (DISTINCT UniqServReqID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_mhs32d_prep;

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

# DBTITLE 1,MHS57b National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL 
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID
             ,CAST(COALESCE(COUNT(DISTINCT(Person_ID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB          
 FROM        $db_output.tmp_mhmab_mhs57b_prep;

# COMMAND ----------

# DBTITLE 1,MHS57b National; Age Group
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL 
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL 
             ,AgeGroupName AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST(COALESCE(COUNT(DISTINCT(Person_ID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB          
 FROM        $db_output.tmp_mhmab_mhs57b_prep
 GROUP BY    AgeGroup,AgeGroupName;

# COMMAND ----------

# DBTITLE 1,MHS57c National
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted_exp
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL 
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57c' AS MEASURE_ID 
             ,CAST(COALESCE(COUNT(DISTINCT(Person_ID)),0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB       
 FROM        $db_output.tmp_mhmab_mhs57c_prep;

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7071_prep prep
        
        INNER JOIN $db_output.validcodes as vc
         ON vc.table = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
         and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7071_prep prep
        
        INNER JOIN $db_output.validcodes as vc
         ON vc.table = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
         and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 
 --       WHERE ClinRespPriorityType = '1'
             WHERE AGE_GROUP = '18 and over';

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7071_prep prep
        
        INNER JOIN $db_output.validcodes as vc
         ON vc.table = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
         and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 
             WHERE AGE_GROUP = '0-17';

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7273_prep prep
        
        INNER JOIN $db_output.validcodes as vc
         ON vc.table = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
         and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7273_prep prep
        
        INNER JOIN $db_output.validcodes as vc
         ON vc.table = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
         and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 
             WHERE AGE_GROUP = '18 and over';

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7273_prep prep
        
        INNER JOIN $db_output.validcodes as vc
         ON vc.table = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
         and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
 
             WHERE AGE_GROUP = '0-17';

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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
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
             ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.CCR7273_prep
       WHERE ClinRespPriorityType = '2'
             AND AGE_GROUP = '0-17';