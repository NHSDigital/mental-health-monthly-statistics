# Databricks notebook source
# DBTITLE 1,MHS30d National
 %sql
 /************ MHS30d - Attended care contacts in the RP, 0-18 ************/
 
 INSERT INTO $db_output.CYP_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30d' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS30d_Prep 

# COMMAND ----------

# DBTITLE 1,MHS30e National
 %sql
 ----- MHS30e -  Attended contacts in the RP, 0-18, by consultation medium -----
 
 INSERT INTO $db_output.CYP_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
             ,'England; ConsMechanismMH' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,ConsMedUsed AS SECONDARY_LEVEL
 			,CMU AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30e' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS30d_Prep  
 GROUP BY    ConsMedUsed
             ,CMU
 --ORDER BY    SECONDARY_LEVEL

# COMMAND ----------

# DBTITLE 1,CYP32
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CYP32' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.CYP32_prep;

# COMMAND ----------

# DBTITLE 1,CYP32a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CYP32a' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.CYP32_prep
      WHERE AgeServReferRecDate BETWEEN 0 AND 18;

# COMMAND ----------

# DBTITLE 1,MHS32a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32a' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MH32_prep;

# COMMAND ----------

# DBTITLE 1,MHS32a(source)
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Referral Source' AS BREAKDOWN 
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,Referral_Source AS SECONDARY_LEVEL
            ,Referral_Description AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32a' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MH32_prep
  GROUP BY  Referral_Source, 
            Referral_Description;

# COMMAND ----------

# DBTITLE 1,MHS32b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS32b' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MH32_prep
     WHERE  SourceOfReferralMH='B1';

# COMMAND ----------

# DBTITLE 1,MHS38a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS38a' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS3839_prep
      WHERE Age BETWEEN 0 AND 18;

# COMMAND ----------

# DBTITLE 1,MHS38b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS38b' AS METRIC
            ,COUNT (DISTINCT PREP.UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS3839_prep AS PREP
 INNER JOIN $db_source.mhs204indirectactivity as IND
            ON PREP.UniqServReqID=IND.UniqServReqID 
            AND IND.UniqMonthID = '$month_id'
      WHERE PREP.AgeServReferRecDate BETWEEN 0 AND 18
            AND IND.IndirectActDate BETWEEN '$rp_startdate' AND '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,MHS39a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS39a' AS METRIC
            ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.MHS3839_prep
      WHERE AgeServReferRecDate BETWEEN 0 AND 18
            AND ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,MHS40
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS40' AS METRIC
            ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MHS404142_prep
      WHERE LACStatus=TRUE ;

# COMMAND ----------

# DBTITLE 1,MHS41
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS41' AS METRIC
            ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MHS404142_prep
      WHERE CPP='3'  ;

# COMMAND ----------

# DBTITLE 1,MHS42
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS42' AS METRIC
            ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MHS404142_prep
      WHERE YoungCarer=TRUE ;

# COMMAND ----------

# DBTITLE 1,MHS55a
 %sql
 /******** MHS55a - People attending at least one contact in the RP, 0-18 ***********/
 
 INSERT INTO $db_output.CYP_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS55a' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS30d_prep  -- prep table in CYP monthly prep folder 

# COMMAND ----------

# DBTITLE 1,MHS56a National
 %sql
 /*********** MHS56a - People with indirect activity in the RP, 0-18 ****************/
 
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS56a' AS METRIC
             ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS56a_Prep
             

# COMMAND ----------

# DBTITLE 1,MHS57a National
 %sql
 /*** MHS57a - People discharged from the service in the RP, 0-18 ***/
 
 -- National 
 
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		 	,'England' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL 
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL 
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
 			,'MHS57a' AS METRIC 
 			,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS57a_Prep
 			

# COMMAND ----------

# DBTITLE 1,MHS58a National V2
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,'England; DNA Reason' AS BREAKDOWN
 			,'England' AS PRIMARY_LEVEL 
 			,'England' AS PRIMARY_LEVEL_DESCRIPTION
 			,AttendOrDNACode AS SECONDARY_LEVEL
             ,DNA_Reason AS SECONDARY_LEVEL_DESCRIPTION 
 			,'MHS58a' AS METRIC 
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS58a_Prep
 GROUP BY    AttendOrDNACode
             ,DNA_Reason

# COMMAND ----------

# DBTITLE 1,MHS61a
 %sql
 /****** MHS61a - Referrals with their first attended contact in the RP, aged 0-18 *******/
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS61a' AS METRIC
            ,CAST (COALESCE(CAST(COUNT (DISTINCT UniqServReqID) AS INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MHS61_prep
      WHERE FirstCareContDate between '$rp_startdate' and '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,MHS61b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
           ,'$status' AS STATUS
           ,'England; ConsMechanismMH' AS BREAKDOWN
           ,'England' AS PRIMARY_LEVEL
           ,'England' AS PRIMARY_LEVEL_DESCRIPTION
           ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level, 'Invalid')
                 END as SECONDARY_LEVEL
           ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level_description, 'Invalid')
                 END as SECONDARY_LEVEL_DESCRIPTION
           ,'MHS61b' as METRIC
           ,cast(coalesce(cast(count(distinct a.UniqCareContID)as int) , 0) as string) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
      from global_temp.MHS61b as a
      
      inner join $db_source.MHS201carecontact as b
      on a.uniqcarecontid = b.uniqcarecontid and a.carecontdate = b.carecontdate and b.uniqmonthid = '$month_id' --and b.ic_use_submission_flag = 'Y'
      
      left join $db_output.ConsMechanismMH as cm
      ON b.ConsMechanismMH = cm.level
      
     where RN=1 and  a.carecontdate between '$rp_startdate' and '$rp_enddate'
  group by CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level, 'Invalid')
                 END
             ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level_description, 'Invalid')
                 END;

# COMMAND ----------

# DBTITLE 1,MHS68
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS68' AS METRIC
            ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.MHS68_prep;
      