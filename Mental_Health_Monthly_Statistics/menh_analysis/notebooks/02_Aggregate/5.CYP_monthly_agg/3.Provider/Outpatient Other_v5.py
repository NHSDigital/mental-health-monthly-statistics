# Databricks notebook source
# DBTITLE 1,MHS30d Provider
 %sql
 /************ MHS30d - Attended care contacts in the RP, 0-18 ************/
 
 
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30d' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS30d_prep  -- prep table in main monthly prep folder 
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS30e Provider
 %sql
 ----- MHS30e -  Attended contacts in the RP, 0-18, by consultation medium -----
 
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider; ConsMechanismMH' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,ConsMedUsed AS SECONDARY_LEVEL
 			,CMU AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30e' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS30d_Prep  -- prep table in main monthly prep folder
 --WHERE       patmrecinrp = 'Y'
 GROUP BY    OrgIDProv
             ,ConsMedUsed
             ,CMU
 ORDER BY    PRIMARY_LEVEL
             ,SECONDARY_LEVEL

# COMMAND ----------

# DBTITLE 1,CYP32
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'CYP32' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  global_temp.CYP32_prep_prov
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,CYP32a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'CYP32a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM  global_temp.CYP32_prep_prov
       WHERE AgeServReferRecDate BETWEEN 0 AND 18
    GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS32a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM global_temp.MH32_prep_prov
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS32a(source)
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider; Referral Source' AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,Referral_Source AS SECONDARY_LEVEL
             ,Referral_Description AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM global_temp.MH32_prep_prov 
   GROUP BY OrgIDProv,Referral_Source,Referral_Description
   ORDER BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS32b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32b' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM  global_temp.MH32_prep_prov
     WHERE  SourceOfReferralMH='B1'
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS38a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS38a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.MHS3839_prep_prov
       WHERE Age BETWEEN 0 AND 18
    GROUP BY OrgIDProv; 

# COMMAND ----------

# DBTITLE 1,MHS38b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,PREP.OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS38b' AS METRIC
             ,COUNT (DISTINCT PREP.UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM global_temp.MHS3839_prep_prov AS PREP
  INNER JOIN $db_source.MHS204IndirectActivity as IND
             ON PREP.UniqServReqID=IND.UniqServReqID 
             AND IND.UniqMonthID = '$month_id'
             AND PREP.OrgIDProv = IND.OrgIDProv
       WHERE PREP.AgeServReferRecDate BETWEEN 0 AND 18
             AND IND.IndirectActDate BETWEEN '$rp_startdate' AND '$rp_enddate'
        GROUP BY PREP.OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS39a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS39a' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM global_temp.MHS3839_prep_prov
       WHERE AgeServReferRecDate BETWEEN 0 AND 18
             AND ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate'
    GROUP BY OrgIDProv; 

# COMMAND ----------

# DBTITLE 1,MHS40
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS40' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM  global_temp.MHS404142_prep_prov
      WHERE LACStatus=TRUE 
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS41
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS41' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM   global_temp.MHS404142_prep_prov
      WHERE CPP='3' 
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS42
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS42' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
      FROM   global_temp.MHS404142_prep_prov
      WHERE YoungCarer=TRUE 
   GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS55a Provider
 %sql
 ----- MHS55a - People attending at least one contact in the RP, 0-18 -----
 
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS55a' AS METRIC
             ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS30d_Prep  -- prep table in main monthly prep folder 
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS56a - Provider
 %sql
 /*********** MHS56a - People with indirect activity in the RP, 0-18 ****************/
 
 INSERT INTO $db_output.CYP_monthly_unformatted
 
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 		    ,'Provider'	AS BREAKDOWN
 			,OrgIDProv	AS PRIMARY_LEVEL
 			,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE'	AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS56a' AS METRIC
             ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        global_temp.MHS56a_Prep
 GROUP BY    OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS57a Provider
 %sql
 /*** MHS57a - People discharged from the service in the RP, 0-18 ***/
 
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS PRIMARY_LEVEL 
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL 
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
 			,'MHS57a' AS METRIC 
 			,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS57a_Prep
 GROUP BY 	OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS58a Provider V2
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,'Provider; DNA Reason' AS BREAKDOWN
 			,CASE WHEN OrgIDProv IS NULL THEN 'Missing' 
             ELSE OrgIDProv END AS PRIMARY_LEVEL 
 			,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
 			,AttendStatus AS SECONDARY_LEVEL 
 			,DNA_Reason AS SECONDARY_LEVEL_DESCRIPTION 
 			,'MHS58a' AS METRIC 
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM		global_temp.MHS58a_Prep
 GROUP BY    CASE WHEN OrgIDProv IS NULL THEN 'Missing' 
             ELSE OrgIDProv END
             ,AttendStatus
             ,DNA_Reason

# COMMAND ----------

# DBTITLE 1,MHS61a
 %sql
   INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
               ,'Provider'	AS BREAKDOWN
               ,OrgIDProv	AS PRIMARY_LEVEL
               ,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
               ,'NONE'	AS SECONDARY_LEVEL
               ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
               ,'MHS61a' AS METRIC
               ,CAST(COALESCE(CAST(COUNT (DISTINCT UniqServReqID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM  global_temp.MHS61a_CCG_Prov
         GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,MHS61b
 %sql
 -- Better to join to table ConsMechanismMH to decode ConsMechanismMH?
   INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'Provider; ConsMechanismMH' AS BREAKDOWN
              ,OrgIDProv AS PRIMARY_LEVEL
              ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
              ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level, 'Invalid')
                 END as SECONDARY_LEVEL
              ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level_description, 'Invalid')
                 END as SECONDARY_LEVEL_DESCRIPTION   
             ,'MHS61b' as METRIC
             ,cast(coalesce(cast(count (distinct UniqCareContID)as int), 0) as string) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        from global_temp.MHS61b_prov as b
        
         left join $db_output.ConsMechanismMH as cm
      ON b.ConsMechanismMH = cm.level
      
      group by OrgIDProv
             ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level, 'Invalid')
                 END
             ,CASE WHEN b.ConsMechanismMH IS NULL THEN 'Missing'
                 ELSE COALESCE(cm.level_description, 'Invalid')
                 END;

# COMMAND ----------

# DBTITLE 1,MHS68
 %sql
 /*** MHS68 - Referrals with any SNOMED Codes and valid PERS score from MH Assess Scale Current View completed in RP, aged 0-18***/
 
 INSERT INTO $db_output.CYP_monthly_unformatted		
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
        ,'Provider'	AS BREAKDOWN
        ,MPI.OrgIDProv	AS PRIMARY_LEVEL
        ,'NONE'	AS PRIMARY_LEVEL_DESCRIPTION
        ,'NONE'	AS SECONDARY_LEVEL
        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
        ,'MHS68' AS METRIC
 	   ,COUNT (DISTINCT REF.UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM	$db_source.MHS001MPI AS MPI
 INNER JOIN $db_source.MHS101Referral AS REF
 ON MPI.Person_ID = REF.Person_ID AND MPI.OrgIDProv = REF.OrgIDProv AND REF.UniqMonthID = '$month_id'-- AND REF.IC_Use_Submission_Flag = 'Y' --need in hue
 INNER JOIN $db_source.MHS606CodedScoreAssessmentRefer AS CSA
 ON REF.UniqServReqID = CSA.UniqServReqID -- AND CSA.IC_Use_Submission_Flag = 'Y' --need in hue
 WHERE	MPI.UniqMonthID = '$month_id'
 and CSA.UniqMonthID = '$month_id'
 --AND	MPI.IC_Use_Submission_Flag = 'Y' --need in hue
 AND REF.Person_ID IS NOT NULL
 AND CSA.Person_ID IS NOT NULL
 AND REF.ReferralRequestReceivedDate <= '$rp_enddate' -- New Referrals
 
 And  -- pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
   (csa.AssToolCompTimestamp between '$rp_startdate' and '$rp_enddate')
 AND REF.AgeServReferRecDate BETWEEN 0 AND 18 -- Age at Ref
 --AND MPI.IC_NAT_MRecent_IN_RP_FLAG = 'Y' -- use latest MPI record (not req for Provider)
 AND CSA.PersScore IN ('0', '1', '2', '3', '888', '999') -- Coded Assess valid Score
 -- Coded Assess Q1 to 30 or Context/Education --
 AND CSA.CodedAssToolType IN
 ('987251000000103','987261000000100','987271000000107','987281000000109','987291000000106'
 ,'987301000000105','987311000000107','987321000000101','987331000000104','987341000000108'
 ,'987351000000106','987361000000109','987371000000102','987381000000100','987391000000103'
 ,'987401000000100','987411000000103','987421000000109','987431000000106','987441000000102'
 ,'987451000000104','987461000000101','987471000000108','987481000000105','987491000000107'
 ,'987501000000101','987511000000104','987521000000105','987531000000107','987541000000103'
 ,'987191000000101','987201000000104','987211000000102','987221000000108','987231000000105','987241000000101')
 GROUP BY	MPI.OrgIDProv

# COMMAND ----------

