# Databricks notebook source
# DBTITLE 1,MH01a CCG - commented out
 %sql
 -- removed 02/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly 

 --MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18
 -- in both monthly and cahms monthly outputs

 -- INSERT INTO $db_output.CYP_monthly_unformatted

 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --     	    ,'CCG - GP Practice or Residence' AS BREAKDOWN
 --     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
 --     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 --     	    ,'NONE' AS SECONDARY_LEVEL
 -- 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 -- 			,'MH01a' AS METRIC
 -- 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
 --             --,'' AS EFFECTIVE_FROM
 --             --,'' AS EFFECTIVE_TO
 -- FROM	    global_temp.MH01_prep	 -- prep table in main monthly prep folder
 -- WHERE		AGE_GROUP = '00-18'
 -- GROUP BY	IC_Rec_CCG
 -- 			,NAME
             

# COMMAND ----------

# DBTITLE 1,CYP01-i - commented out
 %sql

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly

 -- INSERT INTO $db_output.CYP_monthly_unformatted

 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --            ,'$rp_enddate' AS REPORTING_PERIOD_END
 --            ,'$status' AS STATUS
 -- 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 --     	   ,IC_Rec_CCG AS PRIMARY_LEVEL
 --     	   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
 --     	   ,'NONE' AS SECONDARY_LEVEL
 -- 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 --            ,'CYP01' AS METRIC
 --            ,COUNT (DISTINCT REF.Person_ID) AS METRIC_VALUE
 --        FROM $db_output.MHS101Referral_open_end_rp AS REF
 --  INNER JOIN $db_output.MHS001MPI_latest_month_data  AS MPI
 --             ON REF.Person_ID = MPI.Person_ID        
 --       WHERE REF.CYPServiceRefEndRP_temp = True             
 --    GROUP BY IC_Rec_CCG
 --             ,NAME;

# COMMAND ----------

# DBTITLE 1,CYP02
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	   ,IC_Rec_CCG AS PRIMARY_LEVEL
     	   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	   ,'NONE' AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CYP02' AS METRIC
            ,'$db_source' AS SOURCE_DB
            ,CAST(COALESCE(CAST(COUNT (DISTINCT Person_ID)AS INT) , 0) AS STRING) AS METRIC_VALUE
       FROM global_temp.CYP02_prep
   GROUP BY IC_Rec_CCG
            ,NAME;

# COMMAND ----------

# DBTITLE 1,MHS21a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	   ,IC_Rec_CCG AS PRIMARY_LEVEL
     	   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	   ,'NONE' AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS21a' AS METRIC
            ,COUNT (DISTINCT UniqWardStayID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
       FROM $db_output.MHS21_prep
      WHERE AGE_GROUP = '00-18'
   GROUP BY IC_Rec_CCG
            ,NAME;

# COMMAND ----------

# DBTITLE 1,CYP23-i - commented out
 %sql

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly

 -- INSERT INTO $db_output.CYP_monthly_unformatted

 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --            ,'$rp_enddate' AS REPORTING_PERIOD_END
 --            ,'$status' AS STATUS
 -- 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
 --     	   ,MPI.IC_Rec_CCG AS PRIMARY_LEVEL
 --     	   ,MPI.NAME AS PRIMARY_LEVEL_DESCRIPTION
 --     	   ,'NONE' AS SECONDARY_LEVEL
 -- 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 --            ,'CYP23' AS METRIC
 --            ,COUNT (DISTINCT REF.UniqServReqID) AS METRIC_VALUE
 --       FROM $db_output.MHS101Referral_open_end_rp	AS REF	
 -- INNER JOIN $db_output.MHS001MPI_latest_month_data	AS MPI
 -- 		   ON REF.Person_ID = MPI.Person_ID 	
 --      WHERE CYPServiceRefEndRP_temp = true
 --   GROUP BY MPI.IC_Rec_CCG, MPI.NAME;

# COMMAND ----------

# DBTITLE 1,MHS30d CCG
 %sql
 ----- MHS30d - Attended care contacts in the RP, 0-18 -----
            
 INSERT INTO $db_output.CYP_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30d' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS30d_Prep AS A  -- prep table in main monthly prep folder 
 GROUP BY	IC_Rec_CCG
             ,NAME

# COMMAND ----------

# DBTITLE 1,MHS30e CCG
 %sql
 ----- MHS30e -  Attended contacts in the RP, 0-18, by consultation medium ----- 
             
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CCG - GP Practice or Residence; ConsMediumUsed' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,ConsMedUsed AS SECONDARY_LEVEL
 			,CMU AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS30e' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) as INT), 0) AS STRING)	AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM        global_temp.MHS30d_Prep  -- prep table in main monthly prep folder 
 GROUP BY    ConsMedUsed
             ,CMU
             ,IC_Rec_CCG
             ,NAME
 ORDER BY    PRIMARY_LEVEL
             ,SECONDARY_LEVEL

# COMMAND ----------

# DBTITLE 1,MHS32a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MH32_prep
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CYP32
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 		    ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'CYP32' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
       FROM  global_temp.CYP32_prep
    GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,CYP32a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	   ,IC_Rec_CCG AS PRIMARY_LEVEL
     	   ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	   ,'NONE' AS SECONDARY_LEVEL
 		   ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CYP32a' AS METRIC
            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  global_temp.CYP32_prep
      WHERE AgeServReferRecDate BETWEEN 0 AND 18
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS32a(source)
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence; Referral Source' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,Referral_Source AS SECONDARY_LEVEL
             ,Referral_Description AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MH32_prep
   GROUP BY IC_Rec_CCG,NAME,Referral_Source,Referral_Description
   ORDER BY IC_Rec_CCG;

# COMMAND ----------

# DBTITLE 1,MHS32b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS32b' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MH32_prep
     WHERE  SourceOfReferralMH='B1'
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS38a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS38a' AS METRIC
             ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
       FROM global_temp.MHS3839_prep
      WHERE Age BETWEEN 0 AND 18
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS38b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,PREP.IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,PREP.NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS38b' AS METRIC
             ,COUNT (DISTINCT PREP.UniqServReqID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.MHS3839_prep AS PREP
  INNER JOIN $db_source.MHS204IndirectActivity as IND
             ON PREP.UniqServReqID=IND.UniqServReqID 
             AND IND.UniqMonthID = '$month_id'
       WHERE PREP.AgeServReferRecDate BETWEEN 0 AND 18
             AND IND.IndirectActDate BETWEEN '$rp_startdate' AND '$rp_enddate'
        GROUP BY PREP.IC_Rec_CCG, PREP.NAME;

# COMMAND ----------

# DBTITLE 1,MHS39a
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS39a' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
        FROM global_temp.MHS3839_prep
       WHERE AgeServReferRecDate BETWEEN 0 AND 18
             AND ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate'
       GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS40
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted

     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS40' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MHS404142_prep
      WHERE LACStatus=TRUE 
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS41
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS41' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
      FROM global_temp.MHS404142_prep
      WHERE CPP='3' 
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS42
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS42' AS METRIC
             ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MHS404142_prep
      WHERE YoungCarer=TRUE
   GROUP BY IC_Rec_CCG, NAME;

# COMMAND ----------

# DBTITLE 1,MHS55a CCG
 %sql
 ----- MHS55a - People attending at least one contact in the RP, 0-18 -----

 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS55a' AS METRIC
             ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM        global_temp.MHS30d_prep  -- prep table in main monthly prep folder 
 GROUP BY   IC_Rec_CCG
            ,NAME

# COMMAND ----------

# DBTITLE 1,MHS56a CCG
 %sql

 ----- MHS56a - People with indirect activity in the RP, 0-18 -----

 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS56a' AS METRIC
             ,CAST (COALESCE( CAST(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM        global_temp.MHS56a_Prep
 GROUP BY   IC_Rec_CCG
            ,NAME

# COMMAND ----------

# DBTITLE 1,MHS57a CCG
 %sql
 /*** MHS57a - People discharged from the service in the RP, 0-18 ***/

 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL 
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,'NONE' AS SECONDARY_LEVEL 
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
 			,'MHS57a' AS METRIC 
 			,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS57a_Prep
 GROUP BY 	IC_Rec_CCG
 			,NAME

# COMMAND ----------

# DBTITLE 1,MHS58a CCG V2
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 			,'CCG - GP Practice or Residence; DNA Reason' AS BREAKDOWN
 			,IC_Rec_CCG AS PRIMARY_LEVEL 
 			,NAME AS PRIMARY_LEVEL_DESCRIPTION
 			,AttendOrDNACode AS SECONDARY_LEVEL 
             ,DNA_Reason AS SECONDARY_LEVEL_DESCRIPTION
 			,'MHS58a' AS METRIC 
 			,CAST (COALESCE (cast(COUNT (DISTINCT UniqCareContID) AS INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
 FROM		global_temp.MHS58a_Prep
 GROUP BY    IC_Rec_CCG
             ,NAME
             ,AttendOrDNACode
             ,DNA_Reason

# COMMAND ----------

# DBTITLE 1,MHS61a
 %sql
 /****** MHS61a - Referrals with their first attended contact in the RP, aged 0-18 *******/
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		    ,'MHS61a' AS METRIC
            ,CAST(COALESCE(CAST(COUNT (DISTINCT UniqServReqID) AS INT), 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MHS61a_CCG_Prov
     GROUP BY IC_REC_CCG, NAME
   ORDER BY Primary_Level;

# COMMAND ----------

# DBTITLE 1,MHS61b
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
            ,'CCG - GP Practice or Residence; ConsMediumUsed' AS BREAKDOWN
            ,IC_Rec_CCG AS PRIMARY_LEVEL
            ,NAME AS PRIMARY_LEVEL_DESCRIPTION
            ,case 
               when ConsMediumUsed is NULL then 'Missing'
               when ConsMediumUsed not in ('01','02','03','04','05','06','98') then 'Invalid'
               else ConsMediumUsed
             end as SECONDARY_LEVEL
           ,case
               when ConsMediumUsed is NULL then 'Missing'
               when ConsMediumUsed = '01' then 'Face to face communication'
               when ConsMediumUsed = '02' then 'Telephone'
               when ConsMediumUsed = '03' then 'Telemedicine web camera'
               when ConsMediumUsed = '04' then 'Talk type for a person unable to speak'
               when ConsMediumUsed = '05' then 'Email'
               when ConsMediumUsed = '06' then 'Short Message Service (SMS) - Text Messaging'
               when ConsMediumUsed = '98' then 'Other'
               else 'Invalid'
            end as SECONDARY_LEVEL_DESCRIPTION
           ,'MHS61b' as METRIC
           ,cast(coalesce(cast(count (distinct UniqCareContID)as int) , 0) as string) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
      from global_temp.MHS61b_ccg
  group by IC_Rec_CCG
           ,NAME
           ,case 
             when ConsMediumUsed is NULL then 'Missing'
             when ConsMediumUsed not in ('01','02','03','04','05','06','98') then 'Invalid'
             else ConsMediumUsed
            end
         ,case
             when ConsMediumUsed is NULL then 'Missing'
             when ConsMediumUsed = '01' then 'Face to face communication'
             when ConsMediumUsed = '02' then 'Telephone'
             when ConsMediumUsed = '03' then 'Telemedicine web camera'
             when ConsMediumUsed = '04' then 'Talk type for a person unable to speak'
             when ConsMediumUsed = '05' then 'Email'
             when ConsMediumUsed = '06' then 'Short Message Service (SMS) - Text Messaging'
             when ConsMediumUsed = '98' then 'Other'
             else 'Invalid'
           end;
         

# COMMAND ----------

# DBTITLE 1,MHS68
 %sql
 INSERT INTO $db_output.CYP_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
 	        ,'CCG - GP Practice or Residence' AS BREAKDOWN
     	    ,IC_Rec_CCG AS PRIMARY_LEVEL
     	    ,NAME AS PRIMARY_LEVEL_DESCRIPTION
     	    ,'NONE' AS SECONDARY_LEVEL
 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 		    ,'MHS68' AS METRIC
            --,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
            ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  global_temp.MHS68_prep
   GROUP BY IC_REC_CCG, NAME;