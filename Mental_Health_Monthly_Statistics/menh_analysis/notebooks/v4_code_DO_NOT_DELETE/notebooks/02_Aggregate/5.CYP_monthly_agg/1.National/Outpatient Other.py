# Databricks notebook source
# DBTITLE 1,MH01a National - commented out
 %sql
 -- removed 02/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly 

 --MH01a - PEOPLE IN CONTACT WITH MENTAL HEALTH SERVICES AT END OF REPORTING PERIOD, AGED 0-18**/
 -- has both monthly and camhs monthly outputs

 -- INSERT INTO $db_output.CYP_monthly_unformatted -- needs changing to CYP monthly table MS: I have updated this in order to get the code to run. This needs checking by MENH SME.

 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --             ,'$rp_enddate' AS REPORTING_PERIOD_END
 --             ,'$status' AS STATUS
 --     	    ,'England' AS BREAKDOWN
 --     	    ,'England' AS PRIMARY_LEVEL
 --     	    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 --     	    ,'NONE' AS SECONDARY_LEVEL
 -- 			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 -- 			,'MH01a' AS METRIC
 -- 			,CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
 -- FROM		global_temp.MH01_prep	 -- prep table in main monthly prep folder 
 -- WHERE  		AGE_GROUP = '00-18'


# COMMAND ----------

# DBTITLE 1,CYP01-i - commented out
 %sql

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly

 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --           ,'$rp_enddate' AS REPORTING_PERIOD_END
 --           ,'$status' AS STATUS
 --           ,'England' AS BREAKDOWN
 --           ,'England' AS PRIMARY_LEVEL
 --           ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 --           ,'NONE' AS SECONDARY_LEVEL
 --           ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 --           ,'CYP01' AS METRIC
 --           ,COUNT (DISTINCT Person_ID) AS METRIC_VALUE
 --      FROM $db_output.MHS101Referral_open_end_rp AS REF
 --     WHERE REF.CYPServiceRefEndRP_temp = true;

# COMMAND ----------

# DBTITLE 1,CYP02
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
            ,'CYP02' AS METRIC
            ,CAST(COALESCE(CAST(COUNT (DISTINCT Person_ID)AS INT) , 0) AS STRING) AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
       FROM global_temp.CYP02_prep;

# COMMAND ----------

# DBTITLE 1,CYP23-i
 %sql

 -- removed 14/08/2019 due to being duplicated in final formatted CSV output - measure still in main monthly
 -- User note: 29-07-2021 - despite the comment above this code was still live (and also exists in main monthly) - commenting this one out as the England total is appearing twice in the unrounded outputs.


 -- INSERT INTO $db_output.CYP_monthly_unformatted
 --     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
 --            ,'$rp_enddate' AS REPORTING_PERIOD_END
 --            ,'$status' AS STATUS
 --            ,'England' AS BREAKDOWN
 --            ,'England' AS PRIMARY_LEVEL
 --            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
 --            ,'NONE' AS SECONDARY_LEVEL
 --            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 --            ,'CYP23' AS METRIC
 --            ,COUNT (DISTINCT UniqServReqID) AS METRIC_VALUE
 --            ,'$db_source' AS SOURCE_DB
 --       FROM $db_output.MHS101Referral_open_end_rp
 --      WHERE CYPServiceRefEndRP_temp = true;

# COMMAND ----------

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
             ,'England; ConsMediumUsed' AS BREAKDOWN
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
            ,'England; ConsMediumUsed' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,case 
               when b.ConsMediumUsed is NULL then 'Missing'
               when b.ConsMediumUsed not in ('01','02','03','04','05','06','98') then 'Invalid'
               else b.ConsMediumUsed
             end as SECONDARY_LEVEL
           ,case
               when b.ConsMediumUsed is NULL then 'Missing'
               when b.ConsMediumUsed = '01' then 'Face to face communication'
               when b.ConsMediumUsed = '02' then 'Telephone'
               when b.ConsMediumUsed = '03' then 'Telemedicine web camera'
               when b.ConsMediumUsed = '04' then 'Talk type for a person unable to speak'
               when b.ConsMediumUsed = '05' then 'Email'
               when b.ConsMediumUsed = '06' then 'Short Message Service (SMS) - Text Messaging'
               when b.ConsMediumUsed = '98' then 'Other'
               else 'Invalid'
            end as SECONDARY_LEVEL_DESCRIPTION
           ,'MHS61b' as METRIC
           ,cast(coalesce(cast(count(distinct a.UniqCareContID)as int) , 0) as string) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
      from global_temp.MHS61b as a
      inner join $db_source.MHS201carecontact as b
      on a.uniqcarecontid = b.uniqcarecontid and a.carecontdate = b.carecontdate and b.uniqmonthid = '$month_id' --and b.ic_use_submission_flag = 'Y'
     where RN=1 and  a.carecontdate between '$rp_startdate' and '$rp_enddate'
  group by case 
             when b.ConsMediumUsed is NULL then 'Missing'
             when b.ConsMediumUsed not in ('01','02','03','04','05','06','98') then 'Invalid'
             else b.ConsMediumUsed
            end
         ,case
             when b.ConsMediumUsed is NULL then 'Missing'
             when b.ConsMediumUsed = '01' then 'Face to face communication'
             when b.ConsMediumUsed = '02' then 'Telephone'
             when b.ConsMediumUsed = '03' then 'Telemedicine web camera'
             when b.ConsMediumUsed = '04' then 'Talk type for a person unable to speak'
             when b.ConsMediumUsed = '05' then 'Email'
             when b.ConsMediumUsed = '06' then 'Short Message Service (SMS) - Text Messaging'
             when b.ConsMediumUsed = '98' then 'Other'
             else 'Invalid'
           end   ;

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
      