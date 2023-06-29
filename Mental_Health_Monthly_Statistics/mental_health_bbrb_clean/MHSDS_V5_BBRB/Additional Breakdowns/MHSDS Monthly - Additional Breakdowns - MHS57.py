# Databricks notebook source
#Teams are:
# A05	Primary Care Mental Health Service
# A06	Community Mental Health Team - Functional
# A08	Assertive Outreach Team
# A09	Community Rehabilitation Service
# A12	Psychotherapy Service
# A13	Psychological Therapy Service (non IAPT)
# A16	Personality Disorder Service
# C10	Community Eating Disorder Service

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS57b_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS57b_prep USING DELTA AS
     SELECT   MPI.Person_ID
             ,MPI.IC_REC_GP_RES
             ,MPI.NAME
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
     FROM    $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = '$end_month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID 
             AND Serv.UniqMonthID = '$end_month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate_1m' AND '$rp_enddate'
   AND       Serv.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS57b_Prep_Prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS57b_Prep_Prov USING DELTA AS

     SELECT   MPI.Person_ID
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
     FROM    $db_source.MHS101Referral AS REF
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = MPI.UniqMonthID
             AND MPI.OrgIDProv = REF.OrgIDProv
             AND REF.UniqMonthID = '$end_month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID 
             AND Serv.UniqMonthID = '$end_month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate_1m' AND '$rp_enddate'
   AND       Serv.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10')

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS57c_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS57c_prep USING DELTA AS

     SELECT   MPI.Person_ID
             ,MPI.IC_REC_GP_RES
             ,MPI.NAME
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
     FROM    $db_output.tmp_MHMAB_MHS001MPI_latest_month_data AS MPI
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = '$end_month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID 
             AND Serv.UniqMonthID = '$end_month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate_1m' AND '$rp_enddate'
   AND       Serv.ServTeamTypeRefToMH in ('C02')

# COMMAND ----------

 %sql
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS57c_Prep_Prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS57c_Prep_Prov USING DELTA AS

     SELECT   MPI.Person_ID

             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 18 THEN '0-18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
     FROM    $db_source.MHS101Referral AS REF
 INNER JOIN  $db_source.MHS001MPI AS MPI
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = MPI.UniqMonthID
             AND MPI.OrgIDProv = REF.OrgIDProv
             AND REF.UniqMonthID = '$end_month_id' 
 INNER JOIN  $db_source.MHS102ServiceTypeReferredTo AS Serv
             ON REF.UniqServReqID = Serv.UniqServReqID 
             AND Serv.UniqMonthID = '$end_month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate_1m' AND '$rp_enddate'
   AND       Serv.ServTeamTypeRefToMH in ('C02')

# COMMAND ----------

 %sql
 /*** MHS57b - People discharged from the service from Core community services in the RP ***/
  
 -- National 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL 
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57b_prep
             

# COMMAND ----------

 %sql
 /*** MHS57b - People discharged from the service from Core community services in the RP ***/
  
 -- National, Age Group 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL 
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL 
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57b_prep
 GROUP BY    AgeGroup
             

# COMMAND ----------

 %sql
 /*** MHS57b - People discharged from the service from Core community services in the RP ***/
  
 -- CCG 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57b_prep
 GROUP BY    IC_REC_GP_RES
             ,NAME

# COMMAND ----------

 %sql
 /*** MHS57b - People discharged from the service from Core community services in the RP ***/
  
 -- CCG; Age Group
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence; Age Group' AS BREAKDOWN 
             ,IC_REC_GP_RES AS PRIMARY_LEVEL
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL 
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57b_prep
 GROUP BY     IC_REC_GP_RES
             ,NAME
             ,AgeGroup

# COMMAND ----------

 %sql
 /*** MHS57b - People discharged from the service from Core community services in the RP ***/
  
 -- Provider 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL 
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57b_prep_Prov
 GROUP BY     OrgIDProv

# COMMAND ----------

 %sql
 /*** MHS57b - People discharged from the service from Core community services in the RP ***/
  
 -- Provider; Age Group
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL 
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL 
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57b' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57b_prep_Prov
 GROUP BY     OrgIDProv
             ,AgeGroup

# COMMAND ----------

 %sql
 /*** MHS57c - People discharged from the service from Perinatal services in the RP ***/
  
 -- National 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
              ,'England' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL 
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57c' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57c_prep
             

# COMMAND ----------

 %sql
 /*** MHS57c - People discharged from the service from Perinatal services in the RP ***/
  
 -- CCG 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Sub ICB - GP Practice or Residence' AS BREAKDOWN
             ,IC_REC_GP_RES AS PRIMARY_LEVEL 
             ,NAME AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57c' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57c_prep
 GROUP BY     IC_REC_GP_RES
             ,NAME

# COMMAND ----------

 %sql
 /*** MHS57c - People discharged from the service from Perinatal services in the RP ***/
  
 -- Provider 
  
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv AS PRIMARY_LEVEL 
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL 
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
             ,'MHS57c' AS MEASURE_ID 
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
 FROM        $db_output.tmp_MHMAB_MHS57c_prep_Prov
 GROUP BY     OrgIDProv



# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))