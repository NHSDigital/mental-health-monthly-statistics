# Databricks notebook source
 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS23d_prep;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS23d_prep USING DELTA AS
       SELECT 
       MPI.NAME
      ,MPI.IC_REC_GP_RES
             ,REF.UniqServReqID
             ,REF.AMHServiceRefEndRP_temp
             ,REF.CYPServiceRefEndRP_temp
             ,REF.LDAServiceRefEndRP_temp
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
        FROM $db_output.tmp_MHMAB_MHS101Referral_open_end_rp    AS REF    
  INNER JOIN $db_output.tmp_MHMAB_MHS001MPI_latest_month_data    AS MPI
             ON REF.Person_ID = MPI.Person_ID
  INNER JOIN $db_output.tmp_MHMAB_MHS102ServiceTypeReferredTo   AS SERV
             ON REF.UniqServReqID = SERV.UniqServReqID
  WHERE      SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');    
  
  OPTIMIZE $db_output.tmp_MHMAB_MHS23d_prep

# COMMAND ----------

 %sql
  
 DROP table IF EXISTS $db_output.tmp_MHMAB_MHS23d_prep_prov;
  
 CREATE TABLE $db_output.tmp_MHMAB_MHS23d_prep_prov USING DELTA AS
 
       SELECT REF.UniqServReqID
             ,REF.AMHServiceRefEndRP_temp
             ,REF.CYPServiceRefEndRP_temp
             ,REF.LDAServiceRefEndRP_temp
             ,REF.OrgIDProv
             ,CASE WHEN MPI.AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                   WHEN MPI.AgeRepPeriodEnd BETWEEN 18 AND 64 THEN '18-64'
                   WHEN MPI.AgeRepPeriodEnd > 64 THEN '65+'
                   ELSE 'UNKNOWN' END as AgeGroup 
        FROM $db_output.tmp_MHMAB_MHS101Referral_open_end_rp    AS REF    
  INNER JOIN $db_source.MHS001MPI    AS MPI
             ON REF.Person_ID = MPI.Person_ID
            AND MPI.OrgIDProv = REF.OrgIDProv
            AND MPI.UniqMonthID = REF.UniqMonthID
  INNER JOIN $db_output.tmp_MHMAB_MHS102ServiceTypeReferredTo   AS SERV
             ON REF.UniqServReqID = SERV.UniqServReqID
  WHERE      SERV.ServTeamTypeRefToMH in ('A05','A06','A08','A09','A12','A13','A16','C10');     
  
  OPTIMIZE $db_output.tmp_MHMAB_MHS23d_prep_prov 

# COMMAND ----------

 %sql
 /*** MHS23d ***/
  
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
             ,'MHS23d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.tmp_MHMAB_MHS23d_prep;

# COMMAND ----------

 %sql
 /*** MHS23d ***/
  
 -- National; Age Group
 
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'England; Age Group' AS BREAKDOWN
             ,'England' AS PRIMARY_LEVEL
             ,'England' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.tmp_MHMAB_MHS23d_prep
    GROUP BY AgeGroup;

# COMMAND ----------

 %sql
 /*** MHS23d ***/
  
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
            ,'MHS23d' AS MEASURE_ID
            ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_MHMAB_MHS23d_prep
   GROUP BY IC_REC_GP_RES, NAME

# COMMAND ----------

 %sql
 /*** MHS23d ***/
  
 -- CCG; Age Group
 -- ICB; Age Group
 
 INSERT INTO $db_output.output1
 
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'Sub ICB - GP Practice or Residence; Age Group' AS BREAKDOWN 
            ,IC_REC_GP_RES AS PRIMARY_LEVEL
            ,NAME AS PRIMARY_LEVEL_DESCRIPTION
            ,AgeGroup AS SECONDARY_LEVEL
            ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS23d' AS MEASURE_ID
            ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
       FROM $db_output.tmp_MHMAB_MHS23d_prep
   GROUP BY IC_REC_GP_RES, NAME, AgeGroup;

# COMMAND ----------

 %sql
 /*** MHS23d ***/
  
 -- Provider
 
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider' AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.tmp_MHMAB_MHS23d_prep_prov
    GROUP BY OrgIDProv;

# COMMAND ----------

 %sql
 /*** MHS23d ***/
  
 -- Provider; Age Group
 
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status' AS STATUS
             ,'Provider; Age Group' AS BREAKDOWN
             ,OrgIDProv    AS PRIMARY_LEVEL
             ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
             ,AgeGroup AS SECONDARY_LEVEL
             ,AgeGroup AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS23d' AS MEASURE_ID
             ,CAST (COALESCE (cast(COUNT (DISTINCT UniqServReqID) as INT), 0) AS STRING)    AS MEASURE_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.tmp_MHMAB_MHS23d_prep_prov
    GROUP BY OrgIDProv, AgeGroup;

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))