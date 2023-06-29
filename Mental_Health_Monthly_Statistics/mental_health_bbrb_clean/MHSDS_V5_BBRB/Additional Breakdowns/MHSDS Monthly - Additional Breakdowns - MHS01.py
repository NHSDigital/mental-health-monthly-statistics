# Databricks notebook source
 %md
 ##MHS01 Prep Table

# COMMAND ----------

 %sql
  --REPLACED LOWERCASE UNKNOWN TO CAPITALS FOR CONSISTENCY
 DROP table IF EXISTS $db_output.tmp_mhmab_mhs01_prep;
  
 CREATE TABLE $db_output.tmp_mhmab_mhs01_prep USING DELTA AS
 SELECT      REF.UniqServReqID 
            ,REF.Person_ID
            ,COALESCE(MPI.Age_Band, "UNKNOWN") as Age_Band
            ,COALESCE(MPI.Der_Gender, "UNKNOWN") as Der_Gender
            ,COALESCE(MPI.LowerEthnicity, "UNKNOWN") as LowerEthnicity
            ,COALESCE(MPI.LowerEthnicity_Desc, "UNKNOWN") as LowerEthnicity_Desc
            ,COALESCE(MPI.IMD_Decile, "UNKNOWN") as IMD_Decile
            ,COALESCE(MPI.AccommodationType, "UNKNOWN") as AccommodationType
            ,COALESCE(MPI.AccommodationType_Desc, "UNKNOWN") as AccommodationType_Desc
            ,COALESCE(MPI.EmployStatus, "UNKNOWN") as EmployStatus
            ,COALESCE(MPI.EmployStatus_Desc, "UNKNOWN") as EmployStatus_Desc
            ,COALESCE(MPI.DisabCode, "UNKNOWN") as DisabCode
            ,COALESCE(MPI.DisabCode_Desc, "UNKNOWN") as DisabCode_Desc
            ,COALESCE(MPI.Sex_Orient, "UNKNOWN") as Sex_Orient
 FROM $db_output.tmp_mhmab_mhs101referral_open_end_rp AS REF
 LEFT JOIN $db_output.tmp_MHMAB_mhs001mpi_latest_month_data AS MPI
             ON REF.Person_ID = MPI.Person_ID 
             AND MPI.PatMRecInRP = TRUE        

# COMMAND ----------

 %sql
 SELECT *
 FROM $db_output.tmp_MHMAB_mhs001mpi_latest_month_data

# COMMAND ----------

 %md
 ##MHS01 data into output

# COMMAND ----------

# DBTITLE 1,Added for testing purpose - will need removing at BBRB output stage
 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England' AS BREAKDOWN
            ,'England' AS PRIMARY_LEVEL
            ,'England' AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS01_prep

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  AccommodationType, AccommodationType_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
            ,'$status' AS STATUS
            ,'England; Age' AS BREAKDOWN
            ,Age_Band AS PRIMARY_LEVEL
            ,Age_Band AS PRIMARY_LEVEL_DESCRIPTION
            ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'MHS01' AS MEASURE_ID
            ,COUNT (DISTINCT Person_ID) AS MEASURE_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  Age_Band

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  DisabCode, DisabCode_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  EmployStatus, EmployStatus_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  LowerEthnicity, LowerEthnicity_Desc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
     SELECT '$rp_startdate_1m' AS REPORTING_PERIOD_START
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
            
      FROM  $db_output.tmp_mhmab_MHS01_prep
  GROUP BY  Sex_Orient

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))