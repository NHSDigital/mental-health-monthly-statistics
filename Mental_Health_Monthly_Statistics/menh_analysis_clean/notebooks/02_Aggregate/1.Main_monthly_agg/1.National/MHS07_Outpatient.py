# Databricks notebook source
#dbutils.widgets.text("month_id","1472","month_id")
#dbutils.widgets.text("rp_startdate","11-11-01","rp_startdate")
#dbutils.widgets.text("rp_enddate","11-11-30","rp_enddate")
#dbutils.widgets.text("db_output","menh_analysis","db_output")
#dbutils.widgets.text("db_source","db_source","Source Ref Database")
#dbutils.widgets.text("db_source","$db_source","db_source")
#dbutils.widgets.text("status","Performance","status")

# COMMAND ----------

 %md
 - MHSDS Monthly	MHS07	MHSDS Data_MMMPrf_20YY.csv	Monthly	People with an open hospital spell at the end of the reporting period.
 - MHSDS Monthly	MHS07a	MHSDS Data_MMMPrf_20YY.csv	Monthly	People with an open hospital spell at the end of the reporting period aged 0 to 18.
 - MHSDS Monthly	MHS07b	MHSDS Data_MMMPrf_20YY.csv	Monthly	People with an open hospital spell at the end of the reporting period aged 19 to 64.
 - MHSDS Monthly	MHS07c	MHSDS Data_MMMPrf_20YY.csv	Monthly	People with an open hospital spell at the end of the reporting period aged 65 and over.

# COMMAND ----------

# DBTITLE 1,MHS07_prep-Move into Main Monthly
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW tmp_mhmab_MHS07_prep AS
 SELECT      REF.UniqServReqID 
            ,REF.Person_ID
            ,HSP.UniqHospProvSpellID
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
 
 FROM $db_output.tmp_MHMAB_mhs001mpi_latest_month_data AS MPI
 INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND MPI.PatMRecInRP = TRUE
 INNER JOIN global_temp.MHS501HospProvSpell_open_end_rp AS HSP
             ON REF.UniqServReqID = HSP.UniqServReqID
              AND HSP.Uniqmonthid = '$month_id' --open hospital referrals at the end of the reporting month this clause is needed-check for MHS29d, and f

# COMMAND ----------

 %md
 - "CCG - GP Practice or Residence
 - England
 - Provider
 - England; Accommodation Type
 - England; Disability
 - England; Gender
 - England; IMD Decile
 - England; Ethnicity
 - England; Age
 - England; Sexual Orientation
 - England; Employment Status"

# COMMAND ----------

 %sql
 /**MHS07 - PEOPLE WITH A HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD - INTERMEDIATE**/
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS07_prep AS
 SELECT DISTINCT  MPI.Person_ID
                  ,IC_Rec_CCG
                  ,NAME
                  ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 18 THEN '00-18'
                        WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64' 
                        WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
                        END AS AGE_GROUP
 FROM             $db_output.MHS001MPI_latest_month_data AS MPI -- table in generic prep
 INNER JOIN       $db_output.MHS101Referral_open_end_rp AS REF -- table in generic prep
                  ON MPI.Person_ID = REF.Person_ID                 
 INNER JOIN       global_temp.MHS501HospProvSpell_open_end_rp AS HSP -- table in generic prep
                  ON REF.UniqServReqID = HSP.UniqServReqID

# COMMAND ----------

 %sql
 /**MHS07 - PEOPLE WITH AN OPEN HOSPITAL PROVIDER SPELL AT END OF REPORTING PERIOD, PROVIDER**/
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS07_Prov_prep AS
 SELECT DISTINCT   MPI.Person_ID
                   ,MPI.ORGIDProv
                   ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 18 THEN '00-18'
                         WHEN AgeRepPeriodEnd BETWEEN 19 AND 64 THEN '19-64' 
                         WHEN AgeRepPeriodEnd >= 65 THEN '65-120' 
                         END AS AGE_GROUP
 FROM               $db_source.MHS001MPI AS MPI 
 INNER JOIN         $db_output.MHS101Referral_open_end_rp AS REF -- table in generic prep
                    ON MPI.Person_ID = REF.Person_ID  
                    AND MPI.OrgIDProv = REF.OrgIDProv 
 INNER JOIN         global_temp.MHS501HospProvSpell_open_end_rp AS HSP -- table in generic prep
                    ON REF.UniqServReqID = HSP.UniqServReqID     
                    AND REF.OrgIDProv = HSP.OrgIDProv
                    AND MPI.UniqMonthID = '$month_id'

# COMMAND ----------



# COMMAND ----------

