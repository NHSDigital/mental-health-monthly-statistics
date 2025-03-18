-- Databricks notebook source
 %md

 # direct clone from FYFV from menh_analysis (with removals)

-- COMMAND ----------

-- DBTITLE 1,Get widget variables
 %py
 db_output = dbutils.widgets.get("db_output")
 db_source  = dbutils.widgets.get("db_source")
 month_id = dbutils.widgets.get("month_id")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 status = dbutils.widgets.get("status")
 rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")

 params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'rp_startdate_quarterly': rp_startdate_quarterly, 'status': status}

 print(params)

-- COMMAND ----------

-- DBTITLE 1,AMH03e_prep
TRUNCATE TABLE $db_output.AMH03e_prep;

INSERT INTO TABLE $db_output.AMH03e_prep

        SELECT   distinct PRSN.Person_ID
                ,PRSN.OrgIDProv
                ,PRSN.IC_Rec_CCG
                ,PRSN.NAME
                ,CASE 
                   WHEN GC.PrimaryCode is null THEN 'X'
                 ELSE PRSN.Gender 
                   END AS GENDER
                ,Region_code
                ,Region_description
                ,STP_code
                ,STP_description
                ,CASSR  
                ,CASSR_description
           FROM $db_output.MHS001MPI_latest_month_data AS PRSN 
     INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
                ON PRSN.Person_ID = REF.Person_ID 
      LEFT JOIN $db_output.STP_Region_mapping_post_2020 stp ON
                PRSN.IC_Rec_CCG = stp.CCG_code 
                
          LEFT JOIN global_temp.CASSR_mapping AS CASSR
                ON PRSN.LADistrictAuth = CASSR.LADistrictAuth
      LEFT JOIN global_temp.GenderCodes GC
                ON PRSN.Gender = GC.PrimaryCode
                
          WHERE PRSN.AgeRepPeriodEnd >= 18
			    AND PRSN.AgeRepPeriodEnd <= 69
                AND AMHServiceRefEndRP_temp = TRUE

-- COMMAND ----------

 %python

 import os

 db_output = dbutils.widgets.get("db_output")

 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH03e_prep'))

 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH03e_prep'))

-- COMMAND ----------

-- DBTITLE 1,AMH13e_14e_prep
TRUNCATE TABLE $db_output.AMH13e_14e_prep;

INSERT INTO TABLE $db_output.AMH13e_14e_prep
        
    SELECT PREP.Person_ID
           ,PREP.OrgIDProv
           ,PREP.IC_Rec_CCG
           ,PREP.NAME
                ,CASE 
                   WHEN GC.PrimaryCode is null THEN 'X'
                 ELSE PREP.Gender  
                   END AS GENDER
           ,Region_code
           ,Region_description
           ,STP_code
           ,STP_description 
           ,CASSR  
           ,CASSR_description
           ,AccommodationTypeDate
           ,SettledAccommodationInd
           ,rank
      FROM $db_output.AMH03e_prep AS PREP
INNER JOIN $db_output.accommodation_latest AS ACC
		   ON PREP.Person_ID = ACC.Person_ID
      LEFT JOIN global_temp.GenderCodes GC
                ON PREP.Gender = GC.PrimaryCode
     WHERE ACC.AccommodationTypeDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
		   AND ACC.AccommodationTypeDate <= '$rp_enddate'

-- COMMAND ----------

 %python

 import os

 db_output = dbutils.widgets.get("db_output")

 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH13e_14e_prep'))

 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH13e_14e_prep'))

-- COMMAND ----------

-- DBTITLE 1,AMH16e_17e_prep
TRUNCATE TABLE $db_output.AMH16e_17e_prep;

INSERT INTO TABLE $db_output.AMH16e_17e_prep
        
    SELECT MPI.Person_ID
           ,MPI.OrgIDProv
           ,MPI.IC_Rec_CCG
           ,MPI.NAME
                ,CASE 
                   WHEN GC.PrimaryCode is null THEN 'X'
                 ELSE MPI.Gender  
                   END AS GENDER
           ,Region_code
           ,Region_description
           ,STP_code
           ,STP_description
           ,CASSR  
           ,CASSR_description
           ,EMP.EmployStatusRecDate
           ,EmployStatus
           ,RANK
      FROM $db_output.MHS001MPI_latest_month_data AS MPI
INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
		   ON MPI.Person_ID = REF.Person_ID 
INNER JOIN $db_output.employment_latest AS EMP
		   ON MPI.Person_ID = EMP.Person_ID
LEFT JOIN $db_output.STP_Region_mapping_post_2020 AS stp 
           ON MPI.IC_Rec_CCG = stp.CCG_code
                
          LEFT JOIN global_temp.CASSR_mapping AS CASSR
                ON MPI.LADistrictAuth = CASSR.LADistrictAuth
      LEFT JOIN global_temp.GenderCodes GC
                ON MPI.Gender = GC.PrimaryCode
                
     WHERE REF.AMHServiceRefEndRP_temp = TRUE
		   AND MPI.AgeRepPeriodEnd >= 18
		   AND MPI.AgeRepPeriodEnd <= 69

-- COMMAND ----------

 %python

 import os

 db_output = dbutils.widgets.get("db_output")

 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH16e_17e_prep'))

 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH16e_17e_prep'))