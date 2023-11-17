# Databricks notebook source
# DBTITLE 1,MHS08 - Provider Level
 %sql
 --MHS08 - PEOPLE SUBJECT TO MENTAL HEALTH ACT AT END OF REPORTING PERIOD
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS08' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
         FROM 
             (
                 SELECT REF.Person_ID, 
                        REF.RecordNumber,
                        REF.OrgIDProv
                   FROM $db_output.MHS101Referral_open_end_rp AS REF
             INNER JOIN $db_output.MHS401MHActPeriod_GRD_open_end_rp AS MHA
                        ON REF.Person_ID = MHA.Person_ID
                        AND REF.OrgIDProv = MHA.OrgIDProv
             UNION 
                  SELECT Person_ID, RecordNumber, OrgIDProv FROM $db_output.MHS09Prov_INTERMEDIATE 
             UNION 
                  SELECT Person_ID, RecordNumber, OrgIDProv FROM $db_output.MHS10Prov_INTERMEDIATE
             UNION 
                  SELECT Person_ID, RecordNumber, OrgIDProv FROM $db_output.MHS11Prov_INTERMEDIATE
            ) AS MHS08
     GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS08a - Provider Level
 %sql
 --MHS08a - PEOPLE SUBJECT TO MENTAL HEALTH ACT AT END OF REPORTING PERIOD, AGED 0-17
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS08a' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
       FROM 
             (
                 SELECT REF.Person_ID, 
                        REF.RecordNumber,
                        REF.OrgIDProv
                   FROM $db_source.MHS001MPI AS PRSN
             INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
                        ON PRSN.Person_ID = REF.Person_ID
                        AND PRSN.OrgIDProv = REF.OrgIDProv
             INNER JOIN $db_output.MHS401MHActPeriod_GRD_open_end_rp AS MHA
                        ON PRSN.Person_ID = MHA.Person_ID
                        AND PRSN.OrgIDProv = MHA.OrgIDProv
                  WHERE PRSN.AgeRepPeriodEnd <= 17 
                        AND PRSN.UniqMonthID = '${month_id}'
             UNION 
                 SELECT Person_ID, RecordNumber, OrgIDProv FROM $db_output.MHS09Prov_INTERMEDIATE
                  WHERE AgeRepPeriodEnd <= 17   
             UNION 
                 SELECT Person_ID, RecordNumber, OrgIDProv FROM $db_output.MHS10Prov_INTERMEDIATE
                  WHERE AgeRepPeriodEnd <= 17   
             UNION 
                 SELECT Person_ID, RecordNumber, OrgIDProv FROM $db_output.MHS11Prov_INTERMEDIATE
                  WHERE AgeRepPeriodEnd <= 17
            ) AS MHS08a
     GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH08 - Provider
 %sql
 --MH08 - PEOPLE SUBJECT TO MENTAL HEALTH ACT (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH08' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM 
             (
                 SELECT REF.Person_ID, 
                        REF.OrgIDProv
                   FROM $db_output.MHS101Referral_open_end_rp as REF
             INNER JOIN $db_output.MHS401MHActPeriod_GRD_open_end_rp MHA
                        ON REF.Person_ID = MHA.Person_ID
                        AND REF.OrgIDProv = MHA.OrgIDProv
                        WHERE (REF.CYPServiceRefEndRP_temp = TRUE OR REF.AMHServiceRefEndRP_temp = TRUE)
            UNION 
                  SELECT Person_ID, OrgIDProv FROM $db_output.MHS09Prov_INTERMEDIATE
                         WHERE (CYPServiceWSEndRP_temp = TRUE OR AMHServiceWSEndRP_temp = TRUE)
            UNION 
                  SELECT Person_ID, OrgIDProv FROM $db_output.MHS10Prov_INTERMEDIATE
                         WHERE (CYPServiceRefEndRP_temp = TRUE OR AMHServiceRefEndRP_temp = TRUE)
            UNION 
                  SELECT Person_ID, OrgIDProv FROM $db_output.MHS11Prov_INTERMEDIATE
                         WHERE (CYPServiceRefEndRP_temp = TRUE OR AMHServiceRefEndRP_temp = TRUE)
            ) AS MH08
     GROUP BY OrgIDProv
       

# COMMAND ----------

# DBTITLE 1,MH08a Provider
 %sql
 --MH08a - PEOPLE SUBJECT TO MENTAL HEALTH ACT (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD, AGED 0-17
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH08a' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM 
             (
                 SELECT PRSN.Person_ID, 
                        PRSN.OrgIDProv
                   FROM $db_source.MHS001MPI AS PRSN   
             INNER JOIN $db_output.MHS101Referral_open_end_rp as REF
                        ON PRSN.Person_ID = REF.Person_ID
                        AND PRSN.OrgIDProv = REF.OrgIDProv
                        AND PRSN.UniqMonthID = '$month_id'
             INNER JOIN $db_output.MHS401MHActPeriod_GRD_open_end_rp MHA
                        ON PRSN.Person_ID = MHA.Person_ID
                        AND REF.OrgIDProv = MHA.OrgIDProv
                        WHERE (REF.CYPServiceRefEndRP_temp = TRUE OR REF.AMHServiceRefEndRP_temp = TRUE)
                        AND PRSN.AgeRepPeriodEnd <= 17
                UNION 
                  SELECT Person_ID, OrgIDProv FROM $db_output.MHS09Prov_INTERMEDIATE
                         WHERE (CYPServiceWSEndRP_temp = TRUE OR AMHServiceWSEndRP_temp = TRUE)
                         AND AgeRepPeriodEnd <= 17
                UNION 
                  SELECT Person_ID, OrgIDProv FROM $db_output.MHS10Prov_INTERMEDIATE
                         WHERE (CYPServiceRefEndRP_temp = TRUE OR AMHServiceRefEndRP_temp = TRUE)
                         AND AgeRepPeriodEnd <= 17
                UNION 
                  SELECT Person_ID, OrgIDProv FROM $db_output.MHS11Prov_INTERMEDIATE
                         WHERE (CYPServiceRefEndRP_temp = TRUE OR AMHServiceRefEndRP_temp = TRUE)
                         AND AgeRepPeriodEnd <= 17
            ) AS MH08a
           GROUP BY OrgIDProv
 		

# COMMAND ----------

# DBTITLE 1,LDA08 Provider
 %sql
 --LDA08 - PEOPLE SUBJECT TO MENTAL HEALTH ACT (LEARNING DISABILITY AND AUTISM SERVICES) AT END OF REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'LDA08' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
 FROM	(
             SELECT REF.Person_ID,
 				   REF.RecordNumber,
                    REF.OrgIDProv
             FROM $db_output.MHS101Referral_open_end_rp AS REF
 			INNER JOIN $db_output.MHS401MHActPeriod_GRD_open_end_rp MHA
                        ON REF.Person_ID = MHA.Person_ID
                        AND REF.OrgIDProv = MHA.OrgIDProv
             WHERE REF.LDAServiceRefEndRP_temp = TRUE
 	UNION 
             SELECT Person_ID, RecordNumber, OrgIDProv
             FROM $db_output.MHS09Prov_INTERMEDIATE as MHS09
             WHERE LDAServiceWSEndRP_temp = TRUE
 	UNION 
             SELECT Person_ID, RecordNumber, OrgIDProv
             FROM $db_output.MHS10Prov_INTERMEDIATE AS MHS10
             WHERE LDAServiceRefEndRP_temp = TRUE
 	UNION 
             SELECT Person_ID, RecordNumber, OrgIDProv
             FROM $db_output.MHS11Prov_INTERMEDIATE AS MHS11
             WHERE LDAServiceRefEndRP_temp = TRUE
 		) AS LDA08
  GROUP BY OrgIDProv 

# COMMAND ----------

# DBTITLE 1,MHS09 - Provider Level
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS09' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE AS MHS09
    GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH09 Provider
 %sql
 --MH09 - PEOPLE SUBJECT TO DETENTION IN HOSPITAL (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD - FINAL
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH09' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE
       WHERE CYPServiceWSEndRP_temp = TRUE OR AMHServiceWSEndRP_temp = TRUE
    GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH09a Provider
 %sql
 --MH09a - PEOPLE SUBJECT TO DETENTION IN HOSPITAL (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD, AGED 0-17 - FINAL
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH09a' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE
       WHERE (CYPServiceWSEndRP_temp = TRUE OR AMHServiceWSEndRP_temp = TRUE)
             AND AgeRepPeriodEnd <= 17
    GROUP BY OrgIDProv 

# COMMAND ----------

# DBTITLE 1,MH09b Provider
 %sql
 --MH09b - PEOPLE SUBJECT TO DETENTION IN HOSPITAL (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD, AGED 18-64 - FINAL
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH09b' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE
       WHERE (CYPServiceWSEndRP_temp = TRUE OR AMHServiceWSEndRP_temp = TRUE)
             AND AgeRepPeriodEnd >=18
             AND AgeRepPeriodEnd <=64
    GROUP BY OrgIDProv           

# COMMAND ----------

# DBTITLE 1,MH09c Provider
 %sql
 --MH09c - PEOPLE SUBJECT TO DETENTION IN HOSPITAL (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD, AGED 65 AND OVER - FINAL
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH09c' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE
       WHERE (CYPServiceWSEndRP_temp = TRUE OR AMHServiceWSEndRP_temp = TRUE)
             AND AgeRepPeriodEnd >= 65
    GROUP BY OrgIDProv

# COMMAND ----------

# DBTITLE 1,AMH09a Provider
 %sql
 --AMH09a - PEOPLE SUBJECT TO DETENTION IN HOSPITAL (ACUTE ADULT MENTAL HEALTH CARE) AT END OF REPORTING PERIOD 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS09.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'AMH09a' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS09.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE AS MHS09
      LEFT JOIN $db_source.MHS401MHActPeriod AS STO
                ON MHS09.RecordNumber = STO.RecordNumber
                AND STO.uniqmonthid = '${month_id}'
                AND STO.NHSDLegalStatus IN ('04', '05', '06', '19', '20') 
                AND (STO.EndDateMHActLegalStatusClass IS NULL OR STO.EndDateMHActLegalStatusClass > '${rp_enddate}')
     INNER JOIN global_temp.MHS502WardStay_open_end_rp AS WRD
 			   ON MHS09.RecordNumber = WRD.RecordNumber                
    INNER JOIN global_temp.unique_bed_types AS BED
 			  ON WRD.UniqWardStayID = BED.UniqWardStayID
       WHERE MHS09.AMHServiceRefEndRP_temp = true
             AND BED.bed_type = 2
    GROUP BY MHS09.OrgIDProv 

# COMMAND ----------

# DBTITLE 1,LDA09 Provider
 %sql
 --LDA09 - PEOPLE SUBJECT TO DETENTION IN HOSPITAL (LEARNING DISABILITY AND AUTISM SERVICES) AT END OF REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'LDA09' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS09.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS09Prov_INTERMEDIATE AS MHS09
       WHERE LDAServiceWSEndRP_temp = TRUE
    GROUP BY OrgIDProv 

# COMMAND ----------

# DBTITLE 1,MHS10 - Provider Level
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             PRSN.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS10' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS10.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS10Prov_INTERMEDIATE AS MHS10 
   LEFT JOIN $db_output.MHS11Prov_INTERMEDIATE AS MHS11
             ON MHS10.Person_ID = MHS11.Person_ID
             AND MHS10.OrgIDProv = MHS11.OrgIDProv
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS10.Person_ID = MHS09.Person_ID
             AND MHS10.OrgIDProv = MHS09.OrgIDProv
      INNER JOIN $db_source.MHS001MPI AS PRSN 
             ON MHS10.Person_ID = PRSN.Person_ID
             AND MHS10.OrgIDProv = PRSN.OrgIDProv
             AND PRSN.UniqMonthID = '$month_id' 
       WHERE MHS11.Person_ID IS NULL
 			AND MHS09.Person_ID IS NULL
    GROUP BY PRSN.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH10 Provider
 %sql
 --PEOPLE SUBJECT TO A COMMUNITY TREATMENT ORDER OR ON CONDITIONAL DISCHARGE (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS10.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH10' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS10.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS10Prov_INTERMEDIATE AS MHS10 
   LEFT JOIN $db_output.MHS11Prov_INTERMEDIATE AS MHS11
             ON MHS10.Person_ID = MHS11.Person_ID
             AND MHS10.OrgIDProv = MHS11.OrgIDProv
             AND (MHS11.CYPServiceRefEndRP_temp = TRUE OR MHS11.AMHServiceRefEndRP_temp = TRUE)
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS10.Person_ID = MHS09.Person_ID
             AND MHS10.OrgIDProv = MHS09.OrgIDProv
             AND (MHS09.CYPServiceWSEndRP_temp = TRUE OR MHS09.AMHServiceWSEndRP_temp = TRUE)
       WHERE MHS11.Person_ID IS NULL
 			AND MHS09.Person_ID IS NULL
             AND (MHS10.CYPServiceRefEndRP_temp = TRUE OR MHS10.AMHServiceRefEndRP_temp = TRUE)
    GROUP BY MHS10.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH10a Provider
 %sql
 --PEOPLE SUBJECT TO A COMMUNITY TREATMENT ORDER OR ON CONDITIONAL DISCHARGE (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD, AGED 0-17 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS10.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH10a' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS10.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS10Prov_INTERMEDIATE AS MHS10  
   LEFT JOIN $db_output.MHS11Prov_INTERMEDIATE AS MHS11
             ON MHS10.Person_ID = MHS11.Person_ID 
             AND MHS10.OrgIDProv = MHS11.OrgIDProv
             AND (MHS11.CYPServiceRefEndRP_temp = TRUE OR MHS11.AMHServiceRefEndRP_temp = TRUE)
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS10.Person_ID = MHS09.Person_ID 
             AND MHS10.OrgIDProv = MHS09.OrgIDProv
             AND (MHS09.CYPServiceWSEndRP_temp = TRUE OR MHS09.AMHServiceWSEndRP_temp = TRUE)
       WHERE MHS11.Person_ID IS NULL
 			AND MHS09.Person_ID IS NULL
             AND (MHS10.CYPServiceRefEndRP_temp = TRUE OR MHS10.AMHServiceRefEndRP_temp = TRUE)
             AND MHS10.AgeRepPeriodEnd <= 17
    GROUP BY MHS10.OrgIDProv

# COMMAND ----------

# DBTITLE 1,LDA10 Provider
 %sql
 --LDA10 - PEOPLE SUBJECT TO A COMMUNITY TREATMENT ORDER OR ON CONDITIONAL DISCHARGE (LEARNING DISABILITY AND AUTISM SERVICES) AT END OF REPORTING PERIOD 
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS10.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'LDA10' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS10.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS10Prov_INTERMEDIATE AS MHS10  
   LEFT JOIN $db_output.MHS11Prov_INTERMEDIATE AS MHS11
             ON MHS10.Person_ID = MHS11.Person_ID
             AND MHS10.OrgIDProv = MHS11.OrgIDProv
             AND MHS11.LDAServiceRefEndRP_temp = TRUE
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS10.Person_ID = MHS09.Person_ID 
             AND MHS10.OrgIDProv = MHS09.OrgIDProv
             AND MHS09.LDAServiceWSEndRP_temp = TRUE
       WHERE MHS11.Person_ID IS NULL
 			AND MHS09.Person_ID IS NULL
             AND MHS10.LDAServiceRefEndRP_temp = TRUE
    GROUP BY MHS10.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS11 - Provider Level
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS11.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS11' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS11.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS11Prov_INTERMEDIATE AS MHS11
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS11.Person_ID = MHS09.Person_ID
             AND MHS11.OrgIDProv = MHS09.OrgIDProv
       WHERE MHS09.Person_ID IS NULL
    GROUP BY MHS11.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MHS11a - Provider Level
 %sql
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS11.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MHS11a' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS11.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS11Prov_INTERMEDIATE AS MHS11
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS11.Person_ID = MHS09.Person_ID
             AND MHS11.OrgIDProv = MHS09.OrgIDProv
       WHERE MHS09.Person_ID IS NULL
             AND MHS11.AgeRepPeriodEnd <= 17
    GROUP BY MHS11.OrgIDProv

# COMMAND ----------

# DBTITLE 1,MH11 Provider
 %sql
 --MH11 - PEOPLE SUBJECT TO A SHORT TERM ORDER (MENTAL HEALTH SERVICES) AT END OF REPORTING PERIOD - FINAL**/
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS11.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'MH11' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS11.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS11Prov_INTERMEDIATE AS MHS11
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS11.Person_ID = MHS09.Person_ID
             AND MHS11.OrgIDProv = MHS09.OrgIDProv
       WHERE MHS09.Person_ID IS NULL
             AND (MHS11.CYPServiceRefEndRP_temp = TRUE OR MHS11.AMHServiceRefEndRP_temp = TRUE)
    GROUP BY MHS11.OrgIDProv

# COMMAND ----------

# DBTITLE 1,LDA11 Provider
 %sql
 --LDA11 - PEOPLE SUBJECT TO A SHORT TERM ORDER (LEARNING DISABILITY AND AUTISM SERVICES) AT END OF REPORTING PERIOD
 INSERT INTO $db_output.Main_monthly_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
             'Provider' AS BREAKDOWN,
             MHS11.OrgIDProv AS PRIMARY_LEVEL,
             'NONE' AS PRIMARY_LEVEL_DESCRIPTION,
             'NONE' AS SECONDARY_LEVEL,
             'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'LDA11' AS METRIC,
             CAST (COALESCE( cast(COUNT (DISTINCT MHS11.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE ,
             '$db_source' AS SOURCE_DB
        FROM $db_output.MHS11Prov_INTERMEDIATE AS MHS11
   LEFT JOIN $db_output.MHS09Prov_INTERMEDIATE AS MHS09 
             ON MHS11.Person_ID = MHS09.Person_ID
             AND MHS11.OrgIDProv = MHS09.OrgIDProv
 	        AND MHS09.LDAServiceWSEndRP_temp = TRUE
       WHERE MHS09.Person_ID IS NULL
             AND MHS11.LDAServiceRefEndRP_temp = TRUE
    GROUP BY MHS11.OrgIDProv