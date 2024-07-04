# Databricks notebook source
# DBTITLE 1,MHS26 ResponsibleLA
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
 
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'Local Authority of Responsibility or Residence' AS BREAKDOWN
             ,ResponsibleLA AS PRIMARY_LEVEL
             ,ResponsibleLA_Name AS PRIMARY_LEVEL_DESCRIPTION
             ,'NONE' AS SECONDARY_LEVEL
             ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS METRIC
             ,CAST( COALESCE( sum(DateDIFF(CASE  WHEN EndDateDelayDisch IS NULL
                                             THEN DATE_ADD ('$rp_enddate',1)
                                             ELSE EndDateDelayDisch
                                             END
                                             ,case when StartDateDelayDisch < '$rp_startdate' then '$rp_startdate'
                                                   when rnk = 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate'
                                                      then date_add(StartDateDelayDisch,1)
                                                   when rnk <> 1 and startdatedelaydisch  between '$rp_startdate' and '$rp_enddate'
                                                                 and startdatedelaydisch > date_add(lastenddate,1) 
                                                      then date_add(startdatedelaydisch,1)   
                                                   when rnk <> 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate' 
                                                      then StartDateDelayDisch  
                                                   else cast('01/01/1900' as date)
                                              end
                                          )),0) as STRING) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 from global_temp.MHS26_ranking ranking
 group by  ResponsibleLA,ResponsibleLA_Name

# COMMAND ----------

# DBTITLE 1,MHS26 ResponsibleLA by AttribToIndic
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'Local Authority of Responsibility or Residence; Delayed discharge attributable to' AS BREAKDOWN
             ,ResponsibleLA AS PRIMARY_LEVEL
             ,ResponsibleLA_Name AS PRIMARY_LEVEL_DESCRIPTION
             ,AttribToIndic AS SECONDARY_LEVEL
             ,COALESCE(ref.Description,'UNKNOWN') AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS METRIC
             ,CAST( COALESCE( sum(DateDIFF(CASE  WHEN EndDateDelayDisch IS NULL
                                             THEN DATE_ADD ('$rp_enddate',1)
                                             ELSE EndDateDelayDisch
                                             END
                                             ,case when StartDateDelayDisch < '$rp_startdate' then '$rp_startdate'
                                                   when rnk = 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate'
                                                      then date_add(StartDateDelayDisch,1)
                                                   when rnk <> 1 and startdatedelaydisch  between '$rp_startdate' and '$rp_enddate'
                                                                 and startdatedelaydisch > date_add(lastenddate,1) 
                                                      then date_add(startdatedelaydisch,1)   
                                                   when rnk <> 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate' 
                                                      then StartDateDelayDisch  
                                                   else cast('01/01/1900' as date)
                                              end
                                          )),0) as STRING) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 from global_temp.MHS26_ranking ranking
 left outer join $db_output.DelayedDischDim ref
 on ranking.AttribToIndic = ref.Code and ref.key = 'att'
 and '$month_id' >= ref.FirstMonth and (ref.LastMonth is null or '$month_id' <= ref.LastMonth)
 
 group by  ResponsibleLA,AttribToIndic,ref.Description,ResponsibleLA_Name 

# COMMAND ----------

# DBTITLE 1,MHS26 ResponsibleLA by DelayDischReason
 %sql
 
 INSERT INTO $db_output.Main_monthly_unformatted
 SELECT   
             '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
             ,'$status'AS STATUS
             ,'Local Authority of Responsibility or Residence; Delayed discharge reason' AS BREAKDOWN
             ,ResponsibleLA AS PRIMARY_LEVEL
             ,ResponsibleLA_Name AS PRIMARY_LEVEL_DESCRIPTION
             ,DelayDischReason AS SECONDARY_LEVEL
             ,COALESCE(ref.Description,'UNKNOWN') AS SECONDARY_LEVEL_DESCRIPTION
             ,'MHS26' AS METRIC
             ,CAST( COALESCE( sum(DateDIFF(CASE  WHEN EndDateDelayDisch IS NULL
                                             THEN DATE_ADD ('$rp_enddate',1)
                                             ELSE EndDateDelayDisch
                                             END
                                             ,case when StartDateDelayDisch < '$rp_startdate' then '$rp_startdate'
                                                   when rnk = 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate'
                                                      then date_add(StartDateDelayDisch,1)
                                                   when rnk <> 1 and startdatedelaydisch  between '$rp_startdate' and '$rp_enddate'
                                                                 and startdatedelaydisch > date_add(lastenddate,1) 
                                                      then date_add(startdatedelaydisch,1)   
                                                   when rnk <> 1 and StartDateDelayDisch between '$rp_startdate' and '$rp_enddate' 
                                                      then StartDateDelayDisch  
                                                   else cast('01/01/1900' as date)
                                              end
                                          )),0) as STRING) as METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
 from global_temp.MHS26_ranking ranking 
 left outer join $db_output.DelayedDischDim ref
 on ranking.DelayDischReason = ref.Code and ref.key = 'reason'
 and '$month_id' >= ref.FirstMonth and (ref.LastMonth is null or '$month_id' <= ref.LastMonth)
 
 group by  ResponsibleLA,DelayDischReason,ResponsibleLA_Name,ref.Description

# COMMAND ----------

