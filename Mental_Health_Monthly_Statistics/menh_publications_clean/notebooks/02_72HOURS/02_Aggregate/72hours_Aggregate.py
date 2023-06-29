# Databricks notebook source
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate
rp_enddate=dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate

# COMMAND ----------

# DBTITLE 1,populate 72hours_unrounded_stg
%sql
INSERT INTO $db_output.72hours_unrounded_stg
SELECT '$month_id' AS UniqMonthID,
       '$status' AS Status,
       f.ResponsibleProv AS ResponsibleProv,
       ccg.IC_Rec_CCG AS CCG,
       f.ElgibleDischFlag,
       (CASE WHEN f.DiedBeforeFollowUp = 0 AND f.AcuteBed = 1  THEN f.ElgibleDischFlag ELSE 0 END) AS ElgibleDischFlag_Modified,
       (CASE WHEN f.ElgibleDischFlag = 1 AND f.AcuteBed = 1 AND f.DiedBeforeFollowUp = 0 THEN f.FollowedUp3Days ELSE 0 END) AS FollowedUp3Days,
       '$db_source' as SOURCE_DB
FROM              GLOBAL_TEMP.FUp f
LEFT JOIN         GLOBAL_TEMP.CCG_LATEST_2months ccg ON f.person_id = ccg.person_id	
WHERE             DischDateHospProvSpell BETWEEN date_add('$rp_startdate',-4) AND date_add('$rp_enddate',-4)

# COMMAND ----------

# DBTITLE 1,National - MHS78:Discharges from adult acute beds
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'MHS78' AS METRIC
       ,SUM(ElgibleDischFlag_Modified) METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
  FROM $db_output.72hours_unrounded_stg stg 
 WHERE UniqMonthID = '$month_id' 
 AND Status = '$status'
 AND SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,National - MHS79:Discharges from adult acute beds followed up
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT '$month_id' AS MONTH_ID
         ,'$status' AS STATUS
         ,'$rp_startdate' AS REPORTING_PERIOD_START
         ,'$rp_enddate' AS REPORTING_PERIOD_END
         ,'England' AS BREAKDOWN
         ,'England' AS PRIMARY_LEVEL
         ,'England' AS PRIMARY_LEVEL_DESCRIPTION
        ,'NONE' AS SECONDARY_LEVEL
        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
        ,'MHS79' AS METRIC
        ,SUM(FollowedUp3Days) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,National - MHS80:Proportion of discharges from adult acute beds- is that possible?
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
  '$month_id' UniqMonthID
  ,'$status' AS STATUS
  ,'$rp_startdate' AS REPORTING_PERIOD_START
  ,'$rp_enddate' AS REPORTING_PERIOD_END
  ,'England' AS BREAKDOWN
  ,'England' AS PRIMARY_LEVEL
  ,'England' AS PRIMARY_LEVEL_DESCRIPTION
  ,'NONE' AS SECONDARY_LEVEL
  ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
  ,'MHS80' AS METRIC
  ,coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,Provider - MHS78:Discharges from adult acute beds
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
  '$month_id' UniqMonthID
  ,'$status' AS STATUS
  ,'$rp_startdate' AS REPORTING_PERIOD_START
  ,'$rp_enddate' AS REPORTING_PERIOD_END
  ,'Provider of Responsibility' AS BREAKDOWN
  ,ResponsibleProv AS PRIMARY_LEVEL
  ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
  ,'NONE' AS SECONDARY_LEVEL
  ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
  ,'MHS78' AS METRIC
  ,SUM(ElgibleDischFlag_Modified) METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY ResponsibleProv;

# COMMAND ----------

# DBTITLE 1,Provider - MHS79:Discharges from adult acute beds followed up
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'Provider of Responsibility' AS BREAKDOWN
,ResponsibleProv AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,'NONE' AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS79' AS METRIC
,SUM(FollowedUp3Days) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY ResponsibleProv

# COMMAND ----------

# DBTITLE 1,CCG - MHS78:Discharges from adult acute beds
%sql

INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'CCG - GP Practice or Residence' AS BREAKDOWN
,CCG AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,'NONE' AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS78' AS METRIC
,SUM(ElgibleDischFlag_Modified) METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY CCG

# COMMAND ----------

# DBTITLE 1,CCG - MHS79:Discharges from adult acute beds followed up
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'CCG - GP Practice or Residence' AS BREAKDOWN
,CCG AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,'NONE' AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS79' AS METRIC
,SUM(FollowedUp3Days) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY CCG

# COMMAND ----------

# DBTITLE 1,CCG - MHS80
%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'CCG - GP Practice or Residence' AS BREAKDOWN
,CCG AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,'NONE' AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS80' AS METRIC
,coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100), 0) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY CCG

# COMMAND ----------

# DBTITLE 1,CCG - GP Practice or Residence; Provider of Responsibility - MHS78
%sql

INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'CCG - GP Practice or Residence; Provider of Responsibility' AS BREAKDOWN
,CCG AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,ResponsibleProv AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS78' AS METRIC
,SUM(ElgibleDischFlag_Modified) METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY CCG, ResponsibleProv

# COMMAND ----------

# DBTITLE 1,CCG - GP Practice or Residence; Provider of Responsibility- MHS79
%sql

INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'CCG - GP Practice or Residence; Provider of Responsibility' AS BREAKDOWN
,CCG AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,ResponsibleProv AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS79' AS METRIC
,SUM(FollowedUp3Days) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY CCG, ResponsibleProv

# COMMAND ----------

%sql
INSERT INTO $db_output.main_monthly_unformatted_new
SELECT 
'$month_id' UniqMonthID
,'$status' AS STATUS
,'$rp_startdate' AS REPORTING_PERIOD_START
,'$rp_enddate' AS REPORTING_PERIOD_END
,'CCG - GP Practice or Residence; Provider of Responsibility' AS BREAKDOWN
,CCG AS PRIMARY_LEVEL
,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
,ResponsibleProv AS SECONDARY_LEVEL
,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
,'MHS80' AS METRIC
,coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100), 0) AS METRIC_VALUE
       ,'$db_source' as SOURCE_DB
       ,2 AS PRODUCT_NO
FROM   $db_output.72hours_unrounded_stg
WHERE  UniqMonthID = '$month_id'
AND    Status = '$status'
 AND SOURCE_DB = '$db_source'
GROUP  BY CCG, ResponsibleProv