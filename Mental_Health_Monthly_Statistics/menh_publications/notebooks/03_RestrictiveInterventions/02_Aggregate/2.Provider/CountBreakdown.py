# Databricks notebook source
# DBTITLE 1,Restrictive Intervention Provider - Count
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate
rp_enddate=dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
status=dbutils.widgets.get("status")
print(status)
assert status
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id


# COMMAND ----------

# DBTITLE 1,Provider Total
 %sql

 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'Provider' AS BREAKDOWN
                    ,OrgIDProv AS PRIMARY_LEVEL
                    ,DerivedOrgIDProvName AS PRIMARY_LEVEL_DESCRIPTION
                    ,'NONE' AS SECONDARY_LEVEL
                    ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
                    ,'NONE' AS TERTIARY_LEVEL
                    ,'NONE' AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   WHERE            OrgIDProv is not NULL -- exclude zero count
   GROUP BY         OrgIDProv, DerivedOrgIDProvName


# COMMAND ----------

# DBTITLE 1,Restrictive Intervention Type per Provider
 %sql

 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'Provider; Restrictive intervention type' AS BREAKDOWN
                    ,OrgIDProv AS PRIMARY_LEVEL
                    ,DerivedOrgIDProvName AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedRestrictiveIntTypeCode AS SECONDARY_LEVEL
                    ,DerivedRestrictiveIntTypeDescription AS SECONDARY_LEVEL_DESCRIPTION
                    ,'NONE' AS TERTIARY_LEVEL
                    ,'NONE' AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   WHERE            OrgIDProv is not NULL -- exclude zero count
   GROUP BY         OrgIDProv, DerivedOrgIDProvName, DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription
