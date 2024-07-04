# Databricks notebook source
# DBTITLE 1,Restrictive Intervention National Total - Count
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

# DBTITLE 1,Restrictive Intervention Type
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Restrictive intervention type' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
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
   GROUP BY         DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription

# COMMAND ----------

# DBTITLE 1,Gender
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Gender' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedGenderCode AS SECONDARY_LEVEL
                    ,DerivedGenderDescription AS SECONDARY_LEVEL_DESCRIPTION
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
   GROUP BY         DerivedGenderCode, DerivedGenderDescription

# COMMAND ----------

# DBTITLE 1,Gender, Type of restraint
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Gender; Restrictive intervention type' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedGenderCode AS SECONDARY_LEVEL
                    ,DerivedGenderDescription AS SECONDARY_LEVEL_DESCRIPTION
                    ,DerivedRestrictiveIntTypeCode AS TERTIARY_LEVEL
                    ,DerivedRestrictiveIntTypeDescription AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   GROUP BY         DerivedGenderCode, DerivedGenderDescription, DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription

# COMMAND ----------

# DBTITLE 1,Ethnicity
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Ethnic Group (16 categories)' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedEthnicityCode AS SECONDARY_LEVEL
                    ,DerivedEthnicityDescription AS SECONDARY_LEVEL_DESCRIPTION
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
   GROUP BY         DerivedEthnicityCode, DerivedEthnicityDescription

# COMMAND ----------

# DBTITLE 1,Ethnicity, Type of restraint
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Ethnic Group (16 categories); Restrictive intervention type' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedEthnicityCode AS SECONDARY_LEVEL
                    ,DerivedEthnicityDescription AS SECONDARY_LEVEL_DESCRIPTION
                    ,DerivedRestrictiveIntTypeCode AS TERTIARY_LEVEL
                    ,DerivedRestrictiveIntTypeDescription AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   GROUP BY         DerivedEthnicityCode, DerivedEthnicityDescription, DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription

# COMMAND ----------

# DBTITLE 1,Ethnic group
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Ethnic Group (5 categories)' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedEthnicityGroupCode AS SECONDARY_LEVEL
                    ,DerivedEthnicityGroupDescription AS SECONDARY_LEVEL_DESCRIPTION
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
   GROUP BY         DerivedEthnicityGroupCode, DerivedEthnicityGroupDescription

# COMMAND ----------

# DBTITLE 1,Ethnic group, Type of restraint
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Ethnic Group (5 categories); Restrictive intervention type' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedEthnicityGroupCode AS SECONDARY_LEVEL
                    ,DerivedEthnicityGroupDescription AS SECONDARY_LEVEL_DESCRIPTION
                    ,DerivedRestrictiveIntTypeCode AS TERTIARY_LEVEL
                    ,DerivedRestrictiveIntTypeDescription AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   GROUP BY         DerivedEthnicityGroupCode, DerivedEthnicityGroupDescription, DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription

# COMMAND ----------

# DBTITLE 1,Age group
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Age Group' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedAgeBand AS SECONDARY_LEVEL
                    ,DerivedAgeBand AS SECONDARY_LEVEL_DESCRIPTION
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
   GROUP BY         DerivedAgeBand

# COMMAND ----------

# DBTITLE 1,Age group, Type of restraint
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Age Group; Restrictive intervention type' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedAgeBand AS SECONDARY_LEVEL
                    ,DerivedAgeBand AS SECONDARY_LEVEL_DESCRIPTION
                    ,DerivedRestrictiveIntTypeCode AS TERTIARY_LEVEL
                    ,DerivedRestrictiveIntTypeDescription AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   GROUP BY         DerivedAgeBand, DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription

# COMMAND ----------

# DBTITLE 1,Gender, Age group
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Gender; Age Group' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedGenderCode AS SECONDARY_LEVEL
                    ,DerivedGenderDescription AS SECONDARY_LEVEL_DESCRIPTION
                    ,DerivedAgeBand AS TERTIARY_LEVEL
                    ,DerivedAgeBand AS TERTIARY_LEVEL_DESCRIPTION
                    ,'NONE' AS QUARTERNARY_LEVEL
                    ,'NONE' AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   GROUP BY         DerivedGenderCode, DerivedGenderDescription, DerivedAgeBand

# COMMAND ----------

# DBTITLE 1,Gender, Age group, Type of restraint
 %sql
 
 Insert into $db_output.MHSRestrictiveInterventionCount
   SELECT           '$rp_startdate' AS REPORTING_PERIOD_START
                    ,'$rp_enddate' AS REPORTING_PERIOD_END
                    ,'$status' AS Status
                    ,'England; Gender; Age Group; Restrictive intervention type' AS BREAKDOWN
                    ,'England' AS PRIMARY_LEVEL
                    ,'England' AS PRIMARY_LEVEL_DESCRIPTION
                    ,DerivedGenderCode AS SECONDARY_LEVEL
                    ,DerivedGenderDescription AS SECONDARY_LEVEL_DESCRIPTION
                    ,DerivedAgeBand AS TERTIARY_LEVEL
                    ,DerivedAgeBand AS TERTIARY_LEVEL_DESCRIPTION
                    ,DerivedRestrictiveIntTypeCode AS QUARTERNARY_LEVEL
                    ,DerivedRestrictiveIntTypeDescription AS QUARTERNARY_LEVEL_DESCRIPTION
                    ,'MHS77' AS MEASURE_ID
                    ,'Number of restrictive interventions in the reporting period' as MEASURE_NAME
                    ,Count(DISTINCT MHS505UniqID) AS MEASURE_VALUE
                    ,$month_id as UniqMonthID
                    ,current_timestamp() as CreatedAt,
                    '$db_source' as SOURCE_DB
   FROM             $db_output.MHSRestrictiveInterventionRaw
   GROUP BY         DerivedGenderCode, DerivedGenderDescription, DerivedAgeBand, DerivedRestrictiveIntTypeCode, DerivedRestrictiveIntTypeDescription