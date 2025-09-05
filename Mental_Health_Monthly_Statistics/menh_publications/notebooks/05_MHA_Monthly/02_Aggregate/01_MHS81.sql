-- Databricks notebook source
 %py
 db_output=dbutils.widgets.get("db_output")
 print(db_output)
 assert db_output
 db_source=dbutils.widgets.get("db_source")
 print(db_source)
 assert db_source
 reference_data=dbutils.widgets.get("reference_data")
 print(reference_data)
 assert reference_data
 status=dbutils.widgets.get("status")
 print(status)
 assert status
 rp_startdate=dbutils.widgets.get("rp_startdate")
 print(rp_startdate)
 assert rp_startdate
 rp_enddate=dbutils.widgets.get("rp_enddate")
 print(rp_enddate)
 assert rp_enddate
 month_id=dbutils.widgets.get("month_id")
 print(month_id)
 assert month_id

-- COMMAND ----------

-- DBTITLE 1,MHS81 - National level
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'England' AS BREAKDOWN,
    'England' AS PRIMARY_LEVEL,
    'England' AS PRIMARY_LEVEL_DESCRIPTION,
    'NONE' AS SECONDARY_LEVEL,
    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')


-- COMMAND ----------

-- DBTITLE 1,MHS81 - National level; Age
INSERT INTO $db_output.mha_monthly_unformatted

SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'England; Age' AS BREAKDOWN,
    'England' AS PRIMARY_LEVEL,
    'England' AS PRIMARY_LEVEL_DESCRIPTION,
    AGEBAND AS SECONDARY_LEVEL,
    AGEBAND AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')

GROUP BY AGEBAND

-- COMMAND ----------

-- DBTITLE 1,MHS81 - National level; Gender
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'England; Gender' AS BREAKDOWN,
    'England' AS PRIMARY_LEVEL,
    'England' AS PRIMARY_LEVEL_DESCRIPTION,
    DER_GENDER AS SECONDARY_LEVEL,
    GENDER_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY DER_GENDER, GENDER_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - National level; Ethnicity
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'England; Ethnicity' AS BREAKDOWN,
    'England' AS PRIMARY_LEVEL,
    'England' AS PRIMARY_LEVEL_DESCRIPTION,
    ETHNIC_DESCRIPTION AS SECONDARY_LEVEL,
    ETHNIC_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY ETHNIC_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - National level; IMD Decile
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'England; IMD Decile' AS BREAKDOWN,
    'England' AS PRIMARY_LEVEL,
    'England' AS PRIMARY_LEVEL_DESCRIPTION,
    IMD_DESCRIPTION AS SECONDARY_LEVEL,
    IMD_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY IMD_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - Provider level
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'Provider' AS BREAKDOWN,
    OrgIDProv AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    'NONE' AS SECONDARY_LEVEL,
    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY  ORGIDPROV, NAME


-- COMMAND ----------

-- DBTITLE 1,MHS81 - Provider level; Age
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'Provider; Age' AS BREAKDOWN,
    ORGIDPROV AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    AGEBAND AS SECONDARY_LEVEL,
    AGEBAND AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY  ORGIDPROV, NAME, AGEBAND

-- COMMAND ----------

-- DBTITLE 1,MHS81 - Provider level; Gender
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'Provider; Gender' AS BREAKDOWN,
    ORGIDPROV AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    DER_GENDER AS SECONDARY_LEVEL,
    GENDER_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY  ORGIDPROV, NAME, DER_GENDER, GENDER_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - Provider level; Ethnicity
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'Provider; Ethnicity' AS BREAKDOWN,
    ORGIDPROV AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    ETHNIC_DESCRIPTION AS SECONDARY_LEVEL,
    ETHNIC_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY  ORGIDPROV, NAME, ETHNIC_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - Provider level; IMD Decile
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'Provider; IMD Decile' AS BREAKDOWN,
    ORGIDPROV AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    IMD_DESCRIPTION AS SECONDARY_LEVEL,
    IMD_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY ORGIDPROV, NAME, IMD_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - CCG level
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'CCG - GP Practice or Residence' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
    'NONE' AS SECONDARY_LEVEL,
    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB
    
FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY IC_REC_CCG, CCG_NAME

-- COMMAND ----------

-- DBTITLE 1,MHS81 - CCG level; Age
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'CCG - GP Practice or Residence; Age' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
    AGEBAND AS SECONDARY_LEVEL,
    AGEBAND AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB
    
FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY IC_REC_CCG, CCG_NAME, AGEBAND

-- COMMAND ----------

-- DBTITLE 1,MHS81 - CCG level; Gender
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'CCG - GP Practice or Residence; Gender' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
    DER_GENDER AS SECONDARY_LEVEL,
    GENDER_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB
    
FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY IC_REC_CCG, CCG_NAME, DER_GENDER, GENDER_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - CCG level; Ethnicity
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
    ETHNIC_DESCRIPTION AS SECONDARY_LEVEL,
    ETHNIC_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB
    
FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY IC_REC_CCG, CCG_NAME, ETHNIC_DESCRIPTION

-- COMMAND ----------

-- DBTITLE 1,MHS81 - CCG level; IMD Decile
INSERT INTO $db_output.mha_monthly_unformatted
SELECT 
    '$month_id' AS UniqMonthID,
    '$rp_startdate' AS REPORTING_PERIOD_START,
    '$rp_enddate' AS REPORTING_PERIOD_END,
    '$status' AS STATUS,
    'CCG - GP Practice or Residence; IMD Decile' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
    IMD_DESCRIPTION AS SECONDARY_LEVEL,
    IMD_DESCRIPTION AS SECONDARY_LEVEL_DESCRIPTION,
    'MHS81' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB
    
FROM $db_output.Detentions_monthly
WHERE MHA_Logic_Cat_full in ('A','B','C','D','P')
GROUP BY IC_REC_CCG, CCG_NAME, IMD_DESCRIPTION