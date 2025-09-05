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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'


-- COMMAND ----------

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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'

GROUP BY AGEBAND

-- COMMAND ----------

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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'
GROUP BY DER_GENDER, GENDER_DESCRIPTION

-- COMMAND ----------

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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'
GROUP BY ETHNIC_DESCRIPTION

-- COMMAND ----------

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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'
GROUP BY IMD_DESCRIPTION

-- COMMAND ----------

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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'
GROUP BY 
IC_REC_CCG, CCG_NAME

-- COMMAND ----------

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
    'MHS143' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '02'
GROUP BY 
OrgIDProv, NAME

-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'


-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'

GROUP BY AGEBAND

-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'
GROUP BY DER_GENDER, GENDER_DESCRIPTION

-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'
GROUP BY ETHNIC_DESCRIPTION

-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'
GROUP BY IMD_DESCRIPTION

-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'
GROUP BY 
IC_REC_CCG, CCG_NAME

-- COMMAND ----------

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
    'MHS144' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('A','B','C','L')
and LegalStatusCode = '03'
GROUP BY 
OrgIDProv, NAME

-- COMMAND ----------

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
    'MHS145' AS METRIC,
    COUNT(DISTINCT UniqMHActEpisodeID) as METRIC_VALUE,
    '$db_source' AS SOURCE_DB

FROM $db_output.Detentions_monthly 
WHERE
MHA_Logic_Cat_full in ('P') 
and LegalStatusCode in ('02','03','07','08','09','10','38','15','16','17','18','12','13','14')