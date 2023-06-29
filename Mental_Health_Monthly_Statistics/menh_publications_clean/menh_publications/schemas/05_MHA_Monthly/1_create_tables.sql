-- Databricks notebook source
%py
print("MHA_Monthly create tables")

db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

-- COMMAND ----------

%sql
DROP TABLE IF EXISTS $db_output.mha_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.mha_breakdown_values (breakdown string) USING DELTA;

DROP TABLE IF EXISTS $db_output.mha_level_values_1;
CREATE TABLE IF NOT EXISTS $db_output.mha_level_values_1 (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA; --See above.

DROP TABLE IF EXISTS $db_output.mha_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.mha_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,drop original table MHA_monthly and replace with an unformatted version that keeps data for posterity
DROP TABLE IF EXISTS $db_output.mha_monthly;

CREATE TABLE IF NOT EXISTS $db_output.mha_monthly_unformatted 
(
    UniqMonthID INT,
    REPORTING_PERIOD_START string,
    REPORTING_PERIOD_END string,
    STATUS string,
    BREAKDOWN string,
    PRIMARY_LEVEL string,
    PRIMARY_LEVEL_DESCRIPTION string,
    SECONDARY_LEVEL string,
    SECONDARY_LEVEL_DESCRIPTION string,
    METRIC string,
    METRIC_VALUE int,
    SOURCE_DB string
)
USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Create table DETENTIONS_monthly
--DROP TABLE IF EXISTS $db_output.DETENTIONS_monthly;

CREATE TABLE IF NOT EXISTS $db_output.DETENTIONS_monthly
(
    Person_ID string
    ,MHA_RecordNumber string
    ,UniqMHActEpisodeID string
    ,orgidProv string
    ,NAME string
    ,ORG_TYPE_CODE string
    ,StartDateMHActLegalStatusClass date
    ,StartTimeMHActLegalStatusClass timestamp
    ,ExpiryDateMHActLegalStatusClass date
    ,EndDateMHActLegalStatusClass timestamp
    ,LegalStatusCode string
    ,MHA_RANK int
    ,HOSP_RANK int
    ,UniqHospProvSpellNum string
    ,HOSP_ADM_RANK int
    ,PrevDischDestCodeHospProvSpell string
    ,AdmMethCodeHospProvSpell string
    ,StartDateHospProvSpell date
    ,StartTimeHospProvSpell timestamp
    ,DischDateHospProvSpell date
    ,DischTimeHospProvSpell timestamp
    ,Detention_Cat string
    ,Detention_DateTime_Cat string 
    ,PrevUniqMHActEpisodeID string
    ,PrevRecordNumber string
    ,PrevLegalStatus string
    ,PrevMHAStartDate date
    ,PrevMHAEndDate date
    ,MHS404UniqID string
    ,CTORecordNumber string
    ,StartDateCommTreatOrd date
    ,EndDateCommTreatOrd date 
    ,CommTreatOrdEndReason string
    ,MHA_Logic_Cat_full string
    ,AgeRepPeriodEnd string
    ,EthnicCategory string
    ,Gender string
    ,IC_REC_CCG string
    ,CCG_NAME string
)
USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Create table SHORT_TERM_ORDERS_MONTHLY
--DROP TABLE IF EXISTS $db_output.SHORT_TERM_ORDERS_MONTHLY;

CREATE TABLE IF NOT EXISTS $db_output.SHORT_TERM_ORDERS_MONTHLY
(
    Person_ID string
    ,MHA_RecordNumber string
    ,UniqMHActEpisodeID string
    ,orgidProv string
    ,NAME string
    ,ORG_TYPE_CODE string
    ,StartDateMHActLegalStatusClass date
    ,StartTimeMHActLegalStatusClass timestamp
    ,ExpiryDateMHActLegalStatusClass date
    ,EndDateMHActLegalStatusClass timestamp
    ,LegalStatusCode string
    ,MHA_RANK int
    ,HOSP_RANK int
    ,UniqHospProvSpellNum string
    ,HOSP_ADM_RANK int
    ,PrevDischDestCodeHospProvSpell string
    ,AdmMethCodeHospProvSpell string
    ,StartDateHospProvSpell date
    ,StartTimeHospProvSpell timestamp
    ,DischDateHospProvSpell date
    ,DischTimeHospProvSpell timestamp
    ,Detention_Cat string
    ,Detention_DateTime_Cat string 
    ,PrevUniqMHActEpisodeID string
    ,PrevRecordNumber string
    ,PrevLegalStatus string
    ,PrevMHAStartDate date
    ,PrevMHAEndDate date
    ,MHS404UniqID string
    ,CTORecordNumber string
    ,StartDateCommTreatOrd date
    ,EndDateCommTreatOrd date 
    ,CommTreatOrdEndReason string
    ,MHA_Logic_Cat_full string
    ,AgeRepPeriodEnd string
    ,EthnicCategory string
    ,Gender string
    ,IC_REC_CCG string
    ,CCG_NAME string
)
USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Create table CTO_MONTHLY
--DROP TABLE IF EXISTS $db_output.CTO_MONTHLY;

CREATE TABLE IF NOT EXISTS $db_output.CTO_MONTHLY
(
    Person_ID STRING,
    CTO_UniqMHActEpisodeID STRING,
    MHS404UniqID STRING,
    orgidProv STRING,
    NAME STRING,
    ORG_TYPE_CODE STRING,
    StartDateCommTreatOrd DATE,
    ExpiryDateCommTreatOrd DATE,
    EndDateCommTreatOrd DATE,
    CommTreatOrdEndReason STRING,
    MHA_UniqMHActEpisodeID STRING,
    LegalStatusCode STRING,
    StartDateMHActLegalStatusClass DATE,
    ExpiryDateMHActLegalStatusClass DATE,
    EndDateMHActLegalStatusClass DATE,
    AgeRepPeriodEnd STRING,
    EthnicCategory STRING,
    Gender STRING,
    IC_REC_CCG string,
    CCG_NAME string
)
USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Create table MHA_Monthly_CSV - DROP THIS 
DROP TABLE IF EXISTS $db_output.mha_monthly_csv;

-- CREATE TABLE IF NOT EXISTS $db_output.MHA_Monthly_CSV
-- (
--     BREAKDOWN string,
--     PRIMARY_LEVEL string,
--     PRIMARY_LEVEL_DESCRIPTION string,
--     SECONDARY_LEVEL string,
--     SECONDARY_LEVEL_DESCRIPTION string,
--     MEASURE_ID string,
--     MEASURE_NAME string
-- )
-- USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Create formatted table - MHA_All_products_formatted - DROP THIS 
DROP TABLE IF EXISTS $db_output.mha_all_products_formatted;

-- CREATE TABLE IF NOT EXISTS $db_output.MHA_All_products_formatted 
-- (
--     REPORTING_PERIOD_START string,
--     REPORTING_PERIOD_END string,
--     STATUS string,
--     BREAKDOWN string,
--     PRIMARY_LEVEL string,
--     PRIMARY_LEVEL_DESCRIPTION string,
--     SECONDARY_LEVEL string,
--     SECONDARY_LEVEL_DESCRIPTION string,
--     MEASURE_ID string,
--     MEASURE_NAME string,
--     MEASURE_VALUE string,
--     MEASURE_VALUE_ROUNDED string
-- )
-- USING DELTA