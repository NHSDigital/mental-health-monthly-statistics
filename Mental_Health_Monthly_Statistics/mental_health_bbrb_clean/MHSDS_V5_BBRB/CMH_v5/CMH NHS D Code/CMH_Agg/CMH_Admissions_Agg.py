# Databricks notebook source
# DBTITLE 1,Create Output Table
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_admissions_monthly;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_admissions_monthly
 (
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 STATUS string,
 BREAKDOWN string,
 PRIMARY_LEVEL string,
 PRIMARY_LEVEL_DESCRIPTION string,
 SECONDARY_LEVEL string,
 SECONDARY_LEVEL_DESCRIPTION string,
 MEASURE_ID string,
 MEASURE_NAME string,
 MEASURE_VALUE float
 ) USING DELTA

# COMMAND ----------

# DBTITLE 1,Create ICB/STP;Ethnicity Reference Data
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_stp_ethnicity;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_stp_ethnicity USING DELTA AS
 
 SELECT DISTINCT STP21CDH, STP21NM, Ethnicity
 FROM
 (SELECT DISTINCT STP21CDH, STP21NM, 'a' as tag FROM $db_output.CCG_MAPPING_2021) stp  
 CROSS JOIN (SELECT DISTINCT Ethnicity, 'a' as tag FROM $db_output.CMH_Agg) cmh ON stp.tag = cmh.tag
 ORDER BY STP21CDH, Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106 - England - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,MHS106a - England - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,MHS106b - England - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,MHS107a - England - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,MHS107b - England - Rates of admission without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,MHS106 - SubICB - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN, 
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---added to account for blank SubICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

# DBTITLE 1,MHS106a - SubICB - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---added to account for blank SubICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

# DBTITLE 1,MHS106b - SubICB - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---added to account for blank SubICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

# DBTITLE 1,MHS107a - SubICB - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---added to account for blank SubICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

# DBTITLE 1,MHS107b - SubICB - Rates of admission without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN, -- added extra space 4/10/22
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 --ROUND((SUM(NoContact)/SUM(Admissions))*100, 1) AS MEASURE_VALUE
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---added to account for blank SubICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

# DBTITLE 1,MHS106 - Ethnicity - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 Ethnicity AS PRIMARY_LEVEL,
 Ethnicity AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106a - Ethnicity - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 Ethnicity AS PRIMARY_LEVEL,
 Ethnicity AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106b - Ethnicity - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 Ethnicity AS PRIMARY_LEVEL,
 Ethnicity AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS107a - Ethnicity - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 Ethnicity AS PRIMARY_LEVEL,
 Ethnicity AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS107b - Ethnicity - Rates of admission without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 Ethnicity AS PRIMARY_LEVEL,
 Ethnicity AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106 - ICB; Ethnicity - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB; Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 h.Ethnicity AS SECONDARY_LEVEL,
 h.Ethnicity AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM, Ethnicity
       FROM $db_output.cmh_stp_ethnicity) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") AND h.Ethnicity = c.Ethnicity ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM, h.Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106 - ICB - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

# DBTITLE 1,MHS106a - ICB; Ethnicity - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB; Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 h.Ethnicity AS SECONDARY_LEVEL,
 h.Ethnicity AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM, Ethnicity
       FROM $db_output.cmh_stp_ethnicity) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") AND h.Ethnicity = c.Ethnicity ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM, h.Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106a - ICB - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

# DBTITLE 1,MHS106b - ICB; Ethnicity - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB; Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 h.Ethnicity AS SECONDARY_LEVEL,
 h.Ethnicity AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM, Ethnicity
       FROM $db_output.cmh_stp_ethnicity) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") AND h.Ethnicity = c.Ethnicity ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM, h.Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS106b - ICB - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

# DBTITLE 1,MHS107a - ICB; Ethnicity - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB; Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 h.Ethnicity AS SECONDARY_LEVEL,
 h.Ethnicity AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM, Ethnicity
       FROM $db_output.cmh_stp_ethnicity) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") AND h.Ethnicity = c.Ethnicity ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM, h.Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS107a - ICB - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

# DBTITLE 1,MHS107b - ICB; Ethnicity - Rates of admission without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB; Ethnicity (White British/Non-White British)' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 h.Ethnicity AS SECONDARY_LEVEL,
 h.Ethnicity AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0), 0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM, Ethnicity
       FROM $db_output.cmh_stp_ethnicity) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") AND h.Ethnicity = c.Ethnicity ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM, h.Ethnicity

# COMMAND ----------

# DBTITLE 1,MHS107b - ICB - Rates of admission without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB' AS BREAKDOWN,
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Agg c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---added to account for blank ICB Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

# DBTITLE 1,MHS106 - Provider - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Agg c on h.OrgIDProvider = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

# DBTITLE 1,MHS106a - Provider - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Agg c on h.OrgIDProvider = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

# DBTITLE 1,MHS106b - Provider - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Agg c on h.OrgIDProvider = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

# DBTITLE 1,MHS107a - Provider - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Agg c on h.OrgIDProvider = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

# DBTITLE 1,MHS107b - Provider - Rates of admission with no contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Agg c on h.OrgIDProvider = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

# DBTITLE 1,MHS106 - Commissioning Region - Admissions
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Admissions), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---added to account for blank Region Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

# DBTITLE 1,MHS106a - Commissioning Region - Admissions with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(Contact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---added to account for blank Region Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

# DBTITLE 1,MHS106b - Commissioning Region - Admissions without contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS106b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE(SUM(NoContact), 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---added to account for blank Region Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

# DBTITLE 1,MHS107a - Commissioning Region - Rates of admission with contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107a' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(Contact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---added to account for blank Region Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

# DBTITLE 1,MHS107b - Commissioning Region - Rates of admission with no contact
 %sql
 INSERT INTO $db_output.cmh_admissions_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS107b' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 ROUND(COALESCE((SUM(NoContact)/SUM(Admissions))*100, 0),0) AS MEASURE_VALUE
 
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---added to account for blank Region Codes
 WHERE ReportingPeriodStartDate >= '$rp_startdate_qtr' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cmh_admissions_monthly

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))