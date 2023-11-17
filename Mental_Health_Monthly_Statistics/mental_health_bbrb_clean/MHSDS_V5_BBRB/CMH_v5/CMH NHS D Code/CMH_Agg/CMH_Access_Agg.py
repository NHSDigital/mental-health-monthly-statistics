# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_monthly;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_access_monthly
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

# DBTITLE 1,MHS108 - England
 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS108' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT Person_ID) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1

# COMMAND ----------

# DBTITLE 1,MHS108 - Provider
 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS108' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT Person_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov and c.AccessRNProv = 1
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

# DBTITLE 1,MHS108 - SubICB
 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 -- 'CCG of Residence' AS BREAKDOWN,
 -- h.CCG21CDH AS PRIMARY_LEVEL,
 -- h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'Sub ICB of Residence' AS BREAKDOWN, -- updated with extra spac 04/10/22
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS108' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT Person_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 WHERE AccessSubICBRN = 1
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

# DBTITLE 1,MHS108 - ICB
 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 --'STP' AS BREAKDOWN, 
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS108' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT Person_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 WHERE AccessICBRN = 1
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

# DBTITLE 1,MHS108 - Commissioning Region
 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS108' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT Person_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 WHERE AccessRegionRN = 1
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %md
 Calculation of: 
  - Number of referrals (MHS124)
  - Median Waiting Times (MHS125)
  - 90th Percentile Waiting Times (MHS126) 
  for those with 2 contacts

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS124' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity_medians_2nd_cont

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS124' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c on h.OrgIDProvider = c.orgidprov 
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS124' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS124' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS124' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS125' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.5) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity_medians_2nd_cont

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS125' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c on h.OrgIDProvider = c.orgidprov 
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS125' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS125' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS125' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS126' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.9) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity_medians_2nd_cont

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS126' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c on h.OrgIDProvider = c.orgidprov 
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS126' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS126' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS126' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TIME_TO_2ND_CONTACT,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_2nd_cont c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %md
 Calculation of: 
  - Number of referrals (MHS127)
  - Median Waiting Times (MHS128)
  - 90th Percentile Waiting Times (MHS129) 
  for those still waiting

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS127' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity_medians_still_waiting

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS127' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c on h.OrgIDProvider = c.orgidprov 
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS127' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS127' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS127' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS128' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.5) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity_medians_still_waiting

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS128' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c on h.OrgIDProvider = c.orgidprov 
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS128' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS128' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS128' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.5) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS129' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.9) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Rolling_Activity_medians_still_waiting

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS129' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $start_month_id and $end_month_id)
         h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c on h.OrgIDProvider = c.orgidprov 
 GROUP BY h.ORGIDPROVIDER, h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Sub ICB of Residence' AS BREAKDOWN,
 h.CCG21CDH AS PRIMARY_LEVEL,
 h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS129' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
 GROUP BY h.CCG21CDH, h.CCG21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
 h.STP21CDH AS PRIMARY_LEVEL,
 h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS129' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.CCG_MAPPING_2021) h
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
 GROUP BY h.STP21CDH, h.STP21NM

# COMMAND ----------

 %sql
 INSERT INTO $db_output.cmh_access_monthly
 
 SELECT
 '$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 h.NHSER21CDH AS PRIMARY_LEVEL,
 h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS129' AS MEASURE_ID,
 '' AS MEASURE_NAME,
 PERCENTILE(TimeFromRefToEndRP,0.9) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.CCG_MAPPING_2021) h 
 LEFT JOIN $db_output.CMH_Rolling_Activity_medians_still_waiting c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
 GROUP BY h.NHSER21CDH, h.NHSER21NM

# COMMAND ----------

 %sql
 OPTIMIZE  $db_output.cmh_access_monthly

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))