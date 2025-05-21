-- Databricks notebook source
 %md

 # NB this notebook hasn't been fully updated for v5 
 ## perinatal measures are currently being produced within the BBRB code in LIVE - once everything else is stable for v5 this can be addressed - there was still an outstanding difference for MHS92.

-- COMMAND ----------

 %py
 month_id=dbutils.widgets.get("month_id")
 print(month_id)
 assert month_id
 db_output=dbutils.widgets.get("db_output")
 print(db_output)
 assert db_output
 db_source=dbutils.widgets.get("db_source")
 print(db_source)
 assert db_source
 $reference_data=dbutils.widgets.get("$reference_data")
 print($reference_data)
 assert $reference_data
 status=dbutils.widgets.get("status")
 print(status)
 assert status
 rp_startdate=dbutils.widgets.get("rp_startdate")
 print(rp_startdate)
 assert rp_startdate
 rp_enddate=dbutils.widgets.get("rp_enddate")
 print(rp_enddate)
 assert rp_enddate

-- COMMAND ----------

-- DBTITLE 1,Referrals
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW MHA_Referrals AS 
 SELECT DISTINCT
    r.UniqMonthID,
    r.RecordNumber,
    r.Person_ID,
    r.ServiceRequestId,
    r.UniqServReqID,
    r.OrgIDProv,
    r.ReferralRequestReceivedDate,
    COALESCE(map.CCG_CODE, 'UNKNOWN') AS OrgIDCCGRes,
    COALESCE(map.CCG_DESCRIPTION, 'UNKNOWN') AS CCG_NAME,
    m.LADistrictAuth,
    COALESCE(map.STP_CODE, 'UNKNOWN') AS STP_Code,
    COALESCE(map.STP_DESCRIPTION, 'UNKNOWN') AS STP_Name,
    COALESCE(map.REGION_CODE, 'UNKNOWN') AS Region_Code,
    COALESCE(map.REGION_DESCRIPTION, 'UNKNOWN') AS Region_Name
 FROM $db_source.MHS101Referral r

 INNER JOIN $db_output.ServiceTeamType s ON s.RecordNumber = r.RecordNumber AND s.UniqServReqID = r.UniqServReqID AND s.ServTeamTypeRefToMH = 'C02' 
                 and s.uniqmonthid between $month_id-11 AND $month_id

 INNER JOIN $db_source.MHS001MPI m ON m.recordnumber = r.recordnumber AND m.Gender = '2' AND (m.LADistrictAuth IS NULL OR m.LADistrictAuth LIKE ('E%') or ladistrictauth = '')
                 and m.uniqmonthid between $month_id-11 AND $month_id

 LEFT JOIN $db_output.STP_Region_mapping_post_2020 map on m.OrgIDCCGRes = map.CCG_CODE

 WHERE  r.UniqMonthID BETWEEN $month_id-11 AND $month_id


-- COMMAND ----------

-- DBTITLE 1,Contacts from referrals
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW MHA_Contacts AS 

 SELECT
 r.UniqMonthID,
 r.Person_ID,
 r.RecordNumber,
 r.LADistrictAuth,
 r.OrgIDProv,
 r.OrgIDCCGRes,
 r.UniqServReqID,
 c.UniqCareContID,
 c.CareContDate AS Der_ContactDate,
 r.STP_Code,
 r.Region_Code

 FROM global_temp.MHA_Referrals r

 INNER JOIN $db_source.MHS201CareContact c 
        ON r.RecordNumber = c.RecordNumber 
        AND r.UniqServReqID = c.UniqServReqID 
        AND c.AttendStatus IN ('5','6') AND c.ConsMechanismMH IN ('01', '03')                      ---V6_Changes


-- COMMAND ----------

-- DBTITLE 1,Contacts YTD
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW MHA_ContactYTD AS 
 SELECT
        c.UniqMonthID,
        c.Person_ID,
        c.RecordNumber,
        c.LADistrictAuth,
        c.OrgIDProv,
        c.OrgIDCCGRes,
        c.UniqServReqID,
        c.UniqCareContID,
        c.STP_Code,
        c.Region_Code,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.LADistrictAuth ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessLARN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDCCGRes ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessCCGRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDProv ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRNProv,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessEngRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.STP_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessSTPRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.Region_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRegionRN

 FROM global_temp.MHA_Contacts c

 WHERE c.UniqMonthID BETWEEN $month_id-11 AND $month_id



-- COMMAND ----------

-- DBTITLE 1,master
 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW MHA91_Main AS

 SELECT DISTINCT
 r.UniqMonthID,
 r.Person_ID,
 r.UniqServReqID,
 r.OrgIDProv,
 o.NAME,
 r.OrgIDCCGRes,
 CCG_NAME,
 r.STP_Code,
 STP_NAME,
 r.REGION_CODE,
 REGION_NAME,
 COALESCE(r.LADistrictAuth,'Unknown') AS LACode,

 CASE WHEN c1.Contacts >0 THEN 1 ELSE NULL END AS AttContacts,

 CASE WHEN c1.Contacts = 0 THEN 1 ELSE NULL END AS NotAttContacts,

 c2.FYAccessLARN, 
 c2.FYAccessRNProv,
 c2.FYAccessCCGRN,
 c2.FYAccessSTPRN,
 c2.FYAccessRegionRN,
 c2.FYAccessEngRN

 FROM global_temp.MHA_Referrals r    

 LEFT JOIN 
        (SELECT
  c.UniqMonthID,
              c.RecordNumber,
              c.UniqServReqID,
              COUNT(c.UniqCareContID) AS Contacts
        FROM global_temp.MHA_Contacts c
        GROUP BY c.UniqMonthID, c.RecordNumber, c.UniqServReqID) c1
              ON r.RecordNumber = c1.RecordNumber
              AND r.UniqServReqID = c1.UniqServReqID

 LEFT JOIN global_temp.MHA_ContactYTD c2 
        ON r.RecordNumber = c2.RecordNumber
        AND r.UniqServReqID = c2.UniqServReqID
         AND (c2.FYAccessRNProv = 1 
               OR c2.FYAccessCCGRN = 1
               OR c2.FYAccessSTPRN = 1
               OR c2.FYAccessRegionRN = 1)   
    
 LEFT JOIN $db_output.RD_ORG_DAILY_LATEST o
       ON r.OrgIDProv = o.ORG_CODE


-- COMMAND ----------

-- DBTITLE 1,Final data
 %sql

 INSERT INTO $db_output.cyp_peri_monthly_unformatted

 SELECT
  add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS Breakdown, 
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION, 
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS91' AS METRIC,
 COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessEngRN = 1 THEN m.Person_ID END) AS METRIC_VALUE,
 '$db_source' AS SOURCE_DB
 from global_temp.MHA91_Main m

 union all

 SELECT
 add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS Breakdown,
 h.OrgIDProvider AS PRIMARY_LEVEL, 
 o.NAME AS PRIMARY_LEVEL_DESCRIPTION, 
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS91' AS METRIC,
 coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessRNProv = 1 THEN m.Person_ID END),0) AS METRIC_VALUE,
 '$db_source' AS SOURCE_DB
 FROM (SELECT 
         DISTINCT ORGIDPROVIDER 
         FROM $db_source.MHS000HEADER
         WHERE
         UNIQMONTHID BETWEEN $month_id -11 and $month_id)
         h
 LEFT JOIN global_temp.MHA91_Main m on m.orgidprov = h.OrgIDPRovider
 LEFT JOIN $db_output.RD_ORG_DAILY_LATEST o on o.ORG_CODE = h.ORGIDPROVIDER
 GROUP BY h.OrgIDProvider, o.name

 union all

 SELECT
 add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence' AS Breakdown, 
 h.CCG_CODE AS PRIMARY_LEVEL, 
 h.CCG_DESCRIPTION AS PRIMARY_LEVEL_DESCRIPTION, 
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS91' AS METRIC,
 coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessCCGRN = 1 THEN m.Person_ID END),0) AS METRIC_VALUE,
 '$db_source' AS SOURCE_DB
 FROM (SELECT 
        DISTINCT CCG_CODE, CCG_DESCRIPTION
        FROM $db_output.STP_Region_mapping_post_2020) h 
 LEFT JOIN global_temp.MHA91_Main m on h.CCG_CODE = m.orgidccgres
 GROUP BY h.CCG_CODE, h.CCG_DESCRIPTION

 union all

 SELECT
 add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP' AS Breakdown,
  h.STP_CODE AS PRIMARY_LEVEL, 
  h.STP_DESCRIPTION AS LEVEL_ONE_DESCRIPTION, 
  'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS91' AS METRIC,
 coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessSTPRN = 1 THEN m.Person_ID END),0) AS METRIC_VALUE,
 '$db_source' AS SOURCE_DB
 FROM (SELECT 
        DISTINCT STP_CODE, STP_DESCRIPTION
        FROM $db_output.STP_Region_mapping_post_2020) h 
 LEFT JOIN global_temp.MHA91_Main m on h.STP_CODE = m.stp_code
 GROUP BY h.STP_CODE, h.STP_DESCRIPTION


 UNION ALL 

 SELECT
 add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Region' AS Breakdown,
 h.REGION_CODE AS PRIMARY_LEVEL, 
 h.REGION_DESCRIPTION AS LEVEL_ONE_DESCRIPTION, 
 'NONE' AS SECONDARY_LEVEL,
 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
 'MHS91' AS METRIC,
 coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessRegionRN = 1 THEN m.Person_ID END),0) AS METRIC_VALUE,
 '$db_source' AS SOURCE_DB
 FROM (SELECT 
        DISTINCT REGION_CODE, REGION_DESCRIPTION
        FROM $db_output.STP_Region_mapping_post_2020) h 
 LEFT JOIN global_temp.MHA91_Main m on h.REGION_CODE = m.REGION_code
 GROUP BY h.REGION_CODE, h.REGION_DESCRIPTION
