-- Databricks notebook source
-- DBTITLE 1,possible Gender field name change - tbc (additonal field added in V5 GenderIDCode)
/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
IDENTIFY REFERRALS TO PERINATAL SERVICES
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/


CREATE OR REPLACE TEMPORARY VIEW refs AS

SELECT DISTINCT
   r.UniqMonthID,
   r.RecordNumber,
   r.Person_ID,
   r.ServiceRequestId,
   r.UniqServReqID,
   r.OrgIDProv,
   r.ReferralRequestReceivedDate,
   COALESCE(map.CCG21CDH, 'UNKNOWN') AS OrgIDSubICBRes,
   COALESCE(map.CCG21NM, 'UNKNOWN') AS SubICB_Name,
   m.LADistrictAuth,
   COALESCE(m.EthnicCategory, 'UNKNOWN') AS EthnicCategory, 
   CASE 
     WHEN m.NHSDEthnicity = 'A' THEN 'British'
     WHEN m.NHSDEthnicity = 'B' THEN 'Irish'
     WHEN m.NHSDEthnicity = 'C' THEN 'Any Other White Background'
     WHEN m.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
     WHEN m.NHSDEthnicity = 'E' THEN 'White and Black African'
     WHEN m.NHSDEthnicity = 'F' THEN 'White and Asian'
     WHEN m.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
     WHEN m.NHSDEthnicity = 'H' THEN 'Indian'
     WHEN m.NHSDEthnicity = 'J' THEN 'Pakistani'
     WHEN m.NHSDEthnicity = 'K' THEN 'Bangladeshi'
     WHEN m.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
     WHEN m.NHSDEthnicity = 'M' THEN 'Caribbean'
     WHEN m.NHSDEthnicity = 'N' THEN 'African'
     WHEN m.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
     WHEN m.NHSDEthnicity = 'R' THEN 'Chinese'
     WHEN m.NHSDEthnicity = 'S' THEN 'Any Other Ethnic Group'
     WHEN m.NHSDEthnicity = 'Z' THEN 'Not Stated'
     WHEN m.NHSDEthnicity = '99' THEN 'Not Known'
     ELSE 'UNKNOWN'
     END AS EthnicityLow,
   m.EthnicityHigher,
   COALESCE(map.STP21CDH, 'UNKNOWN') AS ICB_Code,
   COALESCE(map.STP21NM, 'UNKNOWN') AS ICB_Name,
   COALESCE(map.NHSER21CDH, 'UNKNOWN') AS Region_Code,
   COALESCE(map.NHSER21NM, 'UNKNOWN') AS Region_Name
FROM $db_source.MHS101Referral r

INNER JOIN $db_source.MHS102ServiceTypeReferredTo s
   ON s.RecordNumber = r.RecordNumber
  AND s.UniqServReqID = r.UniqServReqID
  AND s.ServTeamTypeRefToMH in ('C02', 'F02') --Specialist Perinatal Services 
  and s.uniqmonthid between '$end_month_id'-11 AND '$end_month_id'

INNER JOIN $db_source.MHS001MPI m
   ON m.recordnumber = r.recordnumber
  AND CASE
        WHEN '$end_month_id' >= 1477 and m.GenderIDCode in ('2','3') THEN '1' -- Gender Identity Code is female or non-binary (including trans women)
        WHEN '$end_month_id' >= 1477 and m.GenderIDCode= '1' and m.GenderSameAtBirth = 'N' THEN '1' -- Gender identity code is Male but Gender is not same as birth
        WHEN '$end_month_id' >= 1477 and (m.GenderIDCode is null OR m.GenderIDCode not in ('1','2','3')) and m.Gender = '2' THEN '1' -- Gender identity not recorded so Female Person Stated Gender
        WHEN '$end_month_id' < 1477 and m.Gender = '2' THEN '1'
        ELSE 0 
        END = '1' 
  AND (m.LADistrictAuth IS NULL OR m.LADistrictAuth LIKE ('E%') or ladistrictauth = '')
  and m.uniqmonthid between '$end_month_id'-11 AND '$end_month_id' 
LEFT JOIN $db_output.CCG_mapping_2021 map
   on CASE WHEN r.UniqMonthID <= 1467 then m.OrgIDCCGRes
           WHEN r.UniqMonthID  > 1467 then m.OrgIDSubICBLocResidence
           ELSE 'ERROR' END = map.CCG_unmapped 

WHERE  r.UniqMonthID BETWEEN '$end_month_id'-11 AND '$end_month_id'   
    


-- COMMAND ----------

-- DBTITLE 1,ConsMediumUsed To "ConsMechanismMH", and code '03' (Telemedicine) to '11' (Video conference)
CREATE OR REPLACE TEMPORARY VIEW conts12 AS
---Prior October 2021
SELECT
r.UniqMonthID,
r.Person_ID,
r.RecordNumber,
r.LADistrictAuth,
r.EthnicCategory,
r.EthnicityLow,
r.OrgIDProv,
r.OrgIDSubICBRes,
r.UniqServReqID,
c.UniqCareContID,
c.CareContDate AS Der_ContactDate,
--r.LSOA2011,
r.ICB_Code,
r.Region_Code



FROM Refs r

INNER JOIN $db_source.MHS201CareContact c 
       ON r.RecordNumber = c.RecordNumber 
       AND r.UniqServReqID = c.UniqServReqID 
       AND c.AttendOrDNACode IN ('5','6') AND c.ConsMechanismMH IN ('01', '03') and c.UniqMonthid < '1459'   

UNION ALL

SELECT
r.UniqMonthID,
r.Person_ID,
r.RecordNumber,
r.LADistrictAuth,
r.EthnicCategory,
r.EthnicityLow,
r.OrgIDProv,
r.OrgIDSubICBRes,
r.UniqServReqID,
c.UniqCareContID,
c.CareContDate AS Der_ContactDate,
--r.LSOA2011,
r.ICB_Code,
r.Region_Code


FROM Refs r

INNER JOIN $db_source.MHS201CareContact c 
       ON r.RecordNumber = c.RecordNumber 
       AND r.UniqServReqID = c.UniqServReqID 
       AND c.AttendOrDNACode IN ('5','6') AND c.ConsMechanismMH IN ('01', '11') and c.UniqMonthid >= '1459'  

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW contYTD AS

SELECT
       c.UniqMonthID,
       c.Person_ID,
       c.RecordNumber,
       c.LADistrictAuth,
       c.OrgIDProv,
       c.OrgIDSubICBRes,
       c.UniqServReqID,
       c.UniqCareContID,
        c.ICB_Code,
        c.Region_Code,
       ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.LADistrictAuth ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessLARN,
       ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDSubICBRes ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessSubICBRN,
       ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDProv ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRNProv,
       ROW_NUMBER () OVER(PARTITION BY c.Person_ID ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessEngRN,
       ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.ICB_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessICBRN,
       ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.Region_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRegionRN

FROM Conts12 c

WHERE c.UniqMonthID BETWEEN '$end_month_id'-11 AND '$end_month_id'

-- COMMAND ----------

CREATE OR REPLACE  TEMPORARY VIEW RD_ORG_DAILY_LATEST AS
SELECT DISTINCT ORG_CODE, 
                NAME
           FROM $ref_database.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

-- COMMAND ----------

INSERT INTO $db_output.Perinatal_M_Master

SELECT DISTINCT
r.UniqMonthID,
r.Person_ID,
r.UniqServReqID,
r.OrgIDProv,
o.NAME,
r.OrgIDSubICBRes,
o2.NAME as SubICB_Name, 
r.ICB_Code,
ICB_Name,
r.Region_Code,
Region_Name,
COALESCE(r.LADistrictAuth,'Unknown') AS LACode,
r.EthnicCategory,  
r.EthnicityLow,
r.EthnicityHigher,

CASE WHEN c1.Contacts >0 THEN 1 ELSE NULL END AS AttContacts,

CASE WHEN c1.Contacts = 0 THEN 1 ELSE NULL END AS NotAttContacts,

c2.FYAccessLARN, 
c2.FYAccessRNProv,
c2.FYAccessSubICBRN,
c2.FYAccessICBRN,
c2.FYAccessRegionRN,
c2.FYAccessEngRN

FROM Refs r    

LEFT JOIN 
       (SELECT
 c.UniqMonthID,
             c.RecordNumber,
             c.UniqServReqID,
             COUNT(c.UniqCareContID) AS Contacts
       FROM Conts12 c
       GROUP BY c.UniqMonthID, c.RecordNumber, c.UniqServReqID) c1
             ON r.RecordNumber = c1.RecordNumber
             AND r.UniqServReqID = c1.UniqServReqID

LEFT JOIN ContYTD c2 
       ON r.RecordNumber = c2.RecordNumber
       AND r.UniqServReqID = c2.UniqServReqID
        AND (c2.FYAccessRNProv = 1 
              OR c2.FYAccessSubICBRN = 1
              OR c2.FYAccessICBRN = 1
              OR c2.FYAccessRegionRN = 1)   
   
LEFT JOIN RD_ORG_DAILY_LATEST o
      ON r.OrgIDProv = o.ORG_CODE 
   
LEFT JOIN RD_ORG_DAILY_LATEST o2 --Added to pull out latest CCG Name
      ON r.OrgIDSubICBRes = o2.ORG_CODE

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS Breakdown, 
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION, 
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS91' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessEngRN = 1 THEN m.Person_ID END) AS MEASURE_VALUE
from $db_output.Perinatal_M_Master m


-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England; Ethnicity' AS Breakdown, 
m.EthnicCategory AS PRIMARY_LEVEL,
m.EthnicityLow AS PRIMARY_LEVEL_DESCRIPTION, 
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS91' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessEngRN = 1 THEN m.Person_ID END) AS MEASURE_VALUE
from $db_output.Perinatal_M_Master m
group by 
m.EthnicCategory,
m.EthnicityLow

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS Breakdown,
h.OrgIDProvider AS PRIMARY_LEVEL, 
h.NAME AS PRIMARY_LEVEL_DESCRIPTION, 
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS91' AS MEASURE_ID,
'' AS MEASURE_NAME,
coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessRNProv = 1 THEN m.Person_ID END),0) AS MEASURE_VALUE
FROM (SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN RD_ORG_DAILY_LATEST o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -11 and $end_month_id)
        h
LEFT JOIN $db_output.Perinatal_M_Master m on m.orgidprov = h.OrgIDPRovider
GROUP BY h.OrgIDProvider, h.name


-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly


SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS Breakdown, 
h.CCG21CDH AS PRIMARY_LEVEL, 
COALESCE(m.SubICB_Name, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION, 
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS91' AS MEASURE_ID,
'' AS MEASURE_NAME,
coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessSubICBRN = 1 THEN m.Person_ID END),0) AS MEASURE_VALUE
FROM (SELECT 
       DISTINCT CCG21CDH
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Perinatal_M_Master m on h.ccg21cdh = m.OrgIDSubICBRes
GROUP BY h.CCG21CDH, COALESCE(m.SubICB_Name, 'UNKNOWN')


-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB' AS Breakdown,
 h.STP21CDH AS PRIMARY_LEVEL, 
 h.STP21NM AS LEVEL_ONE_DESCRIPTION, 
 'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS91' AS MEASURE_ID,
'' AS MEASURE_NAME,
coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessICBRN = 1 THEN m.Person_ID END),0) AS MEASURE_VALUE
FROM (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Perinatal_M_Master m on h.STP21CDH = m.ICB_Code
GROUP BY h.STP21CDH, h.STP21NM



-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS Breakdown,
h.NHSER21CDH AS PRIMARY_LEVEL, 
h.NHSER21NM AS LEVEL_ONE_DESCRIPTION, 
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS91' AS MEASURE_ID,
'' AS MEASURE_NAME,
coalesce(COUNT(DISTINCT CASE WHEN m.AttContacts > 0 AND m.FYAccessRegionRN = 1 THEN m.Person_ID END),0) AS MEASURE_VALUE
FROM (SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Perinatal_M_Master m on h.NHSER21CDH = m.REGION_code
GROUP BY h.NHSER21CDH, h.NHSER21NM


-- COMMAND ----------

--  %py
--  import json
--  dbutils.notebook.exit(json.dumps({
--    "status": "OK"
--  }))