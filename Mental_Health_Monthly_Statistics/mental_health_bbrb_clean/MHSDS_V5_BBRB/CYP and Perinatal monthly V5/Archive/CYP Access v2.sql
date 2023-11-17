-- Databricks notebook source
%md
### MHSDS V5.0 Changes
#### AM: Dec 16 2021 - Updated code (Cmd 5)for V5.0 change - ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data

-- COMMAND ----------

CREATE OR REPLACE  TEMPORARY VIEW RD_CCG_LATEST AS
SELECT DISTINCT ORG_CODE,
                NAME
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                AND ORG_TYPE_CODE = 'CC'
                AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                AND ORG_OPEN_DATE <= '$rp_enddate'
                AND NAME NOT LIKE '%HUB'
                AND NAME NOT LIKE '%NATIONAL%';

-- COMMAND ----------

CREATE OR REPLACE  TEMPORARY VIEW RD_ORG_DAILY_LATEST AS
SELECT DISTINCT ORG_CODE, 
                NAME
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Ref AS 

SELECT
	r.UniqMonthID,
	r.OrgIDProv,
	CASE 
      WHEN r.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
      ELSE r.Person_ID
      END AS Person_ID,
	r.RecordNumber,
	r.UniqServReqID,
	CASE WHEN r.OrgIDProv = 'DFC' THEN r.OrgIDComm ELSE m.OrgIDCCGRes END AS Der_OrgComm, -- to correctly allocate commissioner to Kooth
	m.LADistrictAuth,
	r.AgeServReferRecDate,
	m.AgeRepPeriodEnd

FROM $db_source.mhs101referral r

INNER JOIN $db_source.mhs001mpi m ON r.RecordNumber = m.RecordNumber

WHERE 
r.AgeServReferRecDate BETWEEN 0 AND 17 AND 
r.UniqMonthID BETWEEN '$end_month_id' -11 AND '$end_month_id' 
AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR LADistrictAuth = '')

-- COMMAND ----------

-- DBTITLE 1,V5 will change from 'ConsMediumUsed' to 'ConsMechanismMH' , code '06' to '09', code '05' no change

CREATE OR REPLACE TEMPORARY VIEW Comb AS

SELECT
    CASE 
      WHEN c.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
      ELSE c.Person_ID
      END AS Person_ID,
	c.RecordNumber,
	c.UniqServReqID,
	c.CareContDate AS Der_ContactDate,
	c.AgeCareContDate

FROM $db_source.MHS201CareContact c
LEFT JOIN $db_source.MHS001MPI m ON c.RecordNumber = m.RecordNumber

WHERE 
  (
    ( c.AttendOrDNACode IN ('5', '6') and ((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')))   
-------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data /*** updated to v5 AM ***/
    or 
    ( ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')) and c.OrgIdProv = 'DFC')            ---/*** change from Oct2021: v5 change AM **/
   )
--c.AttendOrDNACode IN ('5','6') AND c.ConsMechanismMH NOT IN ('05','09') OR (c.OrgIDProv = 'DFC' AND c.ConsMechanismMH IN ('05','09')) 
-------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data V5.0 /*** updated to v5 AM ***/
UNION ALL

SELECT
	CASE 
      WHEN i.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
      ELSE i.Person_ID
      END AS Person_ID,
	i.RecordNumber,
	i.UniqServReqID,
	i.IndirectActDate AS Der_ContactDate,
	NULL AS AgeCareContDate

FROM $db_source.MHS204IndirectActivity i
LEFT JOIN $db_source.MHS001MPI m ON i.RecordNumber = m.RecordNumber

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Act AS 

SELECT
	r.UniqMonthID,
	r.OrgIDProv,
	r.Der_OrgComm,
	r.LADistrictAuth,
	r.Person_ID,
	r.RecordNumber,
	r.UniqServReqID,
	COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) AS Der_ContAge,
	a.Der_ContactDate--,
--	ROW_NUMBER() OVER (PARTITION BY a.Person_ID, a.UniqServReqID ORDER BY a.Der_ContactDate ASC) AS Der_ContactOrder

FROM Comb a

INNER JOIN Ref r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID

WHERE COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) BETWEEN 0 AND 17

-- COMMAND ----------

TRUNCATE TABLE $db_output.FirstCont_Final

-- COMMAND ----------

INSERT INTO $db_output.FirstCont_Final

SELECT
	a.UniqMonthID,
	a.OrgIDProv,
	o.NAME AS PROV_NAME,
    COALESCE(c.CCG21CDH,'UNKNOWN') AS IC_REC_CCG,
    COALESCE(c.CCG21NM,'UNKNOWN') AS CCG_NAME,
    COALESCE(c.STP21CDH,'UNKNOWN') AS STP_CODE,
    COALESCE(c.STP21NM,'UNKNOWN') AS STP_NAME,
    COALESCE(c.NHSER21CDH,'UNKNOWN') AS REGION_CODE,
    COALESCE(c.NHSER21NM,'UNKNOWN') AS REGION_NAME,
	a.LADistrictAuth,
	a.Person_ID,
	a.RecordNumber,
	a.UniqServReqID,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC) AS AccessLARN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH ORDER BY a.Der_ContactDate ASC) AS AccessCCGRN,
    ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessCCGProvRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessRNProv,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC) AS AccessEngRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.STP21CDH ORDER BY a.Der_ContactDate ASC) AS AccessSTPRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.NHSER21CDH ORDER BY a.Der_ContactDate ASC) AS AccessRegionRN

FROM Act as A
LEFT JOIN $db_output.CCG_MAPPING_2021 C on a.DER_ORGCOMM = C.CCG_UNMAPPED
LEFT JOIN RD_ORG_DAILY_LATEST o on a.OrgIDProv = o.ORG_CODE

--WHERE a.Der_ContactOrder = 1

-- COMMAND ----------

OPTIMIZE $db_output.FirstCont_Final

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
$db_output.FirstCont_Final
WHERE
AccessEngRN = '1'

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER H
        LEFT JOIN RD_ORG_DAILY_LATEST b ON H.ORGIDPROVIDER = B.ORG_CODE 
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -11 and $end_month_id)
        h
LEFT JOIN $db_output.FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1'

GROUP BY 
h.ORGIDPROVIDER,
h.NAME

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'CCG - Residence' AS BREAKDOWN, -- amended from 'CCG of Residence' to 'CCG - Residence' for consistency HL 25/01/22
ccg21CDH AS PRIMARY_LEVEL,
ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.FirstCont_Final a on a.ic_rec_ccg = h.ccg21CDH AND A.AccessCCGRN = '1'
GROUP BY 
ccg21CDH,
ccg21nm

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'CCG; Provider' AS BREAKDOWN,
IC_REC_CCG AS PRIMARY_LEVEL,
CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
OrgIDProv AS SECONDARY_LEVEL,
PROV_NAME AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
$db_output.FirstCont_Final
WHERE
AccessCCGProvRN = '1'
GROUP BY 
IC_REC_CCG,
CCG_NAME,
OrgIDProv,
PROV_NAME

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'STP' AS BREAKDOWN,
STP21CDH AS PRIMARY_LEVEL,
STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.FirstCont_Final a on a.STP_CODE = h.STP21CDH and a.AccessSTPRN = '1'
GROUP BY 
STP21CDH,
STP21nm

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Region' AS BREAKDOWN,
NHSER21CDH AS PRIMARY_LEVEL,
NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.FirstCont_Final a on a.REGION_CODE = h.NHSER21CDH and a.AccessRegionRN = '1'
GROUP BY 
NHSER21CDH,
NHSER21nm

-- COMMAND ----------

OPTIMIZE $db_output.CYP_PERI_monthly