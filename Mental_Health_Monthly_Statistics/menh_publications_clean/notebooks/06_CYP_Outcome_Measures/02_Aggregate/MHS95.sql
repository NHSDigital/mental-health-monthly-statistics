-- Databricks notebook source
%sql

CREATE WIDGET TEXT month_id DEFAULT "1448"; 
CREATE WIDGET TEXT rp_startdate DEFAULT "2020-01-01";
CREATE WIDGET TEXT rp_enddate DEFAULT "2020-12-31";
CREATE WIDGET TEXT db_source DEFAULT "testdata_menh_publications_$db_source";
CREATE WIDGET TEXT db_output DEFAULT "menh_publications";
CREATE WIDGET TEXT status DEFAULT "Performance";
CREATE WIDGET TEXT database DEFAULT "database";

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
database=dbutils.widgets.get("database")
print(database)
assert database
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

FROM $db_source.MHS101Referral r

INNER JOIN $db_source.MHS001MPI m ON r.RecordNumber = m.RecordNumber

WHERE 
r.AgeServReferRecDate BETWEEN 0 AND 17 AND 
r.UniqMonthID BETWEEN $month_id -11 AND $month_id 
AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR LADistrictAuth = '')

-- COMMAND ----------

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

WHERE c.AttendOrDNACode IN ('5','6') AND c.ConsMechanismMH NOT IN ('05','06') OR (c.OrgIDProv = 'DFC' AND c.ConsMechanismMH IN ('05','06'))

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
	a.Der_ContactDate

FROM Comb a
INNER JOIN Ref r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID
WHERE COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) BETWEEN 0 AND 17

-- COMMAND ----------

INSERT INTO $db_output.MHS95_Main
SELECT
	a.UniqMonthID,
	a.OrgIDProv,
	o.NAME AS PROV_NAME,
    COALESCE(c.CCG_CODE,'UNKNOWN') AS IC_REC_CCG,
    COALESCE(c.CCG_DESCRIPTION,'UNKNOWN') AS CCG_NAME,
    COALESCE(c.STP_CODE,'UNKNOWN') AS STP_CODE,
    COALESCE(c.STP_DESCRIPTION,'UNKNOWN') AS STP_NAME,
    COALESCE(c.REGION_CODE,'UNKNOWN') AS REGION_CODE,
    COALESCE(c.REGION_DESCRIPTION,'UNKNOWN') AS REGION_NAME,
	a.LADistrictAuth,
	a.Person_ID,
	a.RecordNumber,
	a.UniqServReqID,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC) AS AccessLARN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG_CODE ORDER BY a.Der_ContactDate ASC) AS AccessCCGRN,
    ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG_CODE, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessCCGProvRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessRNProv,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC) AS AccessEngRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.STP_CODE ORDER BY a.Der_ContactDate ASC) AS AccessSTPRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.REGION_CODE ORDER BY a.Der_ContactDate ASC) AS AccessRegionRN

FROM Act as A
LEFT JOIN $db_output.STP_Region_mapping_post_2020 C on a.DER_ORGCOMM = C.CCG_CODE
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST o on a.OrgIDProv = o.ORG_CODE

-- COMMAND ----------

INSERT INTO $db_output.cyp_peri_monthly_unformatted
SELECT
add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS METRIC,
COUNT(DISTINCT PERSON_ID) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB
FROM $db_output.MHS95_Main
WHERE
AccessEngRN = '1'

-- COMMAND ----------

INSERT INTO $db_output.cyp_peri_monthly_unformatted

SELECT
add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS METRIC,
COUNT(DISTINCT PERSON_ID) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER H
        LEFT JOIN $db_output.RD_ORG_DAILY_LATEST b ON H.ORGIDPROVIDER = B.ORG_CODE 
        WHERE
        UNIQMONTHID BETWEEN $month_id -11 and $month_id) h
LEFT JOIN $db_output.MHS95_Main f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1'

GROUP BY 
h.ORGIDPROVIDER,
h.NAME


-- COMMAND ----------

INSERT INTO $db_output.cyp_peri_monthly_unformatted
SELECT
add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'CCG of Residence; Provider' AS BREAKDOWN,
IC_REC_CCG AS PRIMARY_LEVEL,
CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
OrgIDProv AS SECONDARY_LEVEL,
PROV_NAME AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS METRIC,
COUNT(DISTINCT PERSON_ID) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB
FROM
$db_output.MHS95_Main
WHERE
AccessCCGProvRN = '1'
GROUP BY 
IC_REC_CCG,
CCG_NAME,
OrgIDProv,
PROV_NAME

-- COMMAND ----------

-- DBTITLE 1,CCG of Residence

INSERT INTO $db_output.cyp_peri_monthly_unformatted
SELECT
add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'CCG of Residence' AS BREAKDOWN,
IC_REC_CCG AS PRIMARY_LEVEL,
CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS METRIC,
COUNT(DISTINCT PERSON_ID) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB
FROM
$db_output.MHS95_Main
WHERE
AccessCCGRN = '1'
GROUP BY 
IC_REC_CCG,
CCG_NAME

-- COMMAND ----------

INSERT INTO $db_output.cyp_peri_monthly_unformatted

SELECT
add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'STP' AS BREAKDOWN,
 h.STP_CODE AS PRIMARY_LEVEL,
h.STP_DESCRIPTION AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS METRIC,
COUNT(DISTINCT PERSON_ID) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB
FROM
(SELECT 
       DISTINCT  STP_CODE, STP_DESCRIPTION
       FROM $db_output.STP_Region_mapping_post_2020) h 
LEFT JOIN $db_output.MHS95_Main a on a.STP_CODE = h. STP_CODE and a.AccessSTPRN = '1'
GROUP BY 
h.STP_CODE,
STP_DESCRIPTION

-- COMMAND ----------

INSERT INTO $db_output.cyp_peri_monthly_unformatted

SELECT
add_months('$rp_startdate', -11)  AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Region' AS BREAKDOWN,
h.REGION_CODE AS PRIMARY_LEVEL,
h.REGION_DESCRIPTION AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS METRIC,
COUNT(DISTINCT PERSON_ID) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB
FROM
(SELECT 
       DISTINCT REGION_CODE, REGION_DESCRIPTION
       FROM $db_output.STP_Region_mapping_post_2020) h 
LEFT JOIN $db_output.MHS95_Main a on a.REGION_CODE = h.REGION_CODE and a.AccessRegionRN = '1'
GROUP BY 
h.REGION_CODE,
h.REGION_DESCRIPTION
