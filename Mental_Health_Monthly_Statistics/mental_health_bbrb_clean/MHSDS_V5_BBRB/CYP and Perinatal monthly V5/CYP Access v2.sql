-- Databricks notebook source
%md
### MHSDS V5.0 Changes
#### Dec 16 2021 - Updated code (Cmd 5)for V5.0 change - ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data

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
	Case when r.OrgIDProv = 'DFC' then r.OrgIDComm
         when (r.UniqMonthID <= 1467 and r.OrgIDProv <> "DFC") then m.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
         when (r.UniqMonthID > 1467 and r.OrgIDProv <> "DFC") then m.OrgIDSubICBLocResidence
    else "ERROR" end as Der_OrgComm,
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
-------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data /*** updated to v5 ***/
    or 
    ( ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')) and c.OrgIdProv = 'DFC')            ---/*** change from Oct2021: v5 change **/
   )
--c.AttendOrDNACode IN ('5','6') AND c.ConsMechanismMH NOT IN ('05','09') OR (c.OrgIDProv = 'DFC' AND c.ConsMechanismMH IN ('05','09')) 
-------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data V5.0 /*** updated to v5 ***/
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

CREATE OR REPLACE TEMPORARY VIEW Act_cumulative AS 

SELECT
	r.UniqMonthID,
	r.OrgIDProv,
	Case when r.OrgIDProv = 'DFC' then r.OrgIDComm
         when (r.UniqMonthID <= 1467 and r.OrgIDProv <> "DFC") then m.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
         when (r.UniqMonthID > 1467 and r.OrgIDProv <> "DFC") then m.OrgIDSubICBLocResidence
    else "ERROR" end as Der_OrgComm,
	r.Person_ID,
	r.RecordNumber,
	r.UniqServReqID,
	a.AgeCareContDate AS Der_ContAge,
    r.ReferralRequestReceivedDate,
    r.ServDischDate,
	a.Der_ContactDate,
	DENSE_RANK() OVER (PARTITION BY r.UniqServReqID ORDER BY a.Der_ContactDate ASC, A.AgeCareContDate ASC) AS Der_ContactOrder

FROM $db_source.mhs101referral r

LEFT JOIN Comb a ON a.UniqServReqID = r.UniqServReqID AND a.AgeCareContDate BETWEEN 0 AND 17

LEFT JOIN $db_source.mhs001mpi m 
      ON ((M.RecordEndDate is null or M.RecordEndDate >= '$rp_enddate') and m.recordstartdate < '$rp_enddate') and r.person_id = m.person_id and r.orgidprov = m.orgidprov AND M.PatMRecInRp = True

WHERE 
((r.RecordEndDate is null or r.RecordEndDate >= '$rp_enddate') and r.recordstartdate < '$rp_enddate')
AND r.AgeServReferRecDate BETWEEN 0 AND 17  
AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR LADistrictAuth = '')
AND ReferralRequestReceivedDate >= '2019-04-01'

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.Act_cumulative_master;
CREATE TABLE IF NOT EXISTS $db_output.Act_cumulative_master AS 

SELECT 
DISTINCT 
UniqMonthID,
OrgIDProv,
Der_OrgComm,
Person_ID,
RecordNumber,
UniqServReqID,
Der_ContAge,
ReferralRequestReceivedDate,
ServDischDate,
Der_ContactDate,
Der_ContactOrder,
o.NAME AS PROV_NAME,
COALESCE(c.CCG21CDH,'UNKNOWN') AS SubICB_Code,
COALESCE(c.CCG21NM,'UNKNOWN') AS SubICB_Name,
COALESCE(c.STP21CDH,'UNKNOWN') AS ICB_Code,
COALESCE(c.STP21NM,'UNKNOWN') AS ICB_Name,
COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code,
COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name,
DATEDIFF(Der_ContactDate, ReferralRequestReceivedDate) as TimeFromRefToFirstCont,
DATEDIFF(DATE_ADD('$rp_enddate',1), ReferralRequestReceivedDate) as TimeFromRefToEndRP
FROM
Act_cumulative A
LEFT JOIN menh_bbrb.CCG_MAPPING_2021 C on a.DER_ORGCOMM = C.CCG_UNMAPPED
LEFT JOIN RD_ORG_DAILY_LATEST o on a.OrgIDProv = o.ORG_CODE

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.Act_cumulative_master_first_contact;
CREATE TABLE IF NOT EXISTS $db_output.Act_cumulative_master_first_contact AS 

SELECT 
DISTINCT 
*
FROM
$db_output.Act_cumulative_master A
WHERE
DER_CONTACTORDER = '1' 
AND Der_ContactDate BETWEEN '$rp_startdate_qtr' and '$rp_enddate'

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.Act_cumulative_master_still_waiting;
CREATE TABLE IF NOT EXISTS $db_output.Act_cumulative_master_still_waiting AS 

SELECT 
DISTINCT 
*
FROM
$db_output.Act_cumulative_master A
WHERE
DER_CONTACTORDER = '1' 
AND DER_CONTACTDATE IS NULL 
AND UNIQMONTHID = '$end_month_id'
AND (SERVDISCHDATE IS NULL OR SERVDISCHDATE > '$rp_enddate')

-- COMMAND ----------

TRUNCATE TABLE $db_output.FirstCont_Final

-- COMMAND ----------

-- DBTITLE 1,added Metric field
INSERT INTO $db_output.FirstCont_Final

SELECT
	a.UniqMonthID,
	a.OrgIDProv,
	o.NAME AS PROV_NAME,
    COALESCE(c.CCG21CDH,'UNKNOWN') AS SubICB_Code,
    COALESCE(c.CCG21NM,'UNKNOWN') AS SubICB_Name,
    COALESCE(c.STP21CDH,'UNKNOWN') AS ICB_Code,
    COALESCE(c.STP21NM,'UNKNOWN') AS ICB_Name,
    COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code,
    COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name,
	a.LADistrictAuth,
	a.Person_ID,
	a.RecordNumber,
	a.UniqServReqID,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC) AS AccessLARN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH ORDER BY a.Der_ContactDate ASC) AS AccessSubICBRN,
    ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessSubICBProvRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessRNProv,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC) AS AccessEngRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.STP21CDH ORDER BY a.Der_ContactDate ASC) AS AccessICBRN,
	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.NHSER21CDH ORDER BY a.Der_ContactDate ASC) AS AccessRegionRN,
    'MHS95' AS Metric-- add metric id to table

FROM Act as A
LEFT JOIN $db_output.CCG_MAPPING_2021 C on a.DER_ORGCOMM = C.CCG_UNMAPPED
LEFT JOIN RD_ORG_DAILY_LATEST o on a.OrgIDProv = o.ORG_CODE

--WHERE a.Der_ContactOrder = 1

-- COMMAND ----------

OPTIMIZE $db_output.FirstCont_Final

-- COMMAND ----------

-- DBTITLE 1,England
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
AccessEngRN = '1' AND Metric = 'MHS95'

-- COMMAND ----------

-- DBTITLE 1,Provider
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
LEFT JOIN $db_output.FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
GROUP BY 
h.ORGIDPROVIDER,
h.NAME

-- COMMAND ----------

-- DBTITLE 1,CCG - Residence
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN, -- amended from 'CCG of Residence' to 'CCG of Residence' for consistency HL 25/01/22 chnaged back to CCG of Residence as advised 27/07/22 added space 04/10/22
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
LEFT JOIN $db_output.FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
GROUP BY 
ccg21CDH,
ccg21nm

-- COMMAND ----------

-- DBTITLE 1,CCG; Provider
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence; Provider' AS BREAKDOWN, -- added space 4/10/22
SubICB_Code AS PRIMARY_LEVEL,
SubICB_Name AS PRIMARY_LEVEL_DESCRIPTION,
OrgIDProv AS SECONDARY_LEVEL,
PROV_NAME AS SECONDARY_LEVEL_DESCRIPTION,
'MHS95' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
FROM
$db_output.FirstCont_Final
WHERE
AccessSubICBProvRN = '1'
AND Metric = 'MHS95'
GROUP BY 
SubICB_Code,
SubICB_Name,
OrgIDProv,
PROV_NAME

-- COMMAND ----------

-- DBTITLE 1,STP
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN,
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
LEFT JOIN $db_output.FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
GROUP BY 
STP21CDH,
STP21nm

-- COMMAND ----------

-- DBTITLE 1,Region
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_12m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
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
LEFT JOIN $db_output.FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
GROUP BY 
NHSER21CDH,
NHSER21nm

-- COMMAND ----------

%md
CYP First contact longest waits:
- Number receiving first contact (MHS130)
- Median wait (MHS131)
- 90th percentile wait (MHS132)

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS130' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
$db_output.Act_cumulative_master_first_contact

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS130' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -2 and $end_month_id)
        h
LEFT JOIN $db_output.Act_cumulative_master_first_contact c on h.OrgIDProvider = c.orgidprov 
GROUP BY h.ORGIDPROVIDER, h.NAME

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN,
h.CCG21CDH AS PRIMARY_LEVEL,
h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS130' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
GROUP BY h.CCG21CDH, h.CCG21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
h.STP21CDH AS PRIMARY_LEVEL,
h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS130' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
GROUP BY h.STP21CDH, h.STP21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
h.NHSER21CDH AS PRIMARY_LEVEL,
h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS130' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
GROUP BY h.NHSER21CDH, h.NHSER21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS131' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.50) AS MEASURE_VALUE
FROM
$db_output.Act_cumulative_master_first_contact

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS131' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.50) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -2 and $end_month_id)
        h
LEFT JOIN $db_output.Act_cumulative_master_first_contact c on h.OrgIDProvider = c.orgidprov 
GROUP BY h.ORGIDPROVIDER, h.NAME

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN,
h.CCG21CDH AS PRIMARY_LEVEL,
h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS131' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.50) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
GROUP BY h.CCG21CDH, h.CCG21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
h.STP21CDH AS PRIMARY_LEVEL,
h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS131' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.50) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
GROUP BY h.STP21CDH, h.STP21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
h.NHSER21CDH AS PRIMARY_LEVEL,
h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS131' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.50) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
GROUP BY h.NHSER21CDH, h.NHSER21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS132' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.90) AS MEASURE_VALUE
FROM
$db_output.Act_cumulative_master_first_contact

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS132' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.90) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -2 and $end_month_id)
        h
LEFT JOIN $db_output.Act_cumulative_master_first_contact c on h.OrgIDProvider = c.orgidprov 
GROUP BY h.ORGIDPROVIDER, h.NAME

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN,
h.CCG21CDH AS PRIMARY_LEVEL,
h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS132' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.90) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
GROUP BY h.CCG21CDH, h.CCG21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
h.STP21CDH AS PRIMARY_LEVEL,
h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS132' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.90) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
GROUP BY h.STP21CDH, h.STP21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
h.NHSER21CDH AS PRIMARY_LEVEL,
h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS132' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToFirstCont,0.90) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_first_contact c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
GROUP BY h.NHSER21CDH, h.NHSER21NM

-- COMMAND ----------

%md
CYP First contact longest waits, still waiting:
- Number still waiting (MHS133)
- Median wait (MHS134)
- 90th percentile wait (MHS135)

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS133' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
$db_output.Act_cumulative_master_still_waiting

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS133' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -2 and $end_month_id)
        h
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c on h.OrgIDProvider = c.orgidprov 
GROUP BY h.ORGIDPROVIDER, h.NAME

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN,
h.CCG21CDH AS PRIMARY_LEVEL,
h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS133' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
GROUP BY h.CCG21CDH, h.CCG21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
h.STP21CDH AS PRIMARY_LEVEL,
h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS133' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
GROUP BY h.STP21CDH, h.STP21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
h.NHSER21CDH AS PRIMARY_LEVEL,
h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS133' AS MEASURE_ID,
'' AS MEASURE_NAME,
COUNT(DISTINCT UniqServReqID) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
GROUP BY h.NHSER21CDH, h.NHSER21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS134' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.50) AS MEASURE_VALUE
FROM
$db_output.Act_cumulative_master_still_waiting

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS134' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.50) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -2 and $end_month_id)
        h
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c on h.OrgIDProvider = c.orgidprov 
GROUP BY h.ORGIDPROVIDER, h.NAME

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN,
h.CCG21CDH AS PRIMARY_LEVEL,
h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS134' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.50) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
GROUP BY h.CCG21CDH, h.CCG21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
h.STP21CDH AS PRIMARY_LEVEL,
h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS134' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.50) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
GROUP BY h.STP21CDH, h.STP21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
h.NHSER21CDH AS PRIMARY_LEVEL,
h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS134' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.50) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
GROUP BY h.NHSER21CDH, h.NHSER21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS135' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.90) AS MEASURE_VALUE
FROM
$db_output.Act_cumulative_master_still_waiting

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
h.ORGIDPROVIDER AS PRIMARY_LEVEL,
h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS135' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.90) AS MEASURE_VALUE
FROM
(SELECT 
        DISTINCT ORGIDPROVIDER, NAME
        FROM $db_source.MHS000HEADER h
        LEFT JOIN $db_output.CMH_ORG_DAILY o on o.ORG_CODE = h.ORGIDPROVIDER
        WHERE
        UNIQMONTHID BETWEEN $end_month_id -2 and $end_month_id)
        h
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c on h.OrgIDProvider = c.orgidprov 
GROUP BY h.ORGIDPROVIDER, h.NAME

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN,
h.CCG21CDH AS PRIMARY_LEVEL,
h.CCG21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS135' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.90) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT CCG21CDH, CCG21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.CCG21CDH = COALESCE(c.SubICB_Code, "UNKNOWN") ---Adding to account for blank SubICB codes
GROUP BY h.CCG21CDH, h.CCG21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'ICB of Residence' AS BREAKDOWN, --updated 04/10/22
h.STP21CDH AS PRIMARY_LEVEL,
h.STP21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS135' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.90) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.CCG_MAPPING_2021) h
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.STP21CDH = COALESCE(c.ICB_Code, "UNKNOWN") ---Adding to account for blank ICB codes
GROUP BY h.STP21CDH, h.STP21NM

-- COMMAND ----------

%sql
INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_qtr' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
'Commissioning Region' AS BREAKDOWN,
h.NHSER21CDH AS PRIMARY_LEVEL,
h.NHSER21NM AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS135' AS MEASURE_ID,
'' AS MEASURE_NAME,
PERCENTILE(TimeFromRefToEndRP,0.90) AS MEASURE_VALUE
FROM
(SELECT 
       DISTINCT NHSER21CDH, NHSER21NM
       FROM $db_output.CCG_MAPPING_2021) h 
LEFT JOIN $db_output.Act_cumulative_master_still_waiting c ON h.NHSER21CDH = COALESCE(c.Region_Code, "UNKNOWN") ---Adding to account for blank Region codes
GROUP BY h.NHSER21CDH, h.NHSER21NM

-- COMMAND ----------

OPTIMIZE $db_output.CYP_PERI_monthly

-- COMMAND ----------

%py
import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))