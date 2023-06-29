-- Databricks notebook source
-- DBTITLE 1,'UNIQHOSPPROVSPELLNUM' will change to 'UniqHospProvSpellID'
CREATE OR REPLACE TEMP VIEW INPATIENTS AS 

SELECT 
PERSON_ID,
UNIQSERVREQID,
UniqHospProvSpellID 
FROM
$db_source.MHS501HOSPPROVSPELL 
WHERE
RECORDSTARTDATE < '$rp_enddate' AND (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')

-- COMMAND ----------

-- DBTITLE 1,'UNIQHOSPPROVSPELLNUM' will change to 'UniqHospProvSpellID'
INSERT INTO $db_output.CLOSED_REFS

SELECT 
A.UNIQMONTHID,
A.PERSON_ID,
A.UNIQSERVREQID,
A.RECORDNUMBER,
A.ORGIDPROV,
A.REFERRALREQUESTRECEIVEDDATE,
A.SERVDISCHDATE,
A.AgeServReferRecDate 
,DATEDIFF(A.ServDischDate, A.REFERRALREQUESTRECEIVEDDATE) AS REF_LENGTH
FROM
$db_source.MHS101REFERRAL A
LEFT JOIN INPATIENTS B ON A.PERSON_ID = B.PERSON_ID AND A.UNIQSERVREQID = B.UNIQSERVREQID
INNER JOIN $db_source.MHS001MPI M ON A.RECORDNUMBER = M.RECORDNUMBER
WHERE
A.AgeServReferRecDate  BETWEEN 0 AND 17 
and A.UniqMonthID = '$end_month_id'
and a.ServDischDate is not null 
and B.UniqHospProvSpellID is null 
AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = '')

-- COMMAND ----------

OPTIMIZE $db_output.CLOSED_REFS

-- COMMAND ----------

-- DBTITLE 1,'ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09'
CREATE
OR REPLACE TEMPORARY VIEW Cont AS
SELECT
  'DIRECT' AS CONT_TYPE,
  c.UniqMonthID,
  c.Person_ID,
  c.UniqServReqID,
  c.AgeCareContDate,
  c.UniqCareContID AS ContID,
  c.CareContDate AS ContDate,
  c.MHS201UniqID as UniqID
FROM
  $db_source.MHS201CareContact c
WHERE
  (
    ( c.AttendOrDNACode IN ('5', '6') and ((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')))   
    or 
    ( ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')) and OrgIdProv = 'DFC') 
   )
  and CareContDate <= '$rp_enddate'
  and carecontdate >= '$financial_yr_start'
UNION ALL
SELECT
  'INDIRECT' AS CONT_TYPE,
  i.UniqMonthID,
  i.Person_ID,
  i.UniqServReqID,
  NULL AS AgeCareContDate,
  CAST(i.MHS204UniqID AS string) AS ContID,
  i.IndirectActDate AS ContDate,
  i.MHS204UniqID as UniqID
FROM
  $db_source.MHS204IndirectActivity i
WHERE
  IndirectActDate <= '$rp_enddate'
  and indirectactdate >= '$financial_yr_start'

-- COMMAND ----------

INSERT INTO $db_output.CONT_FINAL 

SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, CONT_TYPE ORDER BY ContDate ASC, UniqID ASC) AS RN1, -- Added contact type to the partition to ensure that only instances of 2 contacts or 2 indirect act are counted.
ROW_NUMBER () OVER(PARTITION BY UniqServReqID, CONT_TYPE ORDER BY ContDate ASC, UniqID ASC) AS DFC_RN1
FROM
Cont

-- COMMAND ----------

OPTIMIZE $db_output.CONT_FINAL

-- COMMAND ----------

INSERT INTO $db_output.ASSESSMENTS 

SELECT
  'CON' as TYPE,
  a.UniqMonthID,
  a.CodedAssToolType,
  a.PersScore,
  c.CareContDate AS Der_AssToolCompDate,
  a.RecordNumber,
  a.MHS607UniqID AS Der_AssUniqID,
  a.OrgIDProv,
  a.Person_ID,
  a.UniqServReqID,
  a.AgeAssessToolCont AS Der_AgeAssessTool,
  a.UniqCareContID,
  a.UniqCareActID
FROM
  $db_source.MHS607CodedScoreAssessmentAct a
  LEFT JOIN $db_source.MHS201CARECONTACT C ON a.RecordNumber = c.RecordNumber
  AND a.UniqServReqID = c.UniqServReqID
  AND a.UniqCareContID = c.UniqCareContID
WHERE
  A.UNIQMONTHID <= '$end_month_id'

-- COMMAND ----------

-- DBTITLE 1,"AssToolCompDate" will change to "AssToolCompTimeStamp" in MHS606CodedScoreAssessmentRefer
INSERT INTO $db_output.ASSESSMENTS 

SELECT
  'REF' AS Type,
  r.UniqMonthID,
  r.CodedAssToolType,
  r.PersScore,
  COALESCE(r.AssToolCompTimeStamp,r.AssToolCompDate) AS Der_AssToolCompDate, 
  r.RecordNumber,
  r.MHS606UniqID AS Der_AssUniqID,
  r.OrgIDProv,
  r.Person_ID,
  r.UniqServReqID,
  r.AgeAssessToolReferCompDate AS Der_AgeAssessTool,
  NULL AS UniqCareContID,
  NULL AS UniqCareActID
FROM
  $db_source.MHS606CodedScoreAssessmentRefer r
WHERE
  r.UNIQMONTHID <= '$end_month_id'


-- COMMAND ----------

-- DBTITLE 1,"AssToolCompDate" no change in MHS801ClusterTool
INSERT INTO $db_output.ASSESSMENTS 

SELECT
  'CLU' AS Der_AssTable,
  c.UniqMonthID,
  a.CodedAssToolType,
  a.PersScore,
 c.AssToolCompDate AS Der_AssToolCompDate, 
 c.RecordNumber,
  a.MHS802UniqID AS Der_AssUniqID,
  a.OrgIDProv,
  a.Person_ID,
  r.UniqServReqID,
  NULL AS Der_AgeAssessTool,
  NULL AS UniqCareContID,
  NULL AS UniqCareActID
FROM
  $db_source.MHS802ClusterAssess a
  LEFT JOIN $db_source.MHS801ClusterTool c ON c.UniqClustID = a.UniqClustID
  AND c.RecordNumber = a.RecordNumber
  LEFT JOIN $db_source.MHS101Referral r ON r.RecordNumber = c.RecordNumber
 AND c.AssToolCompDate BETWEEN r.ReferralRequestReceivedDate 
 AND COALESCE(r.ServDischDate, '$rp_enddate')

WHERE
  a.UniqMonthID <= '$end_month_id'

-- COMMAND ----------

OPTIMIZE $db_output.ASSESSMENTS

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ASS_STG AS

SELECT 	
	a.Der_AssUniqID,
	a.TYPE,
	a.Person_ID,	
	a.UniqMonthID,	
	a.OrgIDProv,
	a.RecordNumber,	
	a.UniqServReqID,	
	a.UniqCareContID,	
	a.UniqCareActID,		
	a.Der_AssToolCompDate,
	a.CodedAssToolType,
	a.PersScore,
	a.Der_AgeAssessTool,
	r.Category AS Der_AssessmentCategory,
	r.Assessment_Tool_Name,
	r.Preferred_Term_SNOMED AS Der_PreferredTermSNOMED,
	r.SNOMED_Version AS Der_SNOMEDCodeVersion,
	r.Lower_Range AS Der_LowerRange,
	r.Upper_Range AS Der_UpperRange,
	CASE 
		WHEN a.PersScore BETWEEN r.Lower_Range AND r.Upper_Range THEN 'Y' 
		ELSE NULL 
	END AS Der_ValidScore,
	CASE 
		WHEN ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.Der_AssToolCompDate, COALESCE(a.UniqServReqID,0), r.Preferred_Term_SNOMED, a.PersScore ORDER BY a.Der_AssUniqID ASC) = 1
		THEN 'Y' 
		ELSE NULL 
	END AS Der_UniqAssessment

FROM $db_output.ASSESSMENTS a

LEFT JOIN $db_output.MH_ASS r ON a.CodedAssToolType = r.Active_Concept_ID_SNOMED

-- COMMAND ----------

INSERT INTO $db_output.ASS_FINAL

SELECT 
a.*,
ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.UniqServReqID, a.CodedAssToolType ORDER BY a.Der_AssToolCompDate ASC) AS Der_AssOrderAsc, --First assessment
ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.UniqServReqID, a.CodedAssToolType ORDER BY a.Der_AssToolCompDate DESC) AS Der_AssOrderDesc 
FROM 
    ( SELECT * FROM ASS_STG WHERE DER_VALIDSCORE = 'Y' AND DER_UNIQASSESSMENT = 'Y') AS a
INNER JOIN $db_output.CLOSED_REFS b on a.UniqServReqID = b.UniqServReqID and a.Person_ID = b.Person_ID
WHERE
DER_VALIDSCORE = 'Y'
AND DER_UNIQASSESSMENT = 'Y'

-- COMMAND ----------

OPTIMIZE $db_output.ASS_FINAL

-- COMMAND ----------

INSERT INTO $db_output.CYP_OUTCOMES
 
SELECT
r.UniqMonthID,
r.Person_ID,
r.UniqServReqID,
r.RecordNumber,
r.OrgIDProv,
r.AgeServReferRecDate,
r.ReferralRequestReceivedDate,
r.ServDischDate,
r.Ref_Length,
CASE
    WHEN r.OrgIDProv = 'DFC' THEN c.DFC_RN1
    ELSE c.RN1
END AS Der_InYearContacts,
a1.Der_AssToolCompDate AS Der_FirstAssessmentDate,
a1.Assessment_Tool_Name AS Der_FirstAssessmentToolName,
a2.Der_AssToolCompDate AS Der_LastAssessmentDate,
a2.Assessment_Tool_Name AS Der_LastAssessmentToolName,
CASE 
    WHEN r.ReferralRequestReceivedDate >= '2016-01-01' 
    THEN DATEDIFF(a1.Der_AssToolCompDate,r.ReferralRequestReceivedDate) 
END AS Der_ReftoFirstAss, --limited to those referrals that were received after the MHSDS started
DATEDIFF(r.ServDischDate,a2.Der_AssToolCompDate) AS Der_LastAsstoDisch
FROM
$db_output.CLOSED_REFS r 
LEFT JOIN $db_output.CONT_FINAL c on r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID
LEFT JOIN $db_output.ASS_FINAL a1 on r.UniqServReqID = a1.UniqServReqID AND r.Person_ID = a1.Person_ID and a1.Der_AssOrderAsc = '1'
LEFT JOIN $db_output.ASS_FINAL a2 on r.UniqServReqID = a2.UniqServReqID AND r.Person_ID = a2.Person_ID AND a1.CodedAssToolType = a2.CodedAssToolType AND a2.Der_AssToolCompDate > a1.Der_AssToolCompDate AND a2.Der_AssOrderDesc = 1

-- COMMAND ----------

OPTIMIZE $db_output.CYP_OUTCOMES

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW DENOMINATOR AS
SELECT
  'England' as OrgID,
  COUNT(DISTINCT r.UNIQSERVREQID) as COUNT
FROM
  $db_output.CLOSED_REFS r
  INNER JOIN $db_output.CONT_FINAL c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID
  AND (
    c.RN1 > 1
    or (
      r.OrgIDProv = 'DFC'
      and c.DFC_RN1 > 1
    )
  )
  WHERE r.REF_LENGTH > 14
UNION ALL
SELECT
  r.OrgIDProv as OrgID,
  COUNT(DISTINCT r.UNIQSERVREQID) as COUNT
FROM
  $db_output.CLOSED_REFS r
  INNER JOIN $db_output.CONT_FINAL c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID
  AND (
    c.RN1 > 1
    or (
      r.OrgIDProv = 'DFC'
      and c.DFC_RN1 > 1
    )
  )
  WHERE r.REF_LENGTH > 14
GROUP BY
  r.OrgIDProv

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW NUMERATOR AS 

SELECT 
'England' as OrgIDProv,
COUNT(DISTINCT(r.uniqservreqid)) as COUNT
FROM
$db_output.CLOSED_REFS r 
INNER JOIN $db_output.CONT_FINAL c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID AND (c.RN1 > 1 or (r.OrgIDProv = 'DFC' and c.DFC_RN1 > 1))
INNER JOIN $db_output.ASS_FINAL a1 on r.UniqServReqID = a1.UniqServReqID and r.Person_ID = a1.Person_ID and a1.Der_AssOrderAsc = '1'
INNER JOIN $db_output.ASS_FINAL a2 on r.UniqServReqID = a2.UniqServReqID and r.Person_ID = a2.Person_ID and a2.Der_AssOrderDesc = '1' and a2.Der_AssToolCompDate > a1.Der_AssToolCompDate and a1.CodedAssToolType = a2.CodedAssToolType
WHERE r.REF_LENGTH > 14

UNION ALL

SELECT 
r.OrgIDProv as OrgIDProv,
COUNT(DISTINCT(r.uniqservreqid)) as COUNT
FROM
$db_output.CLOSED_REFS r 
INNER JOIN $db_output.CONT_FINAL c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID AND (c.RN1 > 1 or (r.OrgIDProv = 'DFC' and c.DFC_RN1 > 1))
INNER JOIN $db_output.ASS_FINAL a1 on r.UniqServReqID = a1.UniqServReqID and r.Person_ID = a1.Person_ID and a1.Der_AssOrderAsc = '1'
INNER JOIN $db_output.ASS_FINAL a2 on r.UniqServReqID = a2.UniqServReqID and r.Person_ID = a2.Person_ID and a2.Der_AssOrderDesc = '1' and a2.Der_AssToolCompDate > a1.Der_AssToolCompDate and a1.CodedAssToolType = a2.CodedAssToolType
WHERE r.REF_LENGTH > 14
GROUP BY 
r.OrgIDProv

-- COMMAND ----------

CREATE OR REPLACE  TEMPORARY VIEW RD_ORG_DAILY_LATEST AS
SELECT DISTINCT ORG_CODE, 
                NAME
           FROM $ref_database.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

-- COMMAND ----------

INSERT INTO $db_output.CYP_OUTCOMES_OUTPUT

SELECT 
d.OrgID,
n.COUNT as MHS92,
d.COUNT as MHS93,
((n.Count / d.Count) * 100)  as MHS94
FROM
Numerator n 
LEFT JOIN Denominator d on n.orgidprov = d.Orgid
WHERE
d.OrgID = 'England'

UNION ALL

SELECT
h.OrgIDProvider as OrgID,
n.COUNT as MHS92,
d.COUNT as MHS93,
((n.Count / d.Count) * 100) as MHS94
FROM
$db_source.MHS000HEADER H
LEFT JOIN Numerator n on h.orgidprovider = n.Orgidprov
LEFT JOIN Denominator d on h.orgidprovider = d.Orgid
WHERE
H.UniqMonthID = '$end_month_id'

-- COMMAND ----------

INSERT INTO $db_output.CYP_PERI_monthly

SELECT
'$rp_startdate_1m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
CASE 
WHEN ORGID = 'England' THEN 'England'
ELSE 'Provider'
END AS BREAKDOWN,
ORGID as PRIMARY_LEVEL, 
CASE 
WHEN OrgID = 'England' THEN 'England'
ELSE NAME
END as PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS92' AS MEASURE_ID,
'' AS MEASURE_NAME,
COALESCE(MHS92, 0) AS MEASURE_VALUE 
FROM $db_output.CYP_OUTCOMES_OUTPUT a
LEFT JOIN RD_ORG_DAILY_LATEST b ON A.ORGID = B.ORG_CODE

UNION ALL

SELECT
'$rp_startdate_1m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
CASE 
WHEN ORGID = 'England' THEN 'England'
ELSE 'Provider'
END AS BREAKDOWN,
ORGID as PRIMARY_LEVEL, 
CASE 
WHEN OrgID = 'England' THEN 'England'
ELSE NAME
END as PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS93' AS MEASURE_ID,
'' AS MEASURE_NAME,
COALESCE(MHS93, 0) AS MEASURE_VALUE 
FROM $db_output.CYP_OUTCOMES_OUTPUT a
LEFT JOIN RD_ORG_DAILY_LATEST b ON A.ORGID = B.ORG_CODE

UNION ALL

SELECT
'$rp_startdate_1m' AS REPORTING_PERIOD_START_DATE,
'$rp_enddate' AS REPORTING_PERIOD_END_DATE,
'$status' AS STATUS,
CASE 
WHEN ORGID = 'England' THEN 'England'
ELSE 'Provider'
END AS BREAKDOWN,
ORGID as PRIMARY_LEVEL, 
CASE 
WHEN OrgID = 'England' THEN 'England'
ELSE NAME
END as PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS94' AS MEASURE_ID,
'' AS MEASURE_NAME,
COALESCE(MHS94, 0) AS MEASURE_VALUE 
FROM $db_output.CYP_OUTCOMES_OUTPUT a
LEFT JOIN RD_ORG_DAILY_LATEST b ON A.ORGID = B.ORG_CODE

-- COMMAND ----------

--  %py
--  import json
--  dbutils.notebook.exit(json.dumps({
--    "status": "OK"
--  }))