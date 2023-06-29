-- Databricks notebook source
%py
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate
rp_enddate=dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate

-- COMMAND ----------

-- DBTITLE 1,NUMERATOR
CREATE OR REPLACE TEMPORARY VIEW NUMERATOR AS 

SELECT 
'England' as OrgIDProv,
COUNT(DISTINCT(r.uniqservreqid)) as COUNT
FROM
$db_output.closed_refs r 
INNER JOIN $db_output.cont_final c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID AND (c.RN1 > 1 or (r.OrgIDProv = 'DFC' and c.DFC_RN1 > 1))
INNER JOIN $db_output.ass_final a1 on r.UniqServReqID = a1.UniqServReqID and r.Person_ID = a1.Person_ID and a1.Der_AssOrderAsc = '1'
INNER JOIN $db_output.ass_final a2 on r.UniqServReqID = a2.UniqServReqID and r.Person_ID = a2.Person_ID and a2.Der_AssOrderDesc = '1' and a2.Der_AssToolCompDate > a1.Der_AssToolCompDate and a1.CodedAssToolType = a2.CodedAssToolType
WHERE r.REF_LENGTH > 14

UNION ALL

SELECT 
r.OrgIDProv as OrgIDProv,
COUNT(DISTINCT(r.uniqservreqid)) as COUNT
FROM
$db_output.closed_refs r 
INNER JOIN $db_output.cont_final c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID AND (c.RN1 > 1 or (r.OrgIDProv = 'DFC' and c.DFC_RN1 > 1))
INNER JOIN $db_output.ass_final a1 on r.UniqServReqID = a1.UniqServReqID and r.Person_ID = a1.Person_ID and a1.Der_AssOrderAsc = '1'
INNER JOIN $db_output.ass_final a2 on r.UniqServReqID = a2.UniqServReqID and r.Person_ID = a2.Person_ID and a2.Der_AssOrderDesc = '1' and a2.Der_AssToolCompDate > a1.Der_AssToolCompDate and a1.CodedAssToolType = a2.CodedAssToolType
WHERE r.REF_LENGTH > 14
GROUP BY 
r.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,DENOMINATOR
CREATE OR REPLACE TEMPORARY VIEW DENOMINATOR AS
SELECT
  'England' as OrgID,
  COUNT(DISTINCT r.UNIQSERVREQID) as COUNT
FROM
  $db_output.closed_refs r
  INNER JOIN $db_output.cont_final c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID
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
  $db_output.closed_refs r
  INNER JOIN $db_output.cont_final c on r.UniqServReqID = c.UniqServReqID and r.Person_ID = c.Person_ID
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

INSERT INTO $db_output.cyp_outcomes_output
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
H.UniqMonthID = '$month_id'

-- COMMAND ----------

-- DBTITLE 1,Load into unformatted table
INSERT INTO $db_output.cyp_peri_monthly_unformatted

SELECT
'$rp_startdate' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
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
'MHS92' AS METRIC,
COALESCE(MHS92, 0) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB 
FROM $db_output.cyp_outcomes_output a
LEFT JOIN $db_output.rd_org_daily_latest b ON A.ORGID = B.ORG_CODE

UNION ALL

SELECT
'$rp_startdate' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
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
'MHS93' AS METRIC,
COALESCE(MHS93, 0) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB 
FROM $db_output.cyp_outcomes_output a
LEFT JOIN $db_output.rd_org_daily_latest b ON A.ORGID = B.ORG_CODE

UNION ALL

SELECT
'$rp_startdate' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
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
'MHS94' AS METRIC,
COALESCE(MHS94, 0) AS METRIC_VALUE,
'$db_source' AS SOURCE_DB 
FROM $db_output.cyp_outcomes_output a
LEFT JOIN $db_output.rd_org_daily_latest b ON A.ORGID = B.ORG_CODE
