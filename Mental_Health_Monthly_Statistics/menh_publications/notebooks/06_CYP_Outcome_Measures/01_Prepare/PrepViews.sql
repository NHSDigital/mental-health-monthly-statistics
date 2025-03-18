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

 Financial_Yr_Start = dbutils.widgets.get("Financial_Yr_Start")
 print(Financial_Yr_Start)
 assert Financial_Yr_Start


-- COMMAND ----------

-- DBTITLE 1,STEP1 Get all closed referrals
CREATE OR REPLACE TEMP VIEW INPATIENTS AS 
SELECT 
PERSON_ID,
UNIQSERVREQID,
UNIQHOSPPROVSPELLID
FROM
$db_source.MHS501HOSPPROVSPELL
WHERE
RECORDSTARTDATE < '$rp_enddate' AND (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')

-- COMMAND ----------

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
and A.UniqMonthID = '$month_id'
and A.ServDischDate is not null
and B.UniqHospProvSpellID is null
AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = '')

-- COMMAND ----------

-- DBTITLE 1,Step 2.  Direct and Indirect contacts (Activity)
 %py

 # adapted to use v4.1 methodology for v4.1 months (pre month_id 1459) and v5 methodology for later months

 if int(month_id) < 1459:
   # v4.1
   print("month_id is pre-v5, executing EXCLUDE statement")
   sql=("CREATE OR REPLACE TEMPORARY VIEW Cont AS \
   SELECT 'DIRECT' AS CONT_TYPE, c.UniqMonthID, c.Person_ID, c.UniqServReqID, c.AgeCareContDate, c.UniqCareContID AS ContID, c.CareContDate AS ContDate, c.MHS201UniqID as UniqID \
   FROM {db_source}.MHS201CareContact c \
   WHERE ((c.AttendStatus IN ('5', '6') and c.ConsMechanismMH NOT IN ('05', '06')) or (c.ConsMechanismMH IN ('05', '06') and OrgIdProv = 'DFC'))  \
   and CareContDate <= '{rp_enddate}' \
   and carecontdate >= '{Financial_Yr_Start}' \
   UNION ALL \
   SELECT 'INDIRECT' AS CONT_TYPE, i.UniqMonthID, i.Person_ID, i.UniqServReqID, NULL AS AgeCareContDate, CAST(i.MHS204UniqID AS string) AS ContID, i.IndirectActDate AS ContDate, i.MHS204UniqID as UniqID \
   FROM {db_source}.MHS204IndirectActivity i \
   WHERE IndirectActDate <= '{rp_enddate}' \
   and indirectactdate >= '{Financial_Yr_Start}'".format(db_source=db_source, rp_enddate=rp_enddate, Financial_Yr_Start=Financial_Yr_Start))
        
        
 else:
   # v5
   print("month_id is post-v4.1, executing INCLUDE statement")
   sql=("CREATE OR REPLACE TEMPORARY VIEW Cont AS \
    SELECT 'DIRECT' AS CONT_TYPE, c.UniqMonthID, c.Person_ID, c.UniqServReqID, c.AgeCareContDate, c.UniqCareContID AS ContID, c.CareContDate AS ContDate, c.MHS201UniqID as UniqID \
    FROM {db_source}.MHS201CareContact c \
    LEFT JOIN {db_output}.validcodes as vc \
    ON vc.tablename = 'mhs201carecontact' and vc.field = 'ConsMechanismMH' and vc.Measure = 'CYP' and vc.type = 'include' and c.ConsMechanismMH = vc.ValidValue \
    and c.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or c.UniqMonthID <= vc.LastMonth) \
    and OrgIdProv != 'DFC' \
    LEFT JOIN {db_output}.validcodes as vck \
    ON vck.tablename = 'mhs201carecontact' and vck.field = 'ConsMechanismMH' and vck.Measure = 'CYP_KOOTH' and vck.type = 'include' and c.ConsMechanismMH = vck.ValidValue \
    and c.UniqMonthID >= vck.FirstMonth and (vck.LastMonth is null or c.UniqMonthID <= vck.LastMonth) \
    and OrgIdProv = 'DFC' \
    WHERE c.AttendStatus IN ('5','6') \
    AND NOT(vc.Field is null AND vck.Field is null) \
    and CareContDate <= '{rp_enddate}' \
    and carecontdate >= '{Financial_Yr_Start}' \
    UNION ALL \
    SELECT 'INDIRECT' AS CONT_TYPE, i.UniqMonthID, i.Person_ID, i.UniqServReqID, NULL AS AgeCareContDate, CAST(i.MHS204UniqID AS string) AS ContID, i.IndirectActDate AS ContDate, i.MHS204UniqID as UniqID \
    FROM {db_source}.MHS204IndirectActivity i \
    WHERE IndirectActDate <= '{rp_enddate}' \
    and indirectactdate >= '{Financial_Yr_Start}'".format(db_source=db_source, db_output=db_output, month_id=month_id, rp_enddate=rp_enddate, Financial_Yr_Start=Financial_Yr_Start))
   
 spark.sql(sql)

-- COMMAND ----------

-- DBTITLE 1,Step 3 Link to Activity 
INSERT INTO $db_output.CONT_FINAL 
SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, CONT_TYPE ORDER BY ContDate ASC, UniqID ASC) AS RN1, -- Added contact type to the partition to ensure that only instances of 2 contacts or 2 indirect act are counted.
ROW_NUMBER () OVER(PARTITION BY UniqServReqID, CONT_TYPE ORDER BY ContDate ASC, UniqID ASC) AS DFC_RN1
FROM
Cont

-- COMMAND ----------

-- DBTITLE 1,Join all assessments together (Referrals, Clusters, Contacts)
INSERT INTO $db_output.ASSESSMENTS 
SELECT
  'CON' as TYPE,
  a.UniqMonthID,
  a.CodedAssToolType,
  TRY_CAST(a.PersScore as int) AS PersScore,
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
  A.UNIQMONTHID <= '$month_id'

-- COMMAND ----------

INSERT INTO $db_output.ASSESSMENTS 

SELECT
  'REF' AS Type,
  r.UniqMonthID,
  r.CodedAssToolType,
  TRY_CAST(r.PersScore as int) AS PersScore,
  r.AssToolCompTimestamp AS Der_AssToolCompDate,
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
  r.UNIQMONTHID <= '$month_id'

-- COMMAND ----------

INSERT INTO $db_output.ASSESSMENTS 

SELECT
  'CLU' AS Der_AssTable,
  c.UniqMonthID,
  a.CodedAssToolType,
  TRY_CAST(a.PersScore as int) AS PersScore,
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
  a.UniqMonthID <= '$month_id'
  and a.UniqMonthID <= '1488'                                          ---V6_Changes

-- COMMAND ----------

-- DBTITLE 1,MH_Assessment Reference Data Join to Assesments 
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

FROM $db_output.assessments a

LEFT JOIN $db_output.MH_Assessment_Reference_Data r ON a.CodedAssToolType = r.Active_Concept_ID_SNOMED


-- COMMAND ----------

-- DBTITLE 1,derivations on Both Assesments 
INSERT INTO $db_output.ASS_FINAL
SELECT 
a.*,
ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.UniqServReqID, a.CodedAssToolType ORDER BY a.Der_AssToolCompDate ASC) AS Der_AssOrderAsc, --First assessment
ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.UniqServReqID, a.CodedAssToolType ORDER BY a.Der_AssToolCompDate DESC) AS Der_AssOrderDesc 
FROM 
ASS_STG a
INNER JOIN $db_output.CLOSED_REFS b on a.UniqServReqID = b.UniqServReqID and a.Person_ID = b.Person_ID
WHERE
DER_VALIDSCORE = 'Y'
AND DER_UNIQASSESSMENT = 'Y'

-- COMMAND ----------

-- DBTITLE 1,MASTER TABLE  ( join Contacts and assessments to referrals)
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