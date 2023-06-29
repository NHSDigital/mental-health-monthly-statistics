# Databricks notebook source
# DBTITLE 1,Needed to get only numeric PersScores
def is_numeric(s):
    try:
        float(s)
        return 1
    except ValueError:
        return 0
spark.udf.register("is_numeric", is_numeric)

# COMMAND ----------

# DBTITLE 1,CYP Outcomes - Closed Referrals
 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Referral;
 DROP TABLE IF EXISTS $db_output.cyp_closed_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_closed_referrals as
 select 
 distinct ---selecting distinct as same referral could have been referred to different servteamtype/careprofteamid in month
      r.Person_ID
 	,r.UniqServReqID
 	,r.RecordNumber
 	,r.Der_FY
 	,r.UniqMonthID
 	,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
 	,r.OrgIDProv AS Provider_Code 
 	,COALESCE(o.NAME,'UNKNOWN') AS Provider_Name
     ,COALESCE(c.STP_CODE,'UNKNOWN') AS ICB_Code
     ,COALESCE(c.STP_NAME,'UNKNOWN') AS ICB_Name
     ,COALESCE(c.REGION_CODE,'UNKNOWN') AS Region_Code
     ,COALESCE(c.REGION_NAME,'UNKNOWN') AS Region_Name
 	,r.AgeServReferRecDate
 	,CASE WHEN r.Gender = '1' THEN 'M' WHEN r.Gender = '2' THEN 'F' ELSE NULL END as Gender
 	,r.ReferralRequestReceivedDate
 	,r.ServDischDate
 -- 	,r.ReferClosReason ---removing ReferClosReason could have been referred to same team type but different careprofteamid (and clsoed ref reason) in month
 	,datediff(r.ServDischDate, r.ReferralRequestReceivedDate)+1 AS RefLength
 	,CASE WHEN datediff(r.ServDischDate, r.ReferralRequestReceivedDate)+1 >= 7 THEN 1 ELSE 0 END as AtLeast7days
     ,r.ReferRejectionDate
     ,i.Der_HospSpellCount
     ,r.LADistrictAuth
 
 FROM $db_output.NHSE_Pre_Proc_Referral r 
 
 LEFT JOIN 
 	(SELECT i.Person_ID, i.UniqServReqID, COUNT(i.Der_HospSpellRecordOrder) AS Der_HospSpellCount
 	FROM $db_output.Der_NHSE_Pre_Proc_Inpatients i 
     ---using this table as it has Der_HospSpellRecordOrder deriavtion (ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID DESC))
 	GROUP BY i.Person_ID, i.UniqServReqID) i ON i.Person_ID = r.Person_ID AND i.UniqServReqID = r.UniqServReqID -- to indentify referrals to inpatient services
 
 LEFT JOIN $db_output.mhsds_org_daily o ON r.OrgIDProv = o.ORG_CODE
 LEFT JOIN $db_output.mhsds_stp_mapping c on r.OrgIDCCGRes = c.CCG_CODE
 
 WHERE r.AgeServReferRecDate BETWEEN 0 AND 17 --- changed from 0-18 to match CQUIN 
 AND r.UniqMonthID BETWEEN $start_month_id AND $end_month_id 
 AND r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND r.ReferRejectionDate IS NULL 
 AND (r.ServDischDate BETWEEN r.ReportingPeriodStartDate AND r.ReportingPeriodEndDate) ---discharged in that month 
 AND i.Der_HospSpellCount IS NULL --- to exclude referrals with an associated hospital spell 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = "") -- to limit to those people whose commissioner is an English organisation 
 AND (r.ServTeamTypeRefToMH NOT IN ('B02','E01','E02','E03','E04','A14') OR r.ServTeamTypeRefToMH IS NULL)--- exclude specialist teams *but include those where team type is null 

# COMMAND ----------

# DBTITLE 1,Get contacts for Closed CYP Referrals
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_closed_contacts;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_closed_contacts as
 SELECT 
 	r.Person_ID
 	,r.UniqServReqID
 	,r.RecordNumber 
 	,r.Provider_Code
 	,COUNT(*) AS TotalContacts 
 	,SUM(CASE WHEN a.AttendOrDNACode IN ('5','6') THEN 1 ELSE 0 END) as AttendedContacts 
 	,MAX(CASE WHEN a.Der_ContactOrder = 1 THEN a.Der_ContactDate ELSE NULL END) AS Contact1 
 	,MAX(CASE WHEN a.Der_ContactOrder = 2 THEN a.Der_ContactDate ELSE NULL END) AS Contact2 
 
 FROM $db_output.cyp_closed_referrals r 
 
 INNER JOIN $db_output.Der_NHSE_Pre_Proc_Activity a ON r.Person_ID = a.Person_ID AND a.UniqServReqID = r.UniqServReqID AND a.Der_ContactDate <= '$rp_enddate' AND a.Der_ContactDate <= r.ServDischDate
 
 GROUP BY r.Person_ID, r.UniqServReqID, r.RecordNumber, r.Provider_Code 

# COMMAND ----------

# DBTITLE 1,Get all assessments related to closed referrals table
 %sql
 REFRESH TABLE $db_output.NHSE_Pre_Proc_Assessments_Stage3;
 REFRESH TABLE $db_output.cyp_closed_referrals;
 DROP TABLE IF EXISTS $db_output.cyp_all_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_all_assessments
 select 
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Provider_Code
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.Der_PreferredTermSNOMED
 ,a.Der_AssessmentToolName
 ,a.Der_AssessmentCategory
 ,a.PersScore
 ,a.Der_ValidScore
 ,CASE 
 		WHEN a.Der_PreferredTermSNOMED IN 
 			('Childrens global assessment scale score'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 1 score - disruptive, antisocial or aggressive behaviour'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 10 score - peer relationships'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 11 score - self care and independence'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 12 score - family life and relationships'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 13 score - poor school attendance'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 2 score - overactivity, attention and concentration'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 3 score - non-accidental self injury'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 4 score - alcohol, substance/solvent misuse'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 5 score - scholastic or language skills'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 6 score - physical illness or disability problems'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 7 score - hallucinations and delusions'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 8 score - non-organic somatic symptoms'
 			,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 9 score - emotional and related symptoms'
 			) THEN 'Clinician' 
 		WHEN a.Der_PreferredTermSNOMED IN 
 			('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 10 score - peer relationships'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 11 score - self care and independence'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 12 score - family life and relationships'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 13 score - poor school attendance'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 2 score - overactivity, attention and concentration'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 3 score - non-accidental self injury'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 4 score - alcohol, substance/solvent misuse'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 5 score - scholastic or language skills'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 6 score - physical illness or disability problems'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 7 score - hallucinations and delusions'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 8 score - non-organic somatic symptoms'
 			,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 9 score - emotional and related symptoms'
 			,'Child Outcome Rating Scale total score'
 			,'Clinical Outcomes in Routine Evaluation - 10 clinical score'
 			,'Generalized anxiety disorder 7 item score'
 			,'Goal Progress Chart - Child/Young Person - goal 1 score'
 			,'Goal Progress Chart - Child/Young Person - goal 2 score'
 			,'Goal Progress Chart - Child/Young Person - goal 3 score'
 			,'Goal Progress Chart - Child/Young Person - goal score'
             ,'Goal-Based Outcomes tool goal progress chart - goal 1 score' 
             ,'Goal-Based Outcomes tool goal progress chart - goal 2 score'
             ,'Goal-Based Outcomes tool goal progress chart - goal 3 score' 
 			,'Outcome Rating Scale total score'
 			,'Patient health questionnaire 9 score'
 			,'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'
 			,'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'
 			,'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'
 			,'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'
 			,'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'
 			,'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'
 			,'SCORE Index of Family Function and Change - 15 - total score'
 			,'Short Warwick-Edinburgh Mental Well-being Scale score'
 			,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score'
 			,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score'
 			,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score'
 			,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score'
 			,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score'
 			,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score'
 			,'Warwick-Edinburgh Mental Well-being Scale score'
 			,"Young Person's Clinical Outcomes in Routine Evaluation clinical score") THEN 'Self' 
 		ELSE 'Parent' 
 		END as Rater 
 
 from $db_output.cyp_closed_referrals r
 INNER JOIN $db_output.NHSE_Pre_Proc_Assessments_Stage3 a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.Der_AssToolCompDate <= '$rp_enddate' AND a.Der_AssToolCompDate <= r.ServDischDate
 WHERE a.Der_AssessmentToolName IN 
 	('Childrens Global Assessment Scale (CGAS)'
 -- 	,'HoNOS-CA (Child and Adolescent)' NHS D Ref data is more specific
     ,"HoNOS-CA (Child and Adolescent) - Self rated"
     ,"HoNOS-CA (Child and Adolescent) - Parent rated"
     ,"HoNOS-CA (Child and Adolescent) - Clinician rated"
     --- only including HONOSCA, not adult HONOS. Many need to include LD HONOS if that's is relevant to CYP 
 	,'Child Outcome Rating Scale (CORS)'
 	,'Clinical Outcomes in Routine Evaluation 10 (CORE 10)'
 	--,'CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure) ' -- check psychometric properties / if it's being used with CYP 
 	,'Genralised Anxiety Disorder 7 (GAD-7)'
 	,'Goal Based Outcomes (GBO)'
 	,'Outcome Rating Scale (ORS)'
 	,'Patient Health Questionnaire (PHQ-9)'
 	,'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)'
 	,'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)'
 	,'YP-CORE'
 	)
 OR a.Der_PreferredTermSNOMED IN 
 	('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score'
 	,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score'
 	,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score'
 	,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score'
 	,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score'
 	,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score'
 	,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score'
 	,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score'
 	,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score'
 	,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score'
 	,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score'
 	,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'
 	,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'
 	,'SCORE Index of Family Function and Change - 15 - total score'
 	)
 
 AND is_numeric(a.PersScore) = 1 
 -- AND r.RN = 1 ---Removed from TB code 10/05/22

# COMMAND ----------

# DBTITLE 1,Bring referrals, contacts and outcomes together
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_ref_cont_out;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_ref_cont_out
 SELECT 
 	r.Person_ID
 	,r.UniqServReqID
 	,r.RecordNumber
 	,r.Der_FY
 	,r.UniqMonthID
 	,r.ReportingPeriodStartDate
 	,r.Provider_Code 
 	,r.Provider_Name
     ,r.ICB_Code
     ,r.ICB_Name
     ,r.Region_Code
     ,r.Region_Name
 	,r.AgeServReferRecDate AS Age
 	,r.Gender
 	,r.ReferralRequestReceivedDate
 	,r.ServDischDate
 	,r.RefLength
 	,r.AtLeast7days
 	,c.TotalContacts
 	,c.AttendedContacts
 	,c.Contact1
 	,c.Contact2
 	,a.Der_AssUniqID
 	,a.Der_AssTable
 	,a.Der_AssToolCompDate
 	,a.CodedAssToolType
 	,a.Der_PreferredTermSNOMED AS AssName
 	,a.Der_AssessmentToolName AS AssType 
 	,a.Rater
 	,a.Der_AssessmentCategory
 	,CAST(a.PersScore AS float) AS RawScore
 	,a.Der_ValidScore
 
 FROM $db_output.cyp_closed_referrals r 
 
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.Person_ID = c.Person_ID AND r.UniqServReqID = c.UniqServReqID AND r.RecordNumber = c.RecordNumber
 
 LEFT JOIN $db_output.cyp_all_assessments a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber 
 
 WHERE PersScore <> 'N' AND PersScore <> 'OK' 

# COMMAND ----------

# DBTITLE 1,Transform RCADS scores
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_valid_unique_assessments_rcads;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_valid_unique_assessments_rcads
 SELECT *,
 	CASE 
 	WHEN Gender	= 'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN ((RawScore-8.25)*10)/4.09+50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score' THEN ((RawScore-6.98)*10)/3.36+50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN ((RawScore-6.15)*10)/3.2+50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score' THEN ((RawScore-5.25)*10)/4.15+50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score' THEN ((RawScore-4.87)*10)/3.93+50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score' THEN ((RawScore-9.77)*10)/4.51+50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score' THEN ((RawScore-8.74)*10)/4.75+50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score' THEN ((RawScore - 7.77)*10)/3.77+50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN((RawScore-7.62)*10)/3.68+50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score' THEN	((RawScore-6.51)*10)/4.73+50
 	WHEN Gender	=	'F'	AND	Age IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score' THEN ((RawScore-7.05)*10)/4.31+50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score' THEN ((RawScore-11.61)*10)/4.98+50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score' THEN ((RawScore-7.07)*10)/3.64+50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore - 6.44) *10)/ 3.13 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore - 6.01) *10)/ 3.26 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore - 4.06) *10)/ 3.6+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore - 3.2) *10)/ 3.05+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore - 10.30 )*10)/ 4.75+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore - 7.64)*10)/  4.1+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore - 8.01)*10)/ 3.68 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore - 6.39)*10)/ 3.46+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore - 5.25)*10)/ 4.3 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore - 4.74)*10)/ 3.78+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore - 12.92)*10)/ 5.21 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore - 6.71) *10)/ 3.64+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore - 6.2) *10)/  3.14+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore - 5.22) *10)/  3.40 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore - 3.62) *10)/  3.36 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore - 2.26) *10)/  2.47+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore - 11.05)*10)/  4.74  + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore - 7.89)*10)/ 3.91 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore - 7.42)*10)/  3.16+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore - 5.12)*10)/  3.34 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore - 5.03)*10)/  3.92 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore - 3.00)*10)/  2.72+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore-13.01 )*10)/  4.94 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore	- 7.44) *10)/ 4.1+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore	- 7.07) *10)/  2.93+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore	- 4.65) *10)/  2.89+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore	- 3.76) *10)/  3.21+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore	- 2.5) *10)/  2.46+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore	- 11.68)*10)/  4.74+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore	- 7.65)*10)/ 3.68+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore	- 7.28)*10)/  3.44+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore	- 4.12  )*10)/  2.79 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore	-  4.18)*10)/  3.07+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore	- 2.34)*10)/  2.23+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore	- 12.27)*10)/ 5.00   + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore	- 7.32) *10)/ 3.81+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore	- 6.76) *10)/  3.44+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore	- 5.18) *10)/ 3.12+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore	- 3.79) *10)/  2.71+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore	- 1.9) *10)/ 2.03+ 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore	- 10.67)*10)/ 4.49+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'	THEN	((RawScore	- 9.36)*10)/ 4.45+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'	THEN	((RawScore	- 8.49)*10)/ 3.71+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'	THEN	((RawScore	- 5.48)*10)/ 3.82+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'	THEN	((RawScore	- 5.26)*10)/ 4.28+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'	THEN	((RawScore	- 3.05)*10)/ 2.57+ 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'	THEN	((RawScore	- 12.85)*10)/ 4.98+ 50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.71)*10)/2.93 + 50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 4.11)*10)/3.00 + 50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 2.04)*10)/2.43 + 50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.91)*10)/1.90 + 50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 4.29)*10)/3.00 + 50
 	WHEN Gender	=	'M'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.44)*10)/3.88 + 50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression RawScore'	THEN	((RawScore	- 3.25)*10)/3.58 + 50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 4.00)*10)/2.87 + 50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 2.01)*10)/2.63 + 50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.87)*10)/2.61 + 50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 4.20)*10)/3.00 + 50
 	WHEN Gender	=	'F'	AND	Age	IN ('8','9')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.01)*10)/3.87 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.62)*10)/2.87 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.74)*10)/2.49 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 2.01)*10)/2.31 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.64)*10)/1.84 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 2.85)*10)/2.79 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 7.71)*10)/3.94 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.75)*10)/3.63 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 4.18)*10)/3.18 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 2.03)*10)/2.65 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.79)*10)/2.30 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 3.46)*10)/2.95 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('10','11')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.94)*10)/5.16 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.54)*10)/3.18 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.26)*10)/2.60 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 1.62)*10)/1.98 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.61)*10)/1.56 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 1.97)*10)/2.21 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 7.59)*10)/4.31 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.6)*10)/3.37 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.23)*10)/2.54 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 1.41)*10)/1.94 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.82)*10)/1.98 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 2.08)*10)/2.33 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('12','13')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.62)*10)/4.65 + 50.
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 5.21)*10)/3.51 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.73)*10)/2.75 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 2.58)*10)/3.03 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 2.19)*10)/2.34 + 50
 	WHEN Gender	=	'M'	AND Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 1.69)*10)/1.89 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.39)*10)/4.19 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.97)*10)/3.25 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.46)*10)/3.02 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 1.89)*10)/2.57 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.83)*10)/2.13 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 1.91)*10)/2.49 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('14','15')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.83)*10)/4.73 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 3.94)*10)/3.88 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.22)*10)/2.50 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 1.11)*10)/1.96 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 1.50)*10)/1.69 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 1.15)*10)/1.55 + 50
 	WHEN Gender	=	'M'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 7.32)*10)/3.69 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'	THEN	((RawScore	- 4.91)*10)/3.17 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'	THEN	((RawScore	- 3.76)*10)/2.28 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'	THEN	((RawScore	- 1.8)*10)/2.34 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'	THEN	((RawScore	- 2.04)*10)/2.27 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'	THEN	((RawScore	- 1.92)*10)/1.98 + 50
 	WHEN Gender	=	'F'	AND	Age	IN	('16','17')	AND	AssName	=	'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'	THEN	((RawScore	- 8.35)*10)/4.38 + 50
 	ELSE RawScore
 	END as TScore
 	, CASE 
 		WHEN Age = 9 THEN 4
 		WHEN Age = 10 THEN 5
 		WHEN Age = 11 THEN 6
 		WHEN Age = 12 THEN 7
 		WHEN Age = 13 THEN 8
 		WHEN Age = 14 THEN 9
 		WHEN Age = 15 THEN 10
 		WHEN Age = 16 THEN 11
 		WHEN Age = 17 THEN 12
 		ELSE NULL
 	END as US_school_grade
 
 FROM $db_output.cyp_ref_cont_out a
 WHERE a.Der_ValidScore = 'Y' 

# COMMAND ----------

# DBTITLE 1,Partition Assessments to identify where rows refer to the same event
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_partition_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_partition_assessments
 SELECT *,
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, AssName, RecordNumber ORDER BY Der_AssToolCompDate ASC, Der_AssUniqID ASC) AS RN
 
 FROM $db_output.cyp_valid_unique_assessments_rcads
 
 WHERE Contact1 <= Der_AssToolCompDate 

# COMMAND ----------

# DBTITLE 1,Get all LAST Assessments (RN = 1 is first Assessment). Paired OM has to be on or after date of second contact
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_last_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_last_assessments
 SELECT 
 	a.Person_ID,
 	a.UniqServReqID,
 	a.AssName,
 	MAX(a.RN) AS max_ass
 
 FROM $db_output.cyp_partition_assessments a
 
 WHERE a.RN > 1 and a.Der_AssToolCompDate >=a.Contact2 
 
 GROUP BY a.Person_ID, a.UniqServReqID, a.AssName

# COMMAND ----------

# DBTITLE 1,Match First Assessments with Last Assessments
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_first_and_last_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_first_and_last_assessments
 SELECT  
 	
 		r.Person_ID
 		,r.UniqServReqID
 		,r.RecordNumber
 		,r.Der_FY
 		,r.UniqMonthID
 		,r.ReportingPeriodStartDate
 		,r.Provider_Code 
 		,r.Provider_Name
         ,r.ICB_Code
         ,r.ICB_Name
         ,r.Region_Code
         ,r.Region_Name
 		,r.AgeServReferRecDate AS Age
 		,r.Gender
 		,r.ReferralRequestReceivedDate
 		,r.ServDischDate
 		,r.RefLength
 		,r.AtLeast7days
 		,c.TotalContacts
 		,c.AttendedContacts
 		,c.Contact1
 		,c.Contact2
 		,a.Der_AssessmentCategory
 		,a.Rater
 		,a.AssType
 		,a.AssName
 		,a.Der_AssUniqID AS Der_AssUniqID_first
 		,a.Der_AssToolCompDate AS AssDate_first
 		,a.TScore AS Score_first
 		,b.Der_AssUniqID AS Der_AssUniqID_last
 		,b.Der_AssToolCompDate AS AssDate_last
 		,b.TScore AS Score_last
 		,b.TScore - a.TScore AS Score_Change
 
 FROM $db_output.cyp_closed_referrals r
 
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID
 
 LEFT JOIN $db_output.cyp_partition_assessments a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.RN = 1 
 
 LEFT JOIN 
 	(SELECT x.* 
     FROM $db_output.cyp_partition_assessments x 
     INNER JOIN $db_output.cyp_last_assessments y 
     ON x.Person_ID = y.Person_ID 
     AND x.UniqServReqID = y.UniqServReqID 
     AND x.Person_ID = y.Person_ID 
     AND x.AssName = y.AssName 
     AND x.RN = y.max_ass) AS b
 ON a.Person_ID = b.Person_ID AND a.UniqServReqID = b.UniqServReqID AND a.AssName = b.AssName

# COMMAND ----------

# DBTITLE 1,Bring in thresholds for reliable change (NEW METHOD)
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_rc_thresholds;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rc_thresholds
 SELECT 
 	* 
 	,CASE
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score' THEN 22.87 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score' THEN 18.3 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score' THEN 24.06 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score' THEN 40.93 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score' THEN 28  ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score' THEN 16.63  ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score' THEN ROUND(4.33,0) ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score' THEN ROUND(4.39,0)  ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score' THEN ROUND(3.82,0) ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score' THEN ROUND(3.17,0) ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score' THEN ROUND(3.09,0) --- derived from SDQ norms / Goodman (2001) paper
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score' THEN ROUND(2.62, 0) --- derived from SDQ norms / Goodman (2001) paper
 		WHEN AssName = 'Child Outcome Rating Scale total score' THEN 10  ---- CYP IAPT 2015
 		WHEN AssName = 'Clinical Outcomes in Routine Evaluation - 10 clinical score' THEN ROUND(5.39,0) ---- CYP IAPT 2015
 		WHEN AssName = 'Generalized anxiety disorder 7 item score' THEN ROUND(4.22,0)  ---- CYP IAPT 2015
 		WHEN AssName = 'Outcome Rating Scale total score' THEN ROUND(6.55,1)  ---- CYP IAPT 2015
 		WHEN AssName = 'Patient health questionnaire 9 score' THEN ROUND(5.99,0)  ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score' THEN 17.73 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score' THEN 14.91 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score' THEN 16.35 ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score' THEN 18.29  ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score' THEN 22.95  ---- CYP IAPT 2015
 		WHEN AssName = 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score' THEN 13.99  ---- CYP IAPT 2015
 		WHEN AssName = 'SCORE Index of Family Function and Change - 15 - total score' THEN ROUND(10.2,0) -- from Stratton et al. (2014)
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score' THEN ROUND(3.74,0)  ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score' THEN ROUND(4.26,0)  ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score' THEN ROUND(3.87,0)  ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score' THEN ROUND(3.24, 0)  ---- CYP IAPT 2015
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score' THEN ROUND(4.76,0) --- derived from SDQ norms / Goodman (2001) paper 
 		WHEN AssName = 'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score' THEN ROUND(4.3,0) --- derived from SDQ norms / Goodman (2001) paper 
 		WHEN AssName = "Young Person's Clinical Outcomes in Routine Evaluation clinical score" THEN ROUND(8.33,0)  ---- CYP IAPT 2015
 		WHEN AssType = 'Goal Based Outcomes (GBO)' THEN 3 
 		WHEN AssType IN ("HoNOS-CA (Child and Adolescent) - Self rated", "HoNOS-CA (Child and Adolescent) - Parent rated", "HoNOS-CA (Child and Adolescent) - Clinician rated") THEN 2
 		WHEN AssType = 'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)' THEN ROUND(2.87,0) --- from Shah et al. (2018)
 		WHEN AssType = 'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)' THEN ROUND(8.55,0) --- from Maheswaran et al. (2012) 
 		WHEN AssType = 'Childrens Global Assessment Scale (CGAS)' THEN ROUND(10.72,0) --- from Bird et al. (1987)
 		ELSE NULL 
 		END as MinChange
 
 FROM $db_output.cyp_first_and_last_assessments

# COMMAND ----------

# DBTITLE 1,Convert Scores to Reliable Change Index (RCI)
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_rci;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rci
 SELECT 
 * 
 ,CASE WHEN AssType 
           NOT IN ('Child Outcome Rating Scale (CORS)', 
           'Outcome Rating Scale (ORS)', 
           'Childrens Global Assessment Scale (CGAS)',
           'Goal Based Outcomes (GBO)',
           'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)',
           'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)',
           'SCORE-15 Index of Family Functioning and Change') 
 			   AND ABS(score_change) >= MinChange 
 			   AND score_change < 0 
 			   THEN 1
 		  WHEN AssType 
           IN ('Child Outcome Rating Scale (CORS)', 
           'Outcome Rating Scale (ORS)', 
           'Childrens Global Assessment Scale (CGAS)',
           'Goal Based Outcomes (GBO)',
           'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)',
           'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)',
           'SCORE-15 Index of Family Functioning and Change')
 			   AND ABS(score_change) >= MinChange 
 			   AND score_change > 0 
 			   THEN 1
 			   ELSE NULL END as Reliable_improvement
                
 ,CASE WHEN AssType 
           NOT IN ('Child Outcome Rating Scale (CORS)', 
           'Outcome Rating Scale (ORS)', 
           'Childrens Global Assessment Scale (CGAS)',
           'Goal Based Outcomes (GBO)',
           'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)',
           'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)',
           'SCORE-15 Index of Family Functioning and Change') 
 			   AND ABS(score_change) >= MinChange 
 			   AND score_change > 0 
 			   THEN 1 
 		  WHEN AssType 
           IN ('Child Outcome Rating Scale (CORS)', 
           'Outcome Rating Scale (ORS)', 
           'Childrens Global Assessment Scale (CGAS)',
           'Goal Based Outcomes (GBO)',
           'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)',
           'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)',
           'SCORE-15 Index of Family Functioning and Change') 
 			   AND ABS(score_change) >= MinChange 
 			   AND score_change < 0 
 			   THEN 1
 			   ELSE NULL END as Reliable_deterioration
 	,CASE WHEN ABS(score_change) < MinChange
 			   THEN 1 
 			   ELSE NULL END as No_change
 
 FROM $db_output.cyp_rc_thresholds

# COMMAND ----------

# DBTITLE 1,Aggregate reliable change across each referral
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_rci_referral;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rci_referral
 SELECT
 	Person_ID,
 	UniqServReqID,
 	Rater,
 	MAX(Contact1) AS Contact1,
 	MAX(Contact2) AS Contact2,
 	MAX(CASE WHEN Contact2 IS NOT NULL AND Der_AssUniqID_first IS NOT NULL THEN 1 ELSE NULL END) as Assessment,
 	MAX(CASE WHEN Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN 1 ELSE NULL END) as Paired,
 	MAX(CASE WHEN Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN Reliable_improvement ELSE NULL END) as Improvement,
 	MAX(CASE WHEN Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN No_change ELSE NULL END) as NoChange,
 	MAX(CASE WHEN Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN Reliable_deterioration ELSE NULL END) as Deter
 
 FROM $db_output.cyp_rci
 
 WHERE AssName IS NOT NULL 
 
 GROUP BY Person_ID, UniqServReqID, Rater

# COMMAND ----------

# DBTITLE 1,Calculate overall meaningful change across multiple measures
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_meaningful_change;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_meaningful_change
 SELECT 
 	Person_ID
 	,UniqServReqID
 	,Rater
 	,Contact1
 	,Contact2
 	,Assessment
 	,Paired 
 	,CASE WHEN Improvement = 1 AND Deter IS NULL THEN 1 ELSE 0 END as Improvement 
 	,CASE WHEN NoChange = 1 AND Improvement IS NULL AND Deter IS NULL THEN 1 ELSE 0 END as NoChange 
 	,CASE WHEN Deter = 1 THEN 1 ELSE 0 END as Deter 
 
 FROM $db_output.cyp_rci_referral

# COMMAND ----------

# DBTITLE 1,Join back on to Referrals (split by perspective)
 %sql
 TRUNCATE TABLE $db_output.cyp_master;
 INSERT INTO $db_output.cyp_master
 SELECT 
 	r.Person_ID
 	,r.UniqServReqID
 	,r.RecordNumber
 	,r.Der_FY
 	,r.UniqMonthID
 	,r.ReportingPeriodStartDate
 	,r.Provider_Code as OrgIDProv
 	,r.Provider_Name
     ,r.ICB_Code as STP_Code
     ,r.ICB_Name as STP_Name
     ,r.Region_Code
     ,r.Region_Name
 	,r.AgeServReferRecDate AS Age
 	,r.Gender
 	,r.ReferralRequestReceivedDate
 	,r.ServDischDate
 	,r.RefLength
 	,r.AtLeast7days 
 	,c.TotalContacts
 	,c.AttendedContacts
 	,c.Contact1
 	,c.Contact2
 	-- self-rated measures 
 	,m1.Assessment AS Assessment_SR
 	,m1.Paired AS Paired_SR
 	,m1.Improvement AS Improvement_SR
 	,m1.NoChange AS NoChange_SR
 	,m1.Deter AS Deter_SR
 	-- parent-rated measures 
 	,m2.Assessment AS Assessment_PR
 	,m2.Paired AS Paired_PR
 	,m2.Improvement AS Improvement_PR
 	,m2.NoChange AS NoChange_PR
 	,m2.Deter AS Deter_PR
 	-- clinician-rated measures
 	,m3.Assessment AS Assessment_CR
 	,m3.Paired AS Paired_CR
 	,m3.Improvement AS Improvement_CR
 	,m3.NoChange AS NoChange_CR
 	,m3.Deter AS Deter_CR
 	,CASE WHEN m1.Assessment = 1 OR m2.Assessment = 1 OR m3.Assessment = 1 THEN 1 ELSE 0 END as Assessment_ANY
 	,CASE WHEN m1.Paired = 1 OR m2.Paired = 1 OR m3.Paired = 1 THEN 1 ELSE 0 END as Paired_ANY
 
 FROM $db_output.cyp_closed_referrals r  
 
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID AND r.RecordNumber = c.RecordNumber 
 
 LEFT JOIN $db_output.cyp_meaningful_change m1 ON r.UniqServReqID = m1.UniqServReqID AND r.Person_ID = m1.Person_ID AND m1.Rater = 'Self' 
 LEFT JOIN $db_output.cyp_meaningful_change m2 ON r.UniqServReqID = m2.UniqServReqID AND r.Person_ID = m2.Person_ID AND m2.Rater = 'Parent' 
 LEFT JOIN $db_output.cyp_meaningful_change m3 ON r.UniqServReqID = m3.UniqServReqID AND r.Person_ID = m3.Person_ID AND m3.Rater = 'Clinician' 
 
 WHERE ReportingPeriodStartDate = "$rp_startdate_1m"