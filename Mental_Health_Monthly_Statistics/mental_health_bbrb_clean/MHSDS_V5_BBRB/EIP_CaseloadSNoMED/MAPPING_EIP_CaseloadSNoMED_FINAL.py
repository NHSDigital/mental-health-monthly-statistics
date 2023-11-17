# Databricks notebook source
startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $db_source.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $db_source.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $db_source.mhs000header order by Uniqmonthid").collect()]

dbutils.widgets.dropdown("rp_startdate", "2021-03-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
dbutils.widgets.dropdown("month_id", "1452", monthid)
dbutils.widgets.text("status","Performance")
dbutils.widgets.text("db_output","$user_id")
dbutils.widgets.text("db_source","$reference_data")
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
params = {'rp_startdate': str(rp_startdate), 'rp_enddate': str(rp_enddate), 'month_id': month_id, 'db_output': db_output, 'db_source': db_source}

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Create SNoMED Reference data
# dbutils.notebook.run('Create_SNoMED_FSN_Ref', 0, params)

# COMMAND ----------

 %sql
 ---Clare B SNoMED Ref data
 ---select * from $db_output.SCT_Concepts_FSN limit 3

# COMMAND ----------

 %sql
 --testing different SNoMED ref data
 ---******can use the SCT_Concepts_FSN table now. It includes all codes which will have been active at the time of submission******
 DROP TABLE IF EXISTS $db_output.EIP_SNOMED_REF;
 CREATE TABLE $db_output.EIP_SNOMED_REF AS 
 select ID, EFFECTIVE_TIME, ACTIVE, MODULE_ID, CONCEPT_ID, LANGUAGE_CODE, TYPE_ID, TERM, CASE_SIGNIFICANCE_ID,
 ROW_NUMBER() OVER (PARTITION BY CONCEPT_ID ORDER BY EFFECTIVE_TIME desc) AS IS_Latest
 from $reference_data.snomed_ct_rf2_descriptions
 where  
 TYPE_ID = '900000000000003001' ----Only want fully specified name of a SNoMED-CT code
 and ACTIVE = 1 --Ensures SNoMED-CT code is active

# COMMAND ----------

# DBTITLE 1,Org daily Prep
 %sql
 DROP TABLE IF EXISTS $db_output.EIP_ORG_DAILY;
 CREATE TABLE $db_output.EIP_ORG_DAILY USING DELTA AS 
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('2021-03-31', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('2021-03-31', 1)	
                 AND (ORG_CLOSE_DATE >= '2021-03-31' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '2021-03-31' 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_ORG_RELATIONSHIP_DAILY;
 CREATE TABLE $db_output.EIP_ORG_RELATIONSHIP_DAILY USING DELTA AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $reference_data.org_relationship_daily
 WHERE
 (REL_CLOSE_DATE >= '2021-03-31' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '2021-03-31'

# COMMAND ----------

# DBTITLE 1,Gets Region & STP reference data
 %sql
 DROP TABLE IF EXISTS $db_output.EIP_STP_MAPPING;
 CREATE TABLE $db_output.EIP_STP_MAPPING AS 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.EIP_ORG_DAILY A
 LEFT JOIN $db_output.EIP_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.EIP_ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.EIP_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.EIP_ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

# DBTITLE 1,Gets CCG reference data
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_LATEST AS
 SELECT DISTINCT ORG_CODE,
                 NAME
            FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('2021-03-31', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('2021-03-31', 1)	
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '2021-03-31' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '2021-03-31'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

# DBTITLE 1,Referral data
 %sql
 
 
 --This cell gets all referrals where servive team type referred to was EIP (A14). It joins on record number to get the demographics at the time rather latest information used elsewhere in the monthly publication.
 --I would like to narrow this down to the latest referrals only rather than all, but not been able to do so due to the methodology used. It looks at EIP referrals, and takes ANY contact/intervention information for that --person and provider during that period, no matter if it's not the same referral
 
 
 DROP TABLE IF EXISTS $db_output.EIP_Refs_v2;
 CREATE TABLE         $db_output.EIP_Refs_v2 AS
 
 SELECT	            r.MHS101UniqID,
                     r.UniqMonthID,
                     r.OrgIDProv,
                     r.Person_ID,
                     r.RecordNumber,
                     r.UniqServReqID,
                     r.ReferralRequestReceivedDate,
                     r.ServDischDate,
                     s.ReferClosReason,
                     s.ReferRejectionDate,
                     s.ReferRejectReason,
                     s.UniqCareProfTeamID,
                     s.ServTeamTypeRefToMH,
                     r.PrimReasonReferralMH,
                     m.OrgIDCCGRes
 FROM                $db_source.mhs101referral r
 INNER JOIN          $db_source.mhs001mpi m 
                     ON r.RecordNumber = m.RecordNumber ---joining on recordnumber opposed to person_id as we want OrgIDCCGRes as it was inputted when referral was submitted in that month
 LEFT JOIN           $db_source.mhs102servicetypereferredto s 
                     ON r.UniqServReqID = s.UniqServReqID 
                     AND r.RecordNumber = s.RecordNumber --joining on recordnumber aswell to match historic records as they will all have the same uniqservreqid
 WHERE               s.ServTeamTypeRefToMH = 'A14' --service type referred to is always Early Intervention Team for Psychosis
                     AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = '') --to include England or blank Local Authorities only
                     ---AND r.UniqMonthID = '$month_id'

# COMMAND ----------

# %sql
# select * from $db_output.EIP_Refs_v2 where UNIQServReqID = 'RT5S:598824727' and UNiqMonthID = 1452

# COMMAND ----------

# %sql
# select uniqservreqid, count(UniqServReqID) from $db_output.EIP_Refs_v2 r
# WHERE ServDischDate IS NULL AND ReferRejectionDate IS NULL AND UniqMonthID = '$month_id'
# group by uniqservreqid
# having count(UniqServReqID) > 1

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Pre_Proc_Activity_v2;
 CREATE TABLE         $db_output.EIP_Pre_Proc_Activity_v2 AS
 
 SELECT
 	'DIRECT' AS Der_ActivityType,
 	c.MHS201UniqID AS Der_ActivityUniqID,
 	c.Person_ID,
 	c.UniqMonthID,
 	c.OrgIDProv,
 	c.RecordNumber,
 	c.UniqServReqID,
     c.OrgIDComm,
 	c.CareContDate AS Der_ContactDate,
     c.CareContTime AS Der_ContactTime,
     cast(unix_timestamp(cast(concat(cast(c.CareContDate as string), right(cast(c.CareContTime as string), 9)) as timestamp)) as timestamp) as Der_ContactDateTime,
     c.AdminCatCode,
 	c.SpecialisedMHServiceCode,
 	c.ClinContDurOfCareCont AS Der_ContactDuration,
 	c.ConsType,
 	c.CareContSubj,
 	c.ConsMechanismMH,                                 -------/*** V5 change - ConsMediumUsed' will change to 'ConsMechanismMH - AM Dec 16 2021***/
 	c.ActLocTypeCode,
 	c.SiteIDOfTreat,
 	c.GroupTherapyInd,
 	c.AttendOrDNACode,
 	c.EarliestReasonOfferDate,
 	c.EarliestClinAppDate,
 	c.CareContCancelDate,
 	c.CareContCancelReas,
 	c.RepApptOfferDate,
 	c.RepApptBookDate,
 	c.UniqCareContID,
 	c.AgeCareContDate,
 	c.ContLocDistanceHome,
 	c.TimeReferAndCareContact,
 	c.UniqCareProfTeamID AS Der_UniqCareProfTeamID,
 	c.PlaceOfSafetyInd,
 	CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
 	'NULL' AS Der_ContactOrder,
 	'NULL' AS Der_FYContactOrder,
 	'NULL' AS Der_DirectContactOrder,
 	'NULL' AS Der_FYDirectContactOrder,
 	'NULL' AS Der_FacetoFaceContactOrder,
 	'NULL' AS Der_FYFacetoFaceContactOrder
 
 	
 FROM $db_source.mhs201carecontact c
 
 ---WHERE c.UniqMonthID >= '$month_id'
 
 UNION ALL
 
 SELECT
 	'INDIRECT' AS Der_ActivityType,
 	i.MHS204UniqID AS Der_ActivityUniqID,
 	i.Person_ID,
 	i.UniqMonthID,
 	i.OrgIDProv,
 	i.RecordNumber,
 	i.UniqServReqID,
     i.OrgIDComm,
 	i.IndirectActDate AS Der_ContactDate,
 	i.IndirectActTime AS Der_ContactTime,
     cast(unix_timestamp(cast(concat(cast(i.IndirectActDate as string), right(cast(i.IndirectActTime as string), 9)) as timestamp)) as timestamp) as Der_ContactDateTime,
     'NULL' AS AdminCatCode,
 	'NULL' AS SpecialisedMHServiceCode,
 	i.DurationIndirectAct AS Der_ContactDuration,
 	'NULL' AS ConsType,
 	'NULL' AS CareContSubj,
 	'NULL' AS ConsMechanismMH,                                             -------/*** V5 change - ConsMediumUsed' will change to 'ConsMechanismMH - AM Dec 16 2021***/
 	'NULL' AS ActLocTypeCode,
 	'NULL' AS SiteIDOfTreat,
 	'NULL' AS GroupTherapyInd,
 	'NULL' AS AttendOrDNACode,
 	'NULL' AS EarliestReasonOfferDate,
 	'NULL' AS EarliestClinAppDate,
 	'NULL' AS CareContCancelDate,
 	'NULL' AS CareContCancelReas,
 	'NULL' AS RepApptOfferDate,
 	'NULL' AS RepApptBookDate,
 	'NULL' AS UniqCareContID,
 	'NULL' AS AgeCareContDate,
 	'NULL' AS ContLocDistanceHome,
 	'NULL' AS TimeReferAndCareContact,
 	i.OrgIDProv + i.CareProfTeamLocalId AS Der_UniqCareProfTeamID,
 	'NULL' AS PlaceOfSafetyInd,
 	CASE WHEN i.OrgIDProv = 'DFC' THEN '1' ELSE i.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
 	'NULL' AS Der_ContactOrder,
 	'NULL' AS Der_FYContactOrder,
 	'NULL' AS Der_DirectContactOrder,
 	'NULL' AS Der_FYDirectContactOrder,
 	'NULL' AS Der_FacetoFaceContactOrder,
 	'NULL' AS Der_FYFacetoFaceContactOrder
 	
 FROM $db_source.mhs204indirectactivity i
 
 ---WHERE i.UniqMonthID >= '$month_id'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Activity_v2;
 CREATE TABLE         $db_output.EIP_Activity_v2 AS
 ---indirect and direct attended activity
 SELECT 
 a.Der_ActivityType,
 a.Der_ActivityUniqID,
 a.Person_ID,
 a.Der_PersonID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.RecordNumber,
 a.UniqServReqID,	
 a.Der_ContactDate,
 a.Der_ContactTime,
 a.Der_ContactDateTime,
 ROW_NUMBER() OVER (PARTITION BY a.Der_PersonID, a.UniqServReqID ORDER BY a.Der_ContactDateTime ASC, a.Der_ActivityUniqID ASC) AS Der_ContactOrder
 FROM $db_output.EIP_Pre_Proc_Activity_v2 a
 WHERE 
 (a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (((a.ConsMechanismMH NOT IN ('05', '06') AND a.UniqMonthID < '1459') OR (a.ConsMechanismMH NOT IN ('05', '09') and a.UniqMonthID >= '1459'))  
  OR a.OrgIDProv = 'DFC' AND ((a.ConsMechanismMH IN ('05', '06') and a.UniqMonthID < '1459') or (a.ConsMechanismMH IN ('05', '09') and a.UniqMonthID >= '1459')))) OR a.Der_ActivityType = 'INDIRECT'
  --/**** V5 Changes- AM Dec 31 2021- ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data  ****/
 
 
 --(a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (a.ConsMediumUsed NOT IN ('05','06') OR a.OrgIDProv = 'DFC' AND a.ConsMediumUsed IN ('05','06'))) OR a.Der_ActivityType = 'INDIRECT'
 ---where patient had direct attended activity and (attended medium used was not via email or SMS or Provider was Kooth and medium was via email or SMS) or activity was indirect

# COMMAND ----------

# DBTITLE 1,Contacts data
 %sql
 
 
 --This cell looks at all activity in the MHS 202 and MHS204 tables. It takes the left position for all snomed codes.
 --Again this could potentially be optimised so that it is only pulling back what we need. NHS E pre process this reference data into their dataset
 
 
 
 --CREATE OR REPLACE GLOBAL TEMPORARY VIEW Pre_Proc AS
 DROP TABLE IF EXISTS $db_output.EIP_Pre_Proc_Interventions_v2;
 CREATE TABLE         $db_output.EIP_Pre_Proc_Interventions_v2 AS
 
 SELECT                ca.RecordNumber,
                       ca.Person_ID,
                       ca.UniqMonthID,
                       ca.UniqServReqID,
                       ca.UniqCareContID,
                       cc.CareContDate AS Der_ContactDate,
                       ca.UniqCareActID,
                       ca.MHS202UniqID as Der_InterventionUniqID,
                       --gets first snomed code in list where CodeProcandProcStatus contains a ":"
                       CASE WHEN position(':' in ca.CodeProcAndProcStatus) > 0 THEN LEFT(ca.CodeProcAndProcStatus, position (':' in ca.CodeProcAndProcStatus)-1) 
                            ELSE ca.CodeProcAndProcStatus
                            END AS Der_SNoMEDProcCode,
                       ca.CodeObs
 FROM                  $db_source.mhs202careactivity ca
 LEFT JOIN             $db_source.mhs201carecontact cc 
                       ON ca.RecordNumber = cc.RecordNumber 
                       AND ca.UniqCareContID = cc.UniqCareContID
 WHERE                 (ca.CodeFind IS NOT NULL OR ca.CodeObs IS NOT NULL OR ca.CodeProcAndProcStatus IS NOT NULL) --AND ca.UniqMonthID >= '$month_id'
 
 UNION ALL
 --This would probably work better as an insert into due to the amount of data processing
 SELECT 	              i.RecordNumber,
                       i.Person_ID,
                       i.UniqMonthID,
                       i.UniqServReqID,
                       'null' as UniqCareContID,
                       i.IndirectActDate AS Der_ContactDate,
                       'null' as UniqCareActID,
                       i.MHS204UniqID as Der_InterventionUniqID,
                       --gets first snomed code in list where CodeProcandProcStatus contains a ":"
                       CASE WHEN position(':' in i.CodeProcAndProcStatus) > 0 THEN LEFT(i.CodeProcAndProcStatus, position (':' in i.CodeProcAndProcStatus)-1) 
                            ELSE i.CodeProcAndProcStatus
                            END AS Der_SNoMEDProcCode,
                       NULL AS CodeObs
 FROM                  $db_source.mhs204indirectactivity i
 WHERE                 (i.CodeFind IS NOT NULL OR i.CodeProcAndProcStatus IS NOT NULL) ---AND i.UniqMonthID >= '$month_id'

# COMMAND ----------

# DBTITLE 1,Processing contacts data
 %sql
 
 
 --This cell makes sure we are only looking at the active and valid snomed codes we require using the reference data type_id, and making sure valid snomed code
 
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW EIP_Interventions_v2 AS
 
 SELECT DISTINCT   RecordNumber,
                   Person_ID,
                   UniqServReqID,
                   Der_ContactDate,
                   UniqCareContID,                  
                   UniqCareActID,
                   Der_InterventionUniqID,                
                   CodeObs,
                   s3.Term AS Der_SNoMEDObsTerm,
                   Der_SNoMEDProcCode,
                   s1.Term AS Der_SNoMEDProcTerm
                   
 FROM              $db_output.EIP_Pre_Proc_Interventions_v2 i
 LEFT JOIN         $db_output.SCT_Concepts_FSN s1 
                   ON i.Der_SNoMEDProcCode = s1.ID                 
                  --- AND s1.ACTIVE = 1 ---REMOVED FROM JOIN 18/08/21 AT               
 LEFT JOIN         $db_output.SCT_Concepts_FSN s3 
                   ON i.CodeObs = s3.ID
                  --- AND s3.ACTIVE = 1   

# COMMAND ----------

# DBTITLE 1,Listing relevant snomed codes for EIP
 %sql
 
 
 --Lists all the NICE concordant snomed codes. The contact data needs to be between the referral start date and the rp_enddate, and joined back on with record number. The EIP referral can include contacts from other referrals within the same provider for the same person for the same month period
 
 
 --CREATE OR REPLACE GLOBAL TEMPORARY VIEW Procedures AS
 DROP TABLE IF EXISTS $db_output.EIP_Procedures_v2;
 CREATE TABLE         $db_output.EIP_Procedures_v2 AS
 
 SELECT              r.Person_ID,
                     r.UniqServReqID,
                     r.UniqMonthID,
                     r.RecordNumber,
                     COALESCE(c.Der_SNoMEDProcTerm, c.Der_SNoMEDObsTerm) as Der_SNoMEDTerm,               
                     CASE WHEN c.Der_SNoMEDProcCode IN ---Checked this list against EIP Recording and Reporting Document from Jan 2019 - AT 15/07/2021
                                --CBTp
 	('718026005', -- 'Cognitive behavioural therapy for psychosis'
 	'1097161000000100', -- 'Referral for cognitive behavioural therapy for psychosis'
 
 --FI
 	'985451000000105', -- 'Family intervention for psychosis'
 	'859411000000105', -- 'Referral for family therapy'
 
 --Clozapine
 	'723948002', -- 'Clozapine therapy'
 
 --Physical health assessments and healthy lifestyle promotion
 	'196771000000101', -- 'Smoking assessment'
 	'443781008', -- 'Assessment of lifestyle'
 	'698094009', -- 'Measurement of body mass index'
 	'171222001', -- 'Hypertension screening'
 	'43396009', -- 'Haemoglobin A1c measurement'
 	'271062006', -- 'Fasting blood glucose measurement'
 	'271061004', -- 'Random blood glucose measurement'
 	'121868005', -- 'Total cholesterol measurement'
 	'17888004', -- 'High density lipoprotein measurement'
 	'166842003', -- 'Total cholesterol: HDL ratio measurement'
 	'763243004', -- 'Assessment using QRISK cardiovascular disease 10 year risk calculator'
 	'225323000', -- 'Smoking cessation education'
 	'710081004', -- 'Smoking cessation therapy'
 	'871661000000106', -- 'Referral to smoking cessation service'
 	'715282001', -- 'Combined healthy eating and physical education programme'
 	'1094331000000103', -- 'Referral for combined healthy eating and physical education programme' ---CHANGED FROM CARL'S CODE TO MATCH PDF---
 	'281078001', -- 'Education about alcohol consumption'
 	'425014005', -- 'Substance use education'
 	'1099141000000106', -- 'Referral to alcohol misuse service'
 	'201521000000104', -- 'Referral to substance misuse service'
 	'699826006', -- 'Lifestyle education regarding risk of diabetes'
 	'718361005', -- 'Weight management programme'
 	'408289007', -- 'Refer to weight management programme'
 	'1097171000000107', -- 'Referral for lifestyle education'
 
 --Physical health interventions
 	'1090411000000105', -- 'Referral to general practice service'
 	'1097181000000109', -- 'Referral for antihypertensive therapy'
 	'308116003', -- 'Antihypertensive therapy'
 	'1099151000000109', -- 'Referral for diabetic care'
 	'385804009', -- 'Diabetic care'
 	'1098021000000108', -- 'Diet modification'
 	'1097191000000106', -- 'Metformin therapy'
 	'134350008', -- 'Lipid lowering therapy'
 
 --Education and empolyment suppoort
 	'183339004', -- 'Education rehabilitation'
 	'415271004', -- 'Referral to education service'
 	'70082004', -- 'Vocational rehabilitation'
 	'18781004', -- 'Patient referral for vocational rehabilitation'
 	'335891000000102', -- 'Supported employment'
 	'1098051000000103', -- 'Employment support'
 	'1098041000000101', -- 'Referral to an employment support service'
 	'1082621000000104', -- 'Individual Placement and Support'
 	'1082611000000105', -- 'Referral to an Individual Placement and Support service'
 
 --Carer focused education and support
 	'726052009', -- 'Carer focused education and support programme'
 	'1097201000000108', -- 'Referral for carer focused education and support programme' ---CHANGED FROM CARL'S CODE TO MATCH PDF---
 
 --ARMS
 	'304891004', -- 'Cognitive behavioural therapy'
 	'519101000000109', -- 'Referral for cognitive behavioural therapy'
 	'1095991000000102') -- 'At Risk Mental State for Psychosis'
 
 --duplicated these codes to pick up those recorded as OBS
 	OR c.CodeObs IN( '196771000000101', -- 'Smoking assessment'
 	'698094009', -- 'Measurement of body mass index'
 	'171222001', -- 'Hypertension screening'
 	'43396009', -- 'Haemoglobin A1c measurement'
 	'271062006', -- 'Fasting blood glucose measurement'
 	'271061004', -- 'Random blood glucose measurement'
 	'121868005', -- 'Total cholesterol measurement'
 	'17888004', -- 'High density lipoprotein measurement'
 	'166842003') -- 'Total cholesterol: HDL ratio measurement'
 	THEN 'NICE concordant' 
 	WHEN COALESCE(c.Der_SNoMEDProcCode,c.CodeObs) IS NOT NULL THEN 'Other' 
 	END AS Intervention_type
 FROM              $db_output.EIP_Refs_v2 r
 INNER JOIN        global_temp.EIP_Interventions_v2 c ON r.recordnumber = c.recordnumber --FLAG
                   ---joining on record number so will be also bringing in activity for the person in the month which may not be associated with EIP referral
                  AND COALESCE(c.Der_SNoMEDProcTerm, c.Der_SNoMEDObsTerm) IS NOT NULL 
                  AND c.Der_ContactDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate, '$rp_enddate') AND r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL

# COMMAND ----------

# %sql
# select person_id, recordnumber, count(distinct uniqservreqid) from $db_output.EIP_Procedures_v2 where uniqmonthid = '$month_id'
# group by person_id, recordnumber
# having count(distinct uniqservreqid) > 1

# COMMAND ----------

# %sql
# select * from $db_output.EIP_Refs_v2 where recordnumber = 587568000032968

# COMMAND ----------

# %sql
# select p.person_id, r.uniqservreqid as eip_referral, p.uniqservreqid as proc_referral, p.recordnumber, p.Intervention_type 
# from $db_output.EIP_Refs_v2 r
# inner join $db_output.EIP_Procedures_v2 p on r.recordnumber = p.recordnumber
# where p.uniqmonthid = '$month_id' and p.recordnumber = 587568000032968 and r.uniqservreqid = 'RT25_1851951'
# order by p.uniqservreqid desc

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Procedures_Agg;
 CREATE TABLE         $db_output.EIP_Procedures_Agg AS
 
 SELECT
 	r.UniqMonthID,
 	r.OrgIDProv,
 	r.RecordNumber,
 	r.UniqServReqID,
 	COUNT(Intervention_type) AS AnySNoMED,
 	SUM(CASE WHEN a.Intervention_type = 'NICE concordant' THEN 1 ELSE 0 END) AS NICESNoMED
     
 FROM $db_output.EIP_Refs_v2 r    
     
 LEFT JOIN $db_output.EIP_Procedures_v2 a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID
 GROUP BY r.RecordNumber, r.UniqServReqID, r.UniqMonthID, r.OrgIDProv

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Activity_Agg;
 CREATE TABLE         $db_output.EIP_Activity_Agg AS
 
 SELECT
 	r.Person_ID,
     r.UniqMonthID,
 	r.RecordNumber,
 	r.UniqServReqID,
 		
 	--in month activity
 	COUNT(CASE WHEN a.Der_ContactOrder IS NOT NULL AND Der_ActivityType = 'DIRECT' THEN a.RecordNumber END) AS Der_InMonthContacts
     
 FROM $db_output.EIP_Refs_v2 r   
 
 INNER JOIN $db_output.EIP_Activity_v2 a ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID
 
 GROUP BY r.Person_ID, r.UniqMonthID, r.RecordNumber, r.UniqServReqID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Activity_Cumulative;
 CREATE TABLE         $db_output.EIP_Activity_Cumulative AS
 
 SELECT
 	r.Person_ID,
     r.UniqMonthID,
 	r.RecordNumber,
 	r.UniqServReqID,
 
 	-- cumulative activity
 	SUM(COALESCE(a.Der_InMonthContacts, 0)) AS Der_CumulativeContacts
 
 FROM $db_output.EIP_Refs_v2 r  
 
 LEFT JOIN $db_output.EIP_Activity_Agg a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.UniqMonthID <= r.UniqMonthID----and r.RecordNumber = a.RecordNumber ----
 
 GROUP BY r.Person_ID, r.UniqMonthID, r.RecordNumber, r.UniqServReqID

# COMMAND ----------

# DBTITLE 1,For Mapping CCG20 to CCG21 (For checking against Carl's output only)
import pandas as pd
import io

data = """
CCG_UNMAPPED,CCG21CDH
00C,16C
00D,84H
00J,84H
00K,16C
00L,00L
00M,16C
00N,00N
00P,00P
00Q,00Q
00R,00R
00T,00T
00V,00V
00X,00X
00Y,00Y
01A,01A
01C,27D
01D,01D
01E,01E
01F,01F
01G,01G
01H,01H
01J,01J
01K,01K
01R,27D
01T,01T
01V,01V
01W,01W
01X,01X
01Y,01Y
02A,02A
02D,27D
02E,02E
02F,27D
02G,02G
02H,02H
02M,02M
02N,36J
02P,02P
02Q,02Q
02R,36J
02T,02T
02W,36J
02X,02X
02Y,02Y
03A,X2C4Y
03D,42D
03E,42D
03F,03F
03H,03H
03J,X2C4Y
03K,03K
03L,03L
03M,42D
03N,03N
03Q,03Q
03R,03R
03T,71E
03V,78H
03W,03W
03X,15M
03Y,15M
04C,04C
04D,71E
04E,52R
04F,M1J4Y
04G,78H
04H,52R
04J,15M
04K,52R
04L,52R
04M,52R
04N,52R
04Q,71E
04R,15M
04V,04V
04Y,04Y
05A,B2M3M
05C,D2P2L
05D,05D
05F,18C
05G,05G
05H,B2M3M
05J,18C
05L,D2P2L
05N,M2L0M
05Q,05Q
05R,B2M3M
05T,18C
05V,05V
05W,05W
05X,M2L0M
05Y,D2P2L
06A,D2P2L
06D,18C
06F,M1J4Y
06H,06H
06K,06K
06L,06L
06M,26A
06N,06N
06P,M1J4Y
06Q,06Q
06T,06T
06V,26A
06W,26A
06Y,26A
07G,07G
07H,07H
07J,26A
07K,07K
07L,A3A8R
07M,93C
07N,72Q
07P,W2U3Z
07Q,72Q
07R,93C
07T,A3A8R
07V,36L
07W,W2U3Z
07X,93C
07Y,W2U3Z
08A,72Q
08C,W2U3Z
08D,93C
08E,W2U3Z
08F,A3A8R
08G,W2U3Z
08H,93C
08J,36L
08K,72Q
08L,72Q
08M,A3A8R
08N,A3A8R
08P,36L
08Q,72Q
08R,36L
08T,36L
08V,A3A8R
08W,A3A8R
08X,36L
08Y,W2U3Z
09A,W2U3Z
09C,91Q
09D,09D
09E,91Q
09F,97R
09G,70F
09H,70F
09J,91Q
09L,92A
09N,92A
09P,97R
09W,91Q
09X,70F
09Y,92A
10A,91Q
10C,D4U1Y
10D,91Q
10E,91Q
10J,D9Y0V
10K,D9Y0V
10L,D9Y0V
10Q,10Q
10R,10R
10V,D9Y0V
10X,D9Y0V
11A,D9Y0V
11E,92G
11J,11J
11M,11M
11N,11N
11X,11X
12D,92G
12F,12F
13T,13T
14L,14L
14Y,14Y
15A,15A
15C,15C
15D,D4U1Y
15E,15E
15F,15F
15M,15M
15N,15N
16C,16C
18C,18C
26A,26A
27D,27D
36J,36J
36L,36L
42D,42D
52R,52R
70F,70F
71E,71E
72Q,72Q
78H,78H
84H,84H
91Q,91Q
92A,92A
92G,92G
93C,93C
97R,97R
99A,99A
99C,99C
99D,71E
99E,99E
99F,99F
99G,99G
99H,92A
99J,91Q
99K,97R
99M,D4U1Y
99N,92G
99P,15N
99Q,15N
A3A8R,A3A8R
B2M3M,B2M3M
D2P2L,D2P2L
03J,03J
03A,03A
04F,04F
05A,05A
05C,05C
05H,05H
10J,10J
10K,10K
10L,10L
10V,10V
10X,10X
11A,11A
15D,15D
99M,99M
08G,08G
08M,08M
08N,08N
08V,08V
08W,08W
08Y,08Y
09A,09A
10C,10C
07L,07L
07P,07P
07T,07T
07W,07W
07Y,07Y
08C,08C
08E,08E
08F,08F
05L,05L
05N,05N
05R,05R
05X,05X
05Y,05Y
06A,06A
06F,06F
06P,06P
"""
df = pd.read_csv(io.StringIO(data), header=0, delimiter=',').astype(str)
spark.createDataFrame(df).createOrReplaceGlobalTempView("ccg20_ccg21_mapping")

# COMMAND ----------

 %sql
 select a.ORG_CODE as RD_CCG_LATEST_CODE, b.CCG21CDH as MAPPING_CCG_CODE
 FROM (select distinct ORG_CODE from global_temp.RD_CCG_LATEST) a
 LEFT JOIN (select distinct CCG21CDH from global_temp.ccg20_ccg21_mapping) b on a.ORG_CODE = b.CCG21CDH
 where b.CCG21CDH is null
 order by a.ORG_CODE

# COMMAND ----------

# DBTITLE 1,Master prep table
 %sql
 
 --Pulls together final preperation table
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW EIP_Master AS
 
 SELECT          r.UniqMonthID,
                 r.OrgIDProv,
                 r.RecordNumber,
                 r.UniqServReqID,
                 r.person_id, 
                 cu.Der_CumulativeContacts,
                 p.AnySNoMED,
                 p.NICESNoMED,
                 coalesce(ccg.org_code,'UNKNOWN') as CCG_CODE,
                 coalesce(ccg.name,'UNKNOWN') as CCG_NAME,
                 coalesce(stpre.STP_CODE,'UNKNOWN') as STP_CODE,
                 coalesce(stpre.STP_NAME,'UNKNOWN') as STP_NAME,
                 coalesce(stpre.REGION_CODE,'UNKNOWN') as REGION_CODE,
                 coalesce(stpre.REGION_NAME,'UNKNOWN') as REGION_NAME,
                 -- get caseload measures
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL THEN 1 ELSE 0 END AS Open_Referrals,
 	            CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND cu.Der_CumulativeContacts >=1 THEN 1 ELSE 0 END AS EIP_Caseload,
 --                 CASE WHEN (r.ServDischDate IS NULL OR r.ServDischDate > '$rp_enddate') AND (r.ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate') THEN 1 ELSE 0 END AS Open_Referrals,
 -- 	            CASE WHEN (r.ServDischDate IS NULL OR r.ServDischDate > '$rp_enddate') AND (r.ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate') AND cu.Der_CumulativeContacts >=1 THEN 1 ELSE 0 END AS  EIP_Caseload,
                 -- get aggregate SNoMED measures
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND p.AnySNoMED > 0 THEN 1 ELSE 0 END AS RefWithAnySNoMED,
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND p.NICESNoMED > 0 THEN 1 ELSE 0 END AS RefWithNICESNoMED
 --                 CASE WHEN (r.ServDischDate IS NULL OR r.ServDischDate > '$rp_enddate') AND (r.ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate') AND p.AnySNoMED > 0 THEN 1 ELSE 0 END AS AnySNoMED,
 --                 CASE WHEN (r.ServDischDate IS NULL OR r.ServDischDate > '$rp_enddate') AND (r.ReferRejectionDate IS NULL OR ReferRejectionDate > '$rp_enddate') AND p.NICESNoMED > 0 THEN 1 ELSE 0 END AS NICESNoMED
 FROM            $db_output.EIP_Refs_v2 r 
 LEFT JOIN       $db_output.EIP_Procedures_Agg p ON r.RecordNumber = p.RecordNumber AND r.UniqServReqID = p.UniqServReqID
 LEFT JOIN       $db_output.EIP_Activity_Cumulative cu ON r.RecordNumber = cu.RecordNumber AND r.UniqServReqID = cu.UniqServReqID
 LEFT JOIN       global_temp.ccg20_ccg21_mapping mp ON r.OrgIDCCGRes = mp.CCG_UNMAPPED
 LEFT JOIN       global_temp.RD_CCG_LATEST ccg --get the latest ccg reference data
                 ON mp.CCG21CDH = ccg.org_code
 LEFT JOIN       $db_output.EIP_STP_MAPPING stpre
                 ON ccg.org_code = stpre.CCG_CODE
 WHERE           
                     (ServDischDate IS NULL AND ReferRejectionDate IS NULL)            
                 AND r.UniqMonthID = '$month_id'

# COMMAND ----------



# COMMAND ----------

 %sql
 select count(distinct CCG_CODE) from global_temp.EIP_Master

# COMMAND ----------

 %sql
 ---Final data check
 select sum(Open_Referrals),sum(EIP_Caseload),sum(RefWithAnySNoMED),sum(RefWithNICESNoMED) from global_temp.EIP_Master

# COMMAND ----------

# DBTITLE 1,SNoMED Dashboard output
 %sql
 SELECT
 	m.UniqMonthID,
 	CASE WHEN a.Intervention_type is null then 'Unknown' else a.Intervention_type end as Intervention_type,
 	CASE WHEN a.Der_SNoMEDTerm is null then 'Unknown' else a.Der_SNoMEDTerm end as Der_SNoMEDTerm,
 	COUNT(a.Der_SNoMEDTerm) AS Number_of_interventions,
     COUNT(distinct m.UniqServReqID) AS Number_of_referrals,
     COUNT(distinct m.OrgIDProv) AS Number_of_Providers
     ---- Number of Referals, Providers
 
 FROM global_temp.EIP_Master m
 
 LEFT JOIN $db_output.EIP_Procedures_v2 a ON m.Person_ID = a.Person_ID AND m.UniqServReqID = a.UniqServReqID AND m.RecordNumber = a.RecordNumber
 
 ---WHERE a.Intervention_type = 'NICE concordant'
 
 GROUP BY  
 m.UniqMonthID,
 CASE WHEN a.Intervention_type is null then 'Unknown' else a.Intervention_type end, 
 CASE WHEN a.Der_SNoMEDTerm is null then 'Unknown' else a.Der_SNoMEDTerm end
 ORDER BY Intervention_type, COUNT(a.Der_SNoMEDTerm) desc

# COMMAND ----------

# DBTITLE 1,Create output table
 %sql
 DROP TABLE IF EXISTS $db_output.csv_output_unsup;
 CREATE TABLE IF NOT EXISTS $db_output.csv_output_unsup(
   REPORTING_PERIOD_START string,
   REPORTING_PERIOD_END string,
   STATUS string,
   BREAKDOWN string,
   PRIMARY_LEVEL string,
   PRIMARY_LEVEL_DESCRIPTION string,
   SECONDARY_LEVEL string,
   SECONDARY_LEVEL_DESCRIPTION string,
   METRIC string,
   METRIC_VALUE string
 )

# COMMAND ----------

# DBTITLE 1,Insert Caseload Breakdowns
 %sql
 INSERT INTO $db_output.csv_output_unsup
 --England breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS METRIC,
   sum(EIP_Caseload) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS METRIC,
   sum(EIP_Caseload) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 GROUP BY  OrgIDProv
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN, -- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency HL 25/01/22
   CCG_CODE AS PRIMARY_LEVEL,
   CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS METRIC,
   sum(EIP_Caseload) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS PRIMARY_LEVEL,
   REGION_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS METRIC,
    sum(EIP_Caseload) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP' AS BREAKDOWN,
   STP_CODE AS PRIMARY_LEVEL,
   STP_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS METRIC,
    sum(EIP_Caseload) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

# DBTITLE 1,Insert AnySNoMED Breakdowns
 %sql
 INSERT INTO $db_output.csv_output_unsup
 --England breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE  
 FROM      global_temp.EIP_master
 GROUP BY  OrgIDProv
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN,-- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency HL 25/01/22
   CCG_CODE AS PRIMARY_LEVEL,
   CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE   
 FROM      global_temp.EIP_master
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS PRIMARY_LEVEL,
   REGION_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE   
 FROM      global_temp.EIP_master
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP' AS BREAKDOWN,
   STP_CODE AS PRIMARY_LEVEL,
   STP_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE   
 FROM      global_temp.EIP_master
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

# DBTITLE 1,Insert NICESNoMED breakdowns
 %sql
 INSERT INTO $db_output.csv_output_unsup
 --England breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE   
 FROM      global_temp.EIP_master
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE 
 FROM      global_temp.EIP_master
 GROUP BY  OrgIDProv
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN,-- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency HL 25/01/22
   CCG_CODE AS PRIMARY_LEVEL,
   CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE    
 FROM      global_temp.EIP_master
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS PRIMARY_LEVEL,
   REGION_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE    
 FROM      global_temp.EIP_master
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP' AS BREAKDOWN,
   STP_CODE AS PRIMARY_LEVEL,
   STP_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE   
 FROM      global_temp.EIP_master
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

 %sql
 
 SELECT 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 METRIC,
 CASE WHEN METRIC = 'EIP68' THEN 'EIP Caseload - Number of referrals with at least one attended contact and are still open at the end of the month'
      WHEN METRIC = 'EIP69a' THEN 'Number of referrals with any SNOMED code and are still open at the end of the month'
      WHEN METRIC = 'EIP69b' THEN 'Number of referrals with NICE Concordant SNOMED code and are still open at the end of the month'
      END AS METRIC_NAME,
 METRIC_VALUE   
 FROM $db_output.csv_output_unsup

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.csv_output_sup;
 CREATE TABLE         $db_output.csv_output_sup AS
 SELECT * FROM $db_output.csv_output_unsup
 WHERE BREAKDOWN = 'England'
 UNION ALL
 SELECT
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 METRIC,
 cast(case when cast(Metric_value as float) < 5 then '*' else cast(round(metric_value/5,0)*5 as int) end as string) as  METRIC_VALUE
 FROM $db_output.csv_output_unsup
 WHERE BREAKDOWN != 'England'

# COMMAND ----------

 %sql
 SELECT * FROM $db_output.csv_output_sup

# COMMAND ----------

