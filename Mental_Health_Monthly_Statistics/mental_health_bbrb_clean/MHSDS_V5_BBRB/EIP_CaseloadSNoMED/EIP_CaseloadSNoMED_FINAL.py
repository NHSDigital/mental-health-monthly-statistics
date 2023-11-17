# Databricks notebook source
 %md
 ##### Please note as of March 2022 the name of the widgets has changed
 example below is for: 
 *January 2022*
 
 **db_output:**
 *Your database name*
 
 **db_source:**
 *$reference_data*
 
 **end_month_id (previously "month_id"):**
 this is the month you are reporting, eg 1462 (Jan 2022)
 
 **rp_enddate (previously "rp_enddate"):**
 this is the end month you are reporting, eg '2022-01-31' (Jan 2022)
 
 **rp_startdate_1m (previoulsy "rp_startdate"):**
 this is the start month you are reporting, eg '2022-01-01' (Jan 2022)
 
 **status:**
 this depends on whether you're running 'Performance' or 'Provisional' so remember to update this
 
 ///////////////////////////////////////////////////////////////////////////////////////
 
 these might appear but not required to populate
 
 rp_startdate_12m (previoulsy "rp_startdate") --this is not required
 this widget is only used for 12 rolling month reports
 
 rp_enddate_1m (previously "rp_enddate") -- this is not required
 this widget is only used for 12 rolling month reports
 
 rp_startdate_qtr (not previously used) -- this is not required
 this widget is only used for rolling month reports
 
 start_month_id (not previoulsy used) -- this is not required
 this is not used for this report

# COMMAND ----------

 %md 
 
 ### [Go to Suppressed output](https://db.core.data.digital.nhs.uk/#notebook/3540351/command/3540381)
 ### [Go to Raw/unsuppressed output](https://db.core.data.digital.nhs.uk/#notebook/3540351/command/3540379)

# COMMAND ----------

 %sql
 -- # dbutils.widgets.removeAll()
 select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.mhs000header order by 1

# COMMAND ----------

# DBTITLE 1,Widgets and explanations of each one
startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]

dbutils.widgets.dropdown("rp_startdate_1m", "2020-04-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
dbutils.widgets.dropdown("end_month_id", "1452", monthid)
dbutils.widgets.text("status","Provisional")
dbutils.widgets.text("db_output","$user_id")
dbutils.widgets.text("db_source","$reference_data")
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
params = {'rp_startdate_1m': str(rp_startdate_1m), 'rp_enddate': str(rp_enddate),  'end_month_id': end_month_id, 'db_output': db_output, 'db_source': db_source, 'status': status}
print(params)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Create SNoMED Reference data
dbutils.notebook.run('Create_SNoMED_FSN_Ref', 0, params)

# COMMAND ----------

 %sql
 ---Fully Specified Name SNOMED REF data
 ---select * from $db_output.SCT_Concepts_FSN limit 3

# COMMAND ----------

 %sql
 ---Preferred Term SNOMED REF data
 ---select * from $db_output.SCT_Concepts_PrefTerm limit 3

# COMMAND ----------

# DBTITLE 1,Org daily Prep
 %sql
 DROP TABLE IF EXISTS $db_output.EIP_ORG_DAILY1;
 CREATE TABLE $db_output.EIP_ORG_DAILY1 USING DELTA AS 
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate' 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_ORG_RELATIONSHIP_DAILY;
 CREATE TABLE $db_output.EIP_ORG_RELATIONSHIP_DAILY USING DELTA AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,commis
 REL_CLOSE_DATE
 FROM 
 $reference_data.org_relationship_daily
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,Gets Region & STP reference data
 %sql
 DROP TABLE IF EXISTS $db_output.EIP_STP_MAPPING;
 CREATE TABLE $db_output.EIP_STP_MAPPING USING DELTA AS 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.EIP_ORG_DAILY1 A
 LEFT JOIN $db_output.EIP_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.EIP_ORG_DAILY1 C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.EIP_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.EIP_ORG_DAILY1 E ON D.REL_TO_ORG_CODE = E.ORG_CODE
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
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

# DBTITLE 1,Referral data
 %sql
 
 
 --This cell gets all referrals where servive team type referred to was EIP (A14). It joins on record number to get the demographics at the time rather latest information used elsewhere in the monthly publication.
 
 DROP TABLE IF EXISTS $db_output.EIP_Refs_v2;
 CREATE TABLE         $db_output.EIP_Refs_v2 USING DELTA AS
 
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
 WHERE               s.ServTeamTypeRefToMH = 'A14' --service type referred to is always Early Intervention Team for Psychosis /***
                     AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = '') --to include England or blank Local Authorities only
                    

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Pre_Proc_Activity_v2;
 CREATE TABLE         $db_output.EIP_Pre_Proc_Activity_v2 USING DELTA AS
 
 ---This table gets all activity data relating to direct and indirect contacts. Has to take every care contact ever due to the methodology used
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
    	c.SpecialisedMHServiceCode,
 	c.ClinContDurOfCareCont AS Der_ContactDuration,
 	c.ConsType,
 	c.CareContSubj,
 	c.ConsMechanismMH, /*** v5 ConsMediumUsed replaced by ConsMechanismMH, code list change  GF ***/
 	c.SiteIDOfTreat,
 	c.AttendOrDNACode,
 	c.EarliestReasonOfferDate,
 	c.EarliestClinAppDate,
 	c.CareContCancelDate,
 	c.CareContCancelReas,
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
 	'NULL' AS SpecialisedMHServiceCode,
 	i.DurationIndirectAct AS Der_ContactDuration,
 	'NULL' AS ConsType,
 	'NULL' AS CareContSubj,
 	'NULL' AS ConsMechanismMH,      /*** v5 ConsMediumUsed replaced by ConsMechanismMH, code list change  ***/
 	'NULL' AS SiteIDOfTreat,
 	'NULL' AS AttendOrDNACode,
 	'NULL' AS EarliestReasonOfferDate,
 	'NULL' AS EarliestClinAppDate,
 	'NULL' AS CareContCancelDate,
 	'NULL' AS CareContCancelReas,
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

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Activity_v2;
 CREATE TABLE         $db_output.EIP_Activity_v2 USING DELTA AS
 ---filtering the above table to only show indirect and direct attended activity and also ordering the contact datetime for each Unique Activity ID
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
 WHERE (a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (((a.ConsMechanismMH NOT IN ('05', '06') and a.UniqMonthID < '1459') OR (a.ConsMechanismMH IN ('01', '02', '04', '11') and a.UniqMonthID >= '1459')) OR a.OrgIDProv = 'DFC' AND ((a.ConsMechanismMH IN ('05', '06') and a.UniqMonthID < '1459') OR (a.ConsMechanismMH IN ('05', '09', '10', '13') and a.UniqMonthID >= '1459')))) OR a.Der_ActivityType = 'INDIRECT'

# COMMAND ----------

# DBTITLE 1,Contacts data
 %sql
 
 
 --This cell looks at all activity in the MHS 202 and MHS204 tables. It takes the left position for all snomed codes.
 
 DROP TABLE IF EXISTS $db_output.EIP_Pre_Proc_Interventions_v2;
 CREATE TABLE         $db_output.EIP_Pre_Proc_Interventions_v2 USING DELTA AS
 
 SELECT                ca.RecordNumber,
                       ca.Person_ID,
                       ca.UniqMonthID,
                       ca.UniqServReqID,
                       ca.UniqCareContID,
                       cc.CareContDate AS Der_ContactDate,
                       ca.UniqCareActID,
                       ca.MHS202UniqID as Der_InterventionUniqID,
                       ca.CodeProcAndProcStatus as CodeProcAndProcStatus, --gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"
                       CASE WHEN position(':' in ca.CodeProcAndProcStatus) > 0 THEN LEFT(ca.CodeProcAndProcStatus, position (':' in ca.CodeProcAndProcStatus)-1) 
                            ELSE ca.CodeProcAndProcStatus
                            END AS Der_SNoMEDProcCode,
                       ca.CodeObs
 FROM                  $db_source.mhs202careactivity ca
 LEFT JOIN             $db_source.mhs201carecontact cc 
                       ON ca.RecordNumber = cc.RecordNumber 
                       AND ca.UniqCareContID = cc.UniqCareContID
 WHERE                 (ca.CodeFind IS NOT NULL OR ca.CodeObs IS NOT NULL OR ca.CodeProcAndProcStatus IS NOT NULL)
 
 UNION ALL
 SELECT 	              i.RecordNumber,
                       i.Person_ID,
                       i.UniqMonthID,
                       i.UniqServReqID,
                       'null' as UniqCareContID,
                       i.IndirectActDate AS Der_ContactDate,
                       'null' as UniqCareActID,
                       i.MHS204UniqID as Der_InterventionUniqID,
                       i.CodeIndActProcAndProcStatus as CodeProcAndProcStatus,  /*** CodeProcAndProcStatus renamed to CodeIndActProcAndProcStatus GF ***/
                       --gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"
                       CASE WHEN position(':' in i.CodeIndActProcAndProcStatus) > 0 THEN LEFT(i.CodeIndActProcAndProcStatus, position (':' in i.CodeIndActProcAndProcStatus)-1) 
                            ELSE i.CodeIndActProcAndProcStatus /*** CodeProcAndProcStatus 4 occurrences renamed to CodeIndActProcAndProcStatus GF ***/
                            END AS Der_SNoMEDProcCode,
                       NULL AS CodeObs
 FROM                  $db_source.mhs204indirectactivity i
 WHERE                 (i.CodeFind IS NOT NULL OR i.CodeIndActProcAndProcStatus IS NOT NULL)

# COMMAND ----------

# DBTITLE 1,Processing contacts data
 %sql
 
 
 --This cell makes sure we are only looking at valid snomed codes and fully specified name we require using the reference data type_id, and making sure valid snomed code
 
 
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
                   CodeProcAndProcStatus,  
                   s1.Term AS Der_SNoMEDProcTerm
                   
 FROM              $db_output.EIP_Pre_Proc_Interventions_v2 i
 LEFT JOIN         $db_output.SCT_Concepts_FSN s1 
                   ON i.Der_SNoMEDProcCode = s1.ID              
 LEFT JOIN         $db_output.SCT_Concepts_FSN s3 
                   ON i.CodeObs = s3.ID               

# COMMAND ----------

# DBTITLE 1,Listing relevant snomed codes for EIP
 %sql
 
 
 --Lists all the NICE concordant snomed codes. The contact data needs to be between the referral start date and the rp_enddate, and joined back on with record number. 
 --The EIP referral can include contacts from other referrals within the same provider for the same person for the same month period. Only Contacts with recorded valid SNOMED codes will be brought through
 
 
 DROP TABLE IF EXISTS $db_output.EIP_Procedures_v2;
 CREATE TABLE         $db_output.EIP_Procedures_v2 USING DELTA AS
 
 SELECT              r.Person_ID,
                     r.UniqServReqID,
                     r.UniqMonthID,
                     r.RecordNumber,
                     c.Der_InterventionUniqID,
                     c.CodeProcAndProcStatus,
                     c.Der_SNoMEDProcCode,
                     c.CodeObs,
                     COALESCE(c.Der_SNoMEDProcTerm, c.Der_SNoMEDObsTerm) as Der_SNoMEDTerm,               
                     CASE WHEN c.Der_SNoMEDProcCode IN ---Checked this list against EIP Recording and Reporting Document from Jan 2019
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
 	'1094331000000103', -- 'Referral for combined healthy eating and physical education programme'
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
 	'1097201000000108', -- 'Referral for carer focused education and support programme'
 
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

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Procedures_Agg;
 CREATE TABLE         $db_output.EIP_Procedures_Agg USING DELTA AS
 
 ---This table aggregates every contact for each uniqservreqid for every month for each provider. 
 ---As only contacts with a valid and recorded SNOMED-CT code are in Procedures_v2 where contact was
 ---between the referral start date and the rp_enddate, we can use a count of this table to 
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
 CREATE TABLE         $db_output.EIP_Activity_Agg USING DELTA AS
 
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
 CREATE TABLE         $db_output.EIP_Activity_Cumulative USING DELTA AS
 
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

# DBTITLE 1,Master prep table
 %sql
 
 --Pulls together final preperation table
 
 DROP TABLE IF EXISTS $db_output.EIP_Master;
 CREATE TABLE         $db_output.EIP_Master USING DELTA AS
 
 SELECT          r.UniqMonthID,
                 r.OrgIDProv,
                 r.RecordNumber,
                 r.UniqServReqID,
                 r.person_id, 
                 cu.Der_CumulativeContacts,
                 p.AnySNoMED,
                 p.NICESNoMED,
                 coalesce(od.NAME, 'UNKNOWN') as PROVIDER_NAME,
                 coalesce(ccg.org_code,'UNKNOWN') as CCG_CODE,
                 coalesce(ccg.name,'UNKNOWN') as CCG_NAME,
                 coalesce(stpre.STP_CODE,'UNKNOWN') as STP_CODE,
                 coalesce(stpre.STP_NAME,'UNKNOWN') as STP_NAME,
                 coalesce(stpre.REGION_CODE,'UNKNOWN') as REGION_CODE,
                 coalesce(stpre.REGION_NAME,'UNKNOWN') as REGION_NAME,
                 -- get caseload measures
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL THEN 1 ELSE 0 END AS Open_Referrals,
 	            CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND cu.Der_CumulativeContacts >=1 THEN 1 ELSE 0 END AS EIP_Caseload, ---EIP68
                 -- get aggregate SNoMED measures
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND p.AnySNoMED > 0 THEN 1 ELSE 0 END AS RefWithAnySNoMED, ---EIP69a
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND p.NICESNoMED > 0 THEN 1 ELSE 0 END AS RefWithNICESNoMED ---EIP6b
 
 FROM            $db_output.EIP_Refs_v2 r 
 LEFT JOIN       $db_output.EIP_Procedures_Agg p ON r.RecordNumber = p.RecordNumber AND r.UniqServReqID = p.UniqServReqID
 LEFT JOIN       $db_output.EIP_Activity_Cumulative cu ON r.RecordNumber = cu.RecordNumber AND r.UniqServReqID = cu.UniqServReqID
 LEFT JOIN       global_temp.RD_CCG_LATEST ccg --get the latest ccg reference data
                 ON r.OrgIDCCGRes = ccg.org_code
 LEFT JOIN       $db_output.EIP_STP_MAPPING stpre
                 ON ccg.org_code = stpre.CCG_CODE
 LEFT JOIN       $db_output.EIP_ORG_DAILY1 od
                 ON r.OrgIDProv = od.ORG_CODE
 WHERE           (ServDischDate IS NULL AND ReferRejectionDate IS NULL)            
                 AND r.UniqMonthID = '$end_month_id'

# COMMAND ----------

 %sql
 ---Final data check
 select sum(Open_Referrals),sum(EIP_Caseload),sum(RefWithAnySNoMED),sum(RefWithNICESNoMED) from $db_output.EIP_Master

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
   MEASURE_ID string,
   MEASURE_NAME string,
   MEASURE_VALUE string
 )
 USING DELTA

# COMMAND ----------

# DBTITLE 1,Insert Caseload Breakdowns
 %sql
 INSERT INTO $db_output.csv_output_unsup
 --England breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS MEASURE_ID,
   'EIP Caseload (Number of Referrals open to EIP services with atleast one attended contact at the end of the reporting period)' AS MEASURE_NAME,
   sum(EIP_Caseload) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS PRIMARY_LEVEL,
   PROVIDER_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS MEASURE_ID,
   'EIP Caseload (Number of Referrals open to EIP services with atleast one attended contact at the end of the reporting period)' AS MEASURE_NAME,
   sum(EIP_Caseload) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 GROUP BY  OrgIDProv, PROVIDER_NAME
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN, -- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency HL 25/01/22
   CCG_CODE AS PRIMARY_LEVEL,
   CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS MEASURE_ID,
   'EIP Caseload (Number of Referrals open to EIP services with atleast one attended contact at the end of the reporting period)' AS MEASURE_NAME,
   sum(EIP_Caseload) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS PRIMARY_LEVEL,
   REGION_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS MEASURE_ID,
   'EIP Caseload (Number of Referrals open to EIP services with atleast one attended contact at the end of the reporting period)' AS MEASURE_NAME,
    sum(EIP_Caseload) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP' AS BREAKDOWN,
   STP_CODE AS PRIMARY_LEVEL,
   STP_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP68' AS MEASURE_ID,
   'EIP Caseload (Number of Referrals open to EIP services with atleast one attended contact at the end of the reporting period)' AS MEASURE_NAME,
    sum(EIP_Caseload) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

# DBTITLE 1,Insert AnySNoMED Breakdowns
 %sql
 INSERT INTO $db_output.csv_output_unsup
 --England breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS MEASURE_ID,
   'Number of open Referrals with any valid SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithAnySNoMED) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS PRIMARY_LEVEL,
   PROVIDER_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS MEASURE_ID,
   'Number of open Referrals with any valid SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithAnySNoMED) AS MEASURE_VALUE  
 FROM      $db_output.EIP_Master
 GROUP BY  OrgIDProv, PROVIDER_NAME
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN, -- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency HL 25/01/22
   CCG_CODE AS PRIMARY_LEVEL,
   CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS MEASURE_ID,
   'Number of open Referrals with any valid SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithAnySNoMED) AS MEASURE_VALUE   
 FROM      $db_output.EIP_Master
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS PRIMARY_LEVEL,
   REGION_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS MEASURE_ID,
   'Number of open Referrals with any valid SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithAnySNoMED) AS MEASURE_VALUE   
 FROM      $db_output.EIP_Master
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP' AS BREAKDOWN,
   STP_CODE AS PRIMARY_LEVEL,
   STP_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69a' AS MEASURE_ID,
   'Number of open Referrals with any valid SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithAnySNoMED) AS MEASURE_VALUE   
 FROM      $db_output.EIP_Master
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

# DBTITLE 1,Insert NICESNoMED breakdowns
 %sql
 INSERT INTO $db_output.csv_output_unsup
 --England breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS PRIMARY_LEVEL,
   'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS MEASURE_ID,
   'Number of open Referrals with any NICE concordant EIP SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithNICESNoMED) AS MEASURE_VALUE   
 FROM      $db_output.EIP_Master
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS PRIMARY_LEVEL,
   PROVIDER_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS MEASURE_ID,
   'Number of open Referrals with any NICE concordant EIP SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithNICESNoMED) AS MEASURE_VALUE 
 FROM      $db_output.EIP_Master
 GROUP BY  OrgIDProv, PROVIDER_NAME
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN,-- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency HL 25/01/22
   CCG_CODE AS PRIMARY_LEVEL,
   CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS MEASURE_ID,
   'Number of open Referrals with any NICE concordant EIP SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithNICESNoMED) AS MEASURE_VALUE    
 FROM      $db_output.EIP_Master
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS PRIMARY_LEVEL,
   REGION_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS MEASURE_ID,
   'Number of open Referrals with any NICE concordant EIP SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithNICESNoMED) AS MEASURE_VALUE    
 FROM      $db_output.EIP_Master
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate_1m' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP' AS BREAKDOWN,
   STP_CODE AS PRIMARY_LEVEL,
   STP_NAME AS PRIMARY_LEVEL_DESCRIPTION,
   'NULL' AS SECONDARY_LEVEL,
   'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
   'EIP69b' AS MEASURE_ID,
   'Number of open Referrals with any NICE concordant EIP SNOMED-CT activity submitted that were linked to an EIP team in the reporting period' AS MEASURE_NAME,
   sum(RefWithNICESNoMED) AS MEASURE_VALUE   
 FROM      $db_output.EIP_Master
 GROUP BY  STP_CODE, STP_NAME;
 
 OPTIMIZE $db_output.csv_output_unsup

# COMMAND ----------

df = spark.table(f"{db_output}.csv_output_unsup")
none_df = df.na.replace("NULL", "NONE")
spark.sql(f"DROP TABLE IF EXISTS {db_output}.eip_csv_output_unsupp")
none_df.write.saveAsTable(f"{db_output}.eip_csv_output_unsupp")

# COMMAND ----------

 %sql
 create or replace global temporary view final_eip_unsupp_output as
 SELECT 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 MEASURE_ID,
 MEASURE_NAME,
 MEASURE_VALUE   
 FROM $db_output.eip_csv_output_unsupp;
 
 select * from global_temp.final_eip_unsupp_output

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.csv_output_raw;
 CREATE TABLE         $db_output.csv_output_raw USING DELTA AS
 SELECT * FROM global_temp.final_eip_unsupp_output

# COMMAND ----------

# DBTITLE 1,Download Final Raw Output
 %sql
 SELECT * FROM $db_output.csv_output_raw order by breakdown, primary_level, MEASURE_ID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.csv_output_sup;
 CREATE TABLE         $db_output.csv_output_sup USING DELTA AS
 SELECT 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 PRIMARY_LEVEL,
 PRIMARY_LEVEL_DESCRIPTION,
 SECONDARY_LEVEL,
 SECONDARY_LEVEL_DESCRIPTION,
 MEASURE_ID,
 MEASURE_NAME,
 MEASURE_VALUE     
 FROM global_temp.final_eip_unsupp_output
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
 MEASURE_ID,
 MEASURE_NAME,
 cast(case when cast(MEASURE_value as float) < 5 then '*' else cast(round(MEASURE_value/5,0)*5 as int) end as string) as  MEASURE_VALUE
 FROM global_temp.final_eip_unsupp_output
 WHERE BREAKDOWN != 'England'

# COMMAND ----------

# DBTITLE 1,Download Final Suppressed Output
 %sql
 SELECT * FROM $db_output.csv_output_sup order by breakdown, primary_level, MEASURE_ID

# COMMAND ----------

# DBTITLE 1,Test for Automated Sense Checks
import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "csv_output_raw",
  "suppressed_table": "csv_output_sup"
}))

# COMMAND ----------

 %md 
 
 #### [Go back to top](https://db.core.data.digital.nhs.uk/#notebook/3540351/command/4792336)