# Databricks notebook source
startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $db_source.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $db_source.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $db_source.mhs000header order by Uniqmonthid").collect()]

dbutils.widgets.dropdown("rp_startdate", "2021-03-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
dbutils.widgets.dropdown("month_id", "1452", monthid)
dbutils.widgets.text("status","Provisional")
dbutils.widgets.text("db_output","$user_id")
dbutils.widgets.text("db_source","$mhsds_database")
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
params = {'rp_startdate': str(rp_startdate), 'rp_enddate': str(rp_enddate), 'month_id': month_id, 'db_output': db_output, 'db_source': db_source}
print(params)

# COMMAND ----------

# DBTITLE 1,All EIP Direct and Attended Contact and related SNOMED interventions
 %sql
 DROP TABLE IF EXISTS $db_output.EIP_Pre_Proc_Interventions_v3;
 CREATE TABLE         $db_output.EIP_Pre_Proc_Interventions_v3 AS
 
 SELECT                ca.RecordNumber,
                       ca.Person_ID,
                       ca.UniqMonthID,
                       ca.UniqServReqID,
                       ca.UniqCareContID,
                       cc.CareContDate AS Der_ContactDate,
                       ca.UniqCareActID,
                       ca.MHS202UniqID as Der_InterventionUniqID,
                       ca.CodeProcAndProcStatus,
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
 AND                   cc.AttendOrDNACode IN ('5','6') AND (((cc.ConsMechanismMH NOT IN ('05', '06') and cc.UniqMonthID < '1459') OR (cc.ConsMechanismMH NOT IN ('05', '09') and cc.UniqMonthID >= '1459')) OR cc.OrgIDProv = 'DFC' AND ((cc.ConsMechanismMH IN ('05', '06') and cc.UniqMonthID < '1459') OR (cc.ConsMechanismMH IN ('05', '09') and cc.UniqMonthID >= '1459')))
 
 --/*** v5 ConsMediumUsed replaced by ConsMechanismMH and code list change from Oct 2021data- '06' changes to '09'-  AM-Dec 31 2021 ***/
 
 --AND                   cc.AttendOrDNACode IN ('5','6') AND (cc.ConsMechanismMH NOT IN ('05','06') OR cc.OrgIDProv = 'DFC' AND cc.ConsMechanismMH IN ('05','06')) /*** ConsMediumUsed changed to ConsMechanismMH GF ***/

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW EIP_Interventions_v3 AS
 
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
                   
 FROM              $db_output.EIP_Pre_Proc_Interventions_v3 i
 LEFT JOIN         $db_output.SCT_Concepts_FSN s1 
                   ON i.Der_SNoMEDProcCode = s1.ID               
 LEFT JOIN         $db_output.SCT_Concepts_FSN s3 
                   ON i.CodeObs = s3.ID

# COMMAND ----------

# DBTITLE 1,Getting only attended and direct contacts with SNOMED related to the EIP Referral
 %sql
 --CREATE OR REPLACE GLOBAL TEMPORARY VIEW Procedures AS
 DROP TABLE IF EXISTS $db_output.EIP_Procedures_v3;
 CREATE TABLE         $db_output.EIP_Procedures_v3 AS
 
 SELECT              r.Person_ID,
                     r.UniqServReqID,
                     r.UniqMonthID,
                     r.RecordNumber,
                     c.Der_InterventionUniqID,
                     c.CodeProcAndProcStatus,
                     c.Der_SNoMEDProcCode,
                     c.CodeObs,
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
 INNER JOIN        global_temp.EIP_Interventions_v3 c ON r.recordnumber = c.recordnumber AND r.UniqServReqID = c.UniqServReqID
                   ---joining on record number so will be also bringing in activity for the person in the month which may not be associated with EIP referral
                  AND COALESCE(c.Der_SNoMEDProcTerm, c.Der_SNoMEDObsTerm) IS NOT NULL 
                  AND c.Der_ContactDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate, '$rp_enddate') AND r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL

# COMMAND ----------

import pyspark.sql.functions as py
from pyspark.sql.functions import regexp_replace
proc = spark.table(f"{db_output}.EIP_Procedures_v3") #spark dataframe of all Procedures ever submitted relating to EIP referrals ServTypeReftoMH = A14
proc = proc.withColumn("Der_CodeProcAndProcStatus", regexp_replace("CodeProcAndProcStatus", "\r\n", "")) #replaces \r\n in the erroneous SNOMED Codes
split_colon = py.split(proc['Der_CodeProcAndProcStatus'], ':') #splits CodeProcAndProcStatus into list items where there is a :
split_equals = py.split(proc['Der_CodeProcAndProcStatus'], '=') #splits CodeProcAndProcStatus into list items where there is a =
proc = proc.withColumn("Code1", split_colon.getItem(0)) #gets first code before : as Code 1 Column
proc = proc.withColumn("CodeX", split_colon.getItem(1)) #gets second code after : (will contain code with equals sign too) as Code X Column
proc = proc.withColumn("Code2", py.split(proc['CodeX'], '=').getItem(0)) #gets first item in new Code X column before = as Code 2 Column
proc = proc.withColumn("Code3", split_equals.getItem(1)) #gets code after = as Code 3 Column
proc.createOrReplaceTempView("EIP_PRE_PROCEDURE")

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW EIP_Procedure_All_Terms_v2 AS
 ---Gets the Fully Specified Name for Procedure Code using SNOMED REF
 ---Gets Preferred Term for Code 2 and Code 3 using SNOMED REF
 SELECT DISTINCT   Der_CodeProcAndProcStatus,
                   Code1,
                   s1.Term as Code1Term, 
                   Code2,
                   s2.Term as Code2Term,
                   Code3,
                   s3.Term as Code3Term,
                   CodeObs,
                   s4.Term as CodeObsTerm
 FROM              EIP_PRE_PROCEDURE i
 LEFT JOIN         $db_output.SCT_Concepts_FSN s1 
                   ON i.Code1 = s1.ID          
 LEFT JOIN         $db_output.SCT_Concepts_PrefTerm s2
                   ON i.Code2 = s2.ID
 LEFT JOIN         $db_output.SCT_Concepts_PrefTerm s3
                   ON i.Code3 = s3.ID 
 LEFT JOIN         $db_output.SCT_Concepts_FSN s4 
                   ON i.CodeObs = s4.ID                  

# COMMAND ----------

 %sql
 create or replace global temporary view Post_Coord_SNOMED_Terms as
 ---Combines the terms of Code 1 and Code 3 (where possible else Term of Code 1)
 select distinct COALESCE(Der_CodeProcAndProcStatus, CodeObs) as Der_CodeProcAndProcStatus,
 CASE WHEN (Code1 is not null and Code2 is not null and Code3 is not null) 
      THEN CONCAT(Code1Term, ': ', Code3Term)
      WHEN (Code1 is not null and Code2 is null and Code3 is null)
      THEN Code1Term
      WHEN (Code1 is null and Code2 is null and Code3 is null) and CodeObs is not null
      THEN CodeObsTerm
      ELSE Code1Term END AS Der_Term
 from global_temp.EIP_Procedure_All_Terms_v2

# COMMAND ----------

 %sql
 create or replace global temporary view Pre_SNOMED_Dash as
 SELECT
     r.UniqMonthID,
     r.Person_ID,
     r.UniqServReqID,
     r.OrgIDProv,
     a.Der_InterventionUniqID,
     a.CodeProcAndProcStatus,
     a.Der_CodeProcAndProcStatus,
     coord.Der_Term as SNOMED_Proc_Term, ---Gets derived SNOMED Term for Der_CodeProcAndProcStatus
     a.CodeObs,
     coord2.Der_Term as SNOMED_Obs_Term, ---Gets SNOMED Term for CodeObs
 	CASE WHEN a.Intervention_type is null then 'No SNOMED Intervention'
          WHEN a.Intervention_type = 'Other' then 'General SNOMED'
          ELSE a.Intervention_type end as Intervention_type,
     CASE WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) IN ('718026005', '1097161000000100') THEN 'Cognitive behavioural therapy for psychosis (CBTp)'
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) IN ('985451000000105', '859411000000105') THEN 'Family interventions (FI)'
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) = '723948002' THEN 'Clozapine'
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) IN ('196771000000101', '443781008', '698094009', '171222001', '43396009', '271062006', '271061004', '121868005', '17888004', '166842003', '763243004') 
          THEN 'Physical health assessments and healthy lifestyle promotion'
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) IN ('225323000', '710081004', '871661000000106', '715282001', '1094331000000103', '281078001', '425014005', '1099141000000106', '201521000000104', '699826006', '718361005', '408289007', '1097171000000107', '1090411000000105', '1097181000000109', '308116003', '1099151000000109', '385804009', '1090411000000105', '1098021000000108', '1097191000000106', '134350008')
          THEN 'Physical health interventions' ---NOTE IN DOCUMENT THERE ARE A FEW PLACEHOLDER SNOMED CODES FOR THIS GROUP SO THESE WILL NEED TO BE LOOKED AT
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) IN ('183339004', '415271004', '70082004', '18781004', '335891000000102', '1098051000000103', '1098041000000101', '1082621000000104', '1082611000000105')
          THEN 'Education and employment support'
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) IN ('726052009', '1097201000000108') THEN 'Carer-focused education and support'
          WHEN COALESCE(a.Der_SNoMEDProcCode, a.CodeObs) is null then 'No SNOMED Intervention'
          ELSE 'Non NICE-recommended EIP intervention group' END AS Intervention_group
     
 FROM            $db_output.EIP_Refs_v2 r 
 LEFT JOIN       EIP_PRE_PROCEDURE a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber
 ---LEFT JOIN       $db_output.EIP_Activity_Cumulative cu ON r.RecordNumber = cu.RecordNumber AND r.UniqServReqID = cu.UniqServReqID
 LEFT JOIN       global_temp.Post_Coord_SNOMED_Terms coord ON a.Der_CodeProcAndProcStatus = coord.Der_CodeProcAndProcStatus
 LEFT JOIN       global_temp.Post_Coord_SNOMED_Terms coord2 ON a.CodeObs = coord2.Der_CodeProcAndProcStatus
 WHERE           (ServDischDate IS NULL AND ReferRejectionDate IS NULL)            
                 AND r.UniqMonthID = '$month_id'

# COMMAND ----------

 %sql
 create or replace global temporary view snomed_dash_unsupp_output as
 select 
 UniqMonthID,
 COALESCE(Der_CodeProcAndProcStatus, CodeObs) as CodeProcAndProcStatus, --Puts Der_CodeProcAndProcStatus and CodeObs into a singular column
 COALESCE(SNOMED_Proc_Term, SNOMED_Obs_Term) as SNOMED_Term, --Puts SNOMED_Proc_Term and SNOMED_Obs_Term into a singular column
 Intervention_type,
 Intervention_Group,
 'England' as OrgIDProv,
 COUNT(DISTINCT UniqServReqID) as Number_of_open_EIP_referrals,
 -- COUNT(DISTINCT CASE WHEN Der_InterventionUniqID is not null THEN UniqServReqID ELSE null end) as Number_of_EIP_Caseload_referrals,
 COUNT(Der_InterventionUniqID) as Number_of_SNOMED_interventions,
 -- COUNT(DISTINCT CASE WHEN Intervention_Type = 'NICE concordant' THEN Der_InterventionUniqID ELSE null end) as Number_of_NICE_Concordant_SNOMED_interventions
 COUNT(DISTINCT OrgIDProv) as Number_of_Providers_Using_SNOMED_Code
 FROM global_temp.Pre_SNOMED_Dash
 ---WHERE CodeProcAndProcStatus is not null
 GROUP BY 
 UniqMonthID,
 COALESCE(Der_CodeProcAndProcStatus, CodeObs),
 COALESCE(SNOMED_Proc_Term, SNOMED_Obs_Term),
 Intervention_type,
 Intervention_Group

# COMMAND ----------

 %sql
 create or replace global temporary view snomed_dash_england_output as
 select
 UniqMonthID,
 COALESCE(CodeProcAndProcStatus, 'No SNOMED Intervention') as CodeProcAndProcStatus,
 COALESCE(SNOMED_Term, 'No SNOMED Intervention') as SNOMED_Term,
 Intervention_type,
 Intervention_Group,
 OrgIDProv,
 Number_of_open_EIP_referrals,
 Number_of_SNOMED_interventions,
 Number_of_Providers_Using_SNOMED_Code
 from global_temp.snomed_dash_unsupp_output

# COMMAND ----------

 %sql
 select * from global_temp.Pre_SNOMED_Dash order by UniqServReqID

# COMMAND ----------

 %sql
 select * from EIP_PRE_PROCEDURE
 where UniqMonthID = 1452
 order by UniqServReqID, Der_InterventionUniqID

# COMMAND ----------

 %sql
 select * from global_temp.snomed_dash_england_output order by Number_of_open_EIP_referrals desc

# COMMAND ----------

 %sql
 select sum(Number_of_SNOMED_interventions) from global_temp.snomed_dash_england_output

# COMMAND ----------

 %sql
 select count(Der_InterventionUniqID), count(distinct Der_InterventionUniqID) from EIP_PRE_PROCEDURE where UniqMonthID = 1452

# COMMAND ----------

 %sql
 select 
 UniqMonthID,
 COALESCE(Der_CodeProcAndProcStatus, CodeObs) as CodeProcAndProcStatus,
 COALESCE(SNOMED_Proc_Term, SNOMED_Obs_Term) as SNOMED_Term,
 Intervention_type,
 Intervention_Group,
 OrgIDProv,
 COUNT(DISTINCT UniqServReqID) as Number_of_open_EIP_referrals,
 -- COUNT(DISTINCT CASE WHEN Der_InterventionUniqID is not null THEN UniqServReqID ELSE null end) as Number_of_EIP_Caseload_referrals,
 COUNT(DISTINCT Der_InterventionUniqID) as Number_of_SNOMED_interventions 
 -- COUNT(DISTINCT CASE WHEN Intervention_Type = 'NICE concordant' THEN Der_InterventionUniqID ELSE null end) as Number_of_NICE_Concordant_SNOMED_interventions
 FROM global_temp.Pre_SNOMED_Dash
 WHERE CodeProcAndProcStatus is not null
 GROUP BY 
 UniqMonthID,
 COALESCE(Der_CodeProcAndProcStatus, CodeObs),
 COALESCE(SNOMED_Proc_Term, SNOMED_Obs_Term),
 Intervention_type,
 Intervention_Group,
 OrgIDProv

# COMMAND ----------

 %sql
 select * from global_temp.snomed_dash_unsupp_output --where Number_of_open_EIP_referrals <> Number_of_EIP_Caseload_referrals

# COMMAND ----------

