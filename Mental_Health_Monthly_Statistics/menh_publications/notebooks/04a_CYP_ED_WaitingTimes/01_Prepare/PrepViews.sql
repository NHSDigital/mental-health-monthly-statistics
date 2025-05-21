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
 ccg_table=dbutils.widgets.get("ccg_table")
 print(ccg_table)
 assert ccg_table

-- COMMAND ----------

 %md
 101 DOESNT NEED CHANGING (MIGHT BE BETTER AS A PROPER TABLE THOUGH)

 Followed by (USE SAME METHODOLOGY AS MHS101 in 02_load_common_ref_data):
 MHS102 
 MHS501

 CCG METHODOLOGY FOR 12 MONTH ROLLING (IT EXISTS ALREADY FOR 3 MONTH ROLLING)


-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP1 
 %sql
 --Step1: Identify the referrals for CYP ED
 CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP1 AS
 SELECT  Distinct
 A.UniqServReqID,
     A.OrgIDProv,
     A.ClinRespPriorityType,
     CCG.IC_Rec_CCG,
     PrimReasonReferralMH,
     AgeServReferRecDate,
     A.ReferralRequestReceivedDate,
     A.ServDischDate,
     A.Person_ID,
     CASE WHEN vc_urgent.type IS NULL AND vc_routine.type = "include" THEN 'Routine'
          WHEN vc_routine.type IS NULL AND vc_urgent.type = "include" THEN 'Urgent'
          ELSE NULL END AS Priority_Type,
     A.UniqMonthID,
     A.RecordNumber
 -- LOOK AT THE POSSIBILITY OF CHANGING GLOBAL_TEMPS AND ADDING FUNCTIONALITY TO RUN 3 MONTH AND 12 MONTH RPS
 FROM global_temp.MHS101Referral_LATEST AS A
  LEFT OUTER JOIN global_temp.MHS001MPI_PATMRECINRP_FIX AS E
     ON A.Person_ID = E.Person_ID 
          AND A.UniqMonthID = E.UniqMonthID
 LEFT OUTER JOIN (SELECT m.Person_ID,
                     CCG.IC_Rec_CCG AS IC_Rec_CCG
                FROM global_temp.MHS001MPI_PATMRECINRP_FIX AS m
                INNER JOIN $ccg_table AS CCG             
                ON CCG.Person_ID = m.Person_ID) AS CCG
     ON CCG.Person_ID = E.Person_ID
 --ADDED JOIN ON PERSON AND REFERRAL TO REMOVE THOSE WHO ARE REFERRED TO SPOA OR MHSTS
 LEFT JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST S
     ON A.Person_ID = S.Person_ID and A.UniqServReqID = S.UniqServReqID and s.ServTeamTypeRefToMH IN ('A18','F01')
 --ADDED JOIN ON PERSON AND REFERRAL TO REMOVE THOSE WHO ARE AN INPATIENT AS PART OF THE REFERRAL
 LEFT JOIN global_temp.MHS501HospProvSpell_LATEST H
     ON A.Person_ID = S.Person_ID and A.UniqServReqID = S.UniqServReqID
     -- join added for uplift from v4.1 to v5
 LEFT JOIN $db_output.validcodes as vc
 --     join updated to evaluate validity at time of data rather than reporting month
     ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CYP_ED_WT' and vc.type = 'include' and A.ClinRespPriorityType = vc.ValidValue
     and A.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or A.UniqMonthID <= vc.LastMonth)
 --ADD INPATIENT AND SPOA / MHST TABLES TO REMOVE THOSE REFERRALS

 LEFT JOIN $db_output.validcodes as vc_routine
 ON vc_routine.tablename = 'mhs101referral' and vc_routine.field = 'ClinRespPriorityType' and vc_routine.Measure = 'ED87_90' and vc_routine.type = 'include' and a.ClinRespPriorityType = vc_routine.ValidValue 
 and a.UniqMonthID >= vc_routine.FirstMonth and (vc_routine.LastMonth is null or a.UniqMonthID <= vc_routine.LastMonth)

 LEFT JOIN $db_output.validcodes as vc_urgent
 ON vc_urgent.tablename = 'mhs101referral' and vc_urgent.field = 'ClinRespPriorityType' and vc_urgent.Measure = 'ED86_89' and vc_urgent.type = 'include' and a.ClinRespPriorityType = vc_urgent.ValidValue 
 and a.UniqMonthID >= vc_urgent.FirstMonth and (vc_urgent.LastMonth is null or a.UniqMonthID <= vc_urgent.LastMonth)

 WHERE A.ReferralRequestReceivedDate <= '$rp_enddate'
     And A.ReferralRequestReceivedDate >= '2016-01-01'  
     And vc.Measure is not null
     AND PrimReasonReferralMH = '12'
     AND (AgeServReferRecDate <= 18 AND AgeServReferRecDate >=0)
     AND (A.ServDischDate >= '$rp_startdate_run' or A.ServDischDate is null)
     AND H.UniqServReqID IS NULL
     AND S.UniqServReqID IS NULL
     

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP2
 %sql
 --Step2: For the above (STEP1) CYP-ED cohort identify the referrals which have care contact with ‘stop the clock’ SNOMED recorded in MHS201CareContact and MHS202
 CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP2 AS
 SELECT 
 step1.UniqServReqID,
     step1.OrgIDProv,
     step1.Person_ID,
     step1.ClinRespPriorityType,
     step1.ReferralRequestReceivedDate,
     step1.ServDischDate,
     cc.CareContDate,
     case when vc.ValidValue is not null then 1 else 0 end as IsConsMechanismMH,
     step1.Priority_Type,
     step1.UniqMonthID,                          -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
     step1.RecordNumber
 FROM  global_temp.CYP_ED_WT_STEP1 step1 

 INNER JOIN $db_source.MHS201CareContact cc
 ON step1.UniqServReqID = cc.UniqServReqID and step1.Person_ID = cc.Person_ID
 INNER JOIN $db_source.MHS202CareActivity ca
 ON cc.Person_ID = ca.Person_ID AND ca.UniqCareContID = cc.UniqCareContID AND ca.UniqMonthID = cc.UniqMonthID

 INNER JOIN $db_output.validcodes as vc
 --     join updated to evaluate validity at time of data rather than reporting month
   ON vc.tablename = 'mhs201carecontact' 
   and vc.field = 'ConsMechanismMH' 
   and vc.Measure = 'CYP_ED_WaitingTimes' 
   and vc.type = 'include' 
   and cc.ConsMechanismMH = vc.ValidValue 
   and cc.UniqMonthID >= vc.FirstMonth 
   and (vc.LastMonth is null or cc.UniqMonthID <= vc.LastMonth)
   
 INNER JOIN $db_output.validcodes as vcc
 --     join updated to evaluate validity at time of data rather than reporting month
   ON vcc.tablename = 'MHS202CareActivity' 
   and vcc.field = 'Procedure' 
   and vcc.Measure = 'CYP_ED_WaitingTimes' 
   and vcc.type = 'include' 
   and CASE
         WHEN CHARINDEX(':', ca.Procedure) > 0 
         THEN RTRIM(LEFT(ca.Procedure, CHARINDEX(':',ca.Procedure) -1))
         ELSE ca.Procedure        
         END = vcc.ValidValue 
   and ca.UniqMonthID >= vcc.FirstMonth 
   and (vcc.LastMonth is null or ca.UniqMonthID <= vcc.LastMonth)
   
 where cc.CareContDate >= ReferralRequestReceivedDate 
 and cc.CareContDate <= '$rp_enddate'
 and cc.AttendStatus in (5,6)
 --VALID CODES have been added to the table so INNER JOIN means we dont need the hardcoded list below...
 --and ca.Procedure  in  ('51484002', '1111811000000109', '443730003', '444175001', '718023002', '984421000000104', '1323681000000103','1362001000000104')
 ;

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP3_prep
--Referrals entering treatment in the RP = For the cohort from Step2, rank the care contacts for each referral and pick only the ones with first care contact within the reporting quarter
CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP3_prep as
select 
    UniqServReqID,
    OrgIDProv,
    Person_ID,
    ClinRespPriorityType,
    ServDischDate,
    CareContDate,
    ReferralRequestReceivedDate,
    Priority_Type,
    UniqMonthID,                          -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
    ROW_NUMBER() OVER (PARTITION BY UniqServReqID ORDER BY CareContDate ASC) as rnk 
FROM global_temp.CYP_ED_WT_STEP2;

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP3
--STEP 3 -  Cohort for which started treatment for an Eating Disorder for the first time in the reporting quarter
CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP3 as
select * from global_temp.CYP_ED_WT_STEP3_prep 
where rnk = 1
and (CareContDate BETWEEN '$rp_startdate_run' AND  '$rp_enddate');

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP4 - Step 3 plus waiting time in weeks
INSERT INTO $db_output.CYP_ED_WT_STEP4

SELECT 
    '$month_id' AS UniqMonthID,
    '$status' AS Status,
    UniqServReqID,
    OrgIDProv,
    Person_ID,
    ClinRespPriorityType,
    CareContDate,
    ReferralRequestReceivedDate,
    Priority_Type,
    DATEDIFF(CareContDate,ReferralRequestReceivedDate)/7 as waiting_time,
    DATEDIFF(CareContDate,ReferralRequestReceivedDate) as waiting_time_days,
    '$db_source' as SOURCE_DB,
    UniqMonthID AS SubmissionMonthID                -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
from global_temp.CYP_ED_WT_STEP3;

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP5
--STEP5 -  Identify the referrals still waiting for a treatment for eating disorder
CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP5 AS
SELECT 
    step1.UniqServReqID,
    step1.OrgIDProv,
    step1.Person_ID,
    step1.ClinRespPriorityType,
    step1.IC_Rec_CCG,
    step1.AgeServReferRecDate,
    step1.ReferralRequestReceivedDate,
    step1.ServDischDate,
    step1.Priority_Type,
    step1.UniqMonthID,
    step1.RecordNumber 
   -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
FROM  global_temp.CYP_ED_WT_STEP1 step1 
left outer join global_temp.CYP_ED_WT_STEP2 step2
on step1.UniqServReqID = step2.UniqServReqID
-- ADDED LEFT JOIN TO MPI BASED ON RECORDNUMBER TO EXCLUDE THOSE STILL WAITING AGE 19 AND OVER
left join $db_source.MHS001MPI M on step1.Person_ID = M.Person_ID and step1.RecordNumber = M.RecordNumber
where step2.UniqServReqID is null
and ((step1.ServDischDate is null or step1.ServDischDate > '$rp_enddate') AND step1.UniqMonthID = '$month_id')
--APPLIED AGE FILTER BASED ON AGE AT END OF MONTH
AND M.AgeRepPeriodEnd < 19;

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP6
INSERT INTO $db_output.CYP_ED_WT_STEP6
select 
   '$month_id' AS UniqMonthID,
    '$status' AS Status,
    UniqServReqID,
    OrgIDProv,
    Person_ID,
    ClinRespPriorityType,
    IC_Rec_CCG,
    ReferralRequestReceivedDate,
    ServDischDate,
    Priority_Type,
    DATEDIFF('$rp_enddate',ReferralRequestReceivedDate)/7 as waiting_time,
    DATEDIFF('$rp_enddate',ReferralRequestReceivedDate) as waiting_time_days,
    '$db_source' as SOURCE_DB,
    UniqMonthID AS SubmissionMonthID                -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
from global_temp.CYP_ED_WT_STEP5;

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP7
--STEP 7 -  Cohort which received their second contact for an Eating Disorder for the first time in the reporting quarter
CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP7 as
select * from global_temp.CYP_ED_WT_STEP3_prep 
where rnk = 2
and (CareContDate BETWEEN '$rp_startdate_run' AND  '$rp_enddate');

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP8
-- STEP 8 - Waiting time between first and second contact for an Eating Disorder, for referrals that have received a second contact for the first time, in the RP
-- Get first contact from step 3 prep (first contact can have taken place at any point, not just during the rp) and second contact from step 7 (received second contact in the RP)
-- Calculate waiting time between first and second contact

INSERT INTO $db_output.CYP_ED_WT_STEP8
SELECT 
    '$month_id' AS UniqMonthID,
    '$status' AS Status,
    c2.UniqServReqID,
    c2.OrgIDProv,
    c2.Person_ID,
    c2.ClinRespPriorityType,
    c1.CareContDate as FirstCareContDate,
    c2.CareContDate as SecondCareContDate,
    c2.ReferralRequestReceivedDate,
    c2.Priority_Type,
    DATEDIFF(c2.CareContDate,c1.CareContDate)/7 as waiting_time,
    DATEDIFF(c2.CareContDate,c1.CareContDate) as waiting_time_days,
    '$db_source' as SOURCE_DB,
    c2.UniqMonthID AS SubmissionMonthID                -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
FROM global_temp.CYP_ED_WT_STEP7 c2
INNER JOIN (SELECT * FROM global_temp.CYP_ED_WT_STEP3_prep WHERE rnk = 1) c1 ON c2.UniqServReqID = c1.UniqServReqID --join to get first contact for the referral