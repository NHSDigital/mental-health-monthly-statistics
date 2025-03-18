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
     A.UniqMonthID
 FROM global_temp.MHS101Referral_LATEST AS A
  LEFT OUTER JOIN global_temp.MHS001MPI_PATMRECINRP_FIX AS E
     ON A.Person_ID = E.Person_ID 
          AND A.UniqMonthID = E.UniqMonthID

 LEFT OUTER JOIN (SELECT m.Person_ID,
                     CCG.IC_Rec_CCG AS IC_Rec_CCG
                FROM global_temp.MHS001MPI_PATMRECINRP_FIX AS m
                INNER JOIN $db_output.MHS001_CCG_LATEST AS CCG             
                ON CCG.Person_ID = m.Person_ID) AS CCG
     ON CCG.Person_ID = E.Person_ID
     -- join added for uplift from v4.1 to v5
     LEFT JOIN $db_output.validcodes as vc
 --     join updated to evaluate validity at time of data rather than reporting month
     ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CYP_ED_WT' and vc.type = 'include' and A.ClinRespPriorityType = vc.ValidValue
     and A.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or A.UniqMonthID <= vc.LastMonth)
 WHERE A.ReferralRequestReceivedDate <= '$rp_enddate'
     And A.ReferralRequestReceivedDate >= '2016-01-01'  
     And vc.Measure is not null
     AND PrimReasonReferralMH = '12'
     AND (AgeServReferRecDate <= 18 AND AgeServReferRecDate >=0)
     AND (A.ServDischDate >= '$rp_startdate_quarterly' or A.ServDischDate is null)
     

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
     step1.UniqMonthID                          -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
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
   
 where cc.CareContDate >= ReferralRequestReceivedDate 
 and cc.CareContDate <= '$rp_enddate'
 and cc.AttendStatus in (5,6)
 and ca.Procedure  in  ('51484002', '1111811000000109', '443730003', '444175001', '718023002', '984421000000104');

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
    UniqMonthID,                          -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
    ROW_NUMBER() OVER (PARTITION BY UniqServReqID ORDER BY CareContDate ASC) as rnk 
FROM global_temp.CYP_ED_WT_STEP2;

-- COMMAND ----------

-- DBTITLE 1,CYP_ED_WT_STEP3
--STEP 3 -  Cohort for which started treatment for an Eating Disorder for the first time in the reporting quarter
CREATE OR REPLACE GLOBAL TEMP VIEW CYP_ED_WT_STEP3 as
select * from global_temp.CYP_ED_WT_STEP3_prep 
where rnk = 1
and (CareContDate BETWEEN '$rp_startdate_quarterly' AND  '$rp_enddate');

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
    DATEDIFF(CareContDate,ReferralRequestReceivedDate)/7 as waiting_time,
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
    step1.UniqMonthID                          -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
FROM  global_temp.CYP_ED_WT_STEP1 step1 
left outer join global_temp.CYP_ED_WT_STEP2 step2
on step1.UniqServReqID = step2.UniqServReqID
where step2.UniqServReqID is null
and ((step1.ServDischDate is null or step1.ServDischDate > '$rp_enddate') AND step1.UniqMonthID = '$month_id');

-- COMMAND ----------

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
    DATEDIFF('$rp_enddate',ReferralRequestReceivedDate)/7 as waiting_time,
    '$db_source' as SOURCE_DB,
    UniqMonthID AS SubmissionMonthID                -- ADDED TO ALLOW VALIDCODES TO COMPARE WITH SUBMISSION MONTH
from global_temp.CYP_ED_WT_STEP5;