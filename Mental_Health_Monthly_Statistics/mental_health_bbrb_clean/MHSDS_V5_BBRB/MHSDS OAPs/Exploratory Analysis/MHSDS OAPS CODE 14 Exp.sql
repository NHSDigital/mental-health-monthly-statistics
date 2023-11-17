-- Databricks notebook source
-- --Referral table contains ReasonOAT column which says if a referral is an OAP or not

-- select uniqmonthid, count(Person_ID) as OAP, count(case when ReasonOAT=10 then Person_ID else null end) as InappropriateOAP
-- from $reference_data.MHS101Referral
-- where ReasonOAT is not null
--   and ReferralrequestReceivedDate <= ReportingPeriodEndDate
--   and (ServDischDate is null or ServDischDate > ReportingPeriodStartDate)
-- group by uniqmonthid
-- order by uniqmonthid

-- COMMAND ----------

-- select *
-- from $reference_data.MHS101Referral
-- where ReasonOAT = 10

-- COMMAND ----------

create or replace temporary view HospProvSpell as 

with HospProvSpell_ordered
as
(
  select a.*, b.ReportingPeriodStartDate, b.ReportingPeriodEndDate
    ,row_number() over(partition by a.uniqmonthid, HospProvSpellID order by StartDateHospProvSpell desc, case when DischDateHospProvSpell is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS501HospProvSpell as a
  inner join (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as b on a.UniqMonthID = b.UniqMonthID
)   
  
select *
from HospProvSpell_ordered as a
where stay_order=1
  and a.StartDateHospProvSpell between a.ReportingPeriodStartDate and a.ReportingPeriodEndDate
--  and (a.DischDateHospProvSpell is null or a.DischDateHospProvSpell >= a.ReportingPeriodStartDate);

-- COMMAND ----------

create or replace temporary view referrals as 

with referrals_ordered
as
(
  select a.*, b.ReportingPeriodStartDate, b.ReportingPeriodEndDate
    ,row_number() over(partition by a.uniqmonthid, ServiceRequestId order by ReferralrequestReceivedDate desc, case when ServDischDate is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS101Referral as a
  inner join (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as b on a.UniqMonthID = b.UniqMonthID
)   
  
select *
from referrals_ordered as a
where stay_order=1
  and a.ReferralrequestReceivedDate between a.ReportingPeriodStartDate and a.ReportingPeriodEndDate
--  and (a.ServDischDate is null or a.ServDischDate >= a.ReportingPeriodStartDate);

-- COMMAND ----------

select a.*
    ,row_number() over(partition by uniqmonthid, Person_ID order by RecordStartDate desc, case when RecordEndDate is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS001MPI as a

-- COMMAND ----------

create or replace temporary view patients as 

with patients_ordered
as
(
  select a.*
    ,row_number() over(partition by uniqmonthid, Person_ID order by RecordStartDate desc, case when RecordEndDate is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS001MPI as a
)   
  
select *
from patients_ordered as a
where stay_order=1
--   and a.RecordStartDate between a.ReportingPeriodStartDate and a.ReportingPeriodEndDate

-- COMMAND ----------

select b.ReportingPeriodStartDate
        , b.uniqmonthid
        , count(*) as records
        , COUNT(CASE WHEN DefaultPostcode is not null THEN b.ServiceRequestId ELSE null END) as defaultpc_records
        , COUNT(CASE WHEN DefaultPostcode is not null THEN b.ServiceRequestId ELSE null END)/count(*) as defaultpc_records_pc
        , count(CASE WHEN c.ReasonOAT is not null THEN b.ServiceRequestId ELSE null END) as records_OAPs
        , COUNT(CASE WHEN c.ReasonOAT is not null and DefaultPostcode is not null THEN b.ServiceRequestId ELSE null END) as defaultpc_records_OAPS
        , COUNT(CASE WHEN c.ReasonOAT is not null and DefaultPostcode is not null THEN b.ServiceRequestId ELSE null END)/count(CASE WHEN c.ReasonOAT is not null THEN b.ServiceRequestId ELSE null END) as defaultpc_records_OAPS_pc
        , count(CASE WHEN c.ReasonOAT = 10 THEN b.ServiceRequestId ELSE null END) as records_OAPs_Inappropriate
        , COUNT(CASE WHEN c.ReasonOAT = 10 and DefaultPostcode is not null THEN b.ServiceRequestId ELSE null END) as defaultpc_records_OAPS_Inappropriate
        , COUNT(CASE WHEN c.ReasonOAT = 10 and DefaultPostcode is not null THEN b.ServiceRequestId ELSE null END)/count(CASE WHEN c.ReasonOAT = 10 THEN b.ServiceRequestId ELSE null END) as defaultpc_records_OAPS_Inappropriate_pc
from HospProvSpell as b
inner join referrals as c on b.ServiceRequestId = c.ServiceRequestId  and b.uniqmonthid = c.uniqmonthid
inner join patients as p on c.Person_ID = p.Person_ID  and b.uniqmonthid = p.uniqmonthid
group by b.ReportingPeriodStartDate, b.uniqmonthid
order by b.ReportingPeriodStartDate desc, b.uniqmonthid
