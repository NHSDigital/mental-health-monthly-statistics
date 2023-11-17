-- Databricks notebook source
--Referral table contains ReasonOAT column which says if a referral is an OAP or not

select uniqmonthid, Person_ID, count(*) as records
from $reference_data.MHS101Referral
where ReasonOAT = 10
group by uniqmonthid, Person_ID
order by uniqmonthid desc, records desc, Person_ID

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

create or replace temporary view OAPs_referrals as 

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
  and ReasonOAT is not null

-- COMMAND ----------

create or replace temporary view NonOverlapping_OAPs_referrals as 

with overlapping_referrals
as
(
  select FirstID, MAX(LateDate) NewDate
  from
      (
      select a.ServiceRequestId as FirstId, b.ServiceRequestId as LaterId, b.ReferralRequestReceivedDate as LateDate
      from OAPs_referrals as a
      inner join OAPs_referrals as b
         on a.Person_ID = b.Person_ID
        and a.uniqmonthid = b.uniqmonthid
      where a.ServiceRequestId != b.ServiceRequestId
        and a.ReferralRequestReceivedDate < b.ReferralRequestReceivedDate
        and (b.ReferralRequestReceivedDate < a.ServDischDate Or a.ServDischDate is null)
      )
  group by FirstID
)

select a.*
        , CASE WHEN b.NewDate is null THEN a.ServDischDate ELSE b.NewDate END as NewServDischDate
from OAPs_referrals a
left join overlapping_referrals b on a.ServiceRequestId = b.FirstId

-- COMMAND ----------

select *
        , CASE WHEN NewServDischDate is null or NewServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE NewServDischDate END as PeriodOAPEndDate
        , CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (NewServDischDate is null or NewServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (NewServDischDate is null or NewServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
               WHEN NewServDischDate is null or NewServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
               ELSE NewServDischDate END as PeriodOAPStartDate
from NonOverlapping_OAPs_referrals
where LEFT(OrgIDProv,3) = LEFT(OrgIDReferring,3)


-- COMMAND ----------

select *
from NonOverlapping_OAPs_referrals
where LEFT(OrgIDProv,3) = LEFT(OrgIDReferring,3)
  and uniqmonthid = 1450

-- COMMAND ----------

select ReportingPeriodStartDate
        , uniqmonthid
        , count(*) as records
        , count(CASE WHEN LEFT(OrgIDProv,3) != LEFT(OrgIDReferring,3) THEN UniqServReqID ELSE null END) as Exernal_OAPs_records
        , count(CASE WHEN ReasonOAT = 10 THEN UniqServReqID ELSE null END) as Inappropriate_OAPs_records
        , count(CASE WHEN ReasonOAT = 10 and LEFT(OrgIDProv,3) != LEFT(CASE WHEN OrgIDReferring is null THEN "ZZZ" ELSE OrgIDReferring END,3) THEN UniqServReqID ELSE null END) as Exernal_Inappropriate_OAPs_records
        , count(CASE WHEN ReasonOAT = 10 and LEFT(OrgIDProv,3) != LEFT(CASE WHEN OrgIDReferring is null THEN "ZZZ" ELSE OrgIDReferring END,3) THEN UniqServReqID ELSE null END)/
                  count(CASE WHEN ReasonOAT = 10 THEN UniqServReqID ELSE null END) as Exernal_Inappropriate_OAPs_records_pc
--         , COUNT(CASE WHEN NewServDischDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN UniqServReqID ELSE null END) as OAPs_Ended
--         , COUNT(CASE WHEN ReasonOAT = 10 and NewServDischDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN UniqServReqID ELSE null END) as Inappropriate_OAPs_Ended
--         , COUNT(CASE WHEN NewServDischDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN UniqServReqID ELSE null END)/
--                   count(*) as OAPs_Ended_pc
--         , COUNT(CASE WHEN ReasonOAT = 10 and NewServDischDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN UniqServReqID ELSE null END)/
--                   count(CASE WHEN ReasonOAT = 10 THEN UniqServReqID ELSE null END) as Inappropriate_OAPs_Ended_pc
from NonOverlapping_OAPs_referrals
group by ReportingPeriodStartDate
        , uniqmonthid
order by ReportingPeriodStartDate
        , uniqmonthid