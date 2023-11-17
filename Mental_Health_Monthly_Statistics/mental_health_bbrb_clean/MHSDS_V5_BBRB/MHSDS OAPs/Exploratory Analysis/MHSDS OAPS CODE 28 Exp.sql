-- Databricks notebook source
--Referral table contains ReasonOAT column which says if a referral is an OAP or not
--Join on Header table to get submission date to see 

select a.*, b.SubmissionDate
from $reference_data.MHS101Referral as a
inner join
  (
  select distinct UniqSubmissionID, DateTimeDatSetCreate, date(DateTimeDatSetCreate) as SubmissionDate
  from $reference_data.MHS000header
  ) as b on a.UniqSubmissionID = b.UniqSubmissionID
where ReasonOAT = 10
  and uniqmonthid = 1449

-- COMMAND ----------

select distinct UniqSubmissionID, DateTimeDatSetCreate, date(DateTimeDatSetCreate) as SubmissionDate
from $reference_data.MHS000header

-- COMMAND ----------

create or replace temporary view referrals as 

select a.*, b.SubmissionDate
from $reference_data.MHS101Referral as a
inner join
  (
  select distinct UniqSubmissionID, DateTimeDatSetCreate, date(DateTimeDatSetCreate) as SubmissionDate
  from $reference_data.MHS000header
  ) as b on a.UniqSubmissionID = b.UniqSubmissionID
where /*ReasonOAT = 10
  and */uniqmonthid = 1462

-- COMMAND ----------

create or replace temporary view OAPs_referrals as 

with referrals_ordered
as
(
  select a.*, b.ReportingPeriodStartDate, b.ReportingPeriodEndDate
    ,CASE WHEN SubmissionDate>='2021-03-01' THEN 0 ELSE 1 END as InclPub
    ,row_number() over(partition by a.uniqmonthid, ServiceRequestId, CASE WHEN SubmissionDate>'2021-02-28' THEN 0 ELSE 1 END
                        order by ReferralrequestReceivedDate desc, case when ServDischDate is null then 1 else 0 end desc, UniqSubmissionID desc) as stay_order
  from referrals as a
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
where uniqmonthid = 1463


-- COMMAND ----------

select ReportingPeriodStartDate
        , uniqmonthid
        , count(*) as records
        , SUM(InclPub) as published
        , count(CASE WHEN ReasonOAT = 10 THEN UniqServReqID ELSE null END) as Inappropriate_OAPs_records
        , SUM(CASE WHEN ReasonOAT = 10 THEN InclPub ELSE 0 END) as Inappropriate_OAPs_records_published
        , SUM(datediff(CASE WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE ServDischDate END,
                          CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
                               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                               WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
                               ELSE ServDischDate END)) as OAPs_Days
        , SUM(CASE WHEN ReasonOAT = 10 THEN
                 datediff(CASE WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE ServDischDate END,
                          CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
                               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                               WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
                               ELSE ServDischDate END)
                   ELSE 0 END) as Inappropriate_OAPs_Days
        , SUM(CASE WHEN InclPub = 1 THEN
                 datediff(CASE WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE ServDischDate END,
                          CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
                               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                               WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
                               ELSE ServDischDate END)
                   ELSE 0 END) as OAPs_Days_published
        , SUM(CASE WHEN ReasonOAT = 10 and InclPub = 1 THEN
                 datediff(CASE WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE ServDischDate END,
                          CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
                               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (ServDischDate is null or ServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                               WHEN ServDischDate is null or ServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
                               ELSE ServDischDate END)
                   ELSE 0 END) as Inappropriate_OAPs_Days_published
from OAPs_referrals
group by ReportingPeriodStartDate
        , uniqmonthid
order by ReportingPeriodStartDate
        , uniqmonthid