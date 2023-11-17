-- Databricks notebook source
select UniqMonthID
        , OrgIDProv
        , COUNT(UniqSubmissionID) as Submissions
        , COUNT(CASE WHEN ReasonOAT is not null THEN UniqSubmissionID ELSE null END) as OAPs_Submissions
        , COUNT(CASE WHEN ReasonOAT = 10 THEN UniqSubmissionID ELSE null END) as Inappropriate_OAPs_Submissions
from $reference_data.MHS101Referral
where UniqMonthID = 1462
group by UniqMonthID, OrgIDProv
order by 4 desc

-- COMMAND ----------

create or replace temporary view referrals as 

with referrals_ordered
as
(
  select a.*, b.ReportingPeriodStartDate, b.ReportingPeriodEndDate, ONW.OATReason
    ,row_number() over(partition by a.uniqmonthid, a.ServiceRequestId order by a.ReferralrequestReceivedDate desc, case when a.ServDischDate is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS101Referral as a
  inner join (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as b on a.UniqMonthID = b.UniqMonthID
  left join $reference_data.MHS105OnwardReferral as ONW
     on a.ServiceRequestID = ONW.ServiceRequestID
    and a.Person_ID = ONW.Person_ID
    and a.Uniqmonthid = ONW.Uniqmonthid
    and (a.OrgIDProv = ONW.OrgIDReceiving
          or ONW.OrgIDProv = a.OrgIDReferring)
)   
  
select *
from referrals_ordered as a
where stay_order=1
  and a.ReferralrequestReceivedDate between a.ReportingPeriodStartDate and a.ReportingPeriodEndDate
--  and (a.ServDischDate is null or a.ServDischDate >= a.ReportingPeriodStartDate);

-- COMMAND ----------

create or replace temporary view provider_referrals as 

select UniqMonthID 
        , ReportingPeriodStartDate
        , LEFT(OrgIDProv,3) as OrgIDProv
        , LEFT(OrgIDReferring,3) as OrgIDReferring
        , COUNT(UniqSubmissionID) as Submissions
        , COUNT(CASE WHEN ReasonOAT is not null THEN UniqSubmissionID ELSE null END) as OAPs_Submissions
        , COUNT(CASE WHEN ReasonOAT = 10 THEN UniqSubmissionID ELSE null END) as Inappropriate_OAPs_Submissions
from referrals
--where UniqMonthID = 1450
group by UniqMonthID, ReportingPeriodStartDate, LEFT(OrgIDProv,3), LEFT(OrgIDReferring,3)
order by 6 desc

-- COMMAND ----------


--drop table $user_id.RefScopeList;
--create table $user_id.RefScopeList (OrgIDProv string);

--drop table timunderwood_100056.RefScopeList;
create table timunderwood_100056.RefScopeList (OrgIDProv string);


insert into timunderwood_100056.RefScopeList
values
('RTQ'),
('RTV'),
('RVN'),
('RRP'),
('RWX'),
('RXT'),
('TAJ'),
('TAD'),
('RT1'),
('TAF'),
('RV3'),
('RXA'),
('RJ8'),
('RYG'),
('RNN'),
('RXM'),
('RWV'),
('RDY'),
('RYK'),
('RWK'),
('RXV'),
('RWR'),
('RV9'),
('R1F'),
('RXY'),
('RW5'),
('RGD'),
('RT5'),
('RP7'),
('RW4'),
('RMY'),
('RAT'),
('RLY'),
('RP1'),
('RX4'),
('RHA'),
('RNU'),
('RPG'),
('RT2'),
('RXE'),
('TAH'),
('R1C'),
('RH5'),
('RV5'),
('RRE'),
('RQY'),
('RXG'),
('RW1'),
('RXX'),
('RX2'),
('RX3'),
('RKL'),
('R1A'),
('NQL'),
('RQ3'),
('R1L'),
('NR5')

-- COMMAND ----------

select *
from timunderwood_100056.RefScopeList as b
left join provider_referrals as a
    on a.OrgIDProv = b.OrgIDProv

-- COMMAND ----------

create or replace temporary view Months as 

select distinct UniqMonthID, ReportingPeriodStartDate
from  $reference_data.MHS000Header

-- COMMAND ----------

select b.UniqMonthID
        , b.ReportingPeriodStartDate
        ,COUNT(DISTINCT a.OrgIDProv) as InScopeOrg
        ,COUNT(DISTINCT c.OrgIDReferring) as SubmittingOrg
        ,COUNT(DISTINCT (CASE WHEN c.OAPs_Submissions > 0 THEN c.OrgIDReferring ELSE null END)) as SubmittingOrg_OAPs
        ,COUNT(DISTINCT (CASE WHEN c.Inappropriate_OAPs_Submissions > 0 THEN c.OrgIDReferring ELSE null END)) as SubmittingOrg_Inappropriate_OAPs
        ,COUNT(DISTINCT c.OrgIDReferring)/COUNT(DISTINCT a.OrgIDProv) as SubmittingOrg_perc
        ,COUNT(DISTINCT (CASE WHEN c.OAPs_Submissions > 0 THEN c.OrgIDReferring ELSE null END))/COUNT(DISTINCT a.OrgIDProv) as SubmittingOrg_OAPs_perc
        ,COUNT(DISTINCT (CASE WHEN c.Inappropriate_OAPs_Submissions > 0 THEN c.OrgIDReferring ELSE null END))/COUNT(DISTINCT a.OrgIDProv) as SubmittingOrg_Inappropriate_OAPs_perc
from timunderwood_100056.RefScopeList as a
cross join Months as b
left join provider_referrals as c
    on c.OrgIDReferring = a.OrgIDProv
   and c.UniqMonthID = b.UniqMonthID
group by b.UniqMonthID
        , b.ReportingPeriodStartDate
order by b.UniqMonthID desc
        , b.ReportingPeriodStartDate