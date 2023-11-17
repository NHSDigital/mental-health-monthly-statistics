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
where uniqmonthid = 1450


-- COMMAND ----------

select ReportingPeriodStartDate
        , uniqmonthid
        , count(*) as records
        , count(CASE WHEN ReasonOAT = 10 THEN UniqServReqID ELSE null END) as Inappropriate_OAPs_records
        , SUM(datediff(CASE WHEN NewServDischDate is null or NewServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE NewServDischDate END,
                          CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (NewServDischDate is null or NewServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
                               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (NewServDischDate is null or NewServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                               WHEN NewServDischDate is null or NewServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
                               ELSE NewServDischDate END)) as OAPs_Days
        , SUM(CASE WHEN ReasonOAT = 10 THEN
                 datediff(CASE WHEN NewServDischDate is null or NewServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate ELSE NewServDischDate END,
                          CASE WHEN ReferralRequestReceivedDate < ReportingPeriodStartDate and (NewServDischDate is null or NewServDischDate>ReportingPeriodStartDate) THEN ReportingPeriodStartDate
                               WHEN ReferralRequestReceivedDate >= ReportingPeriodStartDate and (NewServDischDate is null or NewServDischDate>ReferralRequestReceivedDate) THEN ReferralRequestReceivedDate
                               WHEN NewServDischDate is null or NewServDischDate>ReportingPeriodEndDate THEN ReportingPeriodEndDate
                               ELSE NewServDischDate END)
                   ELSE 0 END) as Inappropriate_OAPs_Days
from NonOverlapping_OAPs_referrals
group by ReportingPeriodStartDate
        , uniqmonthid
order by ReportingPeriodStartDate
        , uniqmonthid

-- COMMAND ----------

drop table timunderwood_100056.RefScopeList;
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

USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(distinct UniqServReqID) as Inappropriate_OAPs
    , SUM(METRIC_VALUE) as Inappropriate_OAPs_Days
    , SUM(IF(InScope_Flag = 1,METRIC_VALUE,0)) as Inappropriate_OAPs_Days_InScope
FROM
  (
  SELECT *
        ,DATEDIFF(ENDDATE,STARTDATE) as METRIC_VALUE
  FROM
    (
    SELECT DISTINCT
          ref.UNiqMonthID
          ,ref.UniqServReqID
          ,hsp.UniqHospProvSpellID
          ,hsp.OrgIDProv
--           ,hsp.HospitalBedTypeMH
          ,IFNULL (hsp.DischDateHospProvSpell, DATE_ADD (mth.ReportingPeriodEndDate,1)) AS ENDDATE
          ,IF(hsp.StartDateHospProvSpell<mth.ReportingPeriodStartDate,mth.ReportingPeriodStartDate,hsp.StartDateHospProvSpell) AS STARTDATE
          ,IF(scp.OrgIDProv is null,0,1) AS InScope_Flag
    FROM mhs101referral AS ref
      INNER JOIN (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as mth on ref.UniqMonthID = mth.UniqMonthID
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
          AND ref.Person_ID = hsp.Person_ID
          AND ref.OrgIDProv = hsp.OrgIDProv
--       LEFT OUTER JOIN mhs502wardstay AS wrd
--         ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
--           AND ref.UNiqMonthID = wrd.UNiqMonthID 
      LEFT OUTER JOIN $user_id.RefScopeList AS scp
        ON LEFT(ref.OrgIDReferring,3) = scp.OrgIDProv
    WHERE ref.ReasonOAT IN ('10')
    ) AS a
  ) AS a
group by UNiqMonthID
order by 1 desc

-- COMMAND ----------

USE $reference_data;

SELECT *
FROM
  (
  SELECT *
--        ,DATEDIFF(ENDDATE,STARTDATE) as METRIC_VALUE
  FROM
    (
    SELECT distinct ref.*
    FROM mhs101referral AS ref
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
      LEFT OUTER JOIN mhs502wardstay AS wrd
        ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
          AND ref.UNiqMonthID = wrd.UNiqMonthID 
      LEFT OUTER JOIN $user_id.RefScopeList AS scp
        ON LEFT(ref.OrgIDReferring,3) = scp.OrgIDProv
    WHERE ref.ReasonOAT IN ('10')
      AND hsp.UniqServReqID is not null
    ) AS a
  ) AS a
WHERE UniqMonthID = 1464

-- COMMAND ----------

select * from $user_id.RefScopeList

-- COMMAND ----------

USE $reference_data;

SELECT 
    OrgIDProv
    , COUNT(distinct UniqServReqID) as Inappropriate_OAPs
    , SUM(METRIC_VALUE) as Inappropriate_OAPs_Days
    , SUM(IF(InScope_Flag = 1,METRIC_VALUE,0)) as Inappropriate_OAPs_Days_InScope
FROM
  (
  SELECT *
        ,DATEDIFF(ENDDATE,STARTDATE) as METRIC_VALUE
  FROM
    (
    SELECT DISTINCT
          ref.UNiqMonthID
          ,ref.UniqServReqID
          ,hsp.UniqHospProvSpellID
          ,hsp.OrgIDProv
--           ,hsp.HospitalBedTypeMH
          ,IFNULL (hsp.DischDateHospProvSpell, DATE_ADD (mth.ReportingPeriodEndDate,1)) AS ENDDATE
          ,IF(hsp.StartDateHospProvSpell<mth.ReportingPeriodStartDate,mth.ReportingPeriodStartDate,hsp.StartDateHospProvSpell) AS STARTDATE
          ,IF(scp.OrgIDProv is null,0,1) AS InScope_Flag
    FROM mhs101referral AS ref
      INNER JOIN (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header WHERE UniqMonthID = '1461') as mth on ref.UniqMonthID = mth.UniqMonthID
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
          AND ref.Person_ID = hsp.Person_ID
          AND ref.OrgIDProv = hsp.OrgIDProv
--       LEFT OUTER JOIN mhs502wardstay AS wrd
--         ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
--           AND ref.UNiqMonthID = wrd.UNiqMonthID 
      LEFT OUTER JOIN $user_id.RefScopeList AS scp
        ON LEFT(ref.OrgIDReferring,3) = scp.OrgIDProv
    WHERE ref.ReasonOAT IN ('10')
    ) AS a
  ) AS a
group by OrgIDProv
with rollup
order by 1 desc

-- COMMAND ----------

select *
from $reference_data.mhs101referral
where UniqMonthID = '1458'
  and OrgIDProv = 'NMJ'
  and ReasonOAT = '10'