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

select * from HospProvSpell

-- COMMAND ----------

select c.ReportingPeriodStartDate
        , A.uniqmonthid
        , count(A.UniqServReqID) as records
        , count(CASE WHEN ReasonOAT = 10 THEN A.UniqServReqID ELSE null END) as Inappropriate_OAPs_records
        , COUNT(CASE WHEN StartDateHospProvSpell between StartDateHospProvSpell and c.ReportingPeriodEndDate THEN A.UniqServReqID ELSE null END) as OAPs_Started
        , COUNT(CASE WHEN ReasonOAT = 10 and StartDateHospProvSpell between c.ReportingPeriodStartDate and c.ReportingPeriodEndDate THEN A.UniqServReqID ELSE null END) as Inappropriate_OAPs_Started
        , COUNT(CASE WHEN ReasonOAT = 10 and DischDateHospProvSpell between c.ReportingPeriodStartDate and c.ReportingPeriodEndDate THEN A.UniqServReqID ELSE null END) as Inappropriate_OAPs_Ended
        , COUNT(CASE WHEN ReasonOAT = 10 and (DischDateHospProvSpell is null or DischDateHospProvSpell > c.ReportingPeriodEndDate) THEN A.UniqServReqID ELSE null END) as Inappropriate_OAPs_Active
        , COUNT(CASE WHEN StartDateHospProvSpell between c.ReportingPeriodStartDate and c.ReportingPeriodEndDate THEN A.UniqServReqID ELSE null END)/
                  count(A.UniqServReqID) as OAPs_Started_pc
        , COUNT(CASE WHEN ReasonOAT = 10 and StartDateHospProvSpell between c.ReportingPeriodStartDate and c.ReportingPeriodEndDate THEN A.UniqServReqID ELSE null END)/
                  count(CASE WHEN ReasonOAT = 10 THEN A.UniqServReqID ELSE null END) as Inappropriate_OAPs_Started_pc
from $reference_data.MHS101Referral as A
inner join (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as c on a.UniqMonthID = c.UniqMonthID
left outer join $reference_data.MHS501HospProvSpell as B
    on A.UniqServReqID = B.UniqServReqID
   and A.uniqmonthid = B.uniqmonthid
group by c.ReportingPeriodStartDate
        , A.uniqmonthid
order by c.ReportingPeriodStartDate
        , A.uniqmonthid

-- COMMAND ----------

USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(UniqServReqID) as Inappropriate_OAPs
    , SUM(STARTEDINPERIOD) as Inappropriate_OAPs_Started
    , SUM(ENDEDINPERIOD) as Inappropriate_OAPs_Ended
    , SUM(ACTIVEENDPERIOD) as Inappropriate_OAPs_Active_End
    , SUM(METRIC_VALUE) as Inappropriate_OAPs_Days
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AOAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AAMHC_pc
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AOAMHC_pc
FROM
  (
  SELECT *
        ,DATEDIFF(ENDDATE,STARTDATE) as METRIC_VALUE
  FROM
    (
    SELECT DISTINCT
          ref.UNiqMonthID
          ,ref.UniqServReqID
          ,wrd.UNIQWARDSTAYID
          ,WRD.OrgIDProv
          ,WRD.HospitalBedTypeMH
          ,IFNULL (hsp.DischDateHospProvSpell, DATE_ADD (mth.ReportingPeriodEndDate,1)) AS ENDDATE
          ,IF(hsp.StartDateHospProvSpell < mth.ReportingPeriodStartDate, mth.ReportingPeriodStartDate, hsp.StartDateHospProvSpell) AS STARTDATE
          ,CASE WHEN hsp.StartDateHospProvSpell between mth.ReportingPeriodStartDate and mth.ReportingPeriodEndDate THEN 1 ELSE 0 END AS STARTEDINPERIOD
          ,CASE WHEN hsp.DischDateHospProvSpell between mth.ReportingPeriodStartDate and mth.ReportingPeriodEndDate THEN 1 ELSE 0 END AS ENDEDINPERIOD
          ,CASE WHEN hsp.DischDateHospProvSpell > mth.ReportingPeriodEndDate OR hsp.DischDateHospProvSpell IS NULL THEN 1 ELSE 0 END AS ACTIVEENDPERIOD
    FROM mhs101referral AS ref
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
      LEFT OUTER JOIN mhs502wardstay AS wrd
        ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
          AND ref.UNiqMonthID = wrd.UNiqMonthID 
      LEFT OUTER JOIN ( SELECT DISTINCT UNiqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate FROM MHS000Header) AS mth
        ON ref.UNiqMonthID = mth.UNiqMonthID 
    WHERE ref.ReasonOAT IN ('10')
    ) AS a
  ) AS a
group by UNiqMonthID
order by 1

-- COMMAND ----------

USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(UniqServReqID) as Inappropriate_OAPs
    , SUM(STARTEDINPERIOD) as Inappropriate_OAPs_Started
    , SUM(ENDEDINPERIOD) as Inappropriate_OAPs_Ended
    , SUM(ACTIVEENDPERIOD) as Inappropriate_OAPs_Active_End
    , SUM(METRIC_VALUE) as Inappropriate_OAPs_Days
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AOAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AAMHC_pc
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AOAMHC_pc
FROM
  (
  SELECT *
        ,DATEDIFF(ENDDATE,STARTDATE) as METRIC_VALUE
  FROM
    (
    SELECT DISTINCT
          ref.UNiqMonthID
          ,ref.UniqServReqID
          ,wrd.UNIQWARDSTAYID
          ,WRD.OrgIDProv
          ,WRD.HospitalBedTypeMH
          ,IFNULL (hsp.DischDateHospProvSpell, DATE_ADD (mth.ReportingPeriodEndDate,1)) AS ENDDATE
          ,IF(hsp.StartDateHospProvSpell < mth.ReportingPeriodStartDate, mth.ReportingPeriodStartDate, hsp.StartDateHospProvSpell) AS STARTDATE
          ,CASE WHEN hsp.StartDateHospProvSpell between mth.ReportingPeriodStartDate and mth.ReportingPeriodEndDate THEN 1 ELSE 0 END AS STARTEDINPERIOD
          ,CASE WHEN hsp.DischDateHospProvSpell between mth.ReportingPeriodStartDate and mth.ReportingPeriodEndDate THEN 1 ELSE 0 END AS ENDEDINPERIOD
          ,CASE WHEN hsp.DischDateHospProvSpell > mth.ReportingPeriodEndDate OR hsp.DischDateHospProvSpell IS NULL THEN 1 ELSE 0 END AS ACTIVEENDPERIOD
    FROM MHS105OnwardReferral AS ref
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
      LEFT OUTER JOIN mhs502wardstay AS wrd
        ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
          AND ref.UNiqMonthID = wrd.UNiqMonthID 
      LEFT OUTER JOIN ( SELECT DISTINCT UNiqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate FROM MHS000Header) AS mth
        ON ref.UNiqMonthID = mth.UNiqMonthID 
    WHERE ref.OATReason IN ('10')
    ) AS a
  ) AS a
group by UNiqMonthID
order by 1

-- COMMAND ----------

USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(UniqServReqID) as Inappropriate_OAPs
    , SUM(STARTEDINPERIOD) as Inappropriate_OAPs_Started
    , SUM(ENDEDINPERIOD) as Inappropriate_OAPs_Ended
    , SUM(ACTIVEENDPERIOD) as Inappropriate_OAPs_Active_End
    , SUM(METRIC_VALUE) as Inappropriate_OAPs_Days
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AOAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AAMHC_pc
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AOAMHC_pc
FROM
  (
  SELECT *
        ,DATEDIFF(ENDDATE,STARTDATE) as METRIC_VALUE
  FROM
    (
    SELECT DISTINCT
          ref.UNiqMonthID
          ,ref.UniqServReqID
          ,wrd.UNIQWARDSTAYID
          ,WRD.OrgIDProv
          ,WRD.HospitalBedTypeMH
          ,IFNULL (hsp.DischDateHospProvSpell, DATE_ADD (mth.ReportingPeriodEndDate,1)) AS ENDDATE
          ,IF(hsp.StartDateHospProvSpell < mth.ReportingPeriodStartDate, mth.ReportingPeriodStartDate, hsp.StartDateHospProvSpell) AS STARTDATE
          ,CASE WHEN hsp.StartDateHospProvSpell between mth.ReportingPeriodStartDate and mth.ReportingPeriodEndDate THEN 1 ELSE 0 END AS STARTEDINPERIOD
          ,CASE WHEN hsp.DischDateHospProvSpell between mth.ReportingPeriodStartDate and mth.ReportingPeriodEndDate THEN 1 ELSE 0 END AS ENDEDINPERIOD
          ,CASE WHEN hsp.DischDateHospProvSpell > mth.ReportingPeriodEndDate OR hsp.DischDateHospProvSpell IS NULL THEN 1 ELSE 0 END AS ACTIVEENDPERIOD
    FROM (  SELECT COALESCE(ref.UNiqMonthID,onw.UNiqMonthID) AS UNiqMonthID,
                   COALESCE(ref.UniqServReqID,onw.UniqServReqID) AS UniqServReqID,
                   COALESCE(ref.ReasonOAT,onw.OATReason) AS ReasonOAT
            FROM mhs101referral AS ref
            FULL JOIN MHS105OnwardReferral AS onw
               ON ref.Person_ID = onw.Person_ID
              AND (onw.OrgIDProv = ref.OrgIDReferring
                   OR onw.OrgIDReceiving = ref.OrgIDProv)
              AND ref.UniqMonthID = onw.UniqmonthID) AS ref
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
      LEFT OUTER JOIN mhs502wardstay AS wrd
        ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
          AND ref.UNiqMonthID = wrd.UNiqMonthID 
      LEFT OUTER JOIN ( SELECT DISTINCT UNiqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate FROM MHS000Header) AS mth
        ON ref.UNiqMonthID = mth.UNiqMonthID 
    WHERE ref.ReasonOAT IN ('10')
    ) AS a
  ) AS a
group by UNiqMonthID
order by 1

-- COMMAND ----------

SELECT * FROM MHS105OnwardReferral WHERE OATReason = '10'