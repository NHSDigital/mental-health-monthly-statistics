-- Databricks notebook source
--Referral table contains ReasonOAT column which says if a referral is an OAP or not

select uniqmonthid, Person_ID, count(*) as records
from $reference_data.MHS101Referral
where ReasonOAT = 10
group by uniqmonthid, Person_ID
order by uniqmonthid desc, records desc, Person_ID

-- COMMAND ----------

create or replace temporary view WardStay as 

with WardStay_ordered
as
(
  select a.*
    ,row_number() over(partition by uniqmonthid, WardStayID order by StartDateWardStay desc, case when EndDateWardStay is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS502WardStay as a
)   
  
select *
from WardStay_ordered as a
where stay_order=1
--  and a.StartDateWardStay != a.EndDateWardStay
--  and (a.DischDateHospProvSpell is null or a.DischDateHospProvSpell >= a.ReportingPeriodStartDate);

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

USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(*) as Inappropriate_OAPs
    , SUM(METRIC_VALUE) as Inappropriate_OAPs_Days
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END) as Inappropriate_OAPs_Days_AOAMHC
    , SUM(CASE WHEN HospitalBedTypeMH = '10' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AAMHC_pc
    , SUM(CASE WHEN HospitalBedTypeMH = '11' THEN METRIC_VALUE ELSE 0 END)/SUM(METRIC_VALUE) as Inappropriate_OAPs_Days_AOAMHC_pc
FROM
  (
  SELECT DISTINCT
        ref.UNiqMonthID
        ,wrd.UNIQWARDSTAYID
        ,WRD.OrgIDProv
        ,WRD.HospitalBedTypeMH
        ,DATEDIFF(
            IFNULL (wrd.EndDateWardStay, DATE_ADD (mth.ReportingPeriodEndDate,1))
            ,IF(wrd.StartDateWardStay < mth.ReportingPeriodStartDate, mth.ReportingPeriodStartDate, wrd.StartDateWardStay)
            ) AS METRIC_VALUE
  FROM mhs101referral AS ref
    INNER JOIN (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as mth on ref.UniqMonthID = mth.UniqMonthID
    LEFT OUTER JOIN MHS501HospProvSpell AS hsp
      ON ref.UniqServReqID = hsp.UniqServReqID 
        AND ref.UNiqMonthID = hsp.UNiqMonthID 
    LEFT OUTER JOIN mhs502wardstay AS wrd
      ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
        AND ref.UNiqMonthID = wrd.UNiqMonthID 
  WHERE ref.ReasonOAT IN ('10')
  ) as a
group by UNiqMonthID
order by 1 desc

-- COMMAND ----------

USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(UniqServReqID) as Inappropriate_OAPs
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
          ,IFNULL (wrd.EndDateWardStay, DATE_ADD (mth.ReportingPeriodEndDate,1)) AS ENDDATE
          ,IF(wrd.StartDateWardStay < mth.ReportingPeriodStartDate, mth.ReportingPeriodStartDate, wrd.StartDateWardStay) AS STARTDATE
    FROM mhs101referral AS ref
      INNER JOIN (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as mth on ref.UniqMonthID = mth.UniqMonthID
      LEFT OUTER JOIN MHS501HospProvSpell AS hsp
        ON ref.UniqServReqID = hsp.UniqServReqID 
          AND ref.UNiqMonthID = hsp.UNiqMonthID 
      LEFT OUTER JOIN mhs502wardstay AS wrd
        ON hsp.UniqHospProvSpellID = wrd.UniqHospProvSpellID 
          AND ref.UNiqMonthID = wrd.UNiqMonthID 
    WHERE ref.ReasonOAT IN ('10')
    ) AS a
  ) AS a
group by UNiqMonthID
order by 1

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

-- COMMAND ----------

-- -- DAE TABLE 1 WHOLE SCRIPT (Part 1 & 2) - INAPPROPRIATE OAP BED_DAYS

-- USE $db_source;

-- SELECT 
-- 'MHSDS_All' as TABLE_NAME
-- ,'$DateSTART' AS REPORTING_PERIOD
-- , 'Final' AS STATUS
-- , 'England' AS BREAKDOWN
-- , 'England' AS PRIMARY_LEVEL
-- , 'England' AS PRIMARY_LEVEL_DESCRIPTION
-- , 'NONE' AS SECONDARY_LEVEL
-- , 'NONE' AS SECONDARY_LEVEL_DESCRIPTION
-- ,'INAPPRIOPRIATE_OAP_BED_DAYS' AS METRIC
-- ,CAST(IFNULL(SUM(METRIC_VALUE),0) AS STRING) AS METRIC_VALUE
-- --,CAST(SUM(IFNULL(METRIC_VALUE,0)) AS STRING) AS METRIC_VALUE1
-- FROM (SELECT DISTINCT
-- wrd.UNIQWARDSTAYID
-- ,WRD.OrgIDProv
-- ,DATEDIFF(
--     IFNULL (wrd.EndDateWardStay, DATE_ADD ('$Date_END',1))
--     ,IF(wrd.StartDateWardStay <'$DateSTART', '$DateSTART', wrd.StartDateWardStay)
--     ) AS METRIC_VALUE
  
-- FROM $db_source.mhs502wardstay AS wrd
--   LEFT OUTER JOIN $db_source.MHS501HospProvSpell AS hsp
--     ON hsp.UniqHospProvSpellNum = wrd.UniqHospProvSpellNum 
--       AND hsp.UNiqMonthID = '$MonthID'
--   LEFT OUTER JOIN $db_source.mhs101referral AS ref
--     ON ref.UniqServReqID = hsp.UniqServReqID 
--       AND ref.UNiqMonthID = '$MonthID'
-- WHERE wrd.UNiqMonthID = '$MonthID'
--   AND ref.ReasonOAT IN ('10'))

-- UNION ALL

-- SELECT 
-- 'MHSDS_All' as TABLE_NAME
-- ,'$DateSTART' AS REPORTING_PERIOD
-- , 'Final' AS STATUS
-- , 'Provider' AS BREAKDOWN
-- , a.OrgIDProvider AS PRIMARY_LEVEL
-- , org.name AS PRIMARY_LEVEL_DESCRIPTION
-- , 'NONE' AS SECONDARY_LEVEL
-- , 'NONE' AS SECONDARY_LEVEL_DESCRIPTION
-- ,'INAPPRIOPRIATE_OAP_BED_DAYS' AS METRIC

-- ,IF(IFNULL(SUM(METRIC_VALUE),0) <5 ,'*'
--      ,CAST((ROUND(CAST(IFNULL(SUM(METRIC_VALUE),0) AS BIGINT)/5,0)*5) AS STRING)
--    ) AS METRIC_VALUE
-- FROM 
--   MHS000Header A
--     LEFT OUTER JOIN TMP_RD_ORG_DAILY_LATEST AS org
--       ON A.OrgIDProvider = org.ORG_CODE
--     LEFT JOIN 
--             ( SELECT OrgIDProv, SUM(METRIC_VALUE) AS METRIC_VALUE
--               FROM
--                 (SELECT DISTINCT
--                 wrd.UNIQWARDSTAYID
--                 ,WRD.OrgIDProv
--                 ,DATEDIFF(
--                     IFNULL (wrd.EndDateWardStay, DATE_ADD ('$Date_END',1))
--                     ,IF(wrd.StartDateWardStay <'$DateSTART', '$DateSTART', wrd.StartDateWardStay)
--                     ) AS METRIC_VALUE
--                 FROM mhs502wardstay AS wrd
--                   LEFT OUTER JOIN MHS501HospProvSpell AS hsp
--                     ON hsp.UniqHospProvSpellNum = wrd.UniqHospProvSpellNum 
--                       AND hsp.UNiqMonthID = CAST('$MonthID' as Integer)
--                   LEFT OUTER JOIN mhs101referral AS ref
--                     ON ref.UniqServReqID = hsp.UniqServReqID 
--                       AND ref.UNiqMonthID = CAST('$MonthID' as Integer)
--                 WHERE wrd.UNiqMonthID = CAST('$MonthID' as Integer)
--                   AND ref.ReasonOAT IN ('10')
--                ) AS A
--                GROUP BY OrgIDProv) AS B 
--      ON A.OrgIDProvider = B.OrgIDProv
-- WHERE a.UNiqMonthID = CAST('$MonthID' as Integer)
-- GROUP BY a.OrgIDProvider, org.Name, metric_Value
-- ORDER BY BREAKDOWN, PRIMARY_LEVEL, SECONDARY_LEVEL, METRIC
-- ;
