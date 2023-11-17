-- Databricks notebook source
USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(UniqServReqID) as Inappropriate_OAPs
    , SUM(STARTEDINPERIOD) as Inappropriate_OAPs_Started
    , SUM(ENDEDINPERIOD) as Inappropriate_OAPs_Ended
    , SUM(ACTIVEENDPERIOD) as Inappropriate_OAPs_Active_End
    , SUM(HASDISTANCE) as Inappropriate_OAPs_Has_Distance
    , SUM(ACTIVEHASDISTANCE) as Inappropriate_OAPs_Active_End_Has_Distance
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
          ,CASE WHEN wrd.WardLocDistanceHome is not null THEN 1 ELSE 0 END AS HASDISTANCE
          ,CASE WHEN (hsp.DischDateHospProvSpell > mth.ReportingPeriodEndDate OR hsp.DischDateHospProvSpell IS NULL) and wrd.WardLocDistanceHome is not null THEN 1 ELSE 0 END AS ACTIVEHASDISTANCE
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
order by 1 desc