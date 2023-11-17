-- Databricks notebook source
USE $reference_data;

SELECT 
    UNiqMonthID
    , COUNT(UniqServReqID) as Inappropriate_OAPs
    , COUNT(CASE WHEN METRIC_VALUE > 365 THEN UniqServReqID ELSE null END) as Inappropriate_OAPs_Over_365_days
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
          ,hsp.StartDateHospProvSpell AS STARTDATE
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