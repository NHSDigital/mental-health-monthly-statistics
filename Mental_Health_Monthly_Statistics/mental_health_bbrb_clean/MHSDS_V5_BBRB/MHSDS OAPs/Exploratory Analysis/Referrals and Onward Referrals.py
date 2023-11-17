# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $user_id.OAPS_In_Scope;
 CREATE TABLE         $user_id.OAPS_In_Scope USING DELTA AS
 SELECT *
 FROM $reference_data.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= add_months('2022-02-28', 1) OR ISNULL(BUSINESS_END_DATE))
   AND BUSINESS_START_DATE <= add_months('$2022-02-28', 1)
   AND ORG_CODE in ('NQL','NR5','R1A','R1C','R1F','R1L','RAT','RDY','RGD','RH5','RHA','RJ8','RKL','RLY','RMY','RNN',
                    'RNU','RP1','RP7','RPG','RQ3','RQY','RRE','RRP','RT1','RT2','RT5','RTQ','RTV','RV3','RV5','RV9',
                    'RVN','RW1','RW4','RW5','RWK','RWR','RWV','RWX','RX2','RX3','RX4','RXA','RXE','RXG','RXM','RXT',
                    'RXV','RXX','RXY','RYG','RYK','TAD','TAF','TAH','TAJ')

# COMMAND ----------

 %sql
 SELECT COUNT(*), SUM(CASE WHEN OrgIDSubmitting is null then 0 ELSE 1 END) as OrgIDSending
 FROM $user_id.OAPS_Month

# COMMAND ----------

 %sql
 SELECT  h.ReportingPeriodSTartDate,
         COALESCE(ins.ORG_CODE,LEFT(onw.OrgIDProv,3)) as ORG_CODE,
         ins.NAME,
         CASE WHEN ins.ORG_CODE IS NULL THEN 'N' ELSE 'Y' END as InScope,
         COUNT(DISTINCT UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '99' THEN UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '10' THEN UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         COUNT(DISTINCT CASE WHEN onw.OnwardReferDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '99'
                              AND onw.OnwardReferDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '10'
                              AND onw.OnwardReferDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS105OnwardReferral as onw
 left join $user_id.OAPS_In_Scope as ins on LEFT(onw.OrgIDProv,3) = ins.ORG_CODE
 inner join (select DISTINCT UniqMonthID, ReportingPeriodSTartDate, ReportingPeriodEndDate from $reference_data.MHS000Header where UniqMonthID in (1463,1464,1465)) as h on onw.UniqMonthID = h.UniqMonthID
 WHERE (onw.OATReason is not null)
 GROUP BY h.ReportingPeriodSTartDate,
          COALESCE(ins.ORG_CODE,LEFT(onw.OrgIDProv,3)),
          ins.NAME,
          CASE WHEN ins.ORG_CODE IS NULL THEN 'N' ELSE 'Y' END
 UNION
 SELECT  h.ReportingPeriodSTartDate,
         'England',
         'England',
         CASE WHEN ins.ORG_CODE IS NULL THEN 'N' ELSE 'Y' END as InScope,
         COUNT(DISTINCT UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '99' THEN UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '10' THEN UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         COUNT(DISTINCT CASE WHEN onw.OnwardReferDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '99'
                              AND onw.OnwardReferDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN onw.OATReason = '10'
                              AND onw.OnwardReferDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS105OnwardReferral as onw
 left join $user_id.OAPS_In_Scope as ins on LEFT(onw.OrgIDProv,3) = ins.ORG_CODE
 inner join (select DISTINCT UniqMonthID, ReportingPeriodSTartDate, ReportingPeriodEndDate from $reference_data.MHS000Header where UniqMonthID in (1463,1464,1465)) as h on onw.UniqMonthID = h.UniqMonthID
 WHERE (onw.OATReason is not null)
 GROUP BY h.ReportingPeriodSTartDate,
         CASE WHEN ins.ORG_CODE IS NULL THEN 'N' ELSE 'Y' END
 ORDER BY 1,2

# COMMAND ----------

 %sql
 SELECT  ins.ORG_CODE,
         ins.NAME,
         COUNT(DISTINCT UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99' THEN UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10' THEN UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS101Referral as ref
 inner join $user_id.OAPS_In_Scope as ins on LEFT(ref.OrgIDReferring,3) = ins.ORG_CODE
 WHERE UNiqMOnthID = 1463
   AND (ref.ReasonOAT is not null)
 GROUP BY ins.ORG_CODE,
          ins.NAME
 UNION
 SELECT  'England',
         'England',
         COUNT(DISTINCT UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99' THEN UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10' THEN UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS101Referral as ref
 inner join $user_id.OAPS_In_Scope as ins on LEFT(ref.OrgIDReferring,3) = ins.ORG_CODE
 WHERE UNiqMOnthID = 1463
   AND (ref.ReasonOAT is not null)
 ORDER BY 1

# COMMAND ----------

 %sql
 SELECT  'England',
         'England',
         COUNT(DISTINCT ref.UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '99' THEN ref.UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '10' THEN ref.UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN ref.UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '99'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN ref.UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '10'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN ref.UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS101Referral as ref
 inner join $reference_data.MHS105OnwardReferral as ONW
    on ref.Person_ID = onw.Person_ID
   and (ref.OrgIDProv = onw.OrgIDReceiving
         or ref.OrgIDReferring = onw.OrgIDProv)
   and ref.ReferralRequestReceivedDate >= onw.OnwardReferDate
 --   and ref.UniqMonthID = onw.UniqMonthID
 inner join $user_id.OAPS_In_Scope as ins on LEFT(ref.OrgIDReferring,3) = ins.ORG_CODE
 WHERE ref.UNiqMOnthID = 1463
   AND (ref.ReasonOAT is not null)
   AND onw.OATReason is not null
 ORDER BY 1

# COMMAND ----------

 %sql
 SELECT -- ins.ORG_CODE,
 --         ins.NAME,
         COUNT(DISTINCT ref.UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '99' THEN ref.UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '10' THEN ref.UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN ref.UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '99'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN ref.UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN COALESCE(ref.ReasonOAT,onw.OATReason) = '10'
                              AND ref.ReferralRequestReceivedDate between '2022-02-01' and '2022-02-28' THEN ref.UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS101Referral as ref
 inner join $reference_data.MHS105OnwardReferral as ONW
    on ref.Person_ID = onw.Person_ID
   and (LEFT(ref.OrgIDProv,3) = LEFT(onw.OrgIDReceiving,3)
         or LEFT(ref.OrgIDReferring,3) = LEFT(onw.OrgIDProv,3))
   and ref.UniqMonthID = onw.UniqMonthID
 -- inner join $user_id.OAPS_In_Scope as ins on LEFT(ref.OrgIDReferring,3) = ins.ORG_CODE
 WHERE ref.UNiqMOnthID = 1475
   AND (ref.ReasonOAT is not null)
   AND onw.OATReason is not null
 -- GROUP BY ins.ORG_CODE,
 --         ins.NAME
 -- ORDER BY 1

# COMMAND ----------

 %sql
 -- SELECT *
 SELECT  h.ReportingPeriodSTartDate,
         ins.ORG_CODE,
         ins.NAME,
         COUNT(DISTINCT ref.UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99' THEN ref.UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10' THEN ref.UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         SUM(COALESCE(ws.BedDaysWSEndRP,0)) as OAPBedDays,
         SUM(COALESCE(CASE WHEN ref.ReasonOAT = '10' THEN ws.BedDaysWSEndRP ELSE 0 END,0)) as InapropriateBedDays,
         COUNT(DISTINCT CASE WHEN ref.ReferralRequestReceivedDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN ref.UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99'
                              AND ref.ReferralRequestReceivedDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN ref.UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10'
                              AND ref.ReferralRequestReceivedDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN ref.UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS101Referral as ref
 inner join $user_id.OAPS_In_Scope as ins on LEFT(ref.OrgIDReferring,3) = ins.ORG_CODE
 inner join (select DISTINCT UniqMonthID, ReportingPeriodSTartDate, ReportingPeriodEndDate from $reference_data.MHS000Header where UniqMonthID in (1462,1463,1464)) as h on ref.UniqMonthID = h.UniqMonthID
 left join $reference_data.mhs501HospProvSpell as hosp on ref.UniqMonthID = hosp.UniqMonthID and ref.UniqServReqID = hosp.UniqServReqID and ref.Person_ID = hosp.Person_ID and ref.OrgIDProv = hosp.OrgIDProv
 left join $reference_data.MHS502WardStay as ws on ref.UniqMonthID = ws.UniqMonthID and hosp.UniqHospProvSpellID = ws.UniqHospProvSpellID and ref.Person_ID = ws.Person_ID and ref.OrgIDProv = ws.OrgIDProv
 WHERE (ref.ReasonOAT is not null)
 GROUP BY h.ReportingPeriodSTartDate,
          ins.ORG_CODE,
          ins.NAME
 UNION
 SELECT  h.ReportingPeriodSTartDate,
         'England',
         'England',
         COUNT(DISTINCT ref.UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99' THEN ref.UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10' THEN ref.UniqServReqID ELSE NULL END) as Inappropriate_OAPs,
         SUM(COALESCE(ws.BedDaysWSEndRP,0)) as OAPBedDays,
         SUM(COALESCE(CASE WHEN ref.ReasonOAT = '10' THEN ws.BedDaysWSEndRP ELSE 0 END,0)) as InapropriateBedDays,
         COUNT(DISTINCT CASE WHEN ref.ReferralRequestReceivedDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN ref.UniqServReqID ELSE NULL END) as New_OAPs,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '99'
                              AND ref.ReferralRequestReceivedDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN ref.UniqServReqID ELSE NULL END) as New_OAPs_99,
         COUNT(DISTINCT CASE WHEN ref.ReasonOAT = '10'
                              AND ref.ReferralRequestReceivedDate between h.ReportingPeriodSTartDate and h.ReportingPeriodEndDate THEN ref.UniqServReqID ELSE NULL END) as New_Inappropriate_OAPs
 FROM $reference_data.MHS101Referral as ref
 inner join $user_id.OAPS_In_Scope as ins on LEFT(ref.OrgIDReferring,3) = ins.ORG_CODE
 inner join (select DISTINCT UniqMonthID, ReportingPeriodSTartDate, ReportingPeriodEndDate from $reference_data.MHS000Header where UniqMonthID in (1462,1463,1464)) as h on ref.UniqMonthID = h.UniqMonthID
 left join $reference_data.mhs501HospProvSpell as hosp on ref.UniqMonthID = hosp.UniqMonthID and ref.UniqServReqID = hosp.UniqServReqID and ref.Person_ID = hosp.Person_ID and ref.OrgIDProv = hosp.OrgIDProv
 left join $reference_data.MHS502WardStay as ws on ref.UniqMonthID = ws.UniqMonthID and hosp.UniqHospProvSpellID = ws.UniqHospProvSpellID and ref.Person_ID = ws.Person_ID and ref.OrgIDProv = ws.OrgIDProv
 WHERE (ref.ReasonOAT is not null)
 GROUP BY h.ReportingPeriodSTartDate
 ORDER BY 1,2

# COMMAND ----------

 %sql
 SELECT CASE WHEN ws.startdatewardstay < h.reportingperiodstartdate then reportingperiodstartdate else startdatewardstay end as MonthStartDate,
        CASE WHEN ws.enddatewardstay > h.reportingperiodenddate or ws.enddatewardstay is null then reportingperiodenddate else enddatewardstay end as MonthEndDate,
        DATEDIFF(DATE_ADD(CASE WHEN ws.enddatewardstay > h.reportingperiodenddate or ws.enddatewardstay is null then reportingperiodenddate else enddatewardstay end,1),
                  CASE WHEN ws.startdatewardstay < h.reportingperiodstartdate then reportingperiodstartdate else startdatewardstay end) as BedDays,
        BedDaysWSEndRP
 FROM $reference_data.MHS502WardStay as ws
 inner join (select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate from $reference_data.MHS000Header) as h
    on ws.uniqmonthid = h.uniqmonthid
 WHERE DATEDIFF(DATE_ADD(CASE WHEN ws.enddatewardstay > h.reportingperiodenddate or ws.enddatewardstay is null then reportingperiodenddate else enddatewardstay end,1),
                  CASE WHEN ws.startdatewardstay < h.reportingperiodstartdate then reportingperiodstartdate else startdatewardstay end) != BedDaysWSEndRP

# COMMAND ----------

 %sql
 SELECT UniqMonthID,
         OrgIDReceiving as ReceivingProvider,
         OrgIDSubmitting as SendingProvider,
         Inscope,
         COUNT(distinct UniqServReqID) as OAPs,
         COUNT(distinct CASE WHEN ReasonOAT = 99 THEN UniqServReqID ELSE NULL END) as OAPs_99,
         COUNT(distinct CASE WHEN ReasonOAT = 10 THEN UniqServReqID ELSE NULL END) as InappropriateOAPs,
         SUM(BedDaysWSEndRP) as BedDays,
         SUM(CASE WHEN ReasonOAT = 99 THEN BedDaysWSEndRP ELSE 0 END) as BedDays_99,
         SUM(CASE WHEN ReasonOAT = 10 THEN BedDaysWSEndRP ELSE 0 END) as InappropriateBedDays
 FROM $user_id.OAPs
 WHERE UniqMOnthID in ('1463','1464','1465')
 GROUP BY UniqMonthID,
         OrgIDReceiving,
         OrgIDSubmitting,
         Inscope
 ORDER BY UniqMonthID,
         OrgIDReceiving,
         OrgIDSubmitting

# COMMAND ----------

 %sql
 SELECT  UniqMonthID,
         COUNT(DISTINCT UniqServReqID) as OAPs,
         COUNT(DISTINCT CASE WHEN OrgIDReferring IS NOT NULL THEN UniqServReqID ELSE NULL END) as Sending_OAPs
 FROM $reference_data.mhs101referral
 where ReasonOAT = '10'
 GROUP BY UniqMonthID
 ORDER BY 1 DESC