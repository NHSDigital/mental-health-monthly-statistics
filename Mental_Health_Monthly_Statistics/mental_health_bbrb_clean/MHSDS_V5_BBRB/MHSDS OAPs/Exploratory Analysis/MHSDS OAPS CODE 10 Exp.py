# Databricks notebook source
 %sql
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

# COMMAND ----------

 %sql
 create or replace temporary view OAPs_referrals as 
 
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
   and COALESCE(a.ReasonOAT,a.OATReason) is not null
 --  and (a.ServDischDate is null or a.ServDischDate >= a.ReportingPeriodStartDate);
 
 -- select * from OAPs_referrals

# COMMAND ----------

 %sql
 create or replace temporary view Overlapping_OAPs_referrals as 
 
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

# COMMAND ----------

 %sql
 select ReportingPeriodStartDate
         , uniqmonthid
         , count(*) as OAPs_records
         , COUNT(CASE WHEN ReferralrequestReceivedDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN ServiceRequestId ELSE null END) as defaultpc_records
         , COUNT(CASE WHEN ReferralrequestReceivedDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN ServiceRequestId ELSE null END)/count(*) as defaultpc_records_pc
         , count(CASE WHEN ReasonOAT = 10 THEN ServiceRequestId ELSE null END) as Inappropriate_OAPs_records
         , COUNT(CASE WHEN ReasonOAT = 10 and ReferralrequestReceivedDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN ServiceRequestId ELSE null END) as Active_Inappropriate_OAPs_records
         , COUNT(CASE WHEN ReasonOAT = 10 and ReferralrequestReceivedDate between ReportingPeriodStartDate and ReportingPeriodEndDate THEN ServiceRequestId ELSE null END)/count(CASE WHEN ReasonOAT = 10 THEN ServiceRequestId ELSE null END) as Active_Inappropriate_OAPs_records_pc
 from Overlapping_OAPs_referrals
 group by ReportingPeriodStartDate
         , uniqmonthid
 order by ReportingPeriodStartDate desc
         , uniqmonthid