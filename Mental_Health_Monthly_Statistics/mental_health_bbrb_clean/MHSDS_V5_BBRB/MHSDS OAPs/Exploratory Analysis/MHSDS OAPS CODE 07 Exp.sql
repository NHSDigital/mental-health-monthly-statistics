-- Databricks notebook source
-- --Referral table contains ReasonOAT column which says if a referral is an OAP or not

-- select uniqmonthid, count(Person_ID) as OAP, count(case when ReasonOAT=10 then Person_ID else null end) as InappropriateOAP
-- from $db_source.MHS101Referral
-- where ReasonOAT is not null
--   and ReferralrequestReceivedDate <= ReportingPeriodEndDate
--   and (ServDischDate is null or ServDischDate > ReportingPeriodStartDate)
-- group by uniqmonthid
-- order by uniqmonthid

-- COMMAND ----------

-- select *
-- from $db_source.MHS101Referral
-- where ReasonOAT = 10

-- COMMAND ----------

-- --Can join Referral table on HospProvSpell table and WardStay table to get distance between Patient PC and Ward PC (which is column WardLocDistanceHome in WardStay table).
-- --Need to join on more than HospProvSpellNum and ServiceRequestID!!!!!!

-- select a.* /*a.uniqmonthid
--         , count(a.Person_ID) as Patients
--         , count(CASE WHEN a.WardLocDistanceHome is not null THEN a.Person_ID ELSE null END) as Distance
--         , count(CASE WHEN a.WardLocDistanceHome is null THEN a.Person_ID ELSE null END) as NoDistance
--         , count(CASE WHEN c. ReasonOAT is not null THEN a.Person_ID ELSE null END) as Patients_OAPS
--         , count(CASE WHEN a.WardLocDistanceHome is not null and c. ReasonOAT is not null THEN a.Person_ID ELSE null END) as Distance_OAPS
--         , count(CASE WHEN a.WardLocDistanceHome is null and c. ReasonOAT is not null THEN a.Person_ID ELSE null END) as NoDistance_OAPS
--         , count(CASE WHEN c. ReasonOAT = 10 THEN a.Person_ID ELSE null END) as Patients_OAPS_Inappropriate
--         , count(CASE WHEN a.WardLocDistanceHome is not null and c. ReasonOAT = 10 THEN a.Person_ID ELSE null END) as Distance_OAPS_Inappropriate
--         , count(CASE WHEN a.WardLocDistanceHome is null and c. ReasonOAT = 10 THEN a.Person_ID ELSE null END) as NoDistance_OAPS_Inappropriate*/
-- from $db_source.MHS502WardStay as a
-- -- inner join $db_source.MHS501HospProvSpell as b on a.HospProvSpellNum = b.HospProvSpellNum
-- -- inner join $db_source.MHS101Referral as c on b.ServiceRequestId = c.ServiceRequestId
-- -- where c.ReasonOAT is not null
-- --   and c.ReferralrequestReceivedDate <= c.ReportingPeriodEndDate
-- --   and (c.ServDischDate is null or c.ServDischDate > c.ReportingPeriodStartDate)
-- --group by a.uniqmonthid
-- --order by a.uniqmonthid


-- COMMAND ----------

create or replace temporary view WardStays as 

with WardStays_ordered
as
(
  select a.*, b.ReportingPeriodStartDate, b.ReportingPeriodEndDate
    ,row_number() over(partition by a.uniqmonthid, wardstayid order by StartDateWardStay desc, case when EndDateWardStay is null then 1 else 0 end desc) as stay_order
  from $reference_data.MHS502WardStay as a
  inner join (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate from $reference_data.MHS000Header) as b on a.UniqMonthID = b.UniqMonthID
)   
  
select *
from WardStays_ordered as a
where stay_order=1
  and a.StartDateWardStay between a.ReportingPeriodStartDate and a.ReportingPeriodEndDate
--  and (a.EndDateWardStay is null or a.EndDateWardStay >= a.ReportingPeriodStartDate);

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
  and a.ReferralrequestReceivedDate between a.ReportingPeriodStartDate and a.ReportingPeriodEndDate;
--  and (a.ServDischDate is null or a.ServDischDate >= a.ReportingPeriodStartDate);

select * from referrals

-- COMMAND ----------

select a.ReportingPeriodStartDate
        , a.uniqmonthid
        , count(a.Person_ID) as Patients
        , count(CASE WHEN a.WardLocDistanceHome is not null THEN a.Person_ID ELSE null END) as Distance
        , count(CASE WHEN a.WardLocDistanceHome is null THEN a.Person_ID ELSE null END) as NoDistance
        , count(CASE WHEN a.WardLocDistanceHome is null THEN a.Person_ID ELSE null END)/count(a.Person_ID) as NoDistance_perc
        , count(CASE WHEN COALESCE(c.ReasonOAT,c.OATReason) is not null THEN a.Person_ID ELSE null END) as Patients_OAPS
        , count(CASE WHEN a.WardLocDistanceHome is not null and c. ReasonOAT is not null THEN a.Person_ID ELSE null END) as Distance_OAPS
        , count(CASE WHEN a.WardLocDistanceHome is null and c. ReasonOAT is not null THEN a.Person_ID ELSE null END) as NoDistance_OAPS
        , count(CASE WHEN a.WardLocDistanceHome is null and c. ReasonOAT is not null THEN a.Person_ID ELSE null END)/count(CASE WHEN c. ReasonOAT is not null THEN a.Person_ID ELSE null END) as NoDistance_OAPS_perc
        , count(CASE WHEN COALESCE(c.ReasonOAT,c.OATReason) = 10 THEN a.Person_ID ELSE null END) as Patients_OAPS_Inappropriate
        , count(CASE WHEN a.WardLocDistanceHome is not null and c. ReasonOAT = 10 THEN a.Person_ID ELSE null END) as Distance_OAPS_Inappropriate
        , count(CASE WHEN a.WardLocDistanceHome is null and COALESCE(c.ReasonOAT,c.OATReason) = 10 THEN a.Person_ID ELSE null END) as NoDistance_OAPS_Inappropriate
        , count(CASE WHEN a.WardLocDistanceHome is null and COALESCE(c.ReasonOAT,c.OATReason) = 10 THEN a.Person_ID ELSE null END)/count(CASE WHEN COALESCE(c.ReasonOAT,c.OATReason) = 10 THEN a.Person_ID ELSE null END) as NoDistance_OAPS_Inappropriate_perc
from WardStays as a
inner join HospProvSpell as b on a.HospProvSpellID = b.HospProvSpellID and a.uniqmonthid = b.uniqmonthid
inner join referrals as c on b.ServiceRequestId = c.ServiceRequestId  and a.uniqmonthid = c.uniqmonthid
group by a.ReportingPeriodStartDate, a.uniqmonthid
order by a.ReportingPeriodStartDate desc, a.uniqmonthid
