# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY FUNCTION ward_stay_bed_days_rp(wardstayid STRING, end_date DATE, end_rp DATE, start_date DATE, start_rp DATE)
 RETURNS INT
 COMMENT 'calculate bed days for a given reporting period - either using start/end date of ward stay or hospital spell'
 RETURN datediff(
   CASE WHEN wardstayid is null then end_rp
        WHEN end_date is null or end_date > end_rp THEN DATE_ADD(end_rp, 1) ELSE end_date END,
   CASE WHEN start_date < start_rp and (end_date is null or end_date > start_rp) THEN start_rp
        WHEN start_date >= start_rp and (end_date is null or end_date > start_date) THEN start_date
        WHEN end_date is null or end_date > end_rp THEN end_rp
        ELSE end_date END
       )

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY FUNCTION hosp_spell_bed_days_rp(end_date DATE, end_rp DATE, start_date DATE, start_rp DATE)
 RETURNS INT
 COMMENT 'calculate bed days for a given reporting period - either using start/end date of ward stay or hospital spell'
 RETURN datediff(
   CASE WHEN end_date is null or end_date > end_rp THEN DATE_ADD(end_rp, 1) ELSE end_date END,
   CASE WHEN start_date < start_rp and (end_date is null or end_date > start_rp) THEN start_rp
        WHEN start_date >= start_rp and (end_date is null or end_date > start_date) THEN start_date
        WHEN end_date is null or end_date > end_rp THEN end_rp
        ELSE end_date END
       )

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.OAPS_In_Scope
 SELECT ORG_CODE
 FROM $reference_data.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
   AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
   AND ORG_CODE in ('NQL','NR5','R1A','R1C','R1F','R1L','RAT','RDY','RGD','RH5','RHA','RJ8','RKL','RLY','RMY','RNN',
                    'RNU','RP1','RP7','RPG','RQ3','RQY','RRE','RRP','RT1','RT2','RT5','RTQ','RTV','RV3','RV5','RV9',
                    'RVN','RW1','RW4','RW5','RWK','RWR','RWV','RWX','RX2','RX3','RX4','RXA','RXE','RXG','RXM','RXT',
                    'RXV','RXX','RXY','RYG','RYK','TAD','TAF','TAH','TAJ')

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.months
  
 select a.*
 from (
   select DISTINCT UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate
   from $db_source.MHS000Header as a
   where uniqmonthid between $end_month_id-11 and $end_month_id) AS a

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_hospprovspell
  
 select a.UniqServReqID,
        a.Person_ID,
        a.Uniqmonthid,
        a.OrgIDProv,
        a.UniqHospProvSpellID,
        b.ReportingPeriodStartDate,
        b.ReportingPeriodEndDate,
        a.StartDateHospProvSpell,
        a.DischDateHospProvSpell
 from $db_source.mhs501hospprovspell as a
 inner join $db_output.months as b
    on a.UniqMonthID = b.UniqMonthID
   and a.StartDateHospProvSpell <= b.ReportingPeriodEndDate
   and (a.DischDateHospProvSpell is null or a.DischDateHospProvSpell >= b.ReportingPeriodStartDate)
   and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate')  --Added recordenddate to get latest version of record
   and (a.RECORDSTARTDATE < '$rp_enddate') 

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW oaps_wardstay_prep AS

 select a.UniqWardStayID,
        CASE 
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 10 THEN 200
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 11 THEN 201
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 12 THEN 202
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 13 THEN 203
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 14 THEN 204
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 15 THEN 205
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 17 THEN 212
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 19 THEN 206
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 20 THEN 207
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 21 THEN 208
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 22 THEN 209
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 23 THEN 300
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 24 THEN 301
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 25 THEN 302
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 26 THEN 302
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 27 THEN 303
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 28 THEN 304
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 29 THEN 305
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 30 THEN 310
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 31 THEN 306
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 32 THEN 307
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 33 THEN 308
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 34 THEN 309
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 35 THEN 212
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 36 THEN 212
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 37 THEN 213
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 38 THEN 212
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 39 THEN 211
           WHEN $end_month_id > 1488 AND MHAdmittedPatientClass = 40 THEN 210
           ELSE MHAdmittedPatientClass
           END AS MHAdmittedPatientClass,
        a.WardType,
        a.BedDaysWSEndRP,
        a.Person_ID,
        a.Uniqmonthid,
        a.UniqHospProvSpellID,
        a.OrgIDProv,
        a.StartDateWardStay,
        a.EndDateWardStay,
        CASE WHEN a.UniqMonthID > 1488 AND a.MHAdmittedPatientClass IN ('200','201','202') THEN 'Y'
             WHEN a.UniqMonthID <= 1488 AND a.MHAdmittedPatientClass IN ('10','11','12') THEN 'Y'
             ELSE 'N'
             END AS Acute_Bed,
        a.RecordStartDate,
        a.RecordEndDate
       
 from $db_source.mhs502wardstay as a


# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_wardstay
  
 select a.UniqWardStayID,
        COALESCE(bt.MHAdmittedPatientClass, 'UNKNOWN') AS MHAdmittedPatientClass,
        COALESCE(bt.MHAdmittedPatientClassName, 'UNKNOWN') AS MHAdmittedPatientClassName,
        a.WardType,
        a.BedDaysWSEndRP,
        a.Person_ID,
        a.Uniqmonthid,
        a.UniqHospProvSpellID,
        a.OrgIDProv,
        b.ReportingPeriodStartDate,
        b.ReportingPeriodEndDate,
        a.StartDateWardStay,
        a.EndDateWardStay,
        a.Acute_Bed
       
 from oaps_wardstay_prep as a
 inner join $db_output.months as b
    on a.UniqMonthID = b.UniqMonthID
   and a.StartDateWardStay <= b.ReportingPeriodEndDate
   and (a.EndDateWardStay is null or a.EndDateWardStay >= b.ReportingPeriodStartDate)
   and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate')  --Added recordenddate to get latest version of record
   and (a.RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
 left join $db_output.hosp_bed_desc bt
   on a.MHAdmittedPatientClass = bt.MHAdmittedPatientClass and '$end_month_id' >= bt.FirstMonth and (bt.LastMonth is null or '$end_month_id' <= bt.LastMonth)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_referrals
  
 select a.UniqServReqID,
        a.ReferralRequestReceivedDate,
        a.Person_ID,
        a.ServDischDate,
        a.uniqmonthid,
        a.ReasonOAT,
        a.PrimReasonReferralMH,
        a.OrgIDProv,
        a.OrgIDReferringOrg
 from $db_source.MHS101Referral as a
 where uniqmonthid between $end_month_id-11 and $end_month_id
 --   and ReasonOAT is not null
   and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate')  --Added recordenddate to get latest version of record
   and (a.RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
   and ReasonOAT = '10'       --ADDED TO CHECK DATA QUALITY

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_onwardreferrals
  
 select a.UniqServReqID,
        a.Person_ID,
        a.Uniqmonthid,
        a.OrgIDProv,
        a.OATReason
 from $db_source.MHS105OnwardReferral as a
 where uniqmonthid between $end_month_id-11 and $end_month_id
 --   and OATReason is not null
   and OATReason = '10'       --ADDED TO CHECK DATA QUALITY

# COMMAND ----------

 %sql
 --NOTE: Need to look at which MHA records we are looking at
 --      We are potentially including people with null and informal legal status
 INSERT OVERWRITE TABLE $db_output.oaps_mha
  
 select a.UniqMonthID,
        a.legalstatuscode,
        a.Person_ID,
        a.OrgIDProv,
        a.StartDateMHActLegalStatusClass,
        a.UniqSubmissionID
 from (  
         select a.*
           ,row_number() over(partition by uniqmonthid, Person_ID, OrgIDProv order by StartDateMHActLegalStatusClass desc, UniqSubmissionID desc) as stay_order
         from $db_source.MHS401MHActPeriod as a
         where uniqmonthid between $end_month_id-11 and $end_month_id
           and legalstatuscode is not null              --EXCLUDES NOT POPULATED
           and legalstatuscode not in ('01','98','99')  --EXCLUDES INFORMAL AND NOT KNOW STATUS
           and (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE >= '$rp_enddate')  --Added recordenddate to get latest version of record
           and (a.RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
         ) AS a
 where stay_order=1

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.overlapping_oaps_referrals
  
 select a.UniqServReqID,
        a.ReferralRequestReceivedDate,
        a.Person_ID,
        a.ServDischDate,
        a.uniqmonthid,
        a.ReasonOAT,
        a.PrimReasonReferralMH,
        a.OrgIDProv,
        a.OrgIDReferringOrg,
        CASE WHEN b.NewDate is null THEN a.ServDischDate ELSE b.NewDate END as NewServDischDate
  
 from $db_output.oaps_referrals a
 left join (select FirstID, MAX(LateDate) NewDate
            from
               (
               select a.UniqServReqID as FirstId, b.UniqServReqID as LaterId, b.ReferralRequestReceivedDate as LateDate
               from $db_output.oaps_referrals as a
               inner join $db_output.oaps_referrals as b on a.Person_ID = b.Person_ID
               where a.UniqServReqID != b.UniqServReqID --unique ID?
                 and a.ReferralRequestReceivedDate < b.ReferralRequestReceivedDate
                 and (b.ReferralRequestReceivedDate < a.ServDischDate Or a.ServDischDate is null)
               )
             group by FirstID) AS b on a.UniqServReqID = b.FirstId

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS005PatInd_LD AS
  
 SELECT DISTINCT A.PERSON_ID, A.UNIQMONTHID, A.LDStatus
 FROM $db_source.MHS005PatInd  A
 INNER JOIN (SELECT PERSON_ID, UNIQMONTHID, RecordNumber, LDStatus, DENSE_RANK() OVER (PARTITION BY PERSON_ID ORDER BY UNIQMONTHID DESC, RecordNumber DESC) AS AUT_RANK
             FROM $db_source.MHS005PatInd 
             WHERE UNIQMONTHID <= '$end_month_id'
             and LDStatus in ('1', '2', '3', '4', '5', 'U', 'X', 'Z')) B ON B.AUT_RANK = 1 AND A.PERSON_ID = B.PERSON_ID AND A.UNIQMONTHID = B.UNIQMONTHID AND  A.RecordNumber = B.RecordNumber

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS005PatInd_AUT AS
  
 SELECT DISTINCT A.PERSON_ID, A.UNIQMONTHID, A.AutismStatus
 FROM $db_source.MHS005PatInd  A
 INNER JOIN (SELECT PERSON_ID, UNIQMONTHID, RecordNumber, AutismStatus, DENSE_RANK() OVER (PARTITION BY PERSON_ID ORDER BY UNIQMONTHID DESC, RecordNumber DESC) AS AUT_RANK
             FROM $db_source.MHS005PatInd 
             WHERE UNIQMONTHID <= '$end_month_id'
             and AutismStatus in ('1', '2', '3', '4', '5', 'U', 'X', 'Z')) B ON B.AUT_RANK = 1 AND A.PERSON_ID = B.PERSON_ID AND A.UNIQMONTHID = B.UNIQMONTHID AND  A.RecordNumber = B.RecordNumber

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_month
 select distinct 
         m.ReportingPeriodStartDate,
         m.ReportingPeriodEndDate,
         oaps.UniqMonthID,
         oaps.Person_ID,
         oaps.OrgIDProv,        
         ccg.IC_REC_CCG as SubICBGPRes,
         COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code,
         COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name,
         COALESCE(stp.STP_Code, 'UNKNOWN') as STP_Code,
         COALESCE(stp.STP_Name, 'UNKNOWN') as STP_Name,
         COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code,
         COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name,
         coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender,
         coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
         mpi.AgeRepPeriodEnd,
         coalesce(ab.Age_Group_OAPs, "UNKNOWN") as Age_Band,
         coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
         coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
         coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
         coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
         coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus, 
         coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc, 
         coalesce(ld.LDStatus, "UNKNOWN") as LDStatus, 
         coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc, 
         coalesce(oaps.ReasonOAT, "UNKNOWN") as ReasonOAT,
         coalesce(dd_reasonoat.Description, "UNKNOWN") as ReasonOATName,
         oaps.UniqServReqID,
         oaps.NewServDischDate,
         oaps.ReferralRequestReceivedDate,
         coalesce(oaps.PrimReasonReferralMH, "UNKNOWN") as PrimReasonReferralMH,
         coalesce(dd_reasonref.Description, "UNKNOWN") as PrimReasonReferralMHName,
         hs.UniqHospProvSpellID,
         ws.UniqWardStayID,
         CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClass
                ELSE 'UNKNOWN' END AS MHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS MHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.Acute_Bed
                ELSE 'UNKNOWN'
                END AS Acute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.MHAdmittedPatientClass
                ELSE 'UNKNOWN'
                END AS StartMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS StartMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.Acute_Bed
                ELSE 'UNKNOWN'
                END AS StartAcute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.MHAdmittedPatientClass
                ELSE 'UNKNOWN' 
                END AS ActiveMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS ActiveMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.Acute_Bed
                ELSE 'UNKNOWN'
                END AS ActiveAcute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.MHAdmittedPatientClass
                ELSE 'UNKNOWN'
                END AS EndMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS EndMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.Acute_Bed
                ELSE 'UNKNOWN'
                END AS EndAcute_Bed,
         ws.WardType,
         ws.BedDaysWSEndRP,
         oaps.OrgIDProv as OrgIDReceiving,
         rec_prov.NAME as ReceivingProvider_Name,
         coalesce(oaps.OrgIDReferring, "UNKNOWN") as OrgIDSubmitting,
         coalesce(subm_prov.NAME, "UNKNOWN") as SendingProvider_Name,
         null as LegalStatusCode, --MHA.LegalStatusCode
         null AS LegalStatusName, --dd_legal.Description AS LegalStatusName
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_1m') END as Bed_Days_Month_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_qtr') END as Bed_Days_Qtr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_12m') END as Bed_Days_Yr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_1m') END as Bed_Days_Month_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', ReferralRequestReceivedDate, '$rp_startdate_qtr') END as Bed_Days_Qtr_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', ReferralRequestReceivedDate, '$rp_startdate_12m') END as Bed_Days_Yr_HS,
         dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK,
         CASE WHEN isc.ORG_CODE is not null THEN 'Y' ELSE 'N' END as InScope,
         CASE WHEN ReferralRequestReceivedDate between "$rp_startdate_1m" and "$rp_enddate" THEN 1 ELSE 0 END as Received_In_RP,
         CASE WHEN m.ReportingPeriodStartDate between "$rp_startdate_1m" and "$rp_enddate" THEN 1 ELSE 0 END as Submitted_In_RP,
         CASE WHEN oaps.NewServDischDate between "$rp_startdate_1m" and "$rp_enddate" THEN 1 ELSE 0 END as Ended_In_RP,
         CASE WHEN oaps.NewServDischDate is null or oaps.NewServDischDate > '$rp_enddate' and oaps.UniqMonthID = '$end_month_id' THEN 1 ELSE 0 END as Active_End
  
 from (select * from $db_output.overlapping_oaps_referrals where UniqMonthID = '$end_month_id') as oaps
  
 inner join $db_output.months m on oaps.UniqMonthID = m.UniqMonthID
  
 left join (select *
            from $db_source.MHS001MPI
            where UniqMonthID = "$end_month_id"
            and PatMRecInRP = 'true'
            and (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')
            and (RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
            ) as mpi  -- WT question #1: all these joins are back to the sending provider when expected to be a mix of sender and receiver linked by person ID...?
    on mpi.Person_ID = oaps.Person_ID
  
 left join $db_output.oaps_hospprovspell hs --are these (this and ward stay) only taking data where they start in that particular month??  -- WT question #1++
   on oaps.UniqServReqID = hs.UniqServReqID
   and oaps.Person_ID = hs.Person_ID
   and oaps.OrgIDProv = hs.OrgIDProv
   and hs.UniqMonthID = "$end_month_id"
  
 left join $db_output.oaps_WardStay as WS  
     on OAPs.Person_ID = WS.Person_ID
    and HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS.OrgIDProv
    and WS.UniqMonthID = '$end_month_id'
  
 left join $db_output.oaps_WardStay as WS_Start  
     on OAPs.Person_ID = WS_Start.Person_ID
    and HS.UniqHospProvSpellID = WS_Start.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_Start.OrgIDProv
    and WS_Start.UniqMonthID = '$end_month_id'
    and OAPs.ReferralRequestReceivedDate between WS_start.Startdatewardstay and WS_Start.EndDateWardStay
  
 left join $db_output.oaps_WardStay as WS_Active 
     on OAPs.Person_ID = WS_Active.Person_ID
    and HS.UniqHospProvSpellID = WS_Active.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_Active.OrgIDProv
    and WS_Active.UniqMonthID = '$end_month_id'
    and (OAPs.NewServDischDate is null or OAPs.NewServDischDate > '$rp_enddate')
    and (WS_Active.EndDateWardStay is null or WS_Active.EndDateWardStay > '$rp_enddate')
  
 left join $db_output.oaps_WardStay as WS_End
     on OAPs.Person_ID = WS_End.Person_ID
    and HS.UniqHospProvSpellID = WS_End.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_End.OrgIDProv
    and WS_End.UniqMonthID = '$end_month_id'
    and OAPs.NewServDischDate between WS_End.Startdatewardstay and WS_End.EndDateWardStay
   
 left join $db_output.oaps_onwardreferrals onw
   on oaps.UniqServReqID = onw.UniqServReqID
   and oaps.Person_ID = onw.Person_ID
   and oaps.UniqMonthID = onw.UniqMonthID
   and oaps.OrgIDProv = onw.OrgIDProv
   and onw.UniqMonthID = "$end_month_id"
   
 left join $db_output.bbrb_ccg_in_month ccg
   on mpi.Person_ID = ccg.Person_ID
   
 left join $db_output.OAPS_In_Scope isc
   on LEFT(OAPs.OrgIDReferring,3) = isc.ORG_CODE
   
 left join $db_output.bbrb_stp_mapping stp
   on ccg.IC_Rec_CCG = stp.CCG_Code
   
 left join $db_output.bbrb_org_daily rec_prov
   on oaps.OrgIDProv = rec_prov.ORG_CODE
  
 left join $db_output.bbrb_org_daily subm_prov
   on oaps.OrgIDReferring = subm_prov.ORG_CODE
    
 left join $reference_data.english_indices_of_dep_v02 imd_ref
   on mpi.LSOA2011 = imd_ref.LSOA_CODE_2011 
   and imd_ref.IMD_YEAR = '2019' 
   
 left join $db_output.imd_desc imd
   on imd_ref.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
   
 left join $db_output.age_band_desc ab
   on mpi.AgeRepPeriodEnd = ab.AgeRepPeriodEnd and '$end_month_id' >= ab.FirstMonth and (ab.LastMonth is null or '$end_month_id' <= ab.LastMonth)
    
 left join $db_output.gender_desc gen
   on CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
           WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
           ELSE 'UNKNOWN' END = gen.Der_Gender
    and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
   
 left join $db_output.ethnicity_desc eth
   on mpi.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
   
 left join $reference_data.datadictionarycodes dd_reasonoat 
   on oaps.ReasonOAT = dd_reasonoat.PrimaryCode
   and dd_reasonoat.ItemName = 'REASON_FOR_OUT_OF_AREA_REFERRAL_FOR_ADULT_ACUTE_MENTAL_HEALTH'
   
 left join $reference_data.datadictionarycodes dd_reasonref
   on OAPs.PrimReasonReferralMH = dd_reasonref.PrimaryCode
   and dd_reasonref.ItemName = 'REASON_FOR_REFERRAL_TO_MENTAL_HEALTH' 
  
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on oaps.PERSON_ID = aut.PERSON_ID
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on oaps.PERSON_ID = ld.PERSON_ID
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_quarter
 select distinct 
         m.ReportingPeriodStartDate,
         m.ReportingPeriodEndDate,
         oaps.UniqMonthID,
         oaps.Person_ID,
         oaps.OrgIDProv,        
         ccg.SubICBGPRes,
         COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code,
         COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name,
         COALESCE(stp.STP_Code, 'UNKNOWN') as STP_Code,
         COALESCE(stp.STP_Name, 'UNKNOWN') as STP_Name,
         COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code,
         COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name,
         coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender,
         coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
         mpi.AgeRepPeriodEnd,
         coalesce(ab.Age_Group_OAPs, "UNKNOWN") as Age_Band,
         coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
         coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
         coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
         coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
         coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus, 
         coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc, 
         coalesce(ld.LDStatus, "UNKNOWN") as LDStatus, 
         coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc, 
         coalesce(oaps.ReasonOAT, "UNKNOWN") as ReasonOAT,
         coalesce(dd_reasonoat.Description, "UNKNOWN") as ReasonOATName,
         oaps.UniqServReqID,
         oaps.NewServDischDate,
         oaps.ReferralRequestReceivedDate,
         coalesce(oaps.PrimReasonReferralMH, "UNKNOWN") as PrimReasonReferralMH,
         coalesce(dd_reasonref.Description, "UNKNOWN") as PrimReasonReferralMHName,
         hs.UniqHospProvSpellID,
         ws.UniqWardStayID,
         CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClass
                ELSE 'UNKNOWN' END AS MHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS MHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.Acute_Bed
                ELSE 'UNKNOWN'
                END AS Acute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.MHAdmittedPatientClass
                ELSE 'UNKNOWN'
                END AS StartMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS StartMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.Acute_Bed
                ELSE 'UNKNOWN'
                END AS StartAcute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.MHAdmittedPatientClass
                ELSE 'UNKNOWN' 
                END AS ActiveMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS ActiveMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.Acute_Bed
                ELSE 'UNKNOWN'
                END AS ActiveAcute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.MHAdmittedPatientClass
                ELSE 'UNKNOWN'
                END AS EndMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS EndMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.Acute_Bed
                ELSE 'UNKNOWN'
                END AS EndAcute_Bed,
         ws.WardType,
         ws.BedDaysWSEndRP,
         oaps.OrgIDProv as OrgIDReceiving,
         rec_prov.NAME as ReceivingProvider_Name,
         coalesce(oaps.OrgIDReferring, "UNKNOWN") as OrgIDSubmitting,
         coalesce(subm_prov.NAME, "UNKNOWN") as SendingProvider_Name,
         null as LegalStatusCode, --MHA.LegalStatusCode
         null AS LegalStatusName, --dd_legal.Description AS LegalStatusName
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_1m') END as Bed_Days_Month_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_qtr') END as Bed_Days_Qtr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_12m') END as Bed_Days_Yr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_1m') END as Bed_Days_Month_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', ReferralRequestReceivedDate, '$rp_startdate_qtr') END as Bed_Days_Qtr_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', ReferralRequestReceivedDate, '$rp_startdate_12m') END as Bed_Days_Yr_HS,
         dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK,
         CASE WHEN isc.ORG_CODE is not null THEN 'Y' ELSE 'N' END as InScope,
         CASE WHEN ReferralRequestReceivedDate between "$rp_startdate_qtr" and "$rp_enddate" THEN 1 ELSE 0 END as Received_In_RP,
         CASE WHEN m.ReportingPeriodStartDate between "$rp_startdate_qtr" and "$rp_enddate" THEN 1 ELSE 0 END as Submitted_In_RP,
         CASE WHEN oaps.NewServDischDate between "$rp_startdate_qtr" and "$rp_enddate" THEN 1 ELSE 0 END as Ended_In_RP,
         CASE WHEN oaps.NewServDischDate is null or oaps.NewServDischDate > '$rp_enddate' and oaps.UniqMonthID = '$end_month_id' THEN 1 ELSE 0 END as Active_End
  
 from (select * from $db_output.overlapping_oaps_referrals where UniqMonthID between $end_month_id-2 and $end_month_id) as oaps
  
 inner join $db_output.months m on oaps.UniqMonthID = m.UniqMonthID
  
 left join (SELECT * FROM $db_source.MHS001MPI
 where Uniqmonthid between $end_month_id-11 and $end_month_id and PatMRecInRP = 'true') as MPI  -- WT question #1: all these joins are back to the sending provider when expected to be a mix of sender and receiver linked by person ID...?
    on MPI.Person_ID = OAPs.Person_ID
   and MPI.UniqMonthID = OAPs.UniqMonthID
  
 left join $db_output.oaps_hospprovspell hs --are these (this and ward stay) only taking data where they start in that particular month??  -- WT question #1++
   on oaps.UniqServReqID = hs.UniqServReqID
   and oaps.Person_ID = hs.Person_ID
   and oaps.OrgIDProv = hs.OrgIDProv
   and hs.UniqMonthID between $end_month_id-2 and $end_month_id
  
 left join  $db_output.oaps_WardStay as WS  
     on OAPs.Person_ID = WS.Person_ID
    and HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS.OrgIDProv
    and WS.UniqMonthID between $end_month_id-2 and $end_month_id
  
 left join $db_output.oaps_WardStay as WS_Start  
     on OAPs.Person_ID = WS_Start.Person_ID
    and HS.UniqHospProvSpellID = WS_Start.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_Start.OrgIDProv
    and WS_Start.UniqMonthID between $end_month_id-2 and $end_month_id
    and OAPs.ReferralRequestReceivedDate between WS_start.Startdatewardstay and WS_Start.EndDateWardStay
  
 left join $db_output.oaps_WardStay as WS_Active 
     on OAPs.Person_ID = WS_Active.Person_ID
    and HS.UniqHospProvSpellID = WS_Active.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_Active.OrgIDProv
    and WS_Active.UniqMonthID between $end_month_id-2 and $end_month_id
    and (OAPs.NewServDischDate is null or OAPs.NewServDischDate > '$rp_enddate')
    and (WS_Active.EndDateWardStay is null or WS_Active.EndDateWardStay > '$rp_enddate')
  
 left join $db_output.oaps_WardStay as WS_End
     on OAPs.Person_ID = WS_End.Person_ID
    and HS.UniqHospProvSpellID = WS_End.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_End.OrgIDProv
    and WS_End.UniqMonthID between $end_month_id-2 and $end_month_id
    and OAPs.NewServDischDate between WS_End.Startdatewardstay and WS_End.EndDateWardStay 
   
 left join $db_output.oaps_onwardreferrals onw
   on oaps.UniqServReqID = onw.UniqServReqID
   and oaps.Person_ID = onw.Person_ID
   and oaps.UniqMonthID = onw.UniqMonthID
   and oaps.OrgIDProv = onw.OrgIDProv
   and onw.UniqMonthID  between $end_month_id-2 and $end_month_id
   
 left join $db_output.bbrb_ccg_in_quarter ccg
   on mpi.Person_ID = ccg.Person_ID
   
 left join $db_output.OAPS_In_Scope isc
   on LEFT(OAPs.OrgIDReferring,3) = isc.ORG_CODE
   
 left join $db_output.bbrb_stp_mapping stp
   on ccg.SubICBGPRes = stp.CCG_Code
   
 left join $db_output.bbrb_org_daily rec_prov
   on oaps.OrgIDProv = rec_prov.ORG_CODE
  
 left join $db_output.bbrb_org_daily subm_prov
   on oaps.OrgIDReferring = subm_prov.ORG_CODE
    
 left join $reference_data.english_indices_of_dep_v02 imd_ref
   on mpi.LSOA2011 = imd_ref.LSOA_CODE_2011 
   and imd_ref.IMD_YEAR = '2019' 
   
 left join $db_output.imd_desc imd
   on imd_ref.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
   
 left join $db_output.age_band_desc ab
   on mpi.AgeRepPeriodEnd = ab.AgeRepPeriodEnd and '$end_month_id' >= ab.FirstMonth and (ab.LastMonth is null or '$end_month_id' <= ab.LastMonth)
    
 left join $db_output.gender_desc gen
   on CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
              WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
               ELSE 'UNKNOWN' END = gen.Der_Gender
   and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
   
 left join $db_output.ethnicity_desc eth
   on mpi.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
   
 left join $reference_data.datadictionarycodes dd_reasonoat 
   on oaps.ReasonOAT = dd_reasonoat.PrimaryCode
   and dd_reasonoat.ItemName = 'REASON_FOR_OUT_OF_AREA_REFERRAL_FOR_ADULT_ACUTE_MENTAL_HEALTH'
   
 left join $reference_data.datadictionarycodes dd_reasonref
   on OAPs.PrimReasonReferralMH = dd_reasonref.PrimaryCode
   and dd_reasonref.ItemName = 'REASON_FOR_REFERRAL_TO_MENTAL_HEALTH' 
  
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on oaps.PERSON_ID = aut.PERSON_ID and oaps.uniqmonthid >= aut.uniqmonthid
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
                                   
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on oaps.PERSON_ID = ld.PERSON_ID and oaps.uniqmonthid >= ld.uniqmonthid
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_year
 select distinct 
         m.ReportingPeriodStartDate,
         m.ReportingPeriodEndDate,
         oaps.UniqMonthID,
         oaps.Person_ID,
         oaps.OrgIDProv,        
         ccg.SubICBGPRes,
         COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code,
         COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name,
         COALESCE(stp.STP_Code, 'UNKNOWN') as STP_Code,
         COALESCE(stp.STP_Name, 'UNKNOWN') as STP_Name,
         COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code,
         COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name,
         coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender,
         coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
         mpi.AgeRepPeriodEnd,
         coalesce(ab.Age_Group_OAPs, "UNKNOWN") as Age_Band,
         coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
         coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
         coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
         coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
         coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus,
         coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc,
         coalesce(ld.LDStatus, "UNKNOWN") as LDStatus,
         coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc,
         coalesce(oaps.ReasonOAT, "UNKNOWN") as ReasonOAT,
         coalesce(dd_reasonoat.Description, "UNKNOWN") as ReasonOATName,
         oaps.UniqServReqID,
         oaps.NewServDischDate,
         oaps.ReferralRequestReceivedDate,
         coalesce(oaps.PrimReasonReferralMH, "UNKNOWN") as PrimReasonReferralMH,
         coalesce(dd_reasonref.Description, "UNKNOWN") as PrimReasonReferralMHName,
         hs.UniqHospProvSpellID,
         ws.UniqWardStayID,
         CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClass
                ELSE 'UNKNOWN' END AS MHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS MHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.Acute_Bed
                ELSE 'UNKNOWN'
                END AS Acute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.MHAdmittedPatientClass
                ELSE 'UNKNOWN'
                END AS StartMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS StartMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Start.UniqHospProvSpellID is not NULL THEN WS_Start.Acute_Bed
                ELSE 'UNKNOWN'
                END AS StartAcute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.MHAdmittedPatientClass
                ELSE 'UNKNOWN' 
                END AS ActiveMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS ActiveMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_Active.UniqHospProvSpellID is not NULL THEN WS_Active.Acute_Bed
                ELSE 'UNKNOWN'
                END AS ActiveAcute_Bed
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.MHAdmittedPatientClass
                ELSE 'UNKNOWN'
                END AS EndMHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS EndMHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS_End.UniqHospProvSpellID is not NULL THEN WS_End.Acute_Bed
                ELSE 'UNKNOWN'
                END AS EndAcute_Bed,
         ws.WardType,
         ws.BedDaysWSEndRP,
         oaps.OrgIDProv as OrgIDReceiving,
         rec_prov.NAME as ReceivingProvider_Name,
         coalesce(oaps.OrgIDReferring, "UNKNOWN") as OrgIDSubmitting,
         coalesce(subm_prov.NAME, "UNKNOWN") as SendingProvider_Name,
         null as LegalStatusCode, --MHA.LegalStatusCode
         null AS LegalStatusName, --dd_legal.Description AS LegalStatusName
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_1m') END as Bed_Days_Month_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_qtr') END as Bed_Days_Qtr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_12m') END as Bed_Days_Yr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_1m') END as Bed_Days_Month_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', ReferralRequestReceivedDate, '$rp_startdate_qtr') END as Bed_Days_Qtr_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', ReferralRequestReceivedDate, '$rp_startdate_12m') END as Bed_Days_Yr_HS,
         dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK,
         CASE WHEN isc.ORG_CODE is not null THEN 'Y' ELSE 'N' END as InScope,
         CASE WHEN ReferralRequestReceivedDate between "$rp_startdate_12m" and "$rp_enddate" THEN 1 ELSE 0 END as Received_In_RP,
         CASE WHEN m.ReportingPeriodStartDate between "$rp_startdate_12m" and "$rp_enddate" THEN 1 ELSE 0 END as Submitted_In_RP,        
         CASE WHEN oaps.NewServDischDate between "$rp_startdate_12m" and "$rp_enddate" THEN 1 ELSE 0 END as Ended_In_RP,
         CASE WHEN oaps.NewServDischDate is null or oaps.NewServDischDate > '$rp_enddate' and oaps.UniqMonthID = '$end_month_id' THEN 1 ELSE 0 END as Active_End
  
 from (select * from $db_output.overlapping_oaps_referrals where UniqMonthID between $end_month_id-11 and $end_month_id) as oaps
  
 inner join $db_output.months m on oaps.UniqMonthID = m.UniqMonthID
  
 left join (SELECT * FROM $db_source.MHS001MPI
 where Uniqmonthid between $end_month_id-11 and $end_month_id and PatMRecInRP = 'true') as MPI  -- WT question #1: all these joins are back to the sending provider when expected to be a mix of sender and receiver linked by person ID...?
    on MPI.Person_ID = OAPs.Person_ID
   and MPI.UniqMonthID = OAPs.UniqMonthID
  
 left join $db_output.oaps_hospprovspell hs --are these (this and ward stay) only taking data where they start in that particular month??  -- WT question #1++
   on oaps.UniqServReqID = hs.UniqServReqID
   and oaps.Person_ID = hs.Person_ID
   and oaps.OrgIDProv = hs.OrgIDProv
   and hs.UniqMonthID between $end_month_id-11 and $end_month_id
  
 left join  $db_output.oaps_WardStay as WS  -- WT question #1++
     on OAPs.Person_ID = WS.Person_ID
  --   and OAPs.Uniqmonthid = WS.Uniqmonthid
    and HS.UniqHospProvSpellID = WS.UniqHospProvSpellID -- WT question #2: why is the HS join included here as this is a WS and OAPS join?
    and OAPs.OrgIDProv = WS.OrgIDProv
    and WS.UniqMonthID between $end_month_id-11 and $end_month_id
  
  left join $db_output.oaps_WardStay as WS_Start  
     on OAPs.Person_ID = WS_Start.Person_ID
    and HS.UniqHospProvSpellID = WS_Start.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_Start.OrgIDProv
    and WS_Start.UniqMonthID between $end_month_id-11 and $end_month_id
    and OAPs.ReferralRequestReceivedDate between WS_start.Startdatewardstay and WS_Start.EndDateWardStay
  
 left join $db_output.oaps_WardStay as WS_Active 
     on OAPs.Person_ID = WS_Active.Person_ID
    and HS.UniqHospProvSpellID = WS_Active.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_Active.OrgIDProv
    and WS_Active.UniqMonthID between $end_month_id-11 and $end_month_id
    and (OAPs.NewServDischDate is null or OAPs.NewServDischDate > '$rp_enddate')
    and (WS_Active.EndDateWardStay is null or WS_Active.EndDateWardStay > '$rp_enddate')
  
 left join $db_output.oaps_WardStay as WS_End
     on OAPs.Person_ID = WS_End.Person_ID
    and HS.UniqHospProvSpellID = WS_End.UniqHospProvSpellID 
    and OAPs.OrgIDProv = WS_End.OrgIDProv
    and WS_End.UniqMonthID between $end_month_id-11 and $end_month_id
    and OAPs.NewServDischDate between WS_End.Startdatewardstay and WS_End.EndDateWardStay 
   
 left join $db_output.oaps_onwardreferrals onw
   on oaps.UniqServReqID = onw.UniqServReqID
   and oaps.Person_ID = onw.Person_ID
   and oaps.UniqMonthID = onw.UniqMonthID
   and oaps.OrgIDProv = onw.OrgIDProv
   and onw.UniqMonthID  between $end_month_id-11 and $end_month_id
   
 left join $db_output.bbrb_ccg_in_year ccg
   on mpi.Person_ID = ccg.Person_ID
   
 left join $db_output.OAPS_In_Scope isc
   on LEFT(OAPs.OrgIDReferring,3) = isc.ORG_CODE
   
 left join $db_output.bbrb_stp_mapping stp
   on ccg.SubICBGPRes = stp.CCG_Code
   
 left join $db_output.bbrb_org_daily rec_prov
   on oaps.OrgIDProv = rec_prov.ORG_CODE
  
 left join $db_output.bbrb_org_daily subm_prov
   on oaps.OrgIDReferring = subm_prov.ORG_CODE
    
 left join $reference_data.english_indices_of_dep_v02 imd_ref
   on mpi.LSOA2011 = imd_ref.LSOA_CODE_2011 
   and imd_ref.IMD_YEAR = '2019' 
   
 left join $db_output.imd_desc imd
   on imd_ref.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
   
 left join $db_output.age_band_desc ab
   on mpi.AgeRepPeriodEnd = ab.AgeRepPeriodEnd and '$end_month_id' >= ab.FirstMonth and (ab.LastMonth is null or '$end_month_id' <= ab.LastMonth)
    
 left join $db_output.gender_desc gen
   on CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
              WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
               ELSE 'UNKNOWN' END = gen.Der_Gender
   and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
   
 left join $db_output.ethnicity_desc eth
   on mpi.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
   
 left join $reference_data.datadictionarycodes dd_reasonoat 
   on oaps.ReasonOAT = dd_reasonoat.PrimaryCode
   and dd_reasonoat.ItemName = 'REASON_FOR_OUT_OF_AREA_REFERRAL_FOR_ADULT_ACUTE_MENTAL_HEALTH'
   
 left join $reference_data.datadictionarycodes dd_reasonref
   on OAPs.PrimReasonReferralMH = dd_reasonref.PrimaryCode
   and dd_reasonref.ItemName = 'REASON_FOR_REFERRAL_TO_MENTAL_HEALTH' 
  
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on oaps.PERSON_ID = aut.PERSON_ID and oaps.uniqmonthid >= aut.uniqmonthid
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on oaps.PERSON_ID = ld.PERSON_ID and oaps.uniqmonthid >= ld.uniqmonthid
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.oaps_month;
 OPTIMIZE $db_output.oaps_quarter;
 OPTIMIZE $db_output.oaps_year;

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_all_admissions_month
 select  hs.UniqMonthID,
         hs.UniqHospProvSpellID,
         ws.UniqWardStayID,
         COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code,
         COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name,
         COALESCE(stp.STP_Code, 'UNKNOWN') as STP_Code,
         COALESCE(stp.STP_Name, 'UNKNOWN') as STP_Name,
         COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code,
         COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name,
         coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender,
         coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
         mpi.AgeRepPeriodEnd,
         coalesce(ab.Age_Group_OAPs, "UNKNOWN") as Age_Band,
         coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
         coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
         coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
         coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,    
         coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus,
         coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc,
         coalesce(ld.LDStatus, "UNKNOWN") as LDStatus,
         coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc,
         CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClass
                ELSE 'UNKNOWN' END AS MHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS MHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.Acute_Bed
                ELSE 'UNKNOWN'
                END AS Acute_Bed,
         ws.StartDateWardStay,
         ws.EndDateWardStay,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_1m') END as Bed_Days_Month_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_qtr') END as Bed_Days_Qtr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_12m') END as Bed_Days_Yr_WS,
         hs.StartDateHospProvSpell,
         hs.DischDateHospProvSpell,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_1m') END as Bed_Days_Month_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_qtr') END as Bed_Days_Qtr_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_12m') END as Bed_Days_Yr_HS,
         dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK,
          CASE WHEN hs.StartDateHospProvSpell between "$rp_startdate_1m" and "$rp_enddate" THEN 1 ELSE 0 END as Received_In_RP,
         CASE WHEN hs.ReportingPeriodStartDate between "$rp_startdate_1m" and "$rp_enddate" THEN 1 ELSE 0 END as Submitted_In_RP,
         CASE WHEN hs.DischDateHospProvSpell between "$rp_startdate_1m" and "$rp_enddate" THEN 1 ELSE 0 END as Ended_In_RP,
         CASE WHEN (hs.DischDateHospProvSpell is null or hs.DischDateHospProvSpell > '$rp_enddate') and hs.UniqMonthID = '$end_month_id' THEN 1 ELSE 0 END as Active_End
  
 from (select * from $db_output.oaps_hospprovspell where UniqMonthID = "$end_month_id") hs 
  
 left join (select *
            from $db_source.MHS001MPI
            where UniqMonthID = "$end_month_id"
            and PatMRecInRP = 'true'
            and (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')
            and (RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
            ) as mpi  -- WT question #1: all these joins are back to the sending provider when expected to be a mix of sender and receiver linked by person ID...?
    on hs.Person_ID = mpi.Person_ID
  
 left join $db_output.oaps_WardStay as WS
    on HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
    and HS.OrgIDProv = WS.OrgIDProv
    and WS.UniqMonthID = '$end_month_id'
    
 left join $db_output.bbrb_ccg_in_year ccg
   on mpi.Person_ID = ccg.Person_ID
     
 left join $db_output.bbrb_stp_mapping stp
   on ccg.SubICBGPRes = stp.CCG_Code
    
 left join $reference_data.english_indices_of_dep_v02 imd_ref
   on mpi.LSOA2011 = imd_ref.LSOA_CODE_2011 
   and imd_ref.IMD_YEAR = '2019' 
   
 left join $db_output.imd_desc imd
   on imd_ref.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
   
 left join $db_output.age_band_desc ab
   on mpi.AgeRepPeriodEnd = ab.AgeRepPeriodEnd and '$end_month_id' >= ab.FirstMonth and (ab.LastMonth is null or '$end_month_id' <= ab.LastMonth)
    
 left join $db_output.gender_desc gen
   on CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
              WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
               ELSE 'UNKNOWN' END = gen.Der_Gender
   and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
   
 left join $db_output.ethnicity_desc eth
   on mpi.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth) 
  
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on hs.PERSON_ID = aut.PERSON_ID
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on hs.PERSON_ID = ld.PERSON_ID
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_all_admissions_quarter
 select  hs.UniqMonthID,
         hs.UniqHospProvSpellID,
         ws.UniqWardStayID,
         COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code,
         COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name,
         COALESCE(stp.STP_Code, 'UNKNOWN') as STP_Code,
         COALESCE(stp.STP_Name, 'UNKNOWN') as STP_Name,
         COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code,
         COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name,
         coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender,
         coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
         mpi.AgeRepPeriodEnd,
         coalesce(ab.Age_Group_OAPs, "UNKNOWN") as Age_Band,
         coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
         coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
         coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
         coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
         coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus,
         coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc,
         coalesce(ld.LDStatus, "UNKNOWN") as LDStatus,
         coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc,
         CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClass
                ELSE 'UNKNOWN' END AS MHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS MHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.Acute_Bed
                ELSE 'UNKNOWN'
                END AS Acute_Bed,
         ws.StartDateWardStay,
         ws.EndDateWardStay,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_1m') END as Bed_Days_Month_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_qtr') END as Bed_Days_Qtr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_12m') END as Bed_Days_Yr_WS,
         hs.StartDateHospProvSpell,
         hs.DischDateHospProvSpell,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_1m') END as Bed_Days_Month_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_qtr') END as Bed_Days_Qtr_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_12m') END as Bed_Days_Yr_HS,
         dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK,
         CASE WHEN hs.StartDateHospProvSpell between "$rp_startdate_qtr" and "$rp_enddate" THEN 1 ELSE 0 END as Received_In_RP,
         CASE WHEN hs.ReportingPeriodStartDate between "$rp_startdate_qtr" and "$rp_enddate" THEN 1 ELSE 0 END as Submitted_In_RP,
         CASE WHEN hs.DischDateHospProvSpell between "$rp_startdate_qtr" and "$rp_enddate" THEN 1 ELSE 0 END as Ended_In_RP,
         CASE WHEN (hs.DischDateHospProvSpell is null or hs.DischDateHospProvSpell > '$rp_enddate') and hs.UniqMonthID = '$end_month_id' THEN 1 ELSE 0 END as Active_End
  
 from (select * from $db_output.oaps_hospprovspell where UniqMonthID between $end_month_id-2 and $end_month_id) hs 
  
 left join (select *
            from $db_source.MHS001MPI
            where UniqMonthID between $end_month_id-2 and $end_month_id
            and PatMRecInRP = 'true'
            and (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')
            and (RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
            ) as mpi  -- WT question #1: all these joins are back to the sending provider when expected to be a mix of sender and receiver linked by person ID...?
    on hs.Person_ID = mpi.Person_ID
  
 left join $db_output.oaps_WardStay as WS
    on HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
    and HS.OrgIDProv = WS.OrgIDProv
    and WS.UniqMonthID between $end_month_id-2 and $end_month_id
    
 left join $db_output.bbrb_ccg_in_year ccg
   on mpi.Person_ID = ccg.Person_ID
     
 left join $db_output.bbrb_stp_mapping stp
   on ccg.SubICBGPRes = stp.CCG_Code
    
 left join $reference_data.english_indices_of_dep_v02 imd_ref
   on mpi.LSOA2011 = imd_ref.LSOA_CODE_2011 
   and imd_ref.IMD_YEAR = '2019' 
   
 left join $db_output.imd_desc imd
   on imd_ref.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
   
 left join $db_output.age_band_desc ab
   on mpi.AgeRepPeriodEnd = ab.AgeRepPeriodEnd and '$end_month_id' >= ab.FirstMonth and (ab.LastMonth is null or '$end_month_id' <= ab.LastMonth)
    
 left join $db_output.gender_desc gen
   on CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
              WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
               ELSE 'UNKNOWN' END = gen.Der_Gender
   and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
   
 left join $db_output.ethnicity_desc eth
   on mpi.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
   
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on hs.PERSON_ID = aut.PERSON_ID and hs.uniqmonthid >= aut.uniqmonthid
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
                                   
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on hs.PERSON_ID = ld.PERSON_ID and hs.uniqmonthid >= ld.uniqmonthid
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.oaps_all_admissions_year
 select  hs.UniqMonthID,
         hs.UniqHospProvSpellID,
         ws.UniqWardStayID,
         COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code,
         COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name,
         COALESCE(stp.STP_Code, 'UNKNOWN') as STP_Code,
         COALESCE(stp.STP_Name, 'UNKNOWN') as STP_Name,
         COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code,
         COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name,
         coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender,
         coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
         mpi.AgeRepPeriodEnd,
         coalesce(ab.Age_Group_OAPs, "UNKNOWN") as Age_Band,
         coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
         coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
         coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
         coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,  
         coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus,
         coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc,
         coalesce(ld.LDStatus, "UNKNOWN") as LDStatus,
         coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc,
         CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClass
                ELSE 'UNKNOWN' END AS MHAdmittedPatientClass
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.MHAdmittedPatientClassName
                ELSE 'UNKNOWN'
                END AS MHAdmittedPatientClassName
          ,CASE WHEN HS.UniqHospProvSpellID is NULL THEN 'No Hospital Spell'
                WHEN WS.UniqHospProvSpellID is not NULL THEN WS.Acute_Bed
                ELSE 'UNKNOWN'
                END AS Acute_Bed,
         ws.StartDateWardStay,
         ws.EndDateWardStay,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_1m') END as Bed_Days_Month_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_qtr') END as Bed_Days_Qtr_WS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE ward_stay_bed_days_rp(ws.UniqWardStayID, ws.EndDateWardStay, '$rp_enddate', ws.StartDateWardStay, '$rp_startdate_12m') END as Bed_Days_Yr_WS,
         hs.StartDateHospProvSpell,
         hs.DischDateHospProvSpell,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_1m') END as Bed_Days_Month_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_qtr') END as Bed_Days_Qtr_HS,
         CASE WHEN hs.UniqHospProvSpellID is NULL THEN 0
              ELSE hosp_spell_bed_days_rp(DischDateHospProvSpell, '$rp_enddate', StartDateHospProvSpell, '$rp_startdate_12m') END as Bed_Days_Yr_HS,
         dense_rank() OVER (PARTITION BY HS.UniqHospProvSpellID ORDER BY WS.UniqWardStayID DESC) AS RANK,
         CASE WHEN hs.StartDateHospProvSpell between "$rp_startdate_12m" and "$rp_enddate" THEN 1 ELSE 0 END as Received_In_RP,
         CASE WHEN hs.ReportingPeriodStartDate between "$rp_startdate_12m" and "$rp_enddate" THEN 1 ELSE 0 END as Submitted_In_RP,        
         CASE WHEN hs.DischDateHospProvSpell between "$rp_startdate_12m" and "$rp_enddate" THEN 1 ELSE 0 END as Ended_In_RP,
         CASE WHEN (hs.DischDateHospProvSpell is null or hs.DischDateHospProvSpell > '$rp_enddate') and hs.UniqMonthID = '$end_month_id' THEN 1 ELSE 0 END as Active_End
  
 from (select * from $db_output.oaps_hospprovspell where UniqMonthID between $end_month_id-11 and $end_month_id) hs 
  
 left join (select *
            from $db_source.MHS001MPI
            where UniqMonthID between $end_month_id-11 and $end_month_id
            and PatMRecInRP = 'true'
            and (RECORDENDDATE IS NULL OR RECORDENDDATE >= '$rp_enddate')
            and (RECORDSTARTDATE < '$rp_enddate')                             --Added recordestartdate to ensure record isn't from after the reporting period
            ) as mpi  -- WT question #1: all these joins are back to the sending provider when expected to be a mix of sender and receiver linked by person ID...?
    on hs.Person_ID = mpi.Person_ID
  
 left join $db_output.oaps_WardStay as WS
    on HS.UniqHospProvSpellID = WS.UniqHospProvSpellID 
    and HS.OrgIDProv = WS.OrgIDProv
    and WS.UniqMonthID between $end_month_id-11 and $end_month_id
    
 left join $db_output.bbrb_ccg_in_year ccg
   on mpi.Person_ID = ccg.Person_ID
     
 left join $db_output.bbrb_stp_mapping stp
   on ccg.SubICBGPRes = stp.CCG_Code
    
 left join $reference_data.english_indices_of_dep_v02 imd_ref
   on mpi.LSOA2011 = imd_ref.LSOA_CODE_2011 
   and imd_ref.IMD_YEAR = '2019' 
   
 left join $db_output.imd_desc imd
   on imd_ref.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
   
 left join $db_output.age_band_desc ab
   on mpi.AgeRepPeriodEnd = ab.AgeRepPeriodEnd and '$end_month_id' >= ab.FirstMonth and (ab.LastMonth is null or '$end_month_id' <= ab.LastMonth)
    
 left join $db_output.gender_desc gen
   on CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
              WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
               ELSE 'UNKNOWN' END = gen.Der_Gender
   and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
   
 left join $db_output.ethnicity_desc eth
   on mpi.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth) 
  
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on hs.PERSON_ID = aut.PERSON_ID and hs.uniqmonthid >= aut.uniqmonthid
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on hs.PERSON_ID = ld.PERSON_ID and hs.uniqmonthid >= ld.uniqmonthid
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.oaps_all_admissions_month;
 OPTIMIZE $db_output.oaps_all_admissions_quarter;
 OPTIMIZE $db_output.oaps_all_admissions_year;