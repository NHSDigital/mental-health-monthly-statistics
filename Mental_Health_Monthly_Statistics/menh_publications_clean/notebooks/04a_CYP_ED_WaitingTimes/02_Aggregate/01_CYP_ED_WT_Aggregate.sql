-- Databricks notebook source
%py
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate
rp_enddate=dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01 - ED85- National - Total referrals for children and young people with eating disorder entering treatment in RP
%sql
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED85' AS METRIC
       ,COUNT(DISTINCT UniqServReqID) METRIC_VALUE
       ,SOURCE_DB
  FROM $db_output.CYP_ED_WT_STEP4 
  WHERE UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
  group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01 - ED85 - Provider- Total referrals for children and young people with eating disorder entering treatment in RP
%sql
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED85' AS METRIC
       ,COUNT(DISTINCT UniqServReqID) METRIC_VALUE
       ,SOURCE_DB
 FROM $db_output.CYP_ED_WT_STEP4  step4
 WHERE UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
 group by step4.orgidprov, SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01 - ED85 - CCG - Total referrals for children and young people with eating disorder entering treatment in RP
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED85' AS METRIC
       ,COUNT(DISTINCT UniqServReqID) METRIC_VALUE
       ,SOURCE_DB
 FROM $db_output.CYP_ED_WT_STEP4  step4
 left join $db_output.MHS001_CCG_LATEST ccg
 on step4.Person_ID = ccg.Person_ID
 WHERE UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
 group by ccg.IC_Rec_CCG,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01 - ED85 - STP- Total referrals for children and young people with eating disorder entering treatment in RP
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(stp.STP_CODE,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED85' AS METRIC
       ,COUNT(DISTINCT UniqServReqID) METRIC_VALUE
       ,SOURCE_DB
 FROM $db_output.CYP_ED_WT_STEP4  step4
 left join $db_output.MHS001_CCG_LATEST ccg
 on step4.Person_ID = ccg.Person_ID
 left join $db_output.STP_Region_mapping_post_2020 stp
 on ccg.IC_Rec_CCG = stp.CCG_CODE
 WHERE UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
 group by stp.STP_CODE,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01a - ED86 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED86' as METRIC
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 as step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01a - ED86 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED86' as METRIC
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4  step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by step4.orgidprov,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01a - ED86 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED86' as METRIC
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
FROM $db_output.CYP_ED_WT_STEP4   step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)


left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by IC_Rec_CCG,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01a - ED86 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(stp.STP_CODE,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED86' as METRIC
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
FROM $db_output.CYP_ED_WT_STEP4   step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)


left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - NATIONAL PREP
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01aa_ad_national_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED86a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED86b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED86c'
             WHEN  step4.waiting_time > 12 THEN 'ED86d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4  step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by UniqServReqID,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ae - ED86 - NATIONAL
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01aa_ad_national_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - Provider - PREP
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01aa_ad_provider_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED86a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED86b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED86c'
             WHEN  step4.waiting_time > 12 THEN 'ED86d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4  step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by OrgIDProv,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01aa_ad_provider_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - CCG - PREP
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01aa_ad_ccg_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED86a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED86b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED86c'
             WHEN  step4.waiting_time > 12 THEN 'ED86d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4  step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)


left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by IC_Rec_CCG,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - CCG
 INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01aa_ad_ccg_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - STP - PREP
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01aa_ad_stp_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(stp.STP_CODE,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED86a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED86b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED86c'
             WHEN  step4.waiting_time > 12 THEN 'ED86d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4  step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)


left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by STP_CODE,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01aa-CYP_ED01ad - ED86 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01aa_ad_stp_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ae - ED86 - National
-- INSERT INTO $db_output.cyp_ed_wt_unformatted
-- select 
--        '$month_id' AS MONTH_ID
--        ,'$status' AS STATUS
--        ,'$rp_startdate_run' AS REPORTING_PERIOD_START
--        ,'$rp_enddate' AS REPORTING_PERIOD_END
--        ,'England' AS BREAKDOWN
--        ,'England' AS PRIMARY_LEVEL
--        ,'England' AS PRIMARY_LEVEL_DESCRIPTION
--        ,'NONE' AS SECONDARY_LEVEL
--        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
--        ,'ED86e' METRIC 
--        ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 1 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
--        ,SOURCE_DB
-- from $db_output.CYP_ED_WT_STEP4  step4

-- INNER JOIN $db_output.validcodes as vc
-- ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
-- and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

-- where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
-- group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ae - ED86 - Provider
-- INSERT INTO $db_output.cyp_ed_wt_unformatted
-- select 
--        '$month_id' AS MONTH_ID
--        ,'$status' AS STATUS
--        ,'$rp_startdate_run' AS REPORTING_PERIOD_START
--        ,'$rp_enddate' AS REPORTING_PERIOD_END
--        ,'Provider' AS BREAKDOWN
--        ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
--        ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
--        ,'NONE' AS SECONDARY_LEVEL
--        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
--        ,'ED86e' METRIC 
--        ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 1 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
--        ,SOURCE_DB
-- from $db_output.CYP_ED_WT_STEP4  step4

-- INNER JOIN $db_output.validcodes as vc
-- ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
-- and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

-- where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
-- group by OrgIDProv,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ae - ED86 - CCG
-- INSERT INTO $db_output.cyp_ed_wt_unformatted
-- select 
--        '$month_id' AS MONTH_ID
--        ,'$status' AS STATUS
--        ,'$rp_startdate_run' AS REPORTING_PERIOD_START
--        ,'$rp_enddate' AS REPORTING_PERIOD_END
--        ,'CCG - GP Practice or Residence' AS BREAKDOWN
--        ,COALESCE(IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
--        ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
--        ,'NONE' AS SECONDARY_LEVEL
--        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
--        ,'ED86e' METRIC 
--        ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 1 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
--        ,SOURCE_DB
-- from $db_output.CYP_ED_WT_STEP4  step4

-- INNER JOIN $db_output.validcodes as vc
-- ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
-- and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)


-- left join $db_output.MHS001_CCG_LATEST ccg
-- on step4.Person_ID = ccg.Person_ID

-- where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
-- group by IC_Rec_CCG,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ae - ED86 - STP
-- INSERT INTO $db_output.cyp_ed_wt_unformatted
-- select 
--        '$month_id' AS MONTH_ID
--        ,'$status' AS STATUS
--        ,'$rp_startdate_run' AS REPORTING_PERIOD_START
--        ,'$rp_enddate' AS REPORTING_PERIOD_END
--        ,'STP - GP Practice or Residence' AS BREAKDOWN
--        ,COALESCE(stp.STP_CODE,NULL) AS PRIMARY_LEVEL
--        ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
--        ,'NONE' AS SECONDARY_LEVEL
--        ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
--        ,'ED86e' METRIC 
--        ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 1 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
--        ,SOURCE_DB
-- from $db_output.CYP_ED_WT_STEP4  step4

-- INNER JOIN $db_output.validcodes as vc
-- ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
-- and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

-- left join $db_output.MHS001_CCG_LATEST ccg
-- on step4.Person_ID = ccg.Person_ID
-- left join $db_output.STP_Region_mapping_post_2020 stp
-- on ccg.IC_Rec_CCG = stp.CCG_CODE

-- where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
-- group by STP_CODE,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01b - ED87 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87' as METRIC  
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 as step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

where  UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source' 
group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01b - ED87 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       , Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87' as METRIC  
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
  ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue 
  and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by OrgIDProv,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01b - ED87 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87' as METRIC  
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by IC_Rec_CCG,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01b - ED87 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(STP_CODE,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87' as METRIC  
       ,count(distinct UniqServReqID) as METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by STP_CODE,metric,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - National - prep
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01ba_to_ED01bd_National_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED87a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED87b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED87c'
             WHEN  step4.waiting_time > 12 THEN 'ED87d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by UniqServReqID,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01ba_to_ED01bd_National_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - Provider- prep
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01ba_to_ED01bd_Provider_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION 
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED87a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED87b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED87c'
             WHEN  step4.waiting_time > 12 THEN 'ED87d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id'
and Status = '$status'
 and SOURCE_DB = '$db_source'
group by step4.orgidprov,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,UntitledCYP_ED01ba-CYP_ED01bd - ED87 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01ba_to_ED01bd_Provider_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - CCG - prep
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01ba_to_ED01bd_CCG_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED87a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED87b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED87c'
             WHEN  step4.waiting_time > 12 THEN 'ED87d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id'
and Status = '$status'
 and SOURCE_DB = '$db_source'
group by IC_Rec_CCG,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01ba_to_ED01bd_CCG_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - STP - prep
CREATE OR REPLACE GLOBAL TEMP VIEW agg_CYP_ED01ba_to_ED01bd_STP_prep AS
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(stp.STP_CODE,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step4.waiting_time <= 1 THEN 'ED87a' 
             WHEN  step4.waiting_time > 1 AND step4.waiting_time <= 4 THEN 'ED87b'
             WHEN  step4.waiting_time > 4 AND step4.waiting_time <= 12 THEN 'ED87c'
             WHEN  step4.waiting_time > 12 THEN 'ED87d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01ba-CYP_ED01bd - ED87 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT  MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,SUM(METRIC_VALUE) as METRIC_VALUE
       ,SOURCE_DB
from global_temp.agg_CYP_ED01ba_to_ED01bd_STP_prep
group by MONTH_ID
       ,STATUS
       ,REPORTING_PERIOD_START
       ,REPORTING_PERIOD_END
       ,BREAKDOWN
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,SECONDARY_LEVEL
       ,SECONDARY_LEVEL_DESCRIPTION
       ,METRIC
       ,METRIC_VALUE
       ,SOURCE_DB


-- COMMAND ----------

-- DBTITLE 1,CYP_ED01be - ED87 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87e' AS METRIC
       ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 4 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01be - ED87 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87e' AS METRIC
       ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 4 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by step4.orgidprov,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01be - ED87 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(ccg.IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87e' AS METRIC
       ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 4 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)

left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by ccg.IC_Rec_CCG,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED01be - ED87 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,COALESCE(stp.STP_CODE,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED87e' AS METRIC
       ,CAST(COUNT(DISTINCT CASE WHEN waiting_time <= 4 THEN UniqServReqID END) AS FLOAT) / CAST(COUNT(DISTINCT UniqServReqID) AS FLOAT)*100 AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP4 step4
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step4.ClinRespPriorityType = vc.ValidValue and step4.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step4.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step4.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02 - ED88 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED88' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02 - ED88 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED88' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by step6.orgidprov,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02 - ED88 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(ccg.IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED88' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by ccg.IC_Rec_CCG,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02 - ED88 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(stp_code,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED88' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02a - ED89 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED89' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
 
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02a - ED89 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED89' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by step6.orgidprov,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02a - ED89 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(ccg.IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED89' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)

left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by ccg.IC_Rec_CCG,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02a - ED89 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(stp_code,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED89' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE, SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02aa_to_ED02ad - ED89 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England'
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
        ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED89a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED89b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED89c'
             WHEN  step6.waiting_time > 12 THEN 'ED89d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
 
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02aa_to_ED02ad - ED89 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
        ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED89a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED89b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED89c'
             WHEN  step6.waiting_time > 12 THEN 'ED89d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by step6.orgidprov,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02aa_to_ED02ad - ED89 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(ccg.IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
        ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED89a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED89b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED89c'
             WHEN  step6.waiting_time > 12 THEN 'ED89d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)

left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by ccg.IC_Rec_CCG,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02aa_to_ED02ad - ED89 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(stp_code,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
        ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED89a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED89b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED89c'
             WHEN  step6.waiting_time > 12 THEN 'ED89d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6  step6 

INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED86_89' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue 
and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)

left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE, METRIC ,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02b - ED90 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England' AS PRIMARY_LEVEL
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED90' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02b - ED90 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED90' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)

where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
and ClinRespPriorityType = 3
group by step6.orgidprov,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02b - ED90 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(ccg.IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED90' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by ccg.IC_Rec_CCG,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02b - ED90 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(stp_code,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,'ED90' AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE, METRIC ,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02ba_to_ED02bd - ED90 - National
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'England' AS BREAKDOWN
       ,'England'
       ,'England' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED90a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED90b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED90c'
             WHEN  step6.waiting_time > 12 THEN 'ED90d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02ba_to_ED02bd - ED90 - Provider
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'Provider' AS BREAKDOWN
       ,Coalesce(OrgIDProv,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED90a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED90b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED90c'
             WHEN  step6.waiting_time > 12 THEN 'ED90d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by step6.orgidprov,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02ba_to_ED02bd - ED90 - CCG
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'CCG - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(ccg.IC_Rec_CCG,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
       ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED90a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED90b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED90c'
             WHEN  step6.waiting_time > 12 THEN 'ED90d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by ccg.IC_Rec_CCG,METRIC,SOURCE_DB

-- COMMAND ----------

-- DBTITLE 1,CYP_ED02ba_to_ED02bd - ED90 - STP
INSERT INTO $db_output.cyp_ed_wt_unformatted
select 
       '$month_id' AS MONTH_ID
       ,'$status' AS STATUS
       ,'$rp_startdate_run' AS REPORTING_PERIOD_START
       ,'$rp_enddate' AS REPORTING_PERIOD_END
       ,'STP - GP Practice or Residence' AS BREAKDOWN
       ,Coalesce(stp_code,NULL) AS PRIMARY_LEVEL
       ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
       ,'NONE' AS SECONDARY_LEVEL
       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 ,(CASE WHEN  step6.waiting_time <= 1 THEN 'ED90a' 
             WHEN  step6.waiting_time > 1 AND step6.waiting_time <= 4 THEN 'ED90b'
             WHEN  step6.waiting_time > 4 AND step6.waiting_time <= 12 THEN 'ED90c'
             WHEN  step6.waiting_time > 12 THEN 'ED90d' END) AS METRIC
       ,count(distinct UniqServReqID) AS METRIC_VALUE
       ,SOURCE_DB
from $db_output.CYP_ED_WT_STEP6 step6
INNER JOIN $db_output.validcodes as vc
ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'ED87_90' and vc.type = 'include' and step6.ClinRespPriorityType = vc.ValidValue and step6.SubmissionMonthID >= vc.FirstMonth and (vc.LastMonth is null or step6.SubmissionMonthID <= vc.LastMonth)
left join $db_output.MHS001_CCG_LATEST ccg
on step6.Person_ID = ccg.Person_ID
left join $db_output.STP_Region_mapping_post_2020 stp
on ccg.IC_Rec_CCG = stp.CCG_CODE
where UniqMonthID = '$month_id' AND Status = '$status' and SOURCE_DB = '$db_source'
group by stp.STP_CODE, METRIC ,SOURCE_DB

-- COMMAND ----------

INSERT INTO $db_output.cyp_ed_wt_unformatted
SELECT 
NUM.MONTH_ID, 
NUM.STATUS, 
NUM.REPORTING_PERIOD_START,
NUM.REPORTING_PERIOD_END,
NUM.BREAKDOWN,
NUM.PRIMARY_LEVEL,
NUM.PRIMARY_LEVEL_DESCRIPTION,
NUM.SECONDARY_LEVEL,
NUM.SECONDARY_LEVEL_DESCRIPTION,
CASE
  WHEN NUM.METRIC = 'ED86a' THEN 'ED86e'
  WHEN NUM.METRIC = 'ED86b' THEN 'ED86f'
  WHEN NUM.METRIC = 'ED86c' THEN 'ED86g'
  WHEN NUM.METRIC = 'ED86d' THEN 'ED86h'
  WHEN NUM.METRIC = 'ED87a' THEN 'ED87f' --ED87e is calculated separately as it includes those waiting less than a week and between 1 and 4 weeks
  WHEN NUM.METRIC = 'ED87b' THEN 'ED87g'
  WHEN NUM.METRIC = 'ED87c' THEN 'ED87h'
  WHEN NUM.METRIC = 'ED87d' THEN 'ED87i'
  WHEN NUM.METRIC = 'ED89a' THEN 'ED89e'
  WHEN NUM.METRIC = 'ED89b' THEN 'ED89f'
  WHEN NUM.METRIC = 'ED89c' THEN 'ED89g'
  WHEN NUM.METRIC = 'ED89d' THEN 'ED89h'
  WHEN NUM.METRIC = 'ED90a' THEN 'ED90e'
  WHEN NUM.METRIC = 'ED90b' THEN 'ED90f'
  WHEN NUM.METRIC = 'ED90c' THEN 'ED90g'
  WHEN NUM.METRIC = 'ED90d' THEN 'ED90h'
  END AS METRIC,
(CAST(NUM.METRIC_VALUE AS FLOAT) / CAST(DEN.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE,
NUM.SOURCE_DB
FROM
$db_output.cyp_ed_wt_unformatted NUM
INNER JOIN $db_output.cyp_ed_wt_unformatted DEN
  ON NUM.REPORTING_PERIOD_START = DEN.REPORTING_PERIOD_START AND NUM.REPORTING_PERIOD_END = DEN.REPORTING_PERIOD_END AND NUM.STATUS = DEN.STATUS AND NUM.BREAKDOWN = DEN.BREAKDOWN AND NUM.PRIMARY_LEVEL = DEN.PRIMARY_LEVEL AND NUM.SECONDARY_LEVEL = DEN.SECONDARY_LEVEL AND NUM.SOURCE_DB = DEN.SOURCE_DB
  AND (
        (NUM.METRIC = 'ED86a' and DEN.METRIC = 'ED86') OR
        (NUM.METRIC = 'ED86b' and DEN.METRIC = 'ED86') OR
        (NUM.METRIC = 'ED86c' and DEN.METRIC = 'ED86') OR
        (NUM.METRIC = 'ED86d' and DEN.METRIC = 'ED86') OR
        (NUM.METRIC = 'ED87a' and DEN.METRIC = 'ED87') OR
        (NUM.METRIC = 'ED87b' and DEN.METRIC = 'ED87') OR
        (NUM.METRIC = 'ED87c' and DEN.METRIC = 'ED87') OR
        (NUM.METRIC = 'ED87d' and DEN.METRIC = 'ED87') OR
        (NUM.METRIC = 'ED89a' and DEN.METRIC = 'ED89') OR
        (NUM.METRIC = 'ED89b' and DEN.METRIC = 'ED89') OR
        (NUM.METRIC = 'ED89c' and DEN.METRIC = 'ED89') OR
        (NUM.METRIC = 'ED89d' and DEN.METRIC = 'ED89') OR
        (NUM.METRIC = 'ED90a' and DEN.METRIC = 'ED90') OR
        (NUM.METRIC = 'ED90b' and DEN.METRIC = 'ED90') OR
        (NUM.METRIC = 'ED90c' and DEN.METRIC = 'ED90') OR
        (NUM.METRIC = 'ED90d' and DEN.METRIC = 'ED90') )