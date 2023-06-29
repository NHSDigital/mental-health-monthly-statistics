# Databricks notebook source
%md

# Expand output for each product to cover all possible metrics

By RIGHT JOINing the unformatted output to the list of possible metrics, and using COALESCE we can 
fill in any metrics which didn't emerge from the submitted data.

# COMMAND ----------

# DBTITLE 1,1. Expand Main monthly - this may not be doing anything any more...
%sql

CREATE OR REPLACE GLOBAL TEMP VIEW Main_monthly_expanded AS
SELECT * FROM (
SELECT  
  '$month_id' AS MONTH_ID,
  COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START,
  COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(m.STATUS, '$status') as STATUS,
  COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN,
  COALESCE(m.PRIMARY_LEVEL, p.PRIMARY_LEVEL) as PRIMARY_LEVEL,
  p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL,
  p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(m.METRIC, p.METRIC) as METRIC,
  p.METRIC_NAME AS METRIC_NAME,
  m.METRIC_VALUE AS METRIC_VALUE,
  COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB
FROM $db_output.main_monthly_unformatted_new as m
RIGHT OUTER JOIN global_temp.main_monthly_possible_metrics as p
  ON m.BREAKDOWN = p.BREAKDOWN
  AND m.PRIMARY_LEVEL = p.PRIMARY_LEVEL
  AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
  AND m.METRIC = p.METRIC
  AND m.REPORTING_PERIOD_START = '$rp_startdate'
  AND m.REPORTING_PERIOD_END = '$rp_enddate'
  AND m.STATUS = '$status'
  AND m.SOURCE_DB = '$db_source'
WHERE (
        (
          p.BREAKDOWN NOT IN ('CASSR; Provider', 'CASSR')
        )
        OR 
        (
          p.BREAKDOWN IN ('CASSR') AND p.METRIC IN ('AMH03','AMH14','AMH15','AMH17','AMH18')
        )
      )
AND p.PRIMARY_LEVEL != 'UNKNOWN'
union all
select
  '$month_id' AS MONTH_ID,
  COALESCE(a.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
  COALESCE(a.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END, 
  a.STATUS as STATUS,
  a.BREAKDOWN as BREAKDOWN,
  a.PRIMARY_LEVEL as PRIMARY_LEVEL,
  a.PRIMARY_LEVEL_DESCRIPTION as PRIMARY_LEVEL_DESCRIPTION,
  a.SECONDARY_LEVEL as SECONDARY_LEVEL,
  b.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  a.METRIC as METRIC,
  b.METRIC_NAME AS METRIC_NAME,
  a.METRIC_VALUE AS METRIC_VALUE,
  COALESCE(a.SOURCE_DB, '$db_source') AS SOURCE_DBB
from $db_output.main_monthly_unformatted_new a
left join global_temp.main_monthly_possible_metrics b 
          ON a.BREAKDOWN = b.BREAKDOWN
          AND a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
          AND a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
          AND a.METRIC = b.METRIC
          AND a.STATUS = '$status'
          AND a.SOURCE_DB = '$db_source'
where a.BREAKDOWN IN ('CASSR; Provider')
          AND a.REPORTING_PERIOD_START = '$rp_startdate_quarterly'
          AND a.REPORTING_PERIOD_END = '$rp_enddate'
          AND a.STATUS = '$status'
          AND a.SOURCE_DB = '$db_source'
) 

# COMMAND ----------

# DBTITLE 1,72HOURS -CCG & PROVIDER
%sql
CREATE OR REPLACE GLOBAL TEMP VIEW Main_monthly_expanded_72hrs AS
SELECT
'$month_id' AS MONTH_ID,
COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START,
COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
m.STATUS as STATUS,
m.BREAKDOWN as BREAKDOWN,
COALESCE(m.PRIMARY_LEVEL, 'UNKNOWN') as PRIMARY_LEVEL,
COALESCE(od1.NAME, 'UNKNOWN') as PRIMARY_LEVEL_DESCRIPTION,
m.SECONDARY_LEVEL as SECONDARY_LEVEL,
od2.NAME as SECONDARY_LEVEL_DESCRIPTION,
m.METRIC as METRIC,
mv.METRIC_NAME AS METRIC_NAME,
m.METRIC_VALUE AS METRIC_VALUE,
m.SOURCE_DB AS SOURCE_DB
FROM $db_output.main_monthly_unformatted_new as m
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od1 ON m.PRIMARY_LEVEL = od1.ORG_CODE
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od2 ON m.SECONDARY_LEVEL = od2.ORG_CODE
JOIN $db_output.Main_monthly_metric_values mv ON mv.metric = m.metric
WHERE m.BREAKDOWN = 'CCG - GP Practice or Residence; Provider of Responsibility'
AND m.MONTH_ID = '$month_id'
AND m.STATUS = '$status'
AND m.SOURCE_DB = '$db_source'

UNION ALL
SELECT  
'$month_id' AS MONTH_ID,
COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START,
COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
m.STATUS as STATUS,
m.BREAKDOWN as BREAKDOWN,
COALESCE(m.PRIMARY_LEVEL, 'UNKNOWN') as PRIMARY_LEVEL,
COALESCE(od1.NAME, 'UNKNOWN') as PRIMARY_LEVEL_DESCRIPTION,
m.SECONDARY_LEVEL as SECONDARY_LEVEL,
od2.NAME as SECONDARY_LEVEL_DESCRIPTION,
m.METRIC as METRIC,
mv.METRIC_NAME AS METRIC_NAME,
m.METRIC_VALUE AS METRIC_VALUE,
m.SOURCE_DB
FROM $db_output.main_monthly_unformatted_new as m
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od1 ON m.PRIMARY_LEVEL = od1.ORG_CODE
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od2 ON m.SECONDARY_LEVEL = od2.ORG_CODE
JOIN $db_output.Main_monthly_metric_values mv ON mv.metric = m.metric
WHERE m.BREAKDOWN = 'CCG - GP Practice or Residence'
AND m.MONTH_ID = '$month_id'
AND m.STATUS = '$status'
AND m.SOURCE_DB = '$db_source'
AND PRIMARY_LEVEL IS NULL

# COMMAND ----------

# DBTITLE 1,CYP_ED_WaitingTimes
%sql
CREATE OR REPLACE GLOBAL TEMP VIEW cyp_ed_wt_expanded AS
select 
  COALESCE(c.REPORTING_PERIOD_START, '$rp_startdate_quarterly') as REPORTING_PERIOD_START, 
  COALESCE(c.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(c.STATUS,'$status') as STATUS,
  COALESCE(c.BREAKDOWN,d.BREAKDOWN,'UNKNOWN') as BREAKDOWN,
  COALESCE(c.PRIMARY_LEVEL,d.PRIMARY_LEVEL,'UNKNOWN') as PRIMARY_LEVEL,
  d.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(c.SECONDARY_LEVEL,d.SECONDARY_LEVEL,'UNKNOWN') as SECONDARY_LEVEL,
  d.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(c.METRIC,d.METRIC) as METRIC,
  d.METRIC_NAME AS METRIC_NAME,
  c.METRIC_VALUE AS METRIC_VALUE,
  COALESCE(c.SOURCE_DB, '$db_source') AS SOURCE_DB
from $db_output.cyp_ed_wt_unformatted c
right outer join global_temp.cyp_ed_wt_possible_metrics d 
          ON c.BREAKDOWN = d.BREAKDOWN
          AND  COALESCE(c.PRIMARY_LEVEL,'UNKNOWN') = d.primary_level
          AND c.SECONDARY_LEVEL = d.secondary_level
          AND c.METRIC = d.metric
          AND c.REPORTING_PERIOD_START = '$rp_startdate_quarterly'
          AND c.REPORTING_PERIOD_END = '$rp_enddate'
          AND c.STATUS = '$status' 
          AND c.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,MHA_Monthly
%sql
CREATE OR REPLACE GLOBAL TEMP VIEW mha_monthly_expanded AS
select 
  COALESCE(c.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
  COALESCE(c.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(c.STATUS,'$status') as STATUS,
  COALESCE(c.BREAKDOWN,d.BREAKDOWN,'UNKNOWN') as BREAKDOWN,
  COALESCE(c.PRIMARY_LEVEL,d.PRIMARY_LEVEL,'UNKNOWN') as PRIMARY_LEVEL,
  d.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(c.SECONDARY_LEVEL,d.SECONDARY_LEVEL,'UNKNOWN') as SECONDARY_LEVEL,
  d.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(c.METRIC,d.METRIC) as METRIC,
  d.METRIC_NAME AS METRIC_NAME,
  c.METRIC_VALUE AS METRIC_VALUE,
  COALESCE(c.SOURCE_DB, '$db_source') AS SOURCE_DB
from $db_output.mha_monthly_unformatted c
right outer join global_temp.mha_possible_metrics d 
          ON c.BREAKDOWN = d.BREAKDOWN
          AND  COALESCE(c.PRIMARY_LEVEL,'UNKNOWN') = d.primary_level
          AND c.SECONDARY_LEVEL = d.secondary_level
          AND c.METRIC = d.metric
          AND c.REPORTING_PERIOD_START = '$rp_startdate'
          AND c.REPORTING_PERIOD_END = '$rp_enddate'
          AND c.STATUS = '$status' 
          AND c.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,Perinatal - CYP Outcome Measures - monthly window
%sql
CREATE OR REPLACE GLOBAL TEMP VIEW peri_monthly_expanded AS
select 
  COALESCE(c.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
  COALESCE(c.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(c.STATUS,'$status') as STATUS,
  COALESCE(c.BREAKDOWN,d.breakdown,'UNKNOWN') as BREAKDOWN,
  COALESCE(c.PRIMARY_LEVEL,d.primary_level,'UNKNOWN') as PRIMARY_LEVEL,
  d.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(c.SECONDARY_LEVEL,d.secondary_level,'UNKNOWN') as SECONDARY_LEVEL,
  d.secondary_level_desc as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(c.METRIC,d.metric) as METRIC,
  d.metric_name AS METRIC_NAME,
  c.METRIC_VALUE AS METRIC_VALUE,
  COALESCE(c.SOURCE_DB, '$db_source') AS SOURCE_DB
from $db_output.cyp_peri_monthly_unformatted c
right outer join global_temp.peri_possible_metrics d 
          ON c.BREAKDOWN = d.breakdown
          AND  COALESCE(c.PRIMARY_LEVEL,'UNKNOWN') = d.primary_level
          AND c.METRIC = d.metric
          AND c.REPORTING_PERIOD_START = '$rp_startdate'
          AND c.REPORTING_PERIOD_END = '$rp_enddate'
          AND c.STATUS = '$status' 
          AND c.SOURCE_DB = '$db_source'
          
where c.METRIC IN ('MHS92', 'MHS93', 'MHS94')

# COMMAND ----------

# DBTITLE 1,Perinatal - CYP Outcome Measures - rolling 12 month window
%sql
CREATE OR REPLACE GLOBAL TEMP VIEW peri_rolling_expanded AS
select 
  COALESCE(c.REPORTING_PERIOD_START, add_months('$rp_startdate', -11)) as REPORTING_PERIOD_START, 
  COALESCE(c.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(c.STATUS,'$status') as STATUS,
  COALESCE(c.BREAKDOWN,d.breakdown,'UNKNOWN') as BREAKDOWN,
  COALESCE(c.PRIMARY_LEVEL,d.PRIMARY_LEVEL,'UNKNOWN') as PRIMARY_LEVEL,
  d.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(c.SECONDARY_LEVEL,d.SECONDARY_LEVEL,'UNKNOWN') as SECONDARY_LEVEL,
  d.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(c.METRIC,d.metric) as METRIC,
  d.METRIC_NAME AS METRIC_NAME,
  c.METRIC_VALUE AS METRIC_VALUE,
  COALESCE(c.SOURCE_DB, '$db_source') AS SOURCE_DB
from $db_output.cyp_peri_monthly_unformatted c
right outer join global_temp.peri_possible_metrics d 
          ON  c.BREAKDOWN = d.breakdown
          AND  COALESCE(c.PRIMARY_LEVEL,'UNKNOWN') = d.primary_level
          AND c.METRIC = d.metric
          AND c.REPORTING_PERIOD_END = '$rp_enddate'
          AND c.STATUS = '$status' 
          AND c.SOURCE_DB = '$db_source'
          
where c.METRIC in ('MHS91', 'MHS95')
and d.metric in ('MHS91', 'MHS95')

# COMMAND ----------

# DBTITLE 1,Perinatal - CYP Outcome Measures - CCG & CCG; PROVIDER
%sql
CREATE OR REPLACE GLOBAL TEMP VIEW peri_monthly_expanded_ccg AS
SELECT
COALESCE(m.REPORTING_PERIOD_START, add_months('$rp_startdate', -11)) as REPORTING_PERIOD_START,
COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
m.STATUS as STATUS,
m.BREAKDOWN as BREAKDOWN,
COALESCE(m.PRIMARY_LEVEL, 'UNKNOWN') as PRIMARY_LEVEL,
COALESCE(od1.NAME, 'UNKNOWN') as PRIMARY_LEVEL_DESCRIPTION,
m.SECONDARY_LEVEL as SECONDARY_LEVEL,
od2.NAME as SECONDARY_LEVEL_DESCRIPTION,
m.METRIC as METRIC,
mv.METRIC_NAME AS METRIC_NAME,
m.METRIC_VALUE AS METRIC_VALUE,
m.SOURCE_DB AS SOURCE_DB
FROM $db_output.cyp_peri_monthly_unformatted as m
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od1 ON m.PRIMARY_LEVEL = od1.ORG_CODE
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od2 ON m.SECONDARY_LEVEL = od2.ORG_CODE
JOIN $db_output.peri_metric_values mv ON mv.metric = m.metric
WHERE m.BREAKDOWN = 'CCG of Residence; Provider'
          AND m.REPORTING_PERIOD_END = '$rp_enddate'
AND m.STATUS = '$status'
AND m.SOURCE_DB = '$db_source'

UNION ALL
SELECT  
COALESCE(m.REPORTING_PERIOD_START, add_months('$rp_startdate', -11)) as REPORTING_PERIOD_START,
COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
m.STATUS as STATUS,
m.BREAKDOWN as BREAKDOWN,
COALESCE(m.PRIMARY_LEVEL, 'UNKNOWN') as PRIMARY_LEVEL,
COALESCE(od1.NAME, 'UNKNOWN') as PRIMARY_LEVEL_DESCRIPTION,
COALESCE(m.SECONDARY_LEVEL,'NONE') as SECONDARY_LEVEL,
COALESCE(m.SECONDARY_LEVEL_DESCRIPTION,'NONE') as SECONDARY_LEVEL_DESCRIPTION,
m.METRIC as METRIC,
mv.METRIC_NAME AS METRIC_NAME,
m.METRIC_VALUE AS METRIC_VALUE,
m.SOURCE_DB
FROM $db_output.cyp_peri_monthly_unformatted as m
LEFT JOIN $db_output.RD_ORG_DAILY_LATEST od1 ON m.PRIMARY_LEVEL = od1.ORG_CODE

JOIN $db_output.peri_metric_values mv ON mv.metric = m.metric
WHERE m.BREAKDOWN = 'CCG of Residence'
--           AND m.REPORTING_PERIOD_START = '$rp_startdate'
          AND m.REPORTING_PERIOD_END = '$rp_enddate'
AND m.STATUS = '$status'
AND m.SOURCE_DB = '$db_source'
-- AND PRIMARY_LEVEL IS NULL

# COMMAND ----------

# DBTITLE 1,Expand AWT (Access and Waiting Times) EIP (Early Intervention in Psychosis)
%sql

CREATE OR REPLACE GLOBAL TEMP VIEW EIP_expanded AS
SELECT DISTINCT 
  COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate_quarterly') as REPORTING_PERIOD_START,
  COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(m.STATUS, '$status') as STATUS,
  COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN,
  COALESCE(m.LEVEL, p.LEVEL) as PRIMARY_LEVEL,
  p.LEVEL_DESC AS PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL,
  p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(m.METRIC, p.METRIC) as METRIC,
  p.METRIC_NAME AS METRIC_NAME,
  cast(coalesce(m.METRIC_VALUE,0) as string) AS METRIC_VALUE,
  COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB  
  
FROM $db_output.AWT_unformatted as m
RIGHT OUTER JOIN global_temp.EIP_possible_metrics as p
  ON m.BREAKDOWN = p.BREAKDOWN
  AND m.LEVEL = p.LEVEL
  AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
  AND m.METRIC = p.METRIC
  AND m.REPORTING_PERIOD_START = '$rp_startdate_quarterly'
  AND m.REPORTING_PERIOD_END = '$rp_enddate'
  AND m.STATUS = '$status'
  AND m.SOURCE_DB = '$db_source'
  
WHERE p.METRIC NOT IN ('EIP68', 'EIP69a', 'EIP69b')

# COMMAND ----------

# DBTITLE 1,EIP - Monthly measure (EIP68,69a,69b)
%sql

CREATE OR REPLACE GLOBAL TEMP VIEW EIP_expanded_Monthly AS
SELECT DISTINCT 
  COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START,
  COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
  COALESCE(m.STATUS, '$status') as STATUS,
  COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN,
  COALESCE(m.LEVEL, p.LEVEL) as PRIMARY_LEVEL,
  p.LEVEL_DESC AS PRIMARY_LEVEL_DESCRIPTION,
  COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL,
  p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
  COALESCE(m.METRIC, p.METRIC) as METRIC,
  p.METRIC_NAME AS METRIC_NAME,
  cast(coalesce(m.METRIC_VALUE,0) as string) AS METRIC_VALUE,
  COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB
  
FROM $db_output.AWT_unformatted as m
RIGHT OUTER JOIN global_temp.EIP_possible_metrics as p
  ON m.BREAKDOWN = p.BREAKDOWN
  AND m.LEVEL = p.LEVEL
  AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
  AND m.METRIC = p.METRIC
  AND m.REPORTING_PERIOD_START = '$rp_startdate'
  AND m.REPORTING_PERIOD_END = '$rp_enddate'
  AND m.STATUS = '$status'
  AND m.SOURCE_DB = '$db_source'
  
WHERE p.METRIC IN ('EIP68', 'EIP69a', 'EIP69b')
  

# COMMAND ----------

# DBTITLE 1,Expand ASCOF
%sql

CREATE OR REPLACE GLOBAL TEMP VIEW ascof_expanded AS 

SELECT 
  COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
  COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END, 
  COALESCE(m.STATUS, '$status') as STATUS, 
  COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, 
  COALESCE(m.PRIMARY_LEVEL, p.primary_level) as PRIMARY_LEVEL, 
  p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION, 
  COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL, 
  p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, 
  COALESCE(m.THIRD_LEVEL, p.THIRD_LEVEL) as THIRD_LEVEL, 
  COALESCE(m.METRIC, p.METRIC) as METRIC, 
  p.METRIC_NAME AS METRIC_NAME, 
  m.METRIC_VALUE AS METRIC_VALUE, 
  COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB 

FROM $db_output.ascof_unformatted as m 
RIGHT OUTER JOIN global_temp.ascof_possible_metrics as p 
  ON m.BREAKDOWN = p.BREAKDOWN 
  AND m.PRIMARY_LEVEL = p.primary_level 
  AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL 
  AND m.THIRD_LEVEL = p.third_level 
  AND m.METRIC = p.METRIC 
  AND m.REPORTING_PERIOD_START = '$rp_startdate' 
  AND m.REPORTING_PERIOD_END = '$rp_enddate' 
  AND m.SOURCE_DB = '$db_source'

WHERE 
-- (
      p.BREAKDOWN NOT IN ('CASSR;Provider', 'CASSR;Provider;Gender', 'Provider;Gender')
AND p.PRIMARY_LEVEL != 'UNKNOWN'



UNION ALL
-- these other measures only include rows where data has been submitted

SELECT 
  COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
  COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END, 
  COALESCE(m.STATUS, '$status') as STATUS, 
  COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, 
  COALESCE(m.PRIMARY_LEVEL, p.primary_level) as PRIMARY_LEVEL, 
  p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION, 
  COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL, 
  p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, 
  COALESCE(m.THIRD_LEVEL, p.THIRD_LEVEL) as THIRD_LEVEL, 
  COALESCE(m.METRIC, p.METRIC) as METRIC, 
  p.METRIC_NAME AS METRIC_NAME, 
  m.METRIC_VALUE AS METRIC_VALUE, 
  COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB 

FROM $db_output.ascof_unformatted as m 
INNER JOIN global_temp.ascof_possible_metrics as p 
  ON m.BREAKDOWN = p.BREAKDOWN 
  AND m.PRIMARY_LEVEL = p.primary_level 
  AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL 
  AND m.THIRD_LEVEL = p.third_level 
  AND m.METRIC = p.METRIC 

WHERE p.BREAKDOWN IN ('CASSR;Provider', 'CASSR;Provider;Gender', 'Provider;Gender')
AND p.PRIMARY_LEVEL != 'UNKNOWN'

AND m.REPORTING_PERIOD_START = '$rp_startdate' 
AND m.REPORTING_PERIOD_END = '$rp_enddate' 
AND m.SOURCE_DB = '$db_source'