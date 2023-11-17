# Databricks notebook source
 %md
 
 # Expand output for each product to cover all possible metrics
 
 By RIGHT JOINing the unformatted output to the list of possible metrics, and using COALESCE we can 
 fill in any metrics which didn't emerge from the submitted data.

# COMMAND ----------

# DBTITLE 1,1. Expand Main monthly - only run when $status = 'Final' - commented out
# only needs to run if data are final

# status = dbutils.widgets.get("status")
# db_output = dbutils.widgets.get("db_output")
# rp_startdate = dbutils.widgets.get("rp_startdate")
# rp_enddate = dbutils.widgets.get("rp_enddate")
# month_id = dbutils.widgets.get("month_id")
# rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")

# sql = "CREATE OR REPLACE GLOBAL TEMP VIEW Main_monthly_expanded AS SELECT COALESCE(m.REPORTING_PERIOD_START, '{rp_startdate}') as REPORTING_PERIOD_START, COALESCE(m.REPORTING_PERIOD_END, '{rp_enddate}') as REPORTING_PERIOD_END,  COALESCE(m.STATUS, '{status}') as STATUS, COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, COALESCE(m.PRIMARY_LEVEL, p.PRIMARY_LEVEL) as PRIMARY_LEVEL, p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,  COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL, p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, COALESCE(m.METRIC, p.METRIC) as METRIC, p.METRIC_NAME AS METRIC_NAME, m.METRIC_VALUE AS METRIC_VALUE FROM {db_output}.Main_monthly_unformatted as m RIGHT OUTER JOIN global_temp.main_monthly_possible_metrics as p ON m.BREAKDOWN = p.BREAKDOWN AND m.PRIMARY_LEVEL = p.PRIMARY_LEVEL AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL AND m.METRIC = p.METRIC AND m.REPORTING_PERIOD_START = '{rp_startdate}' AND m.REPORTING_PERIOD_END = '{rp_enddate}' WHERE ((p.BREAKDOWN NOT IN ('CASSR; Provider', 'CASSR')) OR (p.BREAKDOWN IN ('CASSR') AND p.METRIC IN ('AMH03','AMH14','AMH15','AMH17','AMH18'))) union all select COALESCE(a.REPORTING_PERIOD_START, '{rp_startdate}') as REPORTING_PERIOD_START, COALESCE(a.REPORTING_PERIOD_END, '{rp_enddate}') as REPORTING_PERIOD_END, a.STATUS as STATUS, a.BREAKDOWN as BREAKDOWN, a.PRIMARY_LEVEL as PRIMARY_LEVEL, a.PRIMARY_LEVEL_DESCRIPTION as PRIMARY_LEVEL_DESCRIPTION, a.SECONDARY_LEVEL as SECONDARY_LEVEL, b.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, a.METRIC as METRIC, b.METRIC_NAME AS METRIC_NAME, a.METRIC_VALUE AS METRIC_VALUE from {db_output}.Main_monthly_unformatted a left join global_temp.main_monthly_possible_metrics b ON a.BREAKDOWN = b.BREAKDOWN AND a.PRIMARY_LEVEL = b.PRIMARY_LEVEL AND a.SECONDARY_LEVEL = b.SECONDARY_LEVEL AND a.METRIC = b.METRIC where a.BREAKDOWN IN ('CASSR; Provider') AND a.REPORTING_PERIOD_START = '{rp_startdate}' AND a.REPORTING_PERIOD_END = '{rp_enddate}' AND a.STATUS = '{status}'".format(status=status,db_output=db_output,rp_startdate=rp_startdate,rp_enddate=rp_enddate)

# if status == 'Final':
#   print(status)
#   print(sql)
#   spark.sql(sql)
# else:
#   print(status)


# COMMAND ----------

# DBTITLE 1,1. Expand Main monthly
 %sql
 --reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 CREATE OR REPLACE GLOBAL TEMP VIEW Main_monthly_expanded AS
 -- these top measures include all possible metrics with NULL values returned when no data submitted
 SELECT  
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
 
 FROM $db_output.Main_monthly_unformatted as m
 RIGHT OUTER JOIN global_temp.main_monthly_possible_metrics as p
   ON m.BREAKDOWN = p.BREAKDOWN
   AND m.PRIMARY_LEVEL = p.PRIMARY_LEVEL
   AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
   AND m.METRIC = p.METRIC
   AND m.REPORTING_PERIOD_START = '$rp_startdate'
   AND m.REPORTING_PERIOD_END = '$rp_enddate'
   AND m.STATUS = '$status'
 AND m.SOURCE_DB = '$db_source'
 WHERE
   (p.BREAKDOWN NOT IN 
           ('CASSR; Provider', 
           'CASSR',
           'England; Accommodation Type',
           'England; Age',
           'England; Age Group',
           'England; Attendance',
           'England; Bed Type',
           'England; ConsMechanismMH',
           'England; Disability',
           'England; Employment Status',
           'England; Ethnicity',
           'England; Gender',
           'England; IMD Decile',
           'England; Sexual Orientation',
           'Provider; Age Group',
           'Provider; Attendance',
           'Provider; Bed Type',
           'Provider; ConsMechanismMH',
           'CCG - GP Practice or Residence; Age Group',
           'CCG - GP Practice or Residence; Attendance',
           'CCG - GP Practice or Residence; Bed Type',
           'CCG - GP Practice or Residence; ConsMechanismMH') 
           OR 
           (p.BREAKDOWN IN ('CASSR') AND p.METRIC IN ('AMH03','AMH14','AMH15','AMH17','AMH18')))
           --METRICS REMOVED AS THEY ONLY HAVE GRANULAR BREAKDOWNS (i.e. DO NOT EXIST WITH England, Provider, CCG BREAKDOWNS ONLY)
           AND 
           (p.METRIC NOT IN ('MHS27a','MHS29f','MHS30h'))
 
 union all
 
 SELECT  
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
 
 FROM $db_output.Main_monthly_unformatted as m
 RIGHT OUTER JOIN global_temp.main_monthly_possible_metrics as p
   ON m.BREAKDOWN = p.BREAKDOWN
   AND m.PRIMARY_LEVEL = p.PRIMARY_LEVEL
   AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
   AND m.METRIC = p.METRIC
   AND m.REPORTING_PERIOD_START = '$rp_startdate'
   AND m.REPORTING_PERIOD_END = '$rp_enddate'
   AND m.STATUS = '$status'
 AND m.SOURCE_DB = '$db_source'
 WHERE 
   (p.BREAKDOWN IN ('England; Accommodation Type') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; Age') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; Age Group') AND p.METRIC IN ('MHS23d', 'MHS29d', 'MHS30f', 'MHS32c', 'MHS57b')) OR
   (p.BREAKDOWN IN ('England; Attendance') AND p.METRIC IN ('MHS29a', 'MHS29d', 'MHS29f')) OR
   (p.BREAKDOWN IN ('England; Bed Type')	AND p.METRIC IN ('MHS27a')) OR
   (p.BREAKDOWN IN ('England; ConsMechanismMH') AND p.METRIC IN ('MHS30e','MHS61b','MHS30a','MHS30f','MHS30h')) OR
   (p.BREAKDOWN IN ('England; Disability') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; Employment Status') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; Ethnicity') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; Gender') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; IMD Decile') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   (p.BREAKDOWN IN ('England; Sexual Orientation') AND p.METRIC IN ('MHS01', 'MHS07', 'MHS29', 'MHS32')) OR
   
   (p.BREAKDOWN IN ('Provider; Age Group') AND p.METRIC IN ('MHS23d', 'MHS29d', 'MHS30f', 'MHS32c', 'MHS57b')) OR
   (p.BREAKDOWN IN ('Provider; Attendance') AND p.METRIC IN ('MHS29a', 'MHS29d', 'MHS29f')) OR
   (p.BREAKDOWN IN ('Provider; Bed Type') AND p.METRIC IN ('MHS27a')) OR
   (p.BREAKDOWN IN ('Provider; ConsMechanismMH') AND p.METRIC IN ('MHS30e','MHS61b','MHS30a','MHS30f','MHS30h')) OR
   
   (p.BREAKDOWN IN ('CCG - GP Practice or Residence; Age Group')	AND p.METRIC IN ('MHS23d', 'MHS29d', 'MHS30f', 'MHS32c', 'MHS57b')) OR
   (p.BREAKDOWN IN ('CCG - GP Practice or Residence; Attendance') AND p.METRIC IN ('MHS29a', 'MHS29d', 'MHS29f')) OR
   (p.BREAKDOWN IN ('CCG - GP Practice or Residence; Bed Type')	AND p.METRIC IN ('MHS27a')) OR
   (p.BREAKDOWN IN ('CCG - GP Practice or Residence; ConsMechanismMH') AND p.METRIC IN ('MHS30e','MHS61b','MHS30a','MHS30f','MHS30h')) 
   
   
 union all
 
 -- these other measures only include rows where data has been submitted
 select 
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
   COALESCE(a.SOURCE_DB, '$db_source') AS SOURCE_DB
   
 from $db_output.Main_monthly_unformatted a
 left join global_temp.main_monthly_possible_metrics b 
           ON a.BREAKDOWN = b.BREAKDOWN
           AND a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
           AND a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
           AND a.METRIC = b.METRIC
           AND a.STATUS = '$status'
 where a.BREAKDOWN IN ('CASSR; Provider')
           AND a.REPORTING_PERIOD_START = '$rp_startdate'
           AND a.REPORTING_PERIOD_END = '$rp_enddate'
           AND a.STATUS = '$status'
 AND a.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,1.1 Expand Main monthly - MHA26
 %sql
 --added as a separate element for MHS26
 
 CREATE OR REPLACE GLOBAL TEMP VIEW DD_expanded AS
 
 select 
   COALESCE(c.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
   COALESCE(c.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
   COALESCE(c.STATUS,'$status') as STATUS,
   COALESCE(c.BREAKDOWN,d.BREAKDOWN) as BREAKDOWN,
   COALESCE(c.PRIMARY_LEVEL,d.PRIMARY_LEVEL) as PRIMARY_LEVEL,
   d.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
   COALESCE(c.SECONDARY_LEVEL,d.SECONDARY_LEVEL) as SECONDARY_LEVEL,
   d.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
   COALESCE(c.METRIC,d.METRIC) as METRIC,
   d.METRIC_NAME AS METRIC_NAME,
   c.METRIC_VALUE AS METRIC_VALUE, 
   COALESCE(c.SOURCE_DB, '$db_source') AS SOURCE_DB
   
 from $db_output.Main_monthly_unformatted c
 right outer join global_temp.DD_possible_metrics d 
           ON c.BREAKDOWN = d.BREAKDOWN
           AND c.PRIMARY_LEVEL = d.primary_level
           AND c.SECONDARY_LEVEL = d.secondary_level
           AND c.METRIC = d.metric
           AND c.REPORTING_PERIOD_START = '$rp_startdate'
           AND c.REPORTING_PERIOD_END = '$rp_enddate'
           AND c.STATUS = '$status'
           AND c.SOURCE_DB = '$db_source'
      where d.METRIC = 'MHS26'

# COMMAND ----------

# DBTITLE 1,2. Expand AWT (Access and Waiting Times) - code excluded to prevent 0/* outputs in Rounded outputs
 %sql
 
 -- CREATE OR REPLACE GLOBAL TEMP VIEW AWT_expanded AS
 -- SELECT DISTINCT 
 --   COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate_quarterly') as REPORTING_PERIOD_START,
 --   COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
 --   COALESCE(m.STATUS, '$status') as STATUS,
 --   COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN,
 --   COALESCE(m.LEVEL, p.LEVEL) as PRIMARY_LEVEL,
 --   p.LEVEL_DESC AS PRIMARY_LEVEL_DESCRIPTION,
 --   COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL,
 --   p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
 --   COALESCE(m.METRIC, p.METRIC) as METRIC,
 --   p.METRIC_NAME AS METRIC_NAME,
 --   m.METRIC_VALUE AS METRIC_VALUE,
 --   COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB
   
 -- FROM $db_output.AWT_unformatted as m
 -- RIGHT OUTER JOIN global_temp.AWT_possible_metrics as p
 --   ON m.BREAKDOWN = p.BREAKDOWN
 --   AND m.LEVEL = p.LEVEL
 --   AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
 --   AND m.METRIC = p.METRIC
 --   AND m.REPORTING_PERIOD_START = '$rp_startdate_quarterly'
 --   AND m.REPORTING_PERIOD_END = '$rp_enddate'
 --   AND m.STATUS = '$status'
 --   AND m.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,3. Expand CYP 2nd contact
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CYP_2nd_contact_expanded AS
 SELECT  
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
   
 FROM $db_output.CYP_2nd_contact_unformatted as m
 RIGHT JOIN global_temp.CYP_2nd_contact_possible_metrics as p  
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
           p.BREAKDOWN <> 'CCG - GP Practice or Residence; Provider'
         ) OR (
           p.BREAKDOWN = 'CCG - GP Practice or Residence; Provider'
           AND m.METRIC_VALUE is not null
         )
     )

# COMMAND ----------

# DBTITLE 1,4. Expand CAP  - only run when $status = 'Final' - commented out
# only needs to run if data are final

# sql = "CREATE OR REPLACE GLOBAL TEMP VIEW CAP_expanded AS SELECT COALESCE(m.REPORTING_PERIOD_START, '{rp_startdate}') as REPORTING_PERIOD_START, COALESCE(m.REPORTING_PERIOD_END, '{rp_enddate}') as REPORTING_PERIOD_END,  COALESCE(m.STATUS, '{status}') as STATUS, COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, COALESCE(m.LEVEL, p.LEVEL) as LEVEL, COALESCE(p.LEVEL_DESC) as LEVEL_DESCRIPTION, COALESCE(m.CLUSTER, p.CLUSTER) as SECONDARY_LEVEL,  'NONE' as SECONDARY_LEVEL_DESCRIPTION, COALESCE(m.METRIC,p.METRIC) as METRIC, p.METRIC_NAME AS METRIC_NAME, m.METRIC_VALUE AS METRIC_VALUE FROM {db_output}.CAP_Unformatted as m RIGHT JOIN global_temp.CaP_possible_metrics as p ON m.METRIC = p.METRIC AND m.LEVEL = p.LEVEL AND m.CLUSTER = p.CLUSTER AND m.BREAKDOWN = p.BREAKDOWN AND m.REPORTING_PERIOD_START = '{rp_startdate}' AND m.REPORTING_PERIOD_END = '{rp_enddate}'".format(status=status,db_output=db_output,rp_startdate=rp_startdate,rp_enddate=rp_enddate)

# if status == 'Final':
#   print(status)
#   print(sql)
#   spark.sql(sql).collect()
# else:
#   print(status)

# COMMAND ----------

# DBTITLE 1,4. Expand CAP
 %sql
 
 --reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CAP_expanded AS 
 SELECT COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
 COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
 COALESCE(m.STATUS, '$status') as STATUS, COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, 
 COALESCE(m.LEVEL, p.LEVEL) as LEVEL, COALESCE(p.LEVEL_DESC) as LEVEL_DESCRIPTION, 
 COALESCE(m.CLUSTER, p.CLUSTER) as SECONDARY_LEVEL,  
 'NONE' as SECONDARY_LEVEL_DESCRIPTION, 
 COALESCE(m.METRIC,p.METRIC) as METRIC, 
 p.METRIC_NAME AS METRIC_NAME, 
 m.METRIC_VALUE AS METRIC_VALUE,  
 COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB
 
 FROM $db_output.CAP_Unformatted as m 
 RIGHT JOIN global_temp.CaP_possible_metrics as p 
 ON m.METRIC = p.METRIC 
 AND m.LEVEL = p.LEVEL 
 AND m.CLUSTER = p.CLUSTER 
 AND m.BREAKDOWN = p.BREAKDOWN 
 AND m.REPORTING_PERIOD_START = '$rp_startdate' 
 AND m.REPORTING_PERIOD_END = '$rp_enddate'
 AND m.STATUS = '$status'
 AND m.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,5. CYP Monthly expanded  - only run when $status = 'Final' - commented out
# only needs to run if data are final

# sql = "CREATE OR REPLACE GLOBAL TEMP VIEW CYP_monthly_expanded AS SELECT COALESCE(m.REPORTING_PERIOD_START, '{rp_startdate}') as REPORTING_PERIOD_START, COALESCE(m.REPORTING_PERIOD_END, '{rp_enddate}') as REPORTING_PERIOD_END,  COALESCE(m.STATUS, '{status}') as STATUS, COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, COALESCE(m.PRIMARY_LEVEL, p.PRIMARY_LEVEL) as PRIMARY_LEVEL, p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION, COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL, p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, COALESCE(m.METRIC, p.METRIC) as METRIC, p.METRIC_NAME AS METRIC_NAME, m.METRIC_VALUE AS METRIC_VALUE FROM {db_output}.CYP_monthly_unformatted as m RIGHT OUTER JOIN global_temp.CYP_monthly_possible_metrics as p ON m.BREAKDOWN = p.BREAKDOWN AND m.PRIMARY_LEVEL = p.PRIMARY_LEVEL AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL AND m.METRIC = p.METRIC AND m.REPORTING_PERIOD_START ='{rp_startdate}' AND m.REPORTING_PERIOD_END = '{rp_enddate}' WHERE ((p.METRIC NOT IN ('MHS30e', 'MHS58a', 'MHS61b', 'MHS32a') AND p.BREAKDOWN IN ('England','CCG - GP Practice or Residence','Provider')) OR (p.METRIC IN ('MHS30e', 'MHS61b') AND p.BREAKDOWN IN ('England; ConsMediumUsed', 'CCG - GP Practice or Residence; ConsMediumUsed', 'Provider; ConsMediumUsed')) OR (p.METRIC = 'MHS58a' AND p.BREAKDOWN IN ('England; DNA Reason', 'CCG - GP Practice or Residence; DNA Reason', 'Provider; DNA Reason')) OR (p.METRIC = 'MHS32a' AND p.BREAKDOWN IN ('England', 'CCG - GP Practice or Residence', 'Provider', 'England; Referral Source', 'CCG - GP Practice or Residence; Referral Source', 'Provider; Referral Source')))".format(status=status,db_output=db_output,rp_startdate=rp_startdate,rp_enddate=rp_enddate)

# if status == 'Final':
#   print(status)
#   print(sql)
#   spark.sql(sql).collect()
# else:
#   print(status)

# COMMAND ----------

# DBTITLE 1,5. CYP Monthly expanded
 %sql
 
 -- reinstated this SQL version now that the need to restrict outputs for Provisional & Final has dropped
 --also Final is now Performance anyway!
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CYP_monthly_expanded AS
 SELECT  
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
   
 FROM $db_output.CYP_monthly_unformatted as m
 RIGHT OUTER JOIN global_temp.CYP_monthly_possible_metrics as p
   ON m.BREAKDOWN = p.BREAKDOWN
   AND m.PRIMARY_LEVEL = p.PRIMARY_LEVEL
   AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
   AND m.METRIC = p.METRIC
   AND m.REPORTING_PERIOD_START = '$rp_startdate'
   AND m.REPORTING_PERIOD_END = '$rp_enddate' 
   AND m.STATUS = '$status'
   AND m.SOURCE_DB = '$db_source'
 WHERE (
       (p.METRIC NOT IN ('MHS30e', 'MHS58a', 'MHS61b', 'MHS32a') AND p.BREAKDOWN IN ('England','CCG - GP Practice or Residence','Provider')) OR 
       (p.METRIC IN ('MHS30e', 'MHS61b') AND p.BREAKDOWN IN ('England; ConsMechanismMH', 'CCG - GP Practice or Residence; ConsMechanismMH', 'Provider; ConsMechanismMH')) OR 
       (p.METRIC = 'MHS58a' AND p.BREAKDOWN IN ('England; DNA Reason', 'CCG - GP Practice or Residence; DNA Reason', 'Provider; DNA Reason')) OR 
       (p.METRIC = 'MHS32a' AND p.BREAKDOWN IN ('England', 'CCG - GP Practice or Residence', 'Provider', 'England; Referral Source', 'CCG - GP Practice or Residence; Referral Source', 'Provider; Referral Source'))
     )

# COMMAND ----------

# DBTITLE 1,7. Expand Ascof
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW Ascof_expanded AS 
 SELECT COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START, 
 COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
 COALESCE(m.STATUS, '$status') as STATUS, 
 COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, 
 COALESCE(m.LEVEL_ONE, p.LEVEL) as PRIMARY_LEVEL, 
 m.LEVEL_ONE_DESCRIPTION as PRIMARY_LEVEL_DESCRIPTION, 
 m.LEVEL_TWO as SECONDARY_LEVEL, 
 m.LEVEL_TWO_DESCRIPTION as SECONDARY_LEVEL_DESCRIPTION, 
 m.LEVEL_THREE AS THIRD_LEVEL, 
 'NONE' AS THIRD_LEVEL_DESCRIPTION, 
 COALESCE(m.METRIC, p.METRIC) as METRIC, 
 p.METRIC_NAME AS METRIC_NAME, 
 m.METRIC_VALUE AS METRIC_VALUE, 
 COALESCE(m.SOURCE_DB, '$db_source') AS SOURCE_DB
 
 FROM $db_output.Ascof_unformatted as m 
 RIGHT OUTER JOIN global_temp.ascof_possible_metrics as p 
 ON m.BREAKDOWN = p.BREAKDOWN 
 AND m.LEVEL_ONE = p.LEVEL 
 AND m.METRIC = p.METRIC 
 WHERE m.REPORTING_PERIOD_START = '$rp_startdate' 
 AND m.REPORTING_PERIOD_END = '$rp_enddate'
 AND m.STATUS = '$status'
 AND m.SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,8. Expand FYFV
#only needs to run for quarterly (i.e. when month_id is divisible by 3 with no remainder) and data are final

status = dbutils.widgets.get("status")
db_output = dbutils.widgets.get("db_output")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
month_id = dbutils.widgets.get("month_id")
rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")
db_source = dbutils.widgets.get("db_source")

is_quarter = int(month_id) % 3 ==0

# original (no SOURCE_DB) code
# sql = "CREATE OR REPLACE GLOBAL TEMP VIEW FYFV_expanded AS SELECT COALESCE(m.REPORTING_PERIOD_START, '{rp_startdate}') as REPORTING_PERIOD_START, COALESCE(m.REPORTING_PERIOD_END, '{rp_enddate}') as REPORTING_PERIOD_END,  COALESCE(m.STATUS, '{status}') as STATUS, COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, COALESCE(m.PRIMARY_LEVEL, p.primary_level) as PRIMARY_LEVEL, p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION, COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL, p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, COALESCE(m.METRIC, p.METRIC) as METRIC, p.METRIC_NAME AS METRIC_NAME, m.METRIC_VALUE AS METRIC_VALUE FROM {db_output}.FYFV_unformatted as m RIGHT OUTER JOIN global_temp.FYFV_possible_metrics as p ON m.BREAKDOWN = p.BREAKDOWN AND m.PRIMARY_LEVEL = p.primary_level AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL AND m.METRIC = p.METRIC WHERE m.REPORTING_PERIOD_START = '{rp_startdate_quarterly}' AND m.REPORTING_PERIOD_END = '{rp_enddate}'".format(status=status,db_output=db_output,rp_startdate=rp_startdate,rp_enddate=rp_enddate,rp_startdate_quarterly=rp_startdate_quarterly)

# code below includes SOURCE_DB 
sql = "CREATE OR REPLACE GLOBAL TEMP VIEW FYFV_expanded AS SELECT COALESCE(m.REPORTING_PERIOD_START, '{rp_startdate}') as REPORTING_PERIOD_START, COALESCE(m.REPORTING_PERIOD_END, '{rp_enddate}') as REPORTING_PERIOD_END, COALESCE(m.STATUS, '{status}') as STATUS, COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN, COALESCE(m.PRIMARY_LEVEL, p.primary_level) as PRIMARY_LEVEL, p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION, COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL, p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION, COALESCE(m.METRIC, p.METRIC) as METRIC, p.METRIC_NAME AS METRIC_NAME, m.METRIC_VALUE AS METRIC_VALUE, COALESCE(m.SOURCE_DB, '{db_source}') AS SOURCE_DB FROM {db_output}.FYFV_unformatted as m RIGHT OUTER JOIN global_temp.FYFV_possible_metrics as p ON m.BREAKDOWN = p.BREAKDOWN AND m.PRIMARY_LEVEL = p.primary_level AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL AND m.METRIC = p.METRIC WHERE m.REPORTING_PERIOD_START = '{rp_startdate_quarterly}' AND m.REPORTING_PERIOD_END = '{rp_enddate}' AND m.SOURCE_DB = '{db_source}'".format(status=status,db_output=db_output,db_source=db_source,rp_startdate=rp_startdate,rp_enddate=rp_enddate,rp_startdate_quarterly=rp_startdate_quarterly)

if is_quarter and status != 'Provisional':
  print(is_quarter, status)
  print(sql)
  spark.sql(sql).collect()
else:
  print(is_quarter, status)

# COMMAND ----------

# DBTITLE 1,8. Expand FYFV - commented out - no is_quarter
 %sql
 
 
 --need to add in a condition so that this only runs when month_id is divisible by 3 with no remainder
 
 -- CREATE OR REPLACE GLOBAL TEMP VIEW FYFV_expanded AS
 -- SELECT  
 --   COALESCE(m.REPORTING_PERIOD_START, '$rp_startdate') as REPORTING_PERIOD_START,
 --   COALESCE(m.REPORTING_PERIOD_END, '$rp_enddate') as REPORTING_PERIOD_END,
 --   COALESCE(m.STATUS, '$status') as STATUS,
 --   COALESCE(m.BREAKDOWN, p.BREAKDOWN) as BREAKDOWN,
 --   COALESCE(m.PRIMARY_LEVEL, p.primary_level) as PRIMARY_LEVEL,
 --   p.PRIMARY_LEVEL_DESC as PRIMARY_LEVEL_DESCRIPTION,
 --   COALESCE(m.SECONDARY_LEVEL, p.SECONDARY_LEVEL) as SECONDARY_LEVEL,
 --   p.SECONDARY_LEVEL_DESC as SECONDARY_LEVEL_DESCRIPTION,
 --   COALESCE(m.METRIC, p.METRIC) as METRIC,
 --   p.METRIC_NAME AS METRIC_NAME,
 --   m.METRIC_VALUE AS METRIC_VALUE
 --  FROM $db_output.FYFV_unformatted as m
 -- RIGHT OUTER JOIN global_temp.FYFV_possible_metrics as p
 --   ON m.BREAKDOWN = p.BREAKDOWN
 --   AND m.PRIMARY_LEVEL = p.primary_level
 --   AND m.SECONDARY_LEVEL = p.SECONDARY_LEVEL
 --   AND m.METRIC = p.METRIC
 -- WHERE m.REPORTING_PERIOD_START = '$rp_startdate_quarterly'
 --   AND m.REPORTING_PERIOD_END = '$rp_enddate'