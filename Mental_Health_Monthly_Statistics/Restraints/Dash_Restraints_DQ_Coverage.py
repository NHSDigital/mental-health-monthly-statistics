# Databricks notebook source
# dbutils.widgets.text("db_output", "personal_db")
# db_output  = dbutils.widgets.get("db_output")

# dbutils.widgets.text("db_source", "mhsds_database")
# db_source = dbutils.widgets.get("db_source")

# dbutils.widgets.text("month_id", "1446")
# month_id = dbutils.widgets.get("month_id")

# dbutils.widgets.text("rp_startdate", "2020-09-01")
# rp_startdate = dbutils.widgets.get("rp_startdate")

# dbutils.widgets.text("rp_enddate", "2020-09-30")
# rp_enddate = dbutils.widgets.get("rp_enddate")

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMP VIEW SUBS_TEST AS 
 select
 ReportingPeriodEndDate, ReportingPeriodStartDate ,
 uniqmonthid, 
 ORGIDPROVIDER,
 MAX(UniqSubmissionID) AS UNIQSUBMISSIONID
 from
 $db_source.mhs000header
 where
 FILETYPE <= 2
 AND UNIQMONTHID <= '$month_id'
 GROUP BY 
 ReportingPeriodEndDate, ReportingPeriodStartDate , UNIQMONTHID, ORGIDPROVIDER

# COMMAND ----------

# DBTITLE 1,Changed to MHS515 for v5 changes
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RESTRAINTS_PERFORMANCE AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.restrictiveinttype, a.mhs515uniqid, a.startdaterestrictiveinttype, a.enddaterestrictiveinttype, a.starttimerestrictiveinttype, a.endtimerestrictiveinttype, a.uniqhospprovspellid, ---added for v5 /*** a.uniqwardstayid removed gf ***/
 a.recordnumber
 from $db_source.mhs515restrictiveinterventtype a
 inner join SUBS_TEST b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

 %sql
 ---gets all providers who are on the in scope list provided by Irfan Siddiq on 01/11/2023
 CREATE OR REPLACE GLOBAL TEMP VIEW RESTRAINTS_PROVIDER_LIST AS

 SELECT ORG_CODE as OrgCode, NAME as OrgName

 FROM

 (Select distinct ORG_CODE, NAME, ROW_NUMBER() OVER (PARTITION BY ORG_CODE ORDER BY SYSTEM_CREATED_DATE DESC) AS RN 
 from reference_data.ORG_DAILY
 WHERE ORG_CODE IN ('RBS','RVN','RRP','RXT','RQ3','TAJ','TAD','AHY','RT1','AMX8','RV3','R0A','RXA','RJ8','RYG','RX4','NMJ','RXM','RWV','RDY','RWK','DN7','DE8','R1L','RTQ','RP4','RXV','AHX','RWR','RV9','AHL','AHL','RXY','RW5','RGD','RY6','RT5','RP7','NR5','RW4','RRE','NQL','ATM','RMY','RAT','RLY','RP1','RHA','RNU','RPG','NMV','RT2','NTN','NRC','NRN','RXE','RCU','TAH','RH5','RV5','RQY','RXG','RW1','NYA','8CM63','RX2','RX3','RKE','RKL','NV2','K8E1X'))

 WHERE RN=1

# COMMAND ----------

 %sql
 -- Calculate the actual counts for MHS515 in month
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW TableCounts AS
 (
   SELECT 
     UniqMonthID,
     'MHS515RestrictiveInterventType' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM global_temp.RESTRAINTS_PERFORMANCE
   WHERE uniqmonthid = '$month_id'
   GROUP BY Uniqmonthid, OrgIDProv  
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW restraints_dq_coverage_pre AS
 select
 '$rp_startdate' as REPORTING_PERIOD_START,
 '$rp_enddate' as REPORTING_PERIOD_END,
 'Performance' as STATUS,
 a.OrgCode as ORGANISATION_CODE,
 a.OrgName as ORGANISATION_NAME,
 COALESCE(b.TableName, 'MHS515RestrictiveInterventType') as TABLE_NAME,
 COALESCE(CAST(CASE WHEN b.RowCount < 5 THEN '99999' ELSE ROUND(b.RowCount/5, 0)*5 END as INT), '*') as COVERAGE_COUNT
 FROM global_temp.RESTRAINTS_PROVIDER_LIST a
 LEFT JOIN global_temp.TableCounts b ON a.OrgCode = b.OrgIDProv

# COMMAND ----------

 %sql
 insert into $db_output.dq_coverage_restraints
 select
 date_format(REPORTING_PERIOD_START,  "dd/MM/yyyy"),
 date_format(REPORTING_PERIOD_END,  "dd/MM/yyyy"),
 STATUS,
 ORGANISATION_CODE,
 ORGANISATION_NAME,
 TABLE_NAME,
 CAST(CASE WHEN COVERAGE_COUNT = 99999 THEN '*' ELSE COVERAGE_COUNT END as STRING) as COVERAGE_COUNT
 FROM global_temp.restraints_dq_coverage_pre

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.dq_coverage_restraints