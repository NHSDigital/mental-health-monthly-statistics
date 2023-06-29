# Databricks notebook source
 %sql
 
 OPTIMIZE $db_output.output1 ZORDER BY (Measure_ID);

# COMMAND ----------

 %sql
 --Create lookup table for Metrics
 DROP TABLE IF EXISTS $db_output.metric_name_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.metric_name_lookup
 (
     MEASURE_ID STRING,
     MEASURE_NAME STRING
 )
 using delta;

# COMMAND ----------

# DBTITLE 1,Metric name Lookup
 %sql
 INSERT INTO $db_output.metric_name_lookup VALUES
 ('MHS01', 'People in contact with services at the end of the reporting period'),
 ('MHS07', 'People with an open hospital spell at the end of the reporting period'),
 ('MHS23d','Open referrals at the end of the RP to community mental health services for adult and older adults with severe mental illness'),
 ('MHS27a','Number of admissions to hospital in the reporting period'),
 ('MHS29', 'Contacts in the reporting period'),
 ('MHS29a', 'Contacts with perinatal MH team in the Reporting Period'),
 ('MHS29d','Contacts in the RP with community mental health services for adult and older adults with severe mental illness'),
 ('MHS29f','Care Contacts by Attended / did not attend code'),
 ('MHS30a', 'Attended contacts with perinatal MH team in Reporting Period'),
 ('MHS30f','Attended contacts in the RP with community mental health services for adult and older adults with severe mental illness'),
 ('MHS30h','Attended contacts in the RP by consultation medium'),
 ('MHS32', 'Referrals starting in the reporting period'),
 ('MHS32c','Referrals starting in the RP to community mental health services for adult and older adults with severe mental illness'),
 ('MHS32d','Referrals starting in the RP to specialist perinatal Mental Health services'),
 ('MHS57b','People discharged from a referral in the RP from community mental health services for adult and older adults with severe mental illness'),
 ('MHS57c','People discharged from a referral in the RP from specialist perinatal Mental Health services');

# COMMAND ----------

# DBTITLE 1,Change all NULL cells to NONE
 %sql
 
 DROP TABLE IF EXISTS $db_output.output_v2;
 CREATE TABLE         $db_output.output_v2 USING DELTA AS
 select Breakdown
       ,PRIMARY_LEVEL
       ,PRIMARY_LEVEL_DESCRIPTION
       ,case when SECONDARY_LEVEL = 'NULL' then 'NONE' else SECONDARY_LEVEL end as SECONDARY_LEVEL
       ,case when SECONDARY_LEVEL_DESCRIPTION = 'NULL' then 'NONE' else SECONDARY_LEVEL_DESCRIPTION end as SECONDARY_LEVEL_DESCRIPTION
       ,measure_id
       ,measure_value
 from  $db_output.output1;
 
 OPTIMIZE $db_output.output_v2;

# COMMAND ----------

# DBTITLE 1,Add data onto the csv_master reference data
 %sql
 
 DROP TABLE IF EXISTS $db_output.output_unsuppressed;
 CREATE TABLE         $db_output.output_unsuppressed USING DELTA AS
 select distinct a.*
                 ,c.MEASURE_NAME
                 ,coalesce(CAST(b.measure_value as int), 0) as MEASURE_VALUE
 from $db_output.csv_master a
 left join $db_output.output_v2 b
       on a.breakdown = b.breakdown 
       and a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
       and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL 
       and a.measure_id = b.measure_id
 inner join $db_output.metric_name_lookup c
    on a.MEASURE_ID = c.MEASURE_ID;

# COMMAND ----------

# DBTITLE 1,Create suppression
 %sql
 ---count metric suppression
 DROP TABLE IF EXISTS $db_output.output_v2_suppressed_pt1;
 CREATE TABLE         $db_output.output_v2_suppressed_pt1 USING DELTA AS
 select 
 REPORTING_PERIOD_START 
 ,REPORTING_PERIOD_END
 ,'$status' AS STATUS
 ,BREAKDOWN
 ,PRIMARY_LEVEL
 ,PRIMARY_LEVEL_DESCRIPTION
 ,SECONDARY_LEVEL
 ,SECONDARY_LEVEL_DESCRIPTION
 ,MEASURE_ID
 ,MEASURE_NAME
 ,cast(case when cast(Measure_value as float) < 5 then '9999999' else cast(round(measure_value/5,0)*5 as float) end as string) as  MEASURE_VALUE
 from $db_output.output_unsuppressed
 where LEFT(BREAKDOWN,7) != 'England'
 
 union all
 --adding in breakdowns which don't need to be suppressed
 select 
 REPORTING_PERIOD_START 
 ,REPORTING_PERIOD_END
 ,'$status' AS STATUS
 ,BREAKDOWN
 ,PRIMARY_LEVEL
 ,PRIMARY_LEVEL_DESCRIPTION
 ,SECONDARY_LEVEL
 ,SECONDARY_LEVEL_DESCRIPTION
 ,MEASURE_ID
 ,MEASURE_NAME
 ,MEASURE_VALUE
 from $db_output.output_unsuppressed
 where LEFT(BREAKDOWN,7) = 'England';
 
 OPTIMIZE $db_output.output_v2_suppressed_pt1;

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.output_suppressed_final_1;
 CREATE TABLE         $db_output.output_suppressed_final_1 USING DELTA AS
 Select a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.PRIMARY_LEVEL
 ,a.PRIMARY_LEVEL_DESCRIPTION
 ,a.SECONDARY_LEVEL
 ,a.SECONDARY_LEVEL_DESCRIPTION
 ,a.MEASURE_ID
 ,a.MEASURE_NAME
 ,case when MEASURE_VALUE = '9999999' then '*' else MEASURE_VALUE end as MEASURE_VALUE
 from $db_output.output_v2_suppressed_pt1 a;
 OPTIMIZE $db_output.output_suppressed_final_1

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))