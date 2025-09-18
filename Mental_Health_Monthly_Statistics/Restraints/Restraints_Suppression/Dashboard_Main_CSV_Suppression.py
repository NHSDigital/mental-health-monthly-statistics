# Databricks notebook source
# dbutils.widgets.text("db_output", "personal_db")
# dbutils.widgets.text("db_source", "mhsds_database")
# dbutils.widgets.text("month_id", "1460")
# dbutils.widgets.text("rp_enddate", "2021-10-31")
# dbutils.widgets.text("rp_startdate", "2021-10-01")

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

 %sql
 select DISTINCT uniqmonthid, reportingperiodstartdate from $db_source.mhs000header order by uniqmonthid desc

# COMMAND ----------

df_test = spark.sql(f"""
select 
ReportingPeriodStartDate
,ReportingPeriodEndDate
,region_code
,region_name
,orgidprov
,orgidname
,specialised_service
,restrictiveintcode
,restrictiveintname
,patient_injury
,variabletype
,variable
,restraints
,incidents
,avg_restraints_per_incident
,people
,bed_days
,ri_per_1000_bed_days
,avg_min_of_restraint
,max_min_of_restraint
,median_min_of_restraint
,avg_min_of_incident	
,max_min_of_incident
,median_min_of_incident
,avg_days_of_active_restraint
,median_days_of_active_restraint
from {db_output}.final_table
""")
# add this clause below whether you want to test duplicates for just values which aren't 0 or all values
# where restraints <> 0 and people <> 0
if df_test.count() > df_test.dropDuplicates().count():
    raise ValueError('Data has duplicate rows')

# COMMAND ----------

df_breakdown_test = spark.sql(f"""
select 
ReportingPeriodStartDate
,ReportingPeriodEndDate
,region_code
,region_name
,orgidprov
,orgidname
,specialised_service
,restrictiveintcode
,restrictiveintname
,patient_injury
,variabletype
,variable
from {db_output}.final_table
""")
# add this clause below whether you want to test duplicates for just values which aren't 0 or all values
# where restraints <> 0 and people <> 0
if df_breakdown_test.count() > df_breakdown_test.dropDuplicates().count():
    raise ValueError('Data has duplicate breakdowns and levels')

# COMMAND ----------

 %sql
 SELECT distinct ReportingPeriodEndDate FROM $db_output.final_table

# COMMAND ----------

 %sql

 SELECT ReportingPeriodStartDate
 ,ReportingPeriodEndDate
 ,region_code
 ,region_name
 ,orgidprov
 ,orgidname
 ,specialised_service
 ,restrictiveintcode
 ,restrictiveintname
 ,patient_injury
 ,variabletype
 ,variable
 ,restraints
 ,incidents
 ,avg_restraints_per_incident
 ,people
 ,bed_days
 ,ri_per_1000_bed_days
 ,avg_min_of_restraint
 ,max_min_of_restraint
 ,median_min_of_restraint
 ,avg_min_of_incident	
 ,max_min_of_incident
 ,median_min_of_incident
 ,avg_days_of_active_restraint
 ,median_days_of_active_restraint
 ,count(*)
 FROM $db_output.final_table
 GROUP BY
 ReportingPeriodStartDate
 ,ReportingPeriodEndDate
 ,region_code
 ,region_name
 ,orgidprov
 ,orgidname
 ,specialised_service
 ,restrictiveintcode
 ,restrictiveintname
 ,patient_injury
 ,variabletype
 ,variable
 ,restraints
 ,incidents
 ,avg_restraints_per_incident
 ,people
 ,bed_days
 ,ri_per_1000_bed_days
 ,avg_min_of_restraint
 ,max_min_of_restraint
 ,median_min_of_restraint
 ,avg_min_of_incident	
 ,max_min_of_incident
 ,median_min_of_incident
 ,avg_days_of_active_restraint
 ,median_days_of_active_restraint
 HAVING count(*)>1

# COMMAND ----------

import pyspark.sql.functions as f
df2_test = df_test.join(df_test.groupBy(df_test.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),on=df_test.columns, how="inner")
display(df2_test.filter(f.col('Duplicate_indicator') == 1))

# COMMAND ----------

import pyspark.sql.functions as f
df3_test = df_breakdown_test.join(df_breakdown_test.groupBy(df_breakdown_test.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),on=df_breakdown_test.columns, how="inner")
display(df3_test.filter(f.col('Duplicate_indicator') == 1))

# COMMAND ----------

# %sql
# CREATE OR REPLACE GLOBAL TEMP VIEW final_unsupp_table AS
# select 
# ReportingPeriodStartDate, 
# ReportingPeriodEndDate,
# region_code,
# region_name,
# orgidprov,
# orgidname,
# specialised_service,
# restrictiveintcode,
# restrictiveintname,
# patient_injury,
# variabletype,
# variable,
# restraints,
# people,
# bed_days,
# ri_per_1000_bed_days,
# avg_min_of_restraint,
# max_min_of_restraint
# from $db_output.final_table
# where restraints <> 0 and people <> 0

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW pre_supp_table AS
 select 
 ReportingPeriodStartDate, 
 ReportingPeriodEndDate,
 region_code,
 region_name,
 orgidprov,
 orgidname,
 specialised_service,
 restrictiveintcode,
 restrictiveintname,
 patient_injury,
 variabletype,
 variable,
 CASE WHEN restraints < 5 THEN '*' ELSE ROUND(restraints/5, 0)*5 END as restraints,
 CASE WHEN (people > 0 and people < 5) THEN '*' ELSE ROUND(people/5, 0)*5 END as people,
 CASE WHEN bed_days < 100 THEN '*' ELSE bed_days END as bed_days,
 ri_per_1000_bed_days, 
 avg_min_of_restraint,
 max_min_of_restraint,
 median_min_of_restraint,
 CASE WHEN incidents < 5 THEN '*' ELSE ROUND(incidents/5, 0)*5 END as incidents,
 CASE WHEN incidents < 5 or restraints < 5 THEN '*' ELSE ROUND(avg_restraints_per_incident, 1) END as avg_restraints_per_incident,
 avg_min_of_incident,
 max_min_of_incident,
 median_min_of_incident,
 avg_days_of_active_restraint,
 median_days_of_active_restraint
 from $db_output.final_table

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW final_supp_table AS
 select 
 ReportingPeriodStartDate, 
 ReportingPeriodEndDate,
 region_code,
 region_name,
 orgidprov,
 orgidname,
 specialised_service,
 restrictiveintcode,
 restrictiveintname,
 patient_injury,
 variabletype,
 variable,
 restraints,
 people,
 CASE WHEN restraints = '*' THEN '*' WHEN bed_days = '*' THEN '*' ELSE ROUND(ri_per_1000_bed_days,0) END AS ri_per_1000_bed_days,
 avg_min_of_restraint,
 max_min_of_restraint,
 median_min_of_restraint,
 incidents,
 avg_restraints_per_incident,
 avg_min_of_incident,
 max_min_of_incident,
 median_min_of_incident,
 avg_days_of_active_restraint,
 median_days_of_active_restraint
 from global_temp.pre_supp_table

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.final_supp_table2;
 CREATE TABLE $db_output.final_supp_table2 USING DELTA AS
 select 
 ReportingPeriodStartDate,
 ReportingPeriodEndDate,
 region_code,
 region_name,
 orgidprov,
 orgidname,
 specialised_service,
 restrictiveintcode,
 restrictiveintname,
 patient_injury,
 variabletype,
 variable,
 restraints,
 CAST(people as INT) as people,
 CAST(ri_per_1000_bed_days as INT) as ri_per_1000_bed_days,
 avg_min_of_restraint,
 max_min_of_restraint,
 median_min_of_restraint,
 CAST(incidents as INT) as incidents,
 avg_restraints_per_incident,
 avg_min_of_incident,
 max_min_of_incident,
 median_min_of_incident,
 avg_days_of_active_restraint,
 median_days_of_active_restraint
 from global_temp.final_supp_table
 where
 restraints <> '*' and people <> '*' and ri_per_1000_bed_days <> '*' and incidents <> '*'