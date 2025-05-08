# Databricks notebook source
# dbutils.widgets.text("db_output", "")
# dbutils.widgets.text("month_id", "1446")
# dbutils.widgets.text("rp_enddate", "2020-09-30")
# dbutils.widgets.text("rp_startdate", "2020-09-01")

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
print(month_id)

# COMMAND ----------

df_test = spark.sql("""
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
variabletype,
variable,
restraints,
people,
bed_days,
ri_per_1000_bed_days
from {}.final_table
""".format(db_output))
# add this clause below whether you want to test duplicates for just values which aren't 0 or all values
# where restraints <> 0 and people <> 0
if df_test.count() > df_test.dropDuplicates().count():
    raise ValueError('Data has duplicates')

# COMMAND ----------

import pyspark.sql.functions as f
df2_test = df_test.join(df_test.groupBy(df_test.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),on=df_test.columns, how="inner")
display(df2_test[df2_test['Duplicate_indicator'] == 1])

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW final_unsupp_table AS
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
 variabletype,
 variable,
 restraints,
 people,
 bed_days,
 ri_per_1000_bed_days,
 avg_min_of_restraint,
 max_min_of_restraint
 from $db_output.final_table
 where restraints <> 0 and people <> 0

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
 variabletype,
 variable,
 CASE WHEN restraints < 5 THEN '*' ELSE ROUND(restraints/5, 0)*5 END as restraints,
 CASE WHEN people < 5 THEN '*' ELSE ROUND(people/5, 0)*5 END as people,
 CASE WHEN bed_days < 100 THEN '*' ELSE bed_days END as bed_days,
 ri_per_1000_bed_days, 
 avg_min_of_restraint,
 max_min_of_restraint
 from global_temp.final_unsupp_table

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
 variabletype,
 variable,
 restraints,
 people,
 CASE WHEN restraints = '*' THEN '*' WHEN bed_days = '*' THEN '*' ELSE ROUND(ri_per_1000_bed_days,0) END AS ri_per_1000_bed_days,
 avg_min_of_restraint,
 max_min_of_restraint
 from global_temp.pre_supp_table

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.final_supp_table2;
 CREATE TABLE IF NOT EXISTS $db_output.final_supp_table2 AS
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
 variabletype,
 variable,
 CAST(restraints as INT) as restraints,
 CAST(people as INT) as people,
 CAST(ri_per_1000_bed_days as INT) as ri_per_1000_bed_days,
 avg_min_of_restraint,
 max_min_of_restraint
 from global_temp.final_supp_table
 where
 restraints <> '*' and people <> '*' and ri_per_1000_bed_days <> '*'