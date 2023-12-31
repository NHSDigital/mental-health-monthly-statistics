# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]

# COMMAND ----------

dbutils.widgets.text("db_output","$user_id")
dbutils.widgets.text("db_source","$reference_data")
dbutils.widgets.text("status","Provisional")
dbutils.widgets.dropdown("rp_enddate", "2021-03-31", endchoices)
dbutils.widgets.dropdown("rp_startdate_1m", "2021-03-01", startchoices)
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
status = dbutils.widgets.get("status")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate_1m")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Get MHRunParameters, Aggregation Functions and Measure Metadata
 %run ./Agg_Functions

# COMMAND ----------

#initialise MHRunParameters DataClass
mh_run_params = MHRunParameters(db_output, db_source, status, rp_startdate, "ALL")
mh_run_params.as_dict()

# COMMAND ----------

# DBTITLE 1,Run Output and Reference Tables
dbutils.notebook.run('Reference_Tables', 0, mh_run_params.as_dict())

# COMMAND ----------

# DBTITLE 1,Run NHSE Pre-Processing Tables
dbutils.notebook.run('NHSE_Pre_Processing_Tables', 0, mh_run_params.as_dict())

# COMMAND ----------

# DBTITLE 1,Run CYP Outcomes Prep Tables
dbutils.notebook.run('Prep/CYPOutcomes_Prep', 0, mh_run_params.as_dict())

# COMMAND ----------

# DBTITLE 1,Run IPS Prep Tables
dbutils.notebook.run('Prep/IPS_Prep', 0, mh_run_params.as_dict())

# COMMAND ----------

# DBTITLE 1,Run UEC Prep Tables
dbutils.notebook.run('Prep/UEC_Prep', 0, mh_run_params.as_dict())

# COMMAND ----------

# DBTITLE 1,Aggregate Loop through Measures and Breakdowns
bbrb_output_tables = []
for measure_id in mhsds_measure_metadata:
  measure_name = mhsds_measure_metadata[measure_id]["name"]
  measure_freq = mhsds_measure_metadata[measure_id]["freq"]
  measure_rp_startdate = mh_freq_to_rp_startdate(mh_run_params, measure_freq)
  source_table = mhsds_measure_metadata[measure_id]["source_table"]
  filter_clause = mhsds_measure_metadata[measure_id]["filter_clause"]
  aggregate_field = mhsds_measure_metadata[measure_id]["aggregate_field"]
  aggregate_function = mhsds_measure_metadata[measure_id]["aggregate_function"]
  numerator_id = mhsds_measure_metadata[measure_id]["numerator_id"]
  suppression_type = mhsds_measure_metadata[measure_id]["suppression"]
  measure_breakdowns = mhsds_measure_metadata[measure_id]["breakdowns"]
  output_table = mhsds_measure_metadata[measure_id]["output_table"]
  bbrb_output_tables.append(output_table) #add output table names used in looping into list (used to combine into single table later)  
  for breakdown in measure_breakdowns:
    breakdown_name = breakdown["breakdown_name"]
    primary_level = breakdown["primary_level"]
    primary_level_desc = breakdown["primary_level_desc"]
    secondary_level = breakdown["secondary_level"]
    secondary_level_desc = breakdown["secondary_level_desc"]
    print(measure_id, breakdown_name)
    #create agg df
    agg_df = aggregate_function(
      db_output, source_table, filter_clause,
      measure_rp_startdate, rp_enddate, primary_level, primary_level_desc, secondary_level, secondary_level_desc, 
      aggregate_field, breakdown_name, status, measure_id, numerator_id, measure_name, output_columns
    )
    #insert into unsuppressed table
    insert_unsup_agg(agg_df, db_output, output_columns, "menh_output_unsuppressed")
    #create supp_df
    if breakdown_name in unsup_breakdowns:
      supp_df = agg_df.filter(F.col("BREAKDOWN") == breakdown_name)
    else:
      insert_df = spark.table(f"{db_output}.menh_output_unsuppressed")
      supp_df = mhsds_suppression(insert_df, suppression_type, breakdown_name, measure_id, rp_enddate, status, numerator_id)
    #assert test
    assert agg_df.count() == supp_df.count(), f"Unsuppressed and Suppressed dataframe sizes are not equal. unsuppressed: {agg_df.count()}  suppressed: {supp_df.count()}"
    #insert into suppressed table
    insert_sup_agg(supp_df, db_output, measure_name, output_columns, "menh_output_suppressed")

# COMMAND ----------

insert_bbrb_lookup_values(mh_run_params, mhsds_measure_metadata)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.menh_output_final
 SELECT
 c.REPORTING_PERIOD_START,
 c.REPORTING_PERIOD_END,
 c.STATUS,
 c.BREAKDOWN,
 c.PRIMARY_LEVEL,
 c.PRIMARY_LEVEL_DESCRIPTION,
 c.SECONDARY_LEVEL,
 c.SECONDARY_LEVEL_DESCRIPTION,
 c.MEASURE_ID,
 c.MEASURE_NAME,
 COALESCE(b.MEASURE_VALUE, "*") as MEASURE_VALUE
 FROM $db_output.bbrb_csv_lookup c
 LEFT JOIN $db_output.menh_output_suppressed b
 ON c.REPORTING_PERIOD_END = b.REPORTING_PERIOD_END
 AND c.STATUS = b.STATUS
 AND c.BREAKDOWN = b.BREAKDOWN
 AND c.PRIMARY_LEVEL = b.PRIMARY_LEVEL
 AND c.SECONDARY_LEVEL = b.SECONDARY_LEVEL
 AND c.MEASURE_ID = b.MEASURE_ID

# COMMAND ----------

import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 600)

bbrb_final_df = spark.table(f"{db_output}.menh_output_final").toPandas()
supp_data_df = spark.table(f"{db_output}.menh_output_suppressed").toPandas()

test_df = pd.merge(supp_data_df, bbrb_final_df, on=output_columns, how="left", indicator="exists")
test_df["exists"] = np.where(test_df.exists == "both", True, False)

assert test_df.exists.eq(True).all(), f"""
Rows in submitted data do not exist in final output. {len(test_df[test_df["exists"] != True])} rows below:
{test_df[test_df["exists"] != True].sort_values(by=["MEASURE_ID", "BREAKDOWN", "PRIMARY_LEVEL", "SECONDARY_LEVEL"])}
"""
print("all submitted data present in final output: PASSED")

# COMMAND ----------

if int(mh_run_params.end_month_id) > 1467:  # the last month of CCGs is June 2022
  #unsuppressed output
  sqlContext.sql(f"UPDATE {db_output}.menh_output_unsuppressed SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.menh_output_unsuppressed SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #suppressed output
  sqlContext.sql(f"UPDATE {db_output}.menh_output_suppressed SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.menh_output_suppressed SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #final output
  sqlContext.sql(f"UPDATE {db_output}.menh_output_final SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.menh_output_final SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB

# COMMAND ----------

# DBTITLE 1,Unsuppressed Output (Download)
 %sql
 select * from $db_output.menh_output_unsuppressed order by BREAKDOWN, MEASURE_ID, PRIMARY_LEVEL

# COMMAND ----------

# DBTITLE 1,Suppressed Output (Download)
 %sql
 select * from $db_output.menh_output_final order by MEASURE_ID, BREAKDOWN, PRIMARY_LEVEL

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "menh_output_unsuppressed",
  "suppressed_table": "menh_output_final"
}))