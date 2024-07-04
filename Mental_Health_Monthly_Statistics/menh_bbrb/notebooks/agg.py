# Databricks notebook source
 %run ./mhsds_functions

# COMMAND ----------

 %run ./measure_metadata

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
product = dbutils.widgets.get("product")

# COMMAND ----------

 %sql 
 DELETE FROM $db_output.bbrb_final_raw
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.bbrb_final_raw RETAIN 8 HOURS;

# COMMAND ----------

 %sql 
 DELETE FROM $db_output.bbrb_final_suppressed
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.bbrb_final_suppressed RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Population Agg
for pop_id in pop_metadata:
  pop_name = pop_metadata[pop_id]["measure_name"]
  age_group_filter = pop_metadata[pop_id]["age_group"]
  breakdowns = pop_metadata[pop_id]["breakdowns"]
  for breakdown in breakdowns:
    source_table = breakdown["source_table"]
    aggregate_field = breakdown["aggregate_field"]
    breakdown_name = breakdown["breakdown_name"]
    primary_level = breakdown["primary_level"]
    primary_level_desc = breakdown["primary_level_desc"]
    secondary_level = breakdown["secondary_level"]
    secondary_level_desc = breakdown["secondary_level_desc"]
    print(pop_id, breakdown_name)
    #create agg df
    agg_df = produce_filter_agg_df(
      db_output, db_source, source_table, age_group_filter,
      rp_startdate_1m, rp_enddate, primary_level, primary_level_desc, secondary_level, secondary_level_desc, 
      aggregate_field, breakdown_name, status, 
      pop_id, pop_id, pop_id, pop_name, output_columns
    )
    #insert into unsuppressed table
    insert_unsup_agg(agg_df, db_output, output_columns, "bbrb_final_raw")

# COMMAND ----------

# DBTITLE 1,Get measure metadata depending on product selected
product_measure_metadata = measure_metadata[product] #metadata used in aggregation depends on product

# COMMAND ----------

# DBTITLE 1,Product Agg
bbrb_output_tables = []
for measure_id in product_measure_metadata:
  measure_name = product_measure_metadata[measure_id]["name"]
  measure_freq = product_measure_metadata[measure_id]["freq"]
  measure_rp_startdate = mh_freq_to_rp_startdate(measure_freq, rp_startdate_1m)
  source_table = product_measure_metadata[measure_id]["source_table"]
  filter_clause = product_measure_metadata[measure_id]["filter_clause"]
  aggregate_field = product_measure_metadata[measure_id]["aggregate_field"]
  aggregate_function = product_measure_metadata[measure_id]["aggregate_function"]
  numerator_id = product_measure_metadata[measure_id]["numerator_id"]
  denominator_id = product_measure_metadata[measure_id]["denominator"]
  suppression_type = product_measure_metadata[measure_id]["suppression"]
  measure_breakdowns = product_measure_metadata[measure_id]["breakdowns"]
  output_table = product_measure_metadata[measure_id]["output_table"]
  crude_rate = product_measure_metadata[measure_id]["crude_rate"]
  bbrb_output_tables.append(output_table) #add output table names used in looping into list (used to combine into single table later)
  for breakdown in measure_breakdowns:
    breakdown_name = breakdown["breakdown_name"]
    print(f"{measure_id}: {breakdown_name}")
    if crude_rate == 1:
      primary_level = F.col("PRIMARY_LEVEL")
      primary_level_desc = F.col("PRIMARY_LEVEL_DESCRIPTION")
      secondary_level = F.col("SECONDARY_LEVEL")
      secondary_level_desc = F.col("SECONDARY_LEVEL_DESCRIPTION")
      agg_df = aggregate_function(
      db_output, db_source, output_table, filter_clause,
      measure_rp_startdate, rp_enddate, primary_level, primary_level_desc, secondary_level, secondary_level_desc,
      aggregate_field, breakdown_name, status, measure_id, numerator_id, denominator_id,
      measure_name, output_columns)
    else: ###measure is not a crude rate:
      primary_level = breakdown["primary_level"]
      primary_level_desc = breakdown["primary_level_desc"]
      secondary_level = breakdown["secondary_level"]
      secondary_level_desc = breakdown["secondary_level_desc"]
      #create agg df
      agg_df = aggregate_function(
        db_output, db_source, source_table, filter_clause,
        measure_rp_startdate, rp_enddate, primary_level, primary_level_desc, secondary_level, secondary_level_desc, 
        aggregate_field, breakdown_name, status, measure_id, numerator_id, denominator_id, 
        measure_name, output_columns)
    #insert into unsuppressed table
    insert_unsup_agg(agg_df, db_output, output_columns, output_table)
    #create supp_df
    if breakdown_name in unsup_breakdowns:        
      supp_df = agg_df.filter(F.col("BREAKDOWN") == breakdown_name)
    else:
      insert_df = spark.table(f"{db_output}.bbrb_final_raw")
      supp_df = mhsds_suppression(insert_df, suppression_type, breakdown_name, measure_id, rp_enddate, status, numerator_id, db_source)
    #assert test
    #assert agg_df.count() == supp_df.count(), f"Unsuppressed and Suppressed dataframe sizes are not equal. unsuppressed: {agg_df.count()}  suppressed: {supp_df.count()}"
    #insert into suppressed table
    insert_sup_agg(supp_df, db_output, measure_name, output_columns, "bbrb_final_suppressed")