# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

 %run ./measure_metadata

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
product = dbutils.widgets.get("product")
end_month_id = dbutils.widgets.get("end_month_id")

# COMMAND ----------

# DBTITLE 1,Create Measure_ID lookup dataframe
amd = measure_metadata[product]
 
du = {x: "" for x in unsup_breakdowns}
l2 = []
 
for measure_id in amd:
  for breakdown in amd[measure_id]["breakdowns"]:
    try:
      x1 = unsup_breakdowns.index(breakdown["breakdown_name"])
      SupType = "Nat"
    except:
      SupType = "SubNat"
                                  
#     if any(breakdown["breakdown_name"] in unsup_breakdowns for breakdown in unsup_breakdowns): SupType = "Nat"
    l2 += [[
      SupType, 
      amd[measure_id]["suppression"],       
      measure_id, 
      amd[measure_id]["freq"],
      amd[measure_id]["numerator_id"],       
      amd[measure_id]["crude_rate"],       
      breakdown["breakdown_name"],       
      amd[measure_id]["name"]     
      ]]

for x in l2: 
  pass
 
schema = "SupType string, suppression string, measure_id string, freq string, numerator_id string, crude_rate int, breakdown string, measure_name string"
den_num_df = spark.createDataFrame(l2, schema = schema)
spark.sql(f"drop table if exists {db_output}.mapDenNum")
den_num_df.write.saveAsTable(f"{db_output}.mapDenNum", mode = 'overwrite')
display(den_num_df)

# COMMAND ----------

# DBTITLE 1,Get Breakdown level lookup dataframe
bd_freq_dict = {}
warnings = []
          
for measure_id in amd:
  freq = amd[measure_id]["freq"]
  breakdowns = amd[measure_id]["breakdowns"]
  listdf1 = []
  for breakdown in breakdowns:
    bd_name = get_var_name(breakdown)
    bd_freq = bd_name + freq
          
    if bd_freq not in bd_freq_dict:
      print(timenow(), len(bd_freq_dict), "Create dataframe for", bd_name)
      df1 = createbreakdowndf(db_output, end_month_id, breakdown, freq)
      bd_freq_dict[bd_freq] = createbreakdowndf(db_output, end_month_id, breakdown, freq)
        
for x in warnings: 
  print(x)
  
bd_df = unionAll(*[bd_freq_dict[bd_name] for bd_name in bd_freq_dict])
display(bd_df)

# COMMAND ----------

# DBTITLE 1,Create Measure_ID, Breakdown combination lookup dataframe
lmtbd = []
for measure_id in amd:
  freq = amd[measure_id]["freq"]
  breakdowns = amd[measure_id]["breakdowns"]
  rp_startdate_freq = mh_freq_to_rp_startdate(freq, rp_startdate)
  for breakdown in breakdowns:
    bd_name = get_var_name(breakdown)    
    # SupType = amd[measure_id]["suppression"][:1].upper()
    SupType = "SubNat"
    if breakdown["breakdown_name"] in unsup_breakdowns: SupType = "Nat"
    lmtbd += [[
      bd_name, breakdown["breakdown_name"], freq, SupType, status, db_source, rp_startdate_freq, rp_enddate, measure_id, amd[measure_id]["name"]
    ]]
    
met_bd_df = spark.createDataFrame(lmtbd, schema = "bd_name string, breakdown string, freq string, SupType string, status string, db_source string, rp_startdate string, rp_enddate string, measure_id string, measure_name string")
display(met_bd_df)

# COMMAND ----------

# DBTITLE 1,Join 2 lookup dataframes together based on bd_name (dictionary name), breakdown_name and frequency (to account for measures with multiple RPs)
df2 = bd_df.join(met_bd_df, how = 'left', on=['bd_name', 'breakdown', 'freq'])
df2 = df2.distinct()
df2.createOrReplaceTempView("csv_skeleton")
display(df2)

# COMMAND ----------

# DBTITLE 1,Insert into bbrb_csv_lookup table
 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_csv_lookup
 select
 rp_startdate as REPORTING_PERIOD_START,
 rp_enddate as REPORTING_PERIOD_END,
 status as STATUS,
 breakdown as BREAKDOWN,
 primary_level as PRIMARY_LEVEL,
 primary_level_desc as PRIMARY_LEVEL_DESCRIPTION,
 secondary_level as SECONDARY_LEVEL,
 secondary_level_desc as SECONDARY_LEVEL_DESCRIPTION,
 measure_id as MEASURE_ID,
 measure_name as MEASURE_NAME,
 db_source as SOURCE_DB
 from csv_skeleton