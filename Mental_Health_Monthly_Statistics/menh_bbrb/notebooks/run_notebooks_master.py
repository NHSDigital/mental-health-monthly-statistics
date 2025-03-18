# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

db_source = dbutils.widgets.get("db_source")
print(db_source)
assert db_source

status = dbutils.widgets.get("status")
print(status)
assert status

rp_startdate = dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate

rp_enddate = dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate

product_name = dbutils.widgets.get("product")
print(product_name)
assert product_name

import json

# COMMAND ----------

# DBTITLE 1,Import filepath information for each analysis pipeline preparation
 %run ./prep_filepath_metadata

# COMMAND ----------

# DBTITLE 1,Import parameters
 %run ./parameters

# COMMAND ----------

# DBTITLE 1,Import MHSDS measure metadata
 %run ./measure_metadata

# COMMAND ----------

# DBTITLE 1,Import MHSDS Functions
 %run ./mhsds_functions

# COMMAND ----------

# DBTITLE 1,Initialise MHRunParameters dataclass to set up parameters for month run
mh_run_params = MHRunParameters(db_output, db_source, status, rp_startdate, product_name)
params = mh_run_params.as_dict()
params

# COMMAND ----------

# DBTITLE 1,Create generic tables used in all analysis products
dbutils.notebook.run("./01_Generic_Prepare/00_generic_prep", 0, params)
dbutils.notebook.run("./01_Generic_Prepare/01_generic_stp_region_mapping", 0, params)
dbutils.notebook.run("./01_Generic_Prepare/03_generic_population", 0, params)
dbutils.notebook.run("./01_Generic_Prepare/04_mhsds_ref_tables", 0, params)

# COMMAND ----------

# DBTITLE 1,Run Pre-Processing Table notebooks based on if Header Table has data in or not
header_df = spark.table(f"{db_output}.nhse_pre_proc_header")
if header_df.count() > 0:
  dbutils.notebook.run("./01_Generic_Prepare/05_nhse_pre_processing_tables_static", 0, params)
else:
  dbutils.notebook.run("./01_Generic_Prepare/02_nhse_pre_processing_tables", 0, params)

# COMMAND ----------

# DBTITLE 1,Prepare all or just one product?
if params['product'] != 'ALL':
  for key,values in run_params.items():
    if params['product'][:2] == key[:2]:
      print(values)
      print(f'Product: {key}')
      returned_tables = dbutils.notebook.run(f'{values}', 0, params)
else:
  for key,values in run_params.items():
      print(f'Product: {key}')
      returned_tables = dbutils.notebook.run(f'{values}', 0, params)   

# COMMAND ----------

# DBTITLE 1,Aggregate population and product measures
dbutils.notebook.run("./agg", 0, params)

# COMMAND ----------

# DBTITLE 1,Create CSV output 'skeleton'
dbutils.notebook.run("./csv_skeleton", 0, params)

# COMMAND ----------

 %sql 
 DELETE FROM $db_output.bbrb_final
 WHERE REPORTING_PERIOD_END = '$rp_enddate'
 AND STATUS = '$status'
 AND SOURCE_DB = '$db_source'
 ;
 VACUUM $db_output.bbrb_final_suppressed RETAIN 8 HOURS;

# COMMAND ----------

# DBTITLE 1,Build Unsuppressed Breakdowns dataframe
unsup_breakdowns_data = {"breakdown": unsup_breakdowns} #unsup_breakdowns list is located in parameters
unsup_breakdowns_pdf = pd.DataFrame(unsup_breakdowns_data)
unsup_breakdowns_df = spark.createDataFrame(unsup_breakdowns_pdf)
unsup_breakdowns_df.createOrReplaceTempView("unsup_breakdowns")

# COMMAND ----------

# DBTITLE 1,Join CSV skeleton to suppressed submitters output and insert into final table
 %sql
 INSERT INTO $db_output.bbrb_final
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
 CASE 
   WHEN u.BREAKDOWN is not null THEN COALESCE(b.MEASURE_VALUE, 0) 
   ELSE COALESCE(b.MEASURE_VALUE, "*") 
   END as METRIC_VALUE,
 c.SOURCE_DB
 FROM $db_output.bbrb_csv_lookup c
 LEFT JOIN $db_output.bbrb_final_suppressed b
 ON c.REPORTING_PERIOD_END = b.REPORTING_PERIOD_END
 AND c.STATUS = b.STATUS
 AND c.BREAKDOWN = b.BREAKDOWN
 AND c.PRIMARY_LEVEL = b.PRIMARY_LEVEL
 AND c.SECONDARY_LEVEL = b.SECONDARY_LEVEL
 AND c.MEASURE_ID = b.MEASURE_ID
 AND c.SOURCE_DB = b.SOURCE_DB
 LEFT JOIN unsup_breakdowns u 
 ON c.BREAKDOWN = u.BREAKDOWN

# COMMAND ----------

#unsuppressed output
sqlContext.sql(f"UPDATE {db_output}.bbrb_final_raw SET MEASURE_ID = LEFT(MEASURE_ID, length(MEASURE_ID)-1) WHERE LEFT(MEASURE_ID, 3) = 'OAP'")  #replace OAPs RP-specific MEASURE_IDs with same MEASURE_ID
  #suppressed output
sqlContext.sql(f"UPDATE {db_output}.bbrb_final_suppressed SET MEASURE_ID = LEFT(MEASURE_ID, length(MEASURE_ID)-1) WHERE LEFT(MEASURE_ID, 3) = 'OAP'") #replace OAPs RP-specific MEASURE_IDs with same MEASURE_ID
  #final output
sqlContext.sql(f"UPDATE {db_output}.bbrb_final SET MEASURE_ID = LEFT(MEASURE_ID, length(MEASURE_ID)-1) WHERE LEFT(MEASURE_ID, 3) = 'OAP'") #replace OAPs RP-specific MEASURE_IDs with same MEASURE_ID

# COMMAND ----------

if int(mh_run_params.end_month_id) > 1467:  # the last month of CCGs is June 2022
  #unsuppressed output
  sqlContext.sql(f"UPDATE {db_output}.bbrb_final_raw SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.bbrb_final_raw SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #suppressed output
  sqlContext.sql(f"UPDATE {db_output}.bbrb_final_suppressed SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.bbrb_final_suppressed SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #final output
  sqlContext.sql(f"UPDATE {db_output}.bbrb_csv_lookup SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.bbrb_csv_lookup SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #final output
  sqlContext.sql(f"UPDATE {db_output}.bbrb_final SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.bbrb_final SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB


# COMMAND ----------

# DBTITLE 1,Assert Test to assure all submitted data is in final output
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

bbrb_final_df = spark.table(f"{db_output}.bbrb_final").filter((F.col("REPORTING_PERIOD_END") == rp_enddate) & (F.col("STATUS") == status) & (F.col("SOURCE_DB") == db_source))
bbrb_final_pdf = bbrb_final_df.toPandas()
supp_data_df = spark.table(f"{db_output}.bbrb_final_suppressed").filter((F.col("REPORTING_PERIOD_END") == rp_enddate) & (F.col("STATUS") == status) & (F.col("SOURCE_DB") == db_source))
supp_data_pdf = supp_data_df.toPandas()

test_df = pd.merge(supp_data_pdf, bbrb_final_pdf, on=output_columns, how="left", indicator="exists")
test_df["exists"] = np.where(test_df.exists == "both", True, False)

if test_df.exists.eq(True).all():
  notes = "No errors"
else:
  notes = "Rows in bbrb_final_suppressed do not exist in bbrb_final"
print(notes)

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.bbrb_breakdown_values;
 TRUNCATE TABLE $db_output.bbrb_level_values;
 TRUNCATE TABLE $db_output.bbrb_csv_lookup;
 TRUNCATE TABLE $db_output.bbrb_final_suppressed

# COMMAND ----------

dbutils.notebook.exit(notes)