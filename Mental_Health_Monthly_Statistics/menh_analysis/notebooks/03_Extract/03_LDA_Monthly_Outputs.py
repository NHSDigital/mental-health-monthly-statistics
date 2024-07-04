# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create Widgets
# # databases where the tables to extract from are
# dbutils.widgets.text("db_output", "menh_analysis", "db_output");
# dbutils.widgets.text("db_source", "mh_v5_pre_pseudo_d1", "db_source");

# # parameters for non-standard runs
# dbutils.widgets.text("rp_startdate", "2020-12-01", "rp_startdate");
# dbutils.widgets.text("status", "", "status");
# dbutils.widgets.text("month_id", "1461", "month_id");
# dbutils.widgets.text("justrun", "", "justrun")
# 

# COMMAND ----------

# DBTITLE 1,Import Functions
import os
from datetime import datetime, date
import json
from dateutil.relativedelta import relativedelta
from dsp.common.exports import create_csv_for_download
from dsp.code_promotion.mesh_send import cp_mesh_send

# COMMAND ----------

# DBTITLE 1,Pull Parameters from Widgets
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")

rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate  = dbutils.widgets.get("rp_enddate")
month_id = dbutils.widgets.get("month_id")
# status = dbutils.widgets.get("status")
# justrun = dbutils.widgets.get("justrun")

# COMMAND ----------

# DBTITLE 1,for standalone run/testing
# db_output =  "menh_analysis"
# reference_data =  "reference_data"
# status = "Performance"

# v4.1 code
# month_id = "1449"
# rp_enddate =  "2020-12-31"
# rp_startdate = "2020-12-01"


# v5 code
# month_id = "1461"
# rp_enddate =  "2021-12-31"
# rp_startdate = "2021-12-01"

# COMMAND ----------

params = {
  'db_source' : db_source, 
  'db_output' : db_output, 
  'rp_enddate': rp_enddate,
  'month_id': month_id,
  'rp_startdate' : rp_startdate
}


date_for_month = datetime.strptime(params['rp_startdate'], "%Y-%m-%d")
PreviousMonthEnd = (date_for_month + relativedelta(days = -1)).strftime("%Y-%m-%d")
params['PreviousMonthEnd'] = PreviousMonthEnd

YYYY = params['rp_startdate'][:4]
Mname = datetime.strptime(params['rp_startdate'], '%Y-%m-%d').strftime("%b")
MonthPeriod = Mname + '-' + YYYY[:2]

params['MonthPeriod'] = MonthPeriod 
params['YYYY'] = YYYY 
params['Mname'] = Mname
assert datetime.strptime(params['rp_enddate'], '%Y-%m-%d')

print('Parameters: {}'.format(json.dumps(params,
                                        indent = 4,
                                        sort_keys = True)))

mailbox_to = 'X26HC004'
workflow_id = 'GNASH_MHSDS'
# local_id = str(datetime.now().date()) +'-menh_analysis' # Confluence doesn't specify which id to pass as it says 'user specified id'. So given date combo with project name -- updated to contain filename for renaming via LEAD_MESH process

# COMMAND ----------

# DBTITLE 1,LDA Filename parts (depending on $db_source used)
if db_source == "menh_point_in_time":
  lda_filename_part = f"pit_{Mname}_{YYYY}"
else:
  lda_filename_part = f"{Mname}_{YYYY}"

# COMMAND ----------

# DBTITLE 1,RUN LDA & output LDA_ALL

if int(params['month_id']) < 1459:
  #   RUN LDA_child
  print('running v4.1 code')
  dbutils.notebook.run("./001_Monthly_Code/LDA_child_v4.1", 0, params)
  #   print('outputs can be downloaded from the Ephemeral Notebook link above')
else:
  #   RUN LDA_child
  print('running v5 code')
  dbutils.notebook.run("./001_Monthly_Code/LDA_child_v5", 0, params)
  #   print('outputs can be downloaded from the Ephemeral Notebook link above')

# OUTPUT LDA_ALL  LDA_Monthly_Mmm_YYYY
df_to_extract = spark.table(f'{db_output}.lda_all')
#   RunTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#CHECK FOR DUPLICATES IN OUTPUT SENSE CHECK
if df_to_extract.count() > df_to_extract.dropDuplicates().count():
  display(df_to_extract.exceptAll(df_to_extract.dropDuplicates()))
  raise ValueError("LDA ALL data has duplicates. Duplicate rows above")
else:
  print("test_lda_all_duplicates: PASSED")

filename = f"LDA_Monthly_{lda_filename_part}.csv"
local_id = filename
#to help with local testing and avoiding the commenting and uncommenting the code
print(local_id)

if(os.environ.get('env') == 'prod'):
  filename = f"LDA_Monthly_{lda_filename_part}.csv"
  try:
    request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
    print(f"{filename} file has been pushed to MESH with request id {request_id}. \n")
    print("df_to_extract rowcount", df_to_extract.count())
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
else:
#   display(df_to_extract)
  print("df_to_extract rowcount", df_to_extract.count())

# COMMAND ----------

# DBTITLE 1,output LDA_MI -
# OUTPUT LDA_MI  LDA_MI_Mmm_YYYY
df_to_extract = spark.table(f'{db_output}.lda_mi')
#   RunTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#CHECK FOR DUPLICATES IN OUTPUT SENSE CHECK
if df_to_extract.count() > df_to_extract.dropDuplicates().count():
  display(df_to_extract.exceptAll(df_to_extract.dropDuplicates()))
  raise ValueError("LDA MI data has duplicates. Duplicate rows above")
else:
  print("test_lda_mi_duplicates: PASSED")

if(os.environ.get('env') == 'prod'):
  filename = f"LDA_MI_{lda_filename_part}.csv"
  local_id = filename
  print(local_id)
  try:
    request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
    print(f"{filename} file has been pushed to MESH with request id {request_id}. \n")
    print("df_to_extract rowcount", df_to_extract.count())
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
else:
#   display(df_to_extract)
  print("df_to_extract rowcount", df_to_extract.count())



# COMMAND ----------

# DBTITLE 1,output LDA_Table14_<ShortMonth_<year>
# OUTPUT LDA_Table14
df_to_extract = spark.table(f'{db_output}.lda_table14')
#   RunTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#CHECK FOR DUPLICATES IN OUTPUT SENSE CHECK
if df_to_extract.count() > df_to_extract.dropDuplicates().count():
  display(df_to_extract.exceptAll(df_to_extract.dropDuplicates()))
  raise ValueError("LDA Table14 data has duplicates. Duplicate rows above")
else:
  print("test_lda_table14_duplicates: PASSED")

if(os.environ.get('env') == 'prod'):
  filename = f"LDA_Table14_{lda_filename_part}.csv"
  local_id = filename
  print(local_id)
  try:
    request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
    print(f"{filename} file has been pushed to MESH with request id {request_id}. \n")
    print("df_to_extract rowcount", df_to_extract.count())
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
else:
#   display(df_to_extract)
  print("df_to_extract rowcount", df_to_extract.count())

# COMMAND ----------

# DBTITLE 1,output LDA_Published_<ShortMonth>_<year>
# OUTPUT LDA_Published
df_to_extract = spark.table(f'{db_output}.lda_published')
#   RunTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#CHECK FOR DUPLICATES IN OUTPUT SENSE CHECK
if df_to_extract.count() > df_to_extract.dropDuplicates().count():
  display(df_to_extract.exceptAll(df_to_extract.dropDuplicates()))
  raise ValueError("LDA Published CSV data has duplicates. Duplicate rows above")
else:
  print("test_lda_published_duplicates: PASSED")

if(os.environ.get('env') == 'prod'):
  filename = f"LDA_Published_{lda_filename_part}.csv"
  local_id = filename
  print(local_id)
  try:
    request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
    print(f"{filename} file has been pushed to MESH with request id {request_id}. \n")
    print("df_to_extract rowcount", df_to_extract.count())
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
else:
#   display(df_to_extract)
  print("df_to_extract rowcount", df_to_extract.count())

# COMMAND ----------

# DBTITLE 1,RUN LDA_nonRespite & output 
if int(params['month_id']) < 1459:
  #   RUN LDA_child
  print('running v4.1 code')
  dbutils.notebook.run("./001_Monthly_Code/LDA_child_non-respite_v4.1", 0, params)
else:
  #   RUN LDA_child
  print('running v5 code')
  dbutils.notebook.run("./001_Monthly_Code/LDA_child_non-respite_v5", 0, params)


# OUTPUT LDA_nonRespite  LDA_nonRespite_Mmm_YYYY
df_to_extract = spark.table(f'{db_output}.lda_nonR')
#   RunTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#CHECK FOR DUPLICATES IN OUTPUT SENSE CHECK
if df_to_extract.count() > df_to_extract.dropDuplicates().count():
  display(df_to_extract.exceptAll(df_to_extract.dropDuplicates()))
  raise ValueError("LDA non-respite data has duplicates. Duplicate rows above")
else:
  print("test_lda_non_respite_duplicates: PASSED")


if(os.environ.get('env') == 'prod'):
  filename = f"LDA_nonRespite_{lda_filename_part}.csv"
  local_id = filename
  print(local_id)
  try:
    request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
    print(f"{filename} file has been pushed to MESH with request id {request_id}. \n")
    print("df_to_extract rowcount", df_to_extract.count())
  except Exception as ex:
    print(ex, 'MESH exception on SPARK 3 can be ignored, file will be delivered in the destined path')
else:
#   display(df_to_extract)
  print("df_to_extract rowcount", df_to_extract.count())


# COMMAND ----------

 %sql
 
 -- remove person level data
 
 TRUNCATE TABLE $db_output.LDA_Data_1;