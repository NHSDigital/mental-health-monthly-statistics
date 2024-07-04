# Databricks notebook source
# dbutils.widgets.text("db_source","testdata_menh_analysis_mh_v5_pre_pseudo_d1","db_source")
db_source = dbutils.widgets.get("db_source")
assert db_source

# COMMAND ----------

'''IsColumnInTable'''

# does datebase.table contain column? 1=yes, 0=no
def IsColumnInTable(database_name, table_name, column_name):
  try:
    df = spark.table(f"{database_name}.{table_name}")
    cols = df.columns
    if column_name in cols:
      return 1
    else:
      return 0
  except:
    return -1

# COMMAND ----------

# List of tables and column that needs adding to them
# 'mhs001mpi': 'OrgIDICBRes' name change from OrgIDICSRes
# 'mhs002gp': 'OrgIDICBGPPractice' name change from OrgIDICSGPPractice

tableColumn = {'mhs001mpi': 'ICRECICB'}


# 'mhs001mpi': 'RegionICRECSUBICBName', 
#    'mhs001mpi': 'RegionICRECSUBICB', 
#   , 'mhs002gp': 'OrgIDSubICBLocGP'
# , 'mhs001mpi': 'OrgIDSubICBLocResidence','mhs002gp': 'OrgIDICBGPPractice'
# , 'mhs001mpi': 'OrgIDICBRes', 'mhs001mpi': 'ICRECSUBICBNAME', 'mhs001mpi': 'ICRECSUBICB', 'mhs001mpi': 'ICRECICBName'

# list of tables and columns that need INT type columns adding
# tableColumnINT = {'main_monthly_unformatted_new': 'PRODUCT_NO'}

# COMMAND ----------

'''Add STRING column to table if it doesn't exist'''

for table, column in tableColumn.items():
  print(table)
  print(column)
  exists = IsColumnInTable(db_source, table, column)
  if exists == 0:
    action = """ALTER TABLE {db_source}.{table} ADD COLUMNS ({column} STRING)""".format(db_source=db_source,table=table,column=column)
    print(action)
    spark.sql(action)
    

'''Add INT column to table if it doesn't exist'''    

# for table, column in tableColumnINT.items():
#   print(table)
#   print(column)
#   exists = IsColumnInTable(db_output, table, column)
#   if exists == 0:
#     action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} INT)""".format(db_output=db_output,table=table,column=column)
#     print(action)
#     spark.sql(action)

# COMMAND ----------

 %sql
 
 -- update only needs doing once
 
 UPDATE $db_source.mhs001mpi
 SET ICRECICB = CONCAT(ICRECCCG, "_icb")
 WHERE ICRECICB is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET ICRECICBName = CONCAT(ICRECCCGNAME, "_icb")
 WHERE ICRECICBName is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET ICRECSUBICB = CONCAT(ICRECCCG, "_subicb")
 WHERE ICRECSUBICB is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET ICRECSUBICBNAME = CONCAT(ICRECCCGNAME, "_subicb")
 WHERE ICRECSUBICBNAME is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET OrgIDICBRes = CONCAT(OrgIDCCGRes, "_icb")
 WHERE OrgIDICBRes is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET RegionICRECSUBICBName = CONCAT(OrgIDCCGRes, "_subicb")
 WHERE RegionICRECSUBICBName is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET RegionICRECSUBICB = CONCAT(RegionICRECCCG, "_subicb")
 WHERE RegionICRECSUBICB is NULL;
 
 UPDATE $db_source.mhs001mpi
 SET RegionICRECSUBICBName = CONCAT(RegionICRECCCG, "_name")
 WHERE RegionICRECSUBICBName is NULL;
 
 
 UPDATE $db_source.mhs002gp
 SET OrgIDICBGPPractice = CONCAT(OrgIDCCGGPPractice, "_icb")
 WHERE OrgIDICBGPPractice is NULL;
 
 UPDATE $db_source.mhs002gp
 SET OrgIDSubICBLocGP = CONCAT(OrgIDCCGGPPractice, "_subicb")
 WHERE OrgIDSubICBLocGP is NULL;

# COMMAND ----------

# update only needs doing once
# for table, column in tableColumn.items():
#   action = """Update {db_source}.{table} SET {column} = '{mhsds_database}' where {column} is null""".format(db_source=db_source,table=table,column=column,mhsds_database=mhsds_database)
#   print(action)
#   spark.sql(action)