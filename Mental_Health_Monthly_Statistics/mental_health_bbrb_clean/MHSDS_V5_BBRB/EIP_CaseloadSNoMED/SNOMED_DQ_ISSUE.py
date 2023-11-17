# Databricks notebook source
import pyspark.sql.functions as py
proc = spark.table(f"{db_output}.EIP_Procedures_v2")
split_colon = py.split(proc['CodeProcAndProcStatus'], ':')
split_equals = py.split(proc['CodeProcAndProcStatus'], '=')
proc = proc.withColumn("Code1", split_colon.getItem(0))
proc = proc.withColumn("CodeX", split_colon.getItem(1))
proc = proc.withColumn("Code2", py.split(proc['CodeX'], '=').getItem(0))
proc = proc.withColumn("Code3", split_equals.getItem(1))
proc.createOrReplaceTempView("EIP_PRE_PROCEDURE")

# COMMAND ----------

from pyspark.sql.functions import trim
proc2 = spark.table(f"{db_output}.EIP_Procedures_v2")
proc2 = proc2.withColumn("Der_CodeProcAndProcStatus", trim(proc.CodeProcAndProcStatus))
proc2.createOrReplaceTempView("EIP_PRE_PROCEDURE_v3")

# COMMAND ----------

 %sql
 select * from EIP_PRE_PROCEDURE_v3 where CodeProcAndProcStatus <> Der_CodeProcAndProcStatus

# COMMAND ----------

test1 = spark.sql("select * from EIP_PRE_PROCEDURE_v3 where UniqServReqID = 'RYG19522377' and person_id = '6370440302' and Der_InterventionUniqID = '592598000001643'")
display(test1.select('CodeProcAndProcStatus'))

# COMMAND ----------

test2 = spark.sql("select * from global_temp.Post_Coord_SNOMED_Terms where CodeProcAndProcStatus = '1082621000000104:408730004=443390004'")
display(test2.select('CodeProcAndProcStatus'))

# COMMAND ----------

if test1.select('CodeProcAndProcStatus') == test2.select('CodeProcAndProcStatus'):
  print('These strings are the same')
else:
  print('These strings are not the same')

# COMMAND ----------

###correct code
text = str(test2.select('CodeProcAndProcStatus').collect())
i=0
while i < len(text):
  print(text[i])
  i += 1

# COMMAND ----------

###incorrect code
text = str(test1.select('CodeProcAndProcStatus').collect())
i=0
while i < len(text):
  print(text[i])
  i += 1

# COMMAND ----------

from pyspark.sql.functions import trim
from pyspark.sql.functions import regexp_replace
proc2 = spark.table(f"{db_output}.EIP_Procedures_v3")
proc2 = proc2.withColumn("Der_CodeProcAndProcStatus", regexp_replace("CodeProcAndProcStatus", "\n|\r", ""))
proc2.createOrReplaceTempView("EIP_PRE_PROCEDURE_v4")

# COMMAND ----------

 %sql
 select UniqMonthID, UniqServReqID, Der_InterventionUniqID, OrgIDProv, CodeProcAndProcStatus, Der_CodeProcAndProcStatus, Der_SNoMEDTerm
 from EIP_PRE_PROCEDURE_v4 where CodeProcAndProcStatus <> Der_CodeProcAndProcStatus

# COMMAND ----------

 %sql
 select distinct OrgIDProv from EIP_PRE_PROCEDURE_v4 where CodeProcAndProcStatus <> Der_CodeProcAndProcStatus

# COMMAND ----------

 %sql
 select distinct PrimSystemInUse from $db_source.mhs000header where OrgIDProvider = 'RYG'

# COMMAND ----------

 %sql
 select distinct OrgIDProvider from $db_source.mhs000header where PrimSystemInUse = 'Carenotes'