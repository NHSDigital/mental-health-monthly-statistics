-- Databricks notebook source
%py
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
$db_source=dbutils.widgets.get("$db_source")
print($db_source)
assert $db_source

-- COMMAND ----------

%sql
DROP TABLE IF EXISTS $db_output.cyp_ed_wt_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.cyp_ed_wt_breakdown_values (breakdown string) USING DELTA;

DROP TABLE IF EXISTS $db_output.cyp_ed_wt_level_values_1;
CREATE TABLE IF NOT EXISTS $db_output.cyp_ed_wt_level_values_1 (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA; --See above.

DROP TABLE IF EXISTS $db_output.cyp_ed_wt_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.cyp_ed_wt_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,create table cyp_ed_wt_step4
%sql

-- table can be dropped safely as it is truncated each run and holds no persisted data
DROP TABLE IF EXISTS $db_output.cyp_ed_wt_step4;

CREATE TABLE IF NOT EXISTS $db_output.cyp_ed_wt_step4
(
  UniqMonthID int,
  Status string,
  UniqServReqID string,
  OrgIDProv string,
  Person_ID string,
  ClinRespPriorityType string,
  CareContDate date,
  ReferralRequestReceivedDate date,
  waiting_time float,
  SOURCE_DB string,
  SubmissionMonthID int
)


USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,create table cyp_ed_wt_STEP6
%sql

-- table can be dropped safely as it is truncated each run and holds no persisted data
DROP TABLE IF EXISTS $db_output.cyp_ed_wt_step6;

CREATE TABLE IF NOT EXISTS $db_output.cyp_ed_wt_step6
(
  UniqMonthID int,
  Status string,
  UniqServReqID string,
  OrgIDProv string,
  Person_ID string,
  ClinRespPriorityType string,
  IC_Rec_CCG string,
  ReferralRequestReceivedDate date,
  ServDischDate date,
  waiting_time float,
  SOURCE_DB string,
  SubmissionMonthID int
)


USING DELTA;

-- COMMAND ----------

 
CREATE TABLE IF NOT EXISTS $db_output.cyp_ed_wt_unformatted
(
  MONTH_ID INT,
  STATUS STRING,
  REPORTING_PERIOD_START DATE,
  REPORTING_PERIOD_END DATE,
  BREAKDOWN STRING,
  PRIMARY_LEVEL STRING,
  PRIMARY_LEVEL_DESCRIPTION STRING,
  SECONDARY_LEVEL STRING,
  SECONDARY_LEVEL_DESCRIPTION STRING,
  METRIC STRING,
  METRIC_VALUE FLOAT,
  SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS);

-- COMMAND ----------

-- DBTITLE 1,IsColumnInTable
%py
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

-- COMMAND ----------

%py
# List of tables and column that needs adding to them
tableColumn = {'cyp_ed_wt_step4': 'SOURCE_DB', 'cyp_ed_wt_step6': 'SOURCE_DB', 'cyp_ed_wt_unformatted': 'SOURCE_DB'}

-- COMMAND ----------

-- DBTITLE 1,Add column to table if it doesn't exist
%py
for table, column in tableColumn.items():
  print(table)
  print(column)
  exists = IsColumnInTable(db_output, table, column)
  if exists == 0:
    action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} STRING)""".format(db_output=db_output,table=table,column=column)
    print(action)
    spark.sql(action)

-- COMMAND ----------

-- DBTITLE 1,Set SOURCE_DB to source database
-- # %py
-- # # update only needs doing once - DONE

-- # for table, column in tableColumn.items():
-- #   action = """Update {db_output}.{table} SET {column} = '{$db_source}' where {column} is null""".format(db_output=db_output,table=table,column=column,$db_source=$db_source)
-- #   print(action)
-- #   spark.sql(action)