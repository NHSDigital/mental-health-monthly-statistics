# Databricks notebook source
print("restrictive interventions create tables")

db_output = dbutils.widgets.get("db_output")
assert db_output

$mhsds=dbutils.widgets.get("$mhsds")
print($mhsds)
assert $mhsds

# COMMAND ----------

# DBTITLE 1,RestrictiveIntTypeDim (candidate for REF_DATA)
 %sql
 -- Note that Restrictive Interventions accesses this table via view RestrictiveIntTypeDim_Extended that adds UNKNOWN. This approach to be reviewed.
 CREATE TABLE IF NOT EXISTS $db_output.RestrictiveIntTypeDim(
   key STRING,
   id STRING,
   description STRING
 ) USING DELTA

# COMMAND ----------

# DBTITLE 1,GenderDim (candidate for REF_DATA)
 %sql
 -- Note that Restrictive Interventions accesses this table via view GenderDim_Extended that adds UNKNOWN and removes 'Not stated'. This approach to be reviewed.
 CREATE TABLE IF NOT EXISTS $db_output.GenderDim(
   code STRING,
   description STRING
 ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,NHSDEthnicityDim (candidate for REF_DATA)
 %sql
 -- Note that Restrictive Interventions accesses this table via view NHSDEthnicityDim_Extended that adds UNKNOWN and Unspecified. This approach to be reviewed.
 CREATE TABLE IF NOT EXISTS $db_output.NHSDEthnicityDim(
   key STRING,
   id STRING,
   description STRING
 ) USING DELTA

# COMMAND ----------

# DBTITLE 1,ProviderTypeDim
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.ProviderTypeDim(
   key SMALLINT,
   description STRING)
 USING DELTA

# COMMAND ----------

# DBTITLE 1,AgeBandsDim
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.AgeBandsDim(
   key SMALLINT,
   description STRING)
 USING DELTA

# COMMAND ----------

# DBTITLE 1,RestrictiveInterventionCategories
 %sql
 -- This table provides the reference data for the extract tables to link to. Since all options are present even those where there are 0 records will be shown.
 CREATE TABLE IF NOT EXISTS $db_output.RestrictiveInterventionCategories
 (
 RestrictiveIntTypeKey string,
 RestrictiveIntTypeDescription string,
 EthnicityKey string,
 EthnicityID string,
 EthnicityDescription string,
 GenderCode string,
 GenderDescription string,
 AgeBand string,
 ProviderType string
 )
 USING DELTA;

# COMMAND ----------

# DBTITLE 1,RestrictiveInterventionCategoriesProvider
 %sql
 -- This table provides the reference data for the extract tables to link to. Since all options are present even those where there are 0 records will be shown.
 CREATE TABLE IF NOT EXISTS $db_output.RestrictiveInterventionCategoriesProvider
 (
 RestrictiveIntTypeKey string,
 RestrictiveIntTypeDescription string,
 EthnicityKey string,
 EthnicityID string,
 EthnicityDescription string,
 EthnicCategory string,
 GenderCode string,
 GenderDescription string,
 AgeBand string,
 ProviderType string,
 OrgIDProvider string,
 OrgName string
 )
 USING DELTA;

# COMMAND ----------

# DBTITLE 1,MHSRestrictiveInterventionRaw
 %sql
 -- Contains Rows from MHS505Restrictiveintervention with all derivations used for aggregation
 CREATE TABLE IF NOT EXISTS $db_output.MHSRestrictiveInterventionRaw
     (
      MHS505UniqID                         bigint,
      OrgIDProv                            string,
      Person_ID                            string,
      RestrictiveIntType                   string,
      UniqMonthID                          bigint,
      NHSDEthnicity                        string,
      AgeRepPeriodEnd                      bigint,
      Gender                               string,
      DerivedOrgIDProvName                 string,
      DerivedRestrictiveIntTypeCode        string,
      DerivedRestrictiveIntTypeDescription string,
      DerivedEthnicityCode                 string,
      DerivedEthnicityDescription          string,
      DerivedEthnicityGroupCode            string,
      DerivedEthnicityGroupDescription     string,
      DerivedOrgType                       string,
      DerivedAgeBand                       string,
      DerivedGenderCode                    string,
      DerivedGenderDescription             string
      )
      
 USING delta 
 	

# COMMAND ----------

# DBTITLE 1,MHSRestrictiveInterventionCount
 %sql
 -- Actual counts for MHS77 - Restrictive Interventions Count
 CREATE TABLE IF NOT EXISTS $db_output.MHSRestrictiveInterventionCount
     (REPORTING_PERIOD_START           DATE,
      REPORTING_PERIOD_END             DATE,
      STATUS                           STRING,
      BREAKDOWN                        STRING,
      PRIMARY_LEVEL                    STRING,
      PRIMARY_LEVEL_DESCRIPTION        STRING,
      SECONDARY_LEVEL                  STRING,
      SECONDARY_LEVEL_DESCRIPTION      STRING,
      TERTIARY_LEVEL                   STRING,
      TERTIARY_LEVEL_DESCRIPTION       STRING,
      QUARTERNARY_LEVEL                STRING,
      QUARTERNARY_LEVEL_DESCRIPTION    STRING,
      MEASURE_ID                       STRING,
      MEASURE_NAME                     STRING,
      MEASURE_VALUE                    BIGINT,
      UniqMonthID                      BIGINT,
      CreatedAt                        TIMESTAMP,
      SOURCE_DB                        STRING)
 USING delta 
 								


# COMMAND ----------

# DBTITLE 1,MHSRestrictiveInterventionPeople
 %sql
 -- Actual counts for MHS76 - Restrictive Interventions People
 CREATE TABLE IF NOT EXISTS $db_output.MHSRestrictiveInterventionPeople
     (REPORTING_PERIOD_START           DATE,
      REPORTING_PERIOD_END             DATE,
      STATUS                           STRING,
      BREAKDOWN                        STRING,
      PRIMARY_LEVEL                    STRING,
      PRIMARY_LEVEL_DESCRIPTION        STRING,
      SECONDARY_LEVEL                  STRING,
      SECONDARY_LEVEL_DESCRIPTION      STRING,
      TERTIARY_LEVEL                   STRING,
      TERTIARY_LEVEL_DESCRIPTION       STRING,
      QUARTERNARY_LEVEL                STRING,
      QUARTERNARY_LEVEL_DESCRIPTION    STRING,
      MEASURE_ID                       STRING,
      MEASURE_NAME                     STRING,
      MEASURE_VALUE                    BIGINT,
      UniqMonthID                      BIGINT,
      CreatedAt                        TIMESTAMP,
      SOURCE_DB                        STRING)
 USING delta 
 								


# COMMAND ----------

# DBTITLE 1,MHSRestrictiveInterventionCountSuppressed
 %sql
 -- Actual counts for MHS77 - Restrictive Interventions Count
 CREATE TABLE IF NOT EXISTS $db_output.MHSRestrictiveInterventionCountSuppressed
     (REPORTING_PERIOD_START           DATE,
      REPORTING_PERIOD_END             DATE,
      STATUS                           STRING,
      BREAKDOWN                        STRING,
      PRIMARY_LEVEL                    STRING,
      PRIMARY_LEVEL_DESCRIPTION        STRING,
      SECONDARY_LEVEL                  STRING,
      SECONDARY_LEVEL_DESCRIPTION      STRING,
      TERTIARY_LEVEL                   STRING,
      TERTIARY_LEVEL_DESCRIPTION       STRING,
      QUARTERNARY_LEVEL                STRING,
      QUARTERNARY_LEVEL_DESCRIPTION    STRING,
      MEASURE_ID                       STRING,
      MEASURE_NAME                     STRING,
      MEASURE_VALUE                    STRING,
      UniqMonthID                      BIGINT,
      CreatedAt                        TIMESTAMP,
      SOURCE_DB                        STRING)
 USING delta 
 								


# COMMAND ----------

# DBTITLE 1,MHSRestrictiveInterventionPeopleSuppressed
 %sql
 -- Actual counts for MHS76 - Restrictive Interventions People
 CREATE TABLE IF NOT EXISTS $db_output.MHSRestrictiveInterventionPeopleSuppressed
     (REPORTING_PERIOD_START           DATE,
      REPORTING_PERIOD_END             DATE,
      STATUS                           STRING,
      BREAKDOWN                        STRING,
      PRIMARY_LEVEL                    STRING,
      PRIMARY_LEVEL_DESCRIPTION        STRING,
      SECONDARY_LEVEL                  STRING,
      SECONDARY_LEVEL_DESCRIPTION      STRING,
      TERTIARY_LEVEL                   STRING,
      TERTIARY_LEVEL_DESCRIPTION       STRING,
      QUARTERNARY_LEVEL                STRING,
      QUARTERNARY_LEVEL_DESCRIPTION    STRING,
      MEASURE_ID                       STRING,
      MEASURE_NAME                     STRING,
      MEASURE_VALUE                    STRING,
      UniqMonthID                      BIGINT,
      CreatedAt                        TIMESTAMP,
      SOURCE_DB                        STRING)
 USING delta 
 								


# COMMAND ----------

# DBTITLE 1,IsColumnInTable
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
tableColumn = {'mhsrestrictiveinterventioncount': 'SOURCE_DB', 'mhsrestrictiveinterventionpeople': 'SOURCE_DB', 'mhsrestrictiveinterventioncountsuppressed': 'SOURCE_DB', 'mhsrestrictiveinterventionpeoplesuppressed': 'SOURCE_DB'}

# COMMAND ----------

# DBTITLE 1,Add column to table if it doesn't exist
for table, column in tableColumn.items():
  print(table)
  print(column)
  exists = IsColumnInTable(db_output, table, column)
  if exists == 0:
    action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} STRING)""".format(db_output=db_output,table=table,column=column)
    print(action)
    spark.sql(action)

# COMMAND ----------

# DBTITLE 1,Set SOURCE_DB to source database
# update only needs doing once
for table, column in tableColumn.items():
  action = """Update {db_output}.{table} SET {column} = '{$mhsds}' where {column} is null""".format(db_output=db_output,table=table,column=column,$mhsds=$mhsds)
  print(action)
  spark.sql(action)