# Databricks notebook source
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
database=dbutils.widgets.get("database")
print(database)
assert database

# COMMAND ----------

# DBTITLE 1,GenderDim_Extended
%sql
-- Create a view of Ref_data table as required for restrictive interventions or create a restrictive intervention specific dimension table?
CREATE OR REPLACE VIEW $db_output.GenderDim_Extended AS
SELECT code, description from $db_output.GenderDim
WHERE DESCRIPTION IN ('Male', 'Female')
UNION
(SELECT 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

# DBTITLE 1,RestrictiveIntTypeDim_Extended
%sql
-- Create a view of Ref_data table as required for restrictive interventions or create a restrictive intervention specific dimension table?
CREATE OR REPLACE VIEW $db_output.RestrictiveIntTypeDim_Extended AS
SELECT key, id, description from $db_output.RestrictiveIntTypeDim
UNION (SELECT 'UNKNOWN', 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

# DBTITLE 1,NHSDEthnicityDim_Extended
%sql
-- Create a view of Ref_data table as required for restrictive interventions or create a restrictive intervention specific dimension table?
CREATE OR REPLACE VIEW $db_output.NHSDEthnicityDim_Extended AS
SELECT key, id, description from $db_output.NHSDEthnicityDim
union (select 'UNKNOWN', 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

# DBTITLE 1,EthnicCategory
%sql
CREATE OR REPLACE VIEW $db_output.EthnicCategory AS
SELECT NATIONAL_CODE, NATIONAL_CODE_DESCRIPTION
FROM $database.dd_ethnic_category_code
WHERE BUSINESS_END_DATE is null