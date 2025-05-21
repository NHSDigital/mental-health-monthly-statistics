# Databricks notebook source
# dbutils.widgets.removeAll()

# dbutils.widgets.text("db_output", "menh_publications", "db_output")
# dbutils.widgets.text("$mhsds", "$mhsds", "Input database")

db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

$mhsds = dbutils.widgets.get("$mhsds")
print($mhsds)
assert $mhsds

# COMMAND ----------

 %md

 # tables created in this notebook

 - validcodes
 - referral_dim

# COMMAND ----------

# DBTITLE 1,validcodes
 %sql
 -- populated with valid codes in /notebooks/common_objects/00_version_change_tables

 DROP TABLE IF EXISTS $db_output.validcodes;

 CREATE TABLE IF NOT EXISTS $db_output.validcodes
 (
   Tablename string,
   Field string,
   Measure string,
   Type string,
   ValidValue string,
   FirstMonth int,
   LastMonth int
 ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,referral_dim
 %sql

 -- DROP TABLE IF EXISTS $db_output.referral_dim;

 CREATE TABLE IF NOT EXISTS $db_output.referral_dim
 (
   Referral_Source string,
   Referral_Description string,
   FirstMonth int,
   LastMonth int
 ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,ConsMechanismMH_dim
 %sql

 CREATE TABLE IF NOT EXISTS $db_output.ConsMechanismMH_dim
 (
   Code string,
   Description string,
   FirstMonth int,
   LastMonth int
 ) USING DELTA;