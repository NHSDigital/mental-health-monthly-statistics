# Databricks notebook source
# DBTITLE 1,Restrictive Intervention People Extract
dbutils.widgets.text("db", "menh_publications", "Target database")
db_output = dbutils.widgets.get("db")
assert db_output
print (db_output)

dbutils.widgets.text("rp_startdate", "2019-08-01", "Reporting period start date")
rp_startdate = dbutils.widgets.get("rp_startdate")
assert rp_startdate
print (rp_startdate)

dbutils.widgets.text("status", "Final", "Status")
status = dbutils.widgets.get("status")
assert status
print (status)

# COMMAND ----------

# DBTITLE 1,Suppressed
 %py
 df = spark.sql("SELECT REPORTING_PERIOD_START,REPORTING_PERIOD_END,Status,BREAKDOWN,PRIMARY_LEVEL,PRIMARY_LEVEL_DESCRIPTION,SECONDARY_LEVEL,SECONDARY_LEVEL_DESCRIPTION,TERTIARY_LEVEL,TERTIARY_LEVEL_DESCRIPTION,QUARTERNARY_LEVEL,QUARTERNARY_LEVEL_DESCRIPTION,METRIC,METRIC_VALUE FROM `{db}`.MHSRestrictiveInterventionPeopleSuppressed WHERE REPORTING_PERIOD_START = '{rp_startdate}' AND STATUS = '{status}' AND SOURCE_DB = {mhsds_database}'".format(db=db_output,rp_startdate=rp_startdate,status=status,mhsds_database=mhsds_database)).collect();
 display(df);

# COMMAND ----------

# DBTITLE 1,Unsuppressed
 %py
 df = spark.sql("SELECT REPORTING_PERIOD_START,REPORTING_PERIOD_END,Status,BREAKDOWN,PRIMARY_LEVEL,PRIMARY_LEVEL_DESCRIPTION,SECONDARY_LEVEL,SECONDARY_LEVEL_DESCRIPTION,TERTIARY_LEVEL,TERTIARY_LEVEL_DESCRIPTION,QUARTERNARY_LEVEL,QUARTERNARY_LEVEL_DESCRIPTION,METRIC,METRIC_VALUE FROM `{db}`.MHSRestrictiveInterventionPeople WHERE REPORTING_PERIOD_START = '{rp_startdate}' AND STATUS = '{status}' AND SOURCE_DB = '{mhsds_database}'".format(db=db_output,rp_startdate=rp_startdate,status=status,mhsds_database=mhsds_database)).collect();
 display(df);