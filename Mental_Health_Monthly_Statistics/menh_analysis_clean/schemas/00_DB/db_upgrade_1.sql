-- Databricks notebook source
%md

# Permanent Tables
These tables will not be regularly created and destroyed, but will persist.

If changes are made to the structure of the tables in this script they should be made by an alter table statement in a later script, or the code here should include a "DROP TABLE IF EXISTS <table_name>" statement.  In later scripts the drop table statements have been added so structural table changes can be made in the create table statement as a matter of routine.

If a decision is made to not truncate the tables in the running code and persist previously run statistics then the drop tables should be removed and alter table be included instead.

If a structure is altered without a drop table it will be that later code will not work if the old version of the table aready exists in the database selected.

The following tables are created in this notebook:
- All_products_formatted
- Ascof_formatted
- Breakdown & metrics tables for
 - Main monthly 
 - Delayed Discharge (MHS26)
 - AWT
 - CYP 2nd contact
 - CaP
 - CYP
   - plus dimension tables
   - ConsMechanismMH_dim (previously consmediumused pre v5)
   - DNA_Reason
   - referral_dim
 - ASCOF
- stp_region_mapping_post_2018
- stp_region_mapping_post_2020

-- COMMAND ----------

%md ## Formatted and rounded tables

-- COMMAND ----------

-- DBTITLE 0,All_products_formatted
-- DROP TABLE IF EXISTS $db_output.all_products_formatted;

CREATE TABLE IF NOT EXISTS $db_output.All_products_formatted (
  PRODUCT_NO STRING,
  REPORTING_PERIOD_START DATE,
  REPORTING_PERIOD_END DATE,
  STATUS STRING,
  BREAKDOWN STRING,
  PRIMARY_LEVEL STRING,
  PRIMARY_LEVEL_DESCRIPTION STRING,
  SECONDARY_LEVEL STRING,
  SECONDARY_LEVEL_DESCRIPTION STRING,
  MEASURE_ID STRING,
  MEASURE_NAME STRING,
  MEASURE_VALUE STRING,
  DATETIME_INSERTED TIMESTAMP,
  SOURCE_DB STRING
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db_output.ascof_formatted;
CREATE TABLE IF NOT EXISTS $db_output.Ascof_formatted (
    PRODUCT_NO STRING,
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
    BREAKDOWN STRING,
    PRIMARY_LEVEL STRING,
    PRIMARY_LEVEL_DESCRIPTION STRING,
    SECONDARY_LEVEL STRING,
    SECONDARY_LEVEL_DESCRIPTION STRING,
    THIRD_LEVEL STRING,
    THIRD_LEVEL_DESCRIPTION STRING,
    MEASURE_ID STRING,
    MEASURE_NAME STRING,
    MEASURE_VALUE STRING,
    DATETIME_INSERTED TIMESTAMP,
    SOURCE_DB STRING
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

-- COMMAND ----------

%md

## Breakdowns & metrics tables

-- COMMAND ----------

-- DBTITLE 1,1. Main monthly
-- DROP TABLE IF EXISTS $db_output.main_monthly_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.main_monthly_level_values_1;
-- WORKAROUND: Append "_1", because table name "Main_monthly_level_values" comes back with an unresolvable error
CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_level_values_1 (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA; --See above.

-- DROP TABLE IF EXISTS $db_output.main_monthly_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.Main_monthly_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,1.1 MHS26 in Main monthly (Delayed Discharge)
-- DROP TABLE IF EXISTS $db_output.dd_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.DD_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.dd_level_values;
CREATE TABLE IF NOT EXISTS $db_output.DD_level_values (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.dd_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.DD_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,2. Access and Waiting Times
-- DROP TABLE IF EXISTS $db_output.awt_breakdown_values;
--remove
CREATE TABLE IF NOT EXISTS $db_output.AWT_breakdown_values (breakdown string) USING DELTA;

DROP TABLE IF EXISTS $db_output.awt_level_values;
--CREATE TABLE IF NOT EXISTS $db_output.AWT_level_values (level string, level_desc string, breakdown string) USING DELTA;
CREATE TABLE IF NOT EXISTS $db_output.awt_level_values (level string, level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA;


-- DROP TABLE IF EXISTS $db_output.awt_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.AWT_metric_values (metric string, metric_name string) USING DELTA;


-- COMMAND ----------

-- DBTITLE 1,3. CYP 2nd contact
-- DROP TABLE IF EXISTS $db_output.cyp_2nd_contact_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.CYP_2nd_contact_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cyp_2nd_contact_level_values;
CREATE TABLE IF NOT EXISTS $db_output.CYP_2nd_contact_level_values (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cyp_2nd_contact_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.CYP_2nd_contact_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,4. CaP
-- DROP TABLE IF EXISTS $db_output.cap_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.CaP_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cap_level_values;
CREATE TABLE IF NOT EXISTS $db_output.CaP_level_values (level string, level_desc string, breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cap_cluster_values;
CREATE TABLE IF NOT EXISTS $db_output.CaP_cluster_values (Cluster int) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cap_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.CaP_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,5. CYP monthly
-- DROP TABLE IF EXISTS $db_output.cyp_monthly_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.CYP_monthly_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cyp_monthly_level_values;
CREATE TABLE IF NOT EXISTS $db_output.CYP_monthly_level_values (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.cyp_monthly_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.CYP_monthly_metric_values (metric string, metric_name string) USING DELTA;

-- this is a change to accommodate a fieldname change in MHSDS v5
DROP TABLE IF EXISTS $db_output.consmediumused;
CREATE TABLE IF NOT EXISTS $db_output.ConsMechanismMH_dim(Code string, Description string, FirstMonth int, LastMonth int) USING DELTA;

CREATE TABLE IF NOT EXISTS $db_output.ConsMechanismMH (level string, level_description string) USING DELTA;


-- DROP TABLE IF EXISTS $db_output.dna_reason;
CREATE TABLE IF NOT EXISTS $db_output.DNA_Reason (level string, level_description string) USING DELTA;

DROP TABLE IF EXISTS $db_output.referral_dim;
CREATE TABLE IF NOT EXISTS $db_output.referral_dim (Referral_Source string, Referral_Description string, FirstMonth int, LastMonth int) USING DELTA;

 
DROP TABLE IF EXISTS $db_output.referral_source;
CREATE TABLE IF NOT EXISTS $db_output.Referral_Source (level string, level_description string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,7. Ascof
-- DROP TABLE IF EXISTS $db_output.ascof_breakdown_values;
CREATE TABLE IF NOT EXISTS $db_output.Ascof_breakdown_values (breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.ascof_level_values;
CREATE TABLE IF NOT EXISTS $db_output.Ascof_level_values (level string, level_desc string, breakdown string) USING DELTA;

-- DROP TABLE IF EXISTS $db_output.ascof_metric_values;
CREATE TABLE IF NOT EXISTS $db_output.Ascof_metric_values (metric string, metric_name string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,STP/Region breakdowns April 2018 - updated for April 2019
DROP TABLE IF EXISTS $db_output.stp_region_mapping_post_2018;
CREATE TABLE IF NOT EXISTS $db_output.STP_Region_mapping_post_2018 (CCG_code string, STP_code string, STP_description string, Status string, Region_code string, Region_description string) USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,STP/Region breakdowns April 2020 
--do not comment out this drop table  
DROP TABLE IF EXISTS $db_output.stp_region_mapping_post_2020;
CREATE TABLE IF NOT EXISTS $db_output.STP_Region_mapping_post_2020 (STP_code string, STP_description string, CCG_code string, CCG_description string, Region_code string, Region_description string) USING DELTA;