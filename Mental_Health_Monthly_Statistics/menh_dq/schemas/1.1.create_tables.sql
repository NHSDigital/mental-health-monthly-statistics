-- Databricks notebook source
-- DROP TABLE IF EXISTS $db.dq_dimension_type;

CREATE TABLE IF NOT EXISTS $db.dq_dimension_type
(
  DimensionTypeId tinyint,
  DimensionTypePrefix string,
  DimensionTypeName string,
  DimensionTypeDescription string
);

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_measure_type;

CREATE TABLE IF NOT EXISTS $db.dq_measure_type
(
  MeasureTypeId tinyint,
  MeasureTypeName string
);

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_metric_type;

CREATE TABLE IF NOT EXISTS $db.dq_metric_type
(
  MetricTypeId tinyint,
  DimensionTypeId tinyint,
  MetricTypeName string,
  MetricTypeDescription string
);

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_measure;

CREATE TABLE IF NOT EXISTS $db.dq_measure
(
  MeasureId int,
  DimensionTypeId tinyint,
  MeasureName string,
  MeasureDescription string,
  DataItem string
);

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_measure_description;

CREATE TABLE IF NOT EXISTS $db.dq_measure_description
(
  MeasureId int,
  DimensionTypeId tinyint,
  MeasureTypeId tinyint,
  MetricTypeId tinyint,
  MeasureTypeDescription string,
  StartDate date,
  EndDate date
);

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_stg_integrity;

CREATE TABLE IF NOT EXISTS $db.dq_stg_integrity
(
  DimensionTypeId tinyint,
  MeasureId tinyint,  
  OrgIDProv string,
  Denominator int,
  Integrity int
);

-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_stg_validity;

CREATE TABLE IF NOT EXISTS $db.dq_stg_validity
(
  DimensionTypeID int,
  MeasureID int,
  OrgIDProv string,
  Denominator int,
  Valid int,
  Other int,
  Default int,
  Invalid int,
  Missing int
)USING DELTA ;

-- COMMAND ----------

---------------------------------------------------------------------------------------------------------
--  WARNING: Never ever drop or truncate this table in a LIVE environment. 
--  All our accrued MHSDS DQ calculations (month-by-month) are in here.
---------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS $db.dq_inventory
(
  UniqMonthID int,
  DimensionTypeId tinyint,
  MeasureId tinyint,
  MeasureTypeId tinyint,
  MetricTypeId tinyint,
  OrgIDProv string,
  Value int,
  SOURCE_DB string
) USING DELTA
PARTITIONED BY (UniqMonthID);


-- COMMAND ----------

-- DROP TABLE IF EXISTS $db.dq_smh_service_category_code;

CREATE TABLE IF NOT EXISTS $db.dq_smh_service_category_code
(
  ServiceCode string,
  ServiceCategoryCode string,
  FirstMonth int,
  LastMonth int
)
USING DELTA

-- COMMAND ----------

-- UKD 23/02/2022: BITC-3063 (menh_dq: Create audit table)
CREATE TABLE IF NOT EXISTS $db.audit_menh_dq
(
    MONTH_ID int,   
    STATUS string,   
    REPORTING_PERIOD_START date,  
    REPORTING_PERIOD_END date,   
    SOURCE_DB string,   
    RUN_START timestamp,   
    RUN_END timestamp
)
USING DELTA
PARTITIONED BY (MONTH_ID, STATUS)