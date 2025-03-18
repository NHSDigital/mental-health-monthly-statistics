# Databricks notebook source
# dbutils.widgets.removeAll()


# dbutils.widgets.text("db_output" , "menh_dq", "db_output")
# dbutils.widgets.text("dbm" , "testdata_menh_dq_$mhsds_db", "dbm")

# dbutils.widgets.text("month_id", "1449", "month_id")
# dbutils.widgets.text("reference_data", "reference_data", "reference_data")

# dbutils.widgets.text("rp_startdate", "2020-12-01", "rp_startdate")
# dbutils.widgets.text("rp_enddate", '2020-12-31', "rp_enddate")


dbm  = dbutils.widgets.get("dbm")
print(dbm)
assert dbm

db_output  = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

rp_startdate  = dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate

rp_enddate  = dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate

reference_data  = dbutils.widgets.get("reference_data")
print(reference_data)
assert reference_data

month_id  = dbutils.widgets.get("month_id")
print(month_id)
assert month_id

# COMMAND ----------

# DBTITLE 1,Denominator
 %sql

 WITH InventoryList
 AS
 (
   SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Denominator' AS MeasureTypeName, Denominator AS Value FROM $db_output.dq_stg_validity
 )
 MERGE INTO $db_output.dq_inventory AS target
 USING
 (
   SELECT
     '$month_id' AS UniqMonthID,
     im.DimensionTypeId,
     im.MeasureId,
     im.MeasureTypeId,
     im.MetricTypeId,
     il.OrgIDProv,
     il.Value
   FROM InventoryList il
   INNER JOIN $db_output.dq_vw_inventory_metadata im   on il.DimensionTypeId = im.DimensionTypeId
   AND il.MeasureId = im.MeasureId
   AND il.MeasureTypeName = im.MeasureTypeName
   WHERE im.StartDate <= '$rp_startdate'
   AND CASE
         WHEN im.EndDate IS NULL THEN '$rp_enddate'
        ELSE im.EndDate
       END >= '$rp_enddate'
 ) AS source ON target.UniqMonthID = source.UniqMonthID
 AND target.DimensionTypeId = source.DimensionTypeId
 AND target.MeasureId = source.MeasureId
 AND target.MeasureTypeId = source.MeasureTypeId
 AND CASE
       WHEN target.MetricTypeId IS NULL THEN 'null'
       ELSE target.MetricTypeId
       END = CASE
               WHEN source.MetricTypeId IS NULL THEN 'null'
               ELSE source.MetricTypeId
             END
 AND target.OrgIDProv = source.OrgIDProv
 AND target.SOURCE_DB = '$dbm'
 WHEN MATCHED
   THEN UPDATE SET target.Value = source.Value
 WHEN NOT MATCHED
   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');

# COMMAND ----------

 %sql

 DROP TABLE IF EXISTS $db_output.dq_stg_inventorylist;
 CREATE TABLE IF NOT EXISTS $db_output.dq_stg_inventorylist
 AS 
 SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Valid' AS MetricTypeName, Valid AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Other' AS MetricTypeName, Other AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Default' AS MetricTypeName, Default AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Invalid' AS MetricTypeName, Invalid AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Missing' AS MetricTypeName, Missing AS Value FROM $db_output.dq_stg_validity;

 DROP TABLE IF EXISTS $db_output.dq_stg_inventorylistsource;
 CREATE TABLE $db_output.dq_stg_inventorylistsource
 AS
 SELECT
     '$month_id' AS UniqMonthID,
     im.DimensionTypeId,
     im.MeasureId,
     im.MeasureTypeId,
     im.MetricTypeId,
     il.OrgIDProv,
     il.Value
   FROM $db_output.dq_stg_inventoryList il
   INNER JOIN $db_output.dq_vw_inventory_metadata im ON il.DimensionTypeId = im.DimensionTypeId
   AND il.MeasureId = im.MeasureId
   AND il.MetricTypeName = im.MetricTypeName
   WHERE im.StartDate <= '$rp_startdate'
   AND CASE
         WHEN im.EndDate IS NULL THEN '$rp_enddate'
        ELSE im.EndDate
       END >= '$rp_enddate';

# COMMAND ----------

 %sql

 -- Valid, Other, Default, Invalid, Missing  re-coding to get around an issue in PROD

 MERGE INTO $db_output.dq_inventory AS target
 USING $db_output.dq_stg_inventoryListSource AS source ON target.UniqMonthID = source.UniqMonthID
   AND target.DimensionTypeId = source.DimensionTypeId
   AND target.MeasureId = source.MeasureId
   AND target.MeasureTypeId = source.MeasureTypeId
   AND CASE
         WHEN target.MetricTypeId IS NULL THEN 'null'
         ELSE target.MetricTypeId
         END = CASE
                 WHEN source.MetricTypeId IS NULL THEN 'null'
                 ELSE source.MetricTypeId
               END
   AND target.OrgIDProv = source.OrgIDProv
   AND target.SOURCE_DB = '$dbm'
 WHEN MATCHED
   THEN UPDATE SET target.Value = source.Value
 WHEN NOT MATCHED
   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');