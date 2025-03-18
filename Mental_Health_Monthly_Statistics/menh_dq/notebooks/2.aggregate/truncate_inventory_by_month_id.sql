-- Databricks notebook source
 %py
 dbm  = dbutils.widgets.get("dbm")
 print(dbm)
 assert dbm
 month_id  = dbutils.widgets.get("month_id")
 print(month_id)
 assert month_id

-- COMMAND ----------

-- This clears out the dq_inventory for any given month id. Use this before populating the dq_inventory 
-- with a month (just in case there already is data for that month in that table).

DELETE FROM $db_output.dq_inventory WHERE UniqMonthID = '$month_id' and SOURCE_DB = '$dbm'