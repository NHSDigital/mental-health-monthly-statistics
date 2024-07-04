# Databricks notebook source
# DBTITLE 1,validcodes table for date dependent valid codes
 %sql
 
 -- introduced for MHSDS v5
 
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
 )
 
 USING DELTA
 PARTITIONED BY (Tablename)