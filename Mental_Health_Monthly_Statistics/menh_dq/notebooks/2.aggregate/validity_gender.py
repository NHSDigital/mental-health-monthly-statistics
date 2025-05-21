# Databricks notebook source
# dbutils.widgets.removeAll()


# dbutils.widgets.text("db_output" , "menh_dq", "db_output")
# dbutils.widgets.text("dbm" , "testdata_menh_dq_$mhsds", "dbm")

# dbutils.widgets.text("month_id", "1449", "month_id")
# dbutils.widgets.text("$reference_data", "$reference_data", "$reference_data")

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

$reference_data  = dbutils.widgets.get("$reference_data")
print($reference_data)
assert $reference_data

month_id  = dbutils.widgets.get("month_id")
print(month_id)
assert month_id

# COMMAND ----------

# DBTITLE 1,Gender Identity Code
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Gender Identity Code' AS MeasureName,
   3 as DimensionTypeId,
   81 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN upper(GenderIDCode) in ('1','2','3','Z') THEN 1 
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN GenderIDCode = ('4') THEN 1
         ELSE 0
       END
       ) AS Other,
 	SUM(CASE
         WHEN upper(GenderIDCode) = ('X') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
         WHEN upper(GenderIDCode) NOT IN ('1','2','3','4','Z','X') 
         AND GenderIDCode IS NOT null
         AND GenderIDCode <> ''
         THEN 1
         ELSE 0
       END 
       ) AS Invalid,
   SUM(CASE
         WHEN GenderIDCode IS NULL OR GenderIDCode = ''
         THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS001MPI mpi
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

# DBTITLE 1,Gender Identity Same At Birth Indicator Code
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Gender Identity Code' AS MeasureName,
   3 as DimensionTypeId,
   82 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN upper(GenderSameAtBirth) in ('Y','N','Z') THEN 1 
         ELSE 0
       END
       ) AS Valid,
   0 AS Other,
 	SUM(CASE
         WHEN upper(GenderSameAtBirth) = ('X') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
          WHEN upper(GenderSameAtBirth) NOT IN ('Y','N','Z','X')
         AND GenderSameAtBirth IS NOT null
         AND GenderSameAtBirth <> ''
         THEN 1
         ELSE 0
       END 
       ) AS Invalid,
   SUM(CASE
         WHEN GenderSameAtBirth IS NULL OR GenderSameAtBirth = ''
         THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.MHS001MPI mpi
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

# COMMAND ----------

