-- Databricks notebook source
%py
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate

-- COMMAND ----------

-- DBTITLE 1,Truncate data from CSV
TRUNCATE TABLE $db_output.MHA_Monthly_CSV


-- COMMAND ----------

-- DBTITLE 1,Insert data (monthly) into CSV - National level

insert into $db_output.MHA_Monthly_CSV 
values
('England','England','England','NONE','NONE','MHS81','Detentions in RP'),
('England','England','England','NONE','NONE','MHS82','Short Term Orders in RP'),
('England','England','England','NONE','NONE','MHS83','Uses of Section 136 in RP'),
('England','England','England','NONE','NONE','MHS84','CTOs in RP')


-- COMMAND ----------

-- DBTITLE 1,Insert data (monthly) into CSV - Provider & CCG level at once

INSERT INTO $db_output.MHA_Monthly_CSV
SELECT 
    'Provider' AS BREAKDOWN,
    ORG_CODE AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    A.SECONDARY_LEVEL,
    A.SECONDARY_LEVEL_DESCRIPTION,
    A.MEASURE_ID,
    A.MEASURE_NAME

FROM $db_output.MHA_Monthly_CSV A
CROSS JOIN (SELECT DISTINCT A.ORG_CODE, A.NAME
            FROM global_temp.MHA_RD_ORG_DAILY_LATEST A
              INNER JOIN $db_source.MHS000HEADER B ON A.ORG_CODE = B.ORGIDPROVIDER AND B.REPORTINGPERIODSTARTDATE = '$rp_startdate'
            ) AS x
    ON A.MEASURE_ID IN ('MHS81','MHS82','MHS83','MHS84') and a.Breakdown = 'England'

WHERE A.MEASURE_ID IN ('MHS81','MHS82','MHS83','MHS84') and a.Breakdown = 'England'
UNION
SELECT 
    'CCG - GP Practice or Residence' AS BREAKDOWN,
    IC_REC_CCG AS PRIMARY_LEVEL,
    NAME AS PRIMARY_LEVEL_DESCRIPTION,
    A.SECONDARY_LEVEL,
    A.SECONDARY_LEVEL_DESCRIPTION,
    A.MEASURE_ID,
    A.MEASURE_NAME

FROM $db_output.MHA_Monthly_CSV A
  CROSS JOIN (SELECT DISTINCT IC_REC_CCG, NAME FROM $db_output.CCG ) AS y
      ON A.MEASURE_ID IN ('MHS81','MHS82','MHS83','MHS84') and a.Breakdown = 'England'

WHERE A.MEASURE_ID IN ('MHS81','MHS82','MHS83','MHS84') and a.Breakdown = 'England'


-- COMMAND ----------

select * from $db_output.MHA_Monthly_CSV