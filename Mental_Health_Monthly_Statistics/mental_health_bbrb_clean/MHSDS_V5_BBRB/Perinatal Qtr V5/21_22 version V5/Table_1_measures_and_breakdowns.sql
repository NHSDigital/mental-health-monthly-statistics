-- Databricks notebook source
CREATE WIDGET TEXT MONTH_ID DEFAULT "1444";
CREATE WIDGET TEXT MSDS_15 DEFAULT "$mat15_database";
CREATE WIDGET TEXT MSDS_2 DEFAULT "mat_pre_clear";
CREATE WIDGET TEXT MHSDS DEFAULT "$mhsds_database";
CREATE WIDGET TEXT RP_STARTDATE DEFAULT "2020-07-01";
CREATE WIDGET TEXT RP_ENDDATE DEFAULT "2021-06-30";
CREATE WIDGET TEXT personal_db DEFAULT "chinyere_agu_100064";

--TRUNCATE TABLE $personal_db.Perinatal

-- COMMAND ----------

%md 

National Breakdowns

-- COMMAND ----------

%sql

------- Table 1a breakdown by england level -----
INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'England'
AS BREAKDOWN
,'England'
AS LEVEL
,'England'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH23b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF

-- COMMAND ----------

%sql

------- Table 1a breakdown by england level -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'England'
AS BREAKDOWN
,'England'
AS LEVEL
,'England'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH01b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF

-- COMMAND ----------

%sql

------- Table 1a breakdown england level-----
INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'England'
AS BREAKDOWN
,'England'
AS LEVEL
,'England'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

-- COMMAND ----------

%sql
--DO WE NEED? It's at England Level in the template
------- Table 1a breakdown by england -----
INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'England'
AS BREAKDOWN
,'England'
AS LEVEL
,'England'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

WHERE
MH_REFERRAL12M = 'Y'

-- COMMAND ----------

%sql

------- Table 1a breakdown by england -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'England'
AS BREAKDOWN
,'England'
AS LEVEL
,'England'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'

-- COMMAND ----------

%sql

------- Table 1a breakdown by england -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'England'
AS BREAKDOWN
,'England'
AS LEVEL
,'England'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

WHERE
MH_REFERRAL12_24M_NEW = 'Y'

-- COMMAND ----------

%md

Age at booking

-- COMMAND ----------

%sql

------- Table 1b breakdown by Age grouping -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Age at Booking'
AS BREAKDOWN
,CASE WHEN (AgeAtBookingMother <= 0 OR AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN AgeAtBookingMother < 20
THEN '16-19'
WHEN AgeAtBookingMother < 25
THEN '20-24'
WHEN AgeAtBookingMother < 30
THEN '25-29'
WHEN AgeAtBookingMother < 35
THEN '30-34'
WHEN AgeAtBookingMother < 40
THEN '35-39'
WHEN AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (AgeAtBookingMother <= 0 OR AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN AgeAtBookingMother < 20
THEN '16-19'
WHEN AgeAtBookingMother < 25
THEN '20-24'
WHEN AgeAtBookingMother < 30
THEN '25-29'
WHEN AgeAtBookingMother < 35
THEN '30-34'
WHEN AgeAtBookingMother < 40
THEN '35-39'
WHEN AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH01b'
AS METRIC
,COALESCE (COUNT (DISTINCT A.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF AS A

WHERE
rnk = 1 

group by 
CASE WHEN (AgeAtBookingMother <= 0 OR AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN AgeAtBookingMother < 20
THEN '16-19'
WHEN AgeAtBookingMother < 25
THEN '20-24'
WHEN AgeAtBookingMother < 30
THEN '25-29'
WHEN AgeAtBookingMother < 35
THEN '30-34'
WHEN AgeAtBookingMother < 40
THEN '35-39'
WHEN AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 1b breakdown by Age grouping -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Age at Booking'
AS BREAKDOWN
,CASE WHEN (AgeAtBookingMother <= 0 OR AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN AgeAtBookingMother < 20
THEN '16-19'
WHEN AgeAtBookingMother < 25
THEN '20-24'
WHEN AgeAtBookingMother < 30
THEN '25-29'
WHEN AgeAtBookingMother < 35
THEN '30-34'
WHEN AgeAtBookingMother < 40
THEN '35-39'
WHEN AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (AgeAtBookingMother <= 0 OR AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN AgeAtBookingMother < 20
THEN '16-19'
WHEN AgeAtBookingMother < 25
THEN '20-24'
WHEN AgeAtBookingMother < 30
THEN '25-29'
WHEN AgeAtBookingMother < 35
THEN '30-34'
WHEN AgeAtBookingMother < 40
THEN '35-39'
WHEN AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH23b'
AS METRIC
,COALESCE (COUNT (DISTINCT A.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF AS a

WHERE
rnk = 1 

group by 
CASE WHEN (AgeAtBookingMother <= 0 OR AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN AgeAtBookingMother < 20
THEN '16-19'
WHEN AgeAtBookingMother < 25
THEN '20-24'
WHEN AgeAtBookingMother < 30
THEN '25-29'
WHEN AgeAtBookingMother < 35
THEN '30-34'
WHEN AgeAtBookingMother < 40
THEN '35-39'
WHEN AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 1b breakdown by Age grouping -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Age at Booking'
AS BREAKDOWN
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT A.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

group by 
CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 1b breakdown by Age grouping -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Age at Booking'
AS BREAKDOWN
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT A.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
MH_REFERRAL12M = 'Y'

group by 
CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 1b breakdown by Age grouping -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Age at Booking'
AS BREAKDOWN
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT A.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 1b breakdown by Age grouping -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Age at Booking'
AS BREAKDOWN
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT A.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
MH_REFERRAL12_24M_NEW = 'Y'

group by 
CASE WHEN (B.AgeAtBookingMother <= 0 OR B.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN B.AgeAtBookingMother < 20
THEN '16-19'
WHEN B.AgeAtBookingMother < 25
THEN '20-24'
WHEN B.AgeAtBookingMother < 30
THEN '25-29'
WHEN B.AgeAtBookingMother < 35
THEN '30-34'
WHEN B.AgeAtBookingMother < 40
THEN '35-39'
WHEN B.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%md

Ethnicity

-- COMMAND ----------

%sql

------- Table 1c breakdown by Ethnic Group -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Ethnicity'
AS BREAKDOWN
,EthnicCategoryMother 
AS LEVEL
,EthnicCategoryMother_Description 
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH23b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF AS a

WHERE
rnk = 1 

group by 
EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 1c breakdown by Ethnic Group -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Ethnicity'
AS BREAKDOWN
,EthnicCategoryMother 
AS LEVEL
,EthnicCategoryMother_Description 
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH01b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF AS a

WHERE
rnk = 1 

group by 

EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 1c breakdown by Ethnic Group -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Ethnicity'
AS BREAKDOWN
,b.EthnicCategoryMother 
AS LEVEL
,b.EthnicCategoryMother_Description 
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 1c breakdown by Ethnic Group -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Ethnicity'
AS BREAKDOWN
,b.EthnicCategoryMother 
AS LEVEL
,b.EthnicCategoryMother_Description 
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE
MH_REFERRAL12M = 'Y'

group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 1c breakdown by Ethnic Group -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Ethnicity'
AS BREAKDOWN
,b.EthnicCategoryMother 
AS LEVEL
,b.EthnicCategoryMother_Description 
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 1c breakdown by Ethnic Group -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Ethnicity'
AS BREAKDOWN
,b.EthnicCategoryMother 
AS LEVEL
,b.EthnicCategoryMother_Description 
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE
MH_REFERRAL12_24M_NEW = 'Y'

group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%md 

Provider

-- COMMAND ----------

%sql

------- Table 1a breakdown by Provider -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Provider'
AS BREAKDOWN
,OrgIDProv
AS LEVEL
,'None'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 1a breakdown by Provider -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Provider'
AS BREAKDOWN
,OrgIDProv
AS LEVEL
,'None'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

WHERE
MH_REFERRAL12M = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 1a breakdown by Provider -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Provider'
AS BREAKDOWN
,OrgIDProv
AS LEVEL
,'None'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 1a breakdown by Provider -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'Provider'
AS BREAKDOWN
,OrgIDProv
AS LEVEL
,'None'
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF

WHERE
MH_REFERRAL12_24M_NEW = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%md

CCG

-- COMMAND ----------

%sql

------- Table 1e breakdown by CCG -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'CCG'
AS BREAKDOWN
,COALESCE(c.ORG_CODE, "UNKNOWN") 
AS LEVEL
,COALESCE(c.NAME, "UNKNOWN") AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother
group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 1e breakdown by CCG -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'CCG'
AS BREAKDOWN
,COALESCE(c.ORG_CODE, "UNKNOWN") 
AS LEVEL
,COALESCE(c.NAME, "UNKNOWN") AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE 
         

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother
          
WHERE a.MH_REFERRAL12M = 'Y'
group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 1e breakdown by CCG -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'CCG'
AS BREAKDOWN
,COALESCE(c.ORG_CODE, "UNKNOWN") 
AS LEVEL
,COALESCE(c.NAME, "UNKNOWN") AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE 
          
LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE a.MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 1e breakdown by CCG -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'CCG'
AS BREAKDOWN
,COALESCE(c.ORG_CODE, "UNKNOWN") 
AS LEVEL
,COALESCE(c.NAME, "UNKNOWN") AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c
          ON a.IC_Rec_CCG = c.ORG_CODE --added AT: consistent CCG output

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother 
          
WHERE a.MH_REFERRAL12_24M_NEW = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%md

STP

-- COMMAND ----------

%sql

------- Table 1f breakdown by STP -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'STP'
AS BREAKDOWN
,CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
  AS LEVEL
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END
  AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 1f breakdown by STP -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'STP'
AS BREAKDOWN
,CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
  AS LEVEL
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END
  AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF  AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
MH_REFERRAL12M = 'Y'

group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 1f breakdown by STP -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'STP'
AS BREAKDOWN
,CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
  AS LEVEL
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END
  AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF  AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
CASE
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 1f breakdown by STP -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'STP'
AS BREAKDOWN
,CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
  AS LEVEL
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END
  AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF  AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
MH_REFERRAL12_24M_NEW = 'Y'

group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%md

IMD

-- COMMAND ----------

%sql

------- Table 1d breakdown by Indicies -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'IMD'
AS BREAKDOWN
,IMD_DECILE
AS LEVEL
,IMD_DECILE
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH23b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF AS a

WHERE
rnk = 1 

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 1d breakdown by Indicies -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'IMD'
AS BREAKDOWN
,IMD_DECILE
AS LEVEL
,IMD_DECILE
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH01b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF AS a

WHERE
rnk = 1 

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 1d breakdown by Indicies -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'IMD'
AS BREAKDOWN
,b.IMD_DECILE
AS LEVEL
,b.IMD_DECILE
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH24b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 1d breakdown by Indicies -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'IMD'
AS BREAKDOWN
,b.IMD_DECILE
AS LEVEL
,b.IMD_DECILE
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH02b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE 
MH_REFERRAL12M = 'Y'

group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 1d breakdown by Indicies -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'IMD'
AS BREAKDOWN
,b.IMD_DECILE
AS LEVEL
,b.IMD_DECILE
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH25b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE 
MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 1d breakdown by Indicies -----

INSERT INTO $personal_db.Perinatal

select 
'$RP_STARTDATE'
AS REPORTING_PERIOD_START
,'$RP_ENDDATE'
AS REPORTING_PERIOD_END
,'Final'
AS STATUS
,'IMD'
AS BREAKDOWN
,b.IMD_DECILE
AS LEVEL
,b.IMD_DECILE
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH26b'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH24b_DF AS a


LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE 
MH_REFERRAL12_24M_NEW = 'Y'

group by 
b.IMD_DECILE