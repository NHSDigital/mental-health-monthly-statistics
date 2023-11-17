-- Databricks notebook source
CREATE WIDGET TEXT MONTH_ID DEFAULT "1449";
CREATE WIDGET TEXT MSDS_15 DEFAULT "$mat15_database";
CREATE WIDGET TEXT MSDS_2 DEFAULT "mat_pre_clear";
CREATE WIDGET TEXT MHSDS DEFAULT "$mhsds_database";
CREATE WIDGET TEXT RP_STARTDATE DEFAULT "2020-01-01";
CREATE WIDGET TEXT RP_ENDDATE DEFAULT "2020-12-31";
CREATE WIDGET TEXT personal_db DEFAULT "glenda_fozzard_100069";

--TRUNCATE TABLE $personal_db.Perinatal

-- COMMAND ----------

%md 

National Breakdowns

-- COMMAND ----------

%sql

------- Table 3a breakdown by england level -----
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
,'PMH30a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF

-- COMMAND ----------

%sql

------- Table 3a breakdown by england level -----

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
,'PMH18a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF

-- COMMAND ----------

%sql

------- Table 3a breakdown england -----
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
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

-- COMMAND ----------

%sql
--DO WE NEED?
------- Table 3a breakdown by england -----
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
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12M = 'Y'

-- COMMAND ----------

%sql

------- Table 3a breakdown by england -----

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
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'

-- COMMAND ----------

%sql

------- Table 3a breakdown by england -----

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
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12_24M_NEW = 'Y'

-- COMMAND ----------

%md

Age at booking

-- COMMAND ----------

%sql

------- Table 3b breakdown by Age grouping -----

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
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH18a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF AS PERI

WHERE
preg_rnk = 1

group by 
CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 3b breakdown by Age grouping -----

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
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH30a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF AS PERI

WHERE
preg_rnk = 1

group by 
CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 3b breakdown by Age grouping -----

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
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF  AS PERI

WHERE
preg_rnk = 1

group by 
CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 3b breakdown by Age grouping -----

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
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF  AS PERI

WHERE
MH_REFERRAL12M = 'Y'
AND preg_rnk = 1

group by 
  CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%sql

------- Table 3b breakdown by Age grouping -----

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
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF AS PERI

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'
AND preg_rnk = 1

group by 
CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
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
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL
,CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END
AS LEVEL_DESCRIPTION
,'None' as LEVEL_2
,'None' as LEVEL_2_Description
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF AS PERI

WHERE
MH_REFERRAL12_24M_NEW = 'Y'
AND preg_rnk = 1

group by 
CASE WHEN (PERI.AgeAtBookingMother <= 0 OR PERI.AgeAtBookingMother IS NULL)
THEN 'Unknown'
--WHEN PERI.AgeAtBookingMother < 16
--THEN 'Under 16'
WHEN PERI.AgeAtBookingMother < 20
THEN '16-19'
WHEN PERI.AgeAtBookingMother < 25
THEN '20-24'
WHEN PERI.AgeAtBookingMother < 30
THEN '25-29'
WHEN PERI.AgeAtBookingMother < 35
THEN '30-34'
WHEN PERI.AgeAtBookingMother < 40
THEN '35-39'
WHEN PERI.AgeAtBookingMother < 45
THEN '40-44'
ELSE '45 and over'
END

-- COMMAND ----------

%md

Ethnicity

-- COMMAND ----------

%sql

------- Table 3c breakdown by Ethnic Group -----

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
,'PMH30a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF

WHERE
preg_rnk = 1

group by 

EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 3c breakdown by Ethnic Group -----

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
,'PMH18a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF

WHERE
preg_rnk = 1

group by 

EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 3c breakdown by Ethnic Group -----

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
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
preg_rnk = 1

group by 
EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 3c breakdown by Ethnic Group -----

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
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12M = 'Y'
AND preg_rnk = 1

group by 
EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 3c breakdown by Ethnic Group -----

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
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'
AND preg_rnk = 1

group by 
EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 3c breakdown by Ethnic Group -----

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
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12_24M_NEW = 'Y'
AND preg_rnk = 1

group by 
EthnicCategoryMother
,EthnicCategoryMother_Description

-- COMMAND ----------

%md 

Provider

-- COMMAND ----------

%sql

------- Table 3d breakdown by Provider -----

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
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 3d breakdown by Provider -----

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
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12M = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 3d breakdown by Provider -----

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
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 3d breakdown by Provider -----

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
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE
MH_REFERRAL12_24M_NEW = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%md

CCG

-- COMMAND ----------

%sql

------- Table 3d breakdown by CCG -----

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
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE
          
WHERE a.preg_rnk = 1

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 3d breakdown by CCG -----

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
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE
          
where a.preg_rnk = 1 and MH_REFERRAL12M = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 3d breakdown by CCG -----

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
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE
          
where a.preg_rnk = 1 and MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 3d breakdown by CCG -----

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
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE
          
where a.preg_rnk = 1 and MH_REFERRAL12_24M_NEW = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%md

STP

-- COMMAND ----------

%sql

------- Table 3e breakdown by STP -----

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
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF  as PERI

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = PERI.IC_REC_CCG

where 
preg_rnk = 1


group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 3e breakdown by STP -----

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
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF  as PERI

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = PERI.IC_REC_CCG

where 
preg_rnk = 1
AND MH_REFERRAL12M = 'Y'

group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 3e breakdown by STP -----

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
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF  as PERI

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = PERI.IC_REC_CCG

where 
preg_rnk = 1
AND MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
CASE
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 3e breakdown by STP -----

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
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF  as PERI

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = PERI.IC_REC_CCG

where 
preg_rnk = 1
AND MH_REFERRAL12_24M_NEW = 'Y'

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

------- Table 3f breakdown by Indicies -----

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
,'PMH30a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH23b_DF

WHERE 
preg_rnk = 1

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 3f breakdown by Indicies -----

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
,'PMH18a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH01b_DF

WHERE 
preg_rnk = 1

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 3f breakdown by Indicies -----

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
,'PMH31a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE 
preg_rnk = 1

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 3f breakdown by Indicies -----

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
,'PMH19a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE 
preg_rnk = 1
and MH_REFERRAL12M = 'Y'

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 3f breakdown by Indicies -----

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
,'PMH32a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE 
preg_rnk = 1
and MH_REFERRAL12_24M_ONGOING = 'Y'

group by 
IMD_DECILE

-- COMMAND ----------

%sql

------- Table 3f breakdown by Indicies -----

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
,'PMH33a'
AS METRIC
,COALESCE (COUNT (DISTINCT UniqPregID), 0)
AS METRIC_VALUE

from $personal_db.PMH19a_DF

WHERE 
preg_rnk = 1
and MH_REFERRAL12_24M_NEW = 'Y'

group by 
IMD_DECILE