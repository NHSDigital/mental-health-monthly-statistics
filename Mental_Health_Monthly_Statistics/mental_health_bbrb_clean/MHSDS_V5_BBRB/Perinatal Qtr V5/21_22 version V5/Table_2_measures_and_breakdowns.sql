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

------- Table 2a breakdown by england level -----
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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF

-- COMMAND ----------

%sql

------- Table 2a breakdown england level -----
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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF
WHERE
CONTACT24M = 'Y'

-- COMMAND ----------

%sql
--DO WE NEED?
------- Table 2a breakdown england level -----
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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

WHERE
contact12m = 'Y'

-- COMMAND ----------

%sql

------- Table 2a breakdown england level -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

WHERE
contact12m = 'Y' and CONTACT12_24M = 'Y'

-- COMMAND ----------

%sql

------- Table 1a breakdown england level -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

WHERE
contact12m is null and contact12_24m = 'Y'

-- COMMAND ----------

-- DBTITLE 1, 'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID'
INSERT INTO $personal_db.Perinatal

SELECT '$RP_STARTDATE'
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
,'PMH07a'
AS METRIC
,COALESCE (COUNT (DISTINCT PERI.Person_ID_Mother), 0)
AS METRIC_VALUE

FROM $personal_db.MHSDSPerinatalPeriodMH_DF_NEW
AS PERI
LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID
INNER JOIN $MHSDS.MHS501HospProvSpell
AS HSP
ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate12m AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

INNER JOIN $MHSDS.MHS502WardStay
AS WST
ON HSP.UniqHospProvSpellID = WST.UniqHospProvSpellID  --  'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' v5 20/10/21 /*** Amended 2 instances to v5 GF ***/
AND (WST.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
AND WST.StartDateWardStay <= '$RP_ENDDATE'
AND (WST.EndDateWardStay IS NULL OR WST.EndDateWardStay >= '$RP_STARTDATE') 
AND WST.StartDateWardStay <= PERI.EndDate12m 
AND (WST.EndDateWardStay  IS NULL OR WST.EndDateWardStay  >= PERI.StartDate) 
AND (WST.RecordEndDate IS NULL OR WST.RecordEndDate >= '$RP_ENDDATE') 
AND WST.RecordStartDate <= '$RP_ENDDATE'
AND WST.SiteIDOfTreat in
('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
'RV505','RRE3K','RW119')

WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate12M >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate12m
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16

-- COMMAND ----------

%md

Age at booking

-- COMMAND ----------

%sql

------- Table 2b breakdown by Age grouping -----

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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF  AS a

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

------- Table 2b breakdown by Age grouping -----

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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF  AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
CONTACT24M = 'Y'

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

------- Table 2b breakdown by Age grouping -----

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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE
contact12m = 'Y'

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

------- Table 2b breakdown by Age grouping -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


WHERE
contact12m = 'Y' and CONTACT12_24M = 'Y'


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

------- Table 2b breakdown by Age grouping -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF    AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
contact12m is null and contact12_24m = 'Y'

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

------- Table 2c breakdown by Ethnic Group -----

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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF   AS a

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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
CONTACT24M = 'Y'

group by 

b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 2c breakdown by Ethnic Group -----

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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
contact12m = 'Y'


group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 2c breakdown by Ethnic Group -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
contact12m = 'Y' and CONTACT12_24M = 'Y'

group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%sql

------- Table 2c breakdown by Ethnic Group -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE
contact12m is null and contact12_24m = 'Y'


group by 
b.EthnicCategoryMother
,b.EthnicCategoryMother_Description

-- COMMAND ----------

%md 

Provider

-- COMMAND ----------

%sql

------- Table 2a breakdown by prov -----

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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF 

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 2a breakdown by prov -----

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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

where
CONTACT24M = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 2a breakdown by prov -----

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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

WHERE
contact12m = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 2a breakdown by prov -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

WHERE
contact12m = 'Y' and CONTACT12_24M = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%sql

------- Table 2a breakdown by provider -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF

WHERE
contact12m is null and contact12_24m = 'Y'

group by 
OrgIDProv

-- COMMAND ----------

%md

CCG

-- COMMAND ----------

%sql

------- Table 2e breakdown by CCG -----

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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 2e breakdown by CCG -----

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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother
          
WHERE a.CONTACT24M = 'Y'
group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 2e breakdown by CCG -----

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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE 

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother
          
where contact12m = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 2e breakdown by CCG -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE AND a.contact12m = 'Y'

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE a.CONTACT12_24M = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%sql

------- Table 2e breakdown by CCG -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF AS a

LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
          ON a.IC_Rec_CCG = c.ORG_CODE 

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother
          
where a.contact12m is null and a.contact12_24m = 'Y'

group by 
COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

%md

STP

-- COMMAND ----------

%sql

------- Table 2f breakdown by STP -----

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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF    AS a

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

------- Table 2f breakdown by STP -----

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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF    AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
CONTACT24M = 'Y'

group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 2f breakdown by STP -----

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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
contact12m = 'Y'

group by 
CASE 
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 2f breakdown by STP -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
contact12m = 'Y' and CONTACT12_24M = 'Y'

group by 
CASE
  WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_CODE END
,CASE 
  WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
  ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

%sql

------- Table 2f breakdown by STP -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF   AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

LEFT JOIN global_temp.STP_MAPPING
AS STP
ON STP.CCG_CODE = b.IC_REC_CCG

where 
contact12m is null and contact12_24m = 'Y'

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

------- Table 2d breakdown by Indicies -----

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
,'PMH06a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH06a_DF  AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother


group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 2d breakdown by Indicies -----

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
,'PMH27a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF  AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE 
CONTACT24M = 'Y'

group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 1f breakdown by Indicies -----

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
,'PMH08a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF  AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE 
contact12m = 'Y'

group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 2d breakdown by Indicies -----

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
,'PMH28a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF  AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE 
contact12m = 'Y' and CONTACT12_24M = 'Y'

group by 
b.IMD_DECILE

-- COMMAND ----------

%sql

------- Table 2d breakdown by Indicies -----

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
,'PMH29a'
AS METRIC
,COALESCE (COUNT (DISTINCT a.Person_ID_Mother), 0)
AS METRIC_VALUE

from $personal_db.PMH08a_DF  AS a

LEFT JOIN $personal_db.M_DEM B 
          ON A.Person_ID_Mother = B.Person_ID_Mother

WHERE 
contact12m is null and contact12_24m = 'Y'

group by 
b.IMD_DECILE