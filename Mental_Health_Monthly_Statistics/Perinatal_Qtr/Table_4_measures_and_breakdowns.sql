-- Databricks notebook source
CREATE WIDGET TEXT MONTH_ID DEFAULT "1449";
CREATE WIDGET TEXT MSDS_15 DEFAULT "$mat_1.5";
CREATE WIDGET TEXT MSDS_2 DEFAULT "$maternity";
CREATE WIDGET TEXT MHSDS DEFAULT "$mhsds";
CREATE WIDGET TEXT RP_STARTDATE DEFAULT "2020-01-01";
CREATE WIDGET TEXT RP_ENDDATE DEFAULT "2020-12-31";
CREATE WIDGET TEXT personal_db DEFAULT "$personal_db";

--TRUNCATE TABLE $personal_db.Perinatal

-- COMMAND ----------

 %md 

 National Breakdowns

-- COMMAND ----------

-- DBTITLE 1,'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID'
INSERT INTO $personal_db.Perinatal

SELECT 
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
,'PMH21a'
AS METRIC
,COALESCE (COUNT (DISTINCT PERI.UniqPregID), 0)
AS METRIC_VALUE

FROM $personal_db.MHSDSPerinatalPeriodMH_DF_NEW
AS PERI

LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

INNER JOIN $MHSDS.MHS501HospProvSpell
AS HSP
ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate12M AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

INNER JOIN $MHSDS.MHS502WardStay
AS WST
ON HSP.UniqHospProvSpellID = WST.UniqHospProvSpellID  --'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' v5 20/10/21 /*** Amended 2 instances to v5 GF ***/
AND (WST.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
AND WST.StartDateWardStay <= '$RP_ENDDATE' 
AND (WST.EndDateWardStay IS NULL OR WST.EndDateWardStay >= '$RP_STARTDATE') 
AND WST.StartDateWardStay <= PERI.EndDate12M 
AND (WST.EndDateWardStay  IS NULL OR WST.EndDateWardStay  >= PERI.StartDate) 
AND (WST.RecordEndDate IS NULL OR WST.RecordEndDate >= '$RP_ENDDATE') 
AND WST.RecordStartDate <= '$RP_ENDDATE' 

LEFT JOIN $MHSDS.MHS903WardDetails 
AS WSD
ON WST.UniqWardCode = WSD.UniqWardCode
AND WSD.SiteIDOfWard in
('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
'RV505','RRE3K','RW119')

WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate12M >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate12M
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16
AND
(SiteIDOfTreat in
('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
'RV505','RRE3K','RW119') OR WSD.UniqWardCode is not null)

-- COMMAND ----------

 %sql

 ------- Table 4a breakdown by england level -----
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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF

-- COMMAND ----------

 %sql

 ------- Table 4a breakdown england level -----
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
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF
 where 
 Contact24m = 'Y'

-- COMMAND ----------

 %sql

 ------- Table 4a breakdown england level -----
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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M = 'Y'

-- COMMAND ----------

 %sql

 ------- Table 4a breakdown england level -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M = 'Y' AND CONTACT12_24M = 'Y'

-- COMMAND ----------

 %sql

 ------- Table 4a breakdown england level -----

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
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M IS NULL AND CONTACT12_24M = 'Y'

-- COMMAND ----------

 %md

 Age at booking

-- COMMAND ----------

 %sql

 ------- Table 4b breakdown by Age grouping -----

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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF AS PERI

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

 ------- Table 4b breakdown by Age grouping -----

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
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS PERI

 WHERE
 preg_rnk = 1
 and Contact24m = 'Y'

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

 ------- Table 4b breakdown by Age grouping -----

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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS PERI

 WHERE
 CONTACT12M = 'Y'
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

 ------- Table 4b breakdown by Age grouping -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS PERI

 WHERE
 CONTACT12M = 'Y' AND CONTACT12_24M = 'Y'
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

 ------- Table 4a breakdown by Age grouping -----

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
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS PERI

 WHERE
 CONTACT12M IS NULL AND CONTACT12_24M = 'Y'
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

 ------- Table 4c breakdown by Ethnic Group -----

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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF

 WHERE
 preg_rnk = 1

 group by 

 EthnicCategoryMother
 ,EthnicCategoryMother_Description

-- COMMAND ----------

 %sql

 ------- Table 4c breakdown by Ethnic Group -----

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
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 preg_rnk = 1
 and Contact24m = 'Y'

 group by 

 EthnicCategoryMother
 ,EthnicCategoryMother_Description

-- COMMAND ----------

 %sql

 ------- Table 4c breakdown by Ethnic Group -----

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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M = 'Y'
 AND preg_rnk = 1

 group by 
 EthnicCategoryMother
 ,EthnicCategoryMother_Description

-- COMMAND ----------

 %sql

 ------- Table 4c breakdown by Ethnic Group -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M = 'Y' AND CONTACT12_24M = 'Y'
 AND preg_rnk = 1

 group by 
 EthnicCategoryMother
 ,EthnicCategoryMother_Description

-- COMMAND ----------

 %sql

 ------- Table 4c breakdown by Ethnic Group -----

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
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M IS NULL AND CONTACT12_24M = 'Y'
 AND preg_rnk = 1

 group by 
 EthnicCategoryMother
 ,EthnicCategoryMother_Description

-- COMMAND ----------

 %md 

 Provider

-- COMMAND ----------

 %sql

 ------- Table 4d breakdown by prov -----

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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF

 group by 
 OrgIDProv

-- COMMAND ----------

 %sql

 ------- Table 4d breakdown by prov -----

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
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 where
 Contact24m = 'Y'

 group by 
 OrgIDProv

-- COMMAND ----------

 %sql

 ------- Table 4d breakdown by prov -----

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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M = 'Y'

 group by 
 OrgIDProv

-- COMMAND ----------

 %sql

 ------- Table 4d breakdown by prov -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M = 'Y' AND CONTACT12_24M = 'Y'

 group by 
 OrgIDProv

-- COMMAND ----------

 %sql

 ------- Table 4d breakdown by provider -----

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
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE
 CONTACT12M IS NULL AND CONTACT12_24M = 'Y'

 group by 
 OrgIDProv

-- COMMAND ----------

 %md

 CCG

-- COMMAND ----------

 %sql

 ------- Table 4e breakdown by CCG -----

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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF AS a

 LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
           ON a.IC_Rec_CCG = c.ORG_CODE 
           
 where a.preg_rnk = 1

 group by 
 COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

 %sql

 ------- Table 4e breakdown by CCG -----

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
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS a

 LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
           ON a.IC_Rec_CCG = c.ORG_CODE

 where a.preg_rnk = 1 and a.Contact24m = 'Y'

 group by 
 COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

 %sql

 ------- Table 4e breakdown by CCG -----

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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS a

 LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
           ON a.IC_Rec_CCG = c.ORG_CODE

 where a.preg_rnk = 1 and CONTACT12M = 'Y'

 group by 
 COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

 %sql

 ------- Table 4e breakdown by CCG -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT a.UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS a

 LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
           ON a.IC_Rec_CCG = c.ORG_CODE

 where a.preg_rnk = 1 and a.CONTACT12M = 'Y' AND a.CONTACT12_24M = 'Y'

 group by 
 COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

 %sql

 ------- Table 4e breakdown by CCG -----

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
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF AS a

 LEFT JOIN global_temp.RD_CCG_LATEST c--added AT: consistent CCG output
           ON a.IC_Rec_CCG = c.ORG_CODE

 where a.preg_rnk = 1 and a.CONTACT12M IS NULL AND a.CONTACT12_24M = 'Y'

 group by 
 COALESCE(c.ORG_CODE, "UNKNOWN") , COALESCE(c.NAME, "UNKNOWN")

-- COMMAND ----------

 %md

 STP

-- COMMAND ----------

 %sql

 ------- Table 4f breakdown by STP -----

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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF  as PERI

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

 ------- Table 4f breakdown by STP -----

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
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF  as PERI

 LEFT JOIN global_temp.STP_MAPPING
 AS STP
 ON STP.CCG_CODE = PERI.IC_REC_CCG

 where 
 preg_rnk = 1
 and Contact24m = 'Y'

 group by 
 CASE 
   WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
   ELSE STP.STP_CODE END
 ,CASE 
   WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
   ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

 %sql

 ------- Table 4f breakdown by STP -----

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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF  as PERI

 LEFT JOIN global_temp.STP_MAPPING
 AS STP
 ON STP.CCG_CODE = PERI.IC_REC_CCG

 where 
 preg_rnk = 1
 AND CONTACT12M = 'Y'

 group by 
 CASE 
   WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
   ELSE STP.STP_CODE END
 ,CASE 
   WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
   ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

 %sql

 ------- Table 4f breakdown by STP -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF  as PERI

 LEFT JOIN global_temp.STP_MAPPING
 AS STP
 ON STP.CCG_CODE = PERI.IC_REC_CCG

 where 
 preg_rnk = 1
 AND CONTACT12M = 'Y' AND CONTACT12_24M = 'Y'

 group by 
 CASE
   WHEN STP.STP_CODE IS NULL THEN 'UNKNOWN'
   ELSE STP.STP_CODE END
 ,CASE 
   WHEN STP.STP_DESCRIPTION IS NULL THEN 'UNKNOWN'
   ELSE STP.STP_DESCRIPTION END

-- COMMAND ----------

 %sql

 ------- Table 4f breakdown by STP -----

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
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF  as PERI

 LEFT JOIN global_temp.STP_MAPPING
 AS STP
 ON STP.CCG_CODE = PERI.IC_REC_CCG

 where 
 preg_rnk = 1
 AND CONTACT12M IS NULL AND CONTACT12_24M = 'Y'

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

 ------- Table 4d breakdown by Indicies -----

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
 ,'PMH20a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH20a_DF

 WHERE 
 preg_rnk = 1

 group by 
 IMD_DECILE

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
 ,IMD_DECILE
 AS LEVEL
 ,IMD_DECILE
 AS LEVEL_DESCRIPTION
 ,'None' as LEVEL_2
 ,'None' as LEVEL_2_Description
 ,'PMH34a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE 
 preg_rnk = 1
 and Contact24m = 'Y'

 group by 
 IMD_DECILE

-- COMMAND ----------

 %sql

 ------- Table 4f breakdown by Indicies -----

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
 ,'PMH22a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE 
 preg_rnk = 1
 and CONTACT12M = 'Y'

 group by 
 IMD_DECILE

-- COMMAND ----------

 %sql

 ------- Table 4f breakdown by Indicies -----

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
 ,'PMH35a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE 
 preg_rnk = 1
 and CONTACT12M = 'Y' AND CONTACT12_24M = 'Y'

 group by 
 IMD_DECILE

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
 ,IMD_DECILE
 AS LEVEL
 ,IMD_DECILE
 AS LEVEL_DESCRIPTION
 ,'None' as LEVEL_2
 ,'None' as LEVEL_2_Description
 ,'PMH36a'
 AS METRIC
 ,COALESCE (COUNT (DISTINCT UniqPregID), 0)
 AS METRIC_VALUE

 from $personal_db.PMH22a_DF

 WHERE 
 preg_rnk = 1
 and CONTACT12M IS NULL AND CONTACT12_24M = 'Y'

 group by 
 IMD_DECILE