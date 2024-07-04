# Databricks notebook source
dbm  = dbutils.widgets.get("dbm")
print(dbm)
assert dbm
status  = dbutils.widgets.get("status")
print(status)
assert status
month_id  = dbutils.widgets.get("month_id")
print(month_id)
assert month_id
MonthPeriod  = dbutils.widgets.get("MonthPeriod")
print(MonthPeriod)
assert MonthPeriod

# COMMAND ----------

# DBTITLE 1,Coverage
 %sql
 --First table checks which providers have submitted in a given month based against a list of those that have submitted in the previous months.
 DELETE FROM $db_output.DQMI_Coverage WHERE Month_ID = '$month_id' AND Status = '$status' and SOURCE_DB = '$dbm';
 
 INSERT INTO $db_output.DQMI_Coverage
 SELECT 	'$month_id' AS Month_ID,
 		'$MonthPeriod' AS Period, 
 		'MHSDS' AS Dataset, 
         '$status' as Status,
 		CASE WHEN a.OrgIDProvider IS NULL THEN b.OrgIDProvider ELSE a.OrgIDProvider END AS Organisation, 
         n.NAME AS OrganisationName, 
 		CASE WHEN a.OrgIDProvider IS NULL THEN 0 ELSE 1 END AS ExpectedToSubmit, 
 		CASE WHEN b.OrgIDProvider IS NULL THEN 0 ELSE 1 END AS Submitted,
         '$dbm' as SOURCE_DB
 FROM
 (
 SELECT DISTINCT OrgIdProvider 
 FROM $dbm.MHS000Header
 WHERE uniqMonthID BETWEEN $month_id - 7 AND $month_id - 1
 ) a
 FULL OUTER JOIN (SELECT DISTINCT OrgIDProvider FROM $dbm.MHS000Header WHERE uniqMonthID = $month_id) b ON a.OrgIDProvider = b.OrgIDProvider 
 LEFT JOIN
 (
 SELECT Org_Code, 
        NAME,
        ORG_CLOSE_DATE, 
        BUSINESS_END_DATE, 
        ROW_NUMBER() OVER (PARTITION BY Org_Code ORDER BY IFNULL(Business_End_Date,CURRENT_DATE()) DESC, IFNULL(Org_Close_Date, CURRENT_DATE())DESC) AS RN
 FROM $reference_data.org_daily) n 
 ON CASE WHEN a.OrgIDProvider IS NULL THEN b.OrgIDProvider ELSE a.OrgIDProvider END = n.ORG_CODE 
    AND n.RN = 1 
    AND (n.BUSINESS_END_DATE IS NULL OR n.BUSINESS_END_DATE >= '$rp_enddate') 
    AND (n.ORG_CLOSE_DATE IS NULL OR n.ORG_CLOSE_DATE >='$rp_enddate')
 --ORDER BY uniqMonthID DESC, Organisation

# COMMAND ----------

# DBTITLE 1,Monthly Data
 %sql
 --Monthly Data
 --Second table gets values for numerators and denominators based on definitions specified by the DQMI.
 --Complete numerator is the number of records where the field of interest wasn't left blank. 
 --Complete denominator is simply the number of records submitted. 
 --Valid numerator is all records where the field of interest is a valid code (including other or default codes),
 --Default numerator is the number of records where the field of interest had a default code submitted.
 --Finally, valid Denominator is the number of records where the field of interest wasn't left blank.
 DELETE FROM $db_output.DQMI_Monthly_Data WHERE Month_ID = '$month_id' AND Status = '$status' and SOURCE_DB = '$dbm';
 
 with Denominator as
 (select * from $db_output.dq_inventory where MeasureTypeId = 1 and uniqMonthId = '$month_id' and DimensiontypeID = 3 and SOURCE_DB = '$dbm'),
 Numeratory as
 (select * from $db_output.dq_inventory where MeasureTypeId = 2 and uniqMonthId = '$month_id' and DimensiontypeID = 3 and SOURCE_DB = '$dbm')
 
 INSERT INTO $db_output.DQMI_Monthly_Data 
 SELECT 
     '$month_id' AS Month_ID,
     '$MonthPeriod' AS Period, 
     'MHSDS' AS Dataset, 
     '$status' as Status,
     v.OrgIdProv AS DataProviderId, 
     n.NAME AS DataProviderName, 
     mes.MeasureName AS DataItem,
     SUM(CASE WHEN m.value IS NOT NULL THEN den.value - m.value ELSE den.value END) AS CompleteNumerator,
     SUM(den.value) AS CompleteDenominator,
     SUM(CASE WHEN o.value IS NOT NULL AND d.value IS NULL THEN  o.value + v.value
         WHEN o.value IS NULL AND d.value IS NOT NULL THEN d.value + v.value
         WHEN o.value IS NOT NULL AND d.value IS NOT NULL THEN o.value + d.value + v.value
         ELSE v.value END) AS ValidNumerator,
     SUM(CASE WHEN d.value IS NOT NULL THEN d.value ELSE 0 END) AS DefaultNumerator,
     SUM(CASE WHEN m.value IS NOT NULL THEN den.value - m.value ELSE den.value END) AS ValidDenominator,
     '$dbm' as SOURCE_DB
 FROM Numeratory v
 inner join $db_output.dq_measure mes on v.measureId = mes.measureId and mes.DimensionTypeId = 3
 LEFT Join Denominator den on v.MeasureID = den.MeasureID and v.OrgIDProv = den.OrgIDProv and v.UniqMonthID = den.uniqMonthId
 LEFT Join Numeratory o on v.MeasureId = o.MeasureId AND v.OrgIdProv = o.OrgIdProv AND o.MetricTypeId = 2 --AND o.UniqMonthId = v.UniqMonthId
 LEFT JOIN Numeratory d ON v.MeasureId = d.MeasureId AND v.OrgIdProv = d.OrgIdProv AND d.MetricTypeId = 3 --AND d.UniqMonthId = v.UniqMonthId
 LEFT JOIN Numeratory i ON v.MeasureId = i.MeasureId AND v.OrgIdProv = i.OrgIdProv AND i.MetricTypeId = 4 --AND i.UniqMonthId = v.UniqMonthId
 LEFT JOIN Numeratory m ON v.MeasureId = m.MeasureId AND v.OrgIdProv = m.OrgIdProv AND m.MetricTypeId = 5 --AND m.UniqMonthId = v.UniqMonthId
 
 LEFT JOIN
 (SELECT Org_Code, NAME,ORG_CLOSE_DATE, BUSINESS_END_DATE, ROW_NUMBER() OVER (PARTITION BY Org_Code ORDER BY IFNULL(Business_End_Date,CURRENT_DATE()) DESC, IFNULL(Org_Close_Date, CURRENT_DATE()) DESC) AS RN
 FROM $reference_data.org_daily) n ON v.OrgIDProv = n.ORG_CODE AND n.RN = 1 AND (n.BUSINESS_END_DATE IS NULL OR n.BUSINESS_END_DATE >='$rp_enddate') AND (n.ORG_CLOSE_DATE IS NULL OR n.ORG_CLOSE_DATE >='$rp_enddate')
 WHERE v.UniqMonthID = '$month_id'  AND v.MetricTypeId = 1 AND v.DimensionTypeID = 3
 GROUP BY v.OrgIdProv,
       n.NAME, 
       mes.MeasureName
 ORDER BY v.OrgIdProv, mes.MeasureName

# COMMAND ----------

# DBTITLE 1,Extract the time Dimensions - Integrity measures
 %sql
 DELETE FROM $db_output.DQMI_Integrity WHERE Month_ID = '$month_id' AND Status = '$status' and SOURCE_DB = '$dbm';
 
 WITH Numerator AS
 (SELECT OrgIdProv,MeasureId,value FROM $db_output.dq_vw_Inventory_rollup WHERE MeasureTypeName = 'Numerator' AND UniqMonthID = '$month_id' AND DimensionTypeId = 7 and SOURCE_DB = '$dbm' ORDER BY OrgIdProv),
 Denominator AS
 (SELECT OrgIdProv,MeasureId,value FROM $db_output.dq_vw_Inventory_rollup WHERE MeasureTypeName = 'Denominator' AND UniqMonthID = '$month_id' AND DimensionTypeId = 7 and SOURCE_DB = '$dbm' ORDER BY OrgIdProv)
 
 INSERT INTO $db_output.DQMI_Integrity
 SELECT 
   '$month_id' AS Month_ID,
   '$MonthPeriod' AS Period, 
   'MHSDS' AS Dataset, 
   '$status' AS Status,
   n.OrgIdProv,
   n.MeasureId,
   m.MeasureName,
   n.value AS Numerator,
   d.value AS Denominator,
   '$dbm' as SOURCE_DB
 FROM Numerator n
 INNER JOIN Denominator d
 ON n.orgIdProv = d.OrgIdProv and n.MeasureId = d.MeasureID
 INNER JOIN $db_output.dq_measure m
 ON n.MeasureId = m.MeasureId

# COMMAND ----------

