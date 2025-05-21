-- Databricks notebook source
 %py
 # dbutils.widgets.removeAll()

-- COMMAND ----------

 %py

 # dbutils.widgets.text("dbtable", "MHS101Referral", "dbtable")
 # dbutils.widgets.text("db_output" , "charles_kelechi1_101304", "db_output")
 # dbutils.widgets.text("dbm" , "testdata_menh_dq_$mhsds", "dbm")

 # dbutils.widgets.text("month_id", "1462", "month_id")
 # dbutils.widgets.text("$reference_data", "$reference_data", "$reference_data")

 # dbutils.widgets.text("rp_startdate", "2022-01-01", "rp_startdate")
 # dbutils.widgets.text("rp_enddate", '2022-01-31', "rp_enddate")
 # dbutils.widgets.text("measureid", '81', "measureid")
 # dbutils.widgets.text("defaultcodes", '()', "defaultcodes")

 # db_output=dbutils.widgets.get("db_output")
 # print(db_output)
 # assert db_output

 # month_id=dbutils.widgets.get("month_id")
 # print(month_id)
 # assert month_id

 # dbm=dbutils.widgets.get("dbm")
 # print(dbm)
 # assert dbm

 # dbtable=dbutils.widgets.get("dbtable")
 # print(dbtable)
 # assert dbtable

 # $reference_data=dbutils.widgets.get("$reference_data")
 # print($reference_data)
 # assert $reference_data

 # rp_startdate=dbutils.widgets.get("rp_startdate")
 # print(rp_startdate)
 # assert rp_startdate

 # rp_enddate=dbutils.widgets.get("rp_enddate")
 # print(rp_enddate)
 # assert rp_enddate

 # measureid = dbutils.widgets.get("measureid")
 # print(measureid)
 # assert measureid

 # defaultcodes = dbutils.widgets.get("defaultcodes")
 # print(defaultcodes)
 # assert defaultcodes

-- COMMAND ----------

-----------------------------------------------------------------------------------------------------------
--  This calculates the 7 DQ measures to do with "Organisation Identifier (Of Specialised Responsible Commissioner)", ie MHS-DQM57 - MHS-DQM63.
-----------------------------------------------------------------------------------------------------------
INSERT INTO $db_output.dq_stg_validity
SELECT 
  --'Organisation Identifier (Of Specialised Responsible Commissioner) - Indirect Activity' AS MeasureName,
  3 as DimensionTypeId,
  $measureid as MeasureId,
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN vc.SpecialisedResponsibleCommissionerCode is not null 
        AND ORG_CODE IS NOT NULL -- could be matched to an Organisation Identifier in the ODS Organisational tables
		AND (ORG_CLOSE_DATE > '$rp_enddate' OR ORG_CLOSE_DATE IS NULL) -- which was still open at the end of the reporting period
        AND OrgIdComm NOT IN $defaultcodes -- not in VPP00 or XMD00 or YDD82 default codes
        THEN 1
        ELSE 0
      END) AS Valid
      ,
  SUM(CASE
        WHEN vc.SpecialisedResponsibleCommissionerCode is not null 
        AND ORG_CODE IS NOT NULL -- could be matched to an Organisation Identifier in the ODS Organisational tables
        AND ORG_CLOSE_DATE BETWEEN date_add('$rp_startdate', -1) AND '$rp_enddate' -- which was closed during or one day before the start of the reporting period
        AND OrgIdComm NOT IN $defaultcodes -- not in VPP00 or XMD00 or YDD82 default codes
        THEN 1
        ELSE 0
      END) AS Other
      ,
    SUM(CASE
        WHEN OrgIdComm IN $defaultcodes -- in VPP00 or XMD00 or YDD82 default codes
       THEN 1
        ELSE 0
      END) AS Default
      ,
  SUM(CASE 
        WHEN 
            (
            vc.SpecialisedResponsibleCommissionerCode is null
            OR
              (
                OrgIdComm IS NOT NULL -- no Organisation Identifier (Of Commissioner)
                AND TRIM(OrgIdComm) <> '' -- no Organisation Identifier (Of Commissioner)
                AND (ORG_CODE IS NULL -- Org Identifier (Of Commissioner) that could not be matched to an Org Identifier in the ODS Organisational tables
                     OR ORG_CLOSE_DATE < date_add('$rp_startdate', -1) -- closed prior to the day before the start of the reporting period
                    )
              )
            )       
             AND (OrgIdComm NOT IN $defaultcodes) -- not in VPP00 or XMD00 or YDD82 default codes
		THEN 1 
		ELSE 0
	  END) AS Invalid
      ,
  SUM(CASE
         WHEN OrgIdComm IS NULL -- no Organisation Identifier (Of Commissioner)
         OR TRIM(OrgIdComm) = '' -- no Organisation Identifier (Of Commissioner)
         THEN 1
         ELSE 0
       END) AS Missing
FROM (
  SELECT
	dq.UniqMonthId,
	dq.OrgIDProv,
	dq.OrgIdComm,
	org.BUSINESS_START_DATE,
	org.BUSINESS_END_DATE,
	org.ORG_CLOSE_DATE,
	org.ORG_CODE,
	head.ReportingPeriodStartDate
  FROM $dbm.$dbtable dq
    -----------------------------------------------------------------------------------------------------------
    -- The following JOIN fetches the record that was valid at the time of the reporting period.
    -- The BUSINESS_START_DATE is a bit of a misnomer (see http://ssrs.ic.nhs.uk/Reports/Pages/Report.aspx?ItemPath=%2fDSS%2fDSSInventory%2fDSSInventory_Metadata&ExecId=nxupdb55aqybh245mlfis255&PingId=2s3sk1mqhtqbpo55eqx5amnv).
    -- What it means is that there are one (or many) records for each organisation, and the BUSINESS_START_DATE/BUSINESS_END_DATE columns signify the date when each record has been created and when it has been invalidated.
    --
    -- Example organisations:
    -- 
    -- ORG_CODE   BUSINESS_START_DATE   BUSINESS_END_DATE   ORG_CLOSE_DATE
    --
    -- 01A        2013-05-10            NULL                NULL
    -- 01A        2013-04-01            2013-05-09          NULL
    --
    -- Y02588     2009-04-01            2014-02-12          NULL
    -- Y02588     2014-02-13            2017-02-24          2010-04-02
    --
    -- Y02590     2014-04-18            NULL                2012-09-30
    -- Y02590     2009-03-01            2013-10-15          NULL
    -- Y02590     2014-04-17            2014-04-17          NULL
    -- Y02590     2013-10-16            2014-04-16          2013-09-30
    -----------------------------------------------------------------------------------------------------------
	LEFT JOIN (SELECT 
				ORG_CODE, 
				BUSINESS_START_DATE, 
				BUSINESS_END_DATE,
				ORG_CLOSE_DATE,
				ORG_TYPE_CODE,
                ROW_NUMBER() OVER(PARTITION BY ORG_CODE ORDER BY IFNULL(BUSINESS_END_DATE, CURRENT_DATE()) DESC, IFNULL(ORG_CLOSE_DATE, CURRENT_DATE()) DESC) AS RowNumber
				FROM $$reference_data.org_daily --$db_output.dq_vw_org_daily
				WHERE BUSINESS_START_DATE <= '$rp_enddate'
                AND (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)) org --17/10/2022: updated to >= from > because the previous code was excluding organisations with a BUSINESS_END_DATE of the lat day of the month (BITC-4072)
			ON org.ORG_CODE = dq.OrgIdComm 
               AND org.RowNumber = 1
	LEFT JOIN $dbm.MHS000Header head ON head.UniqSubmissionID = dq.UniqSubmissionID 
                                        AND org.BUSINESS_START_DATE <= head.ReportingPeriodEndDate
                                        AND dq.UniqMonthId = '$month_id'
	) main
  LEFT JOIN global_temp.SpecialisedResponsibleCommissionerCode_view as vc
    on main.OrgIdComm = vc.SpecialisedResponsibleCommissionerCode 
   -- and upper(vc.table) = upper('$dbtable')
   -- and upper(vc.Field) = 'ORGIDCOMM' 
    --and vc.Measure = concat('MHS-DQM','$measureid')
    --and vc.type = 'VALID' 
    --and $month_id >=vc.firstmonth 
    --and (vc.lastmonth is null or $month_id <= vc.lastmonth)
WHERE main.UniqMonthId = '$month_id'
GROUP by main.OrgIDProv, main.UniqMonthId

-- COMMAND ----------

select * from $db_output.dq_stg_validity order by MeasureId desc