-- Databricks notebook source
 %python

 from dsp.udfs import postcode_active_at
 spark.udf.register('PostcodeActiveAt',postcode_active_at)

-- COMMAND ----------

 %py
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


-- COMMAND ----------

TRUNCATE TABLE $db_output.dq_stg_validity;

-- COMMAND ----------

-- DBTITLE 1,Valid NHS Number Flag
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'NHS Number' AS MeasureName,
   3 as DimensionTypeId,
   1 as MeasureId,
   OrgIDProv,
   COUNT(*) As Denominator,
   SUM(CASE
         WHEN NHSNumber IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Valid, 
   0 AS Other,
   0 AS Default,
   0 AS Invalid,
   SUM(CASE
         WHEN NHSNumber IS NULL THEN 1
         ELSE 0
       END
       ) AS Missing 
 FROM $dbm.mhs001mpi
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;


-- COMMAND ----------

 %sql

 drop table if exists $db_output.temp_stg_postcodes_analysis;
 create table $db_output.temp_stg_postcodes_analysis(
   OrgIDProv STRING,
   Valid int,
   Default int,
   Invalid int,
   Missing int
 );

 insert into $db_output.temp_stg_postcodes_analysis
 SELECT
   OrgIDProv,
   (CASE 
         WHEN upper(Postcode) NOT LIKE 'ZZ99%' AND (PostcodeActiveAt(Postcode,'$rp_startdate') = true or PostcodeActiveAt(Postcode,'$rp_enddate') = true) THEN 1
         ELSE 0
       END
       ) AS Valid,
   (CASE
         WHEN upper(Postcode) LIKE 'ZZ99%' THEN 1
         ELSE 0
       END
       ) AS Default,
   (CASE 
         WHEN PostCode is not null and (PostcodeActiveAt(Postcode,'$rp_startdate') = false and PostcodeActiveAt(Postcode,'$rp_enddate') = false) then 1
         ELSE 0
       END
       ) AS Invalid,
   (CASE
       WHEN PostCode is null then 1
       ELSE 0
     END
     ) as Missing       
 FROM $dbm.mhs001mpi
 WHERE UniqMonthID = $month_id


-- COMMAND ----------

 %sql
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Postcode Of Usual Address' AS MeasureName,
   3 as DimensionTypeId,
   2 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(Valid) AS Valid,
   0 as Other,
   SUM(Default) AS Default,
   SUM(Invalid) AS Invalid,
   SUM(Missing) as Missing       
 FROM $db_output.temp_stg_postcodes_analysis
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Valid Postcode Flag - commented out
 %sql


 --INSERT INTO $db_output.dq_stg_validity
 -- SELECT
 --   --'Postcode Of Usual Address' AS MeasureName,
 --   3 as DimensionTypeId,
 --   2 as MeasureId,
 --   OrgIDProv,
 --   COUNT(*) AS Denominator,
 --   SUM(CASE 
 --         WHEN upper(Postcode) NOT LIKE 'ZZ99%' AND PostcodeActiveAt(Postcode) = true THEN 1
 --         ELSE 0
 --       END
 --       ) AS Valid,
 --   0 as Other,
 --   SUM(CASE
 --         WHEN upper(Postcode) LIKE 'ZZ99%' AND PostcodeActiveAt(Postcode) == true THEN 1
 --         ELSE 0
 --       END
 --       ) AS Default,
 --   SUM(CASE 
 --         WHEN PostCode is not null and (PostcodeActiveAt(Postcode) = false) then 1
 --         ELSE 0
 --       END
 --       ) AS Invalid,
 --   SUM(CASE
 --       WHEN PostCode is null then 1
 --       ELSE 0
 --     END
 --     ) as Missing       
 -- FROM $dbm.mhs001mpi
 -- WHERE UniqMonthID = $month_id
 -- GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Age of patient at Reporting Period End
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Person Birth Date' AS MeasureName,
   3 as DimensionTypeId,
   3 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN AgeRepPeriodEnd <= 120 THEN 1
         ELSE 0
       END
       ) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN AgeRepPeriodEnd > 120 THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN AgeRepPeriodEnd IS NULL THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.mhs001mpi
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Person Stated Gender
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Person Stated Gender Code' AS MeasureName,
   3 as DimensionTypeId,
   4 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN Gender IN ('1', '2') THEN 1
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN Gender IN ('9') THEN 1
         ELSE 0
       END
       ) AS Other,
   SUM(CASE
         WHEN upper(Gender) IN ('X') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
         WHEN upper(Gender) NOT IN ('1', '2', '9', 'X')
         AND Gender <> ''
         AND Gender IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN Gender IS NULL
         OR Gender = ''
         OR Gender = ' ' THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.mhs001mpi
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Ethnic Category
 %sql
 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Ethnic Category Code' AS MeasureName,
   3 as DimensionTypeId,
   5 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN LEFT(upper(EthnicCategory), 1) IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S') THEN 1
         ELSE 0
       END
       ) AS Valid,
   SUM(CASE
         WHEN LEFT(upper(EthnicCategory), 1) IN ('Z') THEN 1
         ELSE 0
       END
       ) AS Other,
   SUM(CASE
         WHEN EthnicCategory IN ('99') THEN 1
         ELSE 0
       END
       ) AS Default,
   SUM(CASE
         WHEN LEFT(upper(EthnicCategory), 1) NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'Z')
         and LTRIM(RTRIM(EthnicCategory)) NOT IN ('99')
         AND LTRIM(RTRIM(EthnicCategory)) NOT IN ('')
         AND EthnicCategory IS NOT NULL THEN 1
         ELSE 0
       END
       ) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(EthnicCategory)) = ''
         OR EthnicCategory IS NULL THEN 1
         ELSE 0
       END
       ) AS Missing
 FROM $dbm.mhs001mpi
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;


-- COMMAND ----------

-- DBTITLE 1,General Medical Practice Code (Patient Registration)
WITH cte AS
(
SELECT 
  m.*,o.* 
  FROM $dbm.mhs002gp m
 LEFT OUTER JOIN (SELECT
                      CODE,
                      OPEN_DATE,
                      CLOSE_DATE,
                      ROW_NUMBER() OVER(PARTITION BY CODE ORDER BY IFNULL(CLOSE_DATE, CURRENT_DATE()) DESC) AS RowNUmber
                  FROM $$reference_data.ods_practice_v02
                 ) o ON m.GMPReg = o.CODE AND o.RowNUmber = 1                                   ----V6_Changes
 WHERE m.UniqMonthID = $month_id
 )
 , 
 vodimOutput as
 
(
SELECT  
  --'General Medical Practice Code (Patient Registration)' AS MeasureName,
  3 AS DimensionTypeId,
  6 AS MeasureId,
  OrgIDProv,
  COUNT(*) AS Denominator,
  SUM(CASE
        WHEN upper(GMPReg) NOT IN ('V81999', 'V81998', 'V81997')
        AND (CLOSE_DATE > '$rp_enddate' OR CLOSE_DATE IS NULL) and OPEN_DATE <= '$rp_enddate' THEN 1
        ELSE 0
      END) AS Valid,
  SUM(CASE
        WHEN GMPReg IN (CODE) 
        AND upper(GMPReg) NOT IN ('V81999', 'V81998', 'V81997')
        AND OPEN_DATE <= '$rp_enddate'
        AND CLOSE_DATE >= date_add('$rp_startdate', -1)
        AND CLOSE_DATE <= '$rp_enddate'
        THEN 1
        ELSE 0
      END) AS Other,
  SUM(CASE
        WHEN upper(GMPReg) IN ('V81999', 'V81998', 'V81997') THEN 1
        ELSE 0
      END) AS Default,
  SUM(CASE
        WHEN (upper(GMPReg) NOT IN ('V81999', 'V81998', 'V81997') AND CODE IS NULL)
        OR CLOSE_DATE <= date_add('$rp_startdate', -1) THEN 1
        ELSE 0
      END) AS Invalid,
  0 AS Missing
  from cte
GROUP BY OrgIDProv
) 

INSERT INTO $db_output.dq_stg_validity
(
  SELECT 
    DimensionTypeID,  
    MeasureID,  
    OrgIDProv,  
    Denominator,  
    Valid,  
    Other,  
    Default,  
    Invalid,  
    Missing
  FROM vodimOutput
)


-- COMMAND ----------

-- DBTITLE 1,Legal Status Classification Code
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Mental Health Act Legal Status Classification Code' AS MeasureName,
   3 as DimensionTypeId,
   7 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN LegalStatusCode IN ('02', '03', '04', '05', '06', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '19', '20', '31', '32', '35', '36', '37', '38') THEN 1
         ELSE 0
       END) AS Valid,
   SUM(CASE
         WHEN LegalStatusCode IN ('01') THEN 1
         ELSE 0
       END) AS Other,
   SUM(CASE
         WHEN LegalStatusCode IN ('98', '99') THEN 1
         ELSE 0
       END) AS Default,
   SUM(CASE
         WHEN LegalStatusCode NOT IN ('02', '03', '04', '05', '06', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '19', '20', '31', '32', '35', '36', '37', '38')
         AND LegalStatusCode NOT IN ('01')
         AND LegalStatusCode NOT IN ('98', '99')
         AND LTRIM(RTRIM(LegalStatusCode)) <> ''
         AND LegalStatusCode IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(LegalStatusCode)) = ''
         OR LegalStatusCode IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs401mhactperiod
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Treatment Function Code (Mental Health)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Treatment Function Code (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   8 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND LTRIM(RTRIM(TreatFuncCodeMH)) <> ''
         AND TreatFuncCodeMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(TreatFuncCodeMH)) = ''
         OR TreatFuncCodeMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs503assignedcareprof ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS503AssignedCareProf' and vc.field = 'TreatFuncCodeMH' and vc.Measure = 'MHS-DQM08' and vc.type = 'VALID' and ref.TreatFuncCodeMH = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Organisation Site Identifier (Of Treatment) (V5 and prior)
 %sql

 WITH cte AS
 (SELECT m.*,o.*
 FROM $dbm.mhs502wardstay m
 LEFT OUTER JOIN
 (
 SELECT 
   ORG_CODE,
   BUSINESS_START_DATE, 
   BUSINESS_END_DATE,
   ORG_CLOSE_DATE,
   ORG_TYPE_CODE,
   ROW_NUMBER() OVER(PARTITION BY ORG_CODE ORDER BY IFNULL(BUSINESS_END_DATE, CURRENT_DATE()) DESC, IFNULL(ORG_CLOSE_DATE, CURRENT_DATE()) DESC) AS RowNumber
 FROM $$reference_data.org_daily /* $db_output.dq_vw_org_daily */
 WHERE BUSINESS_START_DATE <= '$rp_enddate'
 AND (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL) --17/10/2022: updated to >= from > because the previous code was excluding organisations with a BUSINESS_END_DATE of the lat day of the month (BITC-4072)
 ) o ON m.SiteIDOfTreat = o.org_CODE
        AND o.RowNumber = 1 
        AND (BUSINESS_START_DATE <= '$rp_enddate' OR SiteIDOfTreat IS NULL)
        AND o.BUSINESS_START_DATE <= '$rp_enddate'
        WHERE UniqMonthID = $month_id 
        AND UniqMonthID <= 1488
  ), vodimOutput AS
  (SELECT
   --'Organisation Site Identifier (Of Treatment)' AS MeasureName,
   3 as DimensionTypeId,
   9 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
    SUM(CASE
          WHEN ORG_CODE IS NOT NULL
          AND upper(SiteIDOfTreat) NOT IN ('R9998', '89997', '89999')
          AND (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL) THEN 1
          ELSE 0
        END) AS Valid,
    SUM(CASE
          WHEN ORG_CODE IS NOT NULL
          AND upper(SiteIDOfTreat) NOT IN ('R9998', '89997', '89999')
          AND BUSINESS_END_DATE >= date_add('$rp_startdate', -1)
          AND BUSINESS_END_DATE <= '$rp_enddate' THEN 1
          ELSE 0
        END) AS Other,
    SUM(CASE
         WHEN upper(SiteIDOfTreat) IN ('R9998', '89997', '89999') THEN 1
         ELSE 0
       END) AS Default,
    SUM(CASE
          WHEN ((upper(SiteIDOfTreat) NOT IN ('R9998', '89997', '89999') AND ORG_CODE IS NULL) OR (upper(SiteIDOfTreat) NOT IN ('R9998', '89997', '89999') AND ORG_CODE IS NOT NULL AND BUSINESS_END_DATE <= date_add('$rp_startdate', -1)))
            AND LTRIM(RTRIM(SiteIDOfTreat)) <> ''
            AND SiteIDOfTreat IS NOT NULL THEN 1
          ELSE 0
        END) AS Invalid,
    SUM(CASE
          WHEN LTRIM(RTRIM(SiteIDOfTreat)) = ''
          OR SiteIDOfTreat IS NULL THEN 1
          ELSE 0
        END) AS Missing
 FROM cte
 GROUP BY OrgIDProv
 ) 
 INSERT INTO $db_output.dq_stg_validity
 (
   SELECT
     DimensionTypeID,  
     MeasureID,  
     OrgIDProv,  
     Denominator,  
     Valid,  
     Other,  
     Default,  
     Invalid,
     Missing 
    FROM vodimOutput
 )



-- COMMAND ----------

-- DBTITLE 1,Organisation Site Identifier (Of Ward) (V6 data)
 %sql

 WITH cte AS
 (SELECT w.*,m.SiteIDOfWard,o.*
 FROM $dbm.MHS502WardStay w
 LEFT JOIN $dbm.MHS903WardDetails m ON w.UniqWardCode = m.UniqWardCode and w.UniqMonthID = m.UniqMonthID
 LEFT OUTER JOIN
 (
 SELECT 
   ORG_CODE,
   BUSINESS_START_DATE, 
   BUSINESS_END_DATE,
   ORG_CLOSE_DATE,
   ORG_TYPE_CODE,
   ROW_NUMBER() OVER(PARTITION BY ORG_CODE ORDER BY IFNULL(BUSINESS_END_DATE, CURRENT_DATE()) DESC, IFNULL(ORG_CLOSE_DATE, CURRENT_DATE()) DESC) AS RowNumber
 FROM $$reference_data.org_daily /* $db_output.dq_vw_org_daily */
 WHERE BUSINESS_START_DATE <= '$rp_enddate'
 AND (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL) --17/10/2022: updated to >= from > because the previous code was excluding organisations with a BUSINESS_END_DATE of the lat day of the month (BITC-4072)
 ) o ON m.SiteIDOfWard = o.org_CODE
        AND o.RowNumber = 1 
        AND (BUSINESS_START_DATE <= '$rp_enddate' OR SiteIDOfWard IS NULL)
        AND o.BUSINESS_START_DATE <= '$rp_enddate'
        WHERE w.UniqMonthID = $month_id 
        AND w.UniqMonthID > 1488
  ), vodimOutput AS
  (SELECT
   --'Organisation Site Identifier (Of Treatment)' AS MeasureName,
   3 as DimensionTypeId,
   9 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
    SUM(CASE
          WHEN ORG_CODE IS NOT NULL
          AND upper(SiteIDOfWard) NOT IN ('R9998', '89997', '89999')
          AND (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL) THEN 1
          ELSE 0
        END) AS Valid,
    SUM(CASE
          WHEN ORG_CODE IS NOT NULL
          AND upper(SiteIDOfWard) NOT IN ('R9998', '89997', '89999')
          AND BUSINESS_END_DATE >= date_add('$rp_startdate', -1)
          AND BUSINESS_END_DATE <= '$rp_enddate' THEN 1
          ELSE 0
        END) AS Other,
    SUM(CASE
         WHEN upper(SiteIDOfWard) IN ('R9998', '89997', '89999') THEN 1
         ELSE 0
       END) AS Default,
    SUM(CASE
          WHEN ((upper(SiteIDOfWard) NOT IN ('R9998', '89997', '89999') AND ORG_CODE IS NULL) OR (upper(SiteIDOfWard) NOT IN ('R9998', '89997', '89999') AND ORG_CODE IS NOT NULL AND BUSINESS_END_DATE <= date_add('$rp_startdate', -1)))
            AND LTRIM(RTRIM(SiteIDOfWard)) <> ''
            AND SiteIDOfWard IS NOT NULL THEN 1
          ELSE 0
        END) AS Invalid,
    SUM(CASE
          WHEN LTRIM(RTRIM(SiteIDOfWard)) = ''
          OR SiteIDOfWard IS NULL THEN 1
          ELSE 0
        END) AS Missing
 FROM cte
 GROUP BY OrgIDProv
 ) 
 INSERT INTO $db_output.dq_stg_validity
 (
   SELECT
     DimensionTypeID,  
     MeasureID,  
     OrgIDProv,  
     Denominator,  
     Valid,  
     Other,  
     Default,  
     Invalid,
     Missing 
    FROM vodimOutput
 )



-- COMMAND ----------

-- DBTITLE 1,Primary Reason for Referral (Mental Health)
 %sql
 -- User changed to exclude all records where MHS102OtherServiceType.ReferRejectReason = '02' 

 INSERT INTO $db_output.dq_stg_validity
 SELECT 
   --Primary Reason for Referral (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   10 as MeasureId,
    ref.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null 
         AND PrimReasonReferralMH is not NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(PrimReasonReferralMH)) = ''
         OR PrimReasonReferralMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral ref
 --LEFT JOIN $dbm.MHS102ServiceTypeReferredTo mhs102
 --on ref.ServiceRequestId = mhs102.ServiceRequestId and ref.uniqmonthID = mhs102.UniqMonthID and ref.UniqServReqID = mhs102.UniqServReqID
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS101Referral' and vc.field = 'PrimReasonReferralMH' and vc.Measure = 'MHS-DQM10' and vc.type = 'VALID' and ref.PrimReasonReferralMH = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE ref.UniqMonthID = $month_id
 AND IFNULL(ref.ReferRejectReason, 'xx') <> '02'
 GROUP BY ref.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,Care Professional Service or Team Type Associate (Mental Health)
 %sql

 /** User updated codes for CareProfServOrTeamTypeAssoc for v4.1 **/

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Care Professional Service or Team Type Association (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   11 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,


 SUM(CASE  WHEN vc.Measure is not null THEN 1
         ELSE 0
      END) AS Valid, 
       
   SUM(CASE
         WHEN upper(CareProfServOrTeamTypeAssoc) IN ('Z01', 'Z02') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND upper(CareProfServOrTeamTypeAssoc) NOT IN ('Z01', 'Z02')
         AND LTRIM(RTRIM(CareProfServOrTeamTypeAssoc)) <> ''
         AND CareProfServOrTeamTypeAssoc IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(CareProfServOrTeamTypeAssoc)) = ''
         OR CareProfServOrTeamTypeAssoc IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs006mhcarecoord ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS006MHCareCoord' and vc.field = 'CareProfServOrTeamTypeAssoc' and vc.Measure = 'MHS-DQM11' and vc.type = 'VALID' and upper(ref.CareProfServOrTeamTypeAssoc) = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Organisation Identifier (Of Commissioner)
 %py
 month_id = dbutils.widgets.get("month_id")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 dbm = dbutils.widgets.get("dbm")
 db_output = dbutils.widgets.get("db_output")
 $reference_data = dbutils.widgets.get("$reference_data")

 for m in [('12','MHS101Referral',('VPP00','XMD00','X99998','X99999')),('13','MHS201CareContact',('VPP00','XMD00','R9998','89997')),('14','MHS204IndirectActivity',('VPP00','XMD00')),('15','MHS301GroupSession',('VPP00','XMD00','R9998','89997','89999')),('16','MHS512HospSpellCommAssPer',('VPP00','XMD00')),('17','MHS608AnonSelfAssess',('VPP00','XMD00'))]:
   measureid = str(m[0])
   dbtable = str(m[1])
   defaultcodes = str(m[2])
   print(measureid)
   print(dbtable)
   print(defaultcodes)
   dbutils.notebook.run("validity_commissioner", 0, {'rp_enddate': rp_enddate,  'rp_startdate': rp_startdate, 'month_id': month_id, '$reference_data': $reference_data , 'db_output': db_output, 'dbm':dbm , 'measureid': measureid , 'dbtable': dbtable , 'defaultcodes': defaultcodes}); 

-- COMMAND ----------

-- DBTITLE 1,TO BE DELETED Organisation Identifier (Of Commissioner)
 %python

 # month_id = dbutils.widgets.get("month_id")
 # rp_startdate = dbutils.widgets.get("rp_startdate")
 # rp_enddate = dbutils.widgets.get("rp_enddate")
 # dbm = dbutils.widgets.get("dbm")
 # db_output = dbutils.widgets.get("db_output")
 # $reference_data = dbutils.widgets.get("$reference_data")

 # ## User: this should probably be changed to pass parameters via a loop into the validity_commissioner notebook - currently rather over-simplified...

 # params12 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, '$reference_data' : $reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 12, "DbTable": "MHS101Referral"}
 # params13 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, '$reference_data' : $reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 13, "DbTable": "MHS201CareContact"}
 # params14 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, '$reference_data' : $reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 14, "DbTable": "MHS204IndirectActivity"}         
 # params15 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, '$reference_data' : $reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 15, "DbTable": "MHS301GroupSession"}
 # params16 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, '$reference_data' : $reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 16, "DbTable": "MHS512HospSpellCommAssPer"}    
 # params17 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, '$reference_data' : $reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 17, "DbTable": "MHS608AnonSelfAssess"}

 # print(params12)
 # print(params13)
 # print(params14)
 # print(params15)
 # print(params16)
 # print(params17)
             
 # # Debugging
 # # month_id = 1417
 # # rp_startdate = "2018-04-01"
 # # rp_enddate = "2018-04-30"
 # # dbm = "$mhsds"

 # # This calculates the 6 DQ measures to do with "Organisation Identifier (Of Commissioner)", ie MHS-DQM12 - MHS-DQM17. 
 # dbutils.notebook.run("validity_commissioner", 0, params12)
 # dbutils.notebook.run("validity_commissioner", 0, params13)
 # dbutils.notebook.run("validity_commissioner", 0, params14)
 # dbutils.notebook.run("validity_commissioner", 0, params15)
 # dbutils.notebook.run("validity_commissioner", 0, params16)
 # dbutils.notebook.run("validity_commissioner", 0, params17)

-- COMMAND ----------

-- DBTITLE 1,Organisation Identifier (Of Specialised Responsible Commissioner) - with new default data
 %python

 month_id = dbutils.widgets.get("month_id")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 dbm = dbutils.widgets.get("dbm")
 db_output = dbutils.widgets.get("db_output")
 $reference_data = dbutils.widgets.get("$reference_data")

 for m in [('57','MHS101Referral',('VPP00','XMD00','X99998','X99999')),('58','MHS201CareContact',('VPP00','XMD00','R9998','89997')),('59','MHS204IndirectActivity',('VPP00','XMD00')),('60','MHS301GroupSession',('VPP00','XMD00','R9998','89997','89999')),('61','MHS512HospSpellCommAssPer',('VPP00','XMD00')),('62','MHS517SMHExceptionalPackOfCare',('VPP00','XMD00')),('63','MHS608AnonSelfAssess',('VPP00','XMD00'))]:
   measureid = str(m[0])
   dbtable = str(m[1])
   defaultcodes = str(m[2])
   print(measureid)
   print(dbtable)
   print(defaultcodes)
   #NP-Feb-22- I couldn't get the code to run when passing a param to dbutils.notebook.run.  But it worked like this!
   dbutils.notebook.run("validity_responsible_commissioner", 0, {'rp_enddate': rp_enddate,  'rp_startdate': rp_startdate, 'month_id': month_id, '$reference_data': $reference_data , 'db_output': db_output, 'dbm':dbm , 'measureid': measureid , 'dbtable': dbtable , 'defaultcodes': defaultcodes}); 
  

-- COMMAND ----------

-- DBTITLE 1,Service Or Team Type Referred To (Mental Health)
 %sql

 ----V6_Changes (ServiceTeamType in new table MHS902ServiceTeamDetails)

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Service Or Team Type Referred To (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   18 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   
   SUM(CASE WHEN vc.Measure is not null THEN 1
         
         ELSE 0
       END) AS Valid,
   SUM(CASE
         WHEN upper(ServTeamTypeMH) IN ('Z01', 'Z02') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND upper(ServTeamTypeMH) NOT IN ('Z01', 'Z02')
         AND LTRIM(RTRIM(ServTeamTypeMH)) <> ''
         AND ServTeamTypeMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ServTeamTypeMH)) = ''
         OR ServTeamTypeMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $db_output.ServiceTeamType as ref                  ----V6_Changes (also to make sure it works for V5 data or before)
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS902ServiceTeamDetails' and vc.field = 'ServTeamTypeMH' and vc.Measure = 'MHS-DQM18' and vc.type = 'VALID' and upper(ref.ServTeamTypeMH) = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Primary Reason for Referral (Mental Health) (Referral received on or after 1st Jan 2016)- V6_data
 %sql

 --User: changed to exclude all records where MHS102ServiceTypeReferredTo.ReferRejectReason = '02'

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Primary Reason for Referral (Mental Health) (Referral received on or after 1st Jan 2016)' AS MeasureName,
   3 as DimensionTypeId,
   19 as MeasureId,
   ref.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
       WHEN vc.Measure is not null THEN 1
         ELSE 0
         END) AS Valid, 
   0 AS Other,
   0 AS Default,

   SUM(CASE
       WHEN vc.Measure is null --28
       AND LTRIM(RTRIM(PrimReasonReferralMH)) <> ''
       AND PrimReasonReferralMH IS NOT NULL THEN 1
         ELSE 0
         END) AS Invalid, 
   
   SUM(CASE
         WHEN LTRIM(RTRIM(PrimReasonReferralMH)) = ''
         OR PrimReasonReferralMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS101Referral' and vc.field = 'PrimReasonReferralMH' and vc.Measure = 'MHS-DQM19' and vc.type = 'VALID' and ref.PrimReasonReferralMH = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 --V6_Changes
 --LEFT JOIN $dbm.MHS102ServiceTypeReferredTo rej
 --on ref.ServiceRequestId = rej.ServiceRequestId and rej.uniqmonthID = ref.UniqMonthID and ref.UniqServReqID = rej.UniqServReqID
 WHERE ref.UniqMonthID = $month_id
 AND ref.UniqMonthID > 1488
 AND IFNULL(ref.ReferRejectReason, 'xx') <> '02'
 AND ReferralRequestReceivedDate >= '2016-01-01'
 GROUP BY ref.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Primary Reason for Referral (Mental Health) (Referral received on or after 1st Jan 2016)- V5_data (data prior to V6)
 %sql

 --User: changed to exclude all records where MHS102ServiceTypeReferredTo.ReferRejectReason = '02'

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Primary Reason for Referral (Mental Health) (Referral received on or after 1st Jan 2016)' AS MeasureName,
   3 as DimensionTypeId,
   19 as MeasureId,
   ref.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
       WHEN vc.Measure is not null THEN 1
         ELSE 0
         END) AS Valid, 
   0 AS Other,
   0 AS Default,

   SUM(CASE
       WHEN vc.Measure is null --28
       AND LTRIM(RTRIM(PrimReasonReferralMH)) <> ''
       AND PrimReasonReferralMH IS NOT NULL THEN 1
         ELSE 0
         END) AS Invalid, 
   
   SUM(CASE
         WHEN LTRIM(RTRIM(PrimReasonReferralMH)) = ''
         OR PrimReasonReferralMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS101Referral' and vc.field = 'PrimReasonReferralMH' and vc.Measure = 'MHS-DQM19' and vc.type = 'VALID' and ref.PrimReasonReferralMH = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth 
 is null or $month_id <= vc.LastMonth)
 LEFT JOIN $dbm.mhs102otherservicetype rej
 on ref.ServiceRequestId = rej.ServiceRequestId and rej.uniqmonthID = ref.UniqMonthID and ref.UniqServReqID = rej.UniqServReqID
 WHERE ref.UniqMonthID = $month_id
 AND ref.UniqMonthID <= 1488
 AND IFNULL(rej.ReferRejectReason, 'xx') <> '02'
 AND ReferralRequestReceivedDate >= '2016-01-01'
 GROUP BY ref.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Clinical Response Priority Type (Eating Disorder)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Clinical Response Priority Type' AS MeasureName,
   3 as DimensionTypeId,
   20 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND LTRIM(RTRIM(ClinRespPriorityType)) <> ''
         AND ClinRespPriorityType IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ClinRespPriorityType)) = ''
         OR ClinRespPriorityType IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS101Referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'MHS-DQM20' and vc.type = 'VALID' and ref.ClinRespPriorityType = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 AND PrimReasonReferralMH = '12'
 AND AgeServReferRecDate < 19
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Hospital Bed Type (Mental Health) (Ward stays started on or after 1st April 2017)
 %sql

 ---V6_Changes

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Hospital Bed Type (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   31 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND LTRIM(RTRIM(MHAdmittedPatientClass)) <> ''
         AND MHAdmittedPatientClass IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(MHAdmittedPatientClass)) = ''
         OR MHAdmittedPatientClass IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs502wardstay ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS502WardStay' and vc.field = 'MHAdmittedPatientClass' and vc.Measure = 'MHS-DQM31' and vc.type = 'VALID' and ref.MHAdmittedPatientClass = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 AND StartDateWardStay >= '2017-04-01'
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Out of Area Treatment Reason
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Referred out of area reason (adult acute mental health)' AS MeasureName,
   3 as DimensionTypeId,
   32 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN OATReason IN ('10', '11', '12', '13', '14') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   SUM(CASE
         WHEN OATReason IN ('99') THEN 1
         ELSE 0
       END) AS Default,
   SUM(CASE
         WHEN OATReason NOT IN ('10', '11', '12', '13', '14') 
         AND OATReason NOT IN ('99') THEN 1
         ELSE 0
       END) AS Invalid,
   0 AS Missing
 FROM $dbm.mhs105onwardreferral
 WHERE UniqMonthID = $month_id
 AND OATReason IS NOT NULL
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Ex-British armed forces indicator
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Ex-British armed forces indicator' AS MeasureName,
   3 as DimensionTypeId,
   33 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN ExBAFIndicator IN ('02', '03', '05') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   SUM(CASE
         WHEN upper(ExBAFIndicator) IN ('UU', 'ZZ') THEN 1
         ELSE 0
       END) AS Default,
   SUM(CASE
         WHEN (ExBAFIndicator) NOT IN ('02', '03', '05')
         AND upper(ExBAFIndicator) NOT IN ('UU', 'ZZ') THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ExBAFIndicator)) = ''
         OR ExBAFIndicator IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs005patind
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Source of Referral
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Source of Referral' AS MeasureName,
   3 as DimensionTypeId,
   34 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND LTRIM(RTRIM(SourceOfReferralMH)) <> ''
         AND SourceOfReferralMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(SourceOfReferralMH)) = ''
         OR SourceOfReferralMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS101Referral' and vc.field = 'SourceOfReferralMH' and vc.Measure = 'MHS-DQM34' and vc.type = 'VALID' and upper(ref.SourceOfReferralMH) = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1, Consultation Mechanism (Mental Health)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --' Consultation Mechanism (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   35 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null
         AND ConsMechanismMH NOT IN ('98') THEN 1
         ELSE 0
       END) AS Valid,
   SUM(CASE
         WHEN ConsMechanismMH IN ('98') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND ConsMechanismMH NOT IN ('98')
         AND LTRIM(RTRIM(ConsMechanismMH)) <> ''
         AND ConsMechanismMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ConsMechanismMH)) = ''
         OR ConsMechanismMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs201carecontact ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS201CareContact' and vc.field = 'ConsMechanismMH' and vc.Measure = 'MHS-DQM35' and vc.type = 'VALID' and ref.ConsMechanismMH = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Activity location type code
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Activity location type code' AS MeasureName,
   3 as DimensionTypeId,
   37 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN upper(ActLocTypeCode) IN ('A01', 'A02', 'A03', 'A04', 'B01', 'B02', 'C01', 'C02', 'C03', 'D01', 'D02', 'D03', 'E01', 'E02', 'E03', 'E04', 'E99', 'F01', 'G01', 'G02', 'G03', 'G04', 'H01', 'J01', 'K01', 'K02', 'L01', 'L02', 'L03', 'L04', 'L05', 'L06', 'L99', 'M01', 'M02', 'M03', 'M04', 'M05', 'N01', 'N02', 'N03', 'N04', 'N05') THEN 1
         ELSE 0
       END) AS Valid,
   SUM(CASE
         WHEN ActLocTypeCode IN ('X01') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN upper(ActLocTypeCode) NOT IN ('A01', 'A02', 'A03', 'A04', 'B01', 'B02', 'C01', 'C02', 'C03', 'D01', 'D02', 'D03', 'E01', 'E02', 'E03', 'E04', 'E99', 'F01', 'G01', 'G02', 'G03', 'G04', 'H01', 'J01', 'K01', 'K02', 'L01', 'L02', 'L03', 'L04', 'L05', 'L06', 'L99', 'M01', 'M02', 'M03', 'M04', 'M05', 'N01', 'N02', 'N03', 'N04', 'N05')
         AND upper(ActLocTypeCode) NOT IN ('X01')
         AND LTRIM(RTRIM(ActLocTypeCode)) <> ''
         AND ActLocTypeCode IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ActLocTypeCode)) = ''
         OR ActLocTypeCode IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs201carecontact
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge reason  - V5 and prior data
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Delayed discharge reason' AS MeasureName,
   3 as DimensionTypeId,
   38 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null 
         and LTRIM(RTRIM(DelayDischReason)) <> '98'
         THEN 1
         ELSE 0
       END) AS Valid,
   SUM(CASE
      WHEN LTRIM(RTRIM(DelayDischReason)) = '98'
          THEN 1
          ELSE 0
     END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND LTRIM(RTRIM(DelayDischReason)) <> ''
         and LTRIM(RTRIM(DelayDischReason)) <> '98'
         AND DelayDischReason IS NOT NULL 
         THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(DelayDischReason)) = ''
         OR DelayDischReason IS NULL 
         THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs504delayeddischarge ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS504DelayedDischarge' and vc.field = 'DelayDischReason' and vc.Measure = 'MHS-DQM38' and vc.type = 'VALID' and upper(ref.DelayDischReason) = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)

 WHERE UniqMonthID = $month_id
 and UniqMonthID <= 1488
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge reason  - V6 data
 %sql

 ---V6_Changes

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Delayed discharge reason' AS MeasureName,
   3 as DimensionTypeId,
   38 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN vc.Measure is not null 
         and LTRIM(RTRIM(ClinReadyforDischDelayReason)) <> '98'
         THEN 1
         ELSE 0
       END) AS Valid,
   SUM(CASE
      WHEN LTRIM(RTRIM(ClinReadyforDischDelayReason)) = '98'
          THEN 1
          ELSE 0
     END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN vc.Measure is null
         AND LTRIM(RTRIM(ClinReadyforDischDelayReason)) <> ''
         and LTRIM(RTRIM(ClinReadyforDischDelayReason)) <> '98'
         AND ClinReadyforDischDelayReason IS NOT NULL 
         THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ClinReadyforDischDelayReason)) = ''
         OR ClinReadyforDischDelayReason IS NULL 
         THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.MHS518ClinReadyforDischarge ref
 LEFT JOIN $db_output.validcodes as vc
 ON vc.tablename = 'MHS518ClinReadyforDischarge' and vc.field = 'ClinReadyforDischDelayReason' and vc.Measure = 'MHS-DQM38' and vc.type = 'VALID' and upper(ref.ClinReadyforDischDelayReason) = vc.ValidValue and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)

 WHERE UniqMonthID = $month_id
 and UniqMonthID > 1488
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge attributable to (V5 data and prior)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Delayed discharge attributable to' AS MeasureName,
   3 as DimensionTypeId,
   39 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN AttribToIndic IN ('04', '05', '06', '07') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN AttribToIndic NOT IN ('04', '05', '06', '07')
         AND LTRIM(RTRIM(AttribToIndic)) <> ''
         AND AttribToIndic IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(AttribToIndic)) = ''
         OR AttribToIndic IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs504delayeddischarge
 WHERE UniqMonthID = $month_id
 and UniqMonthID <= 1488
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge attributable to (V6 Data onwards)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Delayed discharge attributable to' AS MeasureName,
   3 as DimensionTypeId,
   39 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN AttribToIndic IN ('04', '05', '06') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN AttribToIndic NOT IN ('04', '05', '06')
         AND LTRIM(RTRIM(AttribToIndic)) <> ''
         AND AttribToIndic IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(AttribToIndic)) = ''
         OR AttribToIndic IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.MHS518ClinReadyforDischarge
 WHERE UniqMonthID = $month_id
 and UniqMonthID > 1488
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Care plan type
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Care plan type' AS MeasureName,
   3 as DimensionTypeId,
   42 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN CarePlanTypeMH IN ('10', '11', '12', '13', '14') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN CarePlanTypeMH NOT IN ('10', '11', '12', '13', '14') THEN 1
         ELSE 0
       END) AS Invalid,
   0 AS Missing
 FROM $dbm.mhs008careplantype
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Provisional Diagnosis data (V5 data and prior)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Provisional Diagnosis data' AS MeasureName,
   3 as DimensionTypeId,
   45 as MeasureId,
   m.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN (to_date(m.CodedProvDiagTimestamp) BETWEEN r.ReferralRequestReceivedDate AND r.ServDischDate
         AND to_date(m.CodedProvDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.CodedProvDiagTimestamp) >= r.ReferralRequestReceivedDate
         AND m.CodedProvDiagTimestamp IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((to_date(m.CodedProvDiagTimestamp) < r.ReferralRequestReceivedDate OR to_date(m.CodedProvDiagTimestamp) > r.ServDischDate)
         AND to_date(m.CodedProvDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.CodedProvDiagTimestamp) < r.ReferralRequestReceivedDate
         AND to_date(m.CodedProvDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN to_date(m.CodedProvDiagTimestamp) IS NULL THEN 1
         ELSE 0
       END) Missing
 FROM $dbm.mhs603provdiag m
 INNER JOIN $dbm.mhs101referral r ON m.UniqServReqID = r.UniqServReqID
 AND m.Person_ID = r.Person_ID --UniqMHSDSPersID has been changed to Person_ID in Databricks.
 WHERE m.UniqMonthID = $month_id 
 and m.UniqMonthID <= 1488
 and r.UniqMonthID = $month_id
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Presenting Complaint data (V6_Changes)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Presenting Complaint data' AS MeasureName,
   3 as DimensionTypeId,
   44 as MeasureId,
   m.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN (to_date(m.PresCompDate) BETWEEN r.ReferralRequestReceivedDate AND r.ServDischDate
         AND to_date(m.PresCompDate) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.PresCompDate) >= r.ReferralRequestReceivedDate
         AND m.PresCompDate IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((to_date(m.PresCompDate) < r.ReferralRequestReceivedDate OR to_date(m.PresCompDate) > r.ServDischDate)
         AND to_date(m.PresCompDate) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.PresCompDate) < r.ReferralRequestReceivedDate
         AND to_date(m.PresCompDate) IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN to_date(m.PresCompDate) IS NULL THEN 1
         ELSE 0
       END) Missing
 FROM $dbm.MHS609PresComp m
 INNER JOIN $dbm.mhs101referral r ON m.UniqServReqID = r.UniqServReqID
 AND m.Person_ID = r.Person_ID --UniqMHSDSPersID has been changed to Person_ID in Databricks.
 WHERE m.UniqMonthID = $month_id 
 and m.UniqMonthID > 1488
 and r.UniqMonthID = $month_id
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Primary Diagnosis date
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Primary Diagnosis date' AS MeasureName,
   3 as DimensionTypeId,
   46 as MeasureId,
   m.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN (to_date(m.CodedDiagTimestamp) BETWEEN
         r.ReferralRequestReceivedDate
         AND
         r.ServDischDate
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.CodedDiagTimestamp) >= r.ReferralRequestReceivedDate
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((to_date(m.CodedDiagTimestamp) < r.ReferralRequestReceivedDate OR to_date(m.CodedDiagTimestamp) > r.ServDischDate)
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.CodedDiagTimestamp) < r.ReferralRequestReceivedDate
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN to_date(m.CodedDiagTimestamp) IS NULL THEN 1
         ELSE 0
       END) Missing
 FROM $dbm.mhs604primdiag m
 INNER JOIN $dbm.mhs101referral r ON m.UniqServReqID = r.UniqServReqID
 AND m.Person_ID = r.Person_ID --UniqMHSDSPersID has been changed to Person_ID in Databricks.
 WHERE m.UniqMonthID = $month_id
 and r.UniqMonthID = $month_id
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Secondary Diagnosis date
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Secondary Diagnosis date' AS MeasureName,
   3 as DimensionTypeId,
   47 as MeasureId,
   m.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN (to_date(m.CodedDiagTimestamp) BETWEEN
         r.ReferralRequestReceivedDate
         AND
         r.ServDischDate
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.CodedDiagTimestamp) >= r.ReferralRequestReceivedDate
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((to_date(m.CodedDiagTimestamp) < r.ReferralRequestReceivedDate OR to_date(m.CodedDiagTimestamp) > r.ServDischDate)
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (to_date(m.CodedDiagTimestamp) < r.ReferralRequestReceivedDate
         AND to_date(m.CodedDiagTimestamp) IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN to_date(m.CodedDiagTimestamp) IS NULL THEN 1
         ELSE 0
       END) Missing
 FROM $dbm.mhs605secdiag m
 INNER JOIN $dbm.mhs101referral r ON m.UniqServReqID = r.UniqServReqID
 AND m.Person_ID = r.Person_ID --UniqMHSDSPersID has been changed to Person_ID in Databricks.
 WHERE m.UniqMonthID = $month_id
 and r.UniqMonthID = $month_id
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Attend or did not attend
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Attended or did not attend' AS MeasureName,
   3 as DimensionTypeId,
   48 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN AttendStatus IN ('2','3','4','5','6','7') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN AttendStatus NOT IN ('2','3','4','5','6','7')
         AND LTRIM(RTRIM(AttendStatus)) <> ''
         AND AttendStatus IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(AttendStatus)) = ''
         OR AttendStatus IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs201carecontact
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Referral closure reason (V5 data and prior)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Referral closure reason' AS MeasureName,
   3 as DimensionTypeId,
   51 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN ReferClosReason IN ('01','02','03','04','05','06','07','08','09') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ReferClosReason NOT IN ('01','02','03','04','05','06','07','08','09')
         AND LTRIM(RTRIM(ReferClosReason)) <> ''
         AND ReferClosReason IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ReferClosReason)) = ''
         OR ReferClosReason IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $db_output.ServiceTeamType
 WHERE UniqMonthID = $month_id and ReferClosureDate is not null
 GROUP BY OrgIDProv;  

-- COMMAND ----------

-- DBTITLE 1,Estimated discharge date
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Estimated discharge date' AS MeasureName,
   3 as DimensionTypeId,
   52 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN EstimatedDischDateHospProvSpell >= StartDateHospProvSpell
         AND EstimatedDischDateHospProvSpell IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN EstimatedDischDateHospProvSpell < StartDateHospProvSpell
         AND EstimatedDischDateHospProvSpell IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(EstimatedDischDateHospProvSpell)) = ''
         OR EstimatedDischDateHospProvSpell IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs501hospprovspell
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Specialised mental health service code - Referral
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Specialised mental health service code - Referral' AS MeasureName,
   3 as DimensionTypeId,
   53 as MeasureId,
   m.OrgIdProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN s.ServiceCategoryCode IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN s.ServiceCategoryCode IS NULL THEN 1
         ELSE 0
       END) AS Invalid,
   0 AS Missing
 FROM $dbm.mhs101referral m
 LEFT OUTER JOIN $db_output.dq_smh_service_category_code s ON upper(m.SpecialisedMHServiceCode) = upper(s.ServiceCategoryCode)
 AND upper(s.ServiceCategoryCode) <> 'OTHER' --"Other" should be flagged as invalid to prompt providers to use one of the valid service categories. We expect "Other" to be utilised in extremely rare scenarios where the service does not fit into any of the available service category codes.
 and $month_id >= s.FirstMonth and (s.LastMonth is null or $month_id <= s.LastMonth)
 WHERE m.UniqMonthID = $month_id
 AND m.SpecialisedMHServiceCode IS NOT NULL
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Specialised mental health service code - Contact
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Specialised mental health service code - Contact' AS MeasureName,
   3 as DimensionTypeId,
   54 as MeasureId,
   m.OrgIdProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN s.ServiceCategoryCode IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN s.ServiceCategoryCode IS NULL THEN 1
         ELSE 0
       END) AS Invalid,
   0 AS Missing
 FROM $dbm.mhs201carecontact m
 LEFT OUTER JOIN $db_output.dq_smh_service_category_code s ON upper(m.SpecialisedMHServiceCode) = upper(s.ServiceCategoryCode)
 AND upper(s.ServiceCategoryCode) <> 'OTHER' --"Other" should be flagged as invalid to prompt providers to use on of the valid service categories. We expect "Other" to be utilised in extremely rare scenarios where the service does not fit into any of the available service category codes.
 and $month_id >= s.FirstMonth and (s.LastMonth is null or $month_id <= s.LastMonth)
 WHERE m.UniqMonthID = $month_id
 AND m.SpecialisedMHServiceCode IS NOT NULL
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Specialised mental health service code - Ward Stay
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Specialised mental health service code - Ward Stay' AS MeasureName,
   3 as DimensionTypeId,
   55 as MeasureId,
   m.OrgIdProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN s.ServiceCategoryCode IS NOT NULL THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN s.ServiceCategoryCode IS NULL THEN 1
         ELSE 0
       END) AS Invalid,   
   0 AS Missing
 FROM $dbm.mhs502wardstay m
 LEFT OUTER JOIN $db_output.dq_smh_service_category_code s ON upper(m.SpecialisedMHServiceCode) = upper(s.ServiceCategoryCode)
 AND upper(s.ServiceCategoryCode) <> 'OTHER' --"Other" should be flagged as invalid to prompt providers to use on of the valid service categories. We expect "Other" to be utilised in extremely rare scenarios where the service does not fit into any of the available service category codes.
 and $month_id >= s.FirstMonth and (s.LastMonth is null or $month_id <= s.LastMonth)
 WHERE m.UniqMonthID = $month_id
 AND m.SpecialisedMHServiceCode IS NOT NULL
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge; Local Authority of responsibility (V5 data and prior)
 %sql
 INSERT INTO $db_output.dq_stg_validity
 SELECT 
    3 as DimensionTypeId
   ,56 as MeasureId
   ,disch.OrgIdProv
   ,COUNT(*) AS Denominator
   ,sum(Case when disch.OrgIDRespLADelayDisch == (org.org_code) then 1 else 0 end) AS Valid
   ,0 AS Other
   ,0 AS Default
   ,sum(Case when disch.OrgIDRespLADelayDisch is not null and org.org_code is null then 1 else 0 end) AS Invalid
   ,sum(Case when disch.OrgIDRespLADelayDisch is null then 1 else 0 end) AS Missing
 FROM $dbm.MHS504delayeddischarge disch
 left outer join $$reference_data.org_daily org
 on disch.OrgIDRespLADelayDisch = org.org_code 
 and (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1) 
                 AND ORG_TYPE_CODE = 'LA'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
 where disch.UniqMonthID = $month_id
 and disch.UniqMonthID <= 1488
 group by disch.OrgIdProv


-- COMMAND ----------

-- DBTITLE 1,Clinically Ready for Discharge (New table for V6 data); Local Authority of responsibility
 %sql
 INSERT INTO $db_output.dq_stg_validity
 SELECT 
    3 as DimensionTypeId
   ,56 as MeasureId
   ,disch.OrgIdProv
   ,COUNT(*) AS Denominator
   ,sum(Case when disch.OrgIDRespLAClinReadyforDisch == (org.org_code) then 1 else 0 end) AS Valid
   ,0 AS Other
   ,0 AS Default
   ,sum(Case when disch.OrgIDRespLAClinReadyforDisch is not null and org.org_code is null then 1 else 0 end) AS Invalid
   ,sum(Case when disch.OrgIDRespLAClinReadyforDisch is null then 1 else 0 end) AS Missing
 FROM $dbm.MHS518ClinReadyforDischarge disch
 left outer join $$reference_data.org_daily org
 on disch.OrgIDRespLAClinReadyforDisch = org.org_code 
 and (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1) 
                 AND ORG_TYPE_CODE = 'LA'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
 where disch.UniqMonthID = $month_id
 and disch.UniqMonthID > 1488
 group by disch.OrgIdProv

-- COMMAND ----------

-- DBTITLE 1,Denominator - moved into VODIM_aggregation
-- %sql

-- WITH InventoryList
-- AS
-- (
--   SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Denominator' AS MeasureTypeName, Denominator AS Value FROM $db_output.dq_stg_validity
-- )
-- MERGE INTO $db_output.dq_inventory AS target
-- USING
-- (
--   SELECT
--     '$month_id' AS UniqMonthID,
--     im.DimensionTypeId,
--     im.MeasureId,
--     im.MeasureTypeId,
--     im.MetricTypeId,
--     il.OrgIDProv,
--     il.Value
--   FROM InventoryList il
--   INNER JOIN $db_output.dq_vw_inventory_metadata im   on il.DimensionTypeId = im.DimensionTypeId
--   AND il.MeasureId = im.MeasureId
--   AND il.MeasureTypeName = im.MeasureTypeName
--   WHERE im.StartDate <= '$rp_startdate'
--   AND CASE
--         WHEN im.EndDate IS NULL THEN '$rp_enddate'
--        ELSE im.EndDate
--       END >= '$rp_enddate'
-- ) AS source ON target.UniqMonthID = source.UniqMonthID
-- AND target.DimensionTypeId = source.DimensionTypeId
-- AND target.MeasureId = source.MeasureId
-- AND target.MeasureTypeId = source.MeasureTypeId
-- AND CASE
--       WHEN target.MetricTypeId IS NULL THEN 'null'
--       ELSE target.MetricTypeId
--       END = CASE
--               WHEN source.MetricTypeId IS NULL THEN 'null'
--               ELSE source.MetricTypeId
--             END
-- AND target.OrgIDProv = source.OrgIDProv
-- AND target.SOURCE_DB = '$dbm'
-- WHEN MATCHED
--   THEN UPDATE SET target.Value = source.Value
-- WHEN NOT MATCHED
--   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
--   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');

-- COMMAND ----------

-- DBTITLE 1,moved into VODIM_aggregation
-- %sql

-- DROP TABLE IF EXISTS $db_output.dq_stg_inventorylist;
-- CREATE TABLE IF NOT EXISTS $db_output.dq_stg_inventorylist
-- AS 
-- SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Valid' AS MetricTypeName, Valid AS Value FROM $db_output.dq_stg_validity
-- UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Other' AS MetricTypeName, Other AS Value FROM $db_output.dq_stg_validity
-- UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Default' AS MetricTypeName, Default AS Value FROM $db_output.dq_stg_validity
-- UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Invalid' AS MetricTypeName, Invalid AS Value FROM $db_output.dq_stg_validity
-- UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Missing' AS MetricTypeName, Missing AS Value FROM $db_output.dq_stg_validity;

-- DROP TABLE IF EXISTS $db_output.dq_stg_inventorylistsource;
-- CREATE TABLE $db_output.dq_stg_inventorylistsource
-- AS
-- SELECT
--     '$month_id' AS UniqMonthID,
--     im.DimensionTypeId,
--     im.MeasureId,
--     im.MeasureTypeId,
--     im.MetricTypeId,
--     il.OrgIDProv,
--     il.Value
--   FROM $db_output.dq_stg_inventoryList il
--   INNER JOIN $db_output.dq_vw_inventory_metadata im ON il.DimensionTypeId = im.DimensionTypeId
--   AND il.MeasureId = im.MeasureId
--   AND il.MetricTypeName = im.MetricTypeName
--   WHERE im.StartDate <= '$rp_startdate'
--   AND CASE
--         WHEN im.EndDate IS NULL THEN '$rp_enddate'
--        ELSE im.EndDate
--       END >= '$rp_enddate';

-- COMMAND ----------

-- DBTITLE 1,moved into VODIM_aggregation
-- %sql

-- -- Valid, Other, Default, Invalid, Missing  re-coding to get around an issue in PROD

-- MERGE INTO $db_output.dq_inventory AS target
-- USING $db_output.dq_stg_inventoryListSource AS source ON target.UniqMonthID = source.UniqMonthID
--   AND target.DimensionTypeId = source.DimensionTypeId
--   AND target.MeasureId = source.MeasureId
--   AND target.MeasureTypeId = source.MeasureTypeId
--   AND CASE
--         WHEN target.MetricTypeId IS NULL THEN 'null'
--         ELSE target.MetricTypeId
--         END = CASE
--                 WHEN source.MetricTypeId IS NULL THEN 'null'
--                 ELSE source.MetricTypeId
--               END
--   AND target.OrgIDProv = source.OrgIDProv
--   AND target.SOURCE_DB = '$dbm'
-- WHEN MATCHED
--   THEN UPDATE SET target.Value = source.Value
-- WHEN NOT MATCHED
--   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
--   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');

-- COMMAND ----------

-- DBTITLE 1,Valid, Other, Default, Invalid, Missing
-- Commented out as cuasing failures in PROD
-- %sql

-- WITH InventoryList
-- AS
-- (
--   SELECT       DimensionTypeId, MeasureId, OrgIDProv, 'Valid'   AS MetricTypeName, Valid   AS Value FROM $db_output.dq_stg_validity
--   UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Other'   AS MetricTypeName, Other   AS Value FROM $db_output.dq_stg_validity
--   UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Default' AS MetricTypeName, Default AS Value FROM $db_output.dq_stg_validity
--   UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Invalid' AS MetricTypeName, Invalid AS Value FROM $db_output.dq_stg_validity
--   UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Missing' AS MetricTypeName, Missing AS Value FROM $db_output.dq_stg_validity
-- )
-- MERGE INTO $db_output.dq_inventory AS target
-- USING
-- (
--   SELECT
--     '$month_id' AS UniqMonthID,
--     im.DimensionTypeId,
--     im.MeasureId,
--     im.MeasureTypeId,
--     im.MetricTypeId,
--     il.OrgIDProv,
--     il.Value
--   FROM InventoryList il
--   INNER JOIN $db_output.dq_vw_inventory_metadata im ON il.DimensionTypeId = im.DimensionTypeId
--   AND il.MeasureId = im.MeasureId
--   AND il.MetricTypeName = im.MetricTypeName
--   WHERE im.StartDate <= '$rp_startdate'
--   AND CASE
--         WHEN im.EndDate IS NULL THEN '$rp_enddate'
--        ELSE im.EndDate
--       END >= '$rp_enddate'
-- ) AS source ON target.UniqMonthID = source.UniqMonthID
-- AND target.DimensionTypeId = source.DimensionTypeId
-- AND target.MeasureId = source.MeasureId
-- AND target.MeasureTypeId = source.MeasureTypeId
-- AND CASE
--       WHEN target.MetricTypeId IS NULL THEN 'null'
--       ELSE target.MetricTypeId
--       END = CASE
--               WHEN source.MetricTypeId IS NULL THEN 'null'
--               ELSE source.MetricTypeId
--             END
-- AND target.OrgIDProv = source.OrgIDProv
-- WHEN MATCHED
--   THEN UPDATE SET target.Value = source.Value
-- WHEN NOT MATCHED
--   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value)
--   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value);