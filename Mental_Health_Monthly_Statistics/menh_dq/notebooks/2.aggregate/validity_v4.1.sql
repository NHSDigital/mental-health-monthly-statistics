-- Databricks notebook source
 %md

 User note: 16-10-2019 

 Replaced all instances of database dms. with $db_output.

 PLUS changed reference_data. to $reference_data to enable changes to test database when testing.

-- COMMAND ----------

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
 reference_data  = dbutils.widgets.get("reference_data")
 print(reference_data)
 assert reference_data
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
                  FROM $reference_data.ods_practice_v02
                 ) o ON m.GMPCodeReg = o.CODE AND o.RowNUmber = 1
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
        WHEN upper(GMPCodeReg) NOT IN ('V81999', 'V81998', 'V81997')
        AND (CLOSE_DATE > '$rp_enddate' OR CLOSE_DATE IS NULL) and OPEN_DATE <= '$rp_enddate' THEN 1
        ELSE 0
      END) AS Valid,
  SUM(CASE
        WHEN GMPCodeReg IN (CODE) 
        AND upper(GMPCodeReg) NOT IN ('V81999', 'V81998', 'V81997')
        AND OPEN_DATE <= '$rp_enddate'
        AND CLOSE_DATE >= date_add('$rp_startdate', -1)
        AND CLOSE_DATE <= '$rp_enddate'
        THEN 1
        ELSE 0
      END) AS Other,
  SUM(CASE
        WHEN upper(GMPCodeReg) IN ('V81999', 'V81998', 'V81997') THEN 1
        ELSE 0
      END) AS Default,
  SUM(CASE
        WHEN (upper(GMPCodeReg) NOT IN ('V81999', 'V81998', 'V81997') AND CODE IS NULL)
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
         WHEN TreatFuncCodeMH IN ('319', '700', '710', '711', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN TreatFuncCodeMH NOT IN ('319', '700', '710', '711', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727')
         AND LTRIM(RTRIM(TreatFuncCodeMH)) <> ''
         AND TreatFuncCodeMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(TreatFuncCodeMH)) = ''
         OR TreatFuncCodeMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs503assignedcareprof
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Organisation Site Identifier (Of Treatment)
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
 FROM $reference_data.org_daily /* $db_output.dq_vw_org_daily */
 WHERE BUSINESS_START_DATE <= '$rp_enddate'
 AND (BUSINESS_END_DATE > '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
 ) o ON m.SiteIDOfTreat = o.org_CODE
        AND o.RowNumber = 1 
        AND (BUSINESS_START_DATE <= '$rp_enddate' OR SiteIDOfTreat IS NULL)
        AND o.BUSINESS_START_DATE <= '$rp_enddate'
        WHERE UniqMonthID = $month_id 
  ), vodimOutput AS
  (SELECT
   --'Organisation Site Identifier (Of Treatment)' AS MeasureName,
   3 as DimensionTypeId,
   9 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
    SUM(CASE
          WHEN ORG_CODE IS NOT NULL
          AND (BUSINESS_END_DATE > '$rp_enddate' OR BUSINESS_END_DATE IS NULL) THEN 1
          ELSE 0
        END) AS Valid,
    SUM(CASE
          WHEN ORG_CODE IS NOT NULL
          AND BUSINESS_END_DATE >= date_add('$rp_startdate', -1)
          AND BUSINESS_END_DATE <= '$rp_enddate' THEN 1
          ELSE 0
        END) AS Other,
    0 AS Default,
    SUM(CASE
          WHEN (ORG_CODE IS NULL OR (ORG_CODE IS NOT NULL AND BUSINESS_END_DATE <= date_add('$rp_startdate', -1)))
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

-- DBTITLE 1,Primary Reason for Referral (Mental Health)
 %sql
 -- User note changed to exclude all records where MHS102ServiceTypeReferredTo.ReferRejectReason = '02' 

 INSERT INTO $db_output.dq_stg_validity
 SELECT 
   --Primary Reason for Referral (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   10 as MeasureId,
    ref.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN PrimReasonReferralMH IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN PrimReasonReferralMH NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27') THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(PrimReasonReferralMH)) = ''
         OR PrimReasonReferralMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral ref
 LEFT JOIN $dbm.MHS102ServiceTypeReferredTo rej
 on ref.ServiceRequestId = rej.ServiceRequestId and rej.uniqmonthID = ref.UniqMonthID and ref.UniqServReqID = rej.UniqServReqID
 WHERE ref.UniqMonthID = $month_id
 AND IFNULL(rej.ReferRejectReason, 'xx') <> '02'
 GROUP BY ref.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,Care Professional Service or Team Type Associate (Mental Health)
 %sql

 /** User note updated codes for CareProfServOrTeamTypeAssoc for v4.1 **/

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Care Professional Service or Team Type Association (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   11 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,


 SUM(CASE  WHEN upper(CareProfServOrTeamTypeAssoc) IN ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15','A16','A17','A18','A19','A20', 'A21','A22','A23','A24','A25','B01','B02','C01','C02','C04','C05','C06','C07','C08', 'C10','D01','D02','D03','D04','D05','D06','D07','D08','E01','E02','E03','E04') THEN 1

         ELSE 034
      END) AS Valid, 
       
   SUM(CASE
         WHEN upper(CareProfServOrTeamTypeAssoc) IN ('Z01', 'Z02') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN upper(CareProfServOrTeamTypeAssoc) NOT IN ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15','A16','A17','A18','A19','A20', 'A21','A22','A23','A24','A25','B01','B02','C01','C02','C04','C05','C06','C07','C08','C10','D01','D02','D03','D04','D05','D06','D07','D08','E01','E02','E03','E04')
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
 FROM $dbm.mhs006mhcarecoord
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Organisation Identifier (Of Commissioner)
 %python

 month_id = dbutils.widgets.get("month_id")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 dbm = dbutils.widgets.get("dbm")
 db_output = dbutils.widgets.get("db_output")
 reference_data = dbutils.widgets.get("reference_data")

 ## User note: this should probably be changed to pass parameters via a loop into the validity_commissioner notebook - currently rather over-simplified...

 params12 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 12, "DbTable": "MHS101Referral"}
 params13 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 13, "DbTable": "MHS201CareContact"}
 params14 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 14, "DbTable": "MHS204IndirectActivity"}         
 params15 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 15, "DbTable": "MHS301GroupSession"}
 params16 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 16, "DbTable": "MHS512HospSpellComm"}    
 params17 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 17, "DbTable": "MHS608AnonSelfAssess"}

 print(params12)
 print(params13)
 print(params14)
 print(params15)
 print(params16)
 print(params17)
             
 # Debugging
 # month_id = 1417
 # rp_startdate = "2018-04-01"
 # rp_enddate = "2018-04-30"
 # dbm = "mhsds_database"

 # This calculates the 6 DQ measures to do with "Organisation Identifier (Of Commissioner)", ie MHS-DQM12 - MHS-DQM17. 
 dbutils.notebook.run("validity_commissioner", 0, params12)
 dbutils.notebook.run("validity_commissioner", 0, params13)
 dbutils.notebook.run("validity_commissioner", 0, params14)
 dbutils.notebook.run("validity_commissioner", 0, params15)
 dbutils.notebook.run("validity_commissioner", 0, params16)
 dbutils.notebook.run("validity_commissioner", 0, params17)

-- COMMAND ----------

-- DBTITLE 1,Organisation Identifier (Of  Responsible Commissioner)
 %python

 month_id = dbutils.widgets.get("month_id")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 dbm = dbutils.widgets.get("dbm")
 db_output = dbutils.widgets.get("db_output")
 reference_data = dbutils.widgets.get("reference_data")

 ## User note: this should probably be changed to pass parameters via a loop into the validity_commissioner notebook - currently rather over-simplified...

 params12 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 12, "DbTable": "MHS101Referral"}
 params13 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 13, "DbTable": "MHS201CareContact"}
 params14 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 14, "DbTable": "MHS204IndirectActivity"}         
 params15 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 15, "DbTable": "MHS301GroupSession"}
 params16 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 16, "DbTable": "MHS512HospSpellCommAssPer"}    
 params17 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 17, "DbTable": "MHS517MHExceptionalPackOfCare"}
 params18 = {'rp_enddate' : rp_enddate, 'rp_startdate' : rp_startdate, 'month_id' : month_id, 'reference_data' : reference_data, 'db_output' : db_output, 'dbm' : dbm, "MeasureId": 18, "DbTable": "MHS608AnonSelfAssess"}


 print(params12)
 print(params13)
 print(params14)
 print(params15)
 print(params16)
 print(params17)
 print(params18)
             
 # Debugging
 # month_id = 1417
 # rp_startdate = "2018-04-01"
 # rp_enddate = "2018-04-30"
 # dbm = "mhsds_database"

 # This calculates the 6 DQ measures to do with "Organisation Identifier (Of Commissioner)", ie MHS-DQM12 - MHS-DQM17. 
 dbutils.notebook.run("validity_responsible_commissioner", 0, params12)
 dbutils.notebook.run("validity_responsible_commissioner", 0, params13)
 dbutils.notebook.run("validity_responsible_commissioner", 0, params14)
 dbutils.notebook.run("validity_responsible_commissioner", 0, params15)
 dbutils.notebook.run("validity_responsible_commissioner", 0, params16)
 dbutils.notebook.run("validity_responsible_commissioner", 0, params17)
 dbutils.notebook.run("validity_responsible_commissioner", 0, params18)

-- COMMAND ----------

-- DBTITLE 1,Service Or Team Type Referred To (Mental Health)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Service Or Team Type Referred To (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   18 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   
   SUM(CASE WHEN upper(ServTeamTypeRefToMH) IN ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15','A16','A17','A18','A19','A20',      'A21','A22','A23','A24','A25','B01','B02','C01','C02','C04','C05','C06','C07','C08','C10','D01','D02','D03','D04','D05','D06','D07','D08','E01','E02','E03','E04') THEN 1
         
         ELSE 0
       END) AS Valid,
   SUM(CASE
         WHEN upper(ServTeamTypeRefToMH) IN ('Z01', 'Z02') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN upper(ServTeamTypeRefToMH) NOT IN ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15','A16','A17','A18','A19','A20', 'A21','A22','A23','A24','A25','B01','B02','C01','C02','C04','C05','C06','C07','C08','C10','D01','D02','D03','D04','D05','D06','D07','D08','E01','E02','E03','E04')
         AND upper(ServTeamTypeRefToMH) NOT IN ('Z01', 'Z02')
         AND LTRIM(RTRIM(ServTeamTypeRefToMH)) <> ''
         AND ServTeamTypeRefToMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ServTeamTypeRefToMH)) = ''
         OR ServTeamTypeRefToMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs102servicetypereferredto
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Primary Reason for Referral (Mental Health) (Referral received on or after 1st Jan 2016)
 %sql

 --User note: changed to exclude all records where MHS102ServiceTypeReferredTo.ReferRejectReason = '02'

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Primary Reason for Referral (Mental Health) (Referral received on or after 1st Jan 2016)' AS MeasureName,
   3 as DimensionTypeId,
   19 as MeasureId,
   ref.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN PrimReasonReferralMH IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27') THEN 1  
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,

   SUM(CASE
       WHEN PrimReasonReferralMH NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27') --28
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
 LEFT JOIN $dbm.MHS102ServiceTypeReferredTo rej
 on ref.ServiceRequestId = rej.ServiceRequestId and rej.uniqmonthID = ref.UniqMonthID and ref.UniqServReqID = rej.UniqServReqID
 WHERE ref.UniqMonthID = $month_id
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
         WHEN ClinRespPriorityType IN ('1', '2', '3') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ClinRespPriorityType NOT IN ('1', '2', '3')
         AND LTRIM(RTRIM(ClinRespPriorityType)) <> ''
         AND ClinRespPriorityType IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ClinRespPriorityType)) = ''
         OR ClinRespPriorityType IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral
 WHERE UniqMonthID = $month_id
 AND PrimReasonReferralMH = '12'
 AND AgeServReferRecDate < 19
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Hospital Bed Type (Mental Health) (Ward stays started on or after 1st April 2017)
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Hospital Bed Type (Mental Health)' AS MeasureName,
   3 as DimensionTypeId,
   31 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN HospitalBedTypeMH IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN HospitalBedTypeMH NOT IN ('10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34')
         AND LTRIM(RTRIM(HospitalBedTypeMH)) <> ''
         AND HospitalBedTypeMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(HospitalBedTypeMH)) = ''
         OR HospitalBedTypeMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs502wardstay
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
         WHEN upper(SourceOfReferralMH) IN ('A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'C1', 'C2', 'C3', 'D1', 'D2', 'E1', 'E2', 'E3', 'E4', 'E5', 'F1', 'F2', 'F3', 'G1', 'G2', 'G3', 'G4', 'H1', 'H2', 'I1', 'I2', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'N3', 'P1') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN upper(SourceOfReferralMH) NOT IN ('A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'C1', 'C2', 'C3', 'D1', 'D2', 'E1', 'E2', 'E3', 'E4', 'E5', 'F1', 'F2', 'F3', 'G1', 'G2', 'G3', 'G4', 'H1', 'H2', 'I1', 'I2', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'N3', 'P1')
         AND LTRIM(RTRIM(SourceOfReferralMH)) <> ''
         AND SourceOfReferralMH IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(SourceOfReferralMH)) = ''
         OR SourceOfReferralMH IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs101referral
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Consultation medium used
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Consultation medium used' AS MeasureName,
   3 as DimensionTypeId,
   35 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN ConsMediumUsed IN ('01', '02', '03', '04', '05', '06') THEN 1
         ELSE 0
       END) AS Valid,
   SUM(CASE
         WHEN ConsMediumUsed IN ('98') THEN 1
         ELSE 0
       END) AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ConsMediumUsed NOT IN ('01', '02', '03', '04', '05', '06')
         AND ConsMediumUsed NOT IN ('98')
         AND LTRIM(RTRIM(ConsMediumUsed)) <> ''
         AND ConsMediumUsed IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(ConsMediumUsed)) = ''
         OR ConsMediumUsed IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs201carecontact
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

-- DBTITLE 1,Delayed discharge reason
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Delayed discharge reason' AS MeasureName,
   3 as DimensionTypeId,
   38 as MeasureId,
   OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN upper(DelayDischReason) IN ('A2', 'B1', 'C1', 'D1', 'D2', 'E1', 'F2', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'G10', 'G11', 'G12', 'H1', 'I2', 'I3', 'J2', 'K2', 'L1', 'M1', 'N1') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN upper(DelayDischReason) NOT IN ('A2', 'B1', 'C1', 'D1', 'D2', 'E1', 'F2', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'G10', 'G11', 'G12', 'H1', 'I2', 'I3', 'J2', 'K2', 'L1', 'M1', 'N1')
         AND LTRIM(RTRIM(DelayDischReason)) <> ''
         AND DelayDischReason IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(DelayDischReason)) = ''
         OR DelayDischReason IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs504delayeddischarge
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge attributable to
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

-- DBTITLE 1,Provisional Diagnosis data
 %sql

 INSERT INTO $db_output.dq_stg_validity
 SELECT
   --'Provisional Diagnosis data' AS MeasureName,
   3 as DimensionTypeId,
   45 as MeasureId,
   m.OrgIDProv,
   COUNT(*) AS Denominator,
   SUM(CASE
         WHEN (m.ProvDiagDate BETWEEN r.ReferralRequestReceivedDate AND r.ServDischDate
         AND m.ProvDiagDate IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (m.ProvDiagDate >= r.ReferralRequestReceivedDate
         AND m.ProvDiagDate IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((m.ProvDiagDate < r.ReferralRequestReceivedDate OR m.ProvDiagDate > r.ServDischDate)
         AND m.ProvDiagDate IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (m.ProvDiagDate < r.ReferralRequestReceivedDate
         AND m.ProvDiagDate IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN m.ProvDiagDate IS NULL THEN 1
         ELSE 0
       END) Missing
 FROM $dbm.mhs603provdiag m
 INNER JOIN $dbm.mhs101referral r ON m.UniqServReqID = r.UniqServReqID
 AND m.Person_ID = r.Person_ID --UniqMHSDSPersID has been changed to Person_ID in Databricks.
 WHERE m.UniqMonthID = $month_id 
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
         WHEN (m.DiagDate BETWEEN
         r.ReferralRequestReceivedDate
         AND
         r.ServDischDate
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (m.DiagDate >= r.ReferralRequestReceivedDate
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((m.DiagDate < r.ReferralRequestReceivedDate OR m.DiagDate > r.ServDischDate)
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (m.DiagDate < r.ReferralRequestReceivedDate
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN m.DiagDate IS NULL THEN 1
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
         WHEN (m.DiagDate BETWEEN
         r.ReferralRequestReceivedDate
         AND
         r.ServDischDate
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (m.DiagDate >= r.ReferralRequestReceivedDate
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NULL) THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN ((m.DiagDate < r.ReferralRequestReceivedDate OR m.DiagDate > r.ServDischDate)
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NOT NULL)
         OR (m.DiagDate < r.ReferralRequestReceivedDate
         AND m.DiagDate IS NOT NULL
         AND r.ServDischDate IS NULL)THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN m.DiagDate IS NULL THEN 1
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
         WHEN AttendOrDNACode IN ('2','3','4','5','6','7') THEN 1
         ELSE 0
       END) AS Valid,
   0 AS Other,
   0 AS Default,
   SUM(CASE
         WHEN AttendOrDNACode NOT IN ('2','3','4','5','6','7')
         AND LTRIM(RTRIM(AttendOrDNACode)) <> ''
         AND AttendOrDNACode IS NOT NULL THEN 1
         ELSE 0
       END) AS Invalid,
   SUM(CASE
         WHEN LTRIM(RTRIM(AttendOrDNACode)) = ''
         OR AttendOrDNACode IS NULL THEN 1
         ELSE 0
       END) AS Missing
 FROM $dbm.mhs201carecontact
 WHERE UniqMonthID = $month_id
 GROUP BY OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Referral closure reason
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
 FROM $dbm.mhs102servicetypereferredto
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
 WHERE m.UniqMonthID = $month_id
 AND m.SpecialisedMHServiceCode IS NOT NULL
 GROUP BY m.OrgIDProv;

-- COMMAND ----------

-- DBTITLE 1,Delayed discharge; Local Authority of responsibility
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
 left outer join $reference_data.org_daily org
 on disch.OrgIDRespLADelayDisch = org.org_code 
 and (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1) 
                 AND ORG_TYPE_CODE = 'LA'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
 where disch.UniqMonthID = $month_id
 group by disch.OrgIdProv


-- COMMAND ----------

-- DBTITLE 1,Denominator
 %sql

 WITH InventoryList
 AS
 (
   SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Denominator' AS MeasureTypeName, Denominator AS Value FROM $db_output.dq_stg_validity
 )
 MERGE INTO $db_output.dq_inventory AS target
 USING
 (
   SELECT
     '$month_id' AS UniqMonthID,
     im.DimensionTypeId,
     im.MeasureId,
     im.MeasureTypeId,
     im.MetricTypeId,
     il.OrgIDProv,
     il.Value
   FROM InventoryList il
   INNER JOIN $db_output.dq_vw_inventory_metadata im   on il.DimensionTypeId = im.DimensionTypeId
   AND il.MeasureId = im.MeasureId
   AND il.MeasureTypeName = im.MeasureTypeName
   WHERE im.StartDate <= '$rp_startdate'
   AND CASE
         WHEN im.EndDate IS NULL THEN '$rp_enddate'
        ELSE im.EndDate
       END >= '$rp_enddate'
 ) AS source ON target.UniqMonthID = source.UniqMonthID
 AND target.DimensionTypeId = source.DimensionTypeId
 AND target.MeasureId = source.MeasureId
 AND target.MeasureTypeId = source.MeasureTypeId
 AND CASE
       WHEN target.MetricTypeId IS NULL THEN 'null'
       ELSE target.MetricTypeId
       END = CASE
               WHEN source.MetricTypeId IS NULL THEN 'null'
               ELSE source.MetricTypeId
             END
 AND target.OrgIDProv = source.OrgIDProv
 AND target.SOURCE_DB = '$dbm'
 WHEN MATCHED
   THEN UPDATE SET target.Value = source.Value
 WHEN NOT MATCHED
   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');

-- COMMAND ----------

 %sql

 DROP TABLE IF EXISTS $db_output.dq_stg_inventoryList;

 CREATE TABLE $db_output.dq_stg_inventoryList
 AS 
 SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Valid' AS MetricTypeName, Valid AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Other' AS MetricTypeName, Other AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Default' AS MetricTypeName, Default AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Invalid' AS MetricTypeName, Invalid AS Value FROM $db_output.dq_stg_validity
 UNION SELECT DimensionTypeId, MeasureId, OrgIDProv, 'Missing' AS MetricTypeName, Missing AS Value FROM $db_output.dq_stg_validity;

 DROP TABLE IF EXISTS $db_output.dq_stg_inventoryListSource;

 CREATE TABLE $db_output.dq_stg_inventoryListSource
 AS
 SELECT
     '$month_id' AS UniqMonthID,
     im.DimensionTypeId,
     im.MeasureId,
     im.MeasureTypeId,
     im.MetricTypeId,
     il.OrgIDProv,
     il.Value
   FROM $db_output.dq_stg_inventoryList il
   INNER JOIN $db_output.dq_vw_inventory_metadata im ON il.DimensionTypeId = im.DimensionTypeId
   AND il.MeasureId = im.MeasureId
   AND il.MetricTypeName = im.MetricTypeName
   WHERE im.StartDate <= '$rp_startdate'
   AND CASE
         WHEN im.EndDate IS NULL THEN '$rp_enddate'
        ELSE im.EndDate
       END >= '$rp_enddate';

-- COMMAND ----------

 %sql

 -- Valid, Other, Default, Invalid, Missing  re-coding to get around an issue in PROD

 MERGE INTO $db_output.dq_inventory AS target
 USING $db_output.dq_stg_inventoryListSource AS source ON target.UniqMonthID = source.UniqMonthID
   AND target.DimensionTypeId = source.DimensionTypeId
   AND target.MeasureId = source.MeasureId
   AND target.MeasureTypeId = source.MeasureTypeId
   AND CASE
         WHEN target.MetricTypeId IS NULL THEN 'null'
         ELSE target.MetricTypeId
         END = CASE
                 WHEN source.MetricTypeId IS NULL THEN 'null'
                 ELSE source.MetricTypeId
               END
   AND target.OrgIDProv = source.OrgIDProv
   AND target.SOURCE_DB = '$dbm'
 WHEN MATCHED
   THEN UPDATE SET target.Value = source.Value
 WHEN NOT MATCHED
   THEN INSERT (UniqMonthID, DimensionTypeId, MeasureId, MeasureTypeId, MetricTypeId, OrgIDProv, Value, SOURCE_DB)
   VALUES (source.UniqMonthID, source.DimensionTypeId, source.MeasureId, source.MeasureTypeId, source.MetricTypeId, source.OrgIDProv, source.Value, '$dbm');

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