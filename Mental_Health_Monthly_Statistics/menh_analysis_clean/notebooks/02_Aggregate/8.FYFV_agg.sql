-- Databricks notebook source
%python
assert dbutils.widgets.get('db_output')
assert dbutils.widgets.get('db_source')
assert dbutils.widgets.get('month_id')
assert dbutils.widgets.get('rp_enddate')
assert dbutils.widgets.get('rp_startdate_m1')
assert dbutils.widgets.get('status')

-- COMMAND ----------

%md 
Table 1

-- COMMAND ----------


INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH03e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CCG - GP Practice or Residence' AS BREAKDOWN,
			 IC_Rec_CCG AS PRIMARY_LEVEL,
			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH03e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep
    GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------


INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Commissioning Region' as BREAKDOWN,
			 Region_code AS PRIMARY_LEVEL,
			 Region_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH03e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep
    GROUP BY Region_code, Region_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'STP' AS BREAKDOWN,
			 STP_code AS PRIMARY_LEVEL,
			 STP_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH03e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep
    GROUP BY STP_code, STP_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH13e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CCG - GP Practice or Residence' AS BREAKDOWN,
			 IC_Rec_CCG AS PRIMARY_LEVEL,
			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH13e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
    GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Commissioning Region' as BREAKDOWN,
			 Region_code AS PRIMARY_LEVEL,
			 Region_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH13e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
    GROUP BY Region_code, Region_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'STP' AS BREAKDOWN,
			 STP_code AS PRIMARY_LEVEL,
			 STP_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH13e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
    GROUP BY STP_code, STP_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH13e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'CCG - GP Practice or Residence' AS BREAKDOWN
			,AMH03.IC_Rec_CCG AS PRIMARY_LEVEL
			,AMH03.NAME AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH13e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.IC_Rec_CCG = AMH03.IC_Rec_CCG
  GROUP BY  AMH03.IC_Rec_CCG, AMH03.NAME ;

-- COMMAND ----------

%sql

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'Commissioning Region' as BREAKDOWN
			,AMH03.Region_code AS PRIMARY_LEVEL
			,AMH03.Region_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH13e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.Region_code = AMH03.Region_code
  GROUP BY  AMH03.Region_code, AMH03.Region_description ;

-- COMMAND ----------

%sql

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'STP' AS BREAKDOWN
			,AMH03.STP_code AS PRIMARY_LEVEL
			,AMH03.STP_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH13e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.STP_code = AMH03.STP_code
  GROUP BY  AMH03.STP_code, AMH03.STP_description ;

-- COMMAND ----------

%sql

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH14e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y'
             AND RANK = '1';

-- COMMAND ----------

%sql

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'CCG - GP Practice or Residence' AS BREAKDOWN,
			 IC_Rec_CCG AS PRIMARY_LEVEL,
			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH14e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'Commissioning Region' as BREAKDOWN,
			 Region_code AS PRIMARY_LEVEL,
			 Region_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH14e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY Region_code, Region_description;

-- COMMAND ----------

%sql

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'STP' AS BREAKDOWN,
			 STP_code AS PRIMARY_LEVEL,
			 STP_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH14e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY STP_code, STP_description;
    

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH14e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND SettledAccommodationInd = 'Y' 
            AND RANK = '1';

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'CCG - GP Practice or Residence' AS BREAKDOWN
			,AMH03.IC_Rec_CCG AS PRIMARY_LEVEL
			,AMH03.NAME AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH14e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.IC_Rec_CCG = AMH03.IC_Rec_CCG
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
  GROUP BY  AMH03.IC_Rec_CCG, AMH03.NAME;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'Commissioning Region' as BREAKDOWN
			,AMH03.Region_code AS PRIMARY_LEVEL
			,AMH03.Region_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH14e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.Region_code = AMH03.Region_code
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
  GROUP BY  AMH03.Region_code, AMH03.Region_description ;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'STP' AS BREAKDOWN
			,AMH03.STP_code AS PRIMARY_LEVEL
			,AMH03.STP_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH14e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.STP_code = AMH03.STP_code
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
  GROUP BY  AMH03.STP_code, AMH03.STP_description ;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH16e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'CCG - GP Practice or Residence' AS BREAKDOWN,
			 IC_Rec_CCG AS PRIMARY_LEVEL,
			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH16e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
    GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'Commissioning Region' as BREAKDOWN,
			 Region_code AS PRIMARY_LEVEL,
			 Region_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH16e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
    GROUP BY Region_code, Region_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'STP' AS BREAKDOWN,
			 STP_code AS PRIMARY_LEVEL,
			 STP_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH16e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
    GROUP BY STP_code, STP_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH16e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'CCG - GP Practice or Residence' AS BREAKDOWN
			,AMH03.IC_Rec_CCG AS PRIMARY_LEVEL
			,AMH03.NAME AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH16e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.IC_Rec_CCG = AMH03.IC_Rec_CCG
  GROUP BY  AMH03.IC_Rec_CCG, AMH03.NAME;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'Commissioning Region' as BREAKDOWN
			,AMH03.Region_code AS PRIMARY_LEVEL
			,AMH03.Region_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH16e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.Region_code = AMH03.Region_code
  GROUP BY  AMH03.Region_code, AMH03.Region_description ;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'STP' AS BREAKDOWN
			,AMH03.STP_code AS PRIMARY_LEVEL
			,AMH03.STP_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH16e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.STP_code = AMH03.STP_code
  GROUP BY  AMH03.STP_code, AMH03.STP_description ;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH17e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1';

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'CCG - GP Practice or Residence' AS BREAKDOWN,
			 IC_Rec_CCG AS PRIMARY_LEVEL,
			 NAME AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH17e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'Commissioning Region' as BREAKDOWN,
			 Region_code AS PRIMARY_LEVEL,
			 Region_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH17e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY Region_code, Region_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'STP' AS BREAKDOWN,
			 STP_code AS PRIMARY_LEVEL,
			 STP_description AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
			 'AMH17e' AS METRIC,
			 CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY STP_code, STP_description;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH17e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID        
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'CCG - GP Practice or Residence' AS BREAKDOWN
			,AMH03.IC_Rec_CCG AS PRIMARY_LEVEL
			,AMH03.NAME AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH17e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.IC_Rec_CCG = AMH03.IC_Rec_CCG        
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
  GROUP BY  AMH03.IC_Rec_CCG, AMH03.NAME;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'Commissioning Region' as BREAKDOWN
			,AMH03.Region_code AS PRIMARY_LEVEL
			,AMH03.Region_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH17e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.Region_code = AMH03.Region_code                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
  GROUP BY  AMH03.Region_code, AMH03.Region_description ;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
    SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'STP' AS BREAKDOWN
			,AMH03.STP_code AS PRIMARY_LEVEL
			,AMH03.STP_description AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
			,'AMH17e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100	AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.STP_code = AMH03.STP_code                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
  GROUP BY  AMH03.STP_code, AMH03.STP_description ;

-- COMMAND ----------

%md
Table 2

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted

SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'England' AS BREAKDOWN,
       'England' AS PRIMARY_LEVEL,
       'England' AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
       'MHS69' AS METRIC,
       COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB
  FROM $db_output.CYPFinal_2nd_contact_Quarterly w;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'CCG - GP Practice or Residence' AS BREAKDOWN,
       CASE
         WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'UNKNOWN')
         ELSE COALESCE(ccg.IC_Rec_CCG, 'UNKNOWN') END AS PRIMARY_LEVEL,
       CASE
         WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.NAME, 'UNKNOWN')
         ELSE COALESCE(CCG_REF.NAME, 'UNKNOWN') END AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
       'MHS69' AS METRIC,
       COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB
  FROM $db_output.CYPFinal_2nd_contact_Quarterly w 
LEFT OUTER JOIN $db_output.MHS001_CCG_LATEST ccg
       ON w.Person_ID = ccg.Person_ID     
LEFT JOIN $db_output.RD_CCG_LATEST DFC_CCG 
       ON w.OrgIDComm = DFC_CCG.original_ORG_CODE
LEFT JOIN $db_output.RD_CCG_LATEST AS CCG_REF
       ON ccg.IC_Rec_CCG  = CCG_REF.original_ORG_CODE
GROUP BY CASE
         WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'UNKNOWN')
         ELSE COALESCE(ccg.IC_Rec_CCG, 'UNKNOWN') END,
       CASE
         WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.NAME, 'UNKNOWN')
             ELSE COALESCE(CCG_REF.NAME, 'UNKNOWN') END;

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,   
   '$status' as STATUS,
   'Commissioning Region' as BREAKDOWN,
   COALESCE(stp.Region_code, 'UNKNOWN') AS PRIMARY_LEVEL,
   COALESCE(stp.Region_description, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
   'MHS69' AS METRIC,
   COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
   '$db_source' AS SOURCE_DB
FROM $db_output.CYPFinal_2nd_contact_Quarterly w
   LEFT OUTER JOIN $db_output.MHS001_CCG_LATEST ccg 
   ON w.Person_ID = ccg.Person_ID 
   --created a static table in breakdowns for this - will need reviewing as and when
   LEFT JOIN $db_output.STP_Region_mapping_post_2020 stp ON
   CASE WHEN w.OrgIDProv = 'DFC' THEN w.OrgIDComm ELSE ccg.IC_Rec_CCG END = stp.CCG_code
GROUP BY COALESCE(stp.Region_code, 'UNKNOWN'),
   COALESCE(STP.Region_description, 'UNKNOWN');

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'STP' AS BREAKDOWN,
       COALESCE(stp.STP_code, 'UNKNOWN') AS PRIMARY_LEVEL,
       COALESCE(STP.STP_description, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
       'MHS69' AS METRIC,
       COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB
  FROM $db_output.CYPFinal_2nd_contact_Quarterly w
       LEFT OUTER JOIN $db_output.MHS001_CCG_LATEST ccg 
       ON w.Person_ID = ccg.Person_ID 
       --created a static table in breakdowns for this - will need reviewing as and when
       LEFT JOIN $db_output.STP_Region_mapping_post_2020 stp ON 
       CASE WHEN w.OrgIDProv = 'DFC' THEN w.OrgIDComm ELSE ccg.IC_Rec_CCG END = stp.CCG_code
GROUP BY COALESCE(stp.STP_code, 'UNKNOWN'),
       COALESCE(STP.STP_description, 'UNKNOWN');

-- COMMAND ----------

%md
Table 3

-- COMMAND ----------

/***** 1. BED DAYS *****/


/*** National Figures ***/
INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'England' AS BREAKDOWN,
       'England' AS PRIMARY_LEVEL,
       'England' AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
       'BED_DAYS' AS METRIC,
       SUM(BED_DAYS) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB
FROM global_temp.wardstay2

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'CCG - GP Practice or Residence' AS BREAKDOWN,
       COALESCE(B.ORG_CODE, A.IC_REC_CCG, 'UNKNOWN') as PRIMARY_LEVEL,
       COALESCE(B.NAME, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,  
       'BED_DAYS' AS METRIC,
       sum(BED_DAYS) as METRIC_VALUE,
       '$db_source' AS SOURCE_DB

FROM global_temp.wardstay2 as a
full outer join $db_output.RD_CCG_LATEST 
          as b
          on a.IC_Rec_CCG=b.original_ORG_CODE        
GROUP BY
       COALESCE(B.ORG_CODE, A.IC_REC_CCG, 'UNKNOWN'),
       COALESCE(B.NAME, 'UNKNOWN')

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'Commissioning Region' as BREAKDOWN,
        COALESCE(stp.Region_code, 'UNKNOWN') as PRIMARY_LEVEL,
        COALESCE(stp.Region_description, 'UNKNOWN') as PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION, 
       'BED_DAYS' AS METRIC,
       sum(BED_DAYS) as METRIC_VALUE,
       '$db_source' AS SOURCE_DB

FROM global_temp.wardstay2 as a

FULL OUTER JOIN $db_output.RD_CCG_LATEST 
        AS b
        ON a.IC_Rec_CCG=b.original_ORG_CODE 

LEFT JOIN $db_output.STP_Region_mapping_post_2020 
        AS stp 
        ON b.ORG_CODE = stp.CCG_code

GROUP BY
COALESCE(stp.Region_code, 'UNKNOWN'),
COALESCE(stp.Region_description, 'UNKNOWN')

-- COMMAND ----------

--STP
INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'STP' AS BREAKDOWN,
       COALESCE(stp.STP_code, 'UNKNOWN') AS PRIMARY_LEVEL,
       COALESCE(stp.STP_description, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION, 
       'BED_DAYS' AS METRIC,
       sum(BED_DAYS) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB

FROM global_temp.wardstay2 
        AS a

FULL OUTER JOIN $db_output.RD_CCG_LATEST 
        AS b
        ON a.IC_Rec_CCG=b.original_ORG_CODE 

LEFT JOIN $db_output.STP_Region_mapping_post_2020 
        AS stp 
        ON b.ORG_CODE = stp.CCG_code
        
GROUP BY
COALESCE(stp.STP_code, 'UNKNOWN'),
COALESCE(stp.STP_description, 'UNKNOWN')

-- COMMAND ----------

/*** National Figures ***/
 
INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'England' AS BREAKDOWN,
       'England' AS PRIMARY_LEVEL,
       'England' AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
       'CYP_ADULT_WARDS' AS METRIC,
       COUNT (DISTINCT Person_ID) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB
FROM global_temp.wardstay2

-- COMMAND ----------

INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'CCG - GP Practice or Residence' AS BREAKDOWN,
       COALESCE(B.ORG_CODE, A.IC_REC_CCG, 'UNKNOWN') as PRIMARY_LEVEL,
       COALESCE(B.NAME, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,  
       'CYP_ADULT_WARDS' AS METRIC,
       sum(METRIC_VALUE) as METRIC_VALUE,
       '$db_source' AS SOURCE_DB

FROM global_temp.CCG_CYP 
        AS a

FULL OUTER JOIN $db_output.RD_CCG_LATEST 
        AS b
        ON a.IC_Rec_CCG=b.original_ORG_CODE 

GROUP BY
      COALESCE(B.ORG_CODE, A.IC_REC_CCG, 'UNKNOWN'),
      COALESCE(B.NAME, 'UNKNOWN')

-- COMMAND ----------

---- REGION
INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,       
       '$status' AS STATUS,
       'Commissioning Region' as BREAKDOWN,
        COALESCE(A.Region_code, 'UNKNOWN') as PRIMARY_LEVEL,
        COALESCE(A.Region_description, 'UNKNOWN') as PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION, 
       'CYP_ADULT_WARDS' AS METRIC,
       sum(METRIC_VALUE) as METRIC_VALUE,
       '$db_source' AS SOURCE_DB

FROM 
(SELECT DISTINCT Region_code,Region_description  FROM $db_output.STP_Region_mapping_post_2020) 
    AS a
    
FULL JOIN global_temp.Region_CYP 
    AS reg 
    ON a.Region_code = reg.Region_code

GROUP BY
      COALESCE(A.Region_code, 'UNKNOWN'),
      COALESCE(A.Region_description, 'UNKNOWN')

-- COMMAND ----------

---- STP
INSERT INTO $db_output.FYFV_unformatted
SELECT '$rp_startdate_m1' AS REPORTING_PERIOD_START,
       '$rp_enddate' AS REPORTING_PERIOD_END,     
       '$status' AS STATUS,
       'STP' AS BREAKDOWN,
       COALESCE(A.STP_code, 'UNKNOWN') AS PRIMARY_LEVEL,
       COALESCE(A.STP_description, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
       'NONE' AS SECONDARY_LEVEL,
       'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
       'CYP_ADULT_WARDS' AS METRIC,
       sum(METRIC_VALUE) AS METRIC_VALUE,
       '$db_source' AS SOURCE_DB

from 
(SELECT DISTINCT STP_code,STP_description  FROM $db_output.STP_Region_mapping_post_2020) 
    AS a

FULL JOIN global_temp.STP_CYP 
    AS stp 
    ON a.STP_code = stp.STP_code
    
GROUP BY
COALESCE(a.STP_code, 'UNKNOWN'),
COALESCE(A.STP_description, 'UNKNOWN')