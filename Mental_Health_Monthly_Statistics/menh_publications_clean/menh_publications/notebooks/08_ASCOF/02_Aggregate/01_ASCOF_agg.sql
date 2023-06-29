-- Databricks notebook source
%md

# code from FYFV from menh_analysis (with changes)

The only metrics remaining are 
- AMH03e = DENOMINATOR
- AMH14e = 1H_NUMERATOR
- AMH14e% = 1H_OUTCOME
- AMH17e = 1F_NUMERATOR
- AMH17e% = 1F_OUTCOME

-- COMMAND ----------

%python
assert dbutils.widgets.get('db_output')
assert dbutils.widgets.get('db_source')
assert dbutils.widgets.get('month_id')
assert dbutils.widgets.get('rp_enddate')
assert dbutils.widgets.get('rp_startdate')
assert dbutils.widgets.get('status')

-- COMMAND ----------

%md

## DENOMINATOR

-- COMMAND ----------

-- DBTITLE 1,England 

INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep;

-- COMMAND ----------

-- DBTITLE 1,CASSR
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN");

-- COMMAND ----------

-- DBTITLE 1,Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider' AS BREAKDOWN,
			 OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
    GROUP BY OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR; Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR; Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Gender' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             Gender;

-- COMMAND ----------

-- DBTITLE 1,CASSR; Provider; Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider;Gender' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             OrgIDProv,
              b.name,
             Gender;

-- COMMAND ----------

-- DBTITLE 1,England; Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England;Gender' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep
    GROUP BY Gender;

-- COMMAND ----------

-- DBTITLE 1,Provider; Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider;Gender' AS BREAKDOWN,
			 OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH03e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
    GROUP BY OrgIDProv,
              b.name,
             Gender;

-- COMMAND ----------

%md

## 1H_NUMERATOR

-- COMMAND ----------

-- DBTITLE 1,England
%sql

INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y'
             AND RANK = '1';

-- COMMAND ----------

-- DBTITLE 1,CASSR
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN");

-- COMMAND ----------

-- DBTITLE 1,Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider' AS BREAKDOWN,
			 OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Gender' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             Gender;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider;Gender' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             OrgIDProv,
              b.name,
             Gender;

-- COMMAND ----------

-- DBTITLE 1,England;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England;Gender' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY Gender;

-- COMMAND ----------

-- DBTITLE 1,Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider;Gender' AS BREAKDOWN,
			 OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH14e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH13e_14e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
       WHERE SettledAccommodationInd = 'Y' 
             AND RANK = '1'
    GROUP BY OrgIDProv,
              b.name,
             Gender;

-- COMMAND ----------

%md

## 1H_OUTCOME

-- COMMAND ----------

-- DBTITLE 1,England
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'NONE' AS THIRD_LEVEL
			,'AMH14e%' AS METRIC
            ,(cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND SettledAccommodationInd = 'Y' 
            AND RANK = '1';

-- COMMAND ----------

-- DBTITLE 1,CASSR
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.CASSR = AMH03.CASSR
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN");

-- COMMAND ----------

-- DBTITLE 1,Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider' AS BREAKDOWN,
			 AMH03.OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.OrgIDProv = AMH03.OrgIDProv
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY AMH03.OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 AMH03.OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.CASSR = AMH03.CASSR
            AND AMH14.OrgIDProv = AMH03.OrgIDProv
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN"),
             AMH03.OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Gender' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.CASSR = AMH03.CASSR
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN"),
             AMH03.Gender;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider;Gender' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 AMH03.OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.CASSR = AMH03.CASSR
            AND AMH14.OrgIDProv = AMH03.OrgIDProv
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN"),
             AMH03.OrgIDProv,
              b.name,
             AMH03.Gender;

-- COMMAND ----------

-- DBTITLE 1,England;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England;Gender' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England'  AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY AMH03.Gender;

-- COMMAND ----------

-- DBTITLE 1,Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider;Gender' AS BREAKDOWN,
			 AMH03.OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH14e%' AS METRIC,
			 (cast(count(distinct AMH14.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH13e_14e_prep AS AMH14
            ON AMH14.Person_ID = AMH03.Person_ID
            AND AMH14.OrgIDProv = AMH03.OrgIDProv
            AND SettledAccommodationInd = 'Y'
            AND RANK = '1'
    GROUP BY AMH03.OrgIDProv,
              b.name,
             AMH03.Gender;

-- COMMAND ----------

%md

## 1F_NUMERATOR

-- COMMAND ----------

-- DBTITLE 1,England
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,             
		     '$status' AS STATUS,
			 'England' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1';

-- COMMAND ----------

-- DBTITLE 1,CASSR
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN");

-- COMMAND ----------

-- DBTITLE 1,Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider' AS BREAKDOWN,
			 OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE    
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Gender' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             Gender;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider;Gender' AS BREAKDOWN,
			 COALESCE(CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY COALESCE(CASSR,"UNKNOWN"), 
             COALESCE(CASSR_description,"UNKNOWN"),
             OrgIDProv,
              b.name,
             Gender;

-- COMMAND ----------

-- DBTITLE 1,England;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England;Gender' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY Gender;

-- COMMAND ----------

-- DBTITLE 1,Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider;Gender' AS BREAKDOWN,
			 OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             Gender AS THIRD_LEVEL,
			 'AMH17e' AS METRIC,
			 COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH16e_17e_prep a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE        
       WHERE (EmployStatus = '01' OR EmployStatus = '1') 
             AND RANK = '1'
    GROUP BY OrgIDProv,
              b.name,
              Gender;

-- COMMAND ----------

%md

## 1F_OUTCOME 

-- COMMAND ----------

-- DBTITLE 1,England
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END            
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE' AS SECONDARY_LEVEL
			,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'NONE' AS THIRD_LEVEL
			,'AMH17e%' AS METRIC
            ,(cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
      FROM  $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID        
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'

-- COMMAND ----------

-- DBTITLE 1,CASSR
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.CASSR = AMH03.CASSR                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN");

-- COMMAND ----------

-- DBTITLE 1,Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider' AS BREAKDOWN,
			 AMH03.OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.OrgIDProv = AMH03.OrgIDProv                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY AMH03.OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 AMH03.OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             'NONE' AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.CASSR = AMH03.CASSR            
            AND AMH16.OrgIDProv = AMH03.OrgIDProv                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN"),
             AMH03.OrgIDProv,
              b.name;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Gender' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.CASSR = AMH03.CASSR                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN"),
             AMH03.Gender;

-- COMMAND ----------

-- DBTITLE 1,CASSR;Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'CASSR;Provider;Gender' AS BREAKDOWN,
			 COALESCE(AMH03.CASSR,"UNKNOWN") AS PRIMARY_LEVEL,
			 COALESCE(AMH03.CASSR_description,"UNKNOWN") AS PRIMARY_LEVEL_DESCRIPTION,
			 AMH03.OrgIDProv AS SECONDARY_LEVEL,
			 b.name AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.CASSR = AMH03.CASSR            
            AND AMH16.OrgIDProv = AMH03.OrgIDProv                        
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY COALESCE(AMH03.CASSR,"UNKNOWN"), 
             COALESCE(AMH03.CASSR_description,"UNKNOWN"),
             AMH03.OrgIDProv,
              b.name,
             AMH03.Gender;

-- COMMAND ----------

-- DBTITLE 1,England;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'England;Gender' AS BREAKDOWN,
			 'England' AS PRIMARY_LEVEL,
			 'England' AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID               
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY AMH03.Gender;

-- COMMAND ----------

-- DBTITLE 1,Provider;Gender
INSERT INTO $db_output.ascof_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
		     '$status' AS STATUS,
			 'Provider;Gender' AS BREAKDOWN,
			 AMH03.OrgIDProv AS PRIMARY_LEVEL,
			 b.name AS PRIMARY_LEVEL_DESCRIPTION,
			 'NONE' AS SECONDARY_LEVEL,
			 'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
             AMH03.Gender AS THIRD_LEVEL,
			 'AMH17e%' AS METRIC,
            (cast(count(distinct AMH16.Person_ID) as INT) / cast(count(distinct AMH03.Person_ID) as INT))*100 AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
        FROM $db_output.AMH03e_prep AS AMH03
   LEFT JOIN $db_output.Provider_list b 
             on AMH03.orgidprov = b.ORG_CODE
 LEFT JOIN  $db_output.AMH16e_17e_prep AS AMH16
            ON AMH16.Person_ID = AMH03.Person_ID
            AND AMH16.OrgIDProv = AMH03.OrgIDProv                    
            AND (EmployStatus = '01' OR EmployStatus = '1') 
            AND RANK = '1'
    GROUP BY AMH03.OrgIDProv,
              b.name,
             AMH03.Gender;