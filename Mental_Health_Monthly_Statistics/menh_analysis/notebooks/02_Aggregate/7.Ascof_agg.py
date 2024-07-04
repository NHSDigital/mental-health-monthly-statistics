# Databricks notebook source
# DBTITLE 1,1F numerator: National
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			'England' AS BREAKDOWN,
 			'England' AS LEVEL_ONE,
 			'England' AS LEVEL_ONE_DESCRIPTION,
 			'NONE' AS LEVEL_TWO,
 			'NONE' AS LEVEL_TWO_DESCRIPTION,
             'NONE' AS LEVEL_THREE,
 			'1F_NUMERATOR' AS METRIC,
             CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH17_prep
       WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 		    AND EmployStatusRecDate <= '${rp_enddate}';

# COMMAND ----------

# DBTITLE 1,1F numerator: National gender
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			'England;Gender' AS BREAKDOWN,
 			'England' AS  LEVEL_ONE,
 			'England' AS LEVEL_ONE_DESCRIPTION,
 			'NONE' AS LEVEL_TWO,
 			'NONE' AS LEVEL_TWO_DESCRIPTION,
              GENDER AS LEVEL_THREE,
 			'1F_NUMERATOR' AS METRIC,
             CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			 AND EmployStatusRecDate <= '${rp_enddate}'
     GROUP BY GENDER;

# COMMAND ----------

# DBTITLE 1,1F numerator: Provider
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS LEVEL_ONE
 			,b.name AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,'NONE' AS LEVEL_THREE
 			,'1F_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep_prov a
    LEFT JOIN $db_output.Provider_list b 
              on a.orgidprov = b.ORG_CODE
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			AND EmployStatusRecDate <= '${rp_enddate}'
       GROUP BY OrgIDProv
                ,b.name;

# COMMAND ----------

# DBTITLE 1,1F numerator: Provider gender
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider;Gender' AS BREAKDOWN
 			, OrgIDProv AS LEVEL_ONE
 			,b.name AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,GENDER AS LEVEL_THREE
 			,'1F_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep_prov a
    LEFT JOIN $db_output.Provider_list b 
              on a.orgidprov = b.ORG_CODE
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 			 AND EmployStatusRecDate <= '${rp_enddate}'
     GROUP BY OrgIDProv
              ,b.name
              ,GENDER;

# COMMAND ----------

# DBTITLE 1,1F numerator: CASSR
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,'NONE' AS LEVEL_THREE
 			,'1F_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep 
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 	 		 AND EmployStatusRecDate <= '${rp_enddate}'
     GROUP BY COALESCE(CASSR,"UNKNOWN")
 			,COALESCE(CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,1F numerator: CASSR gender
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR;Gender' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,GENDER AS LEVEL_THREE
 			,'1F_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep
        WHERE EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 	 		 AND EmployStatusRecDate <= '${rp_enddate}'
     GROUP BY COALESCE(CASSR,"UNKNOWN")
 			,COALESCE(CASSR_description,"UNKNOWN")
             ,GENDER;

# COMMAND ----------

# DBTITLE 1,1F numerator: CASSR Provider
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR;Provider' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,OrgIDProv AS LEVEL_TWO
 			,b.name AS LEVEL_TWO_DESCRIPTION
             ,'NONE' AS LEVEL_THREE
 			,'1F_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep AS C
   INNER JOIN $db_output.AMH17_prep_prov AS P
              ON C.Person_ID = P.Person_ID
              AND C.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
  	  		 AND C.EmployStatusRecDate <= '${rp_enddate}'
              AND P.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 		 	 AND P.EmployStatusRecDate <= '${rp_enddate}'
    LEFT JOIN $db_output.Provider_list b 
              on p.orgidprov = b.ORG_CODE
     GROUP BY COALESCE(CASSR,"UNKNOWN")
 			,COALESCE(CASSR_description,"UNKNOWN") 
             ,OrgIDProv
             ,b.name;

# COMMAND ----------

# DBTITLE 1,1F numerator: CASSR Provider gender
 %sql
 --Further breakdown of AMH17 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR;Provider;Gender' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,OrgIDProv AS LEVEL_TWO
 			,b.name AS LEVEL_TWO_DESCRIPTION
             ,c.GENDER AS LEVEL_THREE
 			,'1F_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH17_prep AS C
   INNER JOIN $db_output.AMH17_prep_prov AS P
              ON C.Person_ID = P.Person_ID
              AND C.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
  	  		 AND C.EmployStatusRecDate <= '${rp_enddate}'
              AND P.EmployStatusRecDate >= DATE_ADD(ADD_MONTHS('${rp_enddate}', -12),1)
 		 	 AND P.EmployStatusRecDate <= '${rp_enddate}'
    LEFT JOIN $db_output.Provider_list b 
              on p.orgidprov = b.ORG_CODE
     GROUP BY COALESCE(CASSR,"UNKNOWN")
 			,COALESCE(CASSR_description,"UNKNOWN") 
             ,OrgIDProv
             ,b.name
             ,c.GENDER;

# COMMAND ----------

# DBTITLE 1,1H numerator: National
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			'England' AS BREAKDOWN,
 			'England' AS LEVEL_ONE,
 			'England' AS LEVEL_ONE_DESCRIPTION,
 			'NONE' AS LEVEL_TWO,
 			'NONE' AS LEVEL_TWO_DESCRIPTION,
             'NONE' AS LEVEL_THREE,
 			'1H_NUMERATOR' AS METRIC,
             CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
         FROM $db_output.AMH14_prep;

# COMMAND ----------

# DBTITLE 1,1H numerator: National gender
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			'England;Gender' AS BREAKDOWN,
 			'England' AS LEVEL_ONE,
 			'England' AS LEVEL_ONE_DESCRIPTION,
 			'NONE' AS LEVEL_TWO,
 			'NONE' AS LEVEL_TWO_DESCRIPTION,
             GENDER AS LEVEL_THREE,
 			'1H_NUMERATOR' AS METRIC,
             CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE,
             '$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH14_prep
    GROUP BY GENDER;

# COMMAND ----------

# DBTITLE 1,1H numerator: Provider
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider' AS BREAKDOWN
 			,OrgIDProv AS LEVEL_ONE
             ,b.name AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,'NONE' AS LEVEL_THREE
 			,'1H_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH14_prep_prov a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
    GROUP BY OrgIDProv
             ,b.name;    

# COMMAND ----------

# DBTITLE 1,1H numerator: Provider gender
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'Provider;Gender' AS BREAKDOWN
 			,OrgIDProv AS LEVEL_ONE
 			,b.name AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,GENDER AS LEVEL_THREE
 			,'1H_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM $db_output.AMH14_prep_prov a
   LEFT JOIN $db_output.Provider_list b 
             on a.orgidprov = b.ORG_CODE
    GROUP BY OrgIDProv
             ,b.name
             ,GENDER;  

# COMMAND ----------

# DBTITLE 1,1H numerator: CASSR
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,'NONE' AS LEVEL_THREE
 			,'1H_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH14_prep
    GROUP BY  COALESCE(CASSR,"UNKNOWN")
             ,COALESCE(CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,1H numerator: CASSR gender
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR;Gender' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,'NONE' AS LEVEL_TWO
 			,'NONE' AS LEVEL_TWO_DESCRIPTION
             ,GENDER AS LEVEL_THREE
 			,'1H_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH14_prep
    GROUP BY  COALESCE(CASSR,"UNKNOWN")
             ,COALESCE(CASSR_description,"UNKNOWN")
             ,GENDER;

# COMMAND ----------

# DBTITLE 1,1H numerator: CASSR Provider
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR;Provider' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,OrgIDProv AS LEVEL_TWO
 			,b.name AS LEVEL_TWO_DESCRIPTION
             ,'NONE' AS LEVEL_THREE
 			,'1H_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH14_prep AS C
   INNER JOIN $db_output.AMH14_prep_prov AS P
              ON C.Person_ID = P.Person_ID
    LEFT JOIN $db_output.Provider_list b 
              ON p.orgidprov = b.ORG_CODE
     GROUP BY COALESCE(CASSR,"UNKNOWN")
             ,COALESCE(CASSR_description,"UNKNOWN")
             ,OrgIDProv
             ,b.name;

# COMMAND ----------

# DBTITLE 1,1H numerator: CASSR Provider gender
 %sql
 --Further breakdown of AMH14 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START
             ,'$rp_enddate' AS REPORTING_PERIOD_END
 			,'$status' AS STATUS
 			,'CASSR;Provider;Gender' AS BREAKDOWN
 			,COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE
 			,COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION
 			,OrgIDProv AS LEVEL_TWO
 			,b.name AS LEVEL_TWO_DESCRIPTION
             ,c.GENDER AS LEVEL_THREE
 			,'1H_NUMERATOR' AS METRIC
             ,CAST (COALESCE (cast(COUNT (DISTINCT c.Person_ID) as INT), 0) AS STRING) AS METRIC_VALUE
             ,'$db_source' AS SOURCE_DB
             
        FROM  $db_output.AMH14_prep AS C
   INNER JOIN $db_output.AMH14_prep_prov AS P
              ON C.Person_ID = P.Person_ID
    LEFT JOIN $db_output.Provider_list b 
              ON p.orgidprov = b.ORG_CODE
     GROUP BY COALESCE(CASSR,"UNKNOWN")
             ,COALESCE(CASSR_description,"UNKNOWN")
             ,OrgIDProv
             ,b.name
             ,c.GENDER;

# COMMAND ----------

# DBTITLE 1,Denominator: National
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'England' AS BREAKDOWN,
        'England' AS LEVEL_ONE,
        'England' AS LEVEL_ONE_DESCRIPTION,
        'NONE' AS LEVEL_TWO,
        'NONE' AS LEVEL_TWO_DESCRIPTION,
        'NONE' AS LEVEL_THREE,
        'DENOMINATOR' AS METRIC,
        CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_output.AMH03_prep;

# COMMAND ----------

# DBTITLE 1,Denominator: National gender
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'England;Gender' AS BREAKDOWN,
        'England' AS LEVEL_ONE,
        'England' AS LEVEL_ONE_DESCRIPTION,
        'NONE' AS LEVEL_TWO,
        'NONE' AS LEVEL_TWO_DESCRIPTION,
        GENDER AS LEVEL_THREE,
        'DENOMINATOR' AS METRIC,
        CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_output.AMH03_prep
 GROUP BY GENDER;

# COMMAND ----------

# DBTITLE 1,Denominator: Provider
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'Provider' AS BREAKDOWN,
            OrgIDProv AS LEVEL_ONE,
            b.name AS LEVEL_ONE_DESCRIPTION,
            'NONE' AS LEVEL_TWO,
            'NONE' AS LEVEL_TWO_DESCRIPTION,
            'NONE' AS LEVEL_THREE,
            'DENOMINATOR' AS METRIC,
            CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
            '$db_source' AS SOURCE_DB
            
   FROM     $db_output.AMH03_prep_prov a
  LEFT JOIN $db_output.Provider_list b on a.orgidprov = b.ORG_CODE
 GROUP BY   OrgIDProv
            ,b.name;

# COMMAND ----------

# DBTITLE 1,Denominator: Provider gender
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
            '$rp_enddate' AS REPORTING_PERIOD_END,
            '$status' AS STATUS,
            'Provider;Gender' AS BREAKDOWN,
            OrgIDProv AS LEVEL_ONE,
            b.name AS LEVEL_ONE_DESCRIPTION,
            'NONE' AS LEVEL_TWO,
            'NONE' AS LEVEL_TWO_DESCRIPTION,
            GENDER AS LEVEL_THREE,
            'DENOMINATOR' AS METRIC,
            CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
            '$db_source' AS SOURCE_DB
            
   FROM     $db_output.AMH03_prep_prov a
 LEFT JOIN  $db_output.Provider_list b on a.orgidprov = b.ORG_CODE
 GROUP BY   OrgIDProv,
            b.name,
            GENDER;

# COMMAND ----------

# DBTITLE 1,Denominator: CASSR
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'CASSR' AS BREAKDOWN,
        COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE,
        COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION,
        'NONE' AS LEVEL_TWO,
        'NONE' AS LEVEL_TWO_DESCRIPTION,
        'NONE' AS LEVEL_THREE,
        'DENOMINATOR' AS METRIC,
        CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_output.AMH03_prep
 GROUP BY COALESCE(CASSR,"UNKNOWN"),
        COALESCE(CASSR_description,"UNKNOWN");

# COMMAND ----------

# DBTITLE 1,Denominator: CASSR gender
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO  $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'CASSR;Gender' AS BREAKDOWN,
        COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE,
        COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION,
        'NONE' AS LEVEL_TWO,
        'NONE' AS LEVEL_TWO_DESCRIPTION,
        GENDER AS LEVEL_THREE,
        'DENOMINATOR' AS METRIC,
        CAST (COALESCE (CAST(COUNT (DISTINCT Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_output.AMH03_prep
 GROUP BY COALESCE(CASSR,"UNKNOWN"),
        COALESCE(CASSR_description,"UNKNOWN"),
        GENDER;

# COMMAND ----------

# DBTITLE 1,Denominator: CASSR Provider
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO  $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'CASSR;Provider' AS BREAKDOWN,
        COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE,
        COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION,
        OrgIDProv AS LEVEL_TWO,
        b.name AS LEVEL_TWO_DESCRIPTION,
        'NONE' AS LEVEL_THREE,
        'DENOMINATOR' AS METRIC,
        CAST (coalesce (CAST(COUNT (DISTINCT C.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_output.AMH03_prep AS C
 INNER JOIN $db_output.AMH03_prep_prov AS P
        ON C.Person_ID = P.Person_ID
  LEFT JOIN $db_output.Provider_list b 
        on p.orgidprov = b.ORG_CODE
 GROUP BY COALESCE(CASSR,"UNKNOWN"), 
        COALESCE(CASSR_description,"UNKNOWN"), 
        OrgIDProv,
        b.name;

# COMMAND ----------

# DBTITLE 1,Denominator: CASSR Provider gender
 %sql
 --Further breakdown of AMH03 measure from monthly file
 
 INSERT INTO  $db_output.Ascof_unformatted
     SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'CASSR;Provider;Gender' AS BREAKDOWN,
        COALESCE(CASSR,"UNKNOWN") AS LEVEL_ONE,
        COALESCE(CASSR_description,"UNKNOWN") AS LEVEL_ONE_DESCRIPTION,
        OrgIDProv AS LEVEL_TWO,
        b.name AS LEVEL_TWO_DESCRIPTION,
        C.GENDER AS LEVEL_THREE,
        'DENOMINATOR' AS METRIC,
        CAST (coalesce (CAST(COUNT (DISTINCT C.Person_ID) AS INT), 0) AS STRING) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_output.AMH03_prep AS C
 INNER JOIN $db_output.AMH03_prep_prov AS P
        ON C.Person_ID = P.Person_ID
 LEFT JOIN $db_output.Provider_list b 
        on p.orgidprov = b.ORG_CODE
 GROUP BY COALESCE(CASSR,"UNKNOWN"), 
        COALESCE(CASSR_description,"UNKNOWN"), 
        OrgIDProv,
        b.name,
        C.GENDER;