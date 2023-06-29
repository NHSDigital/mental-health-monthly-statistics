# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_org_daily;
 CREATE TABLE $db_output.CMH_ORG_DAILY USING DELTA AS 
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $ref_database.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate' 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_org_relationship_daily;
 CREATE TABLE $db_output.CMH_ORG_RELATIONSHIP_DAILY USING DELTA AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $ref_database.org_relationship_daily
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_stp_mapping;
 CREATE TABLE $db_output.CMH_STP_MAPPING USING DELTA AS 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.CMH_ORG_DAILY A
 LEFT JOIN $db_output.CMH_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST' 
 LEFT JOIN $db_output.CMH_ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.CMH_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE' 
 LEFT JOIN $db_output.CMH_ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST' -- may need revisiting 30/09/22
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

# DBTITLE 1,-- may need revisiting, due to changes to CCG/ICB 30/09/22
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_LATEST AS
 SELECT DISTINCT ORG_CODE,
                 NAME
            FROM $ref_database.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ccg_mapping_2021;
 CREATE TABLE IF NOT EXISTS $db_output.ccg_mapping_2021 
 
 (
 CCG_UNMAPPED STRING, 
 CCG21CDH STRING,  
 CCG21NM STRING,  
 STP21CDH STRING, 
 STP21NM STRING,  
 NHSER21CDH STRING, 
 NHSER21NM STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.ccg_mapping_2021

# COMMAND ----------

 %sql
 insert into $db_output.CCG_MAPPING_2021
 (select CCG_CODE as CCG_UNMAPPED
        ,CCG_CODE as  CCG21CDH
        ,ccg_name as CCG21NM 
        ,stp_code as STP21CDH 
        ,stp_name as STP21NM
        ,region_code as NHSER21CDH 
        ,region_name as NHSER21NM  
 from $db_output.cmh_stp_mapping stp_map
 order by ccg_code
 )

# COMMAND ----------

 %sql
 ---need to add row for UNKNOWN SubICBs/ICBs/Regions
 insert into $db_output.CCG_MAPPING_2021 
 values
 ('UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN')

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))