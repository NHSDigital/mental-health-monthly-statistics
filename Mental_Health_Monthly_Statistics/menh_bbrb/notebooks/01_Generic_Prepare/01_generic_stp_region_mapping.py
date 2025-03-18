# Databricks notebook source
statuses = ["Provisional", "Performance", "Final"]
 
# dbutils.widgets.text("db_output","menh_bbrb")
# dbutils.widgets.text("db_source","$mhsds_db")
# dbutils.widgets.dropdown("status","Provisional", statuses)
# dbutils.widgets.text("pub_month", "202201")
# dbutils.widgets.text("rp_enddate", "2021-12-31")
# dbutils.widgets.text("reference_data", "reference_data")
# dbutils.widgets.text("end_month_id", "")

 
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
status = dbutils.widgets.get("status")
pub_month = dbutils.widgets.get("pub_month")
reference_data = dbutils.widgets.get("reference_data")
rp_enddate = dbutils.widgets.get("rp_enddate")
end_month_id = dbutils.widgets.get("end_month_id")

print(db_output, db_source, status, pub_month,reference_data,rp_enddate)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_daily_in_year 
 SELECT 
 ORG_CODE,
 NAME
 FROM      (SELECT
           DISTINCT 
           ORG_CODE, 
           NAME,
           ROW_NUMBER()OVER(PARTITION BY ORG_CODE ORDER BY SYSTEM_CREATED_DATE DESC) AS RN
           FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months(last_day('$rp_startdate_12m'), 1) OR ISNULL(BUSINESS_END_DATE))
           AND BUSINESS_START_DATE <= add_months(last_day('$rp_startdate_1m'), 1)    
           AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN'))
 WHERE RN = 1;

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_daily_latest 
 SELECT DISTINCT ORG_CODE, 
                 NAME
            FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                 AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_daily
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_daily_latest_mhsds_providers
 SELECT DISTINCT od.ORG_CODE, 
                 od.NAME
 FROM (SELECT DISTINCT OrgIDProvider from $db_source.mhs000header where UniqMonthID = $end_month_id) h
 INNER JOIN $db_output.bbrb_org_daily_latest od on h.OrgIDProvider = od.ORG_CODE

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_daily_past_12_months_mhsds_providers
 SELECT DISTINCT od.ORG_CODE, 
                 od.NAME
 FROM (SELECT DISTINCT OrgIDProvider from $db_source.mhs000header where UniqMonthID between $end_month_id-11 and $end_month_id) h
 INNER JOIN $db_output.bbrb_org_daily_in_year od on h.OrgIDProvider = od.ORG_CODE

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_daily_past_quarter_mhsds_providers
 SELECT DISTINCT od.ORG_CODE, 
                 od.NAME
 FROM (SELECT DISTINCT OrgIDProvider from $db_source.mhs000header where UniqMonthID between $end_month_id-2 and $end_month_id) h
 INNER JOIN $db_output.bbrb_org_daily_in_year od on h.OrgIDProvider = od.ORG_CODE

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_org_relationship_daily
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 $reference_data.org_relationship_daily
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.bbrb_stp_mapping
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.bbrb_org_daily A
 LEFT JOIN $db_output.bbrb_org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.bbrb_org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.bbrb_org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.bbrb_org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 AND C.NAME NOT LIKE '%ENTITY%'
 UNION
 SELECT 
 "UNKNOWN" as STP_CODE, 
 "UNKNOWN" as STP_NAME, 
 "UNKNOWN" as CCG_CODE, 
 "UNKNOWN" as CCG_NAME,
 "UNKNOWN" as REGION_CODE,
 "UNKNOWN" as REGION_NAME

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.ccg_mapping_2021
 (select CCG_CODE as CCG_UNMAPPED
        ,CCG_CODE as CCG21CDH
        ---------,geo_ccg.GEOGRAPHY_CODE as CCG21CD 
        ,CCG_NAME as CCG21NM 
        -------,geo_stp.GEOGRAPHY_CODE as STP21CD
        ,STP_CODE as STP21CDH 
        ,STP_NAME as STP21NM
        -------,geo_region.GEOGRAPHY_CODE as NHSER21CD 
        ,REGION_CODE as NHSER21CDH 
        ,REGION_NAME as NHSER21NM  
 from $db_output.bbrb_stp_mapping stp_map
 -- inner join $reference_data.ONS_CHD_GEO_EQUIVALENTS geo_ccg
 -- on stp_map.ccg_code = geo_ccg.DH_GEOGRAPHY_CODE
 -- inner join $reference_data.ONS_CHD_GEO_EQUIVALENTS geo_stp
 -- on stp_map.stp_code = geo_stp.DH_GEOGRAPHY_CODE
 -- inner join $reference_data.ONS_CHD_GEO_EQUIVALENTS geo_region
 -- on stp_map.region_code = geo_region.DH_GEOGRAPHY_CODE
 -- where geo_ccg.is_current = 1
 -- and geo_stp.is_current = 1
 -- and geo_region.is_current = 1
 order by ccg_code
 )