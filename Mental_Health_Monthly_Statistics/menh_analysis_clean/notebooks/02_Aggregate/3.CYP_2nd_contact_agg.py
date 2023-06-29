# Databricks notebook source
# DBTITLE 1,MHS69 - England
 %sql
 INSERT INTO $db_output.CYP_2nd_contact_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
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
        
   FROM global_temp.CYPFinal w;

# COMMAND ----------

# DBTITLE 1,MHS69 - CCG - GP Practice or Residence; Provider
 %sql
 
 INSERT INTO $db_output.CYP_2nd_contact_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'CCG - GP Practice or Residence; Provider' AS BREAKDOWN,
        CASE
          WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'UNKNOWN')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'UNKNOWN') END AS PRIMARY_LEVEL,
        CASE
          WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.NAME, 'UNKNOWN')
          ELSE COALESCE(ccg.NAME, 'UNKNOWN') END AS PRIMARY_LEVEL_DESCRIPTION,
        Z.OrgIDProvider AS SECONDARY_LEVEL,
        COALESCE(x.NAME, 'UNKNOWN') AS SECONDARY_LEVEL_DESCRIPTION,
        'MHS69' AS METRIC,
        COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM $db_source.MHS000Header z
        LEFT OUTER JOIN global_temp.CYPFinal w 
        ON w.OrgIDProv = Z.OrgIDProvider 
        LEFT OUTER JOIN global_temp.CCG ccg 
        ON w.Person_ID = ccg.Person_ID 
        LEFT JOIN global_temp.RD_ORG_DAILY_LATEST X 
        ON z.OrgIDProvider = x.ORG_CODE
        LEFT JOIN $db_output.RD_CCG_LATEST DFC_CCG 
        ON w.OrgIDComm = DFC_CCG.original_ORG_CODE
 WHERE z.UniqMonthID = '$month_id'
 GROUP BY CASE
        WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'UNKNOWN')
        ELSE COALESCE(ccg.IC_Rec_CCG, 'UNKNOWN') END,
      CASE
        WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.NAME, 'UNKNOWN')
        ELSE COALESCE(ccg.NAME, 'UNKNOWN') END,
      Z.OrgIDProvider,
      COALESCE(x.NAME, 'UNKNOWN');

# COMMAND ----------

# DBTITLE 1,MHS69 - CCG - GP Practice or Residence
 %sql
 
 INSERT INTO $db_output.CYP_2nd_contact_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
        '$rp_enddate' AS REPORTING_PERIOD_END,
        '$status' AS STATUS,
        'CCG - GP Practice or Residence' AS BREAKDOWN,
        CASE
          WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'UNKNOWN')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'UNKNOWN') END AS PRIMARY_LEVEL,
        CASE
          WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.NAME, 'UNKNOWN')
          ELSE COALESCE(ccg.NAME, 'UNKNOWN') END AS PRIMARY_LEVEL_DESCRIPTION,
        'NONE' AS SECONDARY_LEVEL,
        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
        'MHS69' AS METRIC,
        COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
        '$db_source' AS SOURCE_DB
        
   FROM global_temp.CCG ccg
        LEFT OUTER JOIN global_temp.CYPFinal w 
        ON w.Person_ID = ccg.Person_ID     
         LEFT JOIN $db_output.RD_CCG_LATEST DFC_CCG 
        ON w.OrgIDComm = DFC_CCG.original_ORG_CODE
 GROUP BY CASE
          WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'UNKNOWN')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'UNKNOWN') END,
        CASE
          WHEN w.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.NAME, 'UNKNOWN')
              ELSE COALESCE(ccg.NAME, 'UNKNOWN') END;
              

# COMMAND ----------

# DBTITLE 1,MHS69 - Provider
 %sql
 
 INSERT INTO $db_output.CYP_2nd_contact_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
      '$rp_enddate' AS REPORTING_PERIOD_END,
      '$status' AS STATUS,
      'Provider' AS BREAKDOWN,
      z.OrgIDProvider AS PRIMARY_LEVEL,
      COALESCE(x.NAME, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
      'NONE' AS SECONDARY_LEVEL,
      'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
      'MHS69' AS METRIC,
      COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
      '$db_source' AS SOURCE_DB
      
 FROM $db_source.MHS000Header z
 LEFT JOIN global_temp.CYPFinal w ON w.OrgIDProv = Z.OrgIDProvider 
 LEFT JOIN global_temp.RD_ORG_DAILY_LATEST as X on z.OrgIDProvider = x.ORG_CODE
 WHERE z.UniqMonthID = '$month_id'
 GROUP BY z.OrgIDProvider, 
      COALESCE(x.NAME, 'UNKNOWN');

# COMMAND ----------

# DBTITLE 1,MHS69 - Region - uses STP_Region_mapping_post_2018 - keep for old runs - need to put in conditional - commented out
# %sql

# INSERT INTO $db_output.CYP_2nd_contact_unformatted
# SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#    '$rp_enddate' AS REPORTING_PERIOD_END,
#    '$status' as STATUS,
#    'Region' as BREAKDOWN,
#    COALESCE(stp.Region_code, 'UNKNOWN') AS PRIMARY_LEVEL,
#    COALESCE(stp.Region_description, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
#    'NONE' AS SECONDARY_LEVEL,
#    'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
#    'MHS69' AS METRIC,
#    COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
# '$db_source' AS SOURCE_DB
# FROM global_temp.CYPFinal w
#    LEFT OUTER JOIN global_temp.CCG ccg 
#    ON w.Person_ID = ccg.Person_ID 
#    --created a static table in breakdowns for this - will need reviewing as and when
#    LEFT JOIN $db_output.STP_Region_mapping_post_2018 stp ON
#    CASE WHEN w.OrgIDProv = 'DFC' THEN w.OrgIDComm ELSE ccg.IC_Rec_CCG END = stp.CCG_code
# GROUP BY COALESCE(stp.Region_code, 'UNKNOWN'),
#    COALESCE(STP.Region_description, 'UNKNOWN');

# COMMAND ----------

# DBTITLE 1,MHS69 - Region - uses STP_Region_mapping_post_2020 - need to put in conditional
 %sql
 
 INSERT INTO $db_output.CYP_2nd_contact_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
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
    
 FROM global_temp.CYPFinal w
    LEFT OUTER JOIN global_temp.CCG ccg 
    ON w.Person_ID = ccg.Person_ID 
    -- uses new non-static table 
    LEFT JOIN $db_output.STP_Region_mapping_post_2020 stp ON
    CASE WHEN w.OrgIDProv = 'DFC' THEN w.OrgIDComm ELSE ccg.IC_Rec_CCG END = stp.CCG_code
 GROUP BY COALESCE(stp.Region_code, 'UNKNOWN'),
    COALESCE(STP.Region_description, 'UNKNOWN');

# COMMAND ----------

# DBTITLE 1,MHS69 - STP - uses STP_Region_mapping_post_2018 - keep for old runs - need to put in conditional  - commented out
# %sql

# INSERT INTO $db_output.CYP_2nd_contact_unformatted
# SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
#        '$rp_enddate' AS REPORTING_PERIOD_END,
#        '$status' AS STATUS,
#        'STP' AS BREAKDOWN,
#        COALESCE(stp.STP_code, 'UNKNOWN') AS PRIMARY_LEVEL,
#        COALESCE(STP.STP_description, 'UNKNOWN') AS PRIMARY_LEVEL_DESCRIPTION,
#        'NONE' AS SECONDARY_LEVEL,
#        'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
#        'MHS69' AS METRIC,
#        COUNT(DISTINCT w.Person_ID) AS METRIC_VALUE,
# '$db_source' AS SOURCE_DB
#   FROM global_temp.CYPFinal w
#        LEFT OUTER JOIN global_temp.CCG ccg 
#        ON w.Person_ID = ccg.Person_ID 
#        --created a static table in breakdowns for this - will need reviewing as and when
#        LEFT JOIN $db_output.STP_Region_mapping_post_2018 stp ON 
#        CASE WHEN w.OrgIDProv = 'DFC' THEN w.OrgIDComm ELSE ccg.IC_Rec_CCG END = stp.CCG_code
# GROUP BY COALESCE(stp.STP_code, 'UNKNOWN'),
#        COALESCE(STP.STP_description, 'UNKNOWN');

# COMMAND ----------

# DBTITLE 1,MHS69 - STP - uses STP_Region_mapping_post_2020 - need to put in conditional
 %sql
 
 INSERT INTO $db_output.CYP_2nd_contact_unformatted
 SELECT '$rp_startdate' AS REPORTING_PERIOD_START,
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
        
   FROM global_temp.CYPFinal w
        LEFT OUTER JOIN global_temp.CCG ccg 
        ON w.Person_ID = ccg.Person_ID 
        -- uses new non-static table
        LEFT JOIN $db_output.STP_Region_mapping_post_2020 stp ON 
        CASE WHEN w.OrgIDProv = 'DFC' THEN w.OrgIDComm ELSE ccg.IC_Rec_CCG END = stp.CCG_code
 GROUP BY COALESCE(stp.STP_code, 'UNKNOWN'),
        COALESCE(STP.STP_description, 'UNKNOWN');