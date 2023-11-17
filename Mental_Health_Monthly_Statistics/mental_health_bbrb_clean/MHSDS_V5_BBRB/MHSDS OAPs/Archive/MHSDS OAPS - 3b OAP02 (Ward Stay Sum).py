# Databricks notebook source
# DBTITLE 1,OAP02 - OAPS Bed Days in Period
# Prep tables build
# dbutils.widgets.removeAll()

# COMMAND ----------

# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]

# dbutils.widgets.dropdown("rp_startdate_1m", "2021-05-01", startchoices)
# dbutils.widgets.dropdown("rp_enddate", "2021-05-31", endchoices)
# dbutils.widgets.dropdown("rp_startdate_qtr", "2021-03-01", startchoices)
# dbutils.widgets.dropdown("rp_startdate_12m", "2020-06-01", startchoices)
# dbutils.widgets.dropdown("start_month_id", "1454", monthid)
# dbutils.widgets.dropdown("end_month_id", "1454", monthid)
# dbutils.widgets.text("db_output","$user_id")
# dbutils.widgets.text("db_source","$db_source")
# dbutils.widgets.text("status","Final")

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
status  = dbutils.widgets.get("status")

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.metric_info;
 INSERT INTO $db_output.metric_info
 -- SELECT  'OAP02' AS metric,
 --         'Number of OAPs bed days in the period' AS metric_description
 -- UNION ALL
 SELECT  'OAP02' AS metric,
         'Number of Inappropriate OAPs bed days in the period' AS metric_description

# COMMAND ----------

rp_startdates = {'Month'   : {'rp_name':rp_startdate_1m},
                 'Quarter' : {'rp_name':rp_startdate_qtr},
                 'Year'    : {'rp_name':rp_startdate_12m}
                 }
rp_startdates

# COMMAND ----------

counts_metadata = {##'OAP02':  {'type'  : 'sum',
#                               'aggcol': 'Bed_Days',
#                               'reasonoat': '',
#                               'activeend': ''},
                   'OAP02': {'type'  : 'sum',
                              'aggcol': 'Bed_Days',
                              'reasonoat': '10',
                              'activeend': ''}
                  }
counts_metadata

# COMMAND ----------

breakdowns_metdata = { 'England'  :{'name':'England',
                                    'column':'"England"',
                                    'namecol':'"England"',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''},
                       'Age'      :{'name':'Age Group',
                                    'column':'Der_AgeGroup',
                                    'namecol':'Der_AgeGroup',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''},
                       'Ethnicity':{'name':'Ethnicity',
                                    'column':'NHSDEthnicity',
                                    'namecol':'NHSDEthnicityName',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''},
                       'GENDER'   :{'name':'Gender',
                                    'column':'Der_Gender',
                                    'namecol':'Der_GenderName',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''},
                       'IMD'      :{'name':'IMD',
                                    'column':'IMD_Decile',
                                    'namecol':'IMD_Decile',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''},
                       'Reason'   :{'name':'Primary reason for referral',
                                    'column':'PrimReasonReferralMH',
                                    'namecol':'PrimReasonReferralMHName',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''},
                       'BedType'  :{'name':'Bed Type',
                                    'column':'HospitalBedTypeMH',
                                    'namecol':'HospitalBedTypeMHName',
                                    'sumsuffix':'WS',
                                    'rankcol':'',
                                    'rankcolval':''}
                       }

# COMMAND ----------

geog_breakdown_metadata = {'England' : {'name':'England',
                                        'codecol':'"England"',
                                        'namecol':'"England"'},
                           'SubICB'  : {'name':'Sub ICB Location - GP Practice or Residence',
                                        'codecol':'CCG_CODE',
                                        'namecol':'CCG_NAME'},
                           'ICB'     : {'name':'ICB',
                                        'codecol':'STP_CODE',
                                        'namecol':'STP_NAME'},
                           'REGION'  : {'name':'Region',
                                        'codecol':'REGION_CODE',
                                        'namecol':'REGION_NAME'},
                           'SENDPROV'  : {'name':'Sending Provider',
                                        'codecol':'OrgIDSubmitting',
                                        'namecol':'SendingProvName'}
                          }

# COMMAND ----------

for metric in counts_metadata:
  reasonoat = counts_metadata[metric]['reasonoat']
  activeend = counts_metadata[metric]['activeend']
  for rp_startdate in rp_startdates:
    rp_sd_param = rp_startdates[rp_startdate]['rp_name']
    spark.sql(f"DROP TABLE IF EXISTS {db_output}.DATA_{metric}_{rp_startdate}")
    if reasonoat != '' and activeend != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.DATA_{metric}_{rp_startdate} USING DELTA AS
                    SELECT a.*,
                           c.STP_CODE, 
                           c.STP_NAME, 
                           c.CCG_CODE, 
                           c.CCG_NAME, 
                           c.REGION_CODE, 
                           c.REGION_NAME
                    FROM {db_output}.OAPS_{rp_startdate} as a
                    INNER JOIN {db_output}.Months as b
                       ON a.UniqMonthID = b.UniqMonthID
                    LEFT JOIN {db_output}.OAPs_STP_Region_mapping as c
                       ON a.SubICBGPRes = c.CCG_Code
                    WHERE ReasonOAT = '{reasonoat}'
                      AND (NewServDischDate is null or NewServDischDate>'{rp_enddate}')
                      AND b.ReportingPeriodStartDate between '{rp_sd_param}' and '{rp_enddate}'""")
    elif reasonoat != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.DATA_{metric}_{rp_startdate} USING DELTA AS
                    SELECT a.*,
                           c.STP_CODE, 
                           c.STP_NAME, 
                           c.CCG_CODE, 
                           c.CCG_NAME, 
                           c.REGION_CODE, 
                           c.REGION_NAME,
                           orgrec.NAME as ReceivingProvName,
                           orgsend.NAME as SendingProvName
                    FROM {db_output}.OAPS_{rp_startdate} as a
                    INNER JOIN {db_output}.Months as b
                       ON a.UniqMonthID = b.UniqMonthID
                    LEFT JOIN {db_output}.OAPs_STP_Region_mapping as c
                       ON a.SubICBGPRes = c.CCG_Code
                    LEFT JOIN {db_output}.OAPs_ORG_DAILY as orgrec
                       ON a.OrgIDProv = orgrec.ORG_CODE
                    LEFT JOIN {db_output}.OAPs_ORG_DAILY as orgsend
                       ON a.OrgIDSubmitting = orgsend.ORG_CODE
                    WHERE ReasonOAT = '{reasonoat}'
                      AND b.ReportingPeriodStartDate between '{rp_sd_param}' and '{rp_enddate}'""")
    elif activeend != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.DATA_{metric}_{rp_startdate} USING DELTA AS
                    SELECT a.*,
                           c.STP_CODE, 
                           c.STP_NAME, 
                           c.CCG_CODE, 
                           c.CCG_NAME, 
                           c.REGION_CODE, 
                           c.REGION_NAME
                    FROM {db_output}.OAPS_{rp_startdate} as a
                    INNER JOIN {db_output}.Months as b
                       ON a.UniqMonthID = b.UniqMonthID
                    LEFT JOIN {db_output}.OAPs_STP_Region_mapping as c
                       ON a.SubICBGPRes = c.CCG_Code
                    WHERE (NewServDischDate is null or NewServDischDate>'{rp_enddate}')
                      AND b.ReportingPeriodStartDate between '{rp_sd_param}' and '{rp_enddate}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.DATA_{metric}_{rp_startdate} USING DELTA AS
                    SELECT a.*,
                           c.STP_CODE, 
                           c.STP_NAME, 
                           c.CCG_CODE, 
                           c.CCG_NAME, 
                           c.REGION_CODE, 
                           c.REGION_NAME
                    FROM {db_output}.OAPS_{rp_startdate} as a
                    INNER JOIN {db_output}.Months as b
                       ON a.UniqMonthID = b.UniqMonthID
                    LEFT JOIN {db_output}.OAPs_STP_Region_mapping as c
                       ON a.SubICBGPRes = c.CCG_Code
                     WHERE b.ReportingPeriodStartDate between '{rp_sd_param}' and '{rp_enddate}'""")

# COMMAND ----------

 %sql
 -- truncate table $db_output.oaps_output

# COMMAND ----------

for metric in counts_metadata:
  print(f"""Metric: {metric}""")
  aggtype = counts_metadata[metric]['type']
  aggcol = counts_metadata[metric]['aggcol']
  for geog_breakdown in geog_breakdown_metadata:
    geogname = geog_breakdown_metadata[geog_breakdown]['name']
    codecol = geog_breakdown_metadata[geog_breakdown]['codecol']
    namecol = geog_breakdown_metadata[geog_breakdown]['namecol']
    for breakdown in breakdowns_metdata:
      if breakdown != 'England' and geog_breakdown != 'England':
        codecol2 = codecol
        namecol2 = namecol
        name = geogname+'; '+breakdowns_metdata[breakdown]['name']
        col = breakdowns_metdata[breakdown]['column']
        colname = breakdowns_metdata[breakdown]['namecol']
      elif geog_breakdown != 'England':
        codecol2 = codecol
        namecol2 = namecol
        name = geogname
        col = '"NULL"'
        colname = '"NULL"'
      else:
        codecol2 = breakdowns_metdata[breakdown]['column']
        namecol2 = breakdowns_metdata[breakdown]['namecol']
        name = breakdowns_metdata[breakdown]['name']
        col = '"NULL"'
        colname = '"NULL"'
      print(f"""   Breakdown: {name}""")
      sum_sufix = breakdowns_metdata[breakdown]['sumsuffix']
      if breakdowns_metdata[breakdown]['rankcol'] != '':
        rankcol = breakdowns_metdata[breakdown]['rankcol']
        rankcolval = breakdowns_metdata[breakdown]['rankcolval']
        whereclause = f'WHERE {rankcol} = {rankcolval}'
      else:
        whereclause = ''
      for rp_startdate in rp_startdates:
        print(f"""      RP: {rp_startdate}""")
        rp_sd_param = rp_startdates[rp_startdate]['rp_name']
        spark.sql(f"""  INSERT INTO {db_output}.oaps_output

                        SELECT  '{rp_sd_param}' AS REPORTING_PERIOD_START,
                                '{rp_enddate}' AS REPORTING_PERIOD_END,
                                '{status}' AS STATUS,
                                '{name}' AS BREAKDOWN,
                                COALESCE({codecol2},'Unknown') AS PRIMARY_LEVEL,
                                COALESCE({namecol2},'Unknown') AS PRIMARY_LEVEL_DESCRIPTION,
                                COALESCE({col},'Unknown') AS SECONDARY_LEVEL,
                                COALESCE({colname},'Unknown') AS SECONDARY_LEVEL_DESCRIPTION,
                                'NULL' AS Level_3,
                                'NULL' AS Level_3_DESCRIPTION,
                                '{metric}' AS Metric,
                                metric_description,
                                {aggtype}({aggcol}_{rp_startdate}_{sum_sufix}) AS Metric_Value
                        FROM {db_output}.DATA_{metric}_{rp_startdate} as a
                        CROSS JOIN (SELECT *
                                    FROM {db_output}.metric_info
                                    WHERE metric = '{metric}') AS b
                        {whereclause}
                        GROUP BY  metric_description,
                                  COALESCE({codecol2},'Unknown'),
                                  COALESCE({namecol2},'Unknown'),
                                  COALESCE({col},'Unknown'),
                                  COALESCE({colname},'Unknown')""")
      spark.sql(f"OPTIMIZE {db_output}.oaps_output ZORDER BY (Metric, Breakdown)")
  spark.sql(f"DROP TABLE IF EXISTS {db_output}.DATA_{metric}_{rp_startdate}")

# COMMAND ----------

 %sql
 -- select * from $db_output.oaps_output