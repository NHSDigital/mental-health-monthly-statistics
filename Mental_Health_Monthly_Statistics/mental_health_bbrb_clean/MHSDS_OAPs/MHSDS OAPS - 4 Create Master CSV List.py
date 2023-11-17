# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $mhsds_database.mhs000header order by ReportingPeriodStartDate").collect()]
# endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $mhsds_database.mhs000header order by ReportingPeriodEndDate").collect()]
# monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $mhsds_database.mhs000header order by Uniqmonthid").collect()]

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

rp_startdates = {'Month'   : {'rp_name':rp_startdate_1m},
                 'Quarter' : {'rp_name':rp_startdate_qtr},
                 'Year'    : {'rp_name':rp_startdate_12m}
                 }
rp_startdates

# COMMAND ----------

 %run ./measure_metadata

# COMMAND ----------

metriclist = spark.createDataFrame(data=[(" "," ") ],schema=["METRIC","METRIC_DESCRIPTION"])
for metric in measure_metadata:
  metricname = measure_metadata[metric]['name']
  metrics = spark.createDataFrame(data=[(metric,metricname) ],schema=["METRIC","METRIC_DESCRIPTION"])
  metriclist = metriclist.union(metrics)
metriclist = metriclist.where(metriclist.METRIC!=" ")
metriclist.display()

# COMMAND ----------

column_order = [ 
      "REPORTING_PERIOD_START",
      "REPORTING_PERIOD_END",
      "STATUS",
      "BREAKDOWN",
      "PRIMARY_LEVEL",
      "PRIMARY_LEVEL_DESCRIPTION",
      "SECONDARY_LEVEL",
      "SECONDARY_LEVEL_DESCRIPTION",
      "METRIC",
      "METRIC_DESCRIPTION"
  ] 

# for metric in measure_metadata:
#   print(f"""Metric: {metric}""")
#   aggtype = measure_metadata[metric]['type']
#   aggcol = measure_metadata[metric]['aggcol']
#   metric_name = measure_metadata[metric]['name']
for geog_breakdown in geog_breakdown_metadata:
  geogname = geog_breakdown_metadata[geog_breakdown]['name']
  codecol = geog_breakdown_metadata[geog_breakdown]['codecol']
  namecol = geog_breakdown_metadata[geog_breakdown]['namecol']
  incl_breakdowns_month = geog_breakdown_metadata[geog_breakdown]['incl_breakdowns_month']
  incl_breakdowns_quarter = geog_breakdown_metadata[geog_breakdown]['incl_breakdowns_quarter']
  incl_breakdowns_year = geog_breakdown_metadata[geog_breakdown]['incl_breakdowns_year']
  geog_table = geog_breakdown_metadata[geog_breakdown]['geogtable']
  geog_col = geog_breakdown_metadata[geog_breakdown]['geogcol']
  geog_namecol = geog_breakdown_metadata[geog_breakdown]['geognamecol']
#     geog_colval = F.col({geog_col})
#     geog_namecolval = F.col({geog_namecol})

  if geog_table and geog_breakdown not in ('SENDPROV','RECPROV','England'):
    prep_df = spark.table(f"{db_output}.{geog_table}")
    geoglist = prep_df.select(geog_col, geog_namecol).distinct()
    unknown_list = spark.createDataFrame(data=[("UNKNOWN","UNKNOWN") ],schema=[geog_col,geog_namecol])
    geoglist = geoglist.union(unknown_list).distinct()
  elif geog_breakdown in ('SENDPROV','RECPROV'):
    prep_df = spark.sql(f"""SELECT DISTINCT LEVEL_ONE AS CODE,
                                            LEVEL_ONE_DESCRIPTION AS NAME
                            FROM {db_output}.oaps_output
                            WHERE BREAKDOWN = '{geogname}'""")
    geoglist = prep_df.select("CODE", "NAME").distinct()
  else:
    geoglist = spark.createDataFrame(data=[(" "," ") ],schema=["CODE","NAME"])

  for breakdown in breakdowns_metdata:
    if breakdown == 'RECPROV':
      breakdown_name = breakdowns_metdata[breakdown]['name']
      prep_df = spark.sql(f"""SELECT DISTINCT LEVEL_ONE AS CODE2,
                                              LEVEL_ONE_DESCRIPTION AS NAME2
                              FROM {db_output}.oaps_output
                              WHERE BREAKDOWN = '{breakdown_name}'""")
      df_breakdown_list = prep_df.select("CODE2", "NAME2").distinct()
      lvl_cols = ["CODE2","NAME2"]
    elif breakdown == 'SENDPROV':
      breakdown_name = breakdowns_metdata[breakdown]['name']
      prep_df = spark.sql(f"""SELECT DISTINCT LEVEL_ONE AS CODE,
                                              LEVEL_ONE_DESCRIPTION AS NAME
                              FROM {db_output}.oaps_output
                              WHERE BREAKDOWN = '{breakdown_name}'""")
      df_breakdown_list = prep_df.select("CODE", "NAME").distinct()
      lvl_cols = ["CODE","NAME"]
    else:
      breakdown_list = breakdowns_metdata[breakdown]['level_list']
      lvl_cols = breakdowns_metdata[breakdown]['lookup_col']
      df_breakdown_list = spark.createDataFrame(data=breakdown_list,schema=lvl_cols)

    if breakdown != 'England' and geog_breakdown != 'England':  #Sub national demographic breakdowns
      if geog_breakdown in ('SENDPROV','RECPROV'):
        codecol2 = "CODE"
        namecol2 = "NAME"
      else:
        codecol2 = codecol
        namecol2 = namecol
      name = geogname+'; '+breakdowns_metdata[breakdown]['name']
      if breakdown == 'RECPROV':
        col = "CODE2"
        colname = "NAME2"
      else:
        col = breakdowns_metdata[breakdown]['column']
        colname = breakdowns_metdata[breakdown]['namecol']
      if col[:1] == '"':
        col = col[1:-1]
        colname = colname[1:-1]
      levels_list = geoglist.crossJoin(df_breakdown_list)
      level_fields = [  
                      F.lit(name).alias("breakdown"),
                      F.col(codecol2).alias("primary_level"),  
                      F.col(namecol2).alias("primary_level_desc"),  
                      F.col(col).alias("secondary_level"),   
                      F.col(colname).alias("secondary_level_desc")
                    ]
    elif geog_breakdown != 'England': #Sub national breakdowns
      if geog_breakdown in ('SENDPROV','RECPROV'):
        codecol2 = "CODE"
        namecol2 = "NAME"
      else:
        codecol2 = codecol
        namecol2 = namecol
      name = geogname
      col = '"NULL"'
      colname = '"NULL"'
      levels_list = geoglist
      level_fields = [  
                      F.lit(name).alias("breakdown"),
                      F.col(codecol2).alias("primary_level"),  
                      F.col(namecol2).alias("primary_level_desc"),  
                      F.lit("NULL").alias("secondary_level"),   
                      F.lit("NULL").alias("secondary_level_desc")
                    ]
    else: #Demographic breakdowns
#         codecol2 = breakdowns_metdata[breakdown]['column']
#         namecol2 = breakdowns_metdata[breakdown]['namecol']
      name = breakdowns_metdata[breakdown]['name']
      if breakdown == 'RECPROV':
        codecol2 = "CODE2"
        namecol2 = "NAME2"
      else:
        codecol2 = breakdowns_metdata[breakdown]['column']
        namecol2 = breakdowns_metdata[breakdown]['namecol']
      col = '"NULL"'
      colname = '"NULL"'
      if codecol2[:1] == '"':
        codecol2 = codecol2[1:-1]
        namecol2 = namecol2[1:-1]
      levels_list = df_breakdown_list
      level_fields = [  
                      F.lit(name).alias("breakdown"),
                      F.col(codecol2).alias("primary_level"),  
                      F.col(namecol2).alias("primary_level_desc"),  
                      F.lit("NULL").alias("secondary_level"),   
                      F.lit("NULL").alias("secondary_level_desc")
                    ]


    print(f"""   Breakdown: {name}""")
    for rp_startdate in rp_startdates:
      if rp_startdate == 'Month':
        incl_breakdowns = incl_breakdowns_month
      elif rp_startdate == 'Quarter':
        incl_breakdowns = incl_breakdowns_quarter
      else:
        incl_breakdowns = incl_breakdowns_year
      if breakdown in incl_breakdowns:
        print(f"""      RP: {rp_startdate}""")
        rp_sd_param = rp_startdates[rp_startdate]['rp_name']

        insert_df = levels_list.select(level_fields)
        insert_df.write.insertInto(f"{db_output}.oaps_level_values",overwrite=True)

        lvl_df = spark.table(f"{db_output}.oaps_level_values")
        lvl_df_metric = lvl_df.crossJoin(metriclist)
        lvl_measure_df = (
          lvl_df_metric
          .withColumn("REPORTING_PERIOD_START", F.lit(rp_sd_param)) #rp_startdate based off frequency
          .withColumn("REPORTING_PERIOD_END", F.lit(rp_enddate))
          .withColumn("STATUS", F.lit(status))
#           .withColumn("METRIC", F.lit(metric))
#           .withColumn("METRIC_DESCRIPTION", F.lit(metric_name))
          .select(*column_order)
        )
        #write into final ref table
        lvl_measure_df.write.insertInto(f"{db_output}.oaps_csv_lookup")

#           spark.sql(f"TRUNCATE TABLE {db_output}.oaps_level_values")
  spark.sql(f"OPTIMIZE {db_output}.oaps_csv_lookup ZORDER BY (Metric, Breakdown)")

# COMMAND ----------

 %sql
 DELETE FROM $db_output.oaps_csv_lookup WHERE BREAKDOWN = 'Sending Provider; Receiving Provider' AND PRIMARY_LEVEL = SECONDARY_LEVEL

# COMMAND ----------

 %sql
 SELECT * FROM $db_output.oaps_csv_lookup
 WHERE BREAKDOWN = 'Sending Provider'

# COMMAND ----------

 %sql
 SELECT DISTINCT LEVEL_ONE AS CODE,
                 LEVEL_ONE_DESCRIPTION AS NAME
 FROM $db_output.oaps_output
 WHERE BREAKDOWN = 'Sending Provider'

# COMMAND ----------

 %sql
 -- truncate table $db_output.oaps_csv_lookup