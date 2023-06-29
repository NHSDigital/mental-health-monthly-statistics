# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

df = spark.table("{}.restraints_final_output1".format(db_output))
none_df = df.na.replace("NULL", "NONE")
spark.sql("DROP TABLE IF EXISTS {}.restr_pub_csv".format(db_output))
none_df.write.saveAsTable("{}.restr_pub_csv".format(db_output))

# COMMAND ----------

 %sql
 -- create or replace global temporary view supp_pub_csv as
 DROP TABLE IF EXISTS $db_output.supp_pub_csv;
 CREATE TABLE $db_output.supp_pub_csv AS
 select 
 ReportingPeriodStartDate as REPORTING_PERIOD_START,
 ReportingPeriodEndDate as REPORTING_PERIOD_END,
 'Performance' as STATUS,
 breakdown as BREAKDOWN,
 level_one as LEVEL_ONE,
 level_one_description as LEVEL_ONE_DESCRIPTION,
 level_two as LEVEL_TWO,
 level_two_description as LEVEL_TWO_DESCRIPTION,
 level_three as LEVEL_THREE,
 level_three_description as LEVEL_THREE_DESCRIPTION,
 level_four as LEVEL_FOUR,
 level_four_description as LEVEL_FOUR_DESCRIPTION,
 level_five as LEVEL_FIVE,
 level_five_description as LEVEL_FIVE_DESCRIPTION,
 level_six as LEVEL_SIX,
 level_six_description as LEVEL_SIX_DESCRIPTION,
 metric as METRIC,
 cast(case when cast(metric_value as int) < 5 then '9999999' else cast(round(metric_value/5,0)*5 as int) end as string) as METRIC_VALUE
 from $db_output.restr_pub_csv
 where metric in ('MHS76', 'MHS77')
 UNION ALL
 select 
 a.ReportingPeriodStartDate as REPORTING_PERIOD_START,
 a.ReportingPeriodEndDate as REPORTING_PERIOD_END,
 'Performance' as STATUS,
 a.breakdown as BREAKDOWN,
 a.level_one as LEVEL_ONE,
 a.level_one_description as LEVEL_ONE_DESCRIPTION,
 a.level_two as LEVEL_TWO,
 a.level_two_description as LEVEL_TWO_DESCRIPTION,
 a.level_three as LEVEL_THREE,
 a.level_three_description as LEVEL_THREE_DESCRIPTION,
 a.level_four as LEVEL_FOUR,
 a.level_four_description as LEVEL_FOUR_DESCRIPTION,
 a.level_five as LEVEL_FIVE,
 a.level_five_description as LEVEL_FIVE_DESCRIPTION,
 a.level_six as LEVEL_SIX,
 a.level_six_description as LEVEL_SIX_DESCRIPTION,
 a.metric as METRIC,
 case when b.METRIC = 'MHS76' and b.metric_value <5 then '9999999' 
 	 when c.METRIC = 'MHS77' and c.metric_value <5 then '9999999' 
      when d.METRIC = 'MHSXX' and d.metric_value <100 then '9999999'
      else cast(round(a.metric_value,0) as int) end as METRIC_VALUE
 from $db_output.restr_pub_csv a      
 left join $db_output.restr_pub_csv b on a.breakdown = b.breakdown 
                                                     and a.level_one = b.level_one
                                                     and a.level_two = b.level_two
                                                     and a.level_three = b.level_three
                                                     and a.level_four = b.level_four
                                                     and a.level_five = b.level_five
                                                     and a.level_six = b.level_six
                                                     and a.metric = 'MHS96'
                                                     and b.metric = 'MHS76'                                                    
 left join $db_output.restr_pub_csv c on a.breakdown = c.breakdown
                                                     and a.level_one = c.level_one
                                                     and a.level_two = c.level_two
                                                     and a.level_three = c.level_three
                                                     and a.level_four = c.level_four
                                                     and a.level_five = c.level_five
                                                     and a.level_six = c.level_six
                                                     and a.metric = 'MHS96'
                                                     and c.metric = 'MHS77'
 left join $db_output.restr_pub_csv d on a.breakdown = d.breakdown
                                                     and a.level_one = d.level_one
                                                     and a.level_two = d.level_two
                                                     and a.level_three = d.level_three
                                                     and a.level_four = d.level_four
                                                     and a.level_five = d.level_five
                                                     and a.level_six = d.level_six
                                                     and a.metric = 'MHS96'
                                                     and d.metric = 'MHSXX'                                                    
 where a.metric = 'MHS96'
 UNION ALL
 select 
 a.ReportingPeriodStartDate as REPORTING_PERIOD_START,
 a.ReportingPeriodEndDate as REPORTING_PERIOD_END,
 'Performance' as STATUS,
 a.breakdown as BREAKDOWN,
 a.level_one as LEVEL_ONE,
 a.level_one_description as LEVEL_ONE_DESCRIPTION,
 a.level_two as LEVEL_TWO,
 a.level_two_description as LEVEL_TWO_DESCRIPTION,
 a.level_three as LEVEL_THREE,
 a.level_three_description as LEVEL_THREE_DESCRIPTION,
 a.level_four as LEVEL_FOUR,
 a.level_four_description as LEVEL_FOUR_DESCRIPTION,
 a.level_five as LEVEL_FIVE,
 a.level_five_description as LEVEL_FIVE_DESCRIPTION,
 a.level_six as LEVEL_SIX,
 a.level_six_description as LEVEL_SIX_DESCRIPTION,
 a.metric as METRIC,
 case when b.METRIC = 'MHS76' and b.metric_value <5 then '9999999' else cast(round(a.metric_value,0) as int) end as METRIC_VALUE
 from $db_output.restr_pub_csv a      
 left join $db_output.restr_pub_csv b on a.breakdown = b.breakdown 
                                                     and a.level_one = b.level_one
                                                     and a.level_two = b.level_two
                                                     and a.level_three = b.level_three
                                                     and a.level_four = b.level_four
                                                     and a.level_five = b.level_five
                                                     and a.level_six = b.level_six
                                                     and a.metric = 'MHS97'
                                                     and b.metric = 'MHS76'
 where a.metric = 'MHS97'
 UNION ALL
 select 
 ReportingPeriodStartDate as REPORTING_PERIOD_START,
 ReportingPeriodEndDate as REPORTING_PERIOD_END,
 'Performance' as STATUS,
 breakdown as BREAKDOWN,
 level_one as LEVEL_ONE,
 level_one_description as LEVEL_ONE_DESCRIPTION,
 level_two as LEVEL_TWO,
 level_two_description as LEVEL_TWO_DESCRIPTION,
 level_three as LEVEL_THREE,
 level_three_description as LEVEL_THREE_DESCRIPTION,
 level_four as LEVEL_FOUR,
 level_four_description as LEVEL_FOUR_DESCRIPTION,
 level_five as LEVEL_FIVE,
 level_five_description as LEVEL_FIVE_DESCRIPTION,
 level_six as LEVEL_SIX,
 level_six_description as LEVEL_SIX_DESCRIPTION,
 metric as METRIC,
 metric_value as METRIC_VALUE
 from $db_output.restr_pub_csv
 where metric in ('MHS98', 'MHS99')

# COMMAND ----------

 %sql
 select count(breakdown) from $db_output.restr_pub_csv where metric <> 'MHSXX'
 union all
 select count(breakdown) from $db_output.supp_pub_csv

# COMMAND ----------

 %sql
 insert into $db_output.supp_pub_csv_final
 -- create or replace global temporary view supp_pub_csv_final as
 select 
 REPORTING_PERIOD_START,
 REPORTING_PERIOD_END,
 STATUS,
 BREAKDOWN,
 LEVEL_ONE,
 LEVEL_ONE_DESCRIPTION,
 LEVEL_TWO,
 LEVEL_TWO_DESCRIPTION,
 LEVEL_THREE,
 LEVEL_THREE_DESCRIPTION,
 LEVEL_FOUR,
 LEVEL_FOUR_DESCRIPTION,
 LEVEL_FIVE,
 LEVEL_FIVE_DESCRIPTION,
 LEVEL_SIX,
 LEVEL_SIX_DESCRIPTION,
 METRIC,
 CAST(case when METRIC_VALUE = '9999999' then '*' else METRIC_VALUE end as string) as METRIC_VALUE
 from $db_output.supp_pub_csv

# COMMAND ----------

