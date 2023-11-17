# Databricks notebook source
# DBTITLE 1,CODE 28 - Inappropriate OAPS Started in Period
# Prep tables build

# COMMAND ----------

startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from $reference_data.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from $reference_data.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from $reference_data.mhs000header order by Uniqmonthid").collect()]

dbutils.widgets.dropdown("rp_startdate", "2021-05-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2021-05-31", endchoices)
dbutils.widgets.dropdown("rp_qtrstartdate", "2021-03-01", startchoices)
dbutils.widgets.dropdown("rp_12mstartdate", "2020-06-01", startchoices)
dbutils.widgets.dropdown("month_id", "1454", monthid)
dbutils.widgets.text("db_output","$user_id")
dbutils.widgets.text("db_source","$db_source")
dbutils.widgets.text("status","Final")

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_qtrstartdate = dbutils.widgets.get("rp_qtrstartdate")
rp_12mstartdate = dbutils.widgets.get("rp_12mstartdate")
status  = dbutils.widgets.get("status")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.metric_info;
 CREATE TABLE $db_output.metric_info USING DELTA AS
 SELECT  '28' AS metric,
         'Number of Inappropriate OAPs placements (not bed-nights) started in the period' AS metric_description

# COMMAND ----------

# DBTITLE 1,Remove Code 28 results from output
 %sql
 DROP TABLE IF EXISTS $db_output.oaps_output_tmp;
 CREATE TABLE $db_output.oaps_output_tmp USING DELTA AS
 select a.*
 from $db_output.oaps_output as a
 left join $db_output.metric_info as b
    on a.metric = b.metric
 where b.metric is null;
 
 DROP TABLE IF EXISTS $db_output.oaps_output;
 CREATE TABLE $db_output.oaps_output USING DELTA AS
 select a.*
 from $db_output.oaps_output_tmp as a;
 
 DROP TABLE IF EXISTS $db_output.oaps_output_tmp;
 
 OPTIMIZE $db_output.oaps_output;

# COMMAND ----------

# DBTITLE 1,England Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'England' breakdown,
             'None' level_one,
             'None' level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10') AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,England Rolling 3 month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'England' breakdown,
             'None' level_one,
             'None' level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10') AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,England Rolling 12 month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'England' breakdown,
             'None' level_one,
             'None' level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10') AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,CCG - Registration or Residence, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'CCG - Registration or Residence' breakdown,
             COALESCE(CCG.level,'Unknown') level_one,
             COALESCE(CCG.level_description,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_CCG_List as CCG
              ON a.ORGIDCCGRes = CCG.level
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(CCG.level,'Unknown'),
                    COALESCE(CCG.level_description,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,CCG - Registration or Residence, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'CCG - Registration or Residence' breakdown,
             COALESCE(CCG.level,'Unknown') level_one,
             COALESCE(CCG.level_description,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_CCG_List as CCG
              ON a.ORGIDCCGRes = CCG.level
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(CCG.level,'Unknown'),
                    COALESCE(CCG.level_description,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,CCG - Registration or Residence, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'CCG - Registration or Residence' breakdown,
             COALESCE(CCG.level,'Unknown') level_one,
             COALESCE(CCG.level_description,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_CCG_List as CCG
              ON a.ORGIDCCGRes = CCG.level
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(CCG.level,'Unknown'),
                    COALESCE(CCG.level_description,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,STP, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'STP' breakdown,
             COALESCE(STP.STP_Code,'Unknown') level_one,
             COALESCE(STP.STP_Name,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.STP_Code,'Unknown'),
                    COALESCE(STP.STP_Name,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,STP, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'STP' breakdown,
             COALESCE(STP.STP_Code,'Unknown') level_one,
             COALESCE(STP.STP_Name,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid  between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.STP_Code,'Unknown'),
                    COALESCE(STP.STP_Name,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,STP, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'STP' breakdown,
             COALESCE(STP.STP_Code,'Unknown') level_one,
             COALESCE(STP.STP_Name,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid  between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.STP_Code,'Unknown'),
                    COALESCE(STP.STP_Name,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
                    COALESCE(STP.Region_Name,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid  between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
                    COALESCE(STP.Region_Name,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid  between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
                    COALESCE(STP.Region_Name,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Sending Provider, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Sending Provider' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON a.OrgIDSubmitting = Org.ORG_CODE
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Sending Provider, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Sending Provider' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON a.OrgIDSubmitting = Org.ORG_CODE
           WHERE uniqmonthid  between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Sending Provider, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Sending Provider' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON a.OrgIDSubmitting = Org.ORG_CODE
           WHERE uniqmonthid  between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Receiving Provider, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Receiving Provider' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON a.OrgIDReceiving = Org.ORG_CODE
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Receiving Provider, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Receiving Provider' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON a.OrgIDReceiving = Org.ORG_CODE
           WHERE uniqmonthid  between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Receiving Provider, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Receiving Provider' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON a.OrgIDReceiving = Org.ORG_CODE
           WHERE uniqmonthid  between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Receiver Group, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Receiver Group' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON LEFT(a.OrgIDReceiving,3) = Org.ORG_CODE
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Receiver Group, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Receiver Group' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON LEFT(a.OrgIDReceiving,3) = Org.ORG_CODE
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Receiver Group, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Receiver Group' breakdown,
             COALESCE(Org.ORG_CODE,'Unknown') level_one,
             COALESCE(Org.NAME,'Unknown') level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_ORG_DAILY as Org
              ON LEFT(a.OrgIDReceiving,3) = Org.ORG_CODE
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(Org.ORG_CODE,'Unknown'),
             COALESCE(Org.NAME,'Unknown') ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Gender, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Gender' breakdown,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_one,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Gender, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Gender' breakdown,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_one,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Gender, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Gender' breakdown,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_one,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Gender, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Gender' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_two,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Gender, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Gender' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_two,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Gender, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Gender' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_two,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END,
             CASE WHEN a.Gender = '1' THEN '1'
                  WHEN a.Gender = '2' THEN '2'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Bed Type, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Bed Type' breakdown,
             COALESCE(a.HospitalBedTypeMH,'Unknown') level_one,
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.HospitalBedTypeMH,'Unknown'),
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Bed Type, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Bed Type' breakdown,
             COALESCE(a.HospitalBedTypeMH,'Unknown') level_one,
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.HospitalBedTypeMH,'Unknown'),
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Bed Type, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Bed Type' breakdown,
             COALESCE(a.HospitalBedTypeMH,'Unknown') level_one,
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.HospitalBedTypeMH,'Unknown'),
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Bed Type, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Bed Type' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.HospitalBedTypeMH,'Unknown') level_two,
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.HospitalBedTypeMH,'Unknown'),
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Bed Type, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Bed Type' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.HospitalBedTypeMH,'Unknown') level_two,
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.HospitalBedTypeMH,'Unknown'),
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Bed Type, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Bed Type' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.HospitalBedTypeMH,'Unknown') level_two,
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.HospitalBedTypeMH,'Unknown'),
             CASE WHEN a.HospitalBedTypeMH = '10' THEN 'Acute adult mental health care'
                   WHEN a.HospitalBedTypeMH = '11' THEN 'Acute older adult mental health care (organic and functional)'
                   WHEN a.HospitalBedTypeMH = '12' THEN 'Psychiatric Intensive Care Unit (acute mental health care)'
                   WHEN a.HospitalBedTypeMH = '13' THEN 'Eating Disorders'
                   WHEN a.HospitalBedTypeMH = '14' THEN 'Mother and baby'
                   WHEN a.HospitalBedTypeMH = '15' THEN 'Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '16' THEN 'Low secure/locked rehabilitation'
                   WHEN a.HospitalBedTypeMH = '17' THEN 'High dependency rehabilitation'
                   WHEN a.HospitalBedTypeMH = '18' THEN 'Long term complex rehabilitation/ Continuing Care'
                   WHEN a.HospitalBedTypeMH = '19' THEN 'Low secure'
                   WHEN a.HospitalBedTypeMH = '20' THEN 'Medium secure'
                   WHEN a.HospitalBedTypeMH = '21' THEN 'High secure'
                   WHEN a.HospitalBedTypeMH = '22' THEN 'Neuro-psychiatry / Acquired Brain Injury'
                   WHEN a.HospitalBedTypeMH = '23' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Child (including High Dependency)'
                   WHEN a.HospitalBedTypeMH = '24' THEN 'General Child and Adolescent Mental Health (CAMHS) inpatient - Adolescent (including HighDependency)'
                   WHEN a.HospitalBedTypeMH = '25' THEN 'Eating Disorders inpatient - Adolescent (above 12)'
                   WHEN a.HospitalBedTypeMH = '26' THEN 'Eating Disorders inpatient - Child (12 years and under)'
                   WHEN a.HospitalBedTypeMH = '27' THEN 'Low Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '28' THEN 'Medium Secure Mental Illness'
                   WHEN a.HospitalBedTypeMH = '29' THEN 'Child Mental Health inpatient services for the Deaf'
                   WHEN a.HospitalBedTypeMH = '30' THEN 'Learning Disabilities / Autistic Spectrum Disorder inpatient'
                   WHEN a.HospitalBedTypeMH = '31' THEN 'Low Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '32' THEN 'Medium Secure Learning Disabilities'
                   WHEN a.HospitalBedTypeMH = '33' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Adolescent'
                   WHEN a.HospitalBedTypeMH = '34' THEN 'Psychiatric Intensive Care Unit'
                  ELSE 'Unknown' END) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Out of Area Reason, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Out of Area Reason' breakdown,
             COALESCE(a.ReasonOAT,'Unknown') level_one,
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.ReasonOAT,'Unknown'),
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Out of Area Reason, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Out of Area Reason' breakdown,
             COALESCE(a.ReasonOAT,'Unknown') level_one,
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.ReasonOAT,'Unknown'),
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Out of Area Reason, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Out of Area Reason' breakdown,
             COALESCE(a.ReasonOAT,'Unknown') level_one,
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.ReasonOAT,'Unknown'),
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Out of Area Reason, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Out of Area Reason' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.ReasonOAT,'Unknown') level_two,
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.ReasonOAT,'Unknown'),
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Out of Area Reason, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Out of Area Reason' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.ReasonOAT,'Unknown') level_two,
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.ReasonOAT,'Unknown'),
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Out of Area Reason, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Out of Area Reason' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.ReasonOAT,'Unknown') level_two,
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.ReasonOAT,'Unknown'),
             CASE WHEN a.ReasonOAT = '10' THEN 'Unavailability of bed at referring organisation'
                   WHEN a.ReasonOAT = '11' THEN 'Safeguarding'
                   WHEN a.ReasonOAT = '12' THEN 'Offending restrictions'
                   WHEN a.ReasonOAT = '13' THEN 'Staff member or family/friend within the referring organisation'
                   WHEN a.ReasonOAT = '14' THEN 'Patient choice'
                   WHEN a.ReasonOAT = '15' THEN 'Patient away from home'
                   WHEN a.ReasonOAT = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Primary Diagnosis, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Primary Diagnosis' breakdown,
             COALESCE(a.PrimReasonReferralMH,'Unknown') level_one,
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.PrimReasonReferralMH,'Unknown'),
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Primary Diagnosis, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Primary Diagnosis' breakdown,
             COALESCE(a.PrimReasonReferralMH,'Unknown') level_one,
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.PrimReasonReferralMH,'Unknown'),
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Primary Diagnosis, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Primary Diagnosis' breakdown,
             COALESCE(a.PrimReasonReferralMH,'Unknown') level_one,
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.PrimReasonReferralMH,'Unknown'),
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Primary Diagnosis, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Primary Diagnosis' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.PrimReasonReferralMH,'Unknown') level_two,
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.PrimReasonReferralMH,'Unknown'),
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Primary Diagnosis, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Primary Diagnosis' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.PrimReasonReferralMH,'Unknown') level_two,
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.PrimReasonReferralMH,'Unknown'),
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Primary Diagnosis, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Primary Diagnosis' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.PrimReasonReferralMH,'Unknown') level_two,
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.PrimReasonReferralMH,'Unknown'),
             CASE WHEN a.PrimReasonReferralMH = '01' THEN '(Suspected) First Episode Psychosis'
                   WHEN a.PrimReasonReferralMH = '02' THEN 'Ongoing or Recurrent Psychosis'
                   WHEN a.PrimReasonReferralMH = '03' THEN 'Bi polar disorder'
                   WHEN a.PrimReasonReferralMH = '04' THEN 'Depression'
                   WHEN a.PrimReasonReferralMH = '05' THEN 'Anxiety'
                   WHEN a.PrimReasonReferralMH = '06' THEN 'Obsessive compulsive disorder'
                   WHEN a.PrimReasonReferralMH = '07' THEN 'Phobias'
                   WHEN a.PrimReasonReferralMH = '08' THEN 'Organic brain disorder'
                   WHEN a.PrimReasonReferralMH = '09' THEN 'Drug and alcohol difficulties '
                   WHEN a.PrimReasonReferralMH = '10' THEN 'Unexplained physical symptoms'
                   WHEN a.PrimReasonReferralMH = '11' THEN 'Post-traumatic stress disorder'
                   WHEN a.PrimReasonReferralMH = '12' THEN 'Eating disorders'
                   WHEN a.PrimReasonReferralMH = '13' THEN 'Perinatal mental health issues'
                   WHEN a.PrimReasonReferralMH = '14' THEN 'Personality disorders'
                   WHEN a.PrimReasonReferralMH = '15' THEN 'Self harm behaviours'
                   WHEN a.PrimReasonReferralMH = '16' THEN 'Conduct disorders'
                   WHEN a.PrimReasonReferralMH = '18' THEN 'In crisis'
                   WHEN a.PrimReasonReferralMH = '19' THEN 'Relationship difficulties'
                   WHEN a.PrimReasonReferralMH = '20' THEN 'Gender Discomfort issues'
                   WHEN a.PrimReasonReferralMH = '21' THEN 'Attachment difficulties '
                   WHEN a.PrimReasonReferralMH = '22' THEN 'Self - care issues'
                   WHEN a.PrimReasonReferralMH = '23' THEN 'Adjustment to health issues'
                   WHEN a.PrimReasonReferralMH = '24' THEN 'Neurodevelopmental Conditions, excluding Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '25' THEN 'Suspected Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '26' THEN 'Diagnosed Autism Spectrum Disorder'
                   WHEN a.PrimReasonReferralMH = '27' THEN 'Preconception perinatal mental health concern'
                   WHEN a.PrimReasonReferralMH = '28' THEN 'Gambling disorder'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Mental Health Act Status, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Mental Health Act Status' breakdown,
             COALESCE(a.LegalStatusCode,'Unknown') level_one,
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.LegalStatusCode,'Unknown'),
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Mental Health Act Status, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Mental Health Act Status' breakdown,
             COALESCE(a.LegalStatusCode,'Unknown') level_one,
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.LegalStatusCode,'Unknown'),
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Mental Health Act Status, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Mental Health Act Status' breakdown,
             COALESCE(a.LegalStatusCode,'Unknown') level_one,
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_one_description,
             'None' level_two,
             'None' level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(a.LegalStatusCode,'Unknown'),
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Mental Health Act Status, Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_1m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Mental Health Act Status' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.LegalStatusCode,'Unknown') level_two,
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_1m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid = $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.LegalStatusCode,'Unknown'),
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Mental Health Act Status, Rolling 3 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_qtr' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Mental Health Act Status' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.LegalStatusCode,'Unknown') level_two,
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_qtr' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-2 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.LegalStatusCode,'Unknown'),
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

# DBTITLE 1,Region + Mental Health Act Status, Rolling 12 Month
 %sql
 INSERT INTO $db_output.oaps_output
 SELECT  reporting_period_start,
         reporting_period_end,
         status,
         breakdown,
         level_one,
         level_one_description,
         level_two,
         level_two_description,
         level_three,
         level_three_description,
         metric,
         metric_description,
         metric_value
 FROM (    SELECT
             '$rp_startdate_12m' reporting_period_start,
             '$rp_enddate' reporting_period_end,
             '$status' status,
             'Region, Mental Health Act Status' breakdown,
             COALESCE(STP.Region_Code,'Unknown') level_one,
             COALESCE(STP.Region_Name,'Unknown') level_one_description,
             COALESCE(a.LegalStatusCode,'Unknown') level_two,
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END level_two_description,
             'None' level_three,
             'None' level_three_description,
             COUNT(distinct CASE WHEN ReferralrequestReceivedDate between '$rp_startdate_12m' and '$rp_enddate' THEN UniqServReqID ELSE null END) metric_value
           FROM $db_output.OAPs AS a
           LEFT JOIN $db_output.OAPs_STP_Region_mapping as STP
              ON a.ORGIDCCGRes = STP.CCG_Code
           WHERE uniqmonthid between $end_month_id-11 and $end_month_id
             AND ReasonOAT = '10'
           GROUP BY COALESCE(STP.Region_Code,'Unknown'),
             COALESCE(STP.Region_Name,'Unknown'),
             COALESCE(a.LegalStatusCode,'Unknown'),
             CASE WHEN a.LegalStatusCode = '01' THEN 'Informal'
                   WHEN a.LegalStatusCode = '02' THEN 'Formally detained under Mental Health Act Section 2'
                   WHEN a.LegalStatusCode = '03' THEN 'Formally detained under Mental Health Act Section 3'
                   WHEN a.LegalStatusCode = '04' THEN 'Formally detained under Mental Health Act Section 4'
                   WHEN a.LegalStatusCode = '05' THEN 'Formally detained under Mental Health Act Section 5 (2)'
                   WHEN a.LegalStatusCode = '06' THEN 'Formally detained under Mental Health Act Section 5 (4)'
                   WHEN a.LegalStatusCode = '07' THEN 'Formally detained under Mental Health Act Section 35'
                   WHEN a.LegalStatusCode = '08' THEN 'Formally detained under Mental Health Act Section 36'
                   WHEN a.LegalStatusCode = '09' THEN 'Formally detained under Mental Health Act Section 37 with 41 restrictions'
                   WHEN a.LegalStatusCode = '10' THEN 'Formally detained under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '12' THEN 'Formally detained under Mental Health Act Section 38'
                   WHEN a.LegalStatusCode = '13' THEN 'Formally detained under Mental Health Act Section 44'
                   WHEN a.LegalStatusCode = '14' THEN 'Formally detained under Mental Health Act Section 46'
                   WHEN a.LegalStatusCode = '15' THEN 'Formally detained under Mental Health Act Section 47 with 49 restrictions'
                   WHEN a.LegalStatusCode = '16' THEN 'Formally detained under Mental Health Act Section 47'
                   WHEN a.LegalStatusCode = '17' THEN 'Formally detained under Mental Health Act Section 48 with 49 restrictions'
                   WHEN a.LegalStatusCode = '18' THEN 'Formally detained under Mental Health Act Section 48'
                   WHEN a.LegalStatusCode = '19' THEN 'Formally detained under Mental Health Act Section 135'
                   WHEN a.LegalStatusCode = '20' THEN 'Formally detained under Mental Health Act Section 136'
                   WHEN a.LegalStatusCode = '31' THEN 'Formally detained under Criminal Procedure (Insanity) Act 1964 as amended by the Criminal Procedures (Insanity and Unfitness to Plead) Act 1991'
                   WHEN a.LegalStatusCode = '32' THEN 'Formally detained under other acts'
                   WHEN a.LegalStatusCode = '35' THEN 'Subject to guardianship under Mental Health Act Section 7'
                   WHEN a.LegalStatusCode = '36' THEN 'Subject to guardianship under Mental Health Act Section 37'
                   WHEN a.LegalStatusCode = '37' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction in force)'
                   WHEN a.LegalStatusCode = '38' THEN 'Formally detained under Mental Health Act Section 45A (Limitation direction ended)'
                   WHEN a.LegalStatusCode = '98' THEN 'Not Applicable'
                   WHEN a.LegalStatusCode = '99' THEN 'Not Known (Not Recorded)'
                  ELSE 'Unknown' END ) AS a
 CROSS JOIN $db_output.metric_info AS b

# COMMAND ----------

 %sql
 select * from $db_output.OAPs

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.oaps_output;

# COMMAND ----------

 %sql
 select * from $db_output.oaps_output order by metric, reporting_period_start, CASE WHEN breakdown='England' THEN 0 ELSE 1 END, breakdown, level_one, level_two, level_three

# COMMAND ----------

# from dsp.common.exports import create_csv_for_download
# export_dataframe = sqlContext.sql(""" select  * from """+dbutils.widgets.get("db_output")+""".oaps_output order by metric, reporting_period_start, CASE WHEN breakdown='England' THEN 0 ELSE 1 END, breakdown, level_one, level_two, level_three""")
# data = create_csv_for_download(df = export_dataframe)
# filename = 'MHSDS_OAPs_CODE_10.csv.gz'
# displayHTML(f"<h4>Your File {filename} is ready to <a href='data:text/csv;base64,{data.decode()}' download='{filename}'>download</a></h4>")