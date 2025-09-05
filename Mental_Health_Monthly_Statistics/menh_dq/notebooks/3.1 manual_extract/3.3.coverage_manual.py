# Databricks notebook source
# This handy bit of python code removes all existing widgets - useful for cleaning out old nonsense and starting again

# dbutils.widgets.removeAll()

# COMMAND ----------

 %sql

 -- these are the only parameters required to run these outputs (now that the dq measures are being put into final tables ready for extraction during the main monthly run of code)

 CREATE WIDGET TEXT db_output DEFAULT "menh_dq";
 CREATE WIDGET TEXT month_id DEFAULT "1446";
 CREATE WIDGET TEXT status DEFAULT "Performance";
 CREATE WIDGET TEXT dbm DEFAULT "menh_dq";

# COMMAND ----------

# DBTITLE 1,DQ_coverage_MmmPxxx_YYYY.csv - run for Provisional (Prov) & Performance (Perf)
 %sql

 SELECT * FROM $db_output.dq_coverage_monthly_csv WHERE Month_Id = '$month_id' AND status = '$status' and SOURCE_DB = '$dbm';

# COMMAND ----------

# DBTITLE 1,DQ_coverage_monthly_PBI_Mmm_YYYY.csv - run just for Performance
 %sql

 SELECT * FROM $db_output.dq_coverage_monthly_pbi WHERE Month_Id = '$month_id' AND status = '$status' and SOURCE_DB = '$dbm';