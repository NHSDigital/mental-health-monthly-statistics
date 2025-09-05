-- Databricks notebook source
 %py
 # This handy bit of python code removes all existing widgets - useful for cleaning out old nonsense and starting again

 #dbutils.widgets.removeAll()

-- COMMAND ----------

 %sql

 -- these are the only parameters required to run these outputs (now that the dq measures are being put into final tables ready for extraction during the main monthly run of code)

 CREATE WIDGET TEXT db_output DEFAULT "menh_dq";
 CREATE WIDGET TEXT month_id DEFAULT "1443"; 
 CREATE WIDGET TEXT status DEFAULT "Performance";


-- COMMAND ----------

-- DBTITLE 1,DQ_vodim_MmmPxxx_YYYY.csv - run for Provisional (Prov) & Performance (Perf)
 %sql

 SELECT * FROM $db_output.DQ_VODIM_monthly_csv WHERE Month_Id = '$month_id' AND status = '$status';


-- COMMAND ----------

-- DBTITLE 1,DQ_vodim_monthly_PBI_Mmm_YYYY.csv - run just for Performance
 %sql

 SELECT 
 Reporting_Period AS Reporting_Period_Start,
 Reporting_Level,
 Provider_Code, 
 Provider_Name,
 DQ_Measure, 
 DQ_Result,
 DQ_Dataset_Metric_ID AS DQ_Dataset_MetricID,
 Count,
 Percentage


 FROM $db_output.DQ_VODIM_monthly_pbi WHERE Month_Id = '$month_id' AND status = '$status'

 ORDER BY Provider_Code, DQ_Measure, DQ_Dataset_Metric_ID