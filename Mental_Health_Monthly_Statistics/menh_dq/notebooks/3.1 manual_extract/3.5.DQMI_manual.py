# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create Widgets
 %sql

 -- month_id 1445 is August 2020, month_id 1446 is September 2020
 -- month_id increments by 1 each month :o)
 -- the data that landed at the end of October 2020 (and was published in November 2020) was for August Performance and September Provisional data

 CREATE WIDGET TEXT db_output DEFAULT "menh_dq";

 CREATE WIDGET TEXT month_id DEFAULT "1445";

 CREATE WIDGET TEXT status DEFAULT "Performance";

# COMMAND ----------

# DBTITLE 1,Coverage
 %sql

 SELECT * FROM $db_output.DQMI_Coverage
 WHERE Month_ID = '$month_id'
 AND Status = '$status'
 ORDER BY Organisation

# COMMAND ----------

# DBTITLE 1,Monthly Data
 %sql

 SELECT * FROM $db_output.DQMI_Monthly_Data
 WHERE Month_ID = '$month_id'
 AND Status = '$status'
 ORDER BY DataProviderID

# COMMAND ----------

# DBTITLE 1,Integrity
 %sql

 SELECT * FROM $db_output.DQMI_Integrity
 WHERE Month_ID = '$month_id'
 AND Status = '$status'
 ORDER BY MeasureID, OrgIDProv