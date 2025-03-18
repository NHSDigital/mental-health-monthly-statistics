# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

 %md

 #tables created here:

 - ascof_breakdown_values
 - ascof_level_values
 - ascof_metric_values
 - ascof_unformatted
 - amh03_prep
 - amh13e_14e_prep
 - amh16e_17e_prep

# COMMAND ----------

#dbutils.widgets.text("db_output", "", "db_output")

#db_output = dbutils.widgets.get("db_output")

#assert (db_output)
#print(db_output)

# COMMAND ----------

# DBTITLE 1,Breakdown, Level & Metric
 %sql
 -- DROP TABLE IF EXISTS $db_output.ascof_breakdown_values;
 CREATE TABLE IF NOT EXISTS $db_output.ascof_breakdown_values (breakdown string) USING DELTA;

 DROP TABLE IF EXISTS $db_output.ascof_level_values;
 CREATE TABLE IF NOT EXISTS $db_output.ascof_level_values (primary_level string, primary_level_desc string, secondary_level string, secondary_level_desc string, third_level string, breakdown string) USING DELTA;

 -- DROP TABLE IF EXISTS $db_output.ascof_metric_values;
 CREATE TABLE IF NOT EXISTS $db_output.ascof_metric_values (metric string, metric_name string) USING DELTA;

# COMMAND ----------

# DBTITLE 1,ascof_unformatted
 %sql
 -- DROP TABLE IF EXISTS $db_output.ascof_unformatted;

 CREATE TABLE IF NOT EXISTS $db_output.ascof_unformatted 
      (REPORTING_PERIOD_START                                          string,   
       REPORTING_PERIOD_END                                            string,   
       STATUS                                                          string,
       BREAKDOWN                                                       string,
       PRIMARY_LEVEL                                                   string,
       PRIMARY_LEVEL_DESCRIPTION                                       string,   
       SECONDARY_LEVEL                                                 string,
       SECONDARY_LEVEL_DESCRIPTION                                     string, 
       THIRD_LEVEL                                                     string,
       METRIC                                                          string,
       METRIC_VALUE                                                    float,
       SOURCE_DB                                                       string)
 USING delta 
 PARTITIONED BY (REPORTING_PERIOD_END, STATUS)

# COMMAND ----------

# DBTITLE 1,AMH03e_prep
 %sql

 -- DROP TABLE IF EXISTS $db_output.amh03e_prep; 

 CREATE TABLE IF NOT EXISTS $db_output.AMH03e_prep 
      (Person_ID                                          string,   
       OrgIDProv                                          string,
       IC_Rec_CCG                                         string,   
       NAME                                               string,   
       Gender                                             string,   
       Region_code                                        string,   
       Region_description                                 string,   
       STP_code                                           string,   
       STP_description                                    string,   
       CASSR                                              string,   
       CASSR_description                                  string)
 USING delta 
 -- PARTITIONED BY (Person_ID)

# COMMAND ----------

# DBTITLE 1,AMH13e_14e_prep
 %sql

 -- DROP TABLE IF EXISTS $db_output.amh13e_14e_prep;

 CREATE TABLE IF NOT EXISTS $db_output.AMH13e_14e_prep 
      (Person_ID                                          string, 
       OrgIDProv                                          string,
       IC_Rec_CCG                                         string,   
       NAME                                               string,     
       Gender                                             string,  
       Region_code                                        string,   
       Region_description                                 string,   
       STP_code                                           string,   
       STP_description                                    string,   
       CASSR                                              string,   
       CASSR_description                                  string,   
       AccommodationTypeDate                              date,     
       SettledAccommodationInd                            string,   
       rank                                               int)
 USING delta 
 -- PARTITIONED BY (Person_ID)

# COMMAND ----------

# DBTITLE 1,AMH16e_17e_prep
 %sql

 -- DROP TABLE IF EXISTS $db_output.amh16e_17e_prep;

 CREATE TABLE IF NOT EXISTS $db_output.AMH16e_17e_prep 
      (Person_ID                                          string,   
       OrgIDProv                                          string,
       IC_Rec_CCG                                         string,   
       NAME                                               string,      
       Gender                                             string, 
       Region_code                                        string,   
       Region_description                                 string,   
       STP_code                                           string,   
       STP_description                                    string,   
       CASSR                                              string,   
       CASSR_description                                  string,   
       EmployStatusRecDate                                date,     
       EmployStatus                                       string,   
       RANK                                               int)
 USING delta 
 -- PARTITIONED BY (Person_ID)