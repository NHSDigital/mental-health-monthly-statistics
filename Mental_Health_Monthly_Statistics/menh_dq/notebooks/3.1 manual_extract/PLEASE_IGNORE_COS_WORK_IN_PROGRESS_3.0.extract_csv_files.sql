-- Databricks notebook source
CREATE WIDGET TEXT dbm DEFAULT "mh_cur_clear";

-- COMMAND ----------

-- DBTITLE 1,Extract all the CSV files for Primary then Refresh
 %python
 #
 ##we need to automatically extract the csv files when a load has taken place.  This can be done by first creating the provisional csv files then the Final by setting the parameters for each in a for loop. We need to be given an container area into which we can download the files for the analysts to access them.
 #
 #
 #dbm = dbutils.widgets.get("dbm");
 #
 ##loop through the process twice.  First extract primary files then refresh
 #for i in range(0, 2):
 #  if i == 0:
 #    status = 'Provisional';
 #    FileType = 'Primary';
 #    #below we set the parameters for when the provisional csv files are created 
 #    UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate, ReportingPeriod = spark.sql("SELECT MAX(UniqMonthId), MAX(ReportingPeriodStartDate), MAX(ReportingPeriodEndDate), DATE_FORMAT(MAX(ReportingPeriodStartDate), 'MMM-YY') FROM {0}.mhs000header".format(dbm)).collect()[0]; 
 #    #we can run it - how do we save it to a folder?
 #    #from paul 2 below: https://db.ref.core.data.digital.nhs.uk/#notebook/82962/command/82963
 #    #outpath = r's3a://nhsd-dspp-core-ref-dms/msds/outputs/MSDS_' + period.replace(' ', '_') + '_Monthly_Data_File.csv'
 #    # pdf.write.mode("overwrite").option("header","true").csv(outpath)
 #  if i == 1:
 #    status = 'Final';
 #    FileType = 'Refresh';
 #     #below we set the parameters for when the provisional csv files are created
 #    UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate, ReportingPeriod = spark.sql("SELECT MAX(UniqMonthId), MAX(ReportingPeriodStartDate), MAX(ReportingPeriodEndDate), DATE_FORMAT(MAX(ReportingPeriodStartDate), 'MMM-YY') FROM {0}.mhs000header where UniqMonthId = {1}".format(dbm,UniqMonthID-1)).collect()[0]; 
 #  #the below 4 csv files are created for each loop. (when we have the extract folder we can specify the output to be place here)
 #  dbutils.notebook.run("3.3.coverage_csv", 0, {"UniqMonthID": UniqMonthID, "ReportingPeriodStartDate": ReportingPeriodStartDate, "ReportingPeriodEndDate": ReportingPeriodEndDate, "Status": status,"dbm": dbm});
 #  dbutils.notebook.run("3.4.vodim_csv", 0, {"UniqMonthID": UniqMonthID, "ReportingPeriodStartDate": ReportingPeriodStartDate, "ReportingPeriodEndDate": ReportingPeriodEndDate, "Status": status, "dbm": dbm});
 #  dbutils.notebook.run("3.5.power_bi_vodim_csv", 0, {"UniqMonthID": UniqMonthID, "ReportingPeriod": ReportingPeriod});
 #  dbutils.notebook.run("3.6.power_bi_coverage_csv", 0, {"UniqMonthID": UniqMonthID, "ReportingPeriodStartDate": ReportingPeriodStartDate, "ReportingPeriodEndDate": ReportingPeriodEndDate, "Status": status,"dbm": dbm});
 #  #print(FileType,status,UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate, ReportingPeriod,{dbm});