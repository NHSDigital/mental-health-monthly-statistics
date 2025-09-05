# Databricks notebook source
 %md
 1. The DQMI information is extracted monthly.  The requester is Michael Jacques.

 2. The databricks code to produce the outputs is stored in REF (Developement) within data_management_services\DQMI\DQMI_extracts.

 3. The DQMI data extracts are stored in: \\\pl-l-filnas-l2\mhmds$\imports\LIVE\MHSDS\DQMI\Data\Monthly Returns\NewWorld

 4. The document which is prefixed with a number is the document sent to Michael:
     
     a. make a copy of the most recent document (eg 3_DQMI_June_2019_1431.xlsx and name it 4_DQMI_Jul_2019_1432.xlsx)

     b. Run the databricks code from the 'Coverage' Cell. Download the output via AWS and save the extract in the above folder; paste the data into the coverage tab of the document created in 4.a.

     c. Run the databricks code from the 'Monthly Data' cell. Download the output via AWS and save the extract in the above folder; paste the data into the Monthly Data tab of the document created in 4.a.

     d. Update the Title Page tab and the Caveats tab within the Excel document created in 4.a.

     e. Run the databricks code from the 'Extract the time Dimensions - Integrity measures' cell, download the output via AWS and save the extract in the above folder.

 5. Upload the documents which was created in 4.a (including inserted data) and the 4.e extract to SharePoint:  https://hscic365.sharepoint.com/:f:/r/sites/DQ/DQMI/Monthly/Data/MHSDS%20Data?csf=1&e=Yh8UTe then inform Michael Jacques.
