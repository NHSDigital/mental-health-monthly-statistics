# Databricks notebook source
 %md 
 # CYP 2nd Contact prep assets:
  - Cont
  ...
  

# COMMAND ----------

#CCG for provisional data?

# COMMAND ----------

 %python
 assert dbutils.widgets.get('Financial_Yr_Start')
 assert dbutils.widgets.get('Financial_Yr_End')
 assert dbutils.widgets.get('db_output')
 assert dbutils.widgets.get('db_source')
 assert dbutils.widgets.get('month_id')
 assert dbutils.widgets.get('rp_enddate')
 assert dbutils.widgets.get('rp_startdate')
 assert dbutils.widgets.get('status')

# COMMAND ----------

# DBTITLE 1,1. Get all attended contacts (not email or SMS) and indirect activity
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Cont AS	
     SELECT c.UniqMonthID,
            c.Person_ID,
            c.UniqServReqID,
            c.AgeCareContDate,
            c.UniqCareContID AS ContID,
            c.CareContDate AS ContDate
       FROM $db_source.MHS201CareContact c
      WHERE (
 (c.AttendOrDNACode IN ('5','6') and c.ConsMediumUsed NOT IN ('05','06'))
 or 
 (c.ConsMediumUsed IN ('05','06') and OrgIdProv = 'DFC')
 )
 AND UniqMonthID <= '$month_id'

 UNION ALL

     SELECT i.UniqMonthID,
            i.Person_ID,
            i.UniqServReqID,
            NULL AS AgeCareContDate,
            CAST(i.MHS204UniqID AS string) AS ContID,
            i.IndirectActDate AS ContDate
       FROM $db_source.MHS204IndirectActivity i
       WHERE
 UniqMonthID <= '$month_id'

# COMMAND ----------

# DBTITLE 1,2. Link contacts and indirect activity to referral and rank by referral
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RefCont AS
     SELECT c.UniqMonthID,
            r.Person_ID,
            r.UniqServReqID,
            CASE WHEN c.AgeCareContDate IS NULL THEN r.AgeServReferRecDate ELSE c.AgeCareContDate END AS AgeCareContDate,
            r.OrgIDProv,
            r.OrgIDComm,
            c.ContID,
            c.ContDate,
            ROW_NUMBER () OVER(PARTITION BY r.Person_ID, r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS RN1,
            ROW_NUMBER () OVER(PARTITION BY r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS DFC_RN1
       FROM global_temp.Cont c
 INNER JOIN $db_source.MHS101Referral r 
            ON ((c.UniqServReqID = r.UniqServReqID AND c.Person_ID = r.Person_ID) 
            OR (r.OrgIDProv = 'DFC' AND c.UniqServReqID = r.UniqServReqID))
 		   AND AgeServReferRecDate BETWEEN 0 AND 18 
            AND (RecordEndDate IS null OR RecordEndDate >= '$rp_enddate')
            AND RecordStartDate <= '$rp_enddate'
            AND r.UniqMonthID <= '$month_id'

# COMMAND ----------

# DBTITLE 1,3. Get all first contacts where aged 17 or under (1st contact should be before 18th birthday)
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW FirstCont AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM global_temp.RefCont r
      WHERE ((r.RN1 = 1 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 1 and r.OrgIDProv = 'DFC'))
            AND r.AgeCareContDate <18

# COMMAND ----------

# DBTITLE 1,4. Get all second contacts in the financial year where first contact was before 18th birthday
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW SubCont AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM global_temp.RefCont r
 INNER JOIN global_temp.FirstCont f 
            ON f.Person_ID = r.Person_ID 
            AND f.UniqServReqID = r.UniqServReqID
      WHERE ((r.RN1 = 2 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 2 and r.OrgIDProv = 'DFC')) 
            AND (r.ContDate BETWEEN '${Financial_Yr_Start}' AND '${Financial_Yr_End}')

# COMMAND ----------

# DBTITLE 1,5. Get contacts in financial year
 %sql 

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RefCont_inyear AS
     SELECT c.UniqMonthID,
            r.Person_ID,
            r.UniqServReqID,
            CASE WHEN c.AgeCareContDate IS NULL THEN r.AgeServReferRecDate ELSE c.AgeCareContDate END AS AgeCareContDate,
            r.OrgIDProv,
            r.OrgIDComm,
            c.ContID,
            c.ContDate,
            ROW_NUMBER () OVER(PARTITION BY r.Person_ID, r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS RN1,
            ROW_NUMBER () OVER(PARTITION BY r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS DFC_RN1
       FROM global_temp.Cont c
 INNER JOIN $db_source.MHS101Referral r 
            ON ((c.UniqServReqID = r.UniqServReqID AND c.Person_ID = r.Person_ID) 
            OR (r.OrgIDProv = 'DFC' AND c.UniqServReqID = r.UniqServReqID))
 		   AND AgeServReferRecDate BETWEEN 0 AND 18 
            AND (RecordEndDate IS null OR RecordEndDate >= '${rp_enddate}') 
            AND RecordStartDate <= '${rp_enddate}'
      WHERE ContDate BETWEEN '${Financial_Yr_Start}' AND '${Financial_Yr_End}'

# COMMAND ----------

# DBTITLE 1,6. Get all first contacts in financial year where aged 17 or under (1st contact should be before 18th birthday)
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW FirstCont_inyear AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM global_temp.RefCont_inyear r
      WHERE ((r.RN1 = 1 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 1 and r.OrgIDProv = 'DFC')) 
            AND r.AgeCareContDate <18
            AND ContDate BETWEEN '${Financial_Yr_Start}' AND '${Financial_Yr_End}'

# COMMAND ----------

# DBTITLE 1,7. Get all second contacts in the financial year where the first contact was before 18th birthday
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW SubCont_inyear AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM global_temp.RefCont_inyear r
 INNER JOIN global_temp.FirstCont_inyear f 
            ON ((f.UniqServReqID = r.UniqServReqID AND f.Person_ID = r.Person_ID) 
            OR (r.OrgIDProv = 'DFC' AND f.UniqServReqID = r.UniqServReqID))
      WHERE ((r.RN1 = 2 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 2 and r.OrgIDProv = 'DFC'))
            AND r.ContDate BETWEEN '${Financial_Yr_Start}' AND '${Financial_Yr_End}'

# COMMAND ----------

# DBTITLE 1,8. Aggregates the first contacts
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW first_contacts AS
     SELECT *
       FROM global_temp.FirstCont
      UNION
     SELECT * 
       FROM global_temp.FirstCont_inyear

# COMMAND ----------

# DBTITLE 1,9. Aggregates the second contacts
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW contacts AS
     SELECT *
       FROM global_temp.SubCont
      WHERE ContDate BETWEEN '${Financial_Yr_Start}' AND '${Financial_Yr_End}'
      UNION
     SELECT * 
       FROM global_temp.SubCont_inyear
      WHERE ContDate BETWEEN '${Financial_Yr_Start}' AND '${Financial_Yr_End}'

# COMMAND ----------

# DBTITLE 1,10. Ordering to get the first time 2 contacts for each person (can only be counted once in each quarter)
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW FirstPersQtr AS
     SELECT *,
            ROW_NUMBER () OVER(PARTITION BY Person_ID ORDER BY ContDate ASC, ContID ASC) AS QtrRN
       FROM global_temp.contacts 

# COMMAND ----------

# DBTITLE 1,11. Choosing the first occurrance of 2 contacts for each person and allocating the quarter (based on second contact date)
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ContPer AS
     SELECT s.Person_ID,
            s.UniqServReqID,
            s.OrgIDProv,
            s.OrgIDComm,
            s.UniqMonthID,
            s.ContID,
            s.ContDate,
            s.AgeCareContDate,
            s.RN1,
            f.ContDate AS ContDate2,
            f.AgeCareContDate AS AgeCareContDate2,   
            s.UniqMonthID as Qtr
            FROM global_temp.FirstPersQtr s
 INNER JOIN global_temp.first_contacts f 
            ON ((f.UniqServReqID = s.UniqServReqID AND f.Person_ID = s.Person_ID) 
            OR (s.OrgIDProv = 'DFC' AND f.UniqServReqID = s.UniqServReqID))
      WHERE QtrRN=1

# COMMAND ----------

# DBTITLE 1,12. Limit to one record per person and get CCG information and only selecting data for the quarter of interest
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CYPFinal AS
      SELECT c.Person_ID,
             c.UniqServReqID,
             c.OrgIDProv,
             c.OrgIDComm,
             c.UniqMonthID,
             c.ContID,
             c.ContDate,
             c.AgeCareContDate,
             c.RN1,
             c.Qtr,
             c.ContDate2,
             c.AgeCareContDate2
        FROM global_temp.ContPer c
       WHERE Qtr='${month_id}'