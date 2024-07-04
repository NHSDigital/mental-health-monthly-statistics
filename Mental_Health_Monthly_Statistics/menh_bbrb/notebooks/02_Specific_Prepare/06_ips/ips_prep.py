# Databricks notebook source
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_referrals
 SELECT 
 r.UniqServReqID 
 ,r.Person_ID
 ,r.UniqMonthID
 ,r.RecordNumber
 ,r.ReportingPeriodStartDate
 ,r.ReportingPeriodEndDate
 ,r.Der_FY
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate
 ,r.OrgIDProv
 ,r.IC_Rec_CCG
 ,'ServTeamTypeRefToMH' AS Identifier   
  
 FROM $db_output.NHSE_Pre_Proc_Referral r
 WHERE r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND  r.UniqMonthID BETWEEN '$end_month_id' -11 and '$end_month_id'
 AND r.ServTeamTypeRefToMH = 'D05' 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth ='')

# COMMAND ----------

# DBTITLE 1,Get SNOMED Interventions for IPS (All Providers)
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_interventions
 SELECT
 a.UniqMonthID,
 a.OrgIDProv,
 a.RecordNumber, 
 a.UniqServReqID,
 a.Der_SNoMEDProcQual --ADDED IN TO TEST
  
 FROM $db_output.NHSE_Pre_Proc_Interventions a
 LEFT JOIN $db_output.NHSE_Pre_Proc_Activity p ON a.UniqMonthID = p.UniqMonthID and a.OrgIDProv = p.OrgIDProv AND a.RecordNumber = p.RecordNumber AND a.UniqCareContID = p.UniqCareContID
  
 WHERE (a.Der_SNoMEDProcCode IN ('1082621000000104', '772822000') OR regexp_replace(a.Der_SNoMEDProcCode, "\n|\r", "")=1082621000000104 or regexp_replace(a.Der_SNoMEDProcCode, "\n|\r", "")=772822000)
 AND ((a.Der_SNoMEDProcQual != '443390004' OR a.Der_SNoMEDProcQual IS NULL) OR regexp_replace(a.Der_SNoMEDProcQual, "\n|\r", "")!= 443390004)
 AND (a.Der_SNoMEDProcQual != 443390004 OR a.Der_SNoMEDProcQual IS NULL)--to make sure 443390004 is filtered out and not removing nulls, POSSIBLE use case statement to handle this?
 AND p.Der_DirectContact = 1 -- and only bring in direct contacts that are F2F, video, telephone or other, codes need to be linked to snomed code 
  
 GROUP BY a.UniqMonthID, a.OrgIDProv, a.RecordNumber, a.UniqServReqID, a.Der_SNoMEDProcQual

# COMMAND ----------

# DBTITLE 1,Insert Referral Records for IPS SNOMED activity (instead of ServTeamTypeRefToMH) into ips_referrals
 %sql 
 INSERT INTO $db_output.ips_referrals
 SELECT 
     r.UniqServReqID
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.IC_Rec_CCG
     ,'SNoMED' AS Identifier
     
 FROM $db_output.NHSE_Pre_Proc_Referral r
 INNER JOIN $db_output.ips_interventions a 
 ON a.UniqMonthID = r.UniqMonthID 
 AND a.OrgIDProv = r.OrgIDProv 
 AND a.RecordNumber = r.RecordNumber 
 AND a.UniqServReqID = r.UniqServReqID -- Select records for referrals that have an IPS SNOMED intervention recorded
  
 WHERE r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND  r.UniqMonthID BETWEEN '$end_month_id' -11 AND '$end_month_id' 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth ='')

# COMMAND ----------

# DBTITLE 1,Get IPS Care Contacts for Referrals to IPS Team Types (ServTeamTypeRefToMH = 'D05')
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_activity
 SELECT
     r.UniqMonthID
     ,r.Person_ID
     ,r.RecordNumber
     ,r.OrgIDProv
     ,r.IC_Rec_CCG
     ,r.UniqServReqID
     ,r.Identifier
     ,c.UniqCareContID
     ,c.Der_ContactDate
     ,c.Der_FY
  
 FROM $db_output.ips_referrals r
  
 INNER JOIN $db_output.NHSE_Pre_Proc_Activity c ON r.RecordNumber = c.RecordNumber 
        AND r.UniqServReqID = c.UniqServReqID 
        AND c.Der_DirectContact=1
        AND  c.UniqMonthID BETWEEN '$end_month_id' -11 and '$end_month_id'
  
 WHERE r.Identifier = 'ServTeamTypeRefToMH' --bring through care contacts for IPS referrals identified via Team Type code

# COMMAND ----------

# DBTITLE 1,Insert IPS Care Contacts Referral Records for IPS SNOMED activity (instead of ServTeamTypeRefToMH) into ips_activity
 %sql
 INSERT INTO $db_output.ips_activity
  
 SELECT
      r.UniqMonthID
     ,r.Person_ID
     ,r.RecordNumber
     ,r.OrgIDProv
     ,r.IC_Rec_CCG
     ,r.UniqServReqID
     ,r.Identifier
     ,c.UniqCareContID
     ,c.Der_ContactDate
     ,c.Der_FY
  
  FROM $db_output.ips_referrals r
  
  INNER JOIN $db_output.NHSE_Pre_Proc_Activity c ON r.RecordNumber = c.RecordNumber 
     AND r.UniqServReqID = c.UniqServReqID 
     AND r.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id' 
   
  INNER JOIN $db_output.NHSE_Pre_Proc_Interventions i ON r.RecordNumber = i.RecordNumber 
     AND r.UniqServReqID = i.UniqServReqID 
     AND c.UniqCareContID = i.UniqCareContID
  
 WHERE (i.Der_SNoMEDProcCode IN ('1082621000000104', '772822000') OR regexp_replace(i.Der_SNoMEDProcCode, "\n|\r", "")=1082621000000104 or regexp_replace(i.Der_SNoMEDProcCode, "\n|\r", "")=772822000)
 AND ((i.Der_SNoMEDProcQual != '443390004' OR i.Der_SNoMEDProcQual IS NULL) OR regexp_replace(i.Der_SNoMEDProcQual, "\n|\r", "")!= 443390004)
 AND (i.Der_SNoMEDProcQual != 443390004 OR i.Der_SNoMEDProcQual IS NULL) --to make sure 443390004 is filtered out POSSIBLE use case statement to handle this?
 AND c.Der_DirectContact=1-- and only bring in direct contacts that are F2F, video, telephone or other, codes need to be linked to snomed code 
  AND r.Identifier = 'SNoMED' ---bring through contacts for IPS referrals identified via SNoMED codes

# COMMAND ----------

# DBTITLE 1,Select distinct IPS Referrals to get a single referral for referrals which flowed under ServTeamTypeRefToMH and SNOMED
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_referrals_distinct
 SELECT DISTINCT
 r.UniqServReqID
 ,r.Person_ID
 ,r.UniqMonthID
 ,r.RecordNumber
 ,r.ReportingPeriodStartDate
 ,r.ReportingPeriodEndDate
 ,r.Der_FY
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate
 ,r.OrgIDProv
 ,r.IC_Rec_CCG
  
 FROM $db_output.ips_referrals r

# COMMAND ----------

# DBTITLE 1,Select distinct IPS Care Contacts to get a single referral for referrals which flowed under ServTeamTypeRefToMH and SNOMED
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_activity_distinct
 SELECT DISTINCT
      a.UniqMonthID
     ,a.Person_ID
     ,a.RecordNumber
     ,a.OrgIDProv
     ,a.IC_Rec_CCG
     ,a.UniqServReqID
     ,a.UniqCareContID
     ,a.Der_ContactDate
     ,a.Der_FY
  
 FROM $db_output.ips_activity a

# COMMAND ----------

# DBTITLE 1,Partition Care Contacts to flag first contact per referral (ever and within financial year)
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_activity_order
  
 SELECT
     a.RecordNumber
     ,a.UniqMonthID
     ,a.Person_ID
     ,a.UniqServReqID
     ,ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID ORDER BY a.Der_ContactDate ASC) AS AccessFlag 
     ,ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID, a.Der_FY ORDER BY a.Der_ContactDate ASC) AS FYAccessFlag 
     ,a.Der_ContactDate
  
 FROM $db_output.ips_activity_distinct a

# COMMAND ----------

# DBTITLE 1,Aggregate by referral per month
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_activity_agg 
 SELECT
     a.RecordNumber
     ,a.UniqMonthID 
     ,a.Person_ID
     ,a.UniqServReqID 
     ,MIN(a.AccessFlag) AS AccessFlag
     ,MIN(a.FYAccessFlag) AS FYAccessFlag
     ,MIN(a.Der_ContactDate) AS AccessDate
     ,COUNT(*) AS TotalContacts
  
 FROM $db_output.ips_activity_order a
  
 GROUP BY a.RecordNumber, a.UniqMonthID, a.Person_ID, a.UniqServReqID  

# COMMAND ----------

# DBTITLE 1,Final Prep Table for Counts
 %sql
 INSERT OVERWRITE TABLE $db_output.ips_master
  
 SELECT 
     r.UniqServReqID
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY   
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,COALESCE(m.Age_Band, "UNKNOWN") as Age_Band
     ,COALESCE(g.Der_Gender, "UNKNOWN") as Der_Gender
     ,COALESCE(g.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc
     ,COALESCE(m.UpperEthnicity, "UNKNOWN") as UpperEthnicity
     ,COALESCE(m.IMD_Decile, "UNKNOWN") as IMD_Decile
     ,r.OrgIDProv
     ,m.OrgIDProvName as Provider_Name
     ,r.IC_Rec_CCG
     ,COALESCE(i.ccg_code,'UNKNOWN') AS CCG_Code
     ,COALESCE(i.ccg_name,'UNKNOWN') AS CCG_Name
     ,COALESCE(i.STP_CODE, 'UNKNOWN') AS STP_Code
     ,COALESCE(i.STP_NAME, 'UNKNOWN') AS STP_Name
     ,COALESCE(i.REGION_CODE, 'UNKNOWN') AS Region_Code
     ,COALESCE(i.REGION_NAME, 'UNKNOWN') AS Region_Name
     ,CASE WHEN a.AccessFlag = 1 THEN 1 ELSE 0 END AS AccessFlag 
     ,CASE WHEN a.FYAccessFlag = 1 THEN 1 ELSE 0 END AS FYAccessFlag
     ,a.AccessDate
     ,TotalContacts AS Contacts
  
 FROM $db_output.ips_referrals_distinct r 
 LEFT JOIN $db_output.ips_activity_agg a ON r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber
 LEFT JOIN $db_output.bbrb_stp_mapping i ON r.IC_Rec_CCG = i.ccg_code 
 INNER JOIN $db_output.mhs001mpi_12_months_data m ON a.Person_ID = m.Person_ID and a.RecordNumber = m.RecordNumber and a.AccessFlag = 1 ---join on recordnumber to get demographics submitted by provider at firstcontact
 LEFT JOIN $db_output.gender_desc g on m.Der_Gender = g.Der_Gender and '$end_month_id' >= g.FirstMonth and (g.LastMonth is null or '$end_month_id' <= g.LastMonth)

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.ips_master