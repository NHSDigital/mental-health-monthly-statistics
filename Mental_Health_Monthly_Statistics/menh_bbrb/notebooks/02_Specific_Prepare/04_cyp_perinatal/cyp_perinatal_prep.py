# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY VIEW Ref AS
 SELECT
     r.UniqMonthID,
     r.OrgIDProv,
     CASE 
       WHEN r.OrgIDProv = 'DFC' THEN CONCAT(r.OrgIDProv, r.LocalPatientID)
       ELSE r.Person_ID
       END AS Person_ID,
     r.RecordNumber,
     r.UniqServReqID,
     Case when r.OrgIDProv = 'DFC' then r.OrgIDComm
          else r.IC_Rec_CCG end as Der_OrgComm,
     r.LADistrictAuth,
     r.AgeServReferRecDate,
     r.AgeRepPeriodEnd
  
 FROM $db_output.nhse_pre_proc_referral r
  
 WHERE r.AgeServReferRecDate BETWEEN 0 AND 17 AND 
 r.UniqMonthID BETWEEN '$end_month_id' -11 AND '$end_month_id' 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = '')

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Comb AS
 SELECT
     CASE 
       WHEN c.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
       ELSE c.Person_ID
       END AS Person_ID,
     c.RecordNumber,
     c.UniqServReqID,
     c.CareContDate AS Der_ContactDate,
     c.AgeCareContDate
  
 FROM $db_source.MHS201CareContact c
 LEFT JOIN $db_source.MHS001MPI m ON c.RecordNumber = m.RecordNumber
  
 WHERE 
   (
     ( c.AttendStatus IN ('5', '6') and ((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')))   
 -------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data /*** updated to v5 AM ***/
     or 
     ( ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')) and c.OrgIdProv = 'DFC') 
    )-------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data V5.0 /*** updated to v5 AM ***/
 UNION ALL
  
 SELECT
     CASE 
       WHEN i.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
       ELSE i.Person_ID
       END AS Person_ID,
     i.RecordNumber,
     i.UniqServReqID,
     i.IndirectActDate AS Der_ContactDate,
     NULL AS AgeCareContDate
  
 FROM $db_source.MHS204IndirectActivity i
 LEFT JOIN $db_source.MHS001MPI m ON i.RecordNumber = m.RecordNumber

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Act AS 
  
 SELECT
     r.UniqMonthID,
     r.OrgIDProv,
     r.Der_OrgComm,
     r.LADistrictAuth,
     r.Person_ID,
     r.RecordNumber,
     r.UniqServReqID,
     COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) AS Der_ContAge,
     a.Der_ContactDate
  
 FROM Comb a
 INNER JOIN Ref r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID
  
 WHERE COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) BETWEEN 0 AND 17

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Act_cumulative AS 
  
 SELECT
     r.UniqMonthID,
     r.OrgIDProv,
     Case when r.OrgIDProv = 'DFC' then r.OrgIDComm
          else ccg.SubICBGPRes end as Der_OrgComm,
     r.Person_ID,
     r.RecordNumber,
     r.UniqServReqID,
     a.AgeCareContDate AS Der_ContAge,
     r.ReferralRequestReceivedDate,
     r.ServDischDate,
     a.Der_ContactDate,
     DENSE_RANK() OVER (PARTITION BY r.UniqServReqID ORDER BY a.Der_ContactDate ASC, A.AgeCareContDate ASC) AS Der_ContactOrder
  
 FROM $db_source.mhs101referral r
  
 LEFT JOIN Comb a ON a.UniqServReqID = r.UniqServReqID AND a.AgeCareContDate BETWEEN 0 AND 17
  
 LEFT JOIN $db_source.mhs001mpi m 
       ON ((M.RecordEndDate is null or M.RecordEndDate >= '$rp_enddate') and m.recordstartdate < '$rp_enddate') and r.person_id = m.person_id and r.orgidprov = m.orgidprov AND M.PatMRecInRp = True
       
 LEFT JOIN $db_output.bbrb_ccg_in_year ccg on m.Person_ID = ccg.Person_ID
  
 WHERE 
 ((r.RecordEndDate is null or r.RecordEndDate >= '$rp_enddate') and r.recordstartdate < '$rp_enddate')
 AND r.AgeServReferRecDate BETWEEN 0 AND 17  
 AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR LADistrictAuth = '')
 AND ReferralRequestReceivedDate >= '2019-04-01'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.Act_cumulative_master
 SELECT 
 DISTINCT 
 UniqMonthID,
 OrgIDProv,
 Der_OrgComm,
 Person_ID,
 RecordNumber,
 UniqServReqID,
 Der_ContAge,
 ReferralRequestReceivedDate,
 ServDischDate,
 Der_ContactDate,
 Der_ContactOrder,
 o.NAME AS PROVIDER_NAME,
 COALESCE(c.CCG21CDH,'UNKNOWN') AS CCG_Code,
 COALESCE(c.CCG21NM,'UNKNOWN') AS CCG_Name,
 COALESCE(c.STP21CDH,'UNKNOWN') AS STP_Code,
 COALESCE(c.STP21NM,'UNKNOWN') AS STP_Name,
 COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code,
 COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name,
 DATEDIFF(Der_ContactDate, ReferralRequestReceivedDate) as TimeFromRefToFirstCont,
 DATEDIFF(DATE_ADD('$rp_enddate',1), ReferralRequestReceivedDate) as TimeFromRefToEndRP
 FROM Act_cumulative A
 LEFT JOIN $db_output.CCG_MAPPING_2021 C on a.DER_ORGCOMM = C.CCG_UNMAPPED
 LEFT JOIN $db_output.bbrb_org_daily_latest o on a.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.Act_cumulative_master_first_contact
 SELECT DISTINCT * FROM $db_output.Act_cumulative_master A
 WHERE DER_CONTACTORDER = '1' 
 AND Der_ContactDate BETWEEN '$rp_startdate_qtr' and '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.Act_cumulative_master_still_waiting
  
 SELECT DISTINCT * FROM $db_output.Act_cumulative_master A
 WHERE DER_CONTACTORDER = '1' 
 AND DER_CONTACTDATE IS NULL 
 AND UNIQMONTHID = '$end_month_id'
 AND (SERVDISCHDATE IS NULL OR SERVDISCHDATE > '$rp_enddate')

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Ref109 AS
  
 SELECT
     r.UniqMonthID,
     r.OrgIDProv,
     CASE 
       WHEN r.OrgIDProv = 'DFC' THEN CONCAT(r.OrgIDProv, r.LocalPatientID)
       ELSE r.Person_ID
       END AS Person_ID,
     r.RecordNumber,
     r.UniqServReqID,
     Case when r.OrgIDProv = 'DFC' then r.OrgIDComm
          else r.IC_Rec_CCG end as Der_OrgComm,
     r.LADistrictAuth,
     r.AgeServReferRecDate,    
     r.AgeRepPeriodEnd
  
 FROM $db_output.nhse_pre_proc_referral r
  
 WHERE r.AgeServReferRecDate BETWEEN 18 AND 24 AND 
 r.UniqMonthID BETWEEN '$end_month_id' -11 AND '$end_month_id' 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = '')

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Act109 AS 
  
 SELECT
     r.UniqMonthID,
     r.OrgIDProv,
     r.Der_OrgComm,
     r.LADistrictAuth,
     r.Person_ID,
     r.RecordNumber,
     r.UniqServReqID,
     COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) AS Der_ContAge,
     a.Der_ContactDate
  
 FROM Comb a
 INNER JOIN Ref109 r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID
  
 WHERE COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) BETWEEN 18 AND 24

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.FirstCont_Final
 SELECT
     a.UniqMonthID,
     a.OrgIDProv,
     o.NAME AS Prrovider_Name,
     COALESCE(c.CCG21CDH,'UNKNOWN') AS CCG_Code,
     COALESCE(c.CCG21NM,'UNKNOWN') AS CCG_Name,
     COALESCE(c.STP21CDH,'UNKNOWN') AS STP_Code,
     COALESCE(c.STP21NM,'UNKNOWN') AS STP_Name,
     COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code,
     COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name,
     a.LADistrictAuth,
     a.Person_ID,
     a.RecordNumber,
     a.UniqServReqID,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC) AS AccessLARN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH ORDER BY a.Der_ContactDate ASC) AS AccessCCGRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessCCGProvRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessProvRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC) AS AccessEngRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.STP21CDH ORDER BY a.Der_ContactDate ASC) AS AccessSTPRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.NHSER21CDH ORDER BY a.Der_ContactDate ASC) AS AccessRegionRN,
     'MHS95' AS Metric-- add metric id to table
  
 FROM Act as A
 LEFT JOIN $db_output.CCG_MAPPING_2021 C on a.DER_ORGCOMM = C.CCG_UNMAPPED
 LEFT JOIN $db_output.bbrb_org_daily_latest  o on a.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.FirstCont_Final ---Using insert into here as inserting data into table above but for different Metric
  
 SELECT
     a.UniqMonthID,
     a.OrgIDProv,
     o.NAME AS Provider_Name,
     COALESCE(c.CCG21CDH,'UNKNOWN') AS CCG_Code,
     COALESCE(c.CCG21NM,'UNKNOWN') AS CCG_Name,
     COALESCE(c.STP21CDH,'UNKNOWN') AS STP_Code,
     COALESCE(c.STP21NM,'UNKNOWN') AS STP_Name,
     COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code,
     COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name,
     a.LADistrictAuth,
     a.Person_ID,
     a.RecordNumber,
     a.UniqServReqID,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC) AS AccessLARN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH ORDER BY a.Der_ContactDate ASC) AS AccessCCGRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessCCGProvRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessProvRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC) AS AccessEngRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.STP21CDH ORDER BY a.Der_ContactDate ASC) AS AccessSTPRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.NHSER21CDH ORDER BY a.Der_ContactDate ASC) AS AccessRegionRN,
     'MHS109' AS Metric-- add metric id to table
  
 FROM Act109 as A
 LEFT JOIN $db_output.CCG_MAPPING_2021 C on a.Der_OrgComm = C.CCG_UNMAPPED
 LEFT JOIN $db_output.bbrb_org_daily_latest  o on a.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.FirstCont_Final

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW perinatal_refs AS
  
 SELECT DISTINCT
 r.UniqMonthID,
 r.RecordNumber,
 r.Person_ID,
 r.ServiceRequestId,
 r.UniqServReqID,
 r.OrgIDProv,
 r.ReferralRequestReceivedDate,
 COALESCE(mp.CCG21CDH, 'UNKNOWN') AS IC_Rec_CCG,
 COALESCE(mp.CCG21NM, 'UNKNOWN') AS CCG_NAME,
 m.LADistrictAuth,
 coalesce(eth.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode,
 coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName,
 coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity,
 COALESCE(mp.STP21CDH, 'UNKNOWN') AS STP_Code,
 COALESCE(mp.STP21NM, 'UNKNOWN') AS STP_Name,
 COALESCE(mp.NHSER21CDH, 'UNKNOWN') AS Region_Code,
 COALESCE(mp.NHSER21NM, 'UNKNOWN') AS Region_Name
  
 FROM $db_source.MHS101Referral r
 INNER JOIN $db_output.ServiceTeamType s
 ON s.RecordNumber = r.RecordNumber
 AND s.UniqServReqID = r.UniqServReqID
 AND s.ServTeamTypeRefToMH in ('C02', 'F02') --Specialist Perinatal Services 
 and s.uniqmonthid between '$end_month_id'-11 AND '$end_month_id'
  
 INNER JOIN $db_source.MHS001MPI m
 ON m.recordnumber = r.recordnumber
 AND CASE
         WHEN '$end_month_id' >= 1477 and m.GenderIDCode in ('2','3') THEN '1' -- Gender Identity Code is female or non-binary (including trans women)
         WHEN '$end_month_id' >= 1477 and m.GenderIDCode= '1' and m.GenderSameAtBirth = 'N' THEN '1' -- Gender identity code is Male but Gender is not same as birth
         WHEN '$end_month_id' >= 1477 and m.GenderIDCode is null and m.Gender = '2' THEN '1' -- Gender identity not recorded so Female Person Stated Gender
         WHEN '$end_month_id' < 1477 and m.GenderIDCode = '2' THEN '1'
         WHEN '$end_month_id' < 1477 and (m.GenderIDCode is null or m.GenderIDCode not IN ('1','2','3','4','X','Z')) and m.Gender = '2' THEN '1' 
         ELSE '0' 
         END = '1' -- updated added new gender field jan 2022
 AND (m.LADistrictAuth IS NULL OR m.LADistrictAuth LIKE ('E%') or ladistrictauth = '')
 and m.uniqmonthid between '$end_month_id'-11 AND '$end_month_id'
  
 LEFT JOIN $db_output.ethnicity_desc eth on m.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
  
 LEFT JOIN $db_output.bbrb_ccg_in_year ccg on m.Person_ID = ccg.Person_ID
  
 LEFT JOIN $db_output.CCG_mapping_2021 mp 
 on ccg.SubICBGPRes = mp.CCG_unmapped
  
 WHERE r.UniqMonthID BETWEEN '$end_month_id'-11 AND '$end_month_id'

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW conts12 AS
 ---Prior October 2021
 SELECT
 r.UniqMonthID,
 r.Person_ID,
 r.RecordNumber,
 r.LADistrictAuth,
 r.LowerEthnicityCode,
 r.LowerEthnicityName,
 r.OrgIDProv,
 r.IC_Rec_CCG,
 r.UniqServReqID,
 c.UniqCareContID,
 c.CareContDate AS Der_ContactDate,
 r.STP_Code,
 r.Region_Code
  
 FROM perinatal_refs r
 INNER JOIN $db_source.MHS201CareContact c 
 ON r.RecordNumber = c.RecordNumber 
 AND r.UniqServReqID = c.UniqServReqID 
 AND c.AttendStatus IN ('5','6') AND c.ConsMechanismMH IN ('01', '03') and c.UniqMonthid < '1459'
  
 UNION ALL
 ---After October 2021 (dataset version change)
  
 SELECT
 r.UniqMonthID,
 r.Person_ID,
 r.RecordNumber,
 r.LADistrictAuth,
 r.LowerEthnicityCode,
 r.LowerEthnicityName,
 r.OrgIDProv,
 r.IC_Rec_CCG,
 r.UniqServReqID,
 c.UniqCareContID,
 c.CareContDate AS Der_ContactDate,
 r.STP_Code,
 r.Region_Code
  
 FROM perinatal_refs r
 INNER JOIN $db_source.MHS201CareContact c 
 ON r.RecordNumber = c.RecordNumber 
 AND r.UniqServReqID = c.UniqServReqID 
 AND c.AttendStatus IN ('5','6') AND c.ConsMechanismMH IN ('01', '11') and c.UniqMonthid >= '1459'

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW contYTD AS
  
 SELECT
 c.UniqMonthID,
 c.Person_ID,
 c.RecordNumber,
 c.LADistrictAuth,
 c.OrgIDProv,
 c.IC_Rec_CCG,
 c.UniqServReqID,
 c.UniqCareContID,
 c.STP_Code,
 c.Region_Code,
 ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.LADistrictAuth ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessLARN,
 ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.IC_Rec_CCG ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessCCGRN,
 ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDProv ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessProvRN,
 ROW_NUMBER () OVER(PARTITION BY c.Person_ID ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessEngRN,
 ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.STP_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessSTPRN,
 ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.Region_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRegionRN
  
 FROM Conts12 c
  
 WHERE c.UniqMonthID BETWEEN '$end_month_id'-11 AND '$end_month_id'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.Perinatal_M_Master
  
 SELECT DISTINCT
 r.UniqMonthID,
 r.Person_ID,
 r.UniqServReqID,
 r.OrgIDProv,
 o.NAME as Provider_Name,
 COALESCE(r.IC_Rec_CCG, "UNKNOWN") as CCG_Code,
 COALESCE(o2.NAME, "UNKNOWN") as CCG_Name, --Changed to pull out latest CCG Name
 r.STP_Code,
 STP_NAME as STP_Name,
 r.REGION_CODE as Region_Code,
 REGION_NAME as Region_Name,
 COALESCE(r.LADistrictAuth,'Unknown') AS LACode,
 r.LowerEthnicityCode,
 r.LowerEthnicityName,
 r.UpperEthnicity,
 CASE WHEN c1.Contacts >0 THEN 1 ELSE NULL END AS AttContacts,
 CASE WHEN c1.Contacts = 0 THEN 1 ELSE NULL END AS NotAttContacts,
 c2.FYAccessLARN, 
 c2.FYAccessProvRN,
 c2.FYAccessCCGRN,
 c2.FYAccessSTPRN,
 c2.FYAccessRegionRN,
 c2.FYAccessEngRN
  
 FROM perinatal_refs r    
  
 LEFT JOIN 
        (
        SELECT
        c.UniqMonthID,
        c.RecordNumber,
        c.UniqServReqID,
        COUNT(c.UniqCareContID) AS Contacts
        FROM Conts12 c
        GROUP BY c.UniqMonthID, c.RecordNumber, c.UniqServReqID
        ) c1 ON r.RecordNumber = c1.RecordNumber AND r.UniqServReqID = c1.UniqServReqID
  
 LEFT JOIN ContYTD c2 
        ON r.RecordNumber = c2.RecordNumber
        AND r.UniqServReqID = c2.UniqServReqID
        AND (c2.FYAccessProvRN = 1 
             OR c2.FYAccessCCGRN = 1
             OR c2.FYAccessSTPRN = 1
             OR c2.FYAccessRegionRN = 1)   
    
 LEFT JOIN $db_output.bbrb_org_daily_latest o
       ON r.OrgIDProv = o.ORG_CODE 
    
 LEFT JOIN $db_output.bbrb_org_daily_latest o2 --Added to pull out latest CCG Name
       ON r.IC_Rec_CCG = o2.ORG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Perinatal_M_Master