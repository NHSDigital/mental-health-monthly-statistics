# Databricks notebook source
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Referral data
 %sql
 --This cell gets all referrals where service team type referred to was EIP (A14). 
 --It joins on record number to get the demographics at the time rather latest information used elsewhere in the monthly publication.
 --Was EIP_Procedures
 TRUNCATE TABLE $db_output.EIP_Referrals;
 
 INSERT INTO        $db_output.EIP_Referrals
 SELECT	            r.MHS101UniqID,
                     r.UniqMonthID,
                     r.OrgIDProv,
                     r.Person_ID,
                     r.RecordNumber,
                     r.UniqServReqID,
                     r.ReferralRequestReceivedDate,
                     r.ServDischDate,
                     s.ReferClosReason,
                     s.ReferRejectionDate,
                     s.ReferRejectReason,
                     s.UniqCareProfTeamID,
                     s.ServTeamTypeRefToMH,
                     r.PrimReasonReferralMH,
                     CASE 
                        WHEN m.UniqMonthID <= 1467 AND m.OrgIDCCGRes IS not null then m.OrgIDCCGRes
                        WHEN m.UniqMonthID > 1467 AND m.OrgIDSubICBLocResidence IS not null then m.OrgIDSubICBLocResidence
                        ELSE 'UNKNOWN' end as IC_RESIDENCE
 FROM                $db_source.mhs101referral r
 INNER JOIN          $db_source.mhs001mpi m 
                     ON r.RecordNumber = m.RecordNumber ---joining on recordnumber opposed to person_id as we want OrgIDCCGRes as it was inputted when referral was submitted in that month
 LEFT JOIN           $db_source.mhs102servicetypereferredto s 
                    ON r.UniqServReqID = s.UniqServReqID 
                    AND r.RecordNumber = s.RecordNumber --joining on recordnumber aswell to match historic records as they will all have the same uniqservreqid
 WHERE               s.ServTeamTypeRefToMH = 'A14' --service type referred to is always Early Intervention Team for Psychosis /***
                    AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR m.LADistrictAuth = '') --to include England or blank Local Authorities only
       

# COMMAND ----------

 %sql
 -- To compatible for Spark 3, few fields were wrapped with try cast (NULL should NULL not 'NULL' as string)
 TRUNCATE TABLE $db_output.EIP_Pre_Proc_Activity;
  
 INSERT INTO      $db_output.EIP_Pre_Proc_Activity
 SELECT
     'DIRECT' AS Der_ActivityType,
     TRY_CAST(c.MHS201UniqID AS BIGINT) AS Der_ActivityUniqID,
     c.Person_ID,
     c.UniqMonthID,
     c.OrgIDProv,
     c.RecordNumber,
     c.UniqServReqID,
     c.OrgIDComm,
     TRY_CAST(c.CareContDate as date) AS Der_ContactDate,
     TRY_CAST(c.CareContTime as timestamp) AS Der_ContactTime,
     cast(unix_timestamp(cast(concat(cast(c.CareContDate as string), right(cast(c.CareContTime as string), 9)) as timestamp)) as timestamp) as Der_ContactDateTime,
     c.AdminCatCode,
     TRY_CAST(c.SpecialisedMHServiceCode AS BIGINT),
     c.ClinContDurOfCareCont AS Der_ContactDuration,
     c.ConsType,
     c.CareContSubj,
     c.ConsMechanismMH,                                 -------/*** V5 change - ConsMediumUsed' will change to 'ConsMechanismMH***/
     c.ActLocTypeCode,
     c.SiteIDOfTreat,
     c.GroupTherapyInd,
     c.AttendOrDNACode,
     c.EarliestReasonOfferDate,
     c.EarliestClinAppDate,
     c.CareContCancelDate,
     c.CareContCancelReas,
     c.RepApptOfferDate,
     c.RepApptBookDate,
     c.UniqCareContID,
     c.AgeCareContDate,
     c.ContLocDistanceHome,
     c.TimeReferAndCareContact,
     c.UniqCareProfTeamID AS Der_UniqCareProfTeamID,
     c.PlaceOfSafetyInd,
     CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services (such as Kooth) where personID may change every month
     'NULL' AS Der_ContactOrder,
     'NULL' AS Der_FYContactOrder,
     'NULL' AS Der_DirectContactOrder,
     'NULL' AS Der_FYDirectContactOrder,
     'NULL' AS Der_FacetoFaceContactOrder,
     'NULL' AS Der_FYFacetoFaceContactOrder
  
     
 FROM $db_source.mhs201carecontact c
  
 UNION ALL
  
 SELECT
     'INDIRECT' AS Der_ActivityType,
     TRY_CAST(i.MHS204UniqID AS BIGINT) AS Der_ActivityUniqID,
     i.Person_ID,
     i.UniqMonthID,
     i.OrgIDProv,
     i.RecordNumber,
     i.UniqServReqID,
     i.OrgIDComm,
     TRY_CAST(i.IndirectActDate  as date) AS Der_ContactDate,
     TRY_CAST(i.IndirectActTime as timestamp) AS Der_ContactTime,
     cast(unix_timestamp(cast(concat(cast(i.IndirectActDate as string), right(cast(i.IndirectActTime as string), 9)) as timestamp)) as timestamp) as Der_ContactDateTime,
     'NULL' AS AdminCatCode,
     NULL AS SpecialisedMHServiceCode,
     i.DurationIndirectAct AS Der_ContactDuration,
     'NULL' AS ConsType,
     'NULL' AS CareContSubj,
     'NULL' AS ConsMechanismMH,                                             -------/*** V5 change - ConsMediumUsed' will change to 'ConsMechanismMH***/
     'NULL' AS ActLocTypeCode,
     'NULL' AS SiteIDOfTreat,
     'NULL' AS GroupTherapyInd,
     'NULL' AS AttendOrDNACode,
     'NULL' AS EarliestReasonOfferDate,
     'NULL' AS EarliestClinAppDate,
     'NULL' AS CareContCancelDate,
     'NULL' AS CareContCancelReas,
     'NULL' AS RepApptOfferDate,
     'NULL' AS RepApptBookDate,
     'NULL' AS UniqCareContID,
     'NULL' AS AgeCareContDate,
     'NULL' AS ContLocDistanceHome,
     'NULL' AS TimeReferAndCareContact,
     i.OrgIDProv + i.CareProfTeamLocalId AS Der_UniqCareProfTeamID,
     'NULL' AS PlaceOfSafetyInd,
     CASE WHEN i.OrgIDProv = 'DFC' THEN '1' ELSE i.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
     'NULL' AS Der_ContactOrder,
     'NULL' AS Der_FYContactOrder,
     'NULL' AS Der_DirectContactOrder,
     'NULL' AS Der_FYDirectContactOrder,
     'NULL' AS Der_FacetoFaceContactOrder,
     'NULL' AS Der_FYFacetoFaceContactOrder
     
 FROM $db_source.mhs204indirectactivity i

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.EIP_Activity;
 
 INSERT INTO $db_output.EIP_Activity
 ---filtering the above table to only show indirect and direct attended activity and also ordering the contact datetime for each Unique Activity ID
 SELECT 
 a.Der_ActivityType,
 a.Der_ActivityUniqID,
 a.Person_ID,
 a.Der_PersonID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.RecordNumber,
 a.UniqServReqID,	
 a.Der_ContactDate,
 a.Der_ContactTime,
 a.Der_ContactDateTime,
 ROW_NUMBER() OVER (PARTITION BY a.Der_PersonID, a.UniqServReqID ORDER BY a.Der_ContactDateTime ASC, a.Der_ActivityUniqID ASC) AS Der_ContactOrder
 FROM $db_output.EIP_Pre_Proc_Activity a
 WHERE (a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (((a.ConsMechanismMH NOT IN ('05', '06') and a.UniqMonthID < '1459') OR (a.ConsMechanismMH IN ('01', '02', '04', '11') and a.UniqMonthID >= '1459')) OR a.OrgIDProv = 'DFC' AND ((a.ConsMechanismMH IN ('05', '06') and a.UniqMonthID < '1459') OR (a.ConsMechanismMH IN ('05', '09', '10', '13') and a.UniqMonthID >= '1459')))) OR a.Der_ActivityType = 'INDIRECT'

# COMMAND ----------

# DBTITLE 1,Contacts data
 %sql
 TRUNCATE TABLE $db_output.EIP_Pre_Proc_Interventions;
 
 --This cell looks at all activity in the MHS 202 and MHS204 tables. It takes the left position for all snomed codes.
 INSERT INTO      $db_output.EIP_Pre_Proc_Interventions 
 SELECT                ca.RecordNumber,
                       ca.Person_ID,
                       ca.UniqMonthID,
                       ca.UniqServReqID,
                       ca.UniqCareContID,
                       cc.CareContDate AS Der_ContactDate,
                       ca.UniqCareActID,
                       ca.MHS202UniqID as Der_InterventionUniqID,
                       ca.CodeProcAndProcStatus as CodeProcAndProcStatus, --gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"
                       CASE WHEN position(':' in ca.CodeProcAndProcStatus) > 0 THEN LEFT(ca.CodeProcAndProcStatus, position (':' in ca.CodeProcAndProcStatus)-1) 
                            ELSE ca.CodeProcAndProcStatus
                            END AS Der_SNoMEDProcCode,
                       ca.CodeObs
 FROM                  $db_source.mhs202careactivity ca
 LEFT JOIN             $db_source.mhs201carecontact cc 
                       ON ca.RecordNumber = cc.RecordNumber 
                       AND ca.UniqCareContID = cc.UniqCareContID
 WHERE                 (ca.CodeFind IS NOT NULL OR ca.CodeObs IS NOT NULL OR ca.CodeProcAndProcStatus IS NOT NULL)
 
 UNION ALL
 SELECT 	              i.RecordNumber,
                       i.Person_ID,
                       i.UniqMonthID,
                       i.UniqServReqID,
                       'null' as UniqCareContID,
                       i.IndirectActDate AS Der_ContactDate,
                       'null' as UniqCareActID,
                       i.MHS204UniqID as Der_InterventionUniqID,
                       i.CodeIndActProcAndProcStatus as CodeProcAndProcStatus,  /*** CodeProcAndProcStatus renamed to CodeIndActProcAndProcStatus ***/
                       --gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"
                       CASE WHEN position(':' in i.CodeIndActProcAndProcStatus) > 0 THEN LEFT(i.CodeIndActProcAndProcStatus, position (':' in i.CodeIndActProcAndProcStatus)-1) 
                            ELSE i.CodeIndActProcAndProcStatus /*** CodeProcAndProcStatus 4 occurrences renamed to CodeIndActProcAndProcStatus ***/
                            END AS Der_SNoMEDProcCode,
                       NULL AS CodeObs
 FROM                  $db_source.mhs204indirectactivity i
 WHERE                 (i.CodeFind IS NOT NULL OR i.CodeIndActProcAndProcStatus IS NOT NULL)

# COMMAND ----------

# DBTITLE 1,Processing contacts data
 %sql
 --This cell makes sure we are only looking at valid snomed codes and fully specified name we require using the reference data type_id, and making sure valid snomed code
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW EIP_Interventions_DMS AS
 
 SELECT DISTINCT   RecordNumber,
                   Person_ID,
                   UniqServReqID,
                   Der_ContactDate,
                   UniqCareContID,                  
                   UniqCareActID,
                   Der_InterventionUniqID,                
                   CodeObs,
                   s3.Term AS Der_SNoMEDObsTerm,
                   Der_SNoMEDProcCode,
                   CodeProcAndProcStatus,  
                   s1.Term AS Der_SNoMEDProcTerm
                   
 FROM              $db_output.EIP_Pre_Proc_Interventions i
 LEFT JOIN         $db_output.SCT_Concepts_FSN s1 
                   ON i.Der_SNoMEDProcCode = s1.ID              
 LEFT JOIN         $db_output.SCT_Concepts_FSN s3 
                   ON i.CodeObs = s3.ID               

# COMMAND ----------

# DBTITLE 1,Listing relevant snomed codes for EIP
 %sql
 --Lists all the NICE concordant snomed codes. The contact data needs to be between the referral start date and the rp_enddate, and joined back on with record number. 
 --The EIP referral can include contacts from other referrals within the same provider for the same person for the same month period. Only Contacts with recorded valid SNOMED codes will be brought through
 truncate table $db_output.EIP_Snomed;
 
 INSERT INTO      $db_output.EIP_Snomed
 SELECT              r.Person_ID,
                     r.UniqServReqID,
                     r.UniqMonthID,
                     r.RecordNumber,
                     c.Der_InterventionUniqID,
                     c.CodeProcAndProcStatus,
                     c.Der_SNoMEDProcCode,
                     c.CodeObs,
                     COALESCE(c.Der_SNoMEDProcTerm, c.Der_SNoMEDObsTerm) as Der_SNoMEDTerm                    ,           
                     ---Checked this list against EIP Recording and Reporting Document from Jan 2019
                     CASE WHEN (c.Der_SNoMEDProcCode =n.conceptid 
                     --duplicated these codes to pick up those recorded as OBS
                             OR c.CodeObs =o.conceptid)
                   THEN 'NICE concordant' 
 	WHEN COALESCE(c.Der_SNoMEDProcCode,c.CodeObs) IS NOT NULL THEN 'Other' 
 	END AS Intervention_type
 FROM              $db_output.EIP_Referrals r
 INNER JOIN        global_temp.EIP_Interventions_DMS c ON r.recordnumber = c.recordnumber
                   ---joining on record number so will be also bringing in activity for the person in the month which may not be associated with EIP referral
                  AND COALESCE(c.Der_SNoMEDProcTerm, c.Der_SNoMEDObsTerm) IS NOT NULL 
                  AND c.Der_ContactDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate, '$rp_enddate') AND r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL
  left join $db_output.EIP_Nice_Snomed n on n.conceptid = c.Der_SNoMEDProcCode
   left join $db_output.EIP_Nice_Snomed o on o.conceptid = c.CodeObs

# COMMAND ----------

 %sql
 truncate table $db_output.EIP_Snomed_Agg;
 
 INSERT INTO        $db_output.EIP_Snomed_Agg 
 ---This table aggregates every contact for each uniqservreqid for every month for each provider. 
 ---As only contacts with a valid and recorded SNOMED-CT code are in Procedures_v2 where contact was
 ---between the referral start date and the rp_enddate, we can use a count of this table to 
 SELECT
 	r.UniqMonthID,
 	r.OrgIDProv,
 	r.RecordNumber,
 	r.UniqServReqID,
 	COUNT(Intervention_type) AS AnySNoMED,
 	SUM(CASE WHEN a.Intervention_type = 'NICE concordant' THEN 1 ELSE 0 END) AS NICESNoMED
     
 FROM $db_output.EIP_Referrals r    
     
 LEFT JOIN $db_output.EIP_Snomed a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID
 GROUP BY r.RecordNumber, r.UniqServReqID, r.UniqMonthID, r.OrgIDProv

# COMMAND ----------

 %sql
 TRUNCATE TABLE  $db_output.EIP_Activity_Agg;
 
 INSERT INTO   $db_output.EIP_Activity_Agg SELECT
 	r.Person_ID,
     r.UniqMonthID,
 	r.RecordNumber,
 	r.UniqServReqID,	
 	--in month activity
 	COUNT(CASE WHEN a.Der_ContactOrder IS NOT NULL AND Der_ActivityType = 'DIRECT' THEN a.RecordNumber END) AS Der_InMonthContacts
     
 FROM $db_output.EIP_Referrals r   
 
 INNER JOIN $db_output.EIP_Activity a ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID
 
 GROUP BY r.Person_ID, r.UniqMonthID, r.RecordNumber, r.UniqServReqID

# COMMAND ----------

 %sql
 TRUNCATE TABLE  $db_output.EIP_Activity_Cumulative;
 INSERT INTO $db_output.EIP_Activity_Cumulative
 SELECT
 	r.Person_ID,
     r.UniqMonthID,
 	r.RecordNumber,
 	r.UniqServReqID,
 	SUM(COALESCE(a.Der_InMonthContacts, 0)) AS Der_CumulativeContacts
 
 FROM $db_output.EIP_Referrals r  
 
 LEFT JOIN $db_output.EIP_Activity_Agg a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.UniqMonthID <= r.UniqMonthID
 
 GROUP BY r.Person_ID, r.UniqMonthID, r.RecordNumber, r.UniqServReqID

# COMMAND ----------

# DBTITLE 1,Master prep table
 %sql
 --Pulls together final preperation table
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW EIP_master_DMS AS
 
 SELECT          r.UniqMonthID,
                 r.OrgIDProv,
                 r.RecordNumber,
                 r.UniqServReqID,
                 r.person_id, 
                 cu.Der_CumulativeContacts,
                 p.AnySNoMED,
                 p.NICESNoMED,
                 coalesce(od.NAME, 'UNKNOWN') as PROVIDER_NAME,
                 coalesce(ccg.org_code,'UNKNOWN') as CCG_CODE,
                 coalesce(ccg.name,'UNKNOWN') as CCG_NAME,
                 coalesce(stpre.STP_CODE,'UNKNOWN') as STP_CODE,
                 coalesce(stpre.STP_DESCRIPTION,'UNKNOWN') as STP_NAME,
                 coalesce(stpre.REGION_CODE,'UNKNOWN') as REGION_CODE,
                 coalesce(stpre.REGION_DESCRIPTION,'UNKNOWN') as REGION_NAME,
                 -- get caseload measures
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL THEN 1 ELSE 0 END AS Open_Referrals,
 	            CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND cu.Der_CumulativeContacts >=1 THEN 1 ELSE 0 END AS EIP_Caseload, ---EIP68
 
                 -- get aggregate SNoMED measures
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND p.AnySNoMED > 0 THEN 1 ELSE 0 END AS RefWithAnySNoMED, ---EIP69a
                 CASE WHEN r.ServDischDate IS NULL AND r.ReferRejectionDate IS NULL AND p.NICESNoMED > 0 THEN 1 ELSE 0 END AS RefWithNICESNoMED ---EIP6b
 
 FROM            $db_output.EIP_Referrals r 
 LEFT JOIN       $db_output.EIP_Snomed_Agg p ON r.RecordNumber = p.RecordNumber AND r.UniqServReqID = p.UniqServReqID
 LEFT JOIN       $db_output.EIP_Activity_Cumulative cu ON r.RecordNumber = cu.RecordNumber AND r.UniqServReqID = cu.UniqServReqID
 LEFT JOIN       $db_output.RD_CCG_LATEST ccg --get the latest ccg reference data
                 ON r.IC_RESIDENCE = ccg.org_code
 LEFT JOIN       $db_output.STP_Region_mapping_post_2020 stpre
                 ON ccg.org_code = stpre.CCG_CODE
 LEFT JOIN       global_temp.org_daily od
                 ON r.OrgIDProv = od.ORG_CODE
 WHERE           (ServDischDate IS NULL AND ReferRejectionDate IS NULL)            
 AND r.UniqMonthID = '$month_id'

# COMMAND ----------

 %sql
 ---Final data check
 select sum(Open_Referrals),sum(EIP_Caseload),sum(RefWithAnySNoMED),sum(RefWithNICESNoMED) from global_temp.EIP_Master_DMS

# COMMAND ----------

# DBTITLE 1,Insert Caseload Breakdowns - EIP68
 %sql
 -- Single Count - Count of open referrals with atleast one attended contact that were linked to an EIP team, regardless of the primary reason for referral, that were not rejected.
 
 INSERT INTO $db_output.awt_unformatted
 --England breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS LEVEL,
  'NONE' AS LEVEL_DESCRIPTION,
    'EIP68' AS METRIC,
    sum(EIP_Caseload) AS METRIC_VALUE,
    '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
  'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS LEVEL,
   PROVIDER_NAME AS LEVEL_DESCRIPTION,
    'EIP68' AS METRIC,
   sum(EIP_Caseload) AS METRIC_VALUE ,
  '$db_source' as SOURCE_DB,
  'NONE' AS SECONDARY_LEVEL,
  'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  OrgIDProv, PROVIDER_NAME
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN, -- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency
   CCG_CODE AS LEVEL,
   CCG_NAME AS LEVEL_DESCRIPTION,
     'EIP68' AS METRIC,
   sum(EIP_Caseload) AS METRIC_VALUE,
   '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS LEVEL,
   REGION_NAME AS LEVEL_DESCRIPTION,
     'EIP68' AS METRIC,
    sum(EIP_Caseload) AS METRIC_VALUE , 
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP - GP Practice or Residence' AS BREAKDOWN,
   STP_CODE AS LEVEL,
   STP_NAME AS LEVEL_DESCRIPTION,
     'EIP68' AS METRIC,
    sum(EIP_Caseload) AS METRIC_VALUE  ,
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

# DBTITLE 1,Insert AnySNoMED Breakdowns - EIP69a
 %sql
 -- The number of referrals to EIP teams with any SNOMED (Systemised Nomenclature of Medicine) codes recorded
 INSERT INTO $db_output.awt_unformatted
 --England breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS LEVEL,
   'NONE' AS LEVEL_DESCRIPTION,
   'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE,
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS LEVEL,
   PROVIDER_NAME AS LEVEL_DESCRIPTION,
     'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE ,
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 
 FROM      global_temp.EIP_master_DMS
 GROUP BY  OrgIDProv, PROVIDER_NAME
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN, -- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency
   CCG_CODE AS LEVEL,
   CCG_NAME AS LEVEL_DESCRIPTION,
     'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE  ,
   '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS LEVEL,
   REGION_NAME AS LEVEL_DESCRIPTION,
     'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE,
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
  
 FROM      global_temp.EIP_master_DMS
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP - GP Practice or Residence' AS BREAKDOWN,
   STP_CODE AS LEVEL,
   STP_NAME AS LEVEL_DESCRIPTION,
     'EIP69a' AS METRIC,
   sum(RefWithAnySNoMED) AS METRIC_VALUE,
  '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
   
 FROM      global_temp.EIP_master_DMS
 GROUP BY  STP_CODE, STP_NAME

# COMMAND ----------

# DBTITLE 1,Insert NICESNoMED breakdowns EIP69b
 %sql
 --The number of referrals to EIP teams with a NICE (National Institute for Health and Care Excellence ) concordant (compliant) SNOMED code recorded
 INSERT INTO $db_output.awt_unformatted
 --England breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'England' AS BREAKDOWN,
   'England' AS LEVEL,
   'NONE' AS LEVEL_DESCRIPTION,
     'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE, 
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
  
 FROM      global_temp.EIP_master_DMS
 
 union all
 
 --Provider breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Provider' AS BREAKDOWN,
   OrgIDProv AS LEVEL,
   PROVIDER_NAME AS LEVEL_DESCRIPTION,
     'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE,
  '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION 
 
 FROM      global_temp.EIP_master_DMS
 GROUP BY  OrgIDProv, PROVIDER_NAME
 
 union all
 
 --CCG breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'CCG - GP Practice or Residence' AS BREAKDOWN,-- amended breakdown name from 'CCG - Registration or Residence' to 'CCG - GP Practice or Residence' for consistency
   CCG_CODE AS LEVEL,
   CCG_NAME AS LEVEL_DESCRIPTION,
     'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE,
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  CCG_CODE, CCG_NAME
 
 union all
 
 --Region breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'Commissioning Region' AS BREAKDOWN,
   REGION_CODE AS LEVEL,
   REGION_NAME AS LEVEL_DESCRIPTION,
     'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE,  
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
     
 FROM      global_temp.EIP_master_DMS
 GROUP BY  REGION_CODE, REGION_NAME
 
 union all
 
 --STP breakdown
 SELECT 
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   'STP - GP Practice or Residence' AS BREAKDOWN,
   STP_CODE AS LEVEL,
   STP_NAME AS LEVEL_DESCRIPTION,
     'EIP69b' AS METRIC,
   sum(RefWithNICESNoMED) AS METRIC_VALUE,
 '$db_source' as SOURCE_DB,
   'NONE' AS SECONDARY_LEVEL,
   'NONE' AS SECONDARY_LEVEL_DESCRIPTION
 FROM      global_temp.EIP_master_DMS
 GROUP BY  STP_CODE, STP_NAME;
 
 OPTIMIZE $db_output.awt_unformatted