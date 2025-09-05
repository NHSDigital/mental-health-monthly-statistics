# Databricks notebook source
# dbutils.widgets.removeAll()

# dbutils.widgets.text("db_output", "menh_dq", "db_output")
# dbutils.widgets.text("month_id", "1449", "month_id")
# dbutils.widgets.text("dbm", "testdata_menh_dq_mhsds_database", "dbm")
# dbutils.widgets.text("reference_data", "reference_data", "reference_data")
# dbutils.widgets.text("status", "Performance", "status")


# COMMAND ----------

dbm  = dbutils.widgets.get("dbm")
print(dbm)
assert dbm

# COMMAND ----------

# DBTITLE 1,Generic coverage view (the actual publication CSV & Power BI CSV are in the 2 notebook cells underneath)
 %sql

 CREATE OR REPLACE TEMP VIEW dq_vw_coverage
 AS

 -- Fetch latest org names from corporate org look-up table. Prioritise the valid record org name (BUSINESS_END_DATE).
 -- In the rare case that there are multiple in/valid records, prioritise the org that closed most recently.
 WITH OrgList
 AS
 (
 SELECT
   ORG_CODE,
   NAME,
   ROW_NUMBER() OVER(PARTITION BY ORG_CODE ORDER BY IFNULL(BUSINESS_END_DATE, CURRENT_DATE()) DESC, IFNULL(ORG_CLOSE_DATE, CURRENT_DATE()) DESC) AS RowNumber
 FROM $reference_data.org_daily
 ),

 -- Make orgs form above unique
 UniqueOrgList AS
 (SELECT
   ORG_CODE,
   NAME
 FROM OrgList
 WHERE RowNumber = 1),

 -- Get the orgs submitted by Mental Health
 MhsHeaderOrgs AS
 (
   SELECT DISTINCT
     OrgIDProvider AS OrgIDProv,
     UniqMonthID
   FROM $dbm.mhs000header
   WHERE UniqMonthID = $month_id
 ),

 -- Define user-friendly table names
 TableNames AS
 (
   SELECT 'MHS001MPI' AS TableName
   UNION ALL SELECT 'MHS002GP' AS TableName
   UNION ALL SELECT 'MHS003AccommStatus' AS TableName
   UNION ALL SELECT 'MHS004EmpStatus' AS TableName
   UNION ALL SELECT 'MHS005PatInd' AS TableName
   UNION ALL SELECT 'MHS006MHCareCoord' AS TableName
   UNION ALL SELECT 'MHS007DisabilityType' AS TableName
   UNION ALL SELECT 'MHS008CarePlanType' AS TableName
   UNION ALL SELECT 'MHS009CarePlanAgreement' AS TableName
   UNION ALL SELECT 'MHS010AssTechToSupportDisTyp' AS TableName
   UNION ALL SELECT 'MHS011SocPerCircumstances' AS TableName
   UNION ALL SELECT 'MHS012OverseasVisitorChargCat' AS TableName
   UNION ALL SELECT 'MHS013MHCurrencyModel' AS TableName
   UNION ALL SELECT 'MHS014eMED3FitNote' AS TableName
   UNION ALL SELECT 'MHS101Referral' AS TableName
   UNION ALL SELECT 'MHS102OtherServiceType' AS TableName                                                                  ----V6_Changes
   UNION ALL SELECT 'MHS103OtherReasonReferral' AS TableName
   UNION ALL SELECT 'MHS104RTT' AS TableName
   UNION ALL SELECT 'MHS105OnwardReferral' AS TableName
   UNION ALL SELECT 'MHS106DischargePlanAgreement' AS TableName
   UNION ALL SELECT 'MHS107MedicationPrescription' AS TableName
   UNION ALL SELECT 'MHS201CareContact' AS TableName
   UNION ALL SELECT 'MHS202CareActivity' AS TableName
   UNION ALL SELECT 'MHS203OtherAttend' AS TableName
   UNION ALL SELECT 'MHS204IndirectActivity' AS TableName
   UNION ALL SELECT 'MHS205PatientSDDI' AS TableName
   UNION ALL SELECT 'MHS206StaffActivity' AS TableName
   UNION ALL SELECT 'MHS301GroupSession' AS TableName
   UNION ALL SELECT 'MHS302MHDropInContact' AS TableName
   UNION ALL SELECT 'MHS401MHActPeriod' AS TableName
   UNION ALL SELECT 'MHS402RespClinicianAssignPeriod' AS TableName
   UNION ALL SELECT 'MHS403ConditionalDischarge' AS TableName
   UNION ALL SELECT 'MHS404CommTreatOrder' AS TableName
   UNION ALL SELECT 'MHS405CommTreatOrderRecall' AS TableName
   UNION ALL SELECT 'MHS501HospProvSpell' AS TableName
   UNION ALL SELECT 'MHS502WardStay' AS TableName
   UNION ALL SELECT 'MHS503AssignedCareProf' AS TableName
   UNION ALL SELECT 'MHS504DelayedDischarge' AS TableName
   UNION ALL SELECT 'MHS505RestrictiveIntervention' AS TableName
   UNION ALL SELECT 'MHS505RestrictiveInterventInc' AS TableName
   UNION ALL SELECT 'MHS506Assault' AS TableName
   UNION ALL SELECT 'MHS507SelfHarm' AS TableName
   UNION ALL SELECT 'MHS509HomeLeave' AS TableName
   UNION ALL SELECT 'MHS510LeaveOfAbsence' AS TableName
   UNION ALL SELECT 'MHS511AbsenceWithoutLeave' AS TableName
   UNION ALL SELECT 'MHS512HospSpellCommAssPer' AS TableName
   UNION ALL SELECT 'MHS513SubstanceMisuse' AS TableName
   UNION ALL SELECT 'MHS514TrialLeave' AS TableName
   UNION ALL SELECT 'MHS515RestrictiveInterventType' AS TableName
   UNION ALL SELECT 'MHS516PoliceAssistanceRequest' AS TableName
   UNION ALL SELECT 'MHS517SMHExceptionalPackOfCare' AS TableName
   UNION ALL SELECT 'MHS518ClinReadyforDischarge' AS TableName                                                 ----V6_Changes
   UNION ALL SELECT 'MHS601MedHistPrevDiag' AS TableName
   UNION ALL SELECT 'MHS603ProvDiag' AS TableName
   UNION ALL SELECT 'MHS604PrimDiag' AS TableName
   UNION ALL SELECT 'MHS605SecDiag' AS TableName
   UNION ALL SELECT 'MHS606CodedScoreAssessmentRefer' AS TableName
   UNION ALL SELECT 'MHS607CodedScoreAssessmentCont' AS TableName
   UNION ALL SELECT 'MHS608AnonSelfAssess' AS TableName
   UNION ALL SELECT 'MHS609PresComp' AS TableName
   UNION ALL SELECT 'MHS701CPACareEpisode' AS TableName
   UNION ALL SELECT 'MHS702CPAReview' AS TableName
   UNION ALL SELECT 'MHS801ClusterTool' AS TableName
   UNION ALL SELECT 'MHS802ClusterAssess' AS TableName
   UNION ALL SELECT 'MHS803CareCluster' AS TableName
   UNION ALL SELECT 'MHS804FiveForensicPathways' AS TableName
   UNION ALL SELECT 'MHS901StaffDetails' AS TableName
   UNION ALL SELECT 'MHS902ServiceTeamDetails' AS TableName
   UNION ALL SELECT 'MHS903WardDetails' AS TableName
 ),

 -- Calculate the actual counts for each table
 TableCounts AS
 (
   SELECT
     'MHS001MPI' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs001mpi
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS002GP' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs002gp
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS003AccommStatus' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs003accommstatus
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS004EmpStatus' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs004empstatus
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS005PatInd' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs005patind
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS006MHCareCoord' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs006mhcarecoord
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS007DisabilityType' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs007disabilitytype
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS008CarePlanType' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs008careplantype
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS009CarePlanAgreement' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs009careplanagreement
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS010AssTechToSupportDisTyp' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs010asstechtosupportdistyp
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS011SocPerCircumstances' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs011socpercircumstances
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS012OverseasVisitorChargCat' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs012overseasvisitorchargcat
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
 --   new table for v5
   UNION ALL SELECT
     'MHS013MHCurrencyModel' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs013mhcurrencymodel
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
 --   new table for v6
   UNION ALL SELECT
     'MHS014eMED3FitNote' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS014eMED3FitNote
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
   UNION ALL SELECT
     'MHS101Referral' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs101referral
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS102OtherServiceType' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS102OtherServiceType
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS103OtherReasonReferral' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs103otherreasonreferral
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS104RTT' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs104rtt
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS105OnwardReferral' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs105onwardreferral
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS106DischargePlanAgreement' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs106dischargeplanagreement
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS107MedicationPrescription' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs107medicationprescription
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS201CareContact' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs201carecontact
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS202CareActivity' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs202careactivity
   WHERE UniqMonthID = $month_id 
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS203OtherAttend' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs203otherattend
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS204IndirectActivity' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs204indirectactivity
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   --   new table for v6
   UNION ALL SELECT
     'MHS205PatientSDDI' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS205PatientSDDI
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
    --   new table for v6
   UNION ALL SELECT
     'MHS206StaffActivity' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS206StaffActivity
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS301GroupSession' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs301groupsession
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
 --   new table for v5
   UNION ALL SELECT
     'MHS302MHDropInContact' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs302mhdropincontact
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
   UNION ALL SELECT
     'MHS401MHActPeriod' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs401mhactperiod
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS402RespClinicianAssignPeriod' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs402respclinicianassignperiod
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS403ConditionalDischarge' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs403conditionaldischarge
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS404CommTreatOrder' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs404commtreatorder
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS405CommTreatOrderRecall' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs405commtreatorderrecall
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS501HospProvSpell' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs501hospprovspell
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS502WardStay' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs502wardstay
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS503AssignedCareProf' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs503assignedcareprof
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS504DelayedDischarge' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs504delayeddischarge
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
 --   original v4.1 table - kept in for runs of earlier months -- can't exist until mhsds_database is updated :o(
 --  UNION ALL SELECT
 --   'MHS505RestrictiveIntervention' AS TableName,
 --    OrgIDProv,
 --   COUNT(*) AS RowCount
 --  FROM $dbm.mhs505restrictiveintervention_pre_v5
 --  WHERE UniqMonthID = $month_id
 --  GROUP BY OrgIDProv
 --   new v5 table - only has data post v5
   UNION ALL SELECT
     'MHS505RestrictiveInterventInc' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs505restrictiveinterventinc
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
   UNION ALL SELECT
     'MHS506Assault' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs506assault
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS507SelfHarm' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs507selfharm
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS509HomeLeave' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs509homeleave
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS510LeaveOfAbsence' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs510leaveofabsence
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS511AbsenceWithoutLeave' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs511absencewithoutleave
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS512HospSpellCommAssPer' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs512hospspellcommassper
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS513SubstanceMisuse' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs513substancemisuse
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS514TrialLeave' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs514trialleave
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
 --   new table for v5
   UNION ALL SELECT
     'MHS515RestrictiveInterventType' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs515restrictiveinterventtype
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
 --   new table for v5
   UNION ALL SELECT
     'MHS516PoliceAssistanceRequest' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs516policeassistancerequest
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
     
 --   new table for v5
   UNION ALL SELECT
     'MHS517SMHExceptionalPackOfCare' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs517smhexceptionalpackofcare
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
   --   new table for v6
   UNION ALL SELECT
     'MHS518ClinReadyforDischarge' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS518ClinReadyforDischarge
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
   UNION ALL SELECT
     'MHS601MedHistPrevDiag' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs601medhistprevdiag
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS603ProvDiag' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs603provdiag
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS604PrimDiag' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs604primdiag
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS605SecDiag' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs605secdiag
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS606CodedScoreAssessmentRefer' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs606codedscoreassessmentrefer
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS607CodedScoreAssessmentCont' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs607codedscoreassessmentact
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL
   SELECT
     'MHS608AnonSelfAssess' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs608anonselfassess
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL 
    SELECT
     'MHS609PresComp' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS609PresComp
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv 
   UNION ALL
   SELECT
     'MHS701CPACareEpisode' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs701cpacareepisode
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS702CPAReview' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs702cpareview
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS801ClusterTool' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs801clustertool
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS802ClusterAssess' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs802clusterassess
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS803CareCluster' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs803carecluster
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS804FiveForensicPathways' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs804fiveforensicpathways
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   UNION ALL SELECT
     'MHS901StaffDetails' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs901staffdetails
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv
   
 --   new table for v6
   UNION ALL 
   SELECT
     'MHS902ServiceTeamDetails' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS902ServiceTeamDetails
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv  
   UNION ALL 
   SELECT
     'MHS903WardDetails' AS TableName,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS903WardDetails
   WHERE UniqMonthID = $month_id
   GROUP BY OrgIDProv  
 ),

 -- Multiple orgs with tables
 MhsOrgTables AS
 (
   SELECT
     OrgIDProv,
     UniqMonthID,
     TableName
   FROM MhsHeaderOrgs
   CROSS JOIN TableNames
 )

 -- Return final columns
 SELECT
   '$rp_startdate' AS REPORTING_PERIOD_START,
   '$rp_enddate' AS REPORTING_PERIOD_END,
   '$status' AS STATUS,
   (CASE WHEN ot.OrgIdProv IS NULL THEN 'England' ELSE ot.OrgIdProv END) AS ORGANISATION_CODE,
   (CASE WHEN ot.OrgIdProv IS NULL THEN 'England' ELSE o.NAME END) AS ORGANISATION_NAME,
   (CASE WHEN ot.TableName IS NULL THEN 'Any' ELSE ot.TableName END) AS TABLE_NAME,
   SUM(t.RowCount) AS COVERAGE_SUM
 FROM MhsOrgTables ot
   INNER JOIN UniqueOrgList o ON ot.OrgIDProv = o.ORG_CODE
   LEFT OUTER JOIN TableCounts t ON ot.OrgIDProv = t.OrgIDProv 
                                    AND ot.TableName = t.TableName

 -- Add total groups to result set
 GROUP BY
   GROUPING SETS
   (
     (ot.UniqMonthID),
     (ot.TableName, ot.UniqMonthID),
     (ot.OrgIDProv, o.NAME, ot.UniqMonthID),
     (ot.OrgIDProv, o.NAME, ot.TableName, ot.UniqMonthID)
   )

# COMMAND ----------

# DBTITLE 1,Extract data into the dq_coverage_monthly_csv table (for publication)
 %sql
 --CREATE OR REPLACE TEMPORARY VIEW dq_coverage_monthly_csv AS
 delete from $db_output.dq_coverage_monthly_csv WHERE Month_Id = '$month_id' AND status = '$status' and SOURCE_DB = '$dbm';
 insert into $db_output.dq_coverage_monthly_csv 
 SELECT 
   '$month_id' as MONTH_ID,
   REPORTING_PERIOD_START,
   REPORTING_PERIOD_END,
   STATUS,
   ORGANISATION_CODE,
   ORGANISATION_NAME,
   TABLE_NAME,
   TRY_CAST((CASE
      WHEN COVERAGE_SUM >= 5 AND ORGANISATION_CODE = 'England' THEN -- Provider totals & England totals & England table totals
       IFNULL(
         COVERAGE_SUM -- DO NOT round
       , '*')
       WHEN COVERAGE_SUM >= 5 THEN -- Provider table totals (WARNING: OrgIdProv must be a string, ie '-1')
       IFNULL(
         CAST(ROUND(COVERAGE_SUM * 2, -1) / 2 AS INT) -- DO round to nearest 5
       , '*')
     ELSE
       '*' -- Suppress value because less than 5
   END) AS BIGINT) AS COVERAGE_COUNT,
   '$dbm' AS SOURCE_DB
 FROM dq_vw_coverage;


# COMMAND ----------

# DBTITLE 1,Download coverage CSV (for Power BI)
 %sql
 --CREATE OR REPLACE TEMPORARY VIEW dq_coverage_monthly_pbi AS
 delete from $db_output.dq_coverage_monthly_pbi WHERE Month_Id = '$month_id' AND status = '$status' and SOURCE_DB = '$dbm';
 insert into $db_output.dq_coverage_monthly_pbi
 SELECT
   '$month_id' as MONTH_ID,
   '$status' as STATUS,
   ORGANISATION_CODE AS `Organisation Code`,
   ORGANISATION_NAME AS `Organisation Name`,
   TABLE_NAME AS `Table Name`,
   CONCAT('Sum of ', DATE_FORMAT(REPORTING_PERIOD_END, 'MMMM y'), ' $status') AS `Sum Of Period Name`,
   --IFNULL(COVERAGE_SUM, '') AS `X`,
   
   (CASE
     WHEN COVERAGE_SUM >= 5 AND ORGANISATION_CODE = 'England' THEN -- Provider totals & England totals & England table totals
       IFNULL(
         COVERAGE_SUM -- DO NOT round
       , '')
       WHEN COVERAGE_SUM >= 5 THEN -- Provider table totals (WARNING: OrgIdProv must be a string, ie '-1')
       IFNULL(
         CAST(ROUND(COVERAGE_SUM * 2, -1) / 2 AS INT) -- DO round to nearest 5
       , '')
         ELSE
       '*' -- Suppress value because less than 5
   END) AS X,
   
   CONCAT(DATE_FORMAT(REPORTING_PERIOD_END, 'MMMM y'), ' $status') AS `Period Name`,
   DATE_FORMAT(REPORTING_PERIOD_END, 'dd/MM/y') AS `Period End Date`,
   '$dbm' AS SOURCE_DB
 FROM dq_vw_coverage

# COMMAND ----------

# %sql

# select * from $db_output.dq_coverage_monthly_csv 
# WHERE MONTH_ID = '$month_id' 
# order by TABLE_NAME