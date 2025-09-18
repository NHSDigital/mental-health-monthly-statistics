# Databricks notebook source
 %sql
 select distinct uniqmonthid, reportingperiodenddate from $db_source.mhs000header order by uniqmonthid desc

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status = dbutils.widgets.get("status")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY FUNCTION combine_mhsds_date_and_time(date_col DATE, time_col TIMESTAMP)
 RETURNS TIMESTAMP
 COMMENT 'Combines MHSDS date and time columns into a single datetime, as MHSDS time values contain 1970-01-01'
 RETURN to_timestamp(concat(date_col, 'T', substring(time_col, 12, 8)))

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY FUNCTION minutes_between_timestamp(end_time TIMESTAMP, start_time TIMESTAMP)
 RETURNS FLOAT
 COMMENT 'calculates minutes between two given timestamps'
 RETURN CAST(CAST(end_time as long) - CAST(start_time as long) as float) / 60

# COMMAND ----------

# DBTITLE 1,Region Ref Data Part1
 %sql
 DROP TABLE IF EXISTS $db_output.RESTR_ORG_DAILY_A;
 CREATE TABLE IF NOT EXISTS $db_output.RESTR_ORG_DAILY_A USING DELTA AS 
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 --AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate';
                 --AND NAME NOT LIKE '%HUB'
                 --AND NAME NOT LIKE '%NATIONAL%';               
 OPTIMIZE $db_output.RESTR_ORG_DAILY_A;              

# COMMAND ----------

# DBTITLE 1,Region Ref Data Part2
 %sql
 DROP TABLE IF EXISTS $db_output.RESTR_ORG_RELATIONSHIP_DAILY;
 CREATE TABLE $db_output.RESTR_ORG_RELATIONSHIP_DAILY USING DELTA AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 reference_data.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

 -- OPTIMIZE $db_output.RESTR_ORG_RELATIONSHIP_DAILY;

# COMMAND ----------

# DBTITLE 1,Region Ref Data Final
 %sql
 DROP TABLE IF EXISTS $db_output.RI_STP_MAPPING;
 CREATE TABLE $db_output.RI_STP_MAPPING AS 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.RESTR_ORG_DAILY_A A
 LEFT JOIN $db_output.RESTR_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.RESTR_ORG_DAILY_A C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.RESTR_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.RESTR_ORG_DAILY_A E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 --ORG_CODE = 'QF7'
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RI_RD_CCG_LATEST;
 CREATE TABLE $db_output.RI_RD_CCG_LATEST AS 
 SELECT DISTINCT ORG_TYPE_CODE, ORG_CODE, NAME
 FROM reference_data.ORG_DAILY
 WHERE (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
   AND BUSINESS_START_DATE <= '$rp_enddate'
     AND ORG_TYPE_CODE = "CC"
       AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
         AND ORG_OPEN_DATE <= '$rp_enddate'
           AND NAME NOT LIKE '%HUB'
             AND NAME NOT LIKE '%NATIONAL%'
             AND NAME NOT LIKE '%ENTITY%'

# COMMAND ----------

def get_provider_type(providername: str) -> str:
  if providername[0] == 'R' or providername[0] == 'T':
    return 'NHS Providers'
  return 'Non NHS Providers'
spark.udf.register("get_provider_type", get_provider_type)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RI_SUBS

# COMMAND ----------

# DBTITLE 1,Submission Picker to get Performance or Latest Submitted Data
if status == "Provisional": ##filetype = 1
  spark.sql(f"""CREATE TABLE {db_output}.RI_SUBS AS 
  select
  ReportingPeriodEndDate, ReportingPeriodStartDate, UniqMonthID, OrgIDProvider, MAX(UniqSubmissionID) AS UniqSubmissionID
  from {db_source}.mhs000header
  where FileType = 1 AND UniqMonthID <= '{month_id}'
  GROUP BY ReportingPeriodEndDate, ReportingPeriodStartDate, UniqMonthID, OrgIDProvider""")
elif status == "Performance": ##filetype <= 2
  spark.sql(f"""CREATE TABLE {db_output}.RI_SUBS AS 
  select
  ReportingPeriodEndDate, ReportingPeriodStartDate, UniqMonthID, OrgIDProvider, MAX(UniqSubmissionID) AS UniqSubmissionID
  from {db_source}.mhs000header
  where FileType <= 2 AND UniqMonthID <= '{month_id}'
  GROUP BY ReportingPeriodEndDate, ReportingPeriodStartDate, UniqMonthID, OrgIDProvider""")
elif status == "Final":
  spark.sql(f"""CREATE TABLE {db_output}.RI_SUBS AS 
  select
  ReportingPeriodEndDate, ReportingPeriodStartDate, UniqMonthID, OrgIDProvider, MAX(UniqSubmissionID) AS UniqSubmissionID
  from {db_source}.mhs000header
  where UniqMonthID <= '{month_id}'
  GROUP BY ReportingPeriodEndDate, ReportingPeriodStartDate, UniqMonthID, OrgIDProvider""")

# COMMAND ----------

# DBTITLE 1,Header Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_HEADER;
 CREATE TABLE $db_output.RI_HEADER AS
 select distinct uniqmonthid, ReportingPeriodEndDate, ReportingPeriodStartDate 
 from $db_source.mhs000header

# COMMAND ----------

# DBTITLE 1,1. Hospital Spell Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_HOSP_SPELL;
 CREATE TABLE $db_output.RI_HOSP_SPELL AS
 select distinct
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.uniqservreqid, a.MHS501UniqID,
 a.uniqhospprovspellid, 
 a.StartDateHospProvSpell,
 a.DischDateHospProvSpell,
 a.InactTimeHPS, 
 CASE WHEN a.StartDateHospProvSpell < '$rp_startdate' THEN '$rp_startdate' ELSE a.StartDateHospProvSpell END AS Der_HospSpellStartDate,
 CASE WHEN a.DischDateHospProvSpell IS NULL THEN date_add('$rp_enddate', 1) ELSE a.DischDateHospProvSpell END AS Der_HospSpellEndDate,
 datediff(CASE WHEN (DischDateHospProvSpell IS NULL AND InactTimeHPS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeHPS IS NOT NULL THEN InactTimeHPS
                                       ELSE DischDateHospProvSpell END
                                       ,CASE WHEN StartDateHospProvSpell < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateHospProvSpell END) as bed_days
 from $db_source.mhs501hospprovspell a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,2. Restrictive Interventions Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_RESTRAINTS;
 CREATE TABLE IF NOT EXISTS $db_output.RI_RESTRAINTS AS
 select distinct
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.mhs505uniqid, a.restrictiveintincid, a.UniqHospProvSpellID,  a.UniqRestrictiveIntIncID, a.UniqServReqID, a.startdaterestrictiveintinc, a.enddaterestrictiveintinc,a.starttimerestrictiveintinc, a.endtimerestrictiveintinc,
 CASE WHEN StartDateRestrictiveIntInc < '$rp_startdate' THEN '$rp_startdate' ELSE StartDateRestrictiveIntInc END AS Der_RestIntIncStartDate,
 CASE WHEN EndDateRestrictiveIntInc is null then date_add('$rp_enddate', 1) ELSE EndDateRestrictiveIntInc END AS Der_RestIntIncEndDate, 
 CASE WHEN starttimerestrictiveintInc is null then to_timestamp('1970-01-01') else starttimerestrictiveIntInc end as Der_RestIntIncStartTime, 
 CASE WHEN endtimerestrictiveintInc is null then to_timestamp('1970-01-01') else endtimerestrictiveintInc end as Der_RestIntIncEndTime,
 CASE WHEN RestrictiveIntReason is null THEN "UNKNOWN" else RestrictiveIntReason end as RestrictiveIntReasonCode,
 CASE WHEN RestrictiveIntReason = 10	THEN "Prevent a patient being violent to others"
 WHEN RestrictiveIntReason = 11 THEN "Prevent a patient causing serious intentional harm to themselves"
 WHEN RestrictiveIntReason = 12 THEN "Prevent a patient causing serious physical injury to themselves by accident"
 WHEN RestrictiveIntReason = 13 THEN "Lawfully administer medicines or other medical treatment"
 WHEN RestrictiveIntReason = 14 THEN "Facilitate personal care"
 WHEN RestrictiveIntReason = 15 THEN "Facilitate nasogastric (NG) feeding"
 WHEN RestrictiveIntReason = 16 THEN "Prevent the patient exhibiting extreme and prolonged over-activity"
 WHEN RestrictiveIntReason = 17 THEN "Prevent the patient exhibiting otherwise dangerous behaviour"
 WHEN RestrictiveIntReason = 18 THEN "Undertake a search of the patient's clothing or property to ensure the safety of others"
 WHEN RestrictiveIntReason = 19 THEN "Prevent the patient absconding from lawful custody"
 WHEN RestrictiveIntReason = 98 THEN "Other (not listed)"
 WHEN RestrictiveIntReason = 99 THEN "Not Known (Not Recorded)"
 ELSE "UNKNOWN" END AS RestrictiveIntReasonName,
 a.recordnumber,
 case when enddaterestrictiveintinc > '$rp_enddate' ---removed condition for incident before rp_startdate
 or startdaterestrictiveintinc is null 
 or starttimerestrictiveintinc is null 
 or enddaterestrictiveintinc is null 
 or endtimerestrictiveintinc is null
 then 'N'
 else 'Y' end as pre_flag_inc,
 case when enddaterestrictiveintinc > '$rp_enddate' ---removed condition for incident before rp_startdate
 or startdaterestrictiveintinc is null 
 or starttimerestrictiveintinc is null 
 then 'N'
 else 'Y' end as active_pre_flag_inc
 from $db_source.mhs505restrictiveinterventinc a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'
 and ((a.Startdaterestrictiveintinc BETWEEN '$rp_startdate' AND '$rp_enddate') 
                             or (a.Enddaterestrictiveintinc BETWEEN '$rp_startdate' AND '$rp_enddate')
                             or (a.Enddaterestrictiveintinc is null))

# COMMAND ----------

# DBTITLE 1,3. Restrictive Intervention Type Table Prep
 %sql
 CREATE OR REPLACE TABLE $db_output.RI_RESTRAINT_TYPE_v2 AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.restrictiveinttype,a.UniqRestrictiveIntTypeID, a.UniqRestrictiveIntIncID, a.mhs515uniqid, a.startdaterestrictiveinttype, a.enddaterestrictiveinttype, a.starttimerestrictiveinttype, a.endtimerestrictiveinttype, a.uniqhospprovspellid, ---added for v5 /*** a.UniqWardStayID replaced by  a.UniqRestrictiveIntIncID, to be checked, required due to ToS  change gf***/
 CASE WHEN StartDateRestrictiveIntType < '$rp_startdate' THEN '$rp_startdate' ELSE StartDateRestrictiveIntType END AS Der_RestIntStartDate,
 CASE WHEN EndDateRestrictiveIntType is null then date_add('$rp_enddate', 1) ELSE EndDateRestrictiveIntType END AS Der_RestIntEndDate, 
 CASE WHEN starttimerestrictiveintType is null then to_timestamp('1970-01-01') else starttimerestrictiveintType end as Der_RestIntStartTime, 
 CASE WHEN endtimerestrictiveintType is null then to_timestamp('1970-01-01') else endtimerestrictiveintType end as Der_RestIntEndTime,
 case when UPPER(restraintinjurypatient) = "Y" then "Yes"
      when UPPER(restraintinjurypatient) = "N" then "No"
      else "Unknown"
      end as patient_injury,
 case when UPPER(restraintinjurypatient) = "Y" then 1 --rank yes first
      when UPPER(restraintinjurypatient) = "N" then 2 --then rank no
      else 3 --if null then rank as last   
      end as patient_injury_binary,
 case when enddaterestrictiveintType > '$rp_enddate' ---removed condition for type before rp_startdate
 or startdaterestrictiveintType is null 
 or starttimerestrictiveintType is null 
 or enddaterestrictiveintType is null 
 or endtimerestrictiveintType is null
 then 'N'
 else 'Y' end as pre_flag_type,
 case when enddaterestrictiveintType > '$rp_enddate' ---removed condition for type before rp_startdate
 or startdaterestrictiveintType is null 
 or starttimerestrictiveintType is null
 then 'N'
 else 'Y' end as active_pre_flag_type,
 a.recordnumber
 from $db_source.mhs515restrictiveinterventtype a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'
 and ((a.startdaterestrictiveinttype BETWEEN '$rp_startdate' AND '$rp_enddate') /*** updated to startdaterestrictiveinttype for v5 gf8**/
                             or (a.enddaterestrictiveinttype BETWEEN '$rp_startdate' AND '$rp_enddate') /*** updated to enddaterestrictiveinttype for v5 gf ***/
                             or (a.enddaterestrictiveinttype is null)) /*** updated to enddaterestrictiveinttype for v5,  gf ***/

# COMMAND ----------

# DBTITLE 1,4. Ward Stay Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_WARDSTAY;
 CREATE TABLE IF NOT EXISTS $db_output.RI_WARDSTAY AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.uniqwardstayid, 
 c.siteidofward as siteidoftreat, --changed from siteidoftreat to siteidofward for v6
 a.mhs502uniqid, 
 a.uniqhospprovspellid, ---changed from UniqHospProvSpellNum to uniqhospprovspellid for v5
 a.startdatewardstay, a.enddatewardstay, a.inacttimews, 
 CASE WHEN a.StartDateWardStay < '$rp_startdate' THEN '$rp_startdate' ELSE a.StartDateWardStay END AS Der_WrdStartDate,
 CASE WHEN a.EndDateWardStay IS NULL THEN date_add('$rp_enddate', 1) ELSE a.EndDateWardStay END AS Der_WrdEndDate,
 CASE WHEN a.StartTimeWardStay IS NULL THEN to_timestamp('1970-01-01') ELSE a.StartTimeWardStay END AS Der_WrdStartTime, 
 CASE WHEN a.EndTimeWardStay IS NULL THEN to_timestamp('1970-01-01') ELSE a.EndTimeWardStay END AS Der_WrdEndTime, 
 a.recordnumber, a.SpecialisedMHServiceCode, a.StartTimeWardStay, a.EndTimeWardStay, a.WardCode, a.WardLocDistanceHome,
 --CASE WHEN SpecialisedMHServiceCode is null THEN 'Unknown'
 CASE WHEN SpecialisedMHServiceCode is null THEN 'Unknown'
      WHEN ServiceCode = 'NCBPS22E' THEN 'Adult Eating Disorders'
      WHEN ServiceCode = 'NCBPS22D' THEN 'Mental Health Services for the Deaf (Adult)'
      WHEN ServiceCode = 'NCBPS22B' THEN 'Tier 4 CAMHS (Deaf Child)'
      WHEN ServiceCode = 'NCBPS22P' THEN 'Perinatal Mental Health Services'
      WHEN ServiceCode = 'NCBPS22U' THEN 'Secure and Specialised Mental Health Services (Adult) (High)'
      WHEN ServiceCode = 'NCBPS22S' THEN 'Secure and Specialised Mental Health Services (Adult) (Medium and Low)'
      WHEN ServiceCode = 'NCBPS22C' THEN 'Tier 4 CAMHS (Medium Secure)'
      WHEN ServiceCode = 'NCBPS22F' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder (Adult)'
      WHEN ServiceCode = 'NCBPS22H' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder (Child)'
      WHEN ServiceCode = 'NCBPS23K' THEN 'Tier 4 CAMHS (General Adolescent inc. Eating Disorders)'
      WHEN ServiceCode = 'NCBPS23L' THEN 'Tier 4 CAMHS (Low Secure)'
      WHEN ServiceCode = 'NCBPS23O' THEN 'Tier 4 CAMHS (PICU)'
      WHEN ServiceCode = 'NCBPS23U' THEN 'Tier 4 CAMHS (LD)'
      WHEN ServiceCode = 'NCBPS23V' THEN 'Tier 4 CAMHS (ASD)'
      WHEN ServiceCode = 'NCBPS24C' THEN 'Tier 4 CAMHS (Forensic)'
      WHEN ServiceCode = 'NCBPS24E' THEN 'Tier 4 CAMHS (Childrens Services)'
      WHEN ServiceCode = 'NCBPS22T' THEN 'Tier 4 Personality Disorders'
      WHEN ServiceCode = 'NCBPS05E' THEN 'Environmental Controls'
      WHEN ServiceCode = 'NCBPS05C' THEN 'Communication Aids'
      WHEN ServiceCode = 'NCBPS08Y' THEN 'Neuropsychiatry'
      WHEN ServiceCode = 'NCBPSYYY' THEN 'Specialised Mental Health Services Exceptional Packages of Care'
      WHEN ServiceCode = 'NCBPSXXX' THEN 'None (Specialised Service but no attributable)' 
      WHEN ServiceCode = 'NCBPS22O' Then 'Offender Personality Disorder'
      WHEN ServiceCode = 'NCBPS22A' Then 'Gender Identity Development Service - Adolescents'
      WHEN ServiceCode = 'NCBPS42D' Then 'Gender Dysphoria - Non-Surgical Services'
      ELSE 'Non-specialist Service' END AS specialised_service,
      datediff(CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                       ELSE EndDateWardStay END
                                       ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateWardStay END) as bed_days_ws
 from $db_source.mhs502wardstay a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 left join menh_dq.dq_smh_service_category_code sc on UPPER(sc.ServiceCategoryCode) = UPPER(a.SpecialisedMHServiceCode) and (sc.LastMonth >='$month_id' or sc.LastMonth is NULL)
 left join $db_source.mhs903warddetails c on a.UniqWardCode = c.UniqWardCode and a.uniqmonthid = c.uniqmonthid
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,NEW. GP Practice Table Prep
 %sql
 --This table returns rows, carry forward
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PRAC AS
  
 SELECT GP.Person_ID,
       GP.OrgIDCCGGPPractice,
       GP.OrgIDSubICBLocGP,
       GP.RecordNumber
  FROM $db_source.MHS002GP GP
       INNER JOIN 
                  (
                    SELECT Person_ID, 
                           MAX(RecordNumber) as RecordNumber
                      FROM $db_source.MHS002GP
                     WHERE UniqMonthID = '$month_id'
                           AND GMPReg NOT IN ('V81999','V81998','V81997')
                           --AND OrgIDGPPrac <> '-1' --CCG methodology change from TP - change made by DC
                           AND EndDateGMPRegistration is NULL
                  GROUP BY Person_ID
                  ) max_GP  
                  ON GP.Person_ID = max_GP.Person_ID 
                  AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GP.UniqMonthID = '$month_id'
        --AND MPI.PatMRecInRP = TRUE 
        AND GMPReg NOT IN ('V81999','V81998','V81997')
        --AND OrgIDGPPrac <> '-1' 
        AND EndDateGMPRegistration is null

# COMMAND ----------

 %sql
 --Under month_id = 1472 this table returns rows
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_PREP AS
      SELECT a.Person_ID,
             CASE
               WHEN UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice
               WHEN UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP 
               WHEN UNIQMONTHID <= 1467 then OrgIDCCGRes 
               WHEN UNIQMONTHID > 1467 then OrgIDSubICBLocResidence
               ELSE 'ERROR'
               END as IC_Rec_CCG ---> this column name is replaced by IC_REC_GP_RESC see code below, but this name is persisted for now as this column name is used in many group by clauses 
               --END AS IC_REC_GP_RES
        FROM $db_source.MHS001MPI a
   LEFT JOIN global_temp.CCG_PRAC c 
             ON a.Person_ID = c.Person_ID 
             AND a.RecordNumber = c.RecordNumber
       WHERE a.UniqMonthID = '$month_id' 
             AND a.PatMRecInRP = true

# COMMAND ----------

# DBTITLE 1,5. MPI Table Prep
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_MPI AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.AgeRepPeriodEnd, ---will eventually need to be changed to Ethnicity2021 when cencus 2021 data released
 a.Gender, a.RecordNumber, a.PatMRecInRP,
 CASE WHEN a.UniqMonthID <= 1467 then a.OrgIDCCGRes 
      WHEN a.UniqMonthID > 1467 then a.OrgIDSubICBLocResidence ---Use OrgIDCCGRes or OrgIDSubICBLocResidence depending on month being ran
      ELSE 'ERROR' end as OrgIDCCGRes,
 case when a.AgeRepPeriodEnd <= 17 THEN 'Under 18'
                      when a.AgeRepPeriodEnd between 18 and 24 THEN '18-24'
                      when a.AgeRepPeriodEnd between 25 and 34 THEN '25-34'
                      when a.AgeRepPeriodEnd between 35 and 44 THEN '35-44'
                      when a.AgeRepPeriodEnd between 45 and 54 THEN '45-54'
                      when a.AgeRepPeriodEnd between 55 and 64 THEN '55-64'
                      when a.AgeRepPeriodEnd >= 65 THEN '65 or over'
                      else 'UNKNOWN' end as age_group,
 CASE WHEN a.NHSDEthnicity in ('A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','Z','99') THEN a.NHSDEthnicity ELSE 'UNKNOWN' end as NHSDEthnicity,                    
 CASE WHEN a.NHSDEthnicity = 'A' THEN 'British'
                    WHEN a.NHSDEthnicity = 'B' THEN 'Irish'
                    WHEN a.NHSDEthnicity = 'C' THEN 'Any other White background'
                    WHEN a.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
                    WHEN a.NHSDEthnicity = 'E' THEN 'White and Black African'
                    WHEN a.NHSDEthnicity = 'F' THEN 'White and Asian'
                    WHEN a.NHSDEthnicity = 'G' THEN 'Any other mixed background'
                    WHEN a.NHSDEthnicity = 'H' THEN 'Indian'
                    WHEN a.NHSDEthnicity = 'J' THEN 'Pakistani'
                    WHEN a.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                    WHEN a.NHSDEthnicity = 'L' THEN 'Any other Asian background'
                    WHEN a.NHSDEthnicity = 'M' THEN 'Caribbean'
                    WHEN a.NHSDEthnicity = 'N' THEN 'African'
                    WHEN a.NHSDEthnicity = 'P' THEN 'Any other Black background'
                    WHEN a.NHSDEthnicity = 'R' THEN 'Chinese'
                    WHEN a.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                    WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'                  
                    WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'UNKNOWN' END AS LowerEthnicity                     
 from $db_source.mhs001mpi a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PATMRECINRP_MPI AS
 SELECT person_id, MAX(RecordNumber) as RecordNumber FROM global_temp.RI_MPI
 group by person_id

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RI_MPI_FINAL;
 CREATE TABLE $db_output.RI_MPI_FINAL AS
 SELECT a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.OrgIDCCGRes, ccg.IC_Rec_CCG, a.AgeRepPeriodEnd, a.NHSDEthnicity, a.Gender, a.RecordNumber, a.PatMRecInRP,a.age_group,a.LowerEthnicity
 FROM global_temp.RI_MPI a 
 INNER JOIN global_temp.RI_PATMRECINRP_MPI b on a.person_id = b.person_id and a.RecordNumber = b.RecordNumber
 INNER JOIN global_temp.CCG_PREP ccg on a.person_id = ccg.person_id

# COMMAND ----------

# DBTITLE 1,6. Referrals Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_REFERRALS;
 CREATE TABLE IF NOT EXISTS $db_output.RI_REFERRALS AS
 select a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.uniqservreqid, a.ServDischDate 
 from $db_source.mhs101referral a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,Create Initial Prep Table - Part1
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PREP AS
 select 
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 hs.uniqhospprovspellid,
 ws.uniqwardstayid, 
 rt.person_id, 
 hs.orgidprov,
 COALESCE(stp.CCG_CODE, 'UNKNOWN') as ccg_code,
 COALESCE(stp.CCG_NAME, 'UNKNOWN') as ccg_name,
 COALESCE(stp.STP_CODE, 'UNKNOWN') as stp_code,
 COALESCE(stp.STP_NAME, 'UNKNOWN') as stp_name,
 COALESCE(stp.REGION_CODE, 'UNKNOWN') as region_code,
 COALESCE(stp.REGION_NAME, 'UNKNOWN') as region_name,
 m.age_group,
 get_provider_type(hs.OrgidProv) as provider_type,
 COALESCE(eth2.id, 'UNKNOWN') as UpperEthnicityCode,
 COALESCE(eth2.description, 'UNKNOWN') as UpperEthnicityName,
 COALESCE(m.NHSDEthnicity, 'UNKNOWN') as LowerEthnicityCode,
 COALESCE(m.LowerEthnicity, 'UNKNOWN') as LowerEthnicity,
 COALESCE(gen2.Code, 'UNKNOWN') as GenderCode,
 COALESCE(gen2.Description, 'UNKNOWN') as GenderName,
 ----COALESCE(ws.siteidoftreat, 'UNKNOWN') as siteidoftreat,
 COALESCE(case when od.NAME is null then "UNKNOWN" else ws.siteidoftreat end, "UNKNOWN") as siteidoftreat,                    ----AM: changes to remove duplicate when provider input an invalid sitecode
 COALESCE(od.NAME, 'UNKNOWN') as site_name,
 coalesce(int.key, 'UNKNOWN') as restrictiveintcode,
 coalesce(int.description, 'UNKNOWN') as restrictiveintname,
 rt.restrictiveinttype,
 ws.SpecialisedMHServiceCode,
 coalesce(ws.specialised_service, 'No associated Ward Stay') as specialised_service,
 rt.patient_injury,
 rt.patient_injury_binary,
 coalesce(rs.RestrictiveIntReasonCode, "UNKNOWN") as RestrictiveIntReasonCode,
 coalesce(rs.RestrictiveIntReasonName, "UNKNOWN") as RestrictiveIntReasonName,
 rt.startdaterestrictiveintType, 
 rt.enddaterestrictiveintType,
 rt.starttimerestrictiveintType,
 rt.endtimerestrictiveintType,
 rt.Der_RestIntStartDate,
 rt.Der_RestIntEndDate,
 rs.startdaterestrictiveintinc, 
 rs.enddaterestrictiveintinc,
 rs.starttimerestrictiveintinc,
 rs.endtimerestrictiveintinc,
 rs.Der_RestIntIncStartDate,
 rs.Der_RestIntIncEndDate,
 rt.pre_flag_type,
 rs.pre_flag_inc,
 rt.active_pre_flag_type,
 rs.active_pre_flag_inc,
 combine_mhsds_date_and_time(rt.startdaterestrictiveintType, rt.starttimerestrictiveintType) as rawstartdatetimerestrictiveint,
 combine_mhsds_date_and_time(rt.enddaterestrictiveintType,rt.endtimerestrictiveintType) as rawenddatetimerestrictiveint,
 combine_mhsds_date_and_time(rt.startdaterestrictiveintType,Der_RestIntStartTime) as startdatetimerestrictiveint,
 combine_mhsds_date_and_time(Der_RestIntEndDate,Der_RestIntEndTime) as enddatetimerestrictiveint,
 combine_mhsds_date_and_time(Der_WrdStartDate,Der_WrdStartTime) as startdatetimewardstay,
 combine_mhsds_date_and_time(Der_WrdEndDate,Der_WrdEndTime) as enddatetimewardstay,
 combine_mhsds_date_and_time(rs.startdaterestrictiveintinc,Der_RestIntIncStartTime) as startdatetimerestrictiveintinc,
 combine_mhsds_date_and_time(Der_RestIntIncEndDate,Der_RestIntIncEndTime) as enddatetimerestrictiveintinc,
 ws.startdatewardstay, 
 ws.enddatewardstay, 
 ws.Der_WrdStartDate,
 ws.Der_WrdEndDate,
 ws.bed_days_ws,                                                         ------------Ward Stay Flag changes--
 rt.mhs515uniqid, 
 rt.UniqRestrictiveIntTypeID,
 hs.bed_days, 
 hs.StartDateHospProvSpell,
 hs.Der_HospSpellEndDate,
 rs.UniqRestrictiveIntIncID,
 ROW_NUMBER() OVER (PARTITION BY hs.uniqhospprovspellid ORDER BY rt.UniqRestrictiveIntTypeID desc) AS Der_RecordNumber,                       -----AM: V5 updated ranking to pick one hospspell incase of duplicates
 ROW_NUMBER() OVER (PARTITION BY ws.uniqwardstayid ORDER BY rt.UniqRestrictiveIntTypeID desc) AS Der_RecordNumber_WS                      ----AT: Added Ranking for WardStayID for Der_Bed_Days_WS calculation to avoid dupes

 from $db_output.RI_HOSP_SPELL hs
 left join $db_output.RI_RESTRAINTS rs on hs.uniqhospprovspellid = rs.uniqhospprovspellid and hs.person_id = rs.person_id 
 left join $db_output.RI_RESTRAINT_TYPE_v2 rt on rs.UniqRestrictiveIntIncID = rt.UniqRestrictiveIntIncID and rs.person_id = rt.person_id and rs.uniqhospprovspellid = rt.uniqhospprovspellid
 left join $db_output.RI_WARDSTAY ws on hs.uniqhospprovspellid = ws.uniqhospprovspellid and hs.person_id = ws.person_id --and ws.startdatewardstay <= rt.startdaterestrictiveintType and ws.Der_WrdEndDate >= rt.Der_RestIntEndDate
 left join $db_output.RI_MPI_FINAL m on hs.person_id = m.person_id and hs.uniqmonthid = m.uniqmonthid 
 left join $db_output.RI_HEADER h on hs.uniqmonthid = h.uniqmonthid
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on rt.restrictiveinttype = int.key
 left join menh_publications.NHSDEthnicityDim as eth2 on m.NHSDEthnicity = eth2.key
 left join menh_publications.GenderDim as gen2 on m.Gender = gen2.Code
 left join $db_output.RI_STP_MAPPING stp on m.IC_Rec_CCG = stp.CCG_CODE
 left join $db_output.RESTR_ORG_DAILY_A od on ws.siteidoftreat = od.ORG_CODE

# COMMAND ----------

# DBTITLE 1,Create Initial Prep Table - Part2
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PREP2 AS
 select 
 ri.ReportingPeriodStartDate,
 ri.ReportingPeriodEndDate,
 ri.uniqhospprovspellid,
 ri.uniqwardstayid, 
 ri.person_id, 
 ri.orgidprov,
 ri.ccg_code,
 ri.ccg_name,
 ri.stp_code,
 ri.stp_name,
 ri.region_code,
 ri.region_name,
 ri.age_group,
 ri.provider_type,
 ri.UpperEthnicityCode,
 ri.UpperEthnicityName,
 ri.LowerEthnicityCode,
 ri.LowerEthnicity,
 ri.GenderCode,
 ri.GenderName,

 ------------------WS flag changes: Using  Ward stay fields linked to hosp spell for bed day calculation--
 ri.siteidoftreat as siteidoftreat_bd,
 ri.site_name as site_name_bd,
 ri.specialised_service as specialised_service_bd,

 ri.restrictiveintcode,
 ri.restrictiveintname,
 ri.restrictiveinttype,
 ri.SpecialisedMHServiceCode,
 --ri.specialised_service,
 ri.patient_injury,
 ri.patient_injury_binary,
 ri.RestrictiveIntReasonCode,
 ri.RestrictiveIntReasonName,
 ri.startdaterestrictiveintType, 
 ri.enddaterestrictiveintType,
 ri.starttimerestrictiveintType,
 ri.endtimerestrictiveintType,
 ri.Der_RestIntStartDate,
 ri.Der_RestIntEndDate,
 ri.Der_RestIntIncStartDate,
 ri.Der_RestIntIncEndDate,
 ri.StartDateHospProvSpell,
 ri.Der_HospSpellEndDate,
 ri.pre_flag_type,
 ri.pre_flag_inc,
 ri.active_pre_flag_type,
 ri.active_pre_flag_inc,
 case
 when Der_RestIntStartDate = Der_RestIntEndDate and starttimerestrictiveinttype = endtimerestrictiveinttype then 'N'
 when Der_RestIntStartDate = Der_RestIntEndDate and starttimerestrictiveinttype > endtimerestrictiveinttype then 'N'
 when startdaterestrictiveintType < StartDateHospProvSpell or Der_RestIntEndDate > Der_HospSpellEndDate then 'N'                           ----AM: changed this for V5
 else ri.pre_flag_type end as avg_min_flag_type,
 case
 when Der_RestIntIncStartDate = Der_RestIntIncEndDate and starttimerestrictiveintinc = endtimerestrictiveintinc then 'N'
 when Der_RestIntIncStartDate = Der_RestIntIncEndDate and starttimerestrictiveintinc > endtimerestrictiveintinc then 'N'
 when startdaterestrictiveintinc < StartDateHospProvSpell or Der_RestIntIncEndDate > Der_HospSpellEndDate then 'N'                           ----AM: changed this for V5
 else ri.pre_flag_inc end as avg_min_flag_inc,
 case
 when Der_RestIntStartDate = Der_RestIntEndDate and starttimerestrictiveinttype = endtimerestrictiveinttype then 'N'
 when Der_RestIntStartDate = Der_RestIntEndDate and starttimerestrictiveinttype > endtimerestrictiveinttype then 'N'
 when startdaterestrictiveintType < StartDateHospProvSpell or Der_RestIntEndDate > Der_HospSpellEndDate then 'N'                           ----AM: changed this for V5
 else ri.active_pre_flag_type end as active_avg_min_flag_type,
 case
 when Der_RestIntIncStartDate = Der_RestIntIncEndDate and starttimerestrictiveintinc = endtimerestrictiveintinc then 'N'
 when Der_RestIntIncStartDate = Der_RestIntIncEndDate and starttimerestrictiveintinc > endtimerestrictiveintinc then 'N'
 when startdaterestrictiveintinc < StartDateHospProvSpell or Der_RestIntIncEndDate > Der_HospSpellEndDate then 'N'                           ----AM: changed this for V5
 else ri.active_pre_flag_inc end as active_avg_min_flag_inc,
 ri.rawstartdatetimerestrictiveint,
 ri.rawenddatetimerestrictiveint,
 ri.startdatetimerestrictiveint,
 ri.enddatetimerestrictiveint,
 ri.startdatetimerestrictiveintinc,
 ri.enddatetimerestrictiveintinc,
 ri.startdatewardstay, 
 ri.enddatewardstay, 
 ri.Der_WrdStartDate,
 ri.Der_WrdEndDate,
 ri.startdatetimewardstay,
 ri.enddatetimewardstay,
 ri.mhs515uniqid, 
 ri.UniqRestrictiveIntTypeID,
 ri.UniqRestrictiveIntIncID,                                                                                                                    ----AM: V5 changes
 ri.bed_days,
 ri.bed_days_ws,                                                         ------------Ward Stay Flag changes--
 ri.Der_RecordNumber,
 COALESCE(od.NAME, 'UNKNOWN') as orgidname,
 ---Using absolute value as sometimes start and end datetime are inputted into the wrong columns
 CAST(CAST(rawenddatetimerestrictiveint as long) - CAST(rawstartdatetimerestrictiveint as long) as float) / 60 as raw_minutes_of_restraint,
 CAST(CAST(enddatetimerestrictiveint as long) - CAST(startdatetimerestrictiveint as long) as float) / 60 as minutes_of_restraint,
 CAST(CAST(enddatetimerestrictiveint as long) - CAST(startdatetimerestrictiveint as long) as float) / 86400 as days_of_restraint,
 ABS(CAST(CAST(enddatetimerestrictiveint as long) - CAST(startdatetimerestrictiveint as long) as float) / 60) as abs_minutes_of_restraint,
 CAST(CAST(enddatetimerestrictiveintinc as long) - CAST(startdatetimerestrictiveintinc as long) as float) / 60 as minutes_of_incident,
 CAST(CAST(enddatetimerestrictiveintinc as long) - CAST(startdatetimerestrictiveintinc as long) as float) / 86400 as days_of_incident,
 ABS(CAST(CAST(enddatetimerestrictiveintinc as long) - CAST(startdatetimerestrictiveintinc as long) as float) / 60) as abs_minutes_of_incident,
 case when Der_RecordNumber = 1 then bed_days else 0 end as Der_bed_days,
 case when Der_RecordNumber_WS = 1 then bed_days_ws else 0 end as Der_bed_days_ws,
 -----Ward Stay Flag---
 -----New flag to link restraints to wardstay (using start & end date of restraints and start -end date of wardstay)------------
 case when ri.enddatetimerestrictiveint >= ri.startdatetimewardstay and ri.startdatetimerestrictiveint <= ri.enddatetimewardstay then 'Y' else 'N' end as ss_type_ward_flag

 from global_temp.RI_PREP ri
 LEFT JOIN $db_output.RESTR_ORG_DAILY_A as od
           ON COALESCE(ri.orgidprov, 'UNKNOWN') = COALESCE(od.ORG_CODE) AND od.BUSINESS_END_DATE IS NULL
           AND od.ORG_OPEN_DATE <= '$rp_enddate'
           AND ((od.ORG_CLOSE_DATE >= '$rp_startdate') OR od.ORG_CLOSE_DATE is NULL)

# COMMAND ----------

# DBTITLE 1,Create Final Prep Table
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PREP3 AS
 select *,
 ---------------WS flag changes: ---
 case when ss_type_ward_flag = 'Y' then specialised_service_bd else 'No associated Ward Stay' end as specialised_service,
 case when ss_type_ward_flag = 'Y' then siteidoftreat_bd else 'No associated Ward Stay' end as siteidoftreat,
 case when ss_type_ward_flag = 'Y' then site_name_bd else 'No associated Ward Stay' end as site_name,
 ----*WS flag changes: Created this ranking so that when we group MHS76, MHS77 & MHS96 by specialised service & prov site, we pick only relevent wardstay for a restraint type
 dense_rank() OVER (PARTITION BY UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID ORDER BY ss_type_ward_flag desc, enddatetimewardstay, startdatetimewardstay) AS ss_type_ward_Rank,
  CASE 
 -- Physical, Chemical and mechanical restraints
 WHEN restrictiveintcode IN ('01', '04', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17') AND minutes_of_restraint BETWEEN 0 and 10 THEN '0-10 Minutes' 
 WHEN restrictiveintcode IN ('01', '04', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17') AND minutes_of_restraint BETWEEN 11 and 30 THEN '11-30 Minutes' 
 WHEN restrictiveintcode IN ('01', '04', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17') AND minutes_of_restraint BETWEEN 31 and 60 THEN '31-60 Minutes' 
 WHEN restrictiveintcode IN ('01', '04', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17') AND minutes_of_restraint > 60 THEN '60+ Minutes' 
 -- Seclusions
 WHEN restrictiveintcode = '05' AND (minutes_of_restraint/60)/24 BETWEEN 3 and 7 THEN '72 Hours-7 Days' 
 WHEN restrictiveintcode = '05' AND (minutes_of_restraint/60)/24 BETWEEN 8 and 14 THEN '1-2 Weeks' 
 WHEN restrictiveintcode = '05' AND (minutes_of_restraint/60)/24 BETWEEN 15 and 28 THEN '2-4 Weeks' 
 WHEN restrictiveintcode = '05' AND (minutes_of_restraint/60)/24 BETWEEN 29 and 90 THEN '1-3 Months'
 WHEN restrictiveintcode = '05' AND (minutes_of_restraint/60)/24 > 90 THEN '3 Months+' 
 WHEN restrictiveintcode = '05' AND minutes_of_restraint BETWEEN 0 AND 1440 THEN '0-24 Hours'
 WHEN restrictiveintcode = '05' AND minutes_of_restraint BETWEEN 1441 AND 4320 THEN '24-72 Hours' 
 -- Long Term Segregation
 WHEN restrictiveintcode = '06' AND (minutes_of_restraint/60)/24 BETWEEN 0 and 30 THEN '0-1 Months' 
 WHEN restrictiveintcode = '06' AND (minutes_of_restraint/60)/24 BETWEEN 31 and 90 THEN '1-3 Months' 
 WHEN restrictiveintcode = '06' AND (minutes_of_restraint/60)/24 BETWEEN 91 and 180 THEN '3-6 Months' 
 WHEN restrictiveintcode = '06' AND (minutes_of_restraint/60)/24 BETWEEN 181 and 365 THEN '6-12 Months'
 WHEN restrictiveintcode = '06' AND (minutes_of_restraint/60)/24 > 365 THEN '12 Months+' 
 else 'Missing/Invalid' end as length_of_restraint,
 case when age_group = 'Under 18' then 'Children and Young People' else 'Adults' end as age_category,
 case when upperethnicityname in ('White', 'Other') then 'Non-BAME'
      when upperethnicityname in ('Not Stated', 'UNKNOWN') then 'Unknown/Not Stated'
      else 'BAME' end as bame_group,
 'a' as d
 FROM global_temp.RI_PREP2

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PREP4 AS
 select *,
 ROW_NUMBER() OVER (PARTITION BY UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID ORDER BY ss_type_ward_Rank desc) AS RI_RecordNumber_Type ---AM: V5 updated ranking to pick a RI type incase of dup
 from global_temp.RI_PREP3

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RI_FINAL;
 CREATE TABLE $db_output.RI_FINAL AS
 select *,
 ROW_NUMBER() OVER (PARTITION BY UniqRestrictiveIntIncID ORDER BY ss_type_ward_Rank desc, RI_RecordNumber_Type asc) AS RI_RecordNumber_Inc
 from global_temp.RI_PREP4

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.RI_FINAL

# COMMAND ----------

 %sql
 select 
 count(distinct Person_ID), count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) from $db_output.RI_FINAL

# COMMAND ----------

# DBTITLE 1,Create Prep Table for Percentage of People in Hospital subject to a restrictive intervention
 %sql
 ----gets number of people in hospital
 DROP TABLE IF EXISTS $db_output.RI_HOSPITAL_PERC;
 CREATE TABLE $db_output.RI_HOSPITAL_PERC AS
 select b.orgidprov, b.uniqmonthid, count(distinct b.person_id) as people
 FROM $db_output.RI_REFERRALS a          
 INNER JOIN $db_output.RI_HOSP_SPELL b
 ON a.UniqServReqID = b.UniqServReqID 
 AND a.person_id = b.person_id
 AND a.OrgIDProv = b.OrgIDProv
 AND a.UniqMonthID = '$month_id'
 -- where (a.ServDischDate is null or a.ServDischDate > '$rp_enddate') 
 -- AND (b.DischDateHospProvSpell IS NULL OR b.DischDateHospProvSpell > '$rp_enddate')
 group by b.orgidprov, b.uniqmonthid

# COMMAND ----------

#bed days denominator for each demographic for ri per 1000 occupied days calc when broken down by restrictiveinttype

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.RI_FINAL

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.bed_days_pub_csv;
 CREATE TABLE IF NOT EXISTS $db_output.bed_days_pub_csv AS
 select 
 coalesce(age_group, 'NULL') as bd_age_group, 
 coalesce(gendercode, 'NULL') as bd_gendercode, 
 coalesce(gendername, 'NULL') as bd_gendername, 
 coalesce(upperethnicitycode, 'NULL') as bd_upperethnicitycode, 
 coalesce(upperethnicityname, 'NULL') as bd_upperethnicityname, 
 coalesce(lowerethnicitycode, 'NULL') as bd_lowerethnicitycode, 
 coalesce(lowerethnicity, 'NULL') as bd_lowerethnicity, 
 coalesce(orgidprov, 'NULL') as bd_orgidprov, 
 coalesce(orgidname, 'NULL') as bd_orgidname, 
 coalesce(region_code, 'NULL') as bd_region_code, 
 coalesce(region_name, 'NULL') as bd_region_name, 
 --coalesce(specialised_service, 'NULL') as bd_specialised_service, 
 --coalesce(siteidoftreat, 'NULL') as bd_siteidoftreat,
 --coalesce(site_name, 'NULL') as bd_site_name,
 coalesce(provider_type, 'NULL') as bd_provider_type,
 coalesce(length_of_restraint, 'NULL') as bd_length_of_restraint,
 coalesce(age_category, 'NULL') as bd_age_category,
 coalesce(bame_group, 'NULL') as bd_bame_group,
 sum(Der_bed_days) as bed_days
 from $db_output.RI_FINAL 
 group by grouping sets (
 (age_group),
 (gendercode, gendername),
 (upperethnicitycode, upperethnicityname),
 (lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname),
 (region_code, region_name),
 --(specialised_service),
 --(siteidoftreat, site_name),
 (provider_type),
 (length_of_restraint),
 (age_category),
 (bame_group),
 (region_code, region_name, age_group),
 (region_code, region_name, gendercode, gendername),
 (region_code, region_name, upperethnicitycode, upperethnicityname),
 (region_code, region_name, lowerethnicitycode, lowerethnicity),
 --(region_code, region_name, siteidoftreat, site_name),
 (region_code, region_name, provider_type),
 (region_code, region_name, length_of_restraint),
 (region_code, region_name, age_category),
 (region_code, region_name, bame_group),
 (orgidprov, orgidname, age_group),
 (orgidprov, orgidname, gendercode, gendername),
 (orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 --(orgidprov, orgidname, siteidoftreat, site_name),
 (orgidprov, orgidname, provider_type),
 (orgidprov, orgidname, length_of_restraint),
 (orgidprov, orgidname, age_category),
 (orgidprov, orgidname, bame_group),
 --(specialised_service, age_group),
 --(specialised_service, gendercode, gendername),
 --(specialised_service, upperethnicitycode, upperethnicityname),
 --(specialised_service, lowerethnicitycode, lowerethnicity),
 --(specialised_service, siteidoftreat, site_name),
 --(specialised_service, provider_type),
 --(specialised_service, length_of_restraint),
 --(specialised_service, age_category),
 --(specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, orgidprov, orgidname, age_group),
 (region_code, region_name, orgidprov, orgidname, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 --(region_code, region_name, orgidprov, orgidname, siteidoftreat, site_name),
 (region_code, region_name, orgidprov, orgidname, provider_type),
 (region_code, region_name, orgidprov, orgidname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, age_category),
 (region_code, region_name, orgidprov, orgidname, bame_group),
 --(region_code, region_name, specialised_service),
 /*(region_code, region_name, specialised_service, age_group),
 (region_code, region_name, specialised_service, gendercode, gendername),
 (region_code, region_name, specialised_service, upperethnicitycode, upperethnicityname),
 (region_code, region_name, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service, siteidoftreat, site_name),
 (region_code, region_name, specialised_service, provider_type),
 (region_code, region_name, specialised_service, length_of_restraint),
 (region_code, region_name, specialised_service, age_category),
 (region_code, region_name, specialised_service, bame_group),
 (orgidprov, orgidname, specialised_service),
 (orgidprov, orgidname, specialised_service, age_group),
 (orgidprov, orgidname, specialised_service, gendercode, gendername),
 (orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service, siteidoftreat, site_name),
 (orgidprov, orgidname, specialised_service, provider_type),
 (orgidprov, orgidname, specialised_service, length_of_restraint),
 (orgidprov, orgidname, specialised_service, age_category),
 (orgidprov, orgidname, specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, specialised_service, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service, siteidoftreat, site_name),
 (region_code, region_name, orgidprov, orgidname, specialised_service, provider_type),
 (region_code, region_name, orgidprov, orgidname, specialised_service, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, specialised_service, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group), */
 (gendercode, gendername, upperethnicitycode, upperethnicityname), ---NEW BREAKDOWN
 (age_group, upperethnicitycode, upperethnicityname), ---NEW BREAKDOWN
 (gendercode, gendername, lowerethnicitycode, lowerethnicity) ---NEW BREAKDOWN
 )

# COMMAND ----------

 %sql
 REFRESH TABLE $db_output.RI_final

# COMMAND ----------

# DBTITLE 1,Bed day calculation wardstay related bkdowns (Sp Service, provider site)
 %sql
 DROP TABLE IF EXISTS $db_output.WS_bed_days_pub_csv;
 CREATE TABLE IF NOT EXISTS $db_output.WS_bed_days_pub_csv AS
 select 
 coalesce(age_group, 'NULL') as bd_age_group, 
 coalesce(gendercode, 'NULL') as bd_gendercode, 
 coalesce(gendername, 'NULL') as bd_gendername, 
 coalesce(upperethnicitycode, 'NULL') as bd_upperethnicitycode, 
 coalesce(upperethnicityname, 'NULL') as bd_upperethnicityname, 
 coalesce(lowerethnicitycode, 'NULL') as bd_lowerethnicitycode, 
 coalesce(lowerethnicity, 'NULL') as bd_lowerethnicity, 
 coalesce(orgidprov, 'NULL') as bd_orgidprov, 
 coalesce(orgidname, 'NULL') as bd_orgidname, 
 coalesce(region_code, 'NULL') as bd_region_code, 
 coalesce(region_name, 'NULL') as bd_region_name, 
 coalesce(specialised_service_bd, 'NULL') as bd_specialised_service, 
 coalesce(siteidoftreat_bd, 'NULL') as bd_siteidoftreat,
 coalesce(site_name_bd, 'NULL') as bd_site_name,
 coalesce(provider_type, 'NULL') as bd_provider_type,
 --coalesce(length_of_restraint, 'NULL') as bd_length_of_restraint,
 coalesce(age_category, 'NULL') as bd_age_category,
 coalesce(bame_group, 'NULL') as bd_bame_group,
 sum(der_bed_days_ws) as bed_days
 from $db_output.RI_FINAL
 group by grouping sets (
 (specialised_service_bd),
 (siteidoftreat_bd, site_name_bd),
 (region_code, region_name, siteidoftreat_bd, site_name_bd),
 (orgidprov, orgidname, siteidoftreat_bd, site_name_bd),
 (specialised_service_bd, age_group),
 (specialised_service_bd, gendercode, gendername),
 (specialised_service_bd, upperethnicitycode, upperethnicityname),
 (specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (specialised_service_bd, provider_type),
 ---(specialised_service_bd, length_of_restraint),
 (specialised_service_bd, age_category),
 (specialised_service_bd, bame_group),
 (region_code, region_name, orgidprov, orgidname, siteidoftreat_bd, site_name_bd),
 (region_code, region_name, specialised_service_bd),
 (region_code, region_name, specialised_service_bd, age_group),
 (region_code, region_name, specialised_service_bd, gendercode, gendername),
 (region_code, region_name, specialised_service_bd, upperethnicitycode, upperethnicityname),
 (region_code, region_name, specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (region_code, region_name, specialised_service_bd, provider_type),
 --(region_code, region_name, specialised_service_bd, length_of_restraint),
 (region_code, region_name, specialised_service_bd, age_category),
 (region_code, region_name, specialised_service_bd, bame_group),
 (orgidprov, orgidname, specialised_service_bd),
 (orgidprov, orgidname, specialised_service_bd, age_group),
 (orgidprov, orgidname, specialised_service_bd, gendercode, gendername),
 (orgidprov, orgidname, specialised_service_bd, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (orgidprov, orgidname, specialised_service_bd, provider_type),
 --(orgidprov, orgidname, specialised_service_bd, length_of_restraint),
 (orgidprov, orgidname, specialised_service_bd, age_category),
 (orgidprov, orgidname, specialised_service_bd, bame_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_group),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, siteidoftreat_bd, site_name_bd),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, provider_type),
 --(region_code, region_name, orgidprov, orgidname, specialised_service_bd, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, age_category),
 (region_code, region_name, orgidprov, orgidname, specialised_service_bd, bame_group)

 )

# COMMAND ----------

 %sql

 select sum(der_bed_days_ws)

 from  $db_output.RI_FINAL

# COMMAND ----------

 %sql

 select sum(Der_bed_days)

 from  $db_output.RI_FINAL 

# COMMAND ----------

 %sql
 SELECT RestrictiveIntReasonName, count(distinct Person_ID) as people, count(distinct UniqRestrictiveIntIncID) as incidents, count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints
 from  $db_output.RI_FINAL
 GROUP BY RestrictiveIntReasonName

# COMMAND ----------

 %sql
 SELECT Region_Name, RestrictiveIntReasonName, count(distinct Person_ID) as people, count(distinct UniqRestrictiveIntIncID) as incidents, count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints
 from  $db_output.RI_FINAL
 GROUP BY Region_Name, RestrictiveIntReasonName
 ORDER BY Region_Name, RestrictiveIntReasonName

# COMMAND ----------

 %sql
 SELECT RestrictiveIntName, RestrictiveIntReasonName, count(distinct Person_ID) as people, count(distinct UniqRestrictiveIntIncID) as incidents, count(distinct UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID) as restraints
 from  $db_output.RI_FINAL
 GROUP BY RestrictiveIntName, RestrictiveIntReasonName
 ORDER BY RestrictiveIntName, RestrictiveIntReasonName

# COMMAND ----------

 %sql
 select UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID, count(distinct siteidoftreat)
 from $db_output.RI_FINAL
 where person_id is not null and ss_type_ward_Rank = 1
 group by UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID
 having count(distinct siteidoftreat) > 1

# COMMAND ----------

 %sql
 select UniqHospProvSpellID, UniqWardStayID, startdatewardstay, enddatewardstay, startdatetimerestrictiveint, enddatetimerestrictiveint, UniqRestrictiveIntIncID, UniqRestrictiveIntTypeID
 from $db_output.ri_final where UniqRestrictiveIntIncID = "RWV139422" and UniqRestrictiveIntTypeID = "RWV1394223"

# COMMAND ----------

 %sql
 select * from $db_output.RI_WARDSTAY where UniqWardStayID in ("RWV53870618", "RWV61790103")