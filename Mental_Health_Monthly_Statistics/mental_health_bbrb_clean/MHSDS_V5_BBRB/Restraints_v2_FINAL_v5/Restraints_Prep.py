# Databricks notebook source
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# DBTITLE 1,Region Ref Data Part1
 %sql
 DROP TABLE IF EXISTS $db_output.RESTR_ORG_DAILY_1;
 CREATE TABLE $db_output.RESTR_ORG_DAILY_1 USING DELTA AS 
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM $ref_database.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate'               
 OPTIMIZE $db_output.RESTR_ORG_DAILY_1;              

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
 $ref_database.ORG_RELATIONSHIP_DAILY
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
 $db_output.RESTR_ORG_DAILY_1 A
 LEFT JOIN $db_output.RESTR_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.RESTR_ORG_DAILY_1 C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.RESTR_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.RESTR_ORG_DAILY_1 E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

def get_provider_type (providername):
  if providername[0] == 'R' or providername[0] == 'T':
    return 'NHS Providers'
  return 'Non NHS Providers'
spark.udf.register("get_provider_type", get_provider_type)

# COMMAND ----------

# DBTITLE 1,Submission Picker to get Performance or Latest Submitted Data
 %sql
 ---Comment out FILETYPE <=2 when picking latest submitted data 
 DROP TABLE IF EXISTS $db_output.RI_SUBS;
 CREATE TABLE $db_output.RI_SUBS AS 
 select
 ReportingPeriodEndDate, ReportingPeriodStartDate ,
 uniqmonthid, 
 ORGIDPROVIDER,
 MAX(UniqSubmissionID) AS UNIQSUBMISSIONID
 from
 $db_source.mhs000header
 where
 UNIQMONTHID <= '$month_id'
 GROUP BY 
 ReportingPeriodEndDate, ReportingPeriodStartDate , UNIQMONTHID, ORGIDPROVIDER

# COMMAND ----------

# DBTITLE 1,Header Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_HEADER;
 CREATE TABLE $db_output.RI_HEADER AS
 select distinct uniqmonthid, ReportingPeriodEndDate, ReportingPeriodStartDate 
 from $db_source.mhs000header

# COMMAND ----------

# DBTITLE 1,Restrictive Interventions Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_RESTRAINTS;
 CREATE TABLE $db_output.RI_RESTRAINTS AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.mhs505uniqid, a.restrictiveintincid, a.startdaterestrictiveintinc, a.enddaterestrictiveintinc, a.starttimerestrictiveintinc, a.endtimerestrictiveintinc, 
 CASE WHEN StartDateRestrictiveIntInc < '$rp_startdate' THEN '$rp_startdate' ELSE StartDateRestrictiveIntInc END AS Der_RestIntStartDate,
 CASE WHEN EndDateRestrictiveIntInc is null then date_add('$rp_enddate', 1) ELSE EndDateRestrictiveIntInc END AS Der_RestIntEndDate,
 CASE WHEN starttimerestrictiveintInc is null then to_timestamp('1970-01-01') else starttimerestrictiveintInc end as Der_RestIntStartTime, 
 CASE WHEN endtimerestrictiveintInc is null then to_timestamp('1970-01-01') else endtimerestrictiveintInc end as Der_RestIntEndTime,
 case 
 when startdaterestrictiveintinc < '$rp_startdate' 
 or enddaterestrictiveintinc > '$rp_enddate' 
 or startdaterestrictiveintinc is null 
 or starttimerestrictiveintinc is null 
 or enddaterestrictiveintinc is null 
 or endtimerestrictiveintinc is null
 then 'N'
 else 'Y' end as pre_flag,
 a.recordnumber
 from $db_source.mhs505restrictiveinterventinc a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'
 and ((a.Startdaterestrictiveintinc BETWEEN '$rp_startdate' AND '$rp_enddate') 
                             or (a.Enddaterestrictiveintinc BETWEEN '$rp_startdate' AND '$rp_enddate')
                             or (a.Enddaterestrictiveintinc is null))

# COMMAND ----------

# DBTITLE 1,v5 - mhs515 Restrictive Intervention Type - replacing mhs505
 %sql
 DROP TABLE IF EXISTS $db_output.RI_RESTRAINT_TYPE;
 CREATE TABLE $db_output.RI_RESTRAINT_TYPE AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.restrictiveinttype, a.UniqRestrictiveIntIncID, a.mhs515uniqid, a.startdaterestrictiveinttype, a.enddaterestrictiveinttype, a.starttimerestrictiveinttype, a.endtimerestrictiveinttype, a.uniqhospprovspellid, ---added for v5 /*** a.UniqWardStayID replaced by  a.UniqRestrictiveIntIncID, to be checked, required due to ToS  change***/
 CASE WHEN StartDateRestrictiveIntType < '$rp_startdate' THEN '$rp_startdate' ELSE StartDateRestrictiveIntType END AS Der_RestIntStartDate,
 CASE WHEN EndDateRestrictiveIntType is null then date_add('$rp_enddate', 1) ELSE EndDateRestrictiveIntType END AS Der_RestIntEndDate,
 CASE WHEN starttimerestrictiveintType is null then to_timestamp('1970-01-01') else starttimerestrictiveintType end as Der_RestIntStartTime, 
 CASE WHEN endtimerestrictiveintType is null then to_timestamp('1970-01-01') else endtimerestrictiveintType end as Der_RestIntEndTime,
 case 
 when startdaterestrictiveintType < '$rp_startdate' 
 or enddaterestrictiveintType > '$rp_enddate' 
 or startdaterestrictiveintType is null 
 or starttimerestrictiveintType is null 
 or enddaterestrictiveintType is null 
 or endtimerestrictiveintType is null
 then 'N'
 else 'Y' end as pre_flag,
 a.recordnumber
 from $db_source.mhs515restrictiveinterventtype a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'
 and ((a.startdaterestrictiveinttype BETWEEN '$rp_startdate' AND '$rp_enddate') /*** updated to startdaterestrictiveinttype for v5 **/
                             or (a.enddaterestrictiveinttype BETWEEN '$rp_startdate' AND '$rp_enddate') /*** updated to enddaterestrictiveinttype for v5 ***/
                             or (a.enddaterestrictiveinttype is null)) /*** updated to enddaterestrictiveinttype for v5***/

# COMMAND ----------

# DBTITLE 1,Ward Stay Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_WARDSTAY;
 CREATE TABLE $db_output.RI_WARDSTAY AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.uniqwardstayid, a.siteidoftreat, a.mhs502uniqid, 
 a.uniqhospprovspellid, ---changed from UniqHospProvSpellNum to uniqhospprovspellid for v5
 a.startdatewardstay, a.enddatewardstay, a.inacttimews, 
 CASE WHEN a.StartDateWardStay < '$rp_startdate' THEN '$rp_startdate' ELSE a.StartDateWardStay END AS Der_WrdStartDate,
 CASE WHEN a.EndDateWardStay IS NULL THEN date_add('$rp_enddate', 1) ELSE a.EndDateWardStay END AS Der_WrdEndDate,
 CASE WHEN a.EndTimeWardStay IS NULL THEN to_timestamp('1970-01-01') ELSE a.EndTimeWardStay END AS Der_WrdEndTime, 
 a.recordnumber, a.SpecialisedMHServiceCode, a.StartTimeWardStay, a.EndTimeWardStay, a.WardCode, a.WardLocDistanceHome,
 CASE WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22E' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22E' THEN 'Adult Eating Disorders'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22D' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22D' THEN 'Mental Health Services for the Deaf (Adult)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22B' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22B' THEN 'Tier 4 CAMHS (Deaf Child)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22P' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22P' THEN 'Perinatal Mental Health Services'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22U' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22U' THEN 'Secure and Specialised Mental Health Services (Adult) (High)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22S' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22S' THEN 'Secure and Specialised Mental Health Services (Adult) (Medium and Low)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22C' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22C' THEN 'Tier 4 CAMHS (Medium Secure)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22F' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22F' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder (Adult)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22H' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22H' THEN 'Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder (Child)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS23K' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS23K' THEN 'Tier 4 CAMHS (General Adolescent inc. Eating Disorders)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS23L' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS23L' THEN 'Tier 4 CAMHS (Low Secure)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS23O' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS23O' THEN 'Tier 4 CAMHS (PICU)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS23U' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS23U' THEN 'Tier 4 CAMHS (LD)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS23V' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS23V' THEN 'Tier 4 CAMHS (ASD)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS24C' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS24C' THEN 'Tier 4 CAMHS (Forensic)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS24E' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS24E' THEN 'Tier 4 CAMHS (Childrens Services)'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS22T' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS22T' THEN 'Tier 4 Personality Disorders'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS05E' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS05E' THEN 'Environmental Controls'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS05C' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS05C' THEN 'Communication Aids'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPS08Y' OR UPPER(SpecialisedMHServiceCode) = 'NCBPS08Y' THEN 'Neuropsychiatry'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPSYYY' OR UPPER(SpecialisedMHServiceCode) = 'NCBPSYYY' THEN 'Specialised Mental Health Services Exceptional Packages of Care'
      WHEN UPPER(left(SpecialisedMHServiceCode, position('/', SpecialisedMHServiceCode) - 1)) = 'NCBPSXXX' OR UPPER(SpecialisedMHServiceCode) = 'NCBPSXXX' THEN 'None (Specialised Service but no attributable)'     
      ELSE 'Non-specialist Service' END AS specialised_service,
 datediff(CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                       ELSE EndDateWardStay END
                                       ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateWardStay END) as bed_days
 from $db_source.mhs502wardstay a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,MPI Table Prep
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_MPI AS
 select
 a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.OrgIDCCGRes, a.AgeRepPeriodEnd, a.NHSDEthnicity, ---will eventually need to be changed to Ethnicity2021 when cencus 2021 data released
 a.Gender, a.RecordNumber, a.PatMRecInRP,
 case when a.AgeRepPeriodEnd <= 17 THEN 'Under 18'
                      when a.AgeRepPeriodEnd between 18 and 24 THEN '18-24'
                      when a.AgeRepPeriodEnd between 25 and 34 THEN '25-34'
                      when a.AgeRepPeriodEnd between 35 and 44 THEN '35-44'
                      when a.AgeRepPeriodEnd between 45 and 54 THEN '45-54'
                      when a.AgeRepPeriodEnd between 55 and 64 THEN '55-64'
                      when a.AgeRepPeriodEnd >= 65 THEN '65 or over'
                      else 'UNKNOWN' end as age_group,
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
 SELECT a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.OrgIDCCGRes, a.AgeRepPeriodEnd, a.NHSDEthnicity, a.Gender, a.RecordNumber, a.PatMRecInRP,a.age_group,a.LowerEthnicity
 FROM global_temp.RI_MPI a 
 INNER JOIN global_temp.RI_PATMRECINRP_MPI b on a.person_id = b.person_id and a.RecordNumber = b.RecordNumber

# COMMAND ----------

# DBTITLE 1,Referrals Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_REFERRALS;
 CREATE TABLE $db_output.RI_REFERRALS AS
 select a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.uniqservreqid, a.ServDischDate 
 from $db_source.mhs101referral a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,Hospital Spell Table Prep
 %sql
 DROP TABLE IF EXISTS $db_output.RI_HOSP_SPELL;
 CREATE TABLE $db_output.RI_HOSP_SPELL AS
 select a.person_id, a.orgidprov, a.uniqmonthid, a.uniqsubmissionid, a.uniqservreqid, 
 a.uniqhospprovspellid, ---changed from UniqHospProvSpellNum to uniqhospprovspellid for v5
 a.DischDateHospProvSpell 
 from $db_source.mhs501hospprovspell a
 inner join $db_output.RI_SUBS b on a.uniqsubmissionid = b.uniqsubmissionid and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprovider
 where a.uniqmonthid = '$month_id'

# COMMAND ----------

# DBTITLE 1,Create Initial Prep Table - Part1
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PREP AS
 select 
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 ws.uniqwardstayid, 
 rt.person_id, 
 ws.orgidprov,
 COALESCE(stp.CCG_CODE, 'UNKNOWN') as ccg_code,
 COALESCE(stp.CCG_NAME, 'UNKNOWN') as ccg_name,
 COALESCE(stp.STP_CODE, 'UNKNOWN') as stp_code,
 COALESCE(stp.STP_NAME, 'UNKNOWN') as stp_name,
 COALESCE(stp.REGION_CODE, 'UNKNOWN') as region_code,
 COALESCE(stp.REGION_NAME, 'UNKNOWN') as region_name,
 m.age_group,
 get_provider_type(ws.OrgidProv) as provider_type,
 COALESCE(eth2.id, 'UNKNOWN') as UpperEthnicityCode,
 COALESCE(eth2.description, 'UNKNOWN') as UpperEthnicityName,
 COALESCE(m.NHSDEthnicity, 'UNKNOWN') as LowerEthnicityCode,
 COALESCE(m.LowerEthnicity, 'UNKNOWN') as LowerEthnicity,
 COALESCE(gen2.Code, 'UNKNOWN') as GenderCode,
 COALESCE(gen2.Description, 'UNKNOWN') as GenderName,
 COALESCE(ws.siteidoftreat, 'UNKNOWN') as siteidoftreat,
 COALESCE(od.NAME, 'UNKNOWN') as site_name,
 coalesce(int.key, 'UNKNOWN') as restrictiveintcode,
 coalesce(int.description, 'UNKNOWN') as restrictiveintname,
 rt.restrictiveinttype,
 ws.SpecialisedMHServiceCode,
 ws.specialised_service,
 rt.startdaterestrictiveintType, 
 rt.enddaterestrictiveintType,
 rt.starttimerestrictiveintType,
 rt.endtimerestrictiveintType,
 rt.Der_RestIntStartDate,
 rt.Der_RestIntEndDate,
 rt.pre_flag,
 cast(unix_timestamp(cast(concat(cast(Der_RestIntStartDate as string), right(cast(Der_RestIntStartTime as string), 9)) as timestamp)) as timestamp) as startdatetimerestrictiveint,
 cast(unix_timestamp(cast(concat(cast(Der_RestIntEndDate as string), right(cast(Der_RestIntEndTime as string), 9)) as timestamp)) as timestamp) as enddatetimerestrictiveint,
 cast(unix_timestamp(cast(concat(cast(Der_WrdEndDate as string), right(cast(Der_WrdEndTime as string), 9)) as timestamp)) as timestamp) as enddatetimewardstay,
 ws.startdatewardstay, 
 ws.enddatewardstay, 
 ws.Der_WrdStartDate,
 ws.Der_WrdEndDate,
 rt.mhs515uniqid, 
 ws.bed_days,
 ROW_NUMBER() OVER (PARTITION BY ws.uniqwardstayid ORDER BY rt.mhs515uniqid) AS Der_RecordNumber ---amended for v5 (used to use mhs505uniqid) /*** uniqwardstayid is still present in mhs502 /***
 from $db_output.RI_WARDSTAY ws
 left join $db_output.RI_HOSP_SPELL hs on ws.uniqhospprovspellid = hs.uniqhospprovspellid ---added for v5
 -- left join $db_output.RI_RESTRAINTS r on hs.uniqhospprovspellid = r.uniqhospprovspellid ---mhs505 omitted from methodology (for now)
 left join $db_output.RI_RESTRAINT_TYPE rt on hs.uniqhospprovspellid = rt.uniqhospprovspellid ---added for v5 
 left join $db_output.RI_MPI_FINAL m on ws.person_id = m.person_id and ws.uniqmonthid = m.uniqmonthid 
 left join $db_output.RI_HEADER h on ws.uniqmonthid = h.uniqmonthid
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on rt.restrictiveinttype = int.key
 left join menh_publications.NHSDEthnicityDim as eth2 on m.NHSDEthnicity = eth2.key
 left join menh_publications.GenderDim as gen2 on m.Gender = gen2.Code
 left join $db_output.RI_STP_MAPPING stp on m.OrgIDCCGRes = stp.CCG_CODE
 left join $db_output.RESTR_ORG_DAILY_1 od on ws.siteidoftreat = od.ORG_CODE

# COMMAND ----------

# DBTITLE 1,Create Initial Prep Table - Part2
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW RI_PREP2 AS
 select 
 ri.ReportingPeriodStartDate,
 ri.ReportingPeriodEndDate,
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
 ri.siteidoftreat,
 ri.site_name,
 ri.restrictiveintcode,
 ri.restrictiveintname,
 ri.restrictiveinttype,
 ri.SpecialisedMHServiceCode,
 ri.specialised_service,
 ri.startdaterestrictiveintType, 
 ri.enddaterestrictiveintType,
 ri.starttimerestrictiveintType,
 ri.endtimerestrictiveintType,
 ri.Der_RestIntStartDate,
 ri.Der_RestIntEndDate,
 ri.pre_flag,
 case
 when Der_RestIntStartDate = Der_RestIntEndDate and starttimerestrictiveinttype = endtimerestrictiveinttype then 'N'
 when Der_RestIntStartDate = Der_RestIntEndDate and starttimerestrictiveinttype > endtimerestrictiveinttype then 'N'
 when startdatetimerestrictiveint > enddatetimewardstay or enddatetimerestrictiveint > enddatetimewardstay then 'N' 
 else ri.pre_flag end as avg_min_flag,
 ri.startdatetimerestrictiveint,
 ri.enddatetimerestrictiveint,
 ri.startdatewardstay, 
 ri.enddatewardstay, 
 ri.Der_WrdStartDate,
 ri.Der_WrdEndDate,
 ri.enddatetimewardstay,
 ri.mhs515uniqid, 
 ri.bed_days,
 ri.Der_RecordNumber, 
 COALESCE(od.NAME, 'UNKNOWN') as orgidname,
 ---Using absolute value as sometimes start and end datetime are inputted into the wrong columns
 CAST(CAST(enddatetimerestrictiveint as long) - CAST(startdatetimerestrictiveint as long) as float) / 60 as minutes_of_restraint,
 ABS(CAST(CAST(enddatetimerestrictiveint as long) - CAST(startdatetimerestrictiveint as long) as float) / 60) as abs_minutes_of_restraint,
 case when Der_RecordNumber = 1 then bed_days else 0 end as Der_bed_days
 from global_temp.RI_PREP ri
 LEFT JOIN $db_output.RESTR_ORG_DAILY_1 as od
           ON COALESCE(ri.orgidprov, 'UNKNOWN') = COALESCE(od.ORG_CODE) AND od.BUSINESS_END_DATE IS NULL
           AND od.ORG_OPEN_DATE <= '$rp_enddate'
           AND ((od.ORG_CLOSE_DATE >= '$rp_startdate') OR od.ORG_CLOSE_DATE is NULL)

# COMMAND ----------

# DBTITLE 1,Create Final Prep Table
 %sql
 DROP TABLE IF EXISTS $db_output.RI_FINAL;
 CREATE TABLE $db_output.RI_FINAL AS
 select *,
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
 REFRESH TABLE $db_output.RI_FINAL

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
 group by b.orgidprov, b.uniqmonthid

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.bed_days_pub_csv;
 CREATE TABLE $db_output.bed_days_pub_csv AS
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
 coalesce(specialised_service, 'NULL') as bd_specialised_service, 
 coalesce(siteidoftreat, 'NULL') as bd_siteidoftreat,
 coalesce(site_name, 'NULL') as bd_site_name,
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
 (specialised_service),
 (siteidoftreat, site_name),
 (provider_type),
 (length_of_restraint),
 (age_category),
 (bame_group),
 (region_code, region_name, age_group),
 (region_code, region_name, gendercode, gendername),
 (region_code, region_name, upperethnicitycode, upperethnicityname),
 (region_code, region_name, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, siteidoftreat, site_name),
 (region_code, region_name, provider_type),
 (region_code, region_name, length_of_restraint),
 (region_code, region_name, age_category),
 (region_code, region_name, bame_group),
 (orgidprov, orgidname, age_group),
 (orgidprov, orgidname, gendercode, gendername),
 (orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (orgidprov, orgidname, siteidoftreat, site_name),
 (orgidprov, orgidname, provider_type),
 (orgidprov, orgidname, length_of_restraint),
 (orgidprov, orgidname, age_category),
 (orgidprov, orgidname, bame_group),
 (specialised_service, age_group),
 (specialised_service, gendercode, gendername),
 (specialised_service, upperethnicitycode, upperethnicityname),
 (specialised_service, lowerethnicitycode, lowerethnicity),
 (specialised_service, siteidoftreat, site_name),
 (specialised_service, provider_type),
 (specialised_service, length_of_restraint),
 (specialised_service, age_category),
 (specialised_service, bame_group),
 (region_code, region_name, orgidprov, orgidname),
 (region_code, region_name, orgidprov, orgidname, age_group),
 (region_code, region_name, orgidprov, orgidname, gendercode, gendername),
 (region_code, region_name, orgidprov, orgidname, upperethnicitycode, upperethnicityname),
 (region_code, region_name, orgidprov, orgidname, lowerethnicitycode, lowerethnicity),
 (region_code, region_name, orgidprov, orgidname, siteidoftreat, site_name),
 (region_code, region_name, orgidprov, orgidname, provider_type),
 (region_code, region_name, orgidprov, orgidname, length_of_restraint),
 (region_code, region_name, orgidprov, orgidname, age_category),
 (region_code, region_name, orgidprov, orgidname, bame_group),
 (region_code, region_name, specialised_service),
 (region_code, region_name, specialised_service, age_group),
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
 (region_code, region_name, orgidprov, orgidname, specialised_service, bame_group)
 )

# COMMAND ----------

