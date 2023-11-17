-- Databricks notebook source
%md 

## Outputs
#### [Go to Unsuppressed/raw Output](https://db.core.data.digital.nhs.uk/#notebook/4835364/command/4835401)
#### [Go to Suppressed/rounded Output](https://db.core.data.digital.nhs.uk/#notebook/4835364/command/4835402)

-- COMMAND ----------

%md
##### Please note as of March 2022 the name of the widgets has changed
example below is for: January 2022

**db_output:**
Your database name

**db_source:**
$reference_data

~~**rp_enddate_1m: (previously "rp_enddate")**~~
**rp_enddate:**
this is end of the reporting month, in this example '2022-01-31'

**rp_startdate_qtr: (not previously used)**
this is quarterly so its your "rp_startdate_1m" minus two, so in this example its '2021-11-01'

**status**
this depends on whether you're running 'Performance' or 'Provisional' so remember to update this

////////////////// below not required ///////////////////

**start_month_id (not previoulsy used)** (not used for this process)
this is start of the reporting month so you would use the month from 'rp_startdate_12m' in this example will be '1451' (Feb2021)

**end_month_id (previously "month_id")** (not used for this process)
this is the month you are reporting, eg 1462 (Jan 2022)

**rp_startdate_12m (previoulsy "rp_startdate")** (not used for this process)
this is the start date of the report (12 rolling month) so in this example will be '2021-02-01'

-- COMMAND ----------

%py
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
start_month_id = dbutils.widgets.get("start_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
rp_startdate_12m = dbutils.widgets.get("rp_startdate_12m")
rp_startdate_qtr = dbutils.widgets.get("rp_startdate_qtr")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.subicb_pop;
CREATE TABLE IF NOT EXISTS $db_output.SubICB_POP
(SubICB_2021_CODE string,
SubICB_2021_NAME string, 
SubICB_2020_NAME string,
SubICB_2020_CODE string,
All_Ages int,
Aged_18_to_64 int,
Aged_65_OVER int,
Aged_0_to_17 int
) USING DELTA

-- COMMAND ----------

with reference_list_rn as
(
select 
--       predecessor ID
     geo.DH_GEOGRAPHY_CODE
--       successor ID
      ,SuccessorDetails.TargetOrganisationID
     --predecessor ID
      ,SuccessorDetails.OrganisationID
      ,row_number() over (partition by geo.DH_GEOGRAPHY_CODE order by geo.year_of_change desc) as RN
,sum(pop_v2.population_count) as all_ages
,sum(case when (pop_v2.age_lower > 17 and pop_v2.age_lower < 65) then pop_v2.population_count else 0 end) as Aged_18_to_64
,sum(case when (pop_v2.age_lower > 64) then pop_v2.population_count else 0 end) as Aged_65_OVER
,sum(case when (pop_v2.age_lower < 18) then pop_v2.population_count else 0 end) as Aged_0_to_17
from $reference_data.ONS_POPULATION_V2 pop_v2
inner join  $reference_data.ONS_CHD_GEO_EQUIVALENTS geo
on pop_v2.GEOGRAPHIC_SUBGROUP_CODE = geo.geography_code 
Left join $reference_data.odsapisuccessordetails successorDetails 
on geo.DH_GEOGRAPHY_CODE = successorDetails.Organisationid and successorDetails.Type = 'Successor' and successorDetails.startDate > '2021-03-31'--and geo.IS_CURRENT =1
where pop_v2.GEOGRAPHIC_GROUP_CODE = 'E38'
and geo.ENTITY_CODE = 'E38'
-- and geo.IS_CURRENT =1
and pop_v2.year_of_count = '2020'
and pop_v2.RECORD_TYPE = 'E'
-- and DH_GEOGRAPHY_CODE = '99M'
group by geo.DH_GEOGRAPHY_CODE,successorDetails.TargetOrganisationID,successorDetails.OrganisationID,geo.year_of_change
order by geo.DH_GEOGRAPHY_CODE
 
),
reference_list as 
(
select * from reference_list_rn where RN = 1
)
--when successorDetails.Organisationid is not null then we do have a successor organisation after 31 Mar 2021
--likewise when successorDetails.OrganisationID is null then predecessor and successor orgs are the same (ie no successor)
,no_successor as
(select 
        org1.OrganisationId as ccg_successor_code,
        org1.name as ccg_successor_name,
        org1.name as ccg_predecessor_name, 
        org1.OrganisationId as ccg_predecessor_code,
        ref.All_Ages,
        ref.Aged_18_to_64,
        ref.Aged_65_OVER,
        ref.Aged_0_to_17
from reference_list ref
inner join $reference_data.ODSAPIOrganisationDetails org1
-- inner join $reference_data.org_daily org1
on ref.DH_GEOGRAPHY_CODE = org1.OrganisationId
where ref.OrganisationID is null
and org1.EndDate is null
AND org1.DateType = 'Operational'
order by ccg_predecessor_code   
)

,with_successor_this_one_predecessor as
(select ref2.OrganisationID as id,
        ref2.DH_GEOGRAPHY_CODE,
        org2.name as name, 
        org2.OrganisationId as Org_CODE,
        ref2.All_Ages,
        ref2.Aged_18_to_64,
        ref2.Aged_65_OVER,
        ref2.Aged_0_to_17
from reference_list ref2
inner join $reference_data.ODSAPIOrganisationDetails org2
on ref2.DH_GEOGRAPHY_CODE = org2.OrganisationId
where org2.DateType = 'Operational'
)
,with_successor_this_one_successor as
(select    ref3.OrganisationID as id,
        org3.name as name, 
        ref3.TargetOrganisationId as Org_CODE
from reference_list ref3
inner join $reference_data.ODSAPIOrganisationDetails org3
on ref3.TargetOrganisationId = org3.OrganisationId

where org3.DateType = 'Operational'
)
, with_successor as
(select  
      org_successor.org_code as ccg_successor_code,
      org_successor.Name as ccg_successor_name,
      org_predecessor.Name as ccg_predecessor_name,
      org_predecessor.org_code as ccg_predecessor_code,
      org_predecessor.All_Ages,
      org_predecessor.Aged_18_to_64,
      org_predecessor.Aged_65_OVER,
      org_predecessor.Aged_0_to_17
from with_successor_this_one_predecessor org_predecessor
inner join with_successor_this_one_successor org_successor
on org_predecessor.id = org_successor.id
order by ccg_predecessor_code
)
insert into $db_output.subicb_pop
(
select * from no_successor
union
select * from with_successor
)

-- COMMAND ----------

--DROP TABLE IF EXISTS $db_output.RD_CCG_LATEST; updated 13/9/22 gvf
DROP TABLE IF EXISTS $db_output.RD_SubICB_LATEST;

CREATE TABLE IF NOT EXISTS $db_output.RD_SubICB_LATEST USING DELTA AS
SELECT DISTINCT ORG_CODE,
                NAME
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                AND ORG_TYPE_CODE = 'CC'
                AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                AND ORG_OPEN_DATE <= '$rp_enddate'
                AND NAME NOT LIKE '%HUB'
                AND NAME NOT LIKE '%NATIONAL%'
                
UNION

SELECT 'UNKNOWN' AS ORG_CODE,
       'UNKNOWN' AS NAME;

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.rd_org_daily_latest;

CREATE TABLE IF NOT EXISTS $db_output.RD_ORG_DAILY_LATEST USING DELTA AS
SELECT DISTINCT ORG_CODE, 
                NAME
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN','CQ','PP'); 

-- COMMAND ----------

--DROP TABLE IF EXISTS $db_output.spells 

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.spells;

-- COMMAND ----------

%py
if int(end_month_id) > 1467:
  spark.sql(f"""
             CREATE TABLE {db_output}.SPELLS USING DELTA AS 

              SELECT 

              A.PERSON_ID, 
              A.UniqMonthID,
              A.UniqHospProvSpellID, -- renamed A.UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
              A.OrgIDProv,
              E.NAME AS PROV_NAME,
              A.StartDateHospProvSpell,
              A.DischDateHospProvSpell,
              DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
              B.UniqWardStayID,
              B.HospitalBedTypeMH,
              B.StartDateWardStay,
              B.EndDateWardStay,
              DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS,
              C.AgeRepPeriodEnd,
              -- C.OrgIDCCGRes, ---- Old  CCG derivation retained for now in case used further on gvf
              -- C.OrgIDSubICBLocResidence,
              CASE WHEN a.UniqMonthID <= 1467 then OrgIDCCGRes
                WHEN a.UniqMonthID  > 1467 then OrgIDSubICBLocResidence
                ELSE 'ERROR'
                END AS IC_REC_GP_RES
              -- COALESCE(F.ORG_CODE,'UNKNOWN') AS IC_REC_CCG, --amended for sub-icb/icb 8/9/22 gvf
              -- COALESCE(F.NAME, 'UNKNOWN') as CCG_NAME

              FROM
              {db_source}.MHS501HospProvSpell a 
              LEFT JOIN {db_source}.MHS502WARDSTAY B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber -- renamed UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
              LEFT JOIN {db_source}.MHS001MPI C ON A.RECORDNUMBER = C.RECORDNUMBER --AND c.PatMRecInRP = TRUE - Removed this as using RecordNumber (so will use demographics of person in that provider at time of discharge)
              --LEFT JOIN CCG D ON A.PERSON_ID = D.PERSON_ID -- Removed CCG table since moving to using OrgIDCCGRes, previous prep cells
              LEFT JOIN {db_output}.RD_ORG_DAILY_LATEST E ON A.ORGIDPROV = E.ORG_CODE
              -- LEFT JOIN $db_output.RD_CCG_LATEST F ON C.ORGIDCCGRES = F.ORG_CODE ---amended for sub-icb/icb 8/9/22 gvf (moved to below)


              WHERE
              (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE > '{rp_enddate}') AND a.RECORDSTARTDATE BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
              AND A.DischDateHospProvSpell BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
             """)
else:
  spark.sql(f"""
             CREATE TABLE {db_output}.SPELLS USING DELTA AS 

              SELECT 

              A.PERSON_ID, 
              A.UniqMonthID,
              A.UniqHospProvSpellID, -- renamed A.UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
              A.OrgIDProv,
              E.NAME AS PROV_NAME,
              A.StartDateHospProvSpell,
              A.DischDateHospProvSpell,
              DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
              B.UniqWardStayID,
              B.HospitalBedTypeMH,
              B.StartDateWardStay,
              B.EndDateWardStay,
              DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS,
              C.AgeRepPeriodEnd,
              -- C.OrgIDCCGRes, ---- Old  CCG derivation retained for now in case used further on gvf
              -- C.OrgIDSubICBLocResidence,
              OrgIDCCGRes AS IC_REC_GP_RES
              -- COALESCE(F.ORG_CODE,'UNKNOWN') AS IC_REC_CCG, --amended for sub-icb/icb 8/9/22 gvf
              -- COALESCE(F.NAME, 'UNKNOWN') as CCG_NAME

              FROM
              {db_source}.MHS501HospProvSpell a 
              LEFT JOIN {db_source}.MHS502WARDSTAY B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber -- renamed UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
              LEFT JOIN {db_source}.MHS001MPI C ON A.RECORDNUMBER = C.RECORDNUMBER --AND c.PatMRecInRP = TRUE - Removed this as using RecordNumber (so will use demographics of person in that provider at time of discharge)
              --LEFT JOIN CCG D ON A.PERSON_ID = D.PERSON_ID -- Removed CCG table since moving to using OrgIDCCGRes, previous prep cells
              LEFT JOIN {db_output}.RD_ORG_DAILY_LATEST E ON A.ORGIDPROV = E.ORG_CODE
              -- LEFT JOIN $db_output.RD_CCG_LATEST F ON C.ORGIDCCGRES = F.ORG_CODE ---amended for sub-icb/icb 8/9/22 gvf (moved to below)


              WHERE
              (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE > '{rp_enddate}') AND a.RECORDSTARTDATE BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
              AND A.DischDateHospProvSpell BETWEEN '{rp_startdate_qtr}' AND '{rp_enddate}'
             """)

-- COMMAND ----------

-- -- DROP TABLE IF EXISTS $db_output.spells;

-- CREATE TABLE $db_output.SPELLS USING DELTA AS 

-- SELECT 

-- A.PERSON_ID, 
-- A.UniqMonthID,
-- A.UniqHospProvSpellID, -- renamed A.UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
-- A.OrgIDProv,
-- E.NAME AS PROV_NAME,
-- A.StartDateHospProvSpell,
-- A.DischDateHospProvSpell,
-- DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
-- B.UniqWardStayID,
-- B.HospitalBedTypeMH,
-- B.StartDateWardStay,
-- B.EndDateWardStay,
-- DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS,
-- C.AgeRepPeriodEnd,
-- -- C.OrgIDCCGRes, ---- Old  CCG derivation retained for now in case used further on gvf
-- -- C.OrgIDSubICBLocResidence,
-- CASE WHEN a.UniqMonthID <= 1467 then OrgIDCCGRes
--   WHEN a.UniqMonthID  > 1467 then OrgIDSubICBLocResidence
--   ELSE 'ERROR'
--   END AS IC_REC_GP_RES
-- -- COALESCE(F.ORG_CODE,'UNKNOWN') AS IC_REC_CCG, --amended for sub-icb/icb 8/9/22 gvf
-- -- COALESCE(F.NAME, 'UNKNOWN') as CCG_NAME

-- FROM
-- $db_source.MHS501HospProvSpell a 
-- LEFT JOIN $db_source.MHS502WARDSTAY B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber -- renamed UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
-- LEFT JOIN $db_source.MHS001MPI C ON A.RECORDNUMBER = C.RECORDNUMBER --AND c.PatMRecInRP = TRUE - Removed this as using RecordNumber (so will use demographics of person in that provider at time of discharge)
-- --LEFT JOIN CCG D ON A.PERSON_ID = D.PERSON_ID -- Removed CCG table since moving to using OrgIDCCGRes, previous prep cells
-- LEFT JOIN $db_output.RD_ORG_DAILY_LATEST E ON A.ORGIDPROV = E.ORG_CODE
-- -- LEFT JOIN $db_output.RD_CCG_LATEST F ON C.ORGIDCCGRES = F.ORG_CODE ---amended for sub-icb/icb 8/9/22 gvf (moved to below)


-- WHERE
-- (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE > '$rp_enddate') AND a.RECORDSTARTDATE BETWEEN '$rp_startdate_qtr' AND '$rp_enddate'
-- AND A.DischDateHospProvSpell BETWEEN '$rp_startdate_qtr' AND '$rp_enddate'

-- COMMAND ----------

----this step needs to be done to bin invalid/expired ccg/sub-icb codes into UNKNOWN
DROP TABLE IF EXISTS $db_output.spells1;

CREATE TABLE $db_output.SPELLS1 USING DELTA AS 

SELECT 

A.PERSON_ID, 
A.UniqMonthID,
A.UniqHospProvSpellID, -- renamed A.UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
A.OrgIDProv,
A.PROV_NAME,
A.StartDateHospProvSpell,
A.DischDateHospProvSpell,
A.HOSP_LOS,
A.UniqWardStayID,
A.HospitalBedTypeMH,
A.StartDateWardStay,
A.EndDateWardStay,
A.WARD_LOS,
A.AgeRepPeriodEnd,
-- A.OrgIDCCGRes, ---- Old  CCG derivation retained for now in case used further on gvf
-- A.OrgIDSubICBLocResidence,
COALESCE(F.ORG_CODE,'UNKNOWN') as IC_REC_GP_RES,
COALESCE(F.NAME, 'UNKNOWN') as CCG_NAME
FROM $db_output.SPELLS A
LEFT JOIN $db_output.RD_SubICB_LATEST F ON A.IC_REC_GP_RES = F.ORG_CODE

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.mh_acute_los

-- COMMAND ----------

%py
# spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $db_output.MH_ACUTE_LOS
(
REPORTING_PERIOD_START date,
REPORTING_PERIOD_END date,
STATUS string,
BREAKDOWN string,
PRIMARY_LEVEL string,
PRIMARY_LEVEL_DESCRIPTION string,
SECONDARY_LEVEL string,
SECONDARY_LEVEL_DESCRIPTION string,
MEASURE_ID string,
MEASURE_NAME string,
MEASURE_VALUE int
) USING DELTA

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS100' AS MEASURE_ID,
'The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
$db_output.SPELLS1
WHERE
HOSPITALBEDTYPEMH = '10'
AND HOSP_LOS >= 60
AND AGEREPPERIODEND BETWEEN 18 AND 64

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS101' AS MEASURE_ID,
'The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
$db_output.SPELLS1
WHERE
HOSPITALBEDTYPEMH = '10'
AND HOSP_LOS >= 90
AND AGEREPPERIODEND BETWEEN 18 AND 64

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS102' AS MEASURE_ID,
'The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
$db_output.SPELLS1
WHERE
HOSPITALBEDTYPEMH = '11'
AND HOSP_LOS >= 60
AND AGEREPPERIODEND >= 65

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS103' AS MEASURE_ID,
'The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
$db_output.SPELLS1
WHERE
HOSPITALBEDTYPEMH = '11'
AND HOSP_LOS >= 90
AND AGEREPPERIODEND >= 65

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS104' AS MEASURE_ID,
'The number of people discharged in the RP aged 0 to 17 with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
$db_output.SPELLS1
WHERE
HOSP_LOS >= 60
AND AGEREPPERIODEND BETWEEN 0 AND 17

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'England' AS BREAKDOWN,
'England' AS PRIMARY_LEVEL,
'England' AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS105' AS MEASURE_ID,
'The number of people discharged in the RP aged 0 to 17 with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
$db_output.SPELLS1
WHERE
HOSP_LOS >= 90
AND AGEREPPERIODEND BETWEEN 0 AND 17

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
A.OrgIDProvider AS PRIMARY_LEVEL,
A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS100' AS MEASURE_ID,
'The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT 
  DISTINCT 
  A.ORGIDPROVIDER, 
  B.NAME AS PROV_NAME
  FROM $db_source.MHS000HEADER A
  LEFT JOIN $db_output.RD_ORG_DAILY_LATEST B
        ON A.ORGIDPROVIDER = B.ORG_CODE
  WHERE 
  REPORTINGPERIODENDDATE BETWEEN '$rp_startdate_qtr' and '$rp_enddate') as A
LEFT JOIN $db_output.SPELLS1 B ON A.ORGIDPROVIDER = B.ORGIDPROV 
                                    and B.HOSPITALBEDTYPEMH = '10'
                                    AND B.HOSP_LOS >= 60
                                    AND B.AGEREPPERIODEND BETWEEN 18 AND 64
GROUP BY 
A.OrgIDProvider,
A.Prov_Name

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
A.OrgIDProvider AS PRIMARY_LEVEL,
A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS101' AS MEASURE_ID,
'The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT 
  DISTINCT 
  A.ORGIDPROVIDER, 
  B.NAME AS PROV_NAME
  FROM $db_source.MHS000HEADER A
  LEFT JOIN $db_output.RD_ORG_DAILY_LATEST B
        ON A.ORGIDPROVIDER = B.ORG_CODE
  WHERE 
  REPORTINGPERIODENDDATE BETWEEN '$rp_startdate_qtr' and '$rp_enddate') as A
LEFT JOIN $db_output.SPELLS1 B ON A.ORGIDPROVIDER = B.ORGIDPROV
                                  AND B.HOSPITALBEDTYPEMH = '10'
                                  AND B.HOSP_LOS >= 90
                                  AND B.AGEREPPERIODEND BETWEEN 18 AND 64
GROUP BY 
A.OrgIDProvider,
A.Prov_Name

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
A.OrgIDProvider AS PRIMARY_LEVEL,
A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS102' AS MEASURE_ID,
'The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT 
  DISTINCT 
  A.ORGIDPROVIDER, 
  B.NAME AS PROV_NAME
  FROM $db_source.MHS000HEADER A
  LEFT JOIN $db_output.RD_ORG_DAILY_LATEST B
        ON A.ORGIDPROVIDER = B.ORG_CODE
  WHERE 
  REPORTINGPERIODENDDATE BETWEEN '$rp_startdate_qtr' and '$rp_enddate') as A
LEFT JOIN $db_output.SPELLS1 B ON A.ORGIDPROVIDER = B.ORGIDPROV
                                  AND B.HOSPITALBEDTYPEMH = '11'
                                  AND B.HOSP_LOS >= 60
                                  AND B.AGEREPPERIODEND >= 65
GROUP BY 
A.OrgIDProvider,
A.Prov_Name

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
A.OrgIDProvider AS PRIMARY_LEVEL,
A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS103' AS MEASURE_ID,
'The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT 
  DISTINCT 
  A.ORGIDPROVIDER, 
  B.NAME AS PROV_NAME
  FROM $db_source.MHS000HEADER A
  LEFT JOIN $db_output.RD_ORG_DAILY_LATEST B
        ON A.ORGIDPROVIDER = B.ORG_CODE
  WHERE 
  REPORTINGPERIODENDDATE BETWEEN '$rp_startdate_qtr' and '$rp_enddate') as A
LEFT JOIN $db_output.SPELLS1 B ON A.ORGIDPROVIDER = B.ORGIDPROV
                                AND B.HOSPITALBEDTYPEMH = '11'
                                AND B.HOSP_LOS >= 90
                                AND B.AGEREPPERIODEND >= 65
GROUP BY 
A.OrgIDProvider,
A.Prov_Name

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
A.OrgIDProvider AS PRIMARY_LEVEL,
A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS104' AS MEASURE_ID,
'The number of people discharged in the RP aged 0 to 17 with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT 
  DISTINCT 
  A.ORGIDPROVIDER, 
  B.NAME AS PROV_NAME
  FROM $db_source.MHS000HEADER A
  LEFT JOIN $db_output.RD_ORG_DAILY_LATEST B
        ON A.ORGIDPROVIDER = B.ORG_CODE
  WHERE 
  REPORTINGPERIODENDDATE BETWEEN '$rp_startdate_qtr' and '$rp_enddate') as A
LEFT JOIN $db_output.SPELLS1 B ON A.ORGIDPROVIDER = B.ORGIDPROV
                                  AND B.HOSP_LOS >= 60
                                  AND B.AGEREPPERIODEND BETWEEN 0 AND 17
GROUP BY 
A.OrgIDProvider,
A.Prov_Name

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Provider' AS BREAKDOWN,
A.OrgIDProvider AS PRIMARY_LEVEL,
A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS105' AS MEASURE_ID,
'The number of people discharged in the RP aged 0 to 17 with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT 
  DISTINCT 
  A.ORGIDPROVIDER, 
  B.NAME AS PROV_NAME
  FROM $db_source.MHS000HEADER A
  LEFT JOIN $db_output.RD_ORG_DAILY_LATEST B
        ON A.ORGIDPROVIDER = B.ORG_CODE
  WHERE 
  REPORTINGPERIODENDDATE BETWEEN '$rp_startdate_qtr' and '$rp_enddate') as A
LEFT JOIN $db_output.SPELLS1 B ON A.ORGIDPROVIDER = B.ORGIDPROV
                                  AND B.HOSP_LOS >= 90
                                  AND B.AGEREPPERIODEND BETWEEN 0 AND 17
GROUP BY 
A.OrgIDProvider,
A.Prov_Name

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN, -- updated with space 04/10/22
A.IC_REC_GP_RES AS PRIMARY_LEVEL,
A.SubICB_NAME AS PRIMARY_LEVEL_DESCRIPTION,
--'CCG of Residence' AS BREAKDOWN,
-- A.IC_Rec_CCG AS PRIMARY_LEVEL, amended 13/9/22 gvf
-- A.CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS100' AS MEASURE_ID,
'The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT DISTINCT 
-- ORG_CODE AS IC_REC_CCG,
-- NAME AS CCG_NAME
ORG_CODE AS IC_REC_GP_RES,
NAME AS SubICB_NAME
--FROM $db_output.RD_CCG_LATEST) A amended 13/9/22 gvf
FROM $db_output.RD_SubICB_LATEST) A
LEFT JOIN $db_output.SPELLS1 B ON A.IC_REC_GP_RES = COALESCE(B.IC_REC_GP_RES, "UNKNOWN")
                                and b.HOSPITALBEDTYPEMH = '10'
                                AND b.HOSP_LOS >= 60
                                AND b.AGEREPPERIODEND BETWEEN 18 AND 64
GROUP BY 
A.IC_REC_GP_RES,
A.SubICB_NAME
-- A.IC_REC_CCG,amended 13/9/22 gvf
-- A.CCG_NAME

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
'Sub ICB of Residence' AS BREAKDOWN, -- updated with space 04/10/22
A.IC_REC_GP_RES  AS PRIMARY_LEVEL,
A.SubICB_NAME AS PRIMARY_LEVEL_DESCRIPTION,
-- 'CCG of Residence' AS BREAKDOWN,
-- A.IC_Rec_CCG AS PRIMARY_LEVEL,  amended 13/9/22 gvf
-- A.CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS101' AS MEASURE_ID,
'The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT DISTINCT 
-- ORG_CODE AS IC_REC_CCG,
-- NAME AS CCG_NAME
-- FROM $db_output.RD_CCG_LATEST) A
ORG_CODE AS IC_REC_GP_RES,
NAME AS SubICB_NAME
FROM $db_output.RD_SubICB_LATEST) A
--LEFT JOIN $db_output.SPELLS B ON A.IC_REC_CCG = B.IC_REC_CCG   amended 13/9/22 gvf
LEFT JOIN $db_output.SPELLS1 B ON A.IC_REC_GP_RES = B.IC_REC_GP_RES
                                and b.HOSPITALBEDTYPEMH = '10'
                                AND b.HOSP_LOS >= 90
                                AND b.AGEREPPERIODEND BETWEEN 18 AND 64
GROUP BY 
-- A.IC_REC_CCG,
-- A.CCG_NAMEamended 13/9/22 gvf
A.IC_REC_GP_RES,
A.SubICB_NAME

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS  

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
-- 'CCG of Residence' AS BREAKDOWN,
-- A.IC_Rec_CCG AS PRIMARY_LEVEL,   amended 13/9/22 gvf
-- A.CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'Sub ICB of Residence' AS BREAKDOWN, -- updated with space 04/10/22
A.IC_REC_GP_RES AS PRIMARY_LEVEL,
A.SubICB_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS102' AS MEASURE_ID,
'The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT DISTINCT 
-- ORG_CODE AS IC_REC_CCG,
-- NAME AS CCG_NAME   amended 13/9/22 gvf
-- FROM $db_output.RD_CCG_LATEST) A
ORG_CODE AS IC_REC_GP_RES,
NAME AS SubICB_NAME
FROM $db_output.RD_SubICB_LATEST) A
--LEFT JOIN $db_output.SPELLS B ON A.IC_REC_CCG = B.IC_REC_CCG   amended 13/9/22 gvf
LEFT JOIN $db_output.SPELLS1 B ON A.IC_REC_GP_RES = B.IC_REC_GP_RES
                                and b.HOSPITALBEDTYPEMH = '11'
                                AND b.HOSP_LOS >= 60
                                AND b.AGEREPPERIODEND >= 65
GROUP BY 
-- A.IC_REC_CCG,   amended 13/9/22 gvf
-- A.CCG_NAME
A.IC_REC_GP_RES,
A.SubICB_NAME

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS  

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
-- 'CCG of Residence' AS BREAKDOWN,
-- A.IC_Rec_CCG AS PRIMARY_LEVEL,   amended 13/9/22 gvf
-- A.CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'Sub ICB of Residence' AS BREAKDOWN, -- updated with space 04/10/22
A.IC_REC_GP_RES AS PRIMARY_LEVEL,
A.SubICB_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS103' AS MEASURE_ID,
'The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT DISTINCT 
-- ORG_CODE AS IC_REC_CCG,
-- NAME AS CCG_NAME   amended 13/9/22 gvf
-- FROM $db_output.RD_CCG_LATEST) A
ORG_CODE AS IC_REC_GP_RES,
NAME AS SubICB_NAME
FROM $db_output.RD_SubICB_LATEST) A
--LEFT JOIN $db_output.SPELLS B ON A.IC_REC_CCG = B.IC_REC_CCG   amended 13/9/22 gvf
LEFT JOIN $db_output.SPELLS1 B ON A.IC_REC_GP_RES = B.IC_REC_GP_RES
                                and b.HOSPITALBEDTYPEMH = '11'
                                AND b.HOSP_LOS >= 90
                                AND b.AGEREPPERIODEND >= 65
GROUP BY 
-- A.IC_REC_CCG,
-- A.CCG_NAME   amended 13/9/22 gvf
A.IC_REC_GP_RES,
A.SubICB_NAME

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS --IC_REC_GP_RES   RD_SubICB_LATEST 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
-- 'CCG of Residence' AS BREAKDOWN,
-- A.IC_Rec_CCG AS PRIMARY_LEVEL,   amended 13/9/22 gvf
-- A.CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'Sub ICB of Residence' AS BREAKDOWN, -- updated with space
A.IC_REC_GP_RES AS PRIMARY_LEVEL,
A.SubICB_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS104' AS MEASURE_ID,
'The number of people discharged in the RP aged 0 to 17 with a length of stay of 60+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT DISTINCT 
-- ORG_CODE AS IC_REC_CCG,
-- NAME AS CCG_NAME   amended 13/9/22 gvf
-- FROM $db_output.RD_CCG_LATEST) A
ORG_CODE AS IC_REC_GP_RES,
NAME AS SubICB_NAME
FROM $db_output.RD_SubICB_LATEST) A
--LEFT JOIN $db_output.SPELLS B ON A.IC_REC_CCG = B.IC_REC_CCG   amended 13/9/22 gvf
LEFT JOIN $db_output.SPELLS1 B ON A.IC_REC_GP_RES = B.IC_REC_GP_RES
                                AND B.HOSP_LOS >= 60
                                AND AGEREPPERIODEND BETWEEN 0 AND 17
GROUP BY 
-- A.IC_REC_CCG,   amended 13/9/22 gvf
-- A.CCG_NAME
A.IC_REC_GP_RES,
A.SubICB_NAME

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_LOS --IC_REC_GP_RES   RD_SubICB_LATEST 

SELECT 
'$rp_startdate_qtr' AS REPORTING_PERIOD_START,
'$rp_enddate' AS REPORTING_PERIOD_END,
'$status' AS STATUS,
-- 'CCG of Residence' AS BREAKDOWN,
-- A.IC_Rec_CCG AS PRIMARY_LEVEL,
-- A.CCG_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'Sub ICB of Residence' AS BREAKDOWN, -- updated with space 04/10/22
A.IC_REC_GP_RES AS PRIMARY_LEVEL,
A.SubICB_NAME AS PRIMARY_LEVEL_DESCRIPTION,
'NONE' AS SECONDARY_LEVEL,
'NONE' AS SECONDARY_LEVEL_DESCRIPTION,
'MHS105' AS MEASURE_ID,
'The number of people discharged in the RP aged 0 to 17 with a length of stay of 90+ days' AS MEASURE_NAME,
COALESCE(COUNT(DISTINCT PERSON_ID),0) AS MEASURE_VALUE
FROM 
(SELECT DISTINCT 
-- ORG_CODE AS IC_REC_CCG,
-- NAME AS CCG_NAME
-- FROM $db_output.RD_CCG_LATEST) A
ORG_CODE AS IC_REC_GP_RES,
NAME AS SubICB_NAME
FROM $db_output.RD_SubICB_LATEST) A
--LEFT JOIN $db_output.SPELLS B ON A.IC_REC_CCG = B.IC_REC_CCG
LEFT JOIN $db_output.SPELLS1 B ON A.IC_REC_GP_RES = B.IC_REC_GP_RES
                                  AND B.HOSP_LOS >= 90
                                  AND B.AGEREPPERIODEND BETWEEN 0 AND 17
GROUP BY 
-- A.IC_REC_CCG,
-- A.CCG_NAME
A.IC_REC_GP_RES,
A.SubICB_NAME

-- COMMAND ----------

OPTIMIZE $db_output.MH_ACUTE_LOS

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.mh_acute_rates

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $db_output.MH_ACUTE_RATES
(
REPORTING_PERIOD_START date,
REPORTING_PERIOD_END date,
STATUS string,
BREAKDOWN string,
PRIMARY_LEVEL string,
PRIMARY_LEVEL_DESCRIPTION string,
SECONDARY_LEVEL string,
SECONDARY_LEVEL_DESCRIPTION string,
MEASURE_ID string,
MEASURE_NAME string,
MEASURE_VALUE float
) USING DELTA

-- COMMAND ----------

INSERT INTO $db_output.MH_ACUTE_RATES

SELECT 
a.REPORTING_PERIOD_START,
a.REPORTING_PERIOD_END,
a.STATUS,
a.BREAKDOWN,
a.PRIMARY_LEVEL,
a.PRIMARY_LEVEL_DESCRIPTION,
a.SECONDARY_LEVEL,
a.SECONDARY_LEVEL_DESCRIPTION,
CASE 
  WHEN A.MEASURE_ID = 'MHS100' THEN 'MHS100a' 
  WHEN A.MEASURE_ID = 'MHS101' THEN 'MHS101a' 
  WHEN A.MEASURE_ID = 'MHS102' THEN 'MHS102a' 
  WHEN A.MEASURE_ID = 'MHS103' THEN 'MHS103a'
  WHEN A.MEASURE_ID = 'MHS104' THEN 'MHS104a'
  WHEN A.MEASURE_ID = 'MHS105' THEN 'MHS105a'
  END 
  AS MEASURE_ID,
CASE 
  WHEN A.MEASURE_ID = 'MHS100' THEN 'Rate of people discharged per 100,000 in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days' 
  WHEN A.MEASURE_ID = 'MHS101' THEN 'Rate of people discharged per 100,000 in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days'
  WHEN A.MEASURE_ID = 'MHS102' THEN 'Rate of people discharged per 100,000 in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days' 
  WHEN A.MEASURE_ID = 'MHS103' THEN 'Rate of people discharged per 100,000 in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days'
  WHEN A.MEASURE_ID = 'MHS104' THEN 'Rate of people discharged per 100,000 in the RP aged 0 to 17 with a length of stay of 60+ days'
  WHEN A.MEASURE_ID = 'MHS105' THEN 'Rate of people discharged per 100,000 in the RP aged 0 to 17 with a length of stay of 90+ days'
  END 
  AS MEASURE_NAME, 
COALESCE(CASE 
  WHEN A.MEASURE_ID = 'MHS100' AND A.BREAKDOWN = 'England' THEN (A.MEASURE_VALUE / C.Aged_18_to_64)*100000
  WHEN A.MEASURE_ID = 'MHS101' AND A.BREAKDOWN = 'England' THEN (A.MEASURE_VALUE / C.Aged_18_to_64)*100000
  WHEN A.MEASURE_ID = 'MHS100' THEN (A.MEASURE_VALUE / B.Aged_18_to_64)*100000
  WHEN A.MEASURE_ID = 'MHS101' THEN (A.MEASURE_VALUE / B.Aged_18_to_64)*100000
  WHEN A.MEASURE_ID = 'MHS102' AND A.BREAKDOWN = 'England' THEN (A.MEASURE_VALUE / C.Aged_65_OVER)*100000
  WHEN A.MEASURE_ID = 'MHS103' AND A.BREAKDOWN = 'England' THEN (A.MEASURE_VALUE / C.Aged_65_OVER)*100000
  WHEN A.MEASURE_ID = 'MHS102' THEN (A.MEASURE_VALUE / B.Aged_65_OVER)*100000
  WHEN A.MEASURE_ID = 'MHS103' THEN (A.MEASURE_VALUE / B.Aged_65_OVER)*100000
  WHEN A.MEASURE_ID = 'MHS104' AND A.BREAKDOWN = 'England' THEN (A.MEASURE_VALUE / C.Aged_0_to_17)*100000
  WHEN A.MEASURE_ID = 'MHS105' AND A.BREAKDOWN = 'England' THEN (A.MEASURE_VALUE / C.Aged_0_to_17)*100000
  WHEN A.MEASURE_ID = 'MHS104' THEN (A.MEASURE_VALUE / B.Aged_0_to_17)*100000
  WHEN A.MEASURE_ID = 'MHS105' THEN (A.MEASURE_VALUE / B.Aged_0_to_17)*100000
  END,0) 
  AS MEASURE_VALUE
FROM 
$db_output.MH_ACUTE_LOS A 
LEFT JOIN (SELECT 
            SubICB_2021_CODE,
            SubICB_2021_NAME,
--             CCG_2021_CODE,
--             CCG_2021_NAME,
            SUM(Aged_18_to_64) as Aged_18_to_64,
            SUM(Aged_65_OVER) as Aged_65_OVER,
            SUM(Aged_0_to_17) as Aged_0_to_17
            FROM
           -- $db_output.CCG_POP 
            $db_output.SubICB_POP --updated 27/09/22
            GROUP BY
--             CCG_2021_CODE,
--             CCG_2021_NAME) 
            SubICB_2021_CODE,
            SubICB_2021_NAME) 
            as B
            ON A.PRIMARY_LEVEL = B.SubICB_2021_CODE
LEFT JOIN (SELECT 
            'England' as England, 
            SUM(Aged_18_to_64) AS Aged_18_to_64, 
            SUM(Aged_65_OVER) AS Aged_65_OVER ,
            SUM(Aged_0_to_17) AS Aged_0_to_17
            --FROM $db_output.CCG_POP) 
            FROM $db_output.SubICB_POP) 
            AS C 
            ON A.BREAKDOWN = C.England

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW Data AS

SELECT 
*
FROM 
$db_output.MH_ACUTE_LOS

union all

SELECT 
*
FROM 
$db_output.MH_ACUTE_RATES

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW DATA_FINAL_UNROUNDED AS 

SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
PRIMARY_LEVEL,
PRIMARY_LEVEL_DESCRIPTION,
SECONDARY_LEVEL,
SECONDARY_LEVEL_DESCRIPTION,
MEASURE_ID,
MEASURE_NAME,
MEASURE_VALUE
FROM 
DATA
WHERE 
BREAKDOWN = 'England'

UNION ALL

SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
PRIMARY_LEVEL,
PRIMARY_LEVEL_DESCRIPTION,
SECONDARY_LEVEL,
SECONDARY_LEVEL_DESCRIPTION,
MEASURE_ID,
MEASURE_NAME,
MEASURE_VALUE
FROM 
$db_output.MH_ACUTE_LOS 
WHERE
BREAKDOWN <> 'England'

UNION ALL

SELECT 
A.REPORTING_PERIOD_START,
A.REPORTING_PERIOD_END,
A.STATUS,
A.BREAKDOWN,
A.PRIMARY_LEVEL,
A.PRIMARY_LEVEL_DESCRIPTION,
A.SECONDARY_LEVEL,
A.SECONDARY_LEVEL_DESCRIPTION,
A.MEASURE_ID,
A.MEASURE_NAME,
A.MEASURE_VALUE
FROM 
$db_output.MH_ACUTE_RATES A
LEFT JOIN $db_output.MH_ACUTE_LOS B
      on a.PRIMARY_LEVEL = b.PRIMARY_LEVEL 
      and a.BREAKDOWN = b.BREAKDOWN
          and a.MEASURE_ID = CASE 
                                WHEN b.MEASURE_ID = 'MHS100' then 'MHS100a'
                                WHEN b.MEASURE_ID = 'MHS101' then 'MHS101a'
                                WHEN b.MEASURE_ID = 'MHS102' then 'MHS102a'
                                WHEN b.MEASURE_ID = 'MHS103' then 'MHS103a'
                                WHEN b.MEASURE_ID = 'MHS104' then 'MHS104a'
                                WHEN b.MEASURE_ID = 'MHS105' then 'MHS105a'
                                END
WHERE
A.BREAKDOWN = 'Sub ICB of Residence'


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW DATA_FINAL AS 

SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
PRIMARY_LEVEL,
PRIMARY_LEVEL_DESCRIPTION,
SECONDARY_LEVEL,
SECONDARY_LEVEL_DESCRIPTION,
MEASURE_ID,
MEASURE_NAME,
MEASURE_VALUE
FROM 
DATA
WHERE 
BREAKDOWN = 'England'

UNION ALL

SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
PRIMARY_LEVEL,
PRIMARY_LEVEL_DESCRIPTION,
SECONDARY_LEVEL,
SECONDARY_LEVEL_DESCRIPTION,
MEASURE_ID,
MEASURE_NAME,
CASE 
  WHEN MEASURE_VALUE < 5 THEN 0 
  ELSE ROUND(MEASURE_VALUE/5.0,0)*5
  END AS MEASURE_VALUE
FROM 
$db_output.MH_ACUTE_LOS 
WHERE
BREAKDOWN <> 'England'

UNION ALL

SELECT 
A.REPORTING_PERIOD_START,
A.REPORTING_PERIOD_END,
A.STATUS,
A.BREAKDOWN,
A.PRIMARY_LEVEL,
A.PRIMARY_LEVEL_DESCRIPTION,
A.SECONDARY_LEVEL,
A.SECONDARY_LEVEL_DESCRIPTION,
A.MEASURE_ID,
A.MEASURE_NAME,
CASE 
  WHEN B.MEASURE_VALUE < 5 THEN 0 
  ELSE ROUND(A.MEASURE_VALUE/1.0,0)*1
  END AS MEASURE_VALUE
FROM 
$db_output.MH_ACUTE_RATES A
LEFT JOIN $db_output.MH_ACUTE_LOS B
      on a.PRIMARY_LEVEL = b.PRIMARY_LEVEL 
      and a.BREAKDOWN = b.BREAKDOWN
          and a.MEASURE_ID = CASE 
                                WHEN b.MEASURE_ID = 'MHS100' then 'MHS100a'
                                WHEN b.MEASURE_ID = 'MHS101' then 'MHS101a'
                                WHEN b.MEASURE_ID = 'MHS102' then 'MHS102a'
                                WHEN b.MEASURE_ID = 'MHS103' then 'MHS103a'
                                WHEN b.MEASURE_ID = 'MHS104' then 'MHS104a'
                                WHEN b.MEASURE_ID = 'MHS105' then 'MHS105a'
                                END
WHERE
A.BREAKDOWN = 'Sub ICB of Residence'


-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.los_raw;
CREATE TABLE         $db_output.los_raw USING DELTA
SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
PRIMARY_LEVEL,
PRIMARY_LEVEL_DESCRIPTION,
SECONDARY_LEVEL,
SECONDARY_LEVEL_DESCRIPTION,
MEASURE_ID,
MEASURE_NAME,
MEASURE_VALUE
FROM 
DATA_FINAL_UNROUNDED

-- COMMAND ----------

-- DBTITLE 1,The results of this cell should be saved as MHSDS_LOS_Month_YYYY_Perf_UNROUNDED.csv
select * from $db_output.los_raw 

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.los_supp_final;
CREATE TABLE         $db_output.los_supp_final USING DELTA
SELECT 
REPORTING_PERIOD_START,
REPORTING_PERIOD_END,
STATUS,
BREAKDOWN,
PRIMARY_LEVEL,
PRIMARY_LEVEL_DESCRIPTION,
SECONDARY_LEVEL,
SECONDARY_LEVEL_DESCRIPTION,
MEASURE_ID,
MEASURE_NAME,
CASE
  WHEN MEASURE_VALUE = 0 THEN '*'
  ELSE MEASURE_VALUE
  END AS MEASURE_VALUE
FROM 
DATA_FINAL

-- COMMAND ----------

-- DBTITLE 1,The results of this cell should be saved as MHSDS_LOS_Month_YYYY_Perf.csv
select * from $db_output.los_supp_final

-- COMMAND ----------

%md

### [Go back to top](https://db.core.data.digital.nhs.uk/#notebook/4835364/command/4835366)

-- COMMAND ----------

%py
import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "unsuppressed_table": "los_raw",
  "suppressed_table": "los_supp_final"
}))