# Databricks notebook source
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate
rp_enddate=dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
month_id=dbutils.widgets.get("month_id")
print(month_id)
assert month_id
reference_data=dbutils.widgets.get("reference_data")
print(reference_data)
assert reference_data


# COMMAND ----------

# DBTITLE 1,get_provider_type
def get_provider_type (providername):
  if providername[0] == 'R' or providername[0] == 'T':
    return 'NHS Providers'
  return 'Non NHS Providers'
spark.udf.register("get_provider_type", get_provider_type)

# COMMAND ----------

# DBTITLE 1,RestrictiveInterventionCategories
 %sql
 -- Combination of all categories to report on even when count is 0
 TRUNCATE TABLE $db_output.RestrictiveInterventionCategories;

 INSERT INTO TABLE $db_output.RestrictiveInterventionCategories
 SELECT
 int.key,
 int.description,
 eth.key,
 eth.id,
 eth.description,
 gen.code,
 gen.description,
 ab.description,
 pt.description
 FROM             $db_output.RestrictiveIntTypeDim_Extended as int
 CROSS JOIN       $db_output.NHSDEthnicityDim_Extended as eth
 CROSS JOIN       $db_output.GenderDim_Extended as gen
 CROSS JOIN       $db_output.AgeBandsDim as ab
 CROSS JOIN       $db_output.ProviderTypeDim as pt


# COMMAND ----------

# DBTITLE 1,RestrictiveInterventionCategoriesProvider
 %sql
 -- Combination of all categories and providers to report on even when count is 0
 TRUNCATE TABLE $db_output.RestrictiveInterventionCategoriesProvider;

 INSERT INTO TABLE $db_output.RestrictiveInterventionCategoriesProvider
 SELECT
 ri.RestrictiveIntTypeKey,
 ri.RestrictiveIntTypeDescription,
 ri.EthnicityKey,
 ri.EthnicityID,
 ri.EthnicityDescription,
 COALESCE(dec.NATIONAL_CODE_DESCRIPTION, 'UNKNOWN'),
 ri.GenderCode,
 ri.GenderDescription,
 ri.AgeBand,
 ri.ProviderType,
 org.OrgIDProvider,
 org.Name
 FROM             $db_output.RestrictiveInterventionCategories as ri

 CROSS JOIN
 ((select distinct h.OrgIDProvider from $db_source.mhs000header as h inner join $db_source.mhs501hospprovspell as hps 
     on h.UniqMonthID = hps.UniqMonthID and h.OrgIDProvider = hps.OrgIDProv where h.UniqMonthID = '$month_id') as o
 LEFT JOIN        $reference_data.ORG_DAILY as od
                  ON o.OrgIDProvider = od.ORG_CODE AND od.BUSINESS_END_DATE IS NULL
                  AND od.ORG_OPEN_DATE <= '$rp_enddate'
                  AND ((od.ORG_CLOSE_DATE >= '$rp_startdate') OR od.ORG_CLOSE_DATE is NULL)) as org
 LEFT JOIN        $db_output.EthnicCategory as dec
                  ON ri.EthnicityKey = dec.NATIONAL_CODE

# COMMAND ----------

# DBTITLE 1,Populate MHSRestrictiveInterventionRaw
 %sql
 --Insert one row for each MHS505Restrictiveintervention row in in the period of interest. Derive all fields required for aggresgations
 TRUNCATE TABLE $db_output.MHSRestrictiveInterventionRaw;

 INSERT INTO TABLE $db_output.MHSRestrictiveInterventionRaw
 SELECT
      res.MHS505UniqID
      , cat.OrgIDProvider as OrgIDProv
      , res.Person_ID
      , res.RestrictiveIntType
      , '$month_id' as UniqMonthID
      , res.NHSDEthnicity
      , res.AgeRepPeriodEnd
      , res.Gender
      , cat.OrgName as DerivedOrgIDProvName
      , cat.RestrictiveIntTypeKey as DerivedRestrictiveIntTypeCode
      , cat.RestrictiveIntTypeDescription as DerivedRestrictiveIntTypeDescription
      , cat.EthnicityKey as DerivedEthnicityCode
      , cat.EthnicCategory as DerivedEthnicityDescription
      , cat.EthnicityID as DerivedEthnicityGroupCode
      , cat.EthnicityDescription as DerivedEthnicityGroupDescription
      , cat.ProviderType as DerivedOrgType
      , cat.AgeBand as DerivedAgeBand
      , cat.GenderCode as DerivedGenderCode
      , cat.GenderDescription as DerivedGenderDescription
 FROM             $db_output.RestrictiveInterventionCategoriesProvider as cat
 FULL JOIN        (SELECT 
                  ri.MHS505UniqID
                  ,ri.OrgIDProv
                  , ri.Person_ID
                  , ri.RestrictiveIntType
                  , ri.UniqMonthID
                  , mpi.NHSDEthnicity
                  , mpi.AgeRepPeriodEnd
                  , mpi.Gender
                  , coalesce(int.key, 'UNKNOWN') as DerivedRestrictiveIntTypeCode
                  , COALESCE(eth2.key, 'UNKNOWN') as DerivedEthnicityCode
                  , COALESCE(eth2.id, 'UNKNOWN') as DerivedEthnicityGroupCode
                  , COALESCE(eth2.description, 'UNKNOWN') as DerivedEthnicityGroupDescription
                  , get_provider_type(ri.OrgidProv) as DerivedOrgType
                  , CASE WHEN mpi.AgeRepPeriodEnd <= 17 THEN 'Under 18'
                        WHEN mpi.AgeRepPeriodEnd BETWEEN 18 AND 24 THEN '18-24'
                        WHEN mpi.AgeRepPeriodEnd BETWEEN 25 AND 34 THEN '25-34'
                        WHEN mpi.AgeRepPeriodEnd BETWEEN 35 AND 44 THEN '35-44'
                        WHEN mpi.AgeRepPeriodEnd BETWEEN 45 AND 54 THEN '45-54'
                        WHEN mpi.AgeRepPeriodEnd BETWEEN 55 AND 64 THEN '55-64'
                        WHEN mpi.AgeRepPeriodEnd >= 65 THEN '65 or over'
                        ELSE 'UNKNOWN' END as DerivedAgeBand                 
                  , COALESCE (gen2.Code, 'UNKNOWN') as DerivedGenderCode
                  , COALESCE (gen2.Description, 'UNKNOWN') as DerivedGenderDescription
 FROM             $db_source.MHS505Restrictiveintervention as ri
 LEFT JOIN        $db_output.RestrictiveIntTypeDim_Extended as int
                  on ri.RestrictiveIntType = int.key
 LEFT JOIN        $db_source.MHS001MPI as mpi 
                  ON ri.person_id = mpi.person_id
                  AND ri.uniqmonthid = mpi.uniqmonthid
                  AND mpi.PatMRecInRP = true
 LEFT JOIN        $db_output.NHSDEthnicityDim as eth2
                  ON mpi.NHSDEthnicity = eth2.key
 LEFT JOIN        $db_output.GenderDim as gen2
                  ON mpi.Gender = gen2.Code
                  WHERE ((ri.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
                             or (ri.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
                             or (ri.Enddaterestrictiveint is null))
                  AND ri.uniqmonthid = '$month_id'
                  ) as res
                  ON cat.RestrictiveIntTypeKey = res.DerivedRestrictiveIntTypeCode
                  and cat.EthnicityKey = res.DerivedEthnicityCode
                  and cat.GenderCode = res.DerivedGenderCode
                  and cat.AgeBand = res.DerivedAgeBand
                  and cat.ProviderType = res.DerivedOrgType
                  and cat.OrgIDProvider = res.OrgIDProv

         