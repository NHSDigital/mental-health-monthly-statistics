# Databricks notebook source
 %md 
 # LD Aggregation assets:
 These all INSERT INTO the **LDA_Counts** temp view
 - Table 1 National
 - Table 2 Age 
 - Table 3 Gender 
 - Table 4 Ethnicity
 - Table 5 Distance from home
 - Table 6 Ward security
 - Table 7 Planned discharge
 - Table 8 Planned discharge date
 - Table 9 Respite care
 - Table 10 Length of stay
 - Table 11 Discharge destination grouped
 - Table 12 Ward type
 - Table 13 MHA
 - Table 14 Delayed discharges
 - Table 15 Restraints
 - Table 50 LOS by MHA
 - Table 51 Restraints by age
 - Table 70 Provider totals
 - Table 71 LOS by provider
 - Table 72 Ward type by provider
 - Table 74 Restraints by provider
 - Table 24 Ward security level by provider
 - Table 80 Commissioner groupings
 - Table 90 Commissioner
 - Table 100 TCP Region
 - Table 101 TCP

# COMMAND ----------

# DBTITLE 1,Table 1 National Total Counts 
# %sql
# ------------------ TABLE 1 - NATIONAL TOTAL COUNTS
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table1_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 1 AS TableNumber,
# 'Total' AS PrimaryMeasure,
# 1 AS PrimaryMeasureNumber,
# 'Total' AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
# COUNT(DISTINCT CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE 
# from $db_output.LDA_Data_1


# COMMAND ----------

# DBTITLE 1,Table 2 Age Splits
# %sql
# -------------- TABLE 2 - AGE SPLITS

# --AGE
# --INSERT INTO menh_analysis.LDA_counts

# CREATE OR REPLACE TEMP VIEW Table2_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 2 AS TableNumber,
# 'Age' AS PrimaryMeasure,
# CASE
#        WHEN AgeRepPeriodEnd between 0 and 17 then '1'
# 	   WHEN AgeRepPeriodEnd between 18 and 24 then '2'
# 	   WHEN AgeRepPeriodEnd between 25 and 34 then '3'
# 	   WHEN AgeRepPeriodEnd between 35 and 44 then '4'
# 	   WHEN AgeRepPeriodEnd between 45 and 54 then '5'
# 	   WHEN AgeRepPeriodEnd between 55 and 64 then '6'
# 	   WHEN AgeRepPeriodEnd > 64 then '7'
#        ELSE '8'
#        END AS PrimaryMeasureNumber,
# CASE
#        WHEN AgeRepPeriodEnd between 0 and 17 then 'Under 18'
#        WHEN AgeRepPeriodEnd between 18 and 24 then '18-24'
#        WHEN AgeRepPeriodEnd between 25 and 34 then '25-34'
# 	   WHEN AgeRepPeriodEnd between 35 and 44 then '35-44'
# 	   WHEN AgeRepPeriodEnd between 45 and 54 then '45-54'
# 	   WHEN AgeRepPeriodEnd between 55 and 64 then '55-64'
# 	   WHEN AgeRepPeriodEnd > 64 then '65 and Over'
#        ELSE 'Unknown'
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
# COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY
# CASE
#        WHEN AgeRepPeriodEnd between 0 and 17 then '1'
# 	   WHEN AgeRepPeriodEnd between 18 and 24 then '2'
# 	   WHEN AgeRepPeriodEnd between 25 and 34 then '3'
# 	   WHEN AgeRepPeriodEnd between 35 and 44 then '4'
# 	   WHEN AgeRepPeriodEnd between 45 and 54 then '5'
# 	   WHEN AgeRepPeriodEnd between 55 and 64 then '6'
# 	   WHEN AgeRepPeriodEnd > 64 then '7'
#        ELSE '8'
#        END,
# CASE
#        WHEN AgeRepPeriodEnd between 0 and 17 then 'Under 18'
#        WHEN AgeRepPeriodEnd between 18 and 24 then '18-24'
#        WHEN AgeRepPeriodEnd between 25 and 34 then '25-34'
# 	   WHEN AgeRepPeriodEnd between 35 and 44 then '35-44'
# 	   WHEN AgeRepPeriodEnd between 45 and 54 then '45-54'
# 	   WHEN AgeRepPeriodEnd between 55 and 64 then '55-64'
# 	   WHEN AgeRepPeriodEnd > 64 then '65 and Over'
#        ELSE 'Unknown'
#        END 

# COMMAND ----------

# DBTITLE 1,Table 3 Gender Splits 
# %sql
# ---------------- TABLE 3 - GENDER SPLIT

# -- GENDER
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table3_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 3 AS TableNumber,
# 'Gender' AS PrimaryMeasure,
# CASE
#        WHEN Gender = '1' then '1'
#        WHEN Gender = '2' then '2'
#        WHEN Gender = '9' then '3'
#        ELSE '4'
#        END AS PrimaryMeasureNumber,
# CASE
#        WHEN Gender = '1' then 'Male'
#        WHEN Gender = '2' then 'Female'
#        WHEN Gender = '9' then 'Not stated'
#        ELSE 'Unknown'
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY 
# CASE
#        WHEN Gender = '1' then '1'
#        WHEN Gender = '2' then '2'
#        WHEN Gender = '9' then '3'
#        ELSE '4'
#        END,
# CASE
#        WHEN Gender = '1' then 'Male'
#        WHEN Gender = '2' then 'Female'
#        WHEN Gender = '9' then 'Not stated'
#        ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 4 Ethnicity Splits
# %sql
# ----------------- TABLE 4 - ETHNICITY SPLIT

# --ETHNICITY
# --INSERT INTO menh_analysis.LDA_counts

# CREATE OR REPLACE TEMP VIEW Table4_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 4 AS TableNumber,
# 'Ethnicity' AS PrimaryMeasure,
# CASE
#        WHEN NHSDEthnicity IN ('A','B','C') then '1'
#        WHEN NHSDEthnicity IN ('D','E','F','G') then '2'
#        WHEN NHSDEthnicity IN ('H','J','K','L') then '3'
#        WHEN NHSDEthnicity IN ('M','N','P') then '4'
#        WHEN NHSDEthnicity IN ('R','S') then '5'
#        WHEN NHSDEthnicity IN ('Z') then '6'
#        ELSE '7'
#        END AS PrimaryMeasureNumber,
# CASE
#        WHEN NHSDEthnicity IN ('A','B','C') then 'White'
#        WHEN NHSDEthnicity IN ('D','E','F','G') then 'Mixed'
#        WHEN NHSDEthnicity IN ('H','J','K','L') then 'Asian'
#        WHEN NHSDEthnicity IN ('M','N','P') then 'Black'
#        WHEN NHSDEthnicity IN ('R','S') then 'Other'
#        WHEN NHSDEthnicity IN ('Z') then 'Not Stated'
#        ELSE 'Unknown'
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY 
# CASE
#        WHEN NHSDEthnicity IN ('A','B','C') then '1'
#        WHEN NHSDEthnicity IN ('D','E','F','G') then '2'
#        WHEN NHSDEthnicity IN ('H','J','K','L') then '3'
#        WHEN NHSDEthnicity IN ('M','N','P') then '4'
#        WHEN NHSDEthnicity IN ('R','S') then '5'
#        WHEN NHSDEthnicity IN ('Z') then '6'
#        ELSE '7'
#        END,
# CASE
#        WHEN NHSDEthnicity IN ('A','B','C') then 'White'
#        WHEN NHSDEthnicity IN ('D','E','F','G') then 'Mixed'
#        WHEN NHSDEthnicity IN ('H','J','K','L') then 'Asian'
#        WHEN NHSDEthnicity IN ('M','N','P') then 'Black'
#        WHEN NHSDEthnicity IN ('R','S') then 'Other'
#        WHEN NHSDEthnicity IN ('Z') then 'Not Stated'
#        ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 5 Distance from Home Split
# %sql
# --------- TABLE 5 - DISTANCE FROM HOME SPLIT

# -- DISTANCE
# --INSERT INTO menh_analysis.LDA_counts

# CREATE OR REPLACE TEMP VIEW Table5_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 5 AS TableNumber,
# 'Distance from Home' AS PrimaryMeasure,
# CASE 
#        WHEN WardLocDistanceHome between 0 and 10 then '1'
#        WHEN WardLocDistanceHome between 11 and 20 then '2'
#        WHEN WardLocDistanceHome between 21 and 50 then '3'
#        WHEN WardLocDistanceHome between 51 and 100 then '4'
#        WHEN WardLocDistanceHome > 100 then '5'  
#        ELSE '6'
#        END AS PrimaryMeasureNumber,
# CASE 
#        WHEN WardLocDistanceHome between 0 and 10 then 'Up to 10km'
#        WHEN WardLocDistanceHome between 11 and 20 then '11-20km'
#        WHEN WardLocDistanceHome between 21 and 50 then '21-50km'
#        WHEN WardLocDistanceHome between 51 and 100 then '51-100km'
#        WHEN WardLocDistanceHome > 100 then 'Over 100km'       
#        ELSE 'Unknown  '
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY 
# CASE 
#        WHEN WardLocDistanceHome between 0 and 10 then '1'
#        WHEN WardLocDistanceHome between 11 and 20 then '2'
#        WHEN WardLocDistanceHome between 21 and 50 then '3'
#        WHEN WardLocDistanceHome between 51 and 100 then '4'
#        WHEN WardLocDistanceHome > 100 then '5'  
#        ELSE '6'
#        END,
# CASE 
#        WHEN WardLocDistanceHome between 0 and 10 then 'Up to 10km'
#        WHEN WardLocDistanceHome between 11 and 20 then '11-20km'
#        WHEN WardLocDistanceHome between 21 and 50 then '21-50km'
#        WHEN WardLocDistanceHome between 51 and 100 then '51-100km'
#        WHEN WardLocDistanceHome > 100 then 'Over 100km'       
#        ELSE 'Unknown  '
#        END  

# COMMAND ----------

# DBTITLE 1,Table 6 Ward Security Split 
# %sql
# ----------- TABLE 6 - WARD SECURITY SPLIT

# -- WARD SECURITY
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table6_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 6 AS TableNumber,
# 'Ward security' AS PrimaryMeasure,
# CASE 
#        WHEN WardSecLevel = '0' then '1'
#        WHEN WardSecLevel = '1' then '2'
#        WHEN WardSecLevel = '2' then '3'
#        WHEN WardSecLevel = '3' then '4'
#        ELSE '5'
#        END AS PrimaryMeasureNumber,
# CASE 
#        WHEN WardSecLevel = '0' then 'General'
#        WHEN WardSecLevel = '1' then 'Low Secure'
#        WHEN WardSecLevel = '2' then 'Medium Secure'
#        WHEN WardSecLevel = '3' then 'High Secure'
#        ELSE 'Unknown '
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY 
# CASE 
#        WHEN WardSecLevel = '0' then '1'
#        WHEN WardSecLevel = '1' then '2'
#        WHEN WardSecLevel = '2' then '3'
#        WHEN WardSecLevel = '3' then '4'
#        ELSE '5'
#        END,
# CASE 
#        WHEN WardSecLevel = '0' then 'General'
#        WHEN WardSecLevel = '1' then 'Low Secure'
#        WHEN WardSecLevel = '2' then 'Medium Secure'
#        WHEN WardSecLevel = '3' then 'High Secure'
#        ELSE 'Unknown '
#        END

# COMMAND ----------

# DBTITLE 1,Table 7 Planned Discharge Split 
# %sql

# ---------- TABLE 7 - PLANNED DISCHARGE SPLIT

# --Planned Discharge
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table7_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 7 AS TableNumber,
# 'Planned Discharge Date Present' as PrimaryMeasure,
# CASE
#        WHEN PlannedDischDateHospProvSpell is not null then '1'
#        WHEN PlannedDischDateHospProvSpell is null then '2' 
#        END as PrimaryMeasureNumber,
# CASE
#        WHEN PlannedDischDateHospProvSpell is not null then 'Present'
#        WHEN PlannedDischDateHospProvSpell is null then 'Not present' END as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY
# CASE
#        WHEN PlannedDischDateHospProvSpell is not null then '1'
#        WHEN PlannedDischDateHospProvSpell is null then '2' END,
# CASE
#        WHEN PlannedDischDateHospProvSpell is not null then 'Present'
#        WHEN PlannedDischDateHospProvSpell is null then 'Not present' END

# COMMAND ----------

# DBTITLE 1,Table 8 Planned Discharge Date Split 
# %sql
# ------------------ TABLE 8 -PLANNED DISCHARGE DATE SPLIT

# --Planned Discharge
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table8_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 8 AS TableNumber,
# 'Time to Planned Discharge' as PrimaryMeasure,
# CASE
#        --WHEN PlannedDischDateHospProvSpell is null then '1'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then '2'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '3'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '4'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '5'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '6'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '7'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then '8'  
#        END as PrimaryMeasureNumber,
# CASE
#        --WHEN PlannedDischDateHospProvSpell is null then 'No planned discharge'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then 'Planned discharge overdue'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '0 to 3 months'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '3 to 6 months'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '6 to 12 months'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '1 to 2 years'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '2 to 5 years'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then 'Over 5 years'  
#        END as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY
# CASE
#        --WHEN PlannedDischDateHospProvSpell is null then '1'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then '2'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '3'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '4'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '5'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '6'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '7'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then '8'  
#        END,
# CASE
#        --WHEN PlannedDischDateHospProvSpell is null then 'No planned discharge'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then 'Planned discharge overdue'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '0 to 3 months'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '3 to 6 months'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '6 to 12 months'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '1 to 2 years'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '2 to 5 years'
#        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then 'Over 5 years'  
#        END


# COMMAND ----------

# DBTITLE 1,Table 9 Respite Care Split 
# %sql
# ------------------- TABLE 9 - RESPITE CARE SPLIT

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table9_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 9 AS TableNumber,
# 'Respite care' as PrimaryMeasure,
# CASE
#        WHEN RespiteCare = 'Y' then '1'
#        ELSE '2'  
#        END as PrimaryMeasureNumber,
# CASE
#        WHEN RespiteCare = 'Y' then 'Admitted for respite care'
#        ELSE 'Not admitted for respite care'  
#        END as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY
# CASE
#        WHEN RespiteCare = 'Y' then '1'
#        ELSE '2'  
#        END,
# CASE
#        WHEN RespiteCare = 'Y' then 'Admitted for respite care'
#        ELSE 'Not admitted for respite care'  
#        END

# COMMAND ----------

# DBTITLE 1,Table 10 Length of Stay Split 
# %sql
# -------------------- TABLE 10 - LENGTH OF STAY SPLIT. 

# --LOS
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table10_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 10 AS TableNumber,
# 'Length of stay' AS PrimaryMeasure,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '1'
#        WHEN H.HSP_LOS between 4 and 7 then '2'
#        WHEN H.HSP_LOS between 8 and 14 then '3'
#        WHEN H.HSP_LOS between 15 and 28 then '4'
#        WHEN H.HSP_LOS between 29 and 91 then '5'
#        WHEN H.HSP_LOS between 92 and 182 then '6'
#        WHEN H.HSP_LOS between 183 and 365 then '7'
#        WHEN H.HSP_LOS between 366 and 730 then '8'
#        WHEN H.HSP_LOS between 731 and 1826 then '9'
#        WHEN H.HSP_LOS between 1827 and 3652 then '10'
#        WHEN H.HSP_LOS > 3652 then '11'
#        ELSE '12'
#        END AS PrimaryMeasureNumber,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
#        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
#        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
#        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
#        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
#        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
#        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
#        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
#        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
#        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
#        WHEN H.HSP_LOS > 3652 then '10+ years'
#        ELSE 'Unknown'
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# '0' as  WardStaysInCarePreviousMonth,
# '0' as  WardStaysAdmissionsInMonth,
# '0' as  WardStaysDischargedInMonth,
# '0' as  WardStaysAdmittedAndDischargedInMonth,
# '0' as  WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
# global_temp.HSP_Spells H
# GROUP BY 
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '1'
#        WHEN H.HSP_LOS between 4 and 7 then '2'
#        WHEN H.HSP_LOS between 8 and 14 then '3'
#        WHEN H.HSP_LOS between 15 and 28 then '4'
#        WHEN H.HSP_LOS between 29 and 91 then '5'
#        WHEN H.HSP_LOS between 92 and 182 then '6'
#        WHEN H.HSP_LOS between 183 and 365 then '7'
#        WHEN H.HSP_LOS between 366 and 730 then '8'
#        WHEN H.HSP_LOS between 731 and 1826 then '9'
#        WHEN H.HSP_LOS between 1827 and 3652 then '10'
#        WHEN H.HSP_LOS > 3652 then '11'
#        ELSE '12'
#        END,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
#        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
#        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
#        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
#        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
#        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
#        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
#        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
#        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
#        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
#        WHEN H.HSP_LOS > 3652 then '10+ years'
#        ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 11 Discharge Destination Grouped Split 
# %sql
# ------------------- TABLE 11 - DISCHARGE DESTINATION GROUPED SPLIT

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table11_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 11 AS TableNumber,
# 'Discharge Destination Grouped' as PrimaryMeasure,
# CASE
#        WHEN DischDestCodeHospProvSpell in ('19','29','54','65','66','85') then '1'
#        WHEN DischDestCodeHospProvSpell in ('30','48','49','50','51','52','53','84','87') then '2'
#        WHEN DischDestCodeHospProvSpell in ('37','38') then '3'
#        WHEN DischDestCodeHospProvSpell in ('88') then '4'
#        WHEN DischDestCodeHospProvSpell in ('98') then '5'
#        WHEN DischDestCodeHospProvSpell in ('79') then '6'
#        ELSE '7'
#        END as PrimaryMeasureNumber,
# CASE
#        WHEN DischDestCodeHospProvSpell in ('19','29','54','65','66','85') then 'Community'
#        WHEN DischDestCodeHospProvSpell in ('30','48','49','50','51','52','53','84','87') then 'Hospital'
#        WHEN DischDestCodeHospProvSpell in ('37','38') then 'Penal Establishment / Court'
#        WHEN DischDestCodeHospProvSpell in ('88') then 'Non-NHS Hospice'
#        WHEN DischDestCodeHospProvSpell in ('98') then 'Not Applicable'
#        WHEN DischDestCodeHospProvSpell in ('79') then 'Patient died'
#        ELSE 'Not Known'
#        END as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY
# CASE
#        WHEN DischDestCodeHospProvSpell in ('19','29','54','65','66','85') then '1'
#        WHEN DischDestCodeHospProvSpell in ('30','48','49','50','51','52','53','84','87') then '2'
#        WHEN DischDestCodeHospProvSpell in ('37','38') then '3'
#        WHEN DischDestCodeHospProvSpell in ('88') then '4'
#        WHEN DischDestCodeHospProvSpell in ('98') then '5'
#        WHEN DischDestCodeHospProvSpell in ('79') then '6'
#        ELSE '7'
#        END,
# CASE
#        WHEN DischDestCodeHospProvSpell in ('19','29','54','65','66','85') then 'Community'
#        WHEN DischDestCodeHospProvSpell in ('30','48','49','50','51','52','53','84','87') then 'Hospital'
#        WHEN DischDestCodeHospProvSpell in ('37','38') then 'Penal Establishment / Court'
#        WHEN DischDestCodeHospProvSpell in ('88') then 'Non-NHS Hospice'
#        WHEN DischDestCodeHospProvSpell in ('98') then 'Not Applicable'
#        WHEN DischDestCodeHospProvSpell in ('79') then 'Patient died'
#        ELSE 'Not Known'
#        END

# COMMAND ----------

# DBTITLE 1,Table 12 Ward Type Split 
# %sql
# ------------------- TABLE 12 - WARD TYPE SPLIT

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table12_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 12 AS TableNumber,
# 'Ward Type' as PrimaryMeasure,
# CASE
#        WHEN WardType = '01' then '1'
#        WHEN WardType = '02' then '2'
#        WHEN WardType = '03' then '3'
#        WHEN WardType = '04' then '4'
#        WHEN WardType = '05' then '5'
#        WHEN WardType = '06' then '6'
#        else '7'
#        END as PrimaryMeasureNumber,
# CASE
#        WHEN WardType = '01' then 'Child and adolescent mental health ward'
#        WHEN WardType = '02' then 'Paediatric ward'
#        WHEN WardType = '03' then 'Adult mental health ward'
#        WHEN WardType = '04' then 'Non mental health ward'
#        WHEN WardType = '05' then 'Learning disabilities ward'
#        WHEN WardType = '06' then 'Older peoples mental health ward'
#        ELSE 'Unknown   '
#        END as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# GROUP BY
# CASE
#        WHEN WardType = '01' then '1'
#        WHEN WardType = '02' then '2'
#        WHEN WardType = '03' then '3'
#        WHEN WardType = '04' then '4'
#        WHEN WardType = '05' then '5'
#        WHEN WardType = '06' then '6'
#        else '7'
#        END,
# CASE
#        WHEN WardType = '01' then 'Child and adolescent mental health ward'
#        WHEN WardType = '02' then 'Paediatric ward'
#        WHEN WardType = '03' then 'Adult mental health ward'
#        WHEN WardType = '04' then 'Non mental health ward'
#        WHEN WardType = '05' then 'Learning disabilities ward'
#        WHEN WardType = '06' then 'Older peoples mental health ward'
#        ELSE 'Unknown   '
#        END

# COMMAND ----------

# DBTITLE 1,Table 13 MHA 
# %sql
# ------------ TABLE 13 - MHA

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table13_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 13 AS TableNumber,
# 'Mental Health Act' as PrimaryMeasure,
# CASE
# WHEN MHA_Group = 'Informal' then '1'
# WHEN MHA_Group = 'Part 2' then '2'
# WHEN MHA_Group = 'Part 3 no restrictions' then '3'
# WHEN MHA_Group = 'Part 3 with restrictions' then '4'
# WHEN MHA_Group = 'Other' then '5'
# ELSE '1'
# end as PrimaryMeasureNumber,
# CASE
# WHEN MHA_Group = 'Informal' then 'Informal'
# WHEN MHA_Group = 'Part 2' then 'Part 2'
# WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
# WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
# WHEN MHA_Group = 'Other' then 'Other sections'
# ELSE 'Informal'
# end as PrimaryMeasureSplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# '0' as WardStaysInCarePreviousMonth,
# '0' as WardStaysAdmissionsInMonth,
# '0' as WardStaysDischargedInMonth,
# '0' as WardStaysAdmittedAndDischargedInMonth,
# '0' as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM global_temp.MHA
# GROUP BY
# CASE
# WHEN MHA_Group = 'Informal' then '1'
# WHEN MHA_Group = 'Part 2' then '2'
# WHEN MHA_Group = 'Part 3 no restrictions' then '3'
# WHEN MHA_Group = 'Part 3 with restrictions' then '4'
# WHEN MHA_Group = 'Other' then '5'
# ELSE '1'
# end,
# CASE
# WHEN MHA_Group = 'Informal' then 'Informal'
# WHEN MHA_Group = 'Part 2' then 'Part 2'
# WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
# WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
# WHEN MHA_Group = 'Other' then 'Other sections'
# ELSE 'Informal'
# end

# COMMAND ----------

# DBTITLE 1,Table 14 Delayed Discharges 
# %sql

# ------------ TABLE 14 - Delayed Discharges----------------------------

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table14_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' AS OrgCode,
# 'National' as OrgName,
# 14 AS TableNumber,
# 'Delayed Discharges' as PrimaryMeasure,
# CASE
#        when DelayDischReason = 'A2' then '1'
#        when DelayDischReason = 'B1' then '2'
#        when DelayDischReason = 'C1' then '3'
#        when DelayDischReason = 'D1' then '4'
#        when DelayDischReason = 'D2' then '5'
#        when DelayDischReason = 'E1' then '6'
#        when DelayDischReason = 'F2' then '7'
#        when DelayDischReason = 'G2' then '8'
#        when DelayDischReason = 'G3' then '9'
#        when DelayDischReason = 'G4' then '10'
#        when DelayDischReason = 'G5' then '11'
#        when DelayDischReason = 'G6' then '12'
#        when DelayDischReason = 'G7' then '13'
#        when DelayDischReason = 'G8' then '14'
#        when DelayDischReason = 'G9' then '15'
#        when DelayDischReason = 'G10' then '16'
#        when DelayDischReason = 'G11' then '17'
#        when DelayDischReason = 'G12' then '18'
#        when DelayDischReason = 'H1' then '19'
#        when DelayDischReason = 'I2' then '20'
#        when DelayDischReason = 'I3' then '21'
#        when DelayDischReason = 'J2' then '22'
#        when DelayDischReason = 'K2' then '23'
#        when DelayDischReason = 'L1' then '24'
#        when DelayDischReason = 'M1' then '25'
#        when DelayDischReason = 'N1' then '26'
# 	   when DelayDischReason is null then '27'
#        else '27'
#        End as PrimaryMeasureNumber,
# CASE
#  WHEN DelayDischReason = 'A2' then 'Awaiting care coordinator allocation'                        
#     WHEN DelayDischReason = 'B1' then 'Awaiting public funding'                   
#     WHEN DelayDischReason = 'C1' then 'Awaiting further non-acute (including community and mental health) NHS care'                        
#     WHEN DelayDischReason = 'D1' then 'Awaiting care home Without nursing placement or availability'                       
#     WHEN DelayDischReason = 'D2' then 'Awaiting care home With nursing placement or availability'                       
#     WHEN DelayDischReason = 'E1' then 'Awaiting care package in own home'                     
#     WHEN DelayDischReason = 'F2' then 'Awaiting community equipment, telecare and/or adaptations'                        
#     WHEN DelayDischReason = 'G2' then 'Patient or Family choice - (reason not stated by patient or family)'                       
#     WHEN DelayDischReason = 'G3' then 'Patient or Family choice - non-acute (including community and mental health) NHS care'                      
#     WHEN DelayDischReason = 'G4' then 'Patient or Family choice - care home without nursing placement'                    
#     WHEN DelayDischReason = 'G5' then 'Patient or Family choice - care home with nursing placement'                    
#     WHEN DelayDischReason = 'G6' then 'Patient or Family choice - care package in own home'                        
#     WHEN DelayDischReason = 'G7' then 'Patient or Family choice - community equipment, telecare and/or adaptations'                    
#     WHEN DelayDischReason = 'G8' then 'Patient or Family Choice - general needs housing/private landlord acceptance'                        
#     WHEN DelayDischReason = 'G9' then 'Patient or Family choice - supported accommodation'                        
#     WHEN DelayDischReason = 'G10' then 'Patient or Family choice - emergency accommodation from the local luthority under the housing act'                   
#     WHEN DelayDischReason = 'G11' then 'Patient or Family choice - child or young person awaiting social care or family placement'                   
#     WHEN DelayDischReason = 'G12' then 'Patient or Family choice - ministry of justice agreement/permission of proposed placement'                       
#     WHEN DelayDischReason = 'H1' then 'Disputes'                      
#     WHEN DelayDischReason = 'I2' then 'Housing - awaiting availability of general needs housing/private landlord accommodation acceptance'                      
#     WHEN DelayDischReason = 'I3' then 'Housing - single homeless patients or asylum seekers NOT covered by care act'                        
#     WHEN DelayDischReason = 'J2' then 'Housing - awaiting supported accommodation'                        
#     WHEN DelayDischReason = 'K2' then 'Housing - awaiting emergency accommodation from the local authority under the housing act'                      
#     WHEN DelayDischReason = 'L1' then 'Child or young person awaiting social care or family placement'                    
#     WHEN DelayDischReason = 'M1' then 'Awaiting ministry of justice agreement/permission of proposed placement'                       
#     WHEN DelayDischReason = 'N1' then 'Awaiting outcome of legal requirements (mental capacity/mental health legislation)'
# 	WHEN DelayDischReason is null then 'Unknown'    
#        else 'Unknown'
#        End as PrimaryMeasureSplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# '0' as HospitalSpellsInCarePreviousMonth,
# '0' as HospitalSpellsAdmissionsInMonth,
# '0' as HospitalSpellsDischargedInMonth,
# '0' as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# '0' as  WardStaysInCarePreviousMonth,
# '0' as  WardStaysAdmissionsInMonth,
# '0' as  WardStaysDischargedInMonth,
# '0' as  WardStaysAdmittedAndDischargedInMonth,
# '0' as  WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1
# where StartDateDelayDisch is not null
# Group By
# CASE
#        when DelayDischReason = 'A2' then '1'
#        when DelayDischReason = 'B1' then '2'
#        when DelayDischReason = 'C1' then '3'
#        when DelayDischReason = 'D1' then '4'
#        when DelayDischReason = 'D2' then '5'
#        when DelayDischReason = 'E1' then '6'
#        when DelayDischReason = 'F2' then '7'
#        when DelayDischReason = 'G2' then '8'
#        when DelayDischReason = 'G3' then '9'
#        when DelayDischReason = 'G4' then '10'
#        when DelayDischReason = 'G5' then '11'
#        when DelayDischReason = 'G6' then '12'
#        when DelayDischReason = 'G7' then '13'
#        when DelayDischReason = 'G8' then '14'
#        when DelayDischReason = 'G9' then '15'
#        when DelayDischReason = 'G10' then '16'
#        when DelayDischReason = 'G11' then '17'
#        when DelayDischReason = 'G12' then '18'
#        when DelayDischReason = 'H1' then '19'
#        when DelayDischReason = 'I2' then '20'
#        when DelayDischReason = 'I3' then '21'
#        when DelayDischReason = 'J2' then '22'
#        when DelayDischReason = 'K2' then '23'
#        when DelayDischReason = 'L1' then '24'
#        when DelayDischReason = 'M1' then '25'
#        when DelayDischReason = 'N1' then '26'
# 	   when DelayDischReason is null then '27'
#        else '27'
#        End,
# CASE
#     WHEN DelayDischReason = 'A2' then 'Awaiting care coordinator allocation'                        
#     WHEN DelayDischReason = 'B1' then 'Awaiting public funding'                   
#     WHEN DelayDischReason = 'C1' then 'Awaiting further non-acute (including community and mental health) NHS care'                        
#     WHEN DelayDischReason = 'D1' then 'Awaiting care home Without nursing placement or availability'                       
#     WHEN DelayDischReason = 'D2' then 'Awaiting care home With nursing placement or availability'                       
#     WHEN DelayDischReason = 'E1' then 'Awaiting care package in own home'                     
#     WHEN DelayDischReason = 'F2' then 'Awaiting community equipment, telecare and/or adaptations'                        
#     WHEN DelayDischReason = 'G2' then 'Patient or Family choice - (reason not stated by patient or family)'                       
#     WHEN DelayDischReason = 'G3' then 'Patient or Family choice - non-acute (including community and mental health) NHS care'                      
#     WHEN DelayDischReason = 'G4' then 'Patient or Family choice - care home without nursing placement'                    
#     WHEN DelayDischReason = 'G5' then 'Patient or Family choice - care home with nursing placement'                    
#     WHEN DelayDischReason = 'G6' then 'Patient or Family choice - care package in own home'                        
#     WHEN DelayDischReason = 'G7' then 'Patient or Family choice - community equipment, telecare and/or adaptations'                    
#     WHEN DelayDischReason = 'G8' then 'Patient or Family Choice - general needs housing/private landlord acceptance'                        
#     WHEN DelayDischReason = 'G9' then 'Patient or Family choice - supported accommodation'                        
#     WHEN DelayDischReason = 'G10' then 'Patient or Family choice - emergency accommodation from the local luthority under the housing act'                   
#     WHEN DelayDischReason = 'G11' then 'Patient or Family choice - child or young person awaiting social care or family placement'                   
#     WHEN DelayDischReason = 'G12' then 'Patient or Family choice - ministry of justice agreement/permission of proposed placement'                       
#     WHEN DelayDischReason = 'H1' then 'Disputes'                      
#     WHEN DelayDischReason = 'I2' then 'Housing - awaiting availability of general needs housing/private landlord accommodation acceptance'                      
#     WHEN DelayDischReason = 'I3' then 'Housing - single homeless patients or asylum seekers NOT covered by care act'                        
#     WHEN DelayDischReason = 'J2' then 'Housing - awaiting supported accommodation'                        
#     WHEN DelayDischReason = 'K2' then 'Housing - awaiting emergency accommodation from the local authority under the housing act'                      
#     WHEN DelayDischReason = 'L1' then 'Child or young person awaiting social care or family placement'                    
#     WHEN DelayDischReason = 'M1' then 'Awaiting ministry of justice agreement/permission of proposed placement'                       
#     WHEN DelayDischReason = 'N1' then 'Awaiting outcome of legal requirements (mental capacity/mental health legislation)'
# 	WHEN DelayDischReason is null then 'Unknown'     
#       else 'Unknown' end


# COMMAND ----------

# DBTITLE 1,Table 15 Restraints 
# %sql
# ---TABLE 15 -------------------------------------restraints------------------------------------------------------

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table15_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 15 AS TableNumber,
# 'Restraints' AS PrimaryMeasure,
# RestrictiveIntType AS PrimaryMeasureNumber,
# RestrictiveIntTypeDesc AS PrimaryMeasureSplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
# COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
# '0' as HospitalSpellsInCarePreviousMonth,
# '0' as HospitalSpellsAdmissionsInMonth,
# '0' as HospitalSpellsDischargedInMonth,
# '0' as HospitalSpellsAdmittedAndDischargedInMonth,
# '0' as HospitalSpellsOpenAtEndOfMonth,
# '0' as WardStaysInCarePreviousMonth,
# '0' as WardStaysAdmissionsInMonth,
# '0' as WardStaysDischargedInMonth,
# '0' as WardStaysAdmittedAndDischargedInMonth,
# '0' as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE

# FROM 
#  $db_output.LDA_Data_1


# GROUP BY 
# RestrictiveIntType,
# RestrictiveIntTypeDesc



# COMMAND ----------

# DBTITLE 1,Table 50 LOS by MHA 
# %sql
# ---- TABLE 50 - LOS by MHA

# --LOS
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table50_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'National' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 50 AS TableNumber,
# 'Mental Health Act' AS PrimaryMeasure,
# CASE
# WHEN MHA_Group = 'Informal' then '1'
# WHEN MHA_Group = 'Part 2' then '2'
# WHEN MHA_Group = 'Part 3 no restrictions' then '3'
# WHEN MHA_Group = 'Part 3 with restrictions' then '4'
# WHEN MHA_Group = 'Other' then '5'
# ELSE '1'
# end as PrimaryMeasureNumber,
# CASE
# WHEN MHA_Group = 'Informal' then 'Informal'
# WHEN MHA_Group = 'Part 2' then 'Part 2'
# WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
# WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
# WHEN MHA_Group = 'Other' then 'Other sections'
# ELSE 'Informal'
# end AS PrimarySplit,
# 'Length of stay' as SecondaryMeasure,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '1'
#        WHEN H.HSP_LOS between 4 and 7 then '2'
#        WHEN H.HSP_LOS between 8 and 14 then '3'
#        WHEN H.HSP_LOS between 15 and 28 then '4'
#        WHEN H.HSP_LOS between 29 and 91 then '5'
#        WHEN H.HSP_LOS between 92 and 182 then '6'
#        WHEN H.HSP_LOS between 183 and 365 then '7'
#        WHEN H.HSP_LOS between 366 and 730 then '8'
#        WHEN H.HSP_LOS between 731 and 1826 then '9'
#        WHEN H.HSP_LOS between 1827 and 3652 then '10'
#        WHEN H.HSP_LOS > 3652 then '11'
#        ELSE '12'
#        END AS SecondaryMeasureNumber,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
#        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
#        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
#        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
#        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
#        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
#        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
#        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
#        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
#        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
#        WHEN H.HSP_LOS > 3652 then '10+ years'
#        ELSE 'Unknown'
#        END as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# '0' as HospitalSpellsInCarePreviousMonth,
# '0' as HospitalSpellsAdmissionsInMonth,
# '0' as HospitalSpellsDischargedInMonth,
# '0' as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# '0' as  WardStaysInCarePreviousMonth,
# '0' as  WardStaysAdmissionsInMonth,
# '0' as  WardStaysDischargedInMonth,
# '0' as  WardStaysAdmittedAndDischargedInMonth,
# '0' as  WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
# global_temp.MHA H
# GROUP BY 
# CASE
# WHEN MHA_Group = 'Informal' then '1'
# WHEN MHA_Group = 'Part 2' then '2'
# WHEN MHA_Group = 'Part 3 no restrictions' then '3'
# WHEN MHA_Group = 'Part 3 with restrictions' then '4'
# WHEN MHA_Group = 'Other' then '5'
# ELSE '1'
# end,
# CASE
# WHEN MHA_Group = 'Informal' then 'Informal'
# WHEN MHA_Group = 'Part 2' then 'Part 2'
# WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
# WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
# WHEN MHA_Group = 'Other' then 'Other sections'
# ELSE 'Informal'
# end,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '1'
#        WHEN H.HSP_LOS between 4 and 7 then '2'
#        WHEN H.HSP_LOS between 8 and 14 then '3'
#        WHEN H.HSP_LOS between 15 and 28 then '4'
#        WHEN H.HSP_LOS between 29 and 91 then '5'
#        WHEN H.HSP_LOS between 92 and 182 then '6'
#        WHEN H.HSP_LOS between 183 and 365 then '7'
#        WHEN H.HSP_LOS between 366 and 730 then '8'
#        WHEN H.HSP_LOS between 731 and 1826 then '9'
#        WHEN H.HSP_LOS between 1827 and 3652 then '10'
#        WHEN H.HSP_LOS > 3652 then '11'
#        ELSE '12'
#        END,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
#        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
#        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
#        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
#        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
#        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
#        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
#        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
#        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
#        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
#        WHEN H.HSP_LOS > 3652 then '10+ years'
#        ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 51 Restraints by Age 
# %sql
# ---- TABLE 51 - Restraints by Age-----------------------------------------------------------

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table51_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Total data as submitted' AS Geography,
# 'National' as OrgCode,
# 'National' as OrgName,
# 51 AS TableNumber,
# 'Restraints' AS PrimaryMeasure,
# RestrictiveIntType AS PrimaryMeasureNumber,
# RestrictiveIntTypeDesc AS PrimaryMeasureSplit,
# 'Age' as SecondaryMeasure,
# CASE
#        WHEN R.AgeRepPeriodEnd between 0 and 17 then '1'
#           WHEN R.AgeRepPeriodEnd between 18 and 24 then '2'
#           WHEN R.AgeRepPeriodEnd between 25 and 34 then '3'
#           WHEN R.AgeRepPeriodEnd between 35 and 44 then '4'
#           WHEN R.AgeRepPeriodEnd between 45 and 54 then '5'
#           WHEN R.AgeRepPeriodEnd between 55 and 64 then '6'
#           WHEN R.AgeRepPeriodEnd > 64 then '7'
#        ELSE '8'
#       END AS SecondaryMeasureNumber,
# CASE
#        WHEN R.AgeRepPeriodEnd between 0 and 17 then 'Under 18'
#        WHEN R.AgeRepPeriodEnd between 18 and 24 then '18-24'
#        WHEN R.AgeRepPeriodEnd between 25 and 34 then '25-34'
#        WHEN R.AgeRepPeriodEnd between 35 and 44 then '35-44'
#        WHEN R.AgeRepPeriodEnd between 45 and 54 then '45-54'
#        WHEN R.AgeRepPeriodEnd between 55 and 64 then '55-64'
#        WHEN R.AgeRepPeriodEnd > 64 then '65 and Over'
#       ELSE 'Unknown'
#      END AS SecondarySplit,
# COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
# COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
# '0' as HospitalSpellsInCarePreviousMonth,
# '0' as HospitalSpellsAdmissionsInMonth,
# '0' as HospitalSpellsDischargedInMonth,
# '0' as HospitalSpellsAdmittedAndDischargedInMonth,
# '0' as HospitalSpellsOpenAtEndOfMonth,
# '0' as WardStaysInCarePreviousMonth,
# '0' as WardStaysAdmissionsInMonth,
# '0' as WardStaysDischargedInMonth,
# '0' as WardStaysAdmittedAndDischargedInMonth,
# '0' as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1 R
# GROUP BY 
# RestrictiveIntType,
# RestrictiveIntTypeDesc,
# CASE
#        WHEN R.AgeRepPeriodEnd between 0 and 17 then '1'
#           WHEN R.AgeRepPeriodEnd between 18 and 24 then '2'
#           WHEN R.AgeRepPeriodEnd between 25 and 34 then '3'
#           WHEN R.AgeRepPeriodEnd between 35 and 44 then '4'
#           WHEN R.AgeRepPeriodEnd between 45 and 54 then '5'
#           WHEN R.AgeRepPeriodEnd between 55 and 64 then '6'
#           WHEN R.AgeRepPeriodEnd > 64 then '7'
#        ELSE '8'
#       END,
# CASE
#        WHEN R.AgeRepPeriodEnd between 0 and 17 then 'Under 18'
#        WHEN R.AgeRepPeriodEnd between 18 and 24 then '18-24'
#        WHEN R.AgeRepPeriodEnd between 25 and 34 then '25-34'
#        WHEN R.AgeRepPeriodEnd between 35 and 44 then '35-44'
#        WHEN R.AgeRepPeriodEnd between 45 and 54 then '45-54'
#        WHEN R.AgeRepPeriodEnd between 55 and 64 then '55-64'
#        WHEN R.AgeRepPeriodEnd > 64 then '65 and Over'
#       ELSE 'Unknown'
#      END

# COMMAND ----------

# DBTITLE 1,Table 70 Provider Totals Split
# %sql
# ------------ TABLE 70 - PROVIDER TOTALS SPLIT

# -- This table is different in that the OrgCode and OrgName fields also have data in the them. The case statement here breaks the measures down by Provider.
# -- Provider cross tabs are done by including the OrgCode and OrgName in the groupings as well as the PrimaryMeasure.

# --PROVIDERS
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table70_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Provider' AS Geography,
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE l.CombinedProvider END AS OrgCode,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END as OrgName,
# 70 AS TableNumber,
# 'Total' as PrimaryMeasure,
# 1 as PrimaryMeasureNumber,
# 'Total' as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
# --COUNT(distinct CASE when RestrictiveID is not null then Person_ID + RestrictiveIntType else null end) as RestraintsCountOfPeople,
# COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# ORG_TYPE_CODE_PROV AS ORG_TYPE_CODE
# FROM 

#  $db_output.LDA_Data_1 L
# GROUP BY
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END,
# ORG_TYPE_CODE_PROV

# COMMAND ----------

# DBTITLE 1,Table 71 LOS by Provider 
# %sql
# ---- TABLE 71 LOS by provider
# --INSERT INTO menh_analysis.lda_counts

# CREATE OR REPLACE TEMP VIEW Table71_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Provider' AS Geography,
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END AS OrgCode,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END as OrgName,
# 71 AS TableNumber,
# 'Length of stay' AS PrimaryMeasure,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '1'
#        WHEN H.HSP_LOS between 4 and 7 then '2'
#        WHEN H.HSP_LOS between 8 and 14 then '3'
#        WHEN H.HSP_LOS between 15 and 28 then '4'
#        WHEN H.HSP_LOS between 29 and 91 then '5'
#        WHEN H.HSP_LOS between 92 and 182 then '6'
#        WHEN H.HSP_LOS between 183 and 365 then '7'
#        WHEN H.HSP_LOS between 366 and 730 then '8'
#        WHEN H.HSP_LOS between 731 and 1826 then '9'
#        WHEN H.HSP_LOS between 1827 and 3652 then '10'
#        WHEN H.HSP_LOS > 3652 then '11'
#        --ELSE '12'
#        END AS PrimaryMeasureNumber,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
#        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
#        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
#        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
#        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
#        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
#        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
#        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
#        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
#        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
#        WHEN H.HSP_LOS > 3652 then '10+ years'
#        --ELSE 'Unknown'
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell >= '$rp_startdate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell > '$rp_enddate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then L.UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when L.DischDateHospProvSpell is null or L.DischDateHospProvSpell > '$rp_enddate' then L.UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# '0' as WardStaysInCarePreviousMonth,
# '0' as WardStaysAdmissionsInMonth,
# '0' as WardStaysDischargedInMonth,
# '0' as WardStaysAdmittedAndDischargedInMonth,
# '0' as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM 
#  $db_output.LDA_Data_1 L
# left join global_temp.HSP_Spells H on L.UniqHospProvSpellNum = H.UniqHospProvSpellNum
# left join global_temp.ProvNoIPs P on P.OrgCode = L.REF_OrgCodeProv
# where P.OrgCode is not null
# GROUP BY 
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '1'
#        WHEN H.HSP_LOS between 4 and 7 then '2'
#        WHEN H.HSP_LOS between 8 and 14 then '3'
#        WHEN H.HSP_LOS between 15 and 28 then '4'
#        WHEN H.HSP_LOS between 29 and 91 then '5'
#        WHEN H.HSP_LOS between 92 and 182 then '6'
#        WHEN H.HSP_LOS between 183 and 365 then '7'
#        WHEN H.HSP_LOS between 366 and 730 then '8'
#        WHEN H.HSP_LOS between 731 and 1826 then '9'
#        WHEN H.HSP_LOS between 1827 and 3652 then '10'
#        WHEN H.HSP_LOS > 3652 then '11'
#        --ELSE '12'
#        END,
# CASE   
#        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
#        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
#        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
#        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
#        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
#        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
#        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
#        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
#        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
#        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
#        WHEN H.HSP_LOS > 3652 then '10+ years'
#        --ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 72 Ward Type by Provider 
# %sql
# -- TABLE 72 - WARD TYPE BY PROVIDER

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table72_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Provider' AS Geography,
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END AS OrgCode,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END as OrgName,
# 72 AS TableNumber,
# 'Ward Type' AS PrimaryMeasure,
# CASE
#        WHEN WardType = '01' then '1'
#        WHEN WardType = '02' then '2'
#        WHEN WardType = '03' then '3'
#        WHEN WardType = '04' then '4'
#        WHEN WardType = '05' then '5'
#        WHEN WardType = '06' then '6'
#        else '7'
#        END as PrimaryMeasureNumber,
# CASE
#        WHEN WardType = '01' then 'Child and adolescent mental health ward'
#        WHEN WardType = '02' then 'Paediatric ward'
#        WHEN WardType = '03' then 'Adult mental health ward'
#        WHEN WardType = '04' then 'Non mental health ward'
#        WHEN WardType = '05' then 'Learning disabilities ward'
#        WHEN WardType = '06' then 'Older peoples mental health ward'
#        ELSE 'Unknown'
#        END as PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell >= '$rp_startdate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell > '$rp_enddate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then L.UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when L.DischDateHospProvSpell is null or L.DischDateHospProvSpell > '$rp_enddate' then L.UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE

# FROM 
#  $db_output.LDA_Data_1 L
# left join global_temp.ProvNoIPs P on P.OrgCode = L.REF_OrgCodeProv
# where P.OrgCode is not null
# GROUP BY 
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END,
# CASE
#        WHEN WardType = '01' then '1'
#        WHEN WardType = '02' then '2'
#        WHEN WardType = '03' then '3'
#        WHEN WardType = '04' then '4'
#        WHEN WardType = '05' then '5'
#        WHEN WardType = '06' then '6'
#        else '7'
#        END,
# CASE
#        WHEN WardType = '01' then 'Child and adolescent mental health ward'
#        WHEN WardType = '02' then 'Paediatric ward'
#        WHEN WardType = '03' then 'Adult mental health ward'
#        WHEN WardType = '04' then 'Non mental health ward'
#        WHEN WardType = '05' then 'Learning disabilities ward'
#        WHEN WardType = '06' then 'Older peoples mental health ward'
#        ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 74  Restraints by Provider  
# %sql
#  -----------table 74 Restraints by Provider-----------------------------------------------------------------------------------------------------------------
# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table74_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Provider' AS Geography,
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END AS OrgCode,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END as OrgName,
# 74 AS TableNumber,
# 'Restraints' AS PrimaryMeasure,
# RestrictiveIntType AS PrimaryMeasureNumber,
# RestrictiveIntTypeDesc AS PrimaryMeasureSplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
# COUNT(DISTINCT CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
# '0' as HospitalSpellsInCarePreviousMonth,
# '0' as HospitalSpellsAdmissionsInMonth,
# '0' as HospitalSpellsDischargedInMonth,
# '0' as HospitalSpellsAdmittedAndDischargedInMonth,
# '0' as HospitalSpellsOpenAtEndOfMonth,
# '0' as WardStaysInCarePreviousMonth,
# '0' as WardStaysAdmissionsInMonth,
# '0' as WardStaysDischargedInMonth,
# '0' as WardStaysAdmittedAndDischargedInMonth,
# '0' as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' as ORG_TYPE_CODE

# FROM 
#  $db_output.LDA_Data_1 L
# left join global_temp.ProvNoIPs p on P.OrgCode = L.REF_OrgCodeProv
# where P.OrgCode is not null
# GROUP BY 
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END, 
# RestrictiveIntType,
# RestrictiveIntTypeDesc

# COMMAND ----------

# DBTITLE 1,Table 73 Ward Security Level by Provider
# %sql

# ---- TABLE 24 WARD SECURITY LEVEL BY PROVIDER

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table73_LDA AS
# SELECT
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Provider' AS Geography,
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END AS OrgCode,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END as OrgName,
# 73 AS TableNumber,
# 'Ward security' AS PrimaryMeasure,
# CASE 
#        WHEN WardSecLevel = '0' then '1'
#        WHEN WardSecLevel = '1' then '2'
#        WHEN WardSecLevel = '2' then '3'
#        WHEN WardSecLevel = '3' then '4'
#        ELSE '5'
#        END AS PrimaryMeasureNumber,
# CASE 
#        WHEN WardSecLevel = '0' then 'General'
#        WHEN WardSecLevel = '1' then 'Low Secure'
#        WHEN WardSecLevel = '2' then 'Medium Secure'
#        WHEN WardSecLevel = '3' then 'High Secure'
#        ELSE 'Unknown'
#        END AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell >= '$rp_startdate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell > '$rp_enddate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then L.UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then L.UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when L.DischDateHospProvSpell is null or L.DischDateHospProvSpell > '$rp_enddate' then L.UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# '0' as OpenReferralsPreviousMonth,
# '0' as ReferralsStartingInTheMonth,
# '0' as ReferralsEndingInTheMonth,
# '0' as ReferralsStartingAndEndingInTheMonth,
# '0' as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE

# FROM 
#  $db_output.LDA_Data_1 L
# left join global_temp.ProvNoIPs P on P.OrgCode = L.REF_OrgCodeProv
# where P.OrgCode is not null
# GROUP BY 
# CASE
#        WHEN NAME is null then 'Invalid'
#        ELSE L.CombinedProvider END,
# CASE 
#        WHEN NAME is null then 'Invalid'
#        ELSE NAME END,
# CASE 
#        WHEN WardSecLevel = '0' then '1'
#        WHEN WardSecLevel = '1' then '2'
#        WHEN WardSecLevel = '2' then '3'
#        WHEN WardSecLevel = '3' then '4'
#        ELSE '5'
#        END,
# CASE 
#        WHEN WardSecLevel = '0' then 'General'
#        WHEN WardSecLevel = '1' then 'Low Secure'
#        WHEN WardSecLevel = '2' then 'Medium Secure'
#        WHEN WardSecLevel = '3' then 'High Secure'
#        ELSE 'Unknown'
#        END

# COMMAND ----------

# DBTITLE 1,Table 80 Commissioner Groupings 
# %sql
# --INSERT INTO menh_analysis.LDA_COUNTS

# CREATE OR REPLACE TEMP VIEW Table80_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Commissioner Groupings' AS Geography,
# ORG_TYPE_CODE  as OrgCode,  -------  to do? ----
# ORG_TYPE_CODE as OrgName, ------- to do ----
# 80 AS TableNumber,
# 'Total' AS PrimaryMeasure,
# 1 AS PrimaryMeasureNumber,
# 'Total' AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM
#  $db_output.LDA_Data_1 R  

# group by 
# ORG_TYPE_CODE

# COMMAND ----------

# DBTITLE 1,Table 90 Commissioner
# %sql
# ----- TABLE 90 - Commissioner

# -- The two joins to ORG_DAILY temp table are done as some of the Commissioner codes end 00 but are valid as the 3 digit version. Therefore the join is done on the full org code and the SUBSTR( 0, 3) version.

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table90_LDA AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# 'Commissioner' AS Geography,
# OrgCode,
# OrgName,
# 90 AS TableNumber,
# 'Total' AS PrimaryMeasure,
# 1 AS PrimaryMeasureNumber,
# 'Total' AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# ORG_TYPE_CODE
# FROM
#  $db_output.LDA_Data_1 R 

# GROUP BY
# OrgCode,
# OrgName,
# ORG_TYPE_CODE


# COMMAND ----------

# DBTITLE 1,Table 100 TCP  Region
# %sql
# --TCP-----

# --INSERT INTO menh_analysis.LDA_Counts

# CREATE OR REPLACE TEMP VIEW Table100_LDA AS
# SELECT 
# 	  '$PreviousMonthEnd' as PreviousMonthEnd,
# 	  '$rp_startdate' as PeriodStart,
# 	  '$rp_enddate' as PeriodEnd,
#       'TCP Region' AS Geography
#       ,case when REGION_code IS NULL THEN 'Invalid' ELSE REGION_code end as REGION_code
#       ,case when Region_name IS NULL THEN 'Invalid' ELSE Region_name end as Region_name
#       ,100 AS TableNumber,
# 'Total' AS PrimaryMeasure,
# 1 AS PrimaryMeasureNumber,
# 'Total' AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM  $db_output.LDA_Data_1 O

#  --and OrgCode = '13Y'
# --AND NHS_ENGLAND_TCP_CODE <> 'Invalid'

# GROUP BY REGION_code
#       ,Region_name

# COMMAND ----------

 %md
 ## Merge into LDA_counts table - SQL versions

# COMMAND ----------

# DBTITLE 1,Table 101  TCP
# %sql
# --adds TCP mappings into counts table

# CREATE OR REPLACE TEMP VIEW Table101_LDA AS
# SELECT 
# 	  '$PreviousMonthEnd' as PreviousMonthEnd,
# 	  '$rp_startdate' as PeriodStart,
# 	  '$rp_enddate' as PeriodEnd,
#       'TCP' AS Geography
#       ,case when TCP_Code IS NULL then 'No TCP Mapping' else TCP_Code end as TCP_Code
#       ,case when TCP_NAME IS NULL then 'No TCP Mapping' else TCP_NAME end as TCP_NAME
#       ,101 AS TableNumber,
# 'Total' AS PrimaryMeasure,
# 1 AS PrimaryMeasureNumber,
# 'Total' AS PrimarySplit,
# '' as SecondaryMeasure,
# '' as SecondaryMeasureNumber,
# '' as SecondarySplit,
# '0' as RestraintsCountOfPeople,
# '0' as RestraintsCountOfRestraints,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellNum else null end) as HospitalSpellsInCarePreviousMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsAdmissionsInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellNum else null end) as HospitalSpellsDischargedInMonth,
# COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellNum else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellNum else null end) as HospitalSpellsOpenAtEndOfMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
# COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
# COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
# COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
# COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
# '' AS ORG_TYPE_CODE
# FROM $db_output.LDA_Data_1  
 
# GROUP BY 
# TCP_Code,
# TCP_NAME



# COMMAND ----------

# DBTITLE 1,Unions Tables 1-100 together - SQL Version
# %sql
# --uses union all to combine all temp tables above together as table 101 needs all of this information

# CREATE OR REPLACE TEMP VIEW LDA_Counts AS
# SELECT * from Table1_LDA
# union all
# select * from Table2_LDA
# union all
# select * from Table3_LDA
# union all
# SELECT * from Table4_LDA
# union all
# select * from Table5_LDA
# union all
# select * from Table6_LDA
# union all
# select * from Table7_LDA
# union all
# select * from Table8_LDA
# union all
# SELECT * from Table9_LDA
# union all
# select * from Table10_LDA
# union all
# select * from Table11_LDA
# union all
# select * from Table12_LDA
# union all
# select * from Table13_LDA
# union all
# SELECT * from Table14_LDA
# union all
# select * from Table15_LDA
# union all
# select * from Table50_LDA
# union all
# select * from Table51_LDA
# union all
# select * from Table70_LDA
# union all
# SELECT * from Table71_LDA
# union all
# select * from Table72_LDA
# union all
# select * from Table73_LDA
# union all
# select * from Table74_LDA
# union all
# select * from Table80_LDA
# union all
# SELECT * from Table90_LDA
# union all
# select * from Table100_LDA
# union all
# select * from Table101_LDA


# COMMAND ----------

# DBTITLE 1,Create final counts table - doesn't exist in Tim's code - needed?
# %sql
# CREATE OR REPLACE TEMP VIEW LDA_Counts_2 AS
# SELECT * from LDA_Counts
# union all
# select * from Table101_LDA

# COMMAND ----------

# DBTITLE 1,This produces the unrounded monthly files - should be put into a table... LDA_Monthly_Output_Unrounded
# %sql

# select

# previousmonthend,
# periodstart,
# periodend,
# geography,
# orgcode,
# orgname,
# tablenumber,
# primarymeasure,
# primarymeasurenumber,
# primarysplit,
# secondarymeasure,
# secondarymeasurenumber,
# secondarysplit,
# case when restraintscountofpeople is null then 0 else restraintscountofpeople end as restraintscountofpeople,
# case when restraintscountofrestraints is null then 0 else restraintscountofpeople end as restraintscountofpeople,
# case when HospitalSpellsInCarePreviousMonth is null then 0 else HospitalSpellsInCarePreviousMonth end as HospitalSpellsInCarePreviousMonth,
# case when HospitalSpellsAdmissionsInMonth is null then 0 else HospitalSpellsAdmissionsInMonth end as HospitalSpellsAdmissionsInMonth,
# case when HospitalSpellsDischargedInMonth is null then 0 else HospitalSpellsDischargedInMonth end as HospitalSpellsDischargedInMonth,
# case when HospitalSpellsAdmittedAndDischargedInMonth is null then 0 else HospitalSpellsAdmittedAndDischargedInMonth end as HospitalSpellsAdmittedAndDischargedInMonth,
# case when HospitalSpellsOpenAtEndOfMonth is null then 0 else HospitalSpellsOpenAtEndOfMonth end as HospitalSpellsOpenAtEndOfMonth,
# case when WardStaysInCarePreviousMonth is null then 0 else WardStaysInCarePreviousMonth end as WardStaysInCarePreviousMonth,
# case when WardStaysAdmissionsInMonth is null then 0 else WardStaysAdmissionsInMonth end as WardStaysAdmissionsInMonth,
# case when WardStaysDischargedInMonth is null then 0 else WardStaysDischargedInMonth end as WardStaysDischargedInMonth,
# case when WardStaysAdmittedAndDischargedInMonth is null then 0 else WardStaysAdmittedAndDischargedInMonth end as WardStaysAdmittedAndDischargedInMonth,
# case when WardStaysOpenAtEndOfMonth is null then 0 else WardStaysOpenAtEndOfMonth end as WardStaysOpenAtEndOfMonth,
# case when OpenReferralsPreviousMonth is null then 0 else OpenReferralsPreviousMonth end as OpenReferralsPreviousMonth,
# case when ReferralsStartingInTheMonth is null then 0 else ReferralsStartingInTheMonth end as ReferralsStartingInTheMonth,
# case when ReferralsEndingInTheMonth is null then 0 else ReferralsEndingInTheMonth end as ReferralsEndingInTheMonth,
# case when ReferralsStartingAndEndingInTheMonth is null then 0 else ReferralsStartingAndEndingInTheMonth end as ReferralsStartingAndEndingInTheMonth,
# case when ReferralsOpenAtEndOfMonth is null then 0 else ReferralsOpenAtEndOfMonth end as ReferralsOpenAtEndOfMonth,
    
# case when    
# tablenumber not in ('70','71','72','73','74','90') then null
# when orgcode = 'Invalid' then 'Invalid'
# when orgname like '%NHS%' then 'NHS'
# else 'Independent' end as NHS_NHD_SPLIT,
# case
# when tablenumber <> '90' then null
# else ORG_TYPE_CODE END AS ORG_TYPE_CODE

# FROM 

# LDA_Counts


# COMMAND ----------

 %sql
 
 --- tim underwood comment: i dont think any of this below is needed any longer. Need to run above code in the PROD world and compare to my output from my original code to se if it matches --

# COMMAND ----------

# DBTITLE 1,Distinct Providers and Commissioners - GBT: needed for Expand section - add to 0.Insert_lookup_data???  OR is this already covered within existing tables...?
# %sql
# -- CREATES TEMPORARY TABLES WHICH HOUSE ALL DISTINCT PROVIDERS AND COMMISSIONERS
# -- This means that all providers and commissioners are held in a list to join with the menh_analysis.ProviderCrossCategories table to ensure all options are included for all providers.

# CREATE OR REPLACE TEMP VIEW ProviderList AS

# SELECT DISTINCT a.OrgCode,
#                 a.OrgName,
#                 'Provider' as Geography
# FROM LDA_Counts a
# WHERE a.Geography = 'Provider';

# CREATE OR REPLACE TEMP VIEW Commissioner_Groupings AS
# SELECT Distinct a.OrgCode,
#                 a.OrgName,
#                 'Commissioner Groupings' as Geography
# FROM LDA_Counts a
# WHERE a.Geography = 'Commissioner Groupings';

# CREATE OR REPLACE TEMP VIEW CommissionerList AS
# SELECT DISTINCT a.OrgCode,
#                 a.OrgName,
#                 'Commissioner' as Geography
# FROM LDA_Counts a
# WHERE a.Geography = 'Commissioner';

# CREATE OR REPLACE TEMP VIEW TCPRegionList AS
# SELECT DISTINCT
# CASE WHEN o2.REL_TO_ORG_CODE IS NULL THEN 'Invalid' 
#      ELSE o2.REL_TO_ORG_CODE END AS REGION,
# CASE WHEN o3.NAME IS NULL THEN 'Invalid' 
#      ELSE o3.NAME END AS RegionName,
# 'TCP Region' as Geography
# FROM db_source.ORG_RELATIONSHIP_DAILY o1 
# LEFT JOIN db_source.ORG_RELATIONSHIP_DAILY o2 on o2.REL_FROM_ORG_CODE = o1.REL_TO_ORG_CODE and o2.REL_TYPE_CODE = 'CFCE' and o2.REL_IS_CURRENT = 1
# LEFT JOIN db_source.ORG_DAILY o3 on o3.ORG_CODE = o2.REL_TO_ORG_CODE and o3.ORG_IS_CURRENT = 1;

# CREATE OR REPLACE TEMP VIEW TCPList AS
# SELECT DISTINCT NHS_ENGLAND_TCP_CODE,
#                 TCP_NAME,
#                 'TCP' as Geography
# FROM db_source.nhse_ccg_tcp_v01
# WHERE DSS_RECORD_END_DATE IS null;


# COMMAND ----------

# DBTITLE 1,Enable cartesian products
# this enables cross joins within databricks so that the queries below can run to populate the categories table
spark.conf.set("spark.sql.crossJoin.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Inserts into categories table 
# %sql

# --empties categories table so that it can be repopulated with updated geographies etc.

# -- These two select into statements are used to add the provider and commissioner data.

# INSERT INTO menh_analysis.Categories
# SELECT 
# b.Geography,
# a.OrgCode,
# a.OrgName,
# b.TableNumber, 
# b.PrimaryMeasure, 
# b.PrimaryMeasureNumber, 
# b.PrimarySplit, 
# b.SecondaryMeasure, 
# b.SecondaryMeasureNumber, 
# b.SecondarySplit
# FROM ProviderList a
# LEFT JOIN menh_analysis.ProviderCrossCategories  b ON a.Geography = b.Geography
# WHERE TableNumber not in ('71','72','73','74');


# ------------------ add in cross tab table data here for inpatient only provider orgs -------

# INSERT INTO menh_analysis.Categories
# SELECT 
# b.Geography,
# a.OrgCode,
# c.OrgName,
# b.TableNumber, 
# b.PrimaryMeasure, 
# b.PrimaryMeasureNumber, 
# b.PrimarySplit, 
# b.SecondaryMeasure, 
# b.SecondaryMeasureNumber, 
# b.SecondarySplit
# FROM global_temp.ProvNoIPs a
# LEFT JOIN ProviderList c on a.OrgCode = c.orgcode
# CROSS JOIN menh_analysis.ProviderCrossCategories  b 
# WHERE TableNumber in ('71','72','73','74')
# AND c.Geography = 'Provider';

# INSERT INTO menh_analysis.Categories
# SELECT 
# b.Geography,
# a.OrgCode,
# a.OrgName,
# b.TableNumber, 
# b.PrimaryMeasure, 
# b.PrimaryMeasureNumber, 
# b.PrimarySplit, 
# b.SecondaryMeasure, 
# b.SecondaryMeasureNumber, 
# b.SecondarySplit
# FROM Commissioner_Groupings A
# LEFT JOIN menh_analysis.ProviderCrossCategories B 
# ON a.Geography = b.Geography;

# INSERT INTO menh_analysis.Categories
# SELECT 
# b.Geography,
# a.OrgCode,
# a.OrgName,
# b.TableNumber, 
# b.PrimaryMeasure, 
# b.PrimaryMeasureNumber, 
# b.PrimarySplit, 
# b.SecondaryMeasure, 
# b.SecondaryMeasureNumber, 
# b.SecondarySplit
# FROM CommissionerList a
# LEFT JOIN menh_analysis.ProviderCrossCategories b ON a.Geography = b.Geography;

# INSERT INTO menh_analysis.Categories
# SELECT 
# b.Geography,
# a.NHS_ENGLAND_TCP_CODE as OrgCode,
# a.TCP_NAME as OrgName,
# b.TableNumber, 
# b.PrimaryMeasure, 
# b.PrimaryMeasureNumber, 
# b.PrimarySplit, 
# b.SecondaryMeasure, 
# b.SecondaryMeasureNumber, 
# b.SecondarySplit
# FROM TCPList a
# LEFT JOIN menh_analysis.ProviderCrossCategories b ON a.Geography = b.Geography;

# INSERT INTO menh_analysis.Categories
# SELECT 
# b.Geography,
# a.Region as OrgCode,
# a.RegionName as OrgName,
# b.TableNumber, 
# b.PrimaryMeasure, 
# b.PrimaryMeasureNumber, 
# b.PrimarySplit, 
# b.SecondaryMeasure, 
# b.SecondaryMeasureNumber, 
# b.SecondarySplit
# FROM TCPREGIONList a
# LEFT JOIN menh_analysis.ProviderCrossCategories b ON a.Geography = b.Geography;





# COMMAND ----------

# DBTITLE 1,Monthly Output Unrounded 
# %sql

# -- This is where the join to menh_analysis.Categories occurs. Each field is set up using Case statements to take the data from the data previously produced in LDA_Counts (where the data is 
# -- inserted into) unless there is nothing present. If this is not present then it uses the 'menh_analysis'.Categories table and puts 0 in the number fields.

# CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_Monthly_Output_Unrounded AS
# SELECT 
# '$PreviousMonthEnd' as PreviousMonthEnd,
# '$rp_startdate' as PeriodStart,
# '$rp_enddate' as PeriodEnd,
# case when co.Geography is null then ca.Geography else co.Geography end as Geography,
# case when co.OrgCode is null then ca.OrgCode else co.OrgCode end as OrgCode,
# case when co.OrgName is null then ca.OrgName else co.OrgName end as OrgName,
# case when co.TableNumber is null then ca.TableNumber else co.TableNumber end as TableNumber,
# case when co.PrimaryMeasure is null then ca.PrimaryMeasure else co.PrimaryMeasure end as PrimaryMeasure,
# case when co.PrimaryMeasureNumber is null then ca.PrimaryMeasureNumber else co.PrimaryMeasureNumber end as PrimaryMeasureNumber,
# case when co.PrimarySplit is null then ca.PrimarySplit else co.PrimarySplit end as PrimarySplit,
# case when co.SecondaryMeasure is null then ca.SecondaryMeasure else co.SecondaryMeasure end as SecondaryMeasure, 
# case when co.SecondaryMeasureNumber is null then ca.SecondaryMeasureNumber else co.SecondaryMeasureNumber end as SecondaryMeasureNumber, 
# case when co.SecondarySplit is null then ca.SecondarySplit else co.SecondarySplit end as SecondarySplit, 
# case when co.RestraintsCountOfPeople is null then 0 else co.RestraintsCountOfPeople end as RestraintsCountOfPeople,
# case when co.RestraintsCountOfRestraints is null then 0 else co.RestraintsCountOfRestraints end as RestraintsCountOfRestraints,
# case when co.HospitalSpellsInCarePreviousMonth is null then 0 else co.HospitalSpellsInCarePreviousMonth end as HospitalSpellsInCarePreviousMonth,
# case when co.HospitalSpellsAdmissionsInMonth is null then 0 else co.HospitalSpellsAdmissionsInMonth end as HospitalSpellsAdmissionsInMonth,
# case when co.HospitalSpellsDischargedInMonth is null then 0 else co.HospitalSpellsDischargedInMonth end as HospitalSpellsDischargedInMonth,
# case when co.HospitalSpellsAdmittedAndDischargedInMonth is null then 0 else co.HospitalSpellsAdmittedAndDischargedInMonth end as HospitalSpellsAdmittedAndDischargedInMonth,
# case when co.HospitalSpellsOpenAtEndOfMonth is null then 0 else co.HospitalSpellsOpenAtEndOfMonth end as HospitalSpellsOpenAtEndOfMonth,
# case when co.WardStaysInCarePreviousMonth is null then 0 else co.WardStaysInCarePreviousMonth end as WardStaysInCarePreviousMonth,
# case when co.WardStaysAdmissionsInMonth is null then 0 else co.WardStaysAdmissionsInMonth end as WardStaysAdmissionsInMonth,
# case when co.WardStaysDischargedInMonth is null then 0 else co.WardStaysDischargedInMonth end as WardStaysDischargedInMonth,
# case when co.WardStaysAdmittedAndDischargedInMonth is null then 0 else co.WardStaysAdmittedAndDischargedInMonth end as WardStaysAdmittedAndDischargedInMonth,
# case when co.WardStaysOpenAtEndOfMonth is null then 0 else co.WardStaysOpenAtEndOfMonth end as WardStaysOpenAtEndOfMonth,
# case when co.OpenReferralsPreviousMonth is null then 0 else co.OpenReferralsPreviousMonth end as OpenReferralsPreviousMonth,
# case when co.ReferralsStartingInTheMonth is null then 0 else co.ReferralsStartingInTheMonth end as ReferralsStartingInTheMonth,
# case when co.ReferralsEndingInTheMonth is null then 0 else co.ReferralsEndingInTheMonth end as ReferralsEndingInTheMonth,
# case when co.ReferralsStartingAndEndingInTheMonth is null then 0 else co.ReferralsStartingAndEndingInTheMonth end as ReferralsStartingAndEndingInTheMonth,
# case when co.ReferralsOpenAtEndOfMonth is null then 0 else co.ReferralsOpenAtEndOfMonth end as ReferralsOpenAtEndOfMonth,
# case 
# when ca.TableNumber not in ('70','71','72','73','74','90') then null
# when co.OrgCode = 'Invalid' or ca.OrgCode = 'Invalid' then 'Invalid'
# when co.OrgName like '%NHS%' or ca.OrgName like '%NHS%' then 'NHS' 
# when co.OrgCode = 'TAE' or ca.OrgCode = 'TAE' then 'NHS'
# else 'Independent' end as NHS_IND_split,
# o.ORG_TYPE_CODE
# FROM menh_analysis.Categories ca
# left join LDA_Counts_2 co on ca.TableNumber = co.TableNumber and ca.PrimaryMeasureNumber = co.PrimaryMeasureNumber and ca.SecondaryMeasure = co.SecondaryMeasure and ca.OrgCode = co.OrgCode
# left join global_temp.RD_ORG_DAILY_LATEST_LDA o on co.OrgCode = o.ORG_CODE or ca.OrgCode = o.ORG_CODE
# Where 
# ca.TableNumber > 0




# COMMAND ----------

# %sql

# MERGE INTO $db.LDA_monthly_Unformatted  as lhs
# USING global_temp.LDA_Monthly_Output_Unrounded as rhs
# ON 
# lhs.PeriodStart = rhs.PeriodStart
# WHEN MATCHED THEN UPDATE SET lhs.EFFECTIVE_TO = date_add(current_date,-1); -- Close the old records by setting the end date to be todays date minus 1.

# -- now insert the "new" records (including those replacing the records just ended)
# INSERT INTO $db.LDA_monthly_Unformatted
# SELECT
# PreviousMonthEnd ,
# PeriodStart ,
# PeriodEnd  ,
# Geography ,
# OrgCode ,
# OrgName ,
# TableNumber ,
# PrimaryMeasure ,
# PrimaryMeasureNumber ,
# PrimarySplit ,
# SecondaryMeasure ,
# SecondaryMeasureNumber ,
# SecondarySplit ,
# RestraintsCountOfPeople ,
# RestraintsCountOfRestraints ,
# HospitalSpellsInCarePreviousMonth ,
# HospitalSpellsAdmissionsInMonth ,
# HospitalSpellsDischargedInMonth ,
# HospitalSpellsAdmittedAndDischargedInMonth ,
# HospitalSpellsOpenAtEndOfMonth ,
# WardStaysInCarePreviousMonth ,
# WardStaysAdmissionsInMonth ,
# WardStaysDischargedInMonth ,
# WardStaysAdmittedAndDischargedInMonth ,
# WardStaysOpenAtEndOfMonth ,
# OpenReferralsPreviousMonth ,
# ReferralsStartingInTheMonth ,
# ReferralsEndingInTheMonth  ,
# ReferralsStartingAndEndingInTheMonth ,
# ReferralsOpenAtEndOfMonth ,
# NHS_IND_split ,
# ORG_TYPE_CODE
# current_date as EFFECTIVE_FROM,
# null as EFFECTIVE_TO
# FROM global_temp.LDA_Monthly_Output_Unrounded