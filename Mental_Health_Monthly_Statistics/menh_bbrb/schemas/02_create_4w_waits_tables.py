# Databricks notebook source
 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_referrals; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_referrals 
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 OrgIDSubICBLocResidence string,
 RecordNumber bigint,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Der_EndDate date,
 ReferClosReason string,
 ReferRejectionDate date,
 ReferRejectReason string,
 SourceOfReferralMH string,
 PrimReasonReferralMH string,
 Reason_ND int,
 Reason_ASD int,
 Der_ServTeamTypeRefToMH string,
 UniqCareProfTeamID string,
 Der_LatestRecord int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_dates;
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_dates 
 (
 Person_ID string,
 OrgIDProv string,
 UniqServReqID string,
 Der_Date date,
 Inc int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_cumf; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_cumf 
 (
 Person_ID string, 
 OrgIDProv string, 
 Der_Date date, 
 Cumf_Inc int
 ) USING DELTA 

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_groups; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_groups
 (
 Person_ID string,
 OrgIDProv string,
 Inc_Group int,
 SpellID string,
 StartDate date,
 EndDate date,
 Der_EndDate date
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_spell_rank; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_spell_rank 
 (
 Person_ID string,
 OrgIDProv string,
 SpellID string,
 StartDate date,
 EndDate date,
 CareContDate date,
 Contact_Ref string,
 Der_ContactOrder int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_subs; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_subs 
 (
 Person_ID string,
 SpellID string,
 StartDate date,
 EndDate date,
 Der_EventCategory string,
 Der_EventType string,
 Der_EventDate date
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cmh_4ww_subs_ranked; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_subs_ranked 
 (
 Person_ID string,
 SpellID string,
 StartDate date,
 EndDate date,
 Der_EventCategory string,
 First_Event date
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_4ww_spell_master; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_spell_master 
 (
 Person_ID string,
 OrgIDProv string,
 SpellID string,
 StartDate date,
 EndDate date,
 Refs int,
 Reason_ND int,
 Reason_ASD int,
 Open int,
 First_contact date,
 Second_contact date,
 First_outcome_pathway date,
 First_assessment_pathway date,
 First_Care_Plan_or_Intervention_pathway date,
 Pathway_ClockStop date
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_4ww_spell_master_long; 
 CREATE TABLE IF NOT EXISTS $db_output.cmh_4ww_spell_master_long
 ( 
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Person_ID string,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 --BITC-6882: IMD breakdowns
 IMD_Core20 string,
 SpellID string, 
 StartDate date,
 EndDate date,
 Der_Open int,
 Time_start_to_end_rp int,
 Weeks_To_End_RP float,
 Second_contact date,
 Time_to_second_contact int,
 First_outcome_pathway date,
 First_assessment_pathway date,
 First_Care_Plan_or_Intervention_pathway date,
 Pathway_ClockStop date,
 Time_to_clock_stop int,
 Spell_start int,
 Spell_closed int,
 Spell_inactive int,
 Spell_Open int,
 1st_contact_in_RP int,
 With_1st_contact int,
 2nd_contact_in_RP int,
 With_2nd_contact int,
 Outcome_in_RP int,
 With_outcome int,
 Assessment_in_RP int,
 With_assessment int,
 Intervention_or_CP_in_RP int,
 With_Intervention_or_CP int,
 Clock_stop_in_RP int,
 With_clock_stop int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_referrals; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_referrals 
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 OrgIDSubICBLocResidence string,
 RecordNumber bigint,
 UniqServReqID string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Der_EndDate date,
 ReferClosReason string,
 ReferRejectionDate date,
 ReferRejectReason string,
 SourceOfReferralMH string,
 PrimReasonReferralMH string,
 Reason_ND int,
 Reason_ASD int,
 Der_ServTeamTypeRefToMH string,
 UniqCareProfTeamID string,
 Der_LatestRecord int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_dates; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_dates 
 (
 Person_ID string,
 OrgIDProv string,
 UniqServReqID string,
 Der_Date date,
 Inc int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_cumf; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_cumf 
 (
 Person_ID string, 
 OrgIDProv string, 
 Der_Date date, 
 Cumf_Inc int
 ) USING DELTA 

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_groups; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_groups
 (
 Person_ID string,
 OrgIDProv string,
 Inc_Group int,
 SpellID string,
 StartDate date,
 EndDate date,
 Der_EndDate date
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_spell_contacts; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_spell_contacts 
 (
 Der_Activity_Type string,
 Person_ID string,
 OrgIDProv string,
 SpellID string,
 StartDate date,
 EndDate date,
 ContactDate date,
 Contact_Ref string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_indirect_activity; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_indirect_activity 
 (
 Person_ID string,
 OrgIDProv string,
 IndirectActDate date,
 RecordNumber bigint,
 UniqServReqID string,
 Der_ActRN int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_spell_rank; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_spell_rank 
 (
 Person_ID string,
 OrgIDProv string,
 SpellID string,
 StartDate date,
 EndDate date,
 ContactDate date,
 Contact_Ref string,
 Der_ContactOrder int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_subs; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_subs 
 (
 Person_ID string,
 SpellID string,
 StartDate date,
 EndDate date,
 Der_EventCategory string,
 Der_EventType string,
 Der_EventDate date
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_4ww_subs_ranked; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_subs_ranked 
 (
 Person_ID string,
 SpellID string,
 StartDate date,
 EndDate date,
 Der_EventCategory string,
 First_Event date
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_4ww_spell_master; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_spell_master 
 (
 Person_ID string,
 OrgIDProv string,
 SpellID string,
 StartDate date,
 EndDate date,
 Refs int,
 Reason_ND int,
 Reason_ASD int,
 Open int,
 First_contact date,
 First_outcome_pathway date,
 First_Care_Plan_or_Intervention_pathway date,
 Pathway_ClockStop date
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_4ww_spell_master_long; 
 CREATE TABLE IF NOT EXISTS $db_output.cyp_4ww_spell_master_long
 ( 
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Person_ID string,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 --BITC-6882: IMD breakdowns
 IMD_Core20 string,
 SpellID string, 
 StartDate date,
 EndDate date,
 Der_Open int,
 Time_start_to_end_rp int,
 Weeks_To_End_RP float,
 First_contact date,
 First_outcome_pathway date,
 First_Care_Plan_or_Intervention_pathway date,
 Pathway_ClockStop date,
 Time_to_clock_stop int,
 Spell_start int,
 Spell_closed int,
 Spell_inactive int,
 Spell_Open int,
 1st_contact_in_RP int,
 With_1st_contact int,
 Outcome_in_RP int,
 With_outcome int,
 Intervention_or_CP_in_RP int,
 With_Intervention_or_CP int,
 Clock_stop_in_RP int,
 With_clock_stop int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.snomed_4ww_refset; 
 CREATE TABLE IF NOT EXISTS $db_output.snomed_4ww_refset
 ( 
 Ref_Group string,
 Reference_Set string,
 RefSetID string,
 RefSet_EffectiveStartTime date,
 Concept_EffectiveStartTime date,
 RefSet_Active int,
 ConceptID string,
 Concept_Active int,
 RefSet_EffectiveEndTime date,
 Concept_EffectiveEndTime date,
 Der_Active int,
 Der_EffectiveStartTime date,
 Der_EffectiveEndTime date
 ) USING DELTA