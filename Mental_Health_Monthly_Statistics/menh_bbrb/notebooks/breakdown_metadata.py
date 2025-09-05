# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

# DBTITLE 1,National Breakdowns
eng_bd = {
    "breakdown_name": "England",
    "level_tier": 0,
    "level_tables": ["eng_desc"],
    "primary_level": F.lit("England"),
    "primary_level_desc": F.lit("England"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "level_list": [],
    "lookup_col": ["England"],
    "level_fields" : [  
      F.col("England").alias("breakdown"),
      F.col("England").alias("primary_level"),  
      F.col("England").alias("primary_level_desc"),  
      F.lit("NONE").alias("secondary_level"),   
      F.lit("NONE").alias("secondary_level_desc")
    ],   
}
age_band_ips_bd = {
    "breakdown_name": "England; Age Band",
    "level_tier": 0,
    "level_tables": ["age_band_desc"],
    "primary_level": F.col("Age_Band"),
    "primary_level_desc": F.col("Age_Band"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["Age_Group_IPS"],
  "level_fields" : [
    F.lit("England; Age Band").alias("breakdown"),
    F.col("Age_Group_IPS").alias("primary_level"),
    F.col("Age_Group_IPS").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
age_band_oaps_bd = {
    "breakdown_name": "England; Age Band",
    "level_tier": 0,
    "level_tables": ["age_band_desc"],
    "primary_level": F.col("Age_Band"),
    "primary_level_desc": F.col("Age_Band"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["Age_Group_OAPs"],
  "level_fields" : [
    F.lit("England; Age Band").alias("breakdown"),
    F.col("Age_Group_OAPs").alias("primary_level"),
    F.col("Age_Group_OAPs").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
##MHA_Measures
age_band_mha_bd = {
    "breakdown_name": "England; Age Band",
    "level_tier": 0,
    "level_tables": ["age_band_desc"],
    "primary_level": F.col("Age_Band"),
    "primary_level_desc": F.col("Age_Band"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["Age_Group_MHA"],
  "level_fields" : [
    F.lit("England; Age Band").alias("breakdown"),
    F.col("Age_Group_MHA").alias("primary_level"),
    F.col("Age_Group_MHA").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
##CYP_Outcome_Measures
age_band_cyp_bd = {
    "breakdown_name": "England; Age Band",
    "level_tier": 0,
    "level_tables": ["age_band_desc"],
    "primary_level": F.col("Age_Band"),
    "primary_level_desc": F.col("Age_Band"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["Age_Group_CYP"],
  "level_fields" : [
    F.lit("England; Age Band").alias("breakdown"),
    F.col("Age_Group_CYP").alias("primary_level"),
    F.col("Age_Group_CYP").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
gender_bd = {
    "breakdown_name": "England; Gender",
    "level_tier": 0,
    "level_tables": ["gender_desc"],
    "primary_level": F.col("Der_Gender"),
    "primary_level_desc": F.col("Der_Gender_Desc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["Der_Gender", "Der_Gender_Desc"],
  "level_fields" : [
    F.lit("England; Gender").alias("breakdown"),
    F.col("Der_Gender").alias("primary_level"),
    F.col("Der_Gender_Desc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],  
}
lower_eth_bd = {
    "breakdown_name": "England; Ethnicity",
    "level_tier": 0,
    "level_tables": ["ethnicity_desc"],
    "primary_level": F.col("LowerEthnicityCode"),
    "primary_level_desc": F.col("LowerEthnicityName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["LowerEthnicityCode", "LowerEthnicityName"],
  "level_fields" : [
    F.lit("England; Ethnicity").alias("breakdown"),
    F.col("LowerEthnicityCode").alias("primary_level"),
    F.col("LowerEthnicityName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")    
  ]
}
upper_eth_bd = {
    "breakdown_name": "England; Upper Ethnicity",
    "level_tier": 0,
    "level_tables": ["ethnicity_desc"],
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["UpperEthnicity"],
  "level_fields" : [
    F.lit("England; Upper Ethnicity").alias("breakdown"),
    F.col("UpperEthnicity").alias("primary_level"),
    F.col("UpperEthnicity").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
upper_eth_mean_dev_bd = {
    "breakdown_name": "England; Mean Deviation of Upper Ethnicity",
    "level_tier": 1,
    "level_tables": ["eng_desc"],
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["England"],
    "level_fields" : [
    F.lit("England; Mean Deviation of Upper Ethnicity").alias("breakdown"),
    F.col("England").alias("primary_level"),
    F.col("England").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
wnw_eth_bd = {
    "breakdown_name": "England; Ethnicity (White British/Non-White British)",
    "level_tier": 0,
    "level_tables": ["ethnicity_desc"],
    "primary_level": F.col("WNW_Ethnicity"),
    "primary_level_desc": F.col("WNW_Ethnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["WNWEthnicity"],
  "level_fields" : [
    F.lit("England; Ethnicity (White British/Non-White British)").alias("breakdown"),
    F.col("WNWEthnicity").alias("primary_level"),
    F.col("WNWEthnicity").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
wnw_eth_stan_rates_diff_bd = {
    "breakdown_name": "England; Ethnicity - Standardised Rate Difference from White British",
    "level_tier": 1,
    "level_tables": ["eng_desc"],
    "primary_level": F.col("WNW_Ethnicity"),
    "primary_level_desc": F.col("WNW_Ethnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["England"],
    "level_fields" : [
    F.lit("England; Ethnicity - Standardised Rate Difference from White British").alias("breakdown"),
    F.col("England").alias("primary_level"),
    F.col("England").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
imd_decile_bd = {
    "breakdown_name": "England; IMD Decile",
    "level_tier": 0,
    "level_tables": ["imd_desc"],
    "primary_level": F.col("IMD_Decile"),
    "primary_level_desc": F.col("IMD_Decile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["IMD_Decile_Name"],
    "level_fields" : [
    F.lit("England; IMD Decile").alias("breakdown"),
    F.col("IMD_Decile").alias("primary_level"),
    F.col("IMD_Decile").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
imd_quintile_bd = {
    "breakdown_name": "England; IMD Quintile",
    "level_tier": 0,
    "level_tables": ["imd_desc"],
    "primary_level": F.col("IMD_Quintile"),
    "primary_level_desc": F.col("IMD_Quintile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["IMD_Quintile"],
    "level_fields" : [
    F.lit("England; IMD Quintile").alias("breakdown"),
    F.col("IMD_Quintile").alias("primary_level"),
    F.col("IMD_Quintile").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
imd_core20_bd = {
    "breakdown_name": "England; IMD Core20",
    "level_tier": 0,
    "level_tables": ["imd_desc"],
    "primary_level": F.col("IMD_Core20"),
    "primary_level_desc": F.col("IMD_Core20"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["IMD_Core20"],
    "level_fields" : [
    F.lit("England; IMD Core20").alias("breakdown"),
    F.col("IMD_Core20").alias("primary_level"),
    F.col("IMD_Core20").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
imd_core20_stan_rates_diff_bd = {
    "breakdown_name": "England; IMD - Standardised Rate Difference from Most Deprived Quintile",
    "level_tier": 1,
    "level_tables": ["eng_desc"],
    "primary_level": F.col("IMD_Core20"),
    "primary_level_desc": F.col("IMD_Core20"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["England"],
    "level_fields" : [
    F.lit("England; IMD - Standardised Rate Difference from Most Deprived Quintile").alias("breakdown"),
    F.col("England").alias("primary_level"),
    F.col("England").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
reason_for_ref_bd = {
    "breakdown_name": "England; Primary reason for referral",
    "level_tier": 0,
    "level_tables": ["ref_reason_desc"],
    "primary_level": F.col("PrimReasonReferralMH"),
    "primary_level_desc": F.col("PrimReasonReferralMHName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["PrimReasonReferralMH", "PrimReasonReferralMHName"],
  "level_fields" : [
    F.lit("England; Primary reason for referral").alias("breakdown"),
    F.col("PrimReasonReferralMH").alias("primary_level"),
    F.col("PrimReasonReferralMHName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
bed_type_bd = {
    "breakdown_name": "England; Bed Type",
    "level_tier": 0,
    "level_tables": ["hosp_bed_desc"],
    "primary_level": F.col("MHAdmittedPatientClass"),
    "primary_level_desc": F.col("MHAdmittedPatientClassName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["MHAdmittedPatientClass", "MHAdmittedPatientClassName"],
  "level_fields" : [
    F.lit("England; Bed Type").alias("breakdown"),
    F.col("MHAdmittedPatientClass").alias("primary_level"),
    F.col("MHAdmittedPatientClassName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
start_bed_type_bd = {
    "breakdown_name": "England; Bed Type",
    "level_tier": 0,
    "level_tables": ["hosp_bed_desc"],
    "primary_level": F.col("StartMHAdmittedPatientClass"),
    "primary_level_desc": F.col("StartMHAdmittedPatientClassName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["MHAdmittedPatientClass", "MHAdmittedPatientClassName"],
  "level_fields" : [
    F.lit("England; Bed Type").alias("breakdown"),
    F.col("MHAdmittedPatientClass").alias("primary_level"),
    F.col("MHAdmittedPatientClassName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
active_bed_type_bd = {
    "breakdown_name": "England; Bed Type",
    "level_tier": 0,
    "level_tables": ["hosp_bed_desc"],
    "primary_level": F.col("ActiveMHAdmittedPatientClass"),
    "primary_level_desc": F.col("ActiveMHAdmittedPatientClassName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["MHAdmittedPatientClass", "MHAdmittedPatientClassName"],
  "level_fields" : [
    F.lit("England; Bed Type").alias("breakdown"),
    F.col("MHAdmittedPatientClass").alias("primary_level"),
    F.col("MHAdmittedPatientClassName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
end_bed_type_bd = {
    "breakdown_name": "England; Bed Type",
    "level_tier": 0,
    "level_tables": ["hosp_bed_desc"],
    "primary_level": F.col("EndMHAdmittedPatientClass"),
    "primary_level_desc": F.col("EndMHAdmittedPatientClassName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["MHAdmittedPatientClass", "MHAdmittedPatientClassName"],
  "level_fields" : [
    F.lit("England; Bed Type").alias("breakdown"),
    F.col("MHAdmittedPatientClass").alias("primary_level"),
    F.col("MHAdmittedPatientClassName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
serv_team_type_ref_to_bd = {
    "breakdown_name": "England; Service or Team Type Referred To",
    "level_tier": 0,
    "level_tables": ["serv_team_type_ref_to_desc"],
    "primary_level": F.col("ServTeamTypeRefToMH"),
    "primary_level_desc": F.col("ServTeamTypeRefToMHDesc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["ServTeamTypeRefToMH"],
  "level_fields" : [
    F.lit("England; Service or Team Type Referred To").alias("breakdown"),
    F.col("ServTeamTypeRefToMH").alias("primary_level"),
    F.col("ServTeamTypeRefToMHDesc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
##MHA_measures_2025
age_group_higher_bd = {
    "breakdown_name": "England; Age; Under 18 or 18 and over",
    "level_tier": 0,
    "level_tables": ["age_band_desc"],
    "primary_level": F.col("Age_Group_Higher_Level"),
    "primary_level_desc": F.col("Age_Group_Higher_Level"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["Age_Group_Higher_Level"],
  "level_fields" : [
    F.lit("England; Age; Under 18 or 18 and over").alias("breakdown"),
    F.col("Age_Group_Higher_Level").alias("primary_level"),
    F.col("Age_Group_Higher_Level").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
autism_status_bd = {
    "breakdown_name": "England; Autism Status",
    "level_tier": 0,
    "level_tables": ["autism_status_desc"],
    "primary_level": F.col("AutismStatus"),
    "primary_level_desc": F.col("AutismStatus_desc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["AutismStatus", "AutismStatus_desc"],
  "level_fields" : [
    F.lit("England; Autism Status").alias("breakdown"),
    F.col("AutismStatus").alias("primary_level"),
    F.col("AutismStatus_desc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],  
}
ld_status_bd = {
    "breakdown_name": "England; LD Status",
    "level_tier": 0,
    "level_tables": ["ld_status_desc"],
    "primary_level": F.col("LDStatus"),
    "primary_level_desc": F.col("LDStatus_desc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
  "lookup_col" : ["LDStatus", "LDStatus_desc"],
  "level_fields" : [
    F.lit("England; LD Status").alias("breakdown"),
    F.col("LDStatus").alias("primary_level"),
    F.col("LDStatus_desc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],  
}

# COMMAND ----------

# DBTITLE 1,Provider Breakdowns
prov_bd = {
    "breakdown_name": "Provider",
    "level_tier": 1,
    "level_tables": ["prov_placeholder"],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["ORG_CODE", "NAME"],
    "level_fields" : [
    F.lit("Provider").alias("breakdown"),  
    F.col("ORG_CODE").alias("primary_level"),
    F.col("NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
sending_prov_bd = {
    "breakdown_name": "Sending Provider",
    "level_tier": 1,
    "level_tables": ["oaps_prov_placeholder"],
    "primary_level": F.col("OrgIDSubmitting"),
    "primary_level_desc": F.col("SendingProvider_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["ORG_CODE", "NAME"],
    "level_fields" : [
    F.lit("Sending Provider").alias("breakdown"),  
    F.col("OrgIDSubmitting").alias("primary_level"),
    F.col("SendingProvider_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
receiving_prov_bd = {
    "breakdown_name": "Receiving Provider",
    "level_tier": 1,
    "level_tables": ["oaps_prov_placeholder"],
    "primary_level": F.col("OrgIDReceiving"),
    "primary_level_desc": F.col("ReceivingProvider_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["ORG_CODE", "NAME"],
    "level_fields" : [
    F.lit("Receiving Provider").alias("breakdown"),  
    F.col("OrgIDReceiving").alias("primary_level"),
    F.col("ReceivingProvider_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
 
sending_prov_age_band_oaps_bd = cross_dict([sending_prov_bd, age_band_oaps_bd])
sending_prov_lower_eth_bd = cross_dict([sending_prov_bd, lower_eth_bd])
sending_prov_upper_eth_bd = cross_dict([sending_prov_bd, upper_eth_bd])
sending_prov_gender_bd = cross_dict([sending_prov_bd, gender_bd])
sending_prov_imd_decile_bd = cross_dict([sending_prov_bd, imd_decile_bd])
sending_prov_autism_status_bd = cross_dict([sending_prov_bd, autism_status_bd]) ##------BITC-6682 added breakdowns
sending_prov_ld_status_bd = cross_dict([sending_prov_bd, ld_status_bd]) ##------BITC-6682 added breakdowns
sending_prov_reason_for_ref_bd = cross_dict([sending_prov_bd, reason_for_ref_bd])
sending_prov_bed_type_bd = cross_dict([sending_prov_bd, bed_type_bd])
sending_prov_start_bed_type_bd = cross_dict([sending_prov_bd, start_bed_type_bd])
sending_prov_end_bed_type_bd = cross_dict([sending_prov_bd, end_bed_type_bd])
sending_prov_active_bed_type_bd = cross_dict([sending_prov_bd, active_bed_type_bd])
sending_prov_receiving_prov_bd = cross_dict([sending_prov_bd, receiving_prov_bd]) 
 
receiving_prov_age_band_oaps_bd = cross_dict([receiving_prov_bd, age_band_oaps_bd])
receiving_prov_lower_eth_bd = cross_dict([receiving_prov_bd, lower_eth_bd])
receiving_prov_upper_eth_bd = cross_dict([receiving_prov_bd, upper_eth_bd])
receiving_prov_gender_bd = cross_dict([receiving_prov_bd, gender_bd])
receiving_prov_imd_decile_bd = cross_dict([receiving_prov_bd, imd_decile_bd])
receiving_prov_ld_status_bd = cross_dict([receiving_prov_bd, ld_status_bd]) ##------BITC-6682 added breakdowns
receiving_prov_autism_status_bd = cross_dict([receiving_prov_bd, autism_status_bd]) ##------BITC-6682 added breakdowns
receiving_prov_reason_for_ref_bd = cross_dict([receiving_prov_bd, reason_for_ref_bd])
receiving_prov_bed_type_bd = cross_dict([receiving_prov_bd, bed_type_bd])
receiving_prov_start_bed_type_bd = cross_dict([receiving_prov_bd, start_bed_type_bd])
receiving_prov_end_bed_type_bd = cross_dict([receiving_prov_bd, end_bed_type_bd])
receiving_prov_active_bed_type_bd = cross_dict([receiving_prov_bd, active_bed_type_bd])

#MHA_Measures
prov_age_band_mha_bd = cross_dict([prov_bd, age_band_mha_bd])
prov_age_group_higher_bd = cross_dict([prov_bd, age_group_higher_bd])
prov_gender_bd = cross_dict([prov_bd, gender_bd])
prov_upper_eth_bd = cross_dict([prov_bd, upper_eth_bd])
prov_lower_eth_bd = cross_dict([prov_bd, lower_eth_bd])
prov_imd_decile_bd = cross_dict([prov_bd, imd_decile_bd])
prov_autism_status_bd = cross_dict([prov_bd, autism_status_bd])
prov_ld_status_bd = cross_dict([prov_bd, ld_status_bd])

# COMMAND ----------

# DBTITLE 1,CCG Breakdowns
ccg_res_bd = {
    "breakdown_name": "CCG of Residence",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["CCG_CODE", "CCG_NAME"],
    "level_fields" : [ 
    F.lit("CCG of Residence").alias("breakdown"),
    F.col("CCG_CODE").alias("primary_level"),
    F.col("CCG_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
ccg_prac_res_bd = {
    "breakdown_name": "CCG of GP Practice or Residence",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["CCG_CODE", "CCG_NAME"],
    "level_fields" : [ 
    F.lit("CCG of GP Practice or Residence").alias("breakdown"),
    F.col("CCG_CODE").alias("primary_level"),
    F.col("CCG_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
ccg_prac_res_prov_bd = {
    "breakdown_name": "CCG of GP Practice or Residence; Provider",
    "level_tier": 1,
    "level_tables": ["firstcont_final"],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("OrgIDProv"),
    "secondary_level_desc": F.col("Provider_Name"),
    "level_list" : [],
    "lookup_col" : ["CCG_CODE", "CCG_NAME"],
    "level_fields" : [ 
    F.lit("CCG of GP Practice or Residence; Provider").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("OrgIDProv").alias("secondary_level"), 
    F.col("Provider_Name").alias("secondary_level_desc")
    ],
}
 
ccg_res_age_band_oaps_bd = cross_dict([ccg_res_bd, age_band_oaps_bd])
ccg_res_lower_eth_bd = cross_dict([ccg_res_bd, lower_eth_bd])
ccg_res_upper_eth_bd = cross_dict([ccg_res_bd, upper_eth_bd])
ccg_res_gender_bd = cross_dict([ccg_res_bd, gender_bd])
ccg_res_imd_decile_bd = cross_dict([ccg_res_bd, imd_decile_bd])
ccg_res_reason_for_ref_bd = cross_dict([ccg_res_bd, reason_for_ref_bd])
ccg_res_bed_type_bd = cross_dict([ccg_res_bd, bed_type_bd])
ccg_res_start_bed_type_bd = cross_dict([ccg_res_bd, start_bed_type_bd])
ccg_res_end_bed_type_bd = cross_dict([ccg_res_bd, end_bed_type_bd])
ccg_res_active_bed_type_bd = cross_dict([ccg_res_bd, active_bed_type_bd])
ccg_res_receiving_prov_bd = cross_dict([ccg_res_bd, receiving_prov_bd])
 
ccg_prac_res_age_band_oaps_bd = cross_dict([ccg_prac_res_bd, age_band_oaps_bd])
ccg_prac_res_lower_eth_bd = cross_dict([ccg_prac_res_bd, lower_eth_bd])
ccg_prac_res_upper_eth_bd = cross_dict([ccg_prac_res_bd, upper_eth_bd])
ccg_prac_res_gender_bd = cross_dict([ccg_prac_res_bd, gender_bd])
ccg_prac_res_imd_decile_bd = cross_dict([ccg_prac_res_bd, imd_decile_bd])
ccg_prac_res_reason_for_ref_bd = cross_dict([ccg_prac_res_bd, reason_for_ref_bd])
ccg_prac_res_bed_type_bd = cross_dict([ccg_prac_res_bd, bed_type_bd])
ccg_prac_res_start_bed_type_bd = cross_dict([ccg_prac_res_bd, start_bed_type_bd])
ccg_prac_res_end_bed_type_bd = cross_dict([ccg_prac_res_bd, end_bed_type_bd])
ccg_prac_res_active_bed_type_bd = cross_dict([ccg_prac_res_bd, active_bed_type_bd])
ccg_prac_res_receiving_prov_bd = cross_dict([ccg_prac_res_bd, receiving_prov_bd])

#MHA_Measures
ccg_prac_res_age_band_mha_bd = cross_dict([ccg_prac_res_bd, age_band_mha_bd])
ccg_prac_res_age_group_higher_bd = cross_dict([ccg_prac_res_bd, age_group_higher_bd])
ccg_prac_res_upper_eth_bd = cross_dict([ccg_prac_res_bd, upper_eth_bd])
ccg_prac_res_autism_status_bd = cross_dict([ccg_prac_res_bd, autism_status_bd])
ccg_prac_res_ld_status_bd = cross_dict([ccg_prac_res_bd, ld_status_bd])

# COMMAND ----------

# DBTITLE 1,STP Breakdowns
stp_res_bd = {
    "breakdown_name": "STP of Residence",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["STP_CODE", "STP_NAME"],
    "level_fields" : [ 
    F.lit("STP of Residence").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
stp_prac_res_bd = {
    "breakdown_name": "STP of GP Practice or Residence",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["STP_CODE", "STP_NAME"],
    "level_fields" : [ 
    F.lit("STP of GP Practice or Residence").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
stp_prac_res_upper_eth_mean_dev_bd = {
    "breakdown_name": "STP of GP Practice or Residence; Mean Deviation of Upper Ethnicity",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.col("STP_Code"),
    "secondary_level_desc": F.col("STP_Name"),
    "level_list" : [],
    "lookup_col" : ["STP_CODE", "STP_NAME"],
    "level_fields" : [
    F.lit("STP of GP Practice or Residence; Mean Deviation of Upper Ethnicity").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
stp_prac_res_wnw_eth_stan_rates_diff_bd = {
    "breakdown_name": "STP of GP Practice or Residence; Ethnicity - Standardised Rate Difference from White British",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("WNW_Ethnicity"),
    "primary_level_desc": F.col("WNW_Ethnicity"),
    "secondary_level": F.col("STP_Code"),
    "secondary_level_desc": F.col("STP_Name"),
    "level_list" : [],
    "lookup_col" : ["STP_CODE", "STP_NAME"],
    "level_fields" : [
    F.lit("STP of GP Practice or Residence; Ethnicity - Standardised Rate Difference from White British").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
stp_prac_res_imd_core20_stan_rates_diff_bd = {
    "breakdown_name": "STP of GP Practice or Residence; IMD - Standardised Rate Difference from Most Deprived Quintile",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("IMD_Core20"),
    "primary_level_desc": F.col("IMD_Core20"),
    "secondary_level": F.col("STP_Code"),
    "secondary_level_desc": F.col("STP_Name"),
    "level_list" : [],
    "lookup_col" : ["STP_CODE", "STP_NAME"],
    "level_fields" : [
    F.lit("STP of GP Practice or Residence; IMD - Standardised Rate Difference from Most Deprived Quintile").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
 
stp_res_wnw_eth_bd = cross_dict([stp_res_bd, wnw_eth_bd])
stp_res_age_band_oaps_bd = cross_dict([stp_res_bd, age_band_oaps_bd])
stp_res_lower_eth_bd = cross_dict([stp_res_bd, lower_eth_bd])
stp_res_upper_eth_bd = cross_dict([stp_res_bd, upper_eth_bd])
stp_res_gender_bd = cross_dict([stp_res_bd, gender_bd])
stp_res_imd_decile_bd = cross_dict([stp_res_bd, imd_decile_bd])
stp_res_reason_for_ref_bd = cross_dict([stp_res_bd, reason_for_ref_bd])
stp_res_bed_type_bd = cross_dict([stp_res_bd, bed_type_bd])
stp_res_start_bed_type_bd = cross_dict([stp_res_bd, start_bed_type_bd])
stp_res_end_bed_type_bd = cross_dict([stp_res_bd, end_bed_type_bd])
stp_res_active_bed_type_bd = cross_dict([stp_res_bd, active_bed_type_bd])
stp_res_receiving_prov_bd = cross_dict([stp_res_bd, receiving_prov_bd])
 
stp_prac_res_wnw_eth_bd = cross_dict([stp_prac_res_bd, wnw_eth_bd])
stp_prac_res_age_band_oaps_bd = cross_dict([stp_prac_res_bd, age_band_oaps_bd])
stp_prac_res_upper_eth_bd = cross_dict([stp_prac_res_bd, upper_eth_bd])
stp_prac_res_lower_eth_bd = cross_dict([stp_prac_res_bd, lower_eth_bd])
stp_prac_res_wnw_eth_bd = cross_dict([stp_prac_res_bd, wnw_eth_bd])
stp_prac_res_gender_bd = cross_dict([stp_prac_res_bd, gender_bd])
stp_prac_res_imd_decile_bd = cross_dict([stp_prac_res_bd, imd_decile_bd])
stp_prac_res_imd_quintile_bd = cross_dict([stp_prac_res_bd, imd_quintile_bd])
stp_prac_res_imd_core20_bd = cross_dict([stp_prac_res_bd, imd_core20_bd])
stp_prac_res_reason_for_ref_bd = cross_dict([stp_prac_res_bd, reason_for_ref_bd])
stp_prac_res_bed_type_bd = cross_dict([stp_prac_res_bd, bed_type_bd])
stp_prac_res_start_bed_type_bd = cross_dict([stp_prac_res_bd, start_bed_type_bd])
stp_prac_res_end_bed_type_bd = cross_dict([stp_prac_res_bd, end_bed_type_bd])
stp_prac_res_active_bed_type_bd = cross_dict([stp_prac_res_bd, active_bed_type_bd])
stp_prac_res_receiving_prov_bd = cross_dict([stp_prac_res_bd, receiving_prov_bd])
 
#MHA_Measures
stp_prac_res_age_band_mha_bd = cross_dict([stp_prac_res_bd, age_band_mha_bd])
stp_prac_res_age_band_cyp_bd = cross_dict([stp_prac_res_bd, age_band_cyp_bd])
stp_prac_res_age_group_higher_bd = cross_dict([stp_prac_res_bd, age_group_higher_bd])
stp_prac_res_upper_eth_bd = cross_dict([stp_prac_res_bd, upper_eth_bd])
stp_prac_res_autism_status_bd = cross_dict([stp_prac_res_bd, autism_status_bd])
stp_prac_res_ld_status_bd = cross_dict([stp_prac_res_bd, ld_status_bd])

# COMMAND ----------

# DBTITLE 1,Region Breakdowns
comm_region_bd = {
    "breakdown_name": "Commissioning Region",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : ["REGION_CODE", "REGION_NAME"],
    "level_fields" : [ 
    F.lit("Commissioning Region").alias("breakdown"),
    F.col("REGION_CODE").alias("primary_level"),
    F.col("REGION_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
comm_region_upper_eth_mean_dev_bd = {
    "breakdown_name": "Commissioning Region; Mean Deviation of Upper Ethnicity",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.col("Region_Code"),
    "secondary_level_desc": F.col("Region_Name"),
    "level_list" : [],
    "lookup_col" : ["REGION_CODE", "REGION_NAME"],
    "level_fields" : [
    F.lit("Commissioning Region; Mean Deviation of Upper Ethnicity").alias("breakdown"),
    F.col("REGION_CODE").alias("primary_level"),
    F.col("REGION_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
} 
comm_region_wnw_eth_stan_rates_diff_bd = {
    "breakdown_name": "Commissioning Region; Ethnicity - Standardised Rate Difference from White British",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("WNW_Ethnicity"),
    "primary_level_desc": F.col("WNW_Ethnicity"),
    "secondary_level": F.col("Region_Code"),
    "secondary_level_desc": F.col("Region_Name"),
    "level_list" : [],
    "lookup_col" : ["REGION_CODE", "REGION_NAME"],
    "level_fields" : [
    F.lit("Commissioning Region; Ethnicity - Standardised Rate Difference from White British").alias("breakdown"),
    F.col("REGION_CODE").alias("primary_level"),
    F.col("REGION_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
comm_region_imd_core20_stan_rates_diff_bd = {
    "breakdown_name": "Commissioning Region; IMD - Standardised Rate Difference from Most Deprived Quintile",
    "level_tier": 1,
    "level_tables": ["bbrb_stp_mapping"],
    "primary_level": F.col("IMD_Core20"),
    "primary_level_desc": F.col("IMD_Core20"),
    "secondary_level": F.col("Region_Code"),
    "secondary_level_desc": F.col("Region_Name"),
    "level_list" : [],
    "lookup_col" : ["REGION_CODE", "REGION_NAME"],
    "level_fields" : [
    F.lit("Commissioning Region; IMD - Standardised Rate Difference from Most Deprived Quintile").alias("breakdown"),
    F.col("REGION_CODE").alias("primary_level"),
    F.col("REGION_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
 
comm_region_age_band_oaps_bd = cross_dict([comm_region_bd, age_band_oaps_bd])
comm_region_age_band_cyp_bd = cross_dict([comm_region_bd, age_band_cyp_bd])
comm_region_lower_eth_bd = cross_dict([comm_region_bd, lower_eth_bd])
comm_region_upper_eth_bd = cross_dict([comm_region_bd, upper_eth_bd])
comm_region_wnw_eth_bd = cross_dict([comm_region_bd, wnw_eth_bd])
comm_region_gender_bd = cross_dict([comm_region_bd, gender_bd])
comm_region_imd_decile_bd = cross_dict([comm_region_bd, imd_decile_bd])
comm_region_imd_quintile_bd = cross_dict([comm_region_bd, imd_quintile_bd])
comm_region_imd_core20_bd = cross_dict([comm_region_bd, imd_core20_bd])
comm_region_reason_for_ref_bd = cross_dict([comm_region_bd, reason_for_ref_bd])
comm_region_bed_type_bd = cross_dict([comm_region_bd, bed_type_bd])
comm_region_start_bed_type_bd = cross_dict([comm_region_bd, start_bed_type_bd])
comm_region_end_bed_type_bd = cross_dict([comm_region_bd, end_bed_type_bd])
comm_region_active_bed_type_bd = cross_dict([comm_region_bd, active_bed_type_bd])
comm_region_receiving_prov_bd = cross_dict([comm_region_bd, receiving_prov_bd])
 
#MHA_Measures
comm_region_age_band_mha_bd = cross_dict([comm_region_bd, age_band_mha_bd])
comm_region_age_group_higher_bd = cross_dict([comm_region_bd, age_group_higher_bd])
comm_region_upper_eth_bd = cross_dict([comm_region_bd, upper_eth_bd])
comm_region_autism_status_bd = cross_dict([comm_region_bd, autism_status_bd])
comm_region_ld_status_bd = cross_dict([comm_region_bd, ld_status_bd])

# COMMAND ----------

# DBTITLE 1,Population Breakdowns
eng_pop_bd = {
  "breakdown_name": "England", 
  "source_table": "eng_pop", 
  "primary_level": F.lit("England"), 
  "primary_level_desc": F.lit("England"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
age_band_ips_pop_bd = {
  "breakdown_name": "England; Age Band", 
  "source_table": "age_band_pop", 
  "primary_level": F.col("Age_Group_IPS"), 
  "primary_level_desc": F.col("Age_Group_IPS"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
age_band_oaps_pop_bd = {
  "breakdown_name": "England; Age Band", 
  "source_table": "age_band_pop", 
  "primary_level": F.col("Age_Group_OAPs"), 
  "primary_level_desc": F.col("Age_Group_OAPs"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
age_band_mha_pop_bd = {
  "breakdown_name": "England; Age Band", 
  "source_table": "age_band_pop", 
  "primary_level": F.col("Age_Group_MHA"), 
  "primary_level_desc": F.col("Age_Group_MHA"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
age_band_cyp_pop_bd = {
  "breakdown_name": "England; Age Band", 
  "source_table": "age_band_pop", 
  "primary_level": F.col("Age_Group_CYP"), 
  "primary_level_desc": F.col("Age_Group_CYP"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
age_group_higher_pop_bd = {
  "breakdown_name": "England; Age; Under 18 or 18 and over", 
  "source_table": "age_band_pop", 
  "primary_level": F.col("Age_Group_Higher_Level"), 
  "primary_level_desc": F.col("Age_Group_Higher_Level"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
gender_pop_bd = {
  "breakdown_name": "England; Gender", 
  "source_table": "gender_pop", 
  "primary_level": F.col("Der_Gender"), 
  "primary_level_desc": F.col("Der_Gender_Desc"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
lower_eth_pop_bd = {
  "breakdown_name": "England; Ethnicity", 
  "source_table": "eth_pop", 
  "primary_level": F.col("LowerEthnicityCode"), 
  "primary_level_desc": F.col("LowerEthnicityName"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
upper_eth_pop_bd = {
  "breakdown_name": "England; Upper Ethnicity", 
  "source_table": "eth_pop", 
  "primary_level": F.col("UpperEthnicity"), 
  "primary_level_desc": F.col("UpperEthnicity"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
wnw_eth_pop_bd = {
  "breakdown_name": "England; Ethnicity (White British/Non-White British)", 
  "source_table": "eth_pop", 
  "primary_level": F.col("WNWEthnicity"), 
  "primary_level_desc": F.col("WNWEthnicity"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
imd_decile_pop_bd = {
  "breakdown_name": "England; IMD Decile", 
  "source_table": "imd_pop", 
  "primary_level": F.col("IMD_Decile"), 
  "primary_level_desc": F.col("IMD_Decile"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
imd_quintile_pop_bd = {
  "breakdown_name": "England; IMD Quintile", 
  "source_table": "imd_pop", 
  "primary_level": F.col("IMD_Quintile"), 
  "primary_level_desc": F.col("IMD_Quintile"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
imd_core20_pop_bd = {
  "breakdown_name": "England; IMD Core20", 
  "source_table": "imd_pop", 
  "primary_level": F.col("IMD_Core20"), 
  "primary_level_desc": F.col("IMD_Core20"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
ccg_res_pop_bd = {
  "breakdown_name": "CCG of Residence", 
  "source_table": "commissioner_pop", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
ccg_prac_res_pop_bd = {
  "breakdown_name": "CCG of GP Practice or Residence", 
  "source_table": "commissioner_pop", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
ccg_prac_res_age_band_ips_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, age_band_ips_pop_bd])
ccg_prac_res_age_band_oaps_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, age_band_oaps_pop_bd])
##MHA_Measures
ccg_prac_res_age_band_mha_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, age_band_mha_pop_bd])
ccg_prac_res_age_group_higher_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, age_group_higher_pop_bd])
ccg_prac_res_lower_eth_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, lower_eth_pop_bd])
ccg_prac_res_upper_eth_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, upper_eth_pop_bd])
ccg_prac_res_gender_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, gender_pop_bd])
ccg_prac_res_imd_decile_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, imd_decile_pop_bd])
ccg_prac_res_imd_quintile_pop_bd = pop_cross_dict([ccg_prac_res_pop_bd, imd_quintile_pop_bd])
 
stp_prac_res_pop_bd = {
  "breakdown_name": "STP of GP Practice or Residence", 
  "source_table": "commissioner_pop", 
  "primary_level": F.col("STP_Code"), 
  "primary_level_desc": F.col("STP_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
stp_prac_res_age_band_ips_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, age_band_ips_pop_bd])
stp_prac_res_age_band_oaps_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, age_band_oaps_pop_bd])
stp_prac_res_age_band_cyp_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, age_band_cyp_pop_bd])
stp_prac_res_lower_eth_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, lower_eth_pop_bd])
stp_prac_res_upper_eth_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, upper_eth_pop_bd])
stp_prac_res_wnw_eth_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, wnw_eth_pop_bd])
stp_prac_res_gender_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, gender_pop_bd])
stp_prac_res_imd_decile_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, imd_decile_pop_bd])
stp_prac_res_imd_quintile_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, imd_quintile_pop_bd])
stp_prac_res_imd_core20_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, imd_core20_pop_bd])
##MHA_Measures
stp_prac_res_age_band_mha_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, age_band_mha_pop_bd])
stp_prac_res_age_group_higher_pop_bd = pop_cross_dict([stp_prac_res_pop_bd, age_group_higher_pop_bd])
 
comm_region_pop_bd = {
  "breakdown_name": "Commissioning Region", 
  "source_table": "commissioner_pop", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"),
  "aggregate_field": "SUM(POPULATION_COUNT)"
}
comm_region_age_band_ips_pop_bd = pop_cross_dict([comm_region_pop_bd, age_band_ips_pop_bd])
comm_region_age_band_oaps_pop_bd = pop_cross_dict([comm_region_pop_bd, age_band_oaps_pop_bd])
comm_region_age_band_cyp_pop_bd = pop_cross_dict([comm_region_pop_bd, age_band_cyp_pop_bd])
comm_region_lower_eth_pop_bd = pop_cross_dict([comm_region_pop_bd, lower_eth_pop_bd])
comm_region_upper_eth_pop_bd = pop_cross_dict([comm_region_pop_bd, upper_eth_pop_bd])
comm_region_wnw_eth_pop_bd = pop_cross_dict([comm_region_pop_bd, wnw_eth_pop_bd])
comm_region_gender_pop_bd = pop_cross_dict([comm_region_pop_bd, gender_pop_bd])
comm_region_imd_decile_pop_bd = pop_cross_dict([comm_region_pop_bd, imd_decile_pop_bd])
comm_region_imd_quintile_pop_bd = pop_cross_dict([comm_region_pop_bd, imd_quintile_pop_bd])
comm_region_imd_core20_pop_bd = pop_cross_dict([comm_region_pop_bd, imd_core20_pop_bd])
##MHA_Measures
comm_region_age_band_mha_pop_bd = pop_cross_dict([comm_region_pop_bd, age_band_mha_pop_bd])
comm_region_age_group_higher_pop_bd = pop_cross_dict([comm_region_pop_bd, age_group_higher_pop_bd])
 
pop_metadata = {
  "MHSPOPALL": {
    "measure_name": "Mid-year population - All ages",
    "age_group": (F.col("POPULATION_COUNT").isNotNull()),
    "breakdowns": [eng_pop_bd, 
                   age_band_ips_pop_bd, age_band_oaps_pop_bd, age_band_mha_pop_bd, age_group_higher_pop_bd, 
                   gender_pop_bd, 
                   lower_eth_pop_bd, upper_eth_pop_bd, wnw_eth_pop_bd,
                   imd_decile_pop_bd, imd_quintile_pop_bd, imd_core20_pop_bd,
                   ccg_res_pop_bd, 
                   ccg_prac_res_pop_bd, ccg_prac_res_age_band_oaps_pop_bd, ccg_prac_res_age_band_mha_pop_bd, ccg_prac_res_age_group_higher_pop_bd, ccg_prac_res_lower_eth_pop_bd, ccg_prac_res_upper_eth_pop_bd, 
                   ccg_prac_res_gender_pop_bd, ccg_prac_res_imd_decile_pop_bd, 
                   stp_prac_res_pop_bd, stp_prac_res_age_band_oaps_pop_bd, stp_prac_res_age_band_mha_pop_bd, stp_prac_res_age_group_higher_pop_bd, stp_prac_res_lower_eth_pop_bd, stp_prac_res_upper_eth_pop_bd,
                   stp_prac_res_wnw_eth_pop_bd, stp_prac_res_gender_pop_bd, stp_prac_res_imd_decile_pop_bd, stp_prac_res_imd_quintile_pop_bd,stp_prac_res_imd_core20_pop_bd,
                   comm_region_pop_bd, comm_region_age_band_oaps_pop_bd, comm_region_age_band_mha_pop_bd, comm_region_age_group_higher_pop_bd, comm_region_lower_eth_pop_bd, comm_region_upper_eth_pop_bd, 
                   comm_region_wnw_eth_pop_bd, comm_region_gender_pop_bd, comm_region_imd_decile_pop_bd, comm_region_imd_quintile_pop_bd, comm_region_imd_core20_pop_bd
                  ]
  },
  "MHSPOPCYP": {
    "measure_name": "Mid-year population - ages 0 to 17",
    "age_group": (F.col("Age_Group") == "0-17"),
    "breakdowns": [eng_pop_bd, age_band_cyp_pop_bd, gender_pop_bd, upper_eth_pop_bd, wnw_eth_pop_bd, imd_decile_pop_bd, imd_quintile_pop_bd, imd_core20_pop_bd,
                   ccg_res_pop_bd, ccg_prac_res_pop_bd,
                   stp_prac_res_pop_bd, stp_prac_res_age_band_cyp_pop_bd, stp_prac_res_lower_eth_pop_bd, stp_prac_res_upper_eth_pop_bd, stp_prac_res_wnw_eth_pop_bd,
                   stp_prac_res_gender_pop_bd, stp_prac_res_imd_quintile_pop_bd, stp_prac_res_imd_core20_pop_bd,
                   comm_region_pop_bd, comm_region_age_band_cyp_pop_bd, comm_region_lower_eth_pop_bd, comm_region_upper_eth_pop_bd, comm_region_wnw_eth_pop_bd, 
                   comm_region_gender_pop_bd, comm_region_imd_quintile_pop_bd, comm_region_imd_core20_pop_bd
                  ]
  },
  "MHSPOPADU": {
    "measure_name": "Mid-year population - ages 18 to 64",
    "age_group": (F.col("Age_Group") == "18-64"),
    "breakdowns": [eng_pop_bd, ccg_res_pop_bd, ccg_prac_res_pop_bd]
  },
  "MHSPOPOAP": {
    "measure_name": "Mid-year population - ages 65 and over",
    "age_group": (F.col("Age_Group") == "65+"),
    "breakdowns": [eng_pop_bd, ccg_res_pop_bd, ccg_prac_res_pop_bd]
  },
}