# Databricks notebook source
#filepath to notebook for preparation tables for aggregation
mhsds_run_folder_master = "./02_Specific_Prepare/"
 
#adult length of stay
los_run_folder = "01_los/"
los_prep_notebook = "los_prep"
 
#community mental health acute admissions and access
cmh_run_folder = "03_cmh/"
cmh_prep_notebook = "cmh_prep"
 
##cyp perinatal
cyp_peri_run_folder = "04_cyp_perinatal/"
cyp_peri_prep_notebook = "cyp_perinatal_prep"
 
#cyp outcomes
cyp_out_run_folder = "05_cyp_outcomes/"
cyp_out_prep_notebook = "cyp_outcomes_prep"
 
#individual placement and support service
ips_run_folder = "06_ips/"
ips_prep_notebook = "ips_prep"
 
#urgent and emergency care
uec_run_folder = "07_uec/"
uec_prep_notebook = "uec_prep"
 
#out of area placements
oaps_run_folder = "08_oaps/"
oaps_prep_notebook = "oaps_prep"

# COMMAND ----------

run_params = { 
###ADULT LOS###
  "01_los_run_filepath": mhsds_run_folder_master + los_run_folder + los_prep_notebook,
###COMMUNITY MENTAL HEALTH ACUTE ADMISSIONS AND ACCESS
  "03_cmh_run_filepath": mhsds_run_folder_master + cmh_run_folder + cmh_prep_notebook,
###CYP PERINATAL
  "04_cyp_peri_run_filepath": mhsds_run_folder_master + cyp_peri_run_folder + cyp_peri_prep_notebook,
###CYP OUTCOMES
  "05_cyp_out_run_filepath": mhsds_run_folder_master + cyp_out_run_folder + cyp_out_prep_notebook,
###INDIVIDUAL PLACEMENT SUPPORT SERVICES
  "06_ips_run_filepath": mhsds_run_folder_master + ips_run_folder + ips_prep_notebook,
### UEC
  "07_uec_filepath": mhsds_run_folder_master + uec_run_folder + uec_prep_notebook,
###OUT OF AREA PLACEMENTS
  "08_oaps_filepath": mhsds_run_folder_master + oaps_run_folder + oaps_prep_notebook,
}