# Databricks notebook source
 %md
 
 # Breakdowns & metrics
 
 When the output tables are populated, the unpopulated metrics should still be in the file, but with a zero value. This requires a list of all the possible metrics. That's why as follows, we have tables with the possible breakdowns/metrics for each product. 

# COMMAND ----------

# DBTITLE 1,Collect params for Python
 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")

# COMMAND ----------

# DBTITLE 1, Access and Waiting Times
 %sql
 
 TRUNCATE TABLE $db_output.AWT_breakdown_values;
 INSERT INTO $db_output.AWT_breakdown_values VALUES
 ('England'),
 ('England; Ethnicity'),
 ('CCG - GP Practice or Residence'),
 ('CCG - GP Practice or Residence; Ethnicity'),
 ('Provider'),
 ('Provider; Ethnicity'),
 ('STP - GP Practice or Residence'),
 ('Commissioning Region');
 
 TRUNCATE TABLE $db_output.AWT_level_values;
 INSERT INTO $db_output.AWT_level_values
 SELECT DISTINCT
   IC_Rec_CCG as level, 
   COALESCE(NAME, "UNKNOWN") as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CCG - GP Practice or Residence' as breakdown 
 FROM $db_output.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 UNION ALL
 SELECT DISTINCT
   ORG_CODE as level, 
   NAME as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Provider' as breakdown 
   FROM $db_output.providers_between_rp_start_end_dates -- WARNING: The data in this view differs depending on the month_id
 UNION ALL
 SELECT 
   'England' as Level, 
   'England' as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'England' as breakdown
 UNION ALL
 SELECT distinct
   STP_Code as Level, 
   STP_Description as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'STP - GP Practice or Residence' as breakdown
   from $db_output.STP_Region_mapping_post_2020
 UNION ALL
 SELECT distinct
   Region_code as level, 
   Region_description as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Commissioning Region' as breakdown
   from $db_output.STP_Region_mapping_post_2020
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'White' as secondary_level, 'White' as secondary_level_desc, 'England; Ethnicity' as breakdown
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'Mixed' as secondary_level, 'Mixed' as secondary_level_desc, 'England; Ethnicity' as breakdown
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'Asian or Asian British' as secondary_level, 'Asian or Asian British' as secondary_level_desc, 'England; Ethnicity' as breakdown
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'Black or Black British' as secondary_level, 'Black or Black British' as secondary_level_desc, 'England; Ethnicity' as breakdown
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'Other Ethnic Groups' as secondary_level, 'Other Ethnic Groups' as secondary_level_desc, 'England; Ethnicity' as breakdown
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'Not Stated' as secondary_level, 'Not Stated' as secondary_level_desc, 'England; Ethnicity' as breakdown
 UNION ALL
 SELECT 'England' as level, 'England' as level_desc, 'Unknown' as secondary_level, 'Unknown' as secondary_level_desc, 'England; Ethnicity' as breakdown
 
 -- Added below for CCG level breakdown of Ethnicity
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'White' as secondary_level, 'White' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Mixed' as secondary_level, 'Mixed' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Asian or Asian British' as secondary_level, 'Asian or Asian British' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Black or Black British' as secondary_level, 'Black or Black British' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Other Ethnic Groups' as secondary_level, 'Other Ethnic Groups' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Not Stated' as secondary_level, 'Not Stated' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 UNION ALL
 SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Unknown' as secondary_level, 'Unknown' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM $db_output.CCG
 
 -- Added below for Provider level breakdown of Ethnicity
 UNION ALL  
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'White' as secondary_level, 'White' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates
 UNION ALL
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Mixed' as secondary_level, 'Mixed' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates
 UNION ALL
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Asian or Asian British' as secondary_level, 'Asian or Asian British' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates
 UNION ALL
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Black or Black British' as secondary_level, 'Black or Black British' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates
 UNION ALL
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Other Ethnic Groups' as secondary_level, 'Other Ethnic Groups' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates
 UNION ALL
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Not Stated' as secondary_level, 'Not Stated' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates
 UNION ALL
 SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Unknown' as secondary_level, 'Unknown' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM $db_output.providers_between_rp_start_end_dates;
 
 
 TRUNCATE TABLE $db_output.AWT_metric_values;
 INSERT INTO $db_output.AWT_metric_values VALUES 
   ('ED32', 'New referrals with eating disorder issues, aged 0 to 18')
   , ('EIP01', 'Open referrals on EIP pathway in treatment')
   , ('EIP01a', 'Open referrals on EIP pathway in treatment , Aged 0 to 17')
   , ('EIP01b', 'Open referrals on EIP pathway in treatment , Aged 18 to 34')
   , ('EIP01c', 'Open referrals on EIP pathway in treatment , Aged 35 and over')
   , ('EIP23a', 'Referrals on EIP pathway entering treatment')
   , ('EIP23aa', 'Referrals on EIP pathway entering treatment , Aged 0 to 17')
   , ('EIP23ab', 'Referrals on EIP pathway entering treatment , Aged 18 to 34')
   , ('EIP23ac', 'Referrals on EIP pathway entering treatment , Aged 35 and over')
   , ('EIP23b', 'Referrals on EIP pathway entering treatment within two weeks')
   , ('EIP23ba', 'Referrals on EIP pathway entering treatment within two weeks , Aged 0 to 17')
   , ('EIP23bb', 'Referrals on EIP pathway entering treatment within two weeks , Aged 18 to 34')
   , ('EIP23bc', 'Referrals on EIP pathway entering treatment within two weeks , Aged 35 and over')
   , ('EIP23c', 'Referrals on EIP pathway entering treatment more than two weeks')
   , ('EIP23ca', 'Referrals on EIP pathway entering treatment more than two weeks , Aged 0 to 17')
   , ('EIP23cb', 'Referrals on EIP pathway entering treatment more than two weeks , Aged 18 to 34')
   , ('EIP23cc', 'Referrals on EIP pathway entering treatment more than two weeks , Aged 35 and over')
   , ('EIP23d', 'Open referrals on EIP pathway waiting for treatment')
   , ('EIP23da', 'Open referrals on EIP pathway waiting for treatment , Aged 0 to 17')
   , ('EIP23db', 'Open referrals on EIP pathway waiting for treatment , Aged 18 to 34')
   , ('EIP23dc', 'Open referrals on EIP pathway waiting for treatment , Aged 35 and over')
   , ('EIP23e', 'Open referrals on EIP pathway waiting for treatment within two weeks')
   , ('EIP23ea', 'Open referrals on EIP pathway waiting for treatment within two weeks , Aged 0 to 17')
   , ('EIP23eb', 'Open referrals on EIP pathway waiting for treatment within two weeks , Aged 18 to 34')
   , ('EIP23ec', 'Open referrals on EIP pathway waiting for treatment within two weeks , Aged 35 and over')
   , ('EIP23f', 'Open referrals on EIP pathway waiting for treatment more than two weeks')
   , ('EIP23fa', 'Open referrals on EIP pathway waiting for treatment more than two weeks , Aged 0 to 17')
   , ('EIP23fb', 'Open referrals on EIP pathway waiting for treatment more than two weeks , Aged 18 to 34')
   , ('EIP23fc', 'Open referrals on EIP pathway waiting for treatment more than two weeks , Aged 35 and over')
   , ('EIP23g', 'Referrals on EIP pathway that receive a first contact')
   , ('EIP23h', 'Referrals on EIP pathway that are assigned to care coordinator')
   , ('EIP23i', 'Proportion entering treatment waiting two weeks or less')
   , ('EIP23ia', 'Proportion entering treatment waiting two weeks or less , Aged 0 to 17')
   , ('EIP23ib', 'Proportion entering treatment waiting two weeks or less , Aged 18 to 34')
   , ('EIP23ic', 'Proportion entering treatment waiting two weeks or less , Aged 35 and over')
   , ('EIP23j', 'Proportion waiting more than two weeks (still waiting)')
   , ('EIP23ja', 'Proportion waiting more than two weeks (still waiting) , Aged 0 to 17')
   , ('EIP23jb', 'Proportion waiting more than two weeks (still waiting) , Aged 18 to 34')
   , ('EIP23jc', 'Proportion waiting more than two weeks (still waiting) , Aged 35 and over')
   , ('EIP32', 'New referrals with a suspected FEP')
   , ('EIP63', 'Open referrals not on EIP pathway')
   , ('EIP63a', 'Open referrals not on EIP pathway aged 0 to 17')
   , ('EIP63b', 'Open referrals not on EIP pathway aged 18 to 34')
   , ('EIP63c', 'Open referrals not on EIP pathway aged 35 and over')
   , ('EIP64', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team')
   , ('EIP64a', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team aged 0 to 17')
   , ('EIP64b', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team aged 18 to 34')
   , ('EIP64c', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team aged 35 and over')
   , ('EIP65', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral')
   , ('EIP65a', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral aged 0 to 17')
   , ('EIP65b', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral aged 18 to 34')
   , ('EIP65c', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral aged 35 and over')
   , ('EIP66', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral')
   , ('EIP66a', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 0 to 17')
   , ('EIP66b', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 18 to 34')
   , ('EIP66c', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 35 and over')
   , ('EIP67', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral')
   , ('EIP67a', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 0 to 18')
   , ('EIP67b', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 18 to 34')
   , ('EIP67c', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 35 and over')
   , ('EIP68', 'EIP Caseload (Number of Referrals open to EIP services with at least one attended contact at the end of the reporting period)')
   , ('EIP69a', 'Number of open Referrals with any valid SNOMED-CT activity submitted that were linked to an EIP team in the reporting period')
   , ('EIP69b', 'Number of open Referrals with any NICE concordant EIP SNOMED-CT activity submitted that were linked to an EIP team in the reporting period')
   , ('MHS32', 'New referrals');

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.EIP_Nice_Snomed;
 INSERT INTO $db_output.EIP_Nice_Snomed VALUES 
 
   ('718026005',  'Cognitive behavioural therapy for psychosis')
 	,('1097161000000100',  'Referral for cognitive behavioural therapy for psychosis')
 	,('985451000000105',  'Family intervention for psychosis')
 	,('859411000000105',  'Referral for family therapy')
 	,('723948002',  'Clozapine therapy')
 	,('196771000000101',  'Smoking assessment')
 	,('443781008',  'Assessment of lifestyle')
 	,('698094009',  'Measurement of body mass index')
 	,('171222001',  'Hypertension screening')
 	,('43396009',  'Haemoglobin A1c measurement')
 	,('271062006',  'Fasting blood glucose measurement')
 	,('271061004',  'Random blood glucose measurement')
 	,('121868005',  'Total cholesterol measurement')
 	,('17888004',  'High density lipoprotein measurement')
 	,('166842003',  'Total cholesterol: HDL ratio measurement')
 	,('763243004',  'Assessment using QRISK cardiovascular disease 10 year risk calculator')
 	,('225323000',  'Smoking cessation education')
 	,('710081004',  'Smoking cessation therapy')
 	,('871661000000106',  'Referral to smoking cessation service')
 	,('715282001',  'Combined healthy eating and physical education programme')
 	,('1094331000000103',  'Referral for combined healthy eating and physical education programme')
 	,('281078001',  'Education about alcohol consumption')
 	,('425014005',  'Substance use education')
 	,('1099141000000106',  'Referral to alcohol misuse service')
 	,('201521000000104',  'Referral to substance misuse service')
 	,('699826006',  'Lifestyle education regarding risk of diabetes')
 	,('718361005',  'Weight management programme')
 	,('408289007',  'Refer to weight management programme')
 	,('1097171000000107',  'Referral for lifestyle education')
 	,('1090411000000105',  'Referral to general practice service')
 	,('1097181000000109',  'Referral for antihypertensive therapy')
 	,('308116003',  'Antihypertensive therapy')
 	,('1099151000000109',  'Referral for diabetic care')
 	,('385804009',  'Diabetic care')
 	,('1098021000000108',  'Diet modification')
 	,('1097191000000106',  'Metformin therapy')
 	,('134350008',  'Lipid lowering therapy')
 	,('183339004',  'Education rehabilitation')
 	,('415271004',  'Referral to education service')
 	,('70082004',  'Vocational rehabilitation')
 	,('18781004',  'Patient referral for vocational rehabilitation')
 	,('335891000000102',  'Supported employment')
 	,('1098051000000103',  'Employment support')
 	,('1098041000000101',  'Referral to an employment support service')
 	,('1082621000000104',  'Individual Placement and Support')
 	,('1082611000000105',  'Referral to an Individual Placement and Support service')
 	,('726052009',  'Carer focused education and support programme')
 	,('1097201000000108',  'Referral for carer focused education and support programme')
 	,('304891004',  'Cognitive behavioural therapy')
 	,('519101000000109',  'Referral for cognitive behavioural therapy')
 	,('1095991000000102',  'At Risk Mental State for Psychosis')
     ,('196771000000101', 'Smoking assessment')
 	,('698094009', 'Measurement of body mass index')
 	,('171222001', 'Hypertension screening')
 	,('43396009',  'Haemoglobin A1c measurement')
 	,('271062006', 'Fasting blood glucose measurement')
 	,('271061004', 'Random blood glucose measurement')
 	,('121868005', 'Total cholesterol measurement')
 	,('17888004',  'High density lipoprotein measurement')
 	,('166842003', 'Total cholesterol: HDL ratio measurement');

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.SCT_Concepts;
 INSERT INTO $db_output.SCT_Concepts
 SELECT *
 FROM $reference_data.snomed_sct2_concept_full

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.SCT_Descriptions;
 INSERT INTO $db_output.SCT_Descriptions
 SELECT *
 FROM $reference_data.snomed_sct2_description_full

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.SCT_Language;
 INSERT INTO $db_output.SCT_Language
 SELECT DSS_KEY,ID, EFFECTIVETIME ,ACTIVE ,MODULEID ,REFSETID ,REFERENCEDCOMPONENTID,ACCEPTABILITY ,DSS_SYSTEM_CREATED_DATE 
 FROM $reference_data.snomed_sct2_language_full

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CONCEPT_SNAP_DMS AS 
  
 SELECT *
 FROM $db_output.SCT_Concepts c1
 WHERE c1.EFFECTIVE_TIME=(SELECT MAX(c2.EFFECTIVE_TIME)
 FROM $db_output.SCT_Concepts c2 where c1.ID=c2.ID)

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW DESCRIPTION_SNAP_DMS AS 
 SELECT *
 FROM $db_output.sct_descriptions c1
 WHERE c1.EFFECTIVE_TIME=(SELECT MAX(c2.EFFECTIVE_TIME)
 FROM $db_output.sct_descriptions c2 where c1.ID=c2.ID)

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW LANGUAGE_SNAP_DMS AS 
 SELECT *
 FROM $db_output.sct_language c1
 WHERE c1.EFFECTIVE_TIME=(SELECT MAX(c2.EFFECTIVE_TIME) FROM $db_output.sct_language c2 where c1.ID=c2.ID)

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW DESCRIPTION_TYPE_DMS AS 
  
 SELECT d.CONCEPT_ID, d.ID, d.TERM,
 CASE 
   WHEN r.ACCEPTABILITY_ID IN ('900000000000548007') AND d.type_id IN ('900000000000003001') THEN 'F'
   WHEN r.ACCEPTABILITY_ID IN ('900000000000548007') AND d.type_id IN ('900000000000013009') THEN 'P'
   WHEN r.ACCEPTABILITY_ID IN ('900000000000549004') AND d.type_id IN ('900000000000013009') THEN 'S'
 ELSE null END AS TYPE
 FROM  global_temp.DESCRIPTION_SNAP_DMS d
 LEFT JOIN global_temp.LANGUAGE_SNAP_DMS r
 ON d.ID=r.REFERENCED_COMPONENT_ID
 WHERE r.REFSET_ID IN ('999001261000000100', '999000691000001104')
 AND r.active=1 

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.SCT_Concepts_FSN;
 INSERT INTO $db_output.SCT_Concepts_FSN
 SELECT a.ID, b.TERM, a.ACTIVE, a.EFFECTIVE_TIME, a.SYSTEM_CREATED_DATE
 FROM GLOBAL_TEMP.CONCEPT_SNAP_DMS a
 JOIN GLOBAL_TEMP.DESCRIPTION_TYPE_DMS b
 on a.ID=b.CONCEPT_ID
 WHERE b.TYPE='F'

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.SCT_Concepts_PrefTerm;
 INSERT INTO $db_output.SCT_Concepts_PrefTerm
 SELECT a.ID, b.TERM, a.ACTIVE, a.EFFECTIVE_TIME, a.SYSTEM_CREATED_DATE
 FROM GLOBAL_TEMP.CONCEPT_SNAP_DMS a
 JOIN GLOBAL_TEMP.DESCRIPTION_TYPE_DMS b
 on a.ID=b.CONCEPT_ID
 WHERE b.TYPE='P'

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AWT_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AWT_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AWT_metric_values'))