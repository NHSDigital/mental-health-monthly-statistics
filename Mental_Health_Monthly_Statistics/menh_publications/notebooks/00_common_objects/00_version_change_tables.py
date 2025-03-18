# Databricks notebook source
 %md

 # NB this contains all ValidCode lists for measures created in menh_analysis and menh_publications 
 ## if measures are removed from menh_analysis codebase they can be removed from here also  

 ###
 - in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 - subsequent changes in future years can use UPDATE statements
 - please ensure table names are added into validcodes and used in join statements with the same Case.

# COMMAND ----------

 %md

 # tables populated in this notebook

 - validcodes
 - referral_dim

# COMMAND ----------

 %sql

 TRUNCATE TABLE $db_output.validcodes

# COMMAND ----------

# DBTITLE 1,INSERT mhs101referral codes INTO $db_output.validcodes
 %sql

 -- Table below contains the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth

 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 

 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements

 INSERT INTO $db_output.validcodes
 VALUES ('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '1', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '2', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '4', 1459, null)

 ,('mhs101referral', 'ClinRespPriorityType', 'ED87_90', 'include', '3', 1390, null)

 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '3', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '4', 1459, null)

 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '1', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '2', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '3', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '4', 1459, null)

 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'A1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'A2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'A3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'B1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'B2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'C1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'C2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'D1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E6', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'F1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'F2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'F3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'H1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'H2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'I1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'I2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'L1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'L2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M6', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M7', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M9', 1459, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'N3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'P1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'Q1', 1459, null)


# COMMAND ----------

# DBTITLE 1,INSERT MHS102ServiceTypeReferredTo codes INTO $db_output.validcodes
 %sql

 -- Table below contains the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth

 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 

 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements

 INSERT INTO $db_output.validcodes
 VALUES ('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A03', 1429, 1458)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A04', 1429, 1458)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A05', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A06', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A07', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A08', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A09', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A10', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A11', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A12', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A13', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A14', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A15', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A16', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A17', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A18', 1429, null) 

 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A21', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A22', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A23', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A24', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A25', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'B01', 1429, null)

 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C04', 1429, null)

 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C08', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C10', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D03', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D04', 1429, null)

 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D06', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D07', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D08', 1429, null)

 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F01', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F02', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F03', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F04', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F05', 1459, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F06', 1459, null)
 --,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F07', 1489, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z02', 1429, null)


 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A03', 1429, 1458)

 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A03', 1429, 1458)

# COMMAND ----------

# DBTITLE 1,INSERT mhs201carecontact codes INTO $db_output.validcodes
 %sql

 -- Table below contains the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth

 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 

 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements

 INSERT INTO $db_output.validcodes
 VALUES ('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '01', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '11', 1459, null)

 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '01', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '04', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '11', 1459, null)

 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '05', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '06', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '09', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '10', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '12', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '13', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '98', 1459, null)

 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '01', 1390, null) -- 01_Prepare/2.AWT_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '04', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '11', 1459, null)

 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '05', 1390, null) -- 01_Prepare/3.CYP_2nd_contact_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '06', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '09', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '10', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '12', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '13', 1459, null)

 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '01', 1390, null) -- /menh_publications/notebooks/06_CYP_Outcome_Measures/01_Prepare/PrepViews
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '04', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '11', 1459, null)

 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '05', 1390, null) -- /menh_publications/notebooks/06_CYP_Outcome_Measures/01_Prepare/PrepViews
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '06', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '09', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '10', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '13', 1459, null)

# COMMAND ----------

 %sql
 --- V6_Changes
 -- Table below contains the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth

 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 

 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements

 INSERT INTO $db_output.validcodes
 VALUES ('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '51484002', 1390, null)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '1111811000000109', 1390, null)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '443730003', 1390, null)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '444175001', 1390, null)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '718023002', 1390, 1488)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '984421000000104', 1390, 1488)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '1323681000000103', 1477, null)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '1362001000000104', 1477, null)
 ,('MHS202CareActivity', 'Procedure', 'CYP_ED_WaitingTimes', 'include', '301781000000101', 1477, null)

# COMMAND ----------

# DBTITLE 1,INSERT mhs501hospprovspell codes INTO $db_output.validcodes
 %sql

 -- Table below contains the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth

 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements

 INSERT INTO $db_output.validcodes
 VALUES 
 ('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '30', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '37', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '38', 1390, 1458)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '40', 1459, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '42', 1459, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '48', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '49', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '50', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '53', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '79', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '84', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '87', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '89', 1459, null)

 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '37', 1390, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '38', 1390, 1458)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '40', 1459, null)
 ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '42', 1459, null)


# COMMAND ----------

# DBTITLE 1,referral_dim
 %sql

 INSERT OVERWRITE TABLE $db_output.referral_dim
 VALUES ('A', 'Primary Health Care', 1429, null)
 ,('B', 'Self Referral', 1429, null)
 ,('C', 'Local Authority Services', 1429, null)
 ,('D', 'Employer', 1429, null)
 ,('E', 'Justice System', 1429, null)
 ,('F', 'Child Health', 1429, null)
 ,('G', 'Independent/Voluntary Sector', 1429, null)
 ,('H', 'Acute Secondary Care', 1429, null)
 ,('I', 'Other Mental Health NHS Trust', 1429, null)
 ,('J', 'Internal referrals  from Community Mental Health Team (within own NHS Trust)', 1429, null)
 ,('K', 'Internal referrals from Inpatient Service (within own NHS Trust)', 1429, null)
 ,('L', 'Transfer by graduation (within own NHS Trust)', 1429, null)
 ,('M', 'Other', 1429, null)
 ,('N', 'Improving access to psychological  therapies', 1429, null)
 ,('P', 'Internal', 1429, null)
 ,('Q', 'Drop-in Services', 1459, null)