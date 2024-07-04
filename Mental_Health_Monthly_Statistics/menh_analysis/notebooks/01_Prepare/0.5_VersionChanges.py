# Databricks notebook source
 %md
 
 # NB this contains all ValidCode lists for measures created in menh_analysis and menh_publications 
 ## if measures are removed from menh_analysis codebase they can be removed from here also  
 
 ###
 - in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 - subsequent changes in future years can use UPDATE statements
 - please ensure table names are added into validcodes and used in join statements with the same Case.

# COMMAND ----------

dbutils.widgets.text("db_output", "menh_analysis", "Target database")

# COMMAND ----------

 %sql
 
 TRUNCATE TABLE $db_output.validcodes

# COMMAND ----------

# DBTITLE 1,INSERT mhs101referral codes INTO $db_output.validcodes
 %sql
 
 -- Values correspondto the following fields:
 -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth
 
 -- NB this contains all ValidCode lists for measures created in menh_anaysis and menh_publications and should be kept in sync 
 
 -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
 -- subsequent changes in future years can use UPDATE statements
 
 INSERT INTO $db_output.validcodes
  
 VALUES ('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '1', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '2', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'ED87_90', 'include', '3', 1390, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'CCR70_72', 'include', '1', 1429, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CCR70_72', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '3', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '1', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '2', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '3', 1390, null)
 ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '4', 1459, null)
  
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'A1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'A2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'A3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'B1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'B2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'C1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'C2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'D1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'E6', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'F1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'F2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'F3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'G4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'H1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'H2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'I1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'I2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'J4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'K5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'L1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'L2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M2', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M4', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M5', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M6', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M7', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'M9', 1459, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'N3', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'P1', 1429, null)
 ,('mhs101referral', 'SourceOfReferralMH', 'MHS32', 'include', 'Q1', 1459, null)

# COMMAND ----------

# DBTITLE 1,INSERT MHS102ServiceTypeReferredTo codes INTO $db_output.validcodes
 %sql
 
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
 
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z01', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z02', 1429, null)
 
 
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A03', 1429, 1458)
 
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A02', 1429, null)
 ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A03', 1429, 1458)

# COMMAND ----------

# DBTITLE 1,INSERT mhs201carecontact codes INTO $db_output.validcodes
 %sql
 
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
 
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '01', 1390, null) -- 01_Prepare/3.CYP_2nd_contact_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '02', 1390, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '03', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '04', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '11', 1459, null)
 
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '05', 1390, null) -- 01_Prepare/3.CYP_2nd_contact_prep
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '06', 1390, 1458)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '09', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '10', 1459, null)
 ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '13', 1459, null)

# COMMAND ----------

# DBTITLE 1,ConsMechanismMH_dim
 %sql
  
 INSERT OVERWRITE TABLE $db_output.ConsMechanismMH_dim
 VALUES 
   ('01', 'Face to face communication', 1429, 1458)
   ,('01', 'Face to face', 1459, null)
   ,('02', 'Telephone', 1429, null)
   ,('03', 'Telemedicine web camera', 1429, 1458)
   ,('04', 'Talk type for a person unable to speak', 1429, null)
   ,('05', 'Email', 1429, null)
   ,('06', 'Short Message Service (SMS) - Text Messaging', 1429, 1458)
   ,('09', 'Text Message (Asynchronous)', 1459, null)
   ,('10', 'Instant messaging (Synchronous)', 1459, null)
   ,('11', 'Video consultation', 1459, null)
   ,('12', 'Message Board (Asynchronous)', 1459, null)
   ,('13', 'Chat Room (Synchronous)', 1459, null)
   ,('98', 'Other', 1429, 1458)
   ,('98', 'Other (not listed)', 1459, null)
   ,('Invalid', 'Invalid', 1429, null)
   ,('Missing', 'Missing', 1429, null)

# COMMAND ----------

# DBTITLE 1,referral_dim
 %sql
  
 INSERT OVERWRITE TABLE $db_output.referral_dim
 VALUES 
 ('A', 'Primary Health Care', 1429, null)
 ,('B', 'Self Referral', 1429, null)
 ,('C', 'Local Authority Services', 1429, null)
 ,('D', 'Employer', 1429, null)
 ,('E', 'Justice System', 1429, null)
 ,('F', 'Child Health', 1429, null)
 ,('G', 'Independent/Voluntary Sector', 1429, null)
 ,('H', 'Acute Secondary Care', 1429, null)
 ,('I', 'Other Mental Health NHS Trust', 1429, 1488)
 ,('I', 'Transfer from another Mental Health Service', 1489, null)
 ,('J', 'Internal referrals  from Community Mental Health Team (within own NHS Trust)', 1429, null)
 ,('K', 'Internal referrals from Inpatient Service (within own NHS Trust)', 1429, null)
 ,('L', 'Transfer by graduation (within own NHS Trust)', 1429, null)
 ,('M', 'Other', 1429, null)
 ,('N', 'Improving access to psychological therapies', 1429, null)
 ,('P', 'Internal', 1429, null)
 ,('Q', 'Drop-in Services', 1459, null)
 ,('Invalid', 'Invalid', 1429, null)
 ,('Missing', 'Missing', 1429, null)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.validcodes
 VALUES
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A05','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A12','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A13','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A09','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','C10','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A16','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A06','1429',null),				
 ('MHS102ServiceTypeReferredTo','ServTeamTypeRefToMH','CAMHS','include','A08','1429',null);

# COMMAND ----------

 %sql
 Truncate table $db_output.delayeddischdim;
 INSERT INTO $db_output.delayeddischdim VALUES
  ('att','04','NHS, excluding housing', 1429, null),
  ('att','05','Social Care, excluding housing', 1429, null),
  ('att','06','Both (NHS and Social Care), excluding housing', 1429, null),
  ('att','07','Housing (including supported/specialist housing)', 1429, 1488),
  
  ('att','UNKNOWN','UNKNOWN', 1429, null),
  ('reason','UNKNOWN','UNKNOWN', 1429, null),
  
  ---v5 delayed discharge reasons
  ('reason','A2','Awaiting care coordinator allocation', 1429, 1488),
  ('reason','B1','Awaiting public funding', 1429, 1488),
  ('reason','C1','Awaiting further non-acute (including community and mental health) NHS care (including intermediate care, rehabilitation services etc)', 1429, 1488),
  ('reason','D1','Awaiting Care Home Without Nursing placement or availability', 1429, 1488),
  ('reason','D2','Awaiting Care Home With Nursing placement or availability', 1429, 1488),
  ('reason','E1','Awaiting care package in own home', 1429, 1488),
  ('reason','F2','Awaiting community equipment, telecare and/or adaptations', 1429, 1488),
  ('reason','G2','Patient or Family choice (reason not stated by patient or family)', 1429, 1488),
  ('reason','G3','Patient or Family choice - Non-acute (including community and mental health) NHS care (including intermediate care, rehabilitation services etc)', 1429, 1488),
  ('reason','G4','Patient or Family choice - Care Home Without Nursing placement', 1429, 1488),
  ('reason','G5','Patient or Family choice - Care Home With Nursing placement', 1429, 1488),
  ('reason','G6','Patient or Family choice - Care package in own home', 1429, 1488),
  ('reason','G7','Patient or Family choice - Community equipment, telecare and/or adaptations', 1429, 1488),
  ('reason','G8','Patient or Family Choice - general needs housing/private landlord acceptance as patient NOT covered by Housing Act/Care Act', 1429, 1488),
  ('reason','G9','Patient or Family choice - Supported accommodation', 1429, 1488),
  ('reason','G10','Patient or Family choice - Emergency accommodation from the Local Authority under the Housing Act', 1429, 1488),
  ('reason','G11','Patient or Family choice - Child or young person awaiting social care or family placement', 1429, 1488),
  ('reason','G12','Patient or Family choice - Ministry of Justice agreement/permission of proposed placement', 1429, 1488),
  ('reason','H1','Disputes', 1429, 1488),
  ('reason','I2','Housing - Awaiting availability of general needs housing/private landlord accommodation acceptance as patient NOT covered by Housing Act and/or Care Act', 1429, 1488),
  ('reason','I3','Housing - Single homeless patients or asylum seekers NOT covered by Care Act', 1429, 1488),
  ('reason','J2','Housing - Awaiting supported accommodation', 1429, 1488),
  ('reason','K2','Housing - Awaiting emergency accommodation from the Local Authority under the Housing Act', 1429, 1488),
  ('reason','L1','Child or young person awaiting social care or family placement', 1429, 1488),
  ('reason','M1','Awaiting Ministry of Justice agreement/permission of proposed placement', 1429, 1488),
  ('reason','N1','Awaiting outcome of legal requirements (mental capacity/mental health legislation)', 1429, 1488),
  ('reason','P1','Awaiting residential special school or college placement or availability', 1459, 1488),
  ('reason','Q1','Lack of local education support', 1459, 1488),
  ('reason','R1','Public safety concern unrelated to clinical treatment need (care team)', 1459, 1488),
  ('reason','R2','Public safety concern unrelated to clinical treatment need (Ministry of Justice)', 1459, 1488),
  ('reason','S1','No lawful community care package available', 1459, 1488),
  ('reason','T1','Lack of health care service provision', 1459, 1488),
  ('reason','T2','Lack of social care support', 1459, 1488),
  ('reason','98','No reason given', 1459, 1488),
  
  ---v6 delayed discharge reasons
 ('reason','01','Awaiting care coordinator allocation', 1489, null),
 ('reason','02','Awaiting allocation of community psychiatrist', 1489, null),
 ('reason','03','Awaiting allocation of social worker', 1489, null),
 ('reason','04','Awaiting public funding or decision from funding panel', 1489, null),
 ('reason','05','Awaiting further community or mental health NHS Services not delivered in an acute setting including intermediate care, rehabilitation services, step down service', 1489, null),
 ('reason','06','Awaiting availability of placement in prison or Immigration Removal Centre', 1489, null),
 ('reason','07','Awaiting availability of placement in care home without nursing', 1489, null),
 ('reason','08','Awaiting availability of placement in care home with nursing', 1489, null),
 ('reason','09','Awaiting commencement of care package in usual or temporary place of residence', 1489, null),
 ('reason','10','Awaiting provision of community equipment and/or adaptations to own home', 1489, null),
 ('reason','11','Patient or Family choice', 1489, null),
 ('reason','12','Disputes relating to responsible commissioner for post-discharge care', 1489, null),
 ('reason','13','Disputes relating to post-discharge care pathway between clinical teams and/or care panels', 1489, null),
 ('reason','14','Housing - awaiting availability of private rented accommodation', 1489, null),
 ('reason','15','Housing - awaiting availability of social rented housing via council housing waiting list', 1489, null),
 ('reason','16','Housing - awaiting purchase/sourcing of own home', 1489, null),
 ('reason','17','Housing - Patient NOT eligible for funded care or support', 1489, null),
 ('reason','18','Housing - Awaiting supported accommodation', 1489, null),
 ('reason','19','Housing - Awaiting temporary accommodation from the Local Authority under housing legislation', 1489, null),
 ('reason','20',"Awaiting availability of residential children's home (non-secure)", 1489, null),
 ('reason','21',"Awaiting availability of secure children's home (welfare or non-welfare)", 1489, null),
 ('reason','22','Awaiting availability of placement in Youth Offender Institution', 1489, null),
 ('reason','23','Child or young person awaiting foster placement', 1489, null),
 ('reason','24','Awaiting Ministry of Justice agreement to proposed placement', 1489, null),
 ('reason','25','Awaiting outcome of legal proceedings under relevant Mental Health legislation', 1489, null),
 ('reason','26','Awaiting Court of Protection proceedings', 1489, null),
 ('reason','27','Awaiting Deprivation of Liberty Safeguards (DOLS) Application', 1489, null),
 ('reason','28','Delay due to consideration of specific court judgements', 1489, null),
 ('reason','29','Awaiting residential special school or college placement', 1489, null),
 ('reason','30','Lack of local education support', 1489, null),
 ('reason','31','Public safety concern unrelated to clinical treatment need (care team and/or ministry of justice)', 1489, null),
 ('reason','32','Highly bespoke housing and/or care arrangements not available in the community', 1489, null),
 ('reason','33','No lawful support available in the community excluding social care', 1489, null),
 ('reason','34','No social care support including social care funded placement', 1489, null),
 ('reason','35','Delays to NHS-led assessments in the community', 1489, null),
 ('reason','36','Hospital staff shortages', 1489, null),
 ('reason','37','Delays to non-NHS led assessments in the community', 1489, null),
 ('reason','98','Reason not known', 1489, null)