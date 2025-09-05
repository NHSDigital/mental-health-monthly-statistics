# Databricks notebook source
# DBTITLE 1,Create table DETENTIONS_monthly
 %sql
 DROP TABLE IF EXISTS $db_output.DETENTIONS_monthly;

 CREATE TABLE IF NOT EXISTS $db_output.DETENTIONS_monthly
 (
     Person_ID string
     ,MHA_RecordNumber string
     ,UniqMHActEpisodeID string
     ,orgidProv string
     ,Provider_Name string
     ,StartDateMHActLegalStatusClass date
     ,StartTimeMHActLegalStatusClass timestamp
     ,ExpiryDateMHActLegalStatusClass date
     ,EndDateMHActLegalStatusClass timestamp
     ,LegalStatusCode string
     ,MHA_RANK int
     ,HOSP_RANK int
     ,UniqHospProvSpellNum string
     ,HOSP_ADM_RANK int
     ,PrevDischDestCodeHospProvSpell string
     ,AdmMethCodeHospProvSpell string
     ,StartDateHospProvSpell date
     ,StartTimeHospProvSpell timestamp
     ,DischDateHospProvSpell date
     ,DischTimeHospProvSpell timestamp
     ,Detention_Cat string
     ,Detention_DateTime_Cat string 
     ,PrevUniqMHActEpisodeID string
     ,PrevRecordNumber string
     ,PrevLegalStatus string
     ,PrevMHAStartDate date
     ,PrevMHAEndDate date
     ,MHS404UniqID string
     ,CTORecordNumber string
     ,StartDateCommTreatOrd date
     ,EndDateCommTreatOrd date 
     ,CommTreatOrdEndReason string
     ,MHA_Logic_Cat_full string
     ,AgeRepPeriodEnd string
     ,Age_Band string
     ,Age_Group_Higher_Level string
     ,UpperEthnicity string
     ,LowerEthnicityCode string
     ,LowerEthnicityName string
     ,Der_Gender string
     ,Der_Gender_Desc string
     ,IMD_Decile string
     ,CCG_code string
     ,CCG_NAME string
     ,STP_CODE string
     ,STP_NAME string
     ,Region_Code string
     ,Region_Name string
     ,AutismStatus string
     ,AutismStatus_desc string
     ,LDStatus string
     ,LDStatus_desc string
 )
 USING DELTA

# COMMAND ----------

# DBTITLE 1,Create table SHORT_TERM_ORDERS_MONTHLY
 %sql
 DROP TABLE IF EXISTS $db_output.SHORT_TERM_ORDERS_MONTHLY;

 CREATE TABLE IF NOT EXISTS $db_output.SHORT_TERM_ORDERS_MONTHLY
 (
     Person_ID string
     ,MHA_RecordNumber string
     ,UniqMHActEpisodeID string
     ,orgidProv string
     ,Provider_Name string
     ,StartDateMHActLegalStatusClass date
     ,StartTimeMHActLegalStatusClass timestamp
     ,ExpiryDateMHActLegalStatusClass date
     ,EndDateMHActLegalStatusClass timestamp
     ,LegalStatusCode string
     ,MHA_RANK int
     ,HOSP_RANK int
     ,UniqHospProvSpellNum string
     ,HOSP_ADM_RANK int
     ,PrevDischDestCodeHospProvSpell string
     ,AdmMethCodeHospProvSpell string
     ,StartDateHospProvSpell date
     ,StartTimeHospProvSpell timestamp
     ,DischDateHospProvSpell date
     ,DischTimeHospProvSpell timestamp
     ,Detention_Cat string
     ,Detention_DateTime_Cat string 
     ,PrevUniqMHActEpisodeID string
     ,PrevRecordNumber string
     ,PrevLegalStatus string
     ,PrevMHAStartDate date
     ,PrevMHAEndDate date
     ,MHS404UniqID string
     ,CTORecordNumber string
     ,StartDateCommTreatOrd date
     ,EndDateCommTreatOrd date 
     ,CommTreatOrdEndReason string
     ,MHA_Logic_Cat_full string
     ,AgeRepPeriodEnd string
     ,Age_Band string
     ,Age_Group_Higher_Level string
     ,UpperEthnicity string
     ,LowerEthnicityCode string
     ,LowerEthnicityName string
     ,Der_Gender string
     ,Der_Gender_Desc string
     ,IMD_Decile string
     ,CCG_code string
     ,CCG_NAME string
     ,STP_CODE string
     ,STP_NAME string
     ,Region_Code string
     ,Region_Name string
     ,AutismStatus string
     ,AutismStatus_desc string
     ,LDStatus string
     ,LDStatus_desc string
 )
 USING DELTA

# COMMAND ----------

# DBTITLE 1,Create table CTO_MONTHLY
 %sql
 DROP TABLE IF EXISTS $db_output.CTO_MONTHLY;

 CREATE TABLE IF NOT EXISTS $db_output.CTO_MONTHLY
 (
     Person_ID STRING,
     CTO_UniqMHActEpisodeID STRING,
     MHS404UniqID STRING,
     orgidProv STRING,
     Provider_Name STRING,
     StartDateCommTreatOrd DATE,
     ExpiryDateCommTreatOrd DATE,
     EndDateCommTreatOrd DATE,
     CommTreatOrdEndReason STRING,
     MHA_UniqMHActEpisodeID STRING,
     LegalStatusCode STRING,
     StartDateMHActLegalStatusClass DATE,
     ExpiryDateMHActLegalStatusClass DATE,
     EndDateMHActLegalStatusClass DATE,
     AgeRepPeriodEnd string,
     Age_Band string,
     Age_Group_Higher_Level string,
     UpperEthnicity string,
     LowerEthnicityCode string,
     LowerEthnicityName string,
     Der_Gender string,
     Der_Gender_Desc string,
     IMD_Decile string,
     CCG_code string,
     CCG_NAME string,
     STP_CODE string,
     STP_NAME string,
     Region_Code string,
     Region_Name string,
     AutismStatus string,
     AutismStatus_desc string,
     LDStatus string,
     LDStatus_desc string
 )
 USING DELTA

# COMMAND ----------

# DBTITLE 1,Create table DETENTIONS_ENDED
 %sql
 DROP TABLE IF EXISTS $db_output.DETENTIONS_ENDED;

 CREATE TABLE IF NOT EXISTS $db_output.DETENTIONS_ENDED
 (
     Person_ID string
     ,UniqMHActEpisodeID string
     ,orgidProv string
     ,Provider_Name string
     ,LegalStatusCode string
     ,MHA_Logic_Cat_full string
     ,AgeRepPeriodEnd string
     ,Age_Band string
     ,Age_Group_Higher_Level string
     ,UpperEthnicity string
     ,LowerEthnicityCode string
     ,LowerEthnicityName string
     ,Der_Gender string
     ,Der_Gender_Desc string
     ,IMD_Decile string
     ,CCG_code string
     ,CCG_NAME string
     ,STP_CODE string
     ,STP_NAME string
     ,Region_Code string
     ,Region_Name string
     ,AutismStatus string
     ,AutismStatus_desc string
     ,LDStatus string
     ,LDStatus_desc string
     ,Detention_days int
 )
 USING DELTA

# COMMAND ----------

# DBTITLE 1,Create Table DETENTIONS_ACTIVE
 %sql
 DROP TABLE IF EXISTS $db_output.DETENTIONS_ACTIVE;

 CREATE TABLE IF NOT EXISTS $db_output.DETENTIONS_ACTIVE
 (
     Person_ID string
     ,UniqMHActEpisodeID string
     ,orgidProv string
     ,Provider_Name string
     ,LegalStatusCode string
     ,MHA_Logic_Cat_full string
     ,AgeRepPeriodEnd string
     ,Age_Band string
     ,Age_Group_Higher_Level string
     ,UpperEthnicity string
     ,LowerEthnicityCode string
     ,LowerEthnicityName string
     ,Der_Gender string
     ,Der_Gender_Desc string
     ,IMD_Decile string
     ,CCG_code string
     ,CCG_NAME string
     ,STP_CODE string
     ,STP_NAME string
     ,Region_Code string
     ,Region_Name string
     ,AutismStatus string
     ,AutismStatus_desc string
     ,LDStatus string
     ,LDStatus_desc string
     ,Detention_days int
 )
 USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DETENTIONS_YEARLY;
  
 CREATE TABLE IF NOT EXISTS $db_output.DETENTIONS_YEARLY
 (
     Person_ID string
     ,MHA_RecordNumber string
     ,UniqMHActEpisodeID string
     ,orgidProv string
     ,Provider_Name string
     ,StartDateMHActLegalStatusClass date
     ,StartTimeMHActLegalStatusClass timestamp
     ,ExpiryDateMHActLegalStatusClass date
     ,EndDateMHActLegalStatusClass timestamp
     ,LegalStatusCode string
     ,MHA_RANK int
     ,HOSP_RANK int
     ,UniqHospProvSpellNum string
     ,HOSP_ADM_RANK int
     ,PrevDischDestCodeHospProvSpell string
     ,AdmMethCodeHospProvSpell string
     ,StartDateHospProvSpell date
     ,StartTimeHospProvSpell timestamp
     ,DischDateHospProvSpell date
     ,DischTimeHospProvSpell timestamp
     ,Detention_Cat string
     ,Detention_DateTime_Cat string 
     ,PrevUniqMHActEpisodeID string
     ,PrevRecordNumber string
     ,PrevLegalStatus string
     ,PrevMHAStartDate date
     ,PrevMHAEndDate date
     ,MHS404UniqID string
     ,CTORecordNumber string
     ,StartDateCommTreatOrd date
     ,EndDateCommTreatOrd date 
     ,CommTreatOrdEndReason string
     ,MHA_Logic_Cat_full string
     ,AgeRepPeriodEnd string
     ,Age_Band string
     ,Age_Group_Higher_Level string
     ,UpperEthnicity string
     ,LowerEthnicityCode string
     ,LowerEthnicityName string
     ,WNW_Ethnicity string
     ,Der_Gender string
     ,Der_Gender_Desc string
     ,IMD_Decile string
     ,IMD_Core20 string
     ,CCG_code string
     ,CCG_NAME string
     ,STP_CODE string
     ,STP_NAME string
     ,Region_Code string
     ,Region_Name string
     ,AutismStatus string
     ,AutismStatus_desc string
     ,LDStatus string
     ,LDStatus_desc string
 )
 USING DELTA

# COMMAND ----------

  %sql
  DROP TABLE IF EXISTS $db_output.DETENTIONS_YEARLY_FILTERED;

  CREATE TABLE IF NOT EXISTS $db_output.DETENTIONS_YEARLY_FILTERED
  (
      Person_ID string
      ,MHA_RecordNumber string
      ,UniqMHActEpisodeID string
      ,orgidProv string
      ,Provider_Name string
      ,StartDateMHActLegalStatusClass date
      ,StartTimeMHActLegalStatusClass timestamp
      ,ExpiryDateMHActLegalStatusClass date
      ,EndDateMHActLegalStatusClass timestamp
      ,LegalStatusCode string
      ,MHA_RANK int
      ,HOSP_RANK int
      ,UniqHospProvSpellNum string
      ,HOSP_ADM_RANK int
      ,PrevDischDestCodeHospProvSpell string
      ,AdmMethCodeHospProvSpell string
      ,StartDateHospProvSpell date
      ,StartTimeHospProvSpell timestamp
      ,DischDateHospProvSpell date
      ,DischTimeHospProvSpell timestamp
      ,Detention_Cat string
      ,Detention_DateTime_Cat string 
      ,PrevUniqMHActEpisodeID string
      ,PrevRecordNumber string
      ,PrevLegalStatus string
      ,PrevMHAStartDate date
      ,PrevMHAEndDate date
      ,MHS404UniqID string
      ,CTORecordNumber string
      ,StartDateCommTreatOrd date
      ,EndDateCommTreatOrd date 
      ,CommTreatOrdEndReason string
      ,MHA_Logic_Cat_full string
      ,AgeRepPeriodEnd string
      ,Age_Band string
      ,Age_Group_Higher_Level string
      ,UpperEthnicity string
      ,LowerEthnicityCode string
      ,LowerEthnicityName string
      ,WNW_Ethnicity string
      ,Der_Gender string
      ,Der_Gender_Desc string
      ,IMD_Decile string
      ,IMD_Core20 string
      ,CCG_code string
      ,CCG_NAME string
      ,STP_CODE string
      ,STP_NAME string
      ,Region_Code string
      ,Region_Name string
      ,AutismStatus string
      ,AutismStatus_desc string
      ,LDStatus string
      ,LDStatus_desc string
  )
  USING DELTA


# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.CTO_YEARLY;
  
 CREATE TABLE IF NOT EXISTS $db_output.CTO_YEARLY
 (
     Person_ID STRING,
     CTO_UniqMHActEpisodeID STRING,
     MHS404UniqID STRING,
     orgidProv STRING,
     Provider_Name STRING,
     StartDateCommTreatOrd DATE,
     ExpiryDateCommTreatOrd DATE,
     EndDateCommTreatOrd DATE,
     CommTreatOrdEndReason STRING,
     MHA_UniqMHActEpisodeID STRING,
     LegalStatusCode STRING,
     StartDateMHActLegalStatusClass DATE,
     ExpiryDateMHActLegalStatusClass DATE,
     EndDateMHActLegalStatusClass DATE,
     AgeRepPeriodEnd string,
     Age_Band string,
     Age_Group_Higher_Level string,
     UpperEthnicity string,
     LowerEthnicityCode string,
     LowerEthnicityName string,
     WNW_Ethnicity string,
     Der_Gender string,
     Der_Gender_Desc string,
     IMD_Decile string,
     IMD_Core20 string,
     CCG_code string,
     CCG_NAME string,
     STP_CODE string,
     STP_NAME string,
     Region_Code string,
     Region_Name string,
     AutismStatus string,
     AutismStatus_desc string,
     LDStatus string,
     LDStatus_desc string
 )
 USING DELTA