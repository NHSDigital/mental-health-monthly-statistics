# Databricks notebook source
# DBTITLE 1,Get latest Autism Status
 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS005PatInd_AUT AS
  
 SELECT DISTINCT A.PERSON_ID, A.UNIQMONTHID, A.AutismStatus
 FROM $db_source.MHS005PatInd  A
 INNER JOIN (SELECT PERSON_ID, UNIQMONTHID, RecordNumber, AutismStatus, DENSE_RANK() OVER (PARTITION BY PERSON_ID ORDER BY UNIQMONTHID DESC, RecordNumber DESC) AS AUT_RANK
             FROM $db_source.MHS005PatInd 
             WHERE UniqMonthID between $end_month_id - 11 AND $end_month_id 
             and AutismStatus in ('1', '2', '3', '4', '5', 'U', 'X', 'Z')) B ON B.AUT_RANK = 1 AND A.PERSON_ID = B.PERSON_ID AND A.UNIQMONTHID = B.UNIQMONTHID AND  A.RecordNumber = B.RecordNumber

# COMMAND ----------

# DBTITLE 1,Get latest LD Status
 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS005PatInd_LD AS
  
 SELECT DISTINCT A.PERSON_ID, A.UNIQMONTHID, A.LDStatus
 FROM $db_source.MHS005PatInd  A
 INNER JOIN (SELECT PERSON_ID, UNIQMONTHID, RecordNumber, LDStatus, DENSE_RANK() OVER (PARTITION BY PERSON_ID ORDER BY UNIQMONTHID DESC, RecordNumber DESC) AS AUT_RANK
             FROM $db_source.MHS005PatInd 
             WHERE UniqMonthID between $end_month_id - 11 AND $end_month_id 
             and LDStatus in ('1', '2', '3', '4', '5', 'U', 'X', 'Z')) B ON B.AUT_RANK = 1 AND A.PERSON_ID = B.PERSON_ID AND A.UNIQMONTHID = B.UNIQMONTHID AND  A.RecordNumber = B.RecordNumber

# COMMAND ----------

# DBTITLE 1,Create MHA_MHS001MPI_LATEST
 %sql
 -- Get latest MPI records for the year.

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS001MPI_LATEST AS

 SELECT			 
 				 B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender
                 ,coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Gender_Description
 				,B.NHSDEthnicity as LowerEthnicityCode
                 ,coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName
                 ,coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity
                 ,B.AgeRepPeriodEnd
                 ,coalesce(AB.Age_Group_MHA, "UNKNOWN")  as Age_Band
                 ,coalesce(AB.Age_Group_Higher_Level, "UNKNOWN")  as Age_Group_Higher_Level
                 ,C.DECI_IMD
                 ,coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile
                 ,A.IC_REC_CCG
                 ,A.NAME
                 ,COALESCE(c.STP21CDH,'UNKNOWN') AS STP_CODE
                 ,COALESCE(c.STP21NM,'UNKNOWN') AS STP_NAME
                 ,COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code
                 ,COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name
                 ,coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus
                 ,coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc
                 ,coalesce(ld.LDStatus, "UNKNOWN") as LDStatus
                 ,coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc
 FROM			$db_source.MHS001MPI AS B                                     

 LEFT JOIN
          (SELECT LSOA_CODE_2011, DECI_IMD, IMD_YEAR
          FROM $reference_data.english_indices_of_dep_v02
          WHERE IMD_YEAR = (SELECT MAX(IMD_YEAR) FROM $reference_data.english_indices_of_dep_v02) ) C ON B.LSOA2011 = LSOA_CODE_2011     
          
 --LEFT JOIN $db_output.CCG A ON A.PERSON_ID = b.PERSON_ID          ##MHA-MIGRATION
 LEFT JOIN $db_output.ethnicity_desc eth on B.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
 LEFT JOIN $db_output.imd_desc imd on C.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
 LEFT JOIN $db_output.age_band_desc AB on B.AgeRepPeriodEnd = AB.AgeRepPeriodEnd and '$end_month_id' >= AB.FirstMonth and (AB.LastMonth is null or '$end_month_id' <= AB.LastMonth)
 LEFT JOIN $db_output.gender_desc gen on CASE WHEN B.GenderIDCode IN ('1','2','3','4') THEN B.GenderIDCode
                                              WHEN B.Gender IN ('1','2','9') THEN B.Gender
                                              ELSE 'UNKNOWN' END = gen.Der_Gender 
                                         and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
 LEFT JOIN $db_output.bbrb_ccg_in_month A ON A.PERSON_ID = b.PERSON_ID

 LEFT JOIN $db_output.ccg_mapping_2021 c on A.IC_Rec_CCG = c.CCG21CDH 

 --Autsim and LD Status
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on B.PERSON_ID = aut.PERSON_ID
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on B.PERSON_ID = ld.PERSON_ID
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)
 ----
      
 INNER JOIN (SELECT PERSON_ID, MAX(UNIQMONTHID) AS UNIQMONTHID FROM $db_source.MHS001MPI WHERE (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12) AND PatMRecInRP = True GROUP BY PERSON_ID) AS MPI on b.person_id = mpi.person_id and b.uniqmonthid =mpi.uniqmonthid  

 WHERE (b.RecordEndDate is null or b.RecordEndDate >= '$rp_enddate') and b.RecordStartDate <= '$rp_enddate' AND b.RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)
      AND PatMRecInRP = True
     

# COMMAND ----------

 %sql
 -- Get latest MPI records for the year.
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS001MPI_LATEST_IN_YEAR AS
  
 SELECT             
                  B.UniqMonthID
                 ,B.orgidProv
                 ,B.Person_ID
                 ,coalesce(gen.Der_Gender, "UNKNOWN") AS Der_Gender
                 ,coalesce(gen.Der_Gender_Desc, "UNKNOWN") as Gender_Description
                 ,B.NHSDEthnicity as LowerEthnicityCode
                 ,coalesce(eth.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName
                 ,coalesce(eth.UpperEthnicity, "UNKNOWN") as UpperEthnicity
                 ,CASE WHEN B.NHSDEthnicity NOT IN ('A','99','-1') THEN 1 ELSE 0 END as NotWhiteBritish 
                 ,CASE WHEN B.NHSDEthnicity = 'A' THEN 1 ELSE 0 END as WhiteBritish
                 ,B.AgeRepPeriodEnd
                 ,coalesce(AB.Age_Group_MHA, "UNKNOWN")  as Age_Band
                 ,coalesce(AB.Age_Group_Higher_Level, "UNKNOWN")  as Age_Group_Higher_Level
                 ,D.DECI_IMD
                 ,coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile   
                 ,coalesce(imd.IMD_Core20, "UNKNOWN") as IMD_Core20
                 ,COALESCE(stp.CCG_Code, 'UNKNOWN') as CCG_Code
                 ,COALESCE(stp.CCG_Name, 'UNKNOWN') as CCG_Name
                 ,COALESCE(stp.STP_Code, 'UNKNOWN') as STP_CODE
                 ,COALESCE(stp.STP_Name, 'UNKNOWN') as STP_NAME
                 ,COALESCE(stp.Region_Code, 'UNKNOWN') as Region_Code
                 ,COALESCE(stp.Region_Name, 'UNKNOWN') as Region_Name
                 ,coalesce(aut.AutismStatus, "UNKNOWN") as AutismStatus
                 ,coalesce(aut1.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc
                 ,coalesce(ld.LDStatus, "UNKNOWN") as LDStatus
                 ,coalesce(ld1.LDStatus_desc, "UNKNOWN") as LDStatus_desc
 FROM            $db_source.MHS001MPI AS B                                     
  
 LEFT JOIN
          (SELECT LSOA_CODE_2011, DECI_IMD, IMD_YEAR
          FROM $reference_data.english_indices_of_dep_v02
          WHERE IMD_YEAR = (SELECT MAX(IMD_YEAR) FROM $reference_data.english_indices_of_dep_v02) ) D ON B.LSOA2011 = LSOA_CODE_2011     
          
 LEFT JOIN $db_output.ethnicity_desc eth on B.NHSDEthnicity = eth.LowerEthnicityCode and '$end_month_id' >= eth.FirstMonth and (eth.LastMonth is null or '$end_month_id' <= eth.LastMonth)
 LEFT JOIN $db_output.imd_desc imd on D.DECI_IMD = imd.IMD_Number and '$end_month_id' >= imd.FirstMonth and (imd.LastMonth is null or '$end_month_id' <= imd.LastMonth)
 LEFT JOIN $db_output.age_band_desc AB on B.AgeRepPeriodEnd = AB.AgeRepPeriodEnd and '$end_month_id' >= AB.FirstMonth and (AB.LastMonth is null or '$end_month_id' <= AB.LastMonth)
 LEFT JOIN $db_output.gender_desc gen on CASE WHEN B.GenderIDCode IN ('1','2','3','4') THEN B.GenderIDCode
                                              WHEN B.Gender IN ('1','2','9') THEN B.Gender
                                              ELSE 'UNKNOWN' END = gen.Der_Gender 
                                         and '$end_month_id' >= gen.FirstMonth and (gen.LastMonth is null or '$end_month_id' <= gen.LastMonth)
 LEFT JOIN $db_output.bbrb_ccg_in_year ccg ON ccg.PERSON_ID = B.PERSON_ID   
  
 --LEFT JOIN $db_output.ccg_mapping_2021 c on A.IC_Rec_CCG = c.CCG21CDH 
  
 left join $db_output.bbrb_stp_mapping stp  on ccg.SubICBGPRes = stp.CCG_Code
  
 --Autsim and LD Status
 LEFT JOIN global_temp.MHS005PatInd_AUT aut on B.PERSON_ID = aut.PERSON_ID
 LEFT JOIN $db_output.autism_status_desc aut1 on aut.AutismStatus = aut1.AutismStatus and '$end_month_id' >= aut1.FirstMonth and (aut1.LastMonth is null or '$end_month_id' <= aut1.LastMonth)
  
 LEFT JOIN global_temp.MHS005PatInd_LD ld on B.PERSON_ID = ld.PERSON_ID
 LEFT JOIN $db_output.ld_status_desc ld1 on ld.LDStatus = ld1.LDStatus and '$end_month_id' >= ld1.FirstMonth and (ld1.LastMonth is null or '$end_month_id' <= ld1.LastMonth)
 ----
      
 INNER JOIN (SELECT PERSON_ID, MAX(UNIQMONTHID) AS UNIQMONTHID FROM $db_source.MHS001MPI WHERE (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12) AND PatMRecInRP = True GROUP BY PERSON_ID) AS MPI on b.person_id = mpi.person_id and b.uniqmonthid =mpi.uniqmonthid  
  
 WHERE (b.RecordEndDate is null or b.RecordEndDate >= '$rp_enddate') and b.RecordStartDate <= '$rp_enddate' AND b.RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)
      AND PatMRecInRP = True
     

# COMMAND ----------

# DBTITLE 1,Create MHA_MHS401MHA_Latest
 %sql
 -- Get latest MHA records for the year.

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS401MHA_Latest AS

 SELECT			 
 				B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateMHActLegalStatusClass
 				,B.StartTimeMHActLegalStatusClass
 				,B.ExpiryDateMHActLegalStatusClass
 				,B.ExpiryTimeMHActLegalStatusClass
 				,B.EndDateMHActLegalStatusClass
 				,B.EndTimeMHActLegalStatusClass
 				,B.LegalStatusCode
 				,B.RecordStartDate
 				,B.RecordNumber

 FROM			$db_source.MHS401MHActPeriod
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)
 				

# COMMAND ----------

# DBTITLE 1,Create MHA_MHS403_CD_Latest
 %sql
 CREATE OR REPLACE TEMPORARY  VIEW MHA_MHS403_CD_Latest AS 

 SELECT			 B.MHS403UniqID
 				,B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateMHCondDisch
 				,B.EndDateMHCondDisch
 				,B.CondDischEndReason
 				,B.AbsDischResp
 				,B.RecordStartDate
 				,B.RecordNumber

 FROM			$db_source.MHS403ConditionalDischarge
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)
 				

# COMMAND ----------

# DBTITLE 1,Create MHA_MHS404CTO_Latest
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS404CTO_Latest AS

 SELECT			 B.MHS404UniqID
 				,B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateCommTreatOrd
 				,B.ExpiryDateCommTreatOrd
 				,B.EndDateCommTreatOrd
 				,B.CommTreatOrdEndReason
 				,B.RecordStartDate
 				,B.RecordNumber

 FROM			$db_source.MHS404CommTreatOrder	AS B
 where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)


# COMMAND ----------

# DBTITLE 1,Create MHA_MHS404CTO_Latest_Ranked
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS404CTO_Latest_Ranked AS

 SELECT *
 FROM
     (SELECT *
         , dense_rank() over (partition by Person_ID, orgidProv, StartDateCommTreatOrd 
                                 order by RecordStartDate DESC, CASE WHEN EndDateCommTreatOrd is not null then 2 else 1 end DESC, EndDateCommTreatOrd DESC, MHS404UniqID DESC
                              ) AS CTO_DUP_RANK
                              
     FROM global_temp.MHA_MHS404CTO_Latest) a
 where CTO_DUP_RANK = '1'
 order by Person_ID, StartDateCommTreatOrd


# COMMAND ----------

# DBTITLE 1,Create MHA_MHS405CTO_Recall_Latest
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS405CTO_Recall_Latest AS

 SELECT			 B.MHS405UniqID
 				,B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqMHActEpisodeID
 				,B.StartDateCommTreatOrdRecall
 				,B.EndDateCommTreatOrdRecall
 				,B.RecordStartDate
 				,B.RecordNumber

 FROM			$db_source.MHS405CommTreatOrderRecall
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)


# COMMAND ----------

# DBTITLE 1,Create MHA_Revoked_CTO
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_Revoked_CTO AS

 SELECT			B.Person_ID,
 				b.UniqMHActEpisodeID,
 				B.MHS404UniqID,
 				B.StartDateCommTreatOrd,
 				B.EndDateCommTreatOrd,
 				B.CommTreatOrdEndReason,
 				a.LegalStatusCode,
 				A.StartDateMHActLegalStatusClass,
 				A.EndDateMHActLegalStatusClass,
 				B.recordnumber

 FROM			global_temp.MHA_MHS404CTO_Latest_Ranked	AS B
   LEFT JOIN $db_source.MHS401MHActPeriod AS A 
             ON B.UniqMHActEpisodeID = A.UniqMHActEpisodeID 
                 AND (a.RecordEndDate is null or a.RecordEndDate >= '$rp_enddate') 
                 AND a.RecordStartDate <= '$rp_enddate' 
                 AND a.RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)
 				
 WHERE B.CommTreatOrdEndReason = '02'


# COMMAND ----------

# DBTITLE 1,Create MHA_MHS501HospSpell_Latest
 %sql

 -- Get latest Hospital Spell records for the year.

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS501HospSpell_Latest AS

 SELECT			 
 				 B.UniqMonthID
 				,B.orgidProv
 				,B.Person_ID
 				,B.UniqHospProvSpellID
 				,B.StartDateHospProvSpell
 				,B.StartTimeHospProvSpell
 				,B.DischDateHospProvSpell
 				,B.DischTimeHospProvSpell
 				,B.SourceAdmMHHospProvSpell
 				,B.MethAdmMHHospProvSpell
 				,B.DestOfDischHospProvSpell
 				,B.MethOfDischMHHospProvSpell

 FROM			$db_source.MHS501HospProvSpell
 					AS B
 				where (RecordEndDate is null or RecordEndDate >= '$rp_enddate') and RecordStartDate <= '$rp_enddate' AND RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)

# COMMAND ----------

# DBTITLE 1,Create MHA_MHS501_Ranked
 %sql
 -- Rank hospital spells in order. This is done per patient. 
 -- This means that for each patient their hospital spells will be ranked into chronological order. This allows us to get the Previous Discharge Destination for a hospital spell which is used later.

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS501_Ranked AS

 SELECT * 
   ,  dense_rank() over (partition by Person_ID order by StartDateHospProvSpell ASC, StartTimeHospProvSpell ASC
                                 , Case when DischDateHospProvSpell is not null then 1 else 2 end ASC
                                 , Case when DischDateHospProvSpell is not null then DischDateHospProvSpell end ASC
                                 , DischTimeHospProvSpell ASC
                                 , Case when DischDateHospProvSpell is null then 1 else 2 end asc, UniqMonthID DESC
                          ) AS HOSP_ADM_RANK

 FROM	(SELECT *
               ,dense_rank() over (partition by Person_ID, orgidProv, StartDateHospProvSpell, DischDateHospProvSpell  
                                       order by UniqMonthID DESC, UniqHospProvSpellID DESC
                                    ) AS HOSP_DUP_RANK                                   
 		FROM global_temp.MHA_MHS501HospSpell_Latest) AS A
         
 WHERE HOSP_DUP_RANK = '1'
 ORDER BY Person_ID, StartDateHospProvSpell, DischDateHospProvSpell


# COMMAND ----------

# DBTITLE 1,Create MHA_HOSP_ADM
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_HOSP_ADM AS

 Select distinct HOSP_ADM_RANK, (HOSP_ADM_RANK - 1) AS PREV_HOSP_ADM_RANK
 FROM global_temp.MHA_MHS501_Ranked


# COMMAND ----------

# DBTITLE 1,Create MHA_MHS501HospSpell_Latest_Ranked
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS501HospSpell_Latest_Ranked AS

 SELECT 
    a.UniqMonthID
   ,a.orgidProv
   ,a.Person_ID
   ,a.UniqHospProvSpellID
   ,a.StartDateHospProvSpell
   ,a.StartTimeHospProvSpell
   ,a.DischDateHospProvSpell
   ,a.DischTimeHospProvSpell
   ,a.SourceAdmMHHospProvSpell
   ,a.MethAdmMHHospProvSpell
   ,a.DestOfDischHospProvSpell
   ,a.MethOfDischMHHospProvSpell
   ,a.HOSP_ADM_RANK
   ,B.StartDateHospProvSpell as PrevStartDateHospProvSpell
   ,b.DischDateHospProvSpell as PrevDischDateHospProvSpell
   ,b.DestOfDischHospProvSpell as PrevDischDestCodeHospProvSpell
   
 FROM global_temp.MHA_MHS501_Ranked a
   left join global_temp.MHA_HOSP_ADM rnk on a.HOSP_ADM_RANK = rnk.HOSP_ADM_RANK
   left join global_temp.MHA_MHS501_Ranked b on a.Person_ID = b.Person_ID and b.HOSP_ADM_RANK = rnk.PREV_HOSP_ADM_RANK


# COMMAND ----------

# DBTITLE 1,Create MHA_KP90
 %sql

 /*
 This joins the MHA data to the Hosp Spell data.
 Two ranks are done at this stage:
 1) MHA_rank ranks the MHA episodes based on PersonID, orgid, StartDate and Legal Status. 
 This is to try and remove instances where the same MHA record has been submitted repeeatedly with a different ID. Rank =1 is used later.
 2) HOSP_Rank ranks the MHA episodes based on the hospital spell number. 
 This is used to see if multiple sections have been used during one hospital spell. It is also useful to see if a patient has transferred on section / has a previous section etc.
 Detention_Cat fields are used to compare MHA start dates to the Hospital Spell start dates. Records which link but where the dates do not lineup are removed in the where clause.

 DSA = Detained Subsequent to admission
 DOA = Detained on Admission
 TOS = Transfer on Section
 */

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_KP90 AS

 SELECT
    A.Person_ID
   ,A.UniqMHActEpisodeID
   ,A.RecordNumber
   ,A.orgidProv
   ,A.StartDateMHActLegalStatusClass
   ,A.StartTimeMHActLegalStatusClass
   ,A.ExpiryDateMHActLegalStatusClass
   ,A.EndDateMHActLegalStatusClass
   ,A.EndTimeMHActLegalStatusClass
   ,A.LegalStatusCode
   ,dense_rank() over (partition by a.Person_ID, A.orgidProv ,a.StartDateMHActLegalStatusClass, A.LegalStatusCode 
                          order by A.RecordStartDate DESC, ExpiryDateMHActLegalStatusClass DESC, EndDateMHActLegalStatusCLass DESC, A.UniqMHActEpisodeID DESC) AS MHA_RANK
   ,B.UniqHospProvSpellID
   ,B.HOSP_ADM_RANK
   ,B.PrevDischDestCodeHospProvSpell
   ,B.MethAdmMHHospProvSpell
   ,B.StartDateHospProvSpell
   ,B.StartTimeHospProvSpell
   ,B.DischDateHospProvSpell
   ,B.DischTimeHospProvSpell
   ,CASE
       WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
       WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass THEN 'NA'
       WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
       WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell THEN 'DOA'
       WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
       ELSE 'UNKNOWN' 
      END AS Detention_Cat 
   ,CASE
       WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
       WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass 
             or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass 
             or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) THEN 'NA'
       WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
       WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA'
       WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell 
               and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) = 1 THEN 'DOA'
       WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell 
               and dense_rank() over (partition by a.Person_ID, B.UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) > 1 THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
       ELSE 'UNKNOWN' 
      END AS Detention_DateTime_Cat 

 FROM global_temp.MHA_MHS401MHA_Latest A
 LEFT JOIN global_temp.MHA_MHS501HospSpell_Latest_Ranked B 
   ON A.Person_ID = B.Person_ID 
       and A.orgidProv = B.orgidProv 
       and CASE WHEN B.UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
               WHEN B.StartDateHospProvSpell > A.EndDateMHActLegalStatusClass 
                     or B.StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass 
                     or (a.StartDateMHActLegalStatusClass = b.DischDateHospProvSpell and a.StartTimeMHActLegalStatusClass > b.DischTimeHospProvSpell) THEN 'NA'
               WHEN B.DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
               WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = B.StartTimeHospProvSpell THEN 'DOA'
               WHEN A.StartDateMHActLegalStatusClass = B.StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > B.StartTimeHospProvSpell THEN 'DSA'
               WHEN A.StartDateMHActLegalStatusClass > B.StartDateHospProvSpell THEN 'DSA'
               WHEN A.StartDateMHActLegalStatusClass < B.StartDateHospProvSpell THEN 'TOS'
               ELSE 'UNKNOWN' 
             END <> 'NA'
 ORDER BY 
    A.Person_ID
   ,a.StartDateMHActLegalStatusClass
   ,MHA_RANK
   ,A.UniqMHActEpisodeID
   ,b.StartDateHospProvSpell


# COMMAND ----------

# DBTITLE 1,Create MHA_KP90a
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_KP90a AS

 SELECT
    A.Person_ID
   ,A.UniqMHActEpisodeID
   ,A.RecordNumber
   ,A.orgidProv
   ,A.StartDateMHActLegalStatusClass
   ,A.StartTimeMHActLegalStatusClass
   ,A.ExpiryDateMHActLegalStatusClass
   ,A.EndDateMHActLegalStatusClass
   ,A.LegalStatusCode
   ,MHA_RANK
   ,dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID 
                         order by A.StartDateMHActLegalStatusClass ASC, A.StartTimeMHActLegalStatusClass ASC
                                 , CASE when A.EndDateMHActLegalStatusClass is null then 1 else 2 end asc
                                 , A.EndDateMHActLegalStatusClass ASC, CASE when A.EndTimeMHActLegalStatusClass is null then 1 else 2 end asc
                                 , A.EndTimeMHActLegalStatusClass ASC) AS HOSP_RANK
   ,UniqHospProvSpellID
   ,HOSP_ADM_RANK
   ,PrevDischDestCodeHospProvSpell
   ,MethAdmMHHospProvSpell
   ,StartDateHospProvSpell
   ,StartTimeHospProvSpell
   ,DischDateHospProvSpell
   ,DischTimeHospProvSpell
   ,CASE
       WHEN UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
       WHEN StartDateHospProvSpell > A.EndDateMHActLegalStatusClass or StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass THEN 'NA'
       WHEN DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
       WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell THEN 'DOA'
       WHEN A.StartDateMHActLegalStatusClass > StartDateHospProvSpell THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass < StartDateHospProvSpell THEN 'TOS'
       ELSE 'UNKNOWN' 
     END AS Detention_Cat 
   ,CASE
       WHEN UniqHospProvSpellID IS NULL THEN 'NO HOSPITAL SPELL'
       WHEN StartDateHospProvSpell > A.EndDateMHActLegalStatusClass 
             or StartDateHospProvSpell > A.ExpiryDateMHActLegalStatusClass 
             or (a.StartDateMHActLegalStatusClass = DischDateHospProvSpell 
             and a.StartTimeMHActLegalStatusClass > DischTimeHospProvSpell) THEN 'NA'
       WHEN DischDateHospProvSpell < A.StartDateMHActLegalStatusClass THEN 'NA' 
       WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass = StartTimeHospProvSpell THEN 'DOA'
       WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell and a.StartTimeMHActLegalStatusClass > StartTimeHospProvSpell THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell 
             and dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) = 1 THEN 'DOA'
       WHEN A.StartDateMHActLegalStatusClass = StartDateHospProvSpell 
             and dense_rank() over (partition by a.Person_ID, UniqHospProvSpellID order by A.StartDateMHActLegalStatusClass, A.StartTimeMHActLegalStatusClass) > 1 THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass > StartDateHospProvSpell THEN 'DSA'
       WHEN A.StartDateMHActLegalStatusClass < StartDateHospProvSpell THEN 'TOS'
       ELSE 'UNKNOWN' 
      END AS Detention_DateTime_Cat 
 FROM global_temp.MHA_KP90 A
 WHERE MHA_RANK = '1'
 ORDER BY 
    A.Person_ID
   ,a.StartDateMHActLegalStatusClass
   ,MHA_RANK
   ,A.UniqMHActEpisodeID
   ,StartDateHospProvSpell


# COMMAND ----------

# DBTITLE 1,Create MHA_HOSP_RANK
 %sql
 /*
 A table is created to link the curernt MHA episode to any exisiting previous one. This link is done using PersonID and Hospital Spell Number. 
 The current MHA episode is selected using = @RANK and the join uses = @RANK -1.
 */
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_HOSP_RANK AS 

 SELECT DISTINCT HOSP_RANK, (HOSP_RANK - 1) AS PREV_HOSP_RANK
 FROM global_temp.MHA_KP90a


# COMMAND ----------

# DBTITLE 1,Create MHA_KP90_1
 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_KP90_1 AS

 SELECT 
     A.Person_ID
     ,A.UniqMHActEpisodeID
     ,A.RecordNumber
     ,A.orgidProv
     ,A.StartDateMHActLegalStatusClass
     ,A.StartTimeMHActLegalStatusClass
     ,A.ExpiryDateMHActLegalStatusClass
     ,A.EndDateMHActLegalStatusClass
     ,A.LegalStatusCode
     ,A.MHA_RANK
     ,A.HOSP_RANK
     ,A.UniqHospProvSpellID
     ,A.HOSP_ADM_RANK
     ,A.PrevDischDestCodeHospProvSpell
     ,A.MethAdmMHHospProvSpell
     ,A.StartDateHospProvSpell
     ,A.StartTimeHospProvSpell
     ,A.DischDateHospProvSpell
     ,A.DischTimeHospProvSpell
     ,A.Detention_Cat
     ,A.Detention_DateTime_Cat
     ,B.UniqMHActEpisodeID as PrevUniqMHActEpisodeID
     ,B.RecordNumber as PrevRecordNumber
     ,B.LegalStatusCode AS PrevLegalStatus
     ,B.StartDateMHActLegalStatusClass as PrevMHAStartDate
     ,B.EndDateMHActLegalStatusClass as PrevMHAEndDate
 FROM global_temp.MHA_KP90a A 
 LEFT JOIN global_temp.MHA_HOSP_RANK rnk on a.HOSP_RANK = rnk.HOSP_RANK 
 LEFT JOIN global_temp.MHA_KP90a B ON A.Person_ID = B.Person_ID AND A.UniqHospProvSpellID = B.UniqHospProvSpellID AND B.HOSP_RANK = rnk.PREV_HOSP_RANK


# COMMAND ----------

# DBTITLE 1,Create MHA_KP90_2
 %sql

 /* 
 This is the final data sheet. Some Organisations are hard coded as they have since expired in the year and as such dont get pulled through in the ORG_DAILY table.

 The MHA_Logic_Cat provides basic logic on how each MHA episode falls into the certain categories.

 MHA_Logic_Cat_Full is the full logic and includes much more detail. This is the one which should be used..

 Everything is included in the group by as some rows seemed to be coming through as complete duplicates.

 Hard coded Organisations are due to some Orgs expiring midway through the year.
 */

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_KP90_2 AS

 SELECT 
     A.Person_ID
     ,A.RecordNumber as MHA_RecordNumber
     ,A.UniqMHActEpisodeID
     ,A.orgidProv
     ,B.NAME as Provider_Name
     ,A.StartDateMHActLegalStatusClass
     ,A.StartTimeMHActLegalStatusClass
     ,A.ExpiryDateMHActLegalStatusClass
     ,A.EndDateMHActLegalStatusClass
     ,A.LegalStatusCode
     ,A.MHA_RANK
     ,A.HOSP_RANK
     ,A.UniqHospProvSpellID
     ,A.HOSP_ADM_RANK
     ,A.PrevDischDestCodeHospProvSpell
     ,A.MethAdmMHHospProvSpell
     ,A.StartDateHospProvSpell
     ,A.StartTimeHospProvSpell
     ,A.DischDateHospProvSpell
     ,A.DischTimeHospProvSpell
     ,A.Detention_Cat
     ,A.Detention_DateTime_Cat
     ,PrevUniqMHActEpisodeID
     ,PrevRecordNumber
     ,PrevLegalStatus
     ,PrevMHAStartDate
     ,PrevMHAEndDate
     ,CASE
         when c.MHS404UniqID is null then d.MHS404UniqID
         ELSE c.MHS404UniqID
         END AS MHS404UniqID
     ,CASE
         when c.RecordNumber is null then d.RecordNumber
         ELSE c.RecordNumber
         END AS CTORecordNumber
     ,CASE
         when c.StartDateCommTreatOrd is null then d.StartDateCommTreatOrd
         ELSE c.StartDateCommTreatOrd
         END AS StartDateCommTreatOrd
     ,CASE
         when c.EndDateCommTreatOrd is null then d.EndDateCommTreatOrd
         ELSE c.EndDateCommTreatOrd
         END AS EndDateCommTreatOrd
     ,CASE
         when c.CommTreatOrdEndReason is null then d.CommTreatOrdEndReason
         ELSE c.CommTreatOrdEndReason
         END AS CommTreatOrdEndReason
     
 FROM global_temp.MHA_KP90_1 a
   left join $db_output.bbrb_org_daily_latest b on a.orgidProv = b.ORG_CODE
   left join global_temp.MHA_Revoked_CTO c on a.PrevUniqMHActEpisodeID = c.UniqMHActEpisodeID 
                                                 and c.StartDateCommTreatOrd < a.StartDateHospProvSpell 
                                                 and (c.EndDateCommTreatOrd > a.StartDateHospProvSpell or c.EndDateCommTreatOrd is null)
   left join global_temp.MHA_Revoked_CTO d on a.UniqMHActEpisodeID = d.UniqMHActEpisodeID 
                                                 and d.StartDateCommTreatOrd < a.StartDateHospProvSpell 
                                                 and (d.EndDateCommTreatOrd > a.StartDateHospProvSpell or d.EndDateCommTreatOrd is null)

 WHERE MHA_RANK = 1 

 GROUP BY 
   A.Person_ID
   ,A.RecordNumber
   ,A.UniqMHActEpisodeID
   ,A.orgidProv
   ,B.NAME 
   ,A.StartDateMHActLegalStatusClass
   ,A.StartTimeMHActLegalStatusClass
   ,A.ExpiryDateMHActLegalStatusClass
   ,A.EndDateMHActLegalStatusClass
   ,A.LegalStatusCode
   ,A.MHA_RANK
   ,A.HOSP_RANK
   ,A.UniqHospProvSpellID
   ,A.HOSP_ADM_RANK
   ,A.PrevDischDestCodeHospProvSpell
   ,A.MethAdmMHHospProvSpell
   ,A.StartDateHospProvSpell
   ,A.StartTimeHospProvSpell
   ,A.DischDateHospProvSpell
   ,A.DischTimeHospProvSpell
   ,A.Detention_Cat
   ,A.Detention_DateTime_Cat
   ,PrevUniqMHActEpisodeID
   ,PrevRecordNumber
   ,PrevLegalStatus
   ,PrevMHAStartDate
   ,PrevMHAEndDate
   ,CASE
       when c.MHS404UniqID is null then d.MHS404UniqID
       ELSE c.MHS404UniqID
       END
   ,CASE
       when c.RecordNumber is null then d.RecordNumber
       ELSE c.RecordNumber
       END
   ,CASE
       when c.StartDateCommTreatOrd is null then d.StartDateCommTreatOrd
       ELSE c.StartDateCommTreatOrd
       END
   ,CASE
       when c.EndDateCommTreatOrd is null then d.EndDateCommTreatOrd
       ELSE c.EndDateCommTreatOrd
       END
   ,CASE
       when c.CommTreatOrdEndReason is null then d.CommTreatOrdEndReason
       ELSE c.CommTreatOrdEndReason
       END
       
 ORDER BY 
    Person_ID
   ,StartDateMHActLegalStatusClass
   ,StartDateHospProvSpell
   ,HOSP_RANK


# COMMAND ----------

# DBTITLE 1,Create MHA_Final
 %sql

 /*
 For the MHA_Final logic is calculated to work out how the MHA is being used. This field is MHA_Logic_Cat_full.

 Categories for MHA_Logic_Cat_full:
 A = Detentions on admission to hospital
 B = Detentions subsequent to admission
 C = Detentions following Place of Safety Order
 D = Detentions following revocation of CTO or Conditional Discharge
 E = Place of Safety Order
 F = Other short term holding order
 G = Renewal
 H = Transfer on Section
 J = 5(2) subsequent to 5(4)
 K = 37 subsequent to 35
 L = 3 subsequent to 2
 M = Guardianship
 P = Criminal Jusitce admissions
 N = Inconsistent value

 */

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_Final AS

 SELECT a.*
     ,CASE 
         WHEN LegalStatusCode = '01' 
         THEN NULL
         WHEN LegalStatusCode in ('02','03') 
               and StartDateMHActLegalStatusClass = StartDateHospProvSpell 
               and (StartTimeMHActLegalStatusClass <= StartTimeHospProvSpell or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null)
               and ((PrevLegalStatus is null or PrevLegalStatus = '01') or (DATEDIFF(StartDateHospProvSpell,PrevMHAEndDate) > 1)) 
         THEN 'A'
         WHEN LegalStatusCode = '02' 
               AND StartDateHospProvSpell between DATE_ADD(StartDateMHActLegalStatusClass,-5) and StartDateMHActLegalStatusClass 
               and MethAdmMHHospProvSpell = '2A' 
         THEN 'A'
         WHEN LegalStatusCode in ('02','03') 
               and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass 
                   and ((StartTimeHospProvSpell < StartTimeMHActLegalStatusClass) or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null))
                   or StartDateMHActLegalStatusClass > StartDateHospProvSpell)
               and (((PrevLegalStatus is null or PrevMHAEndDate < StartDateMHActLegalStatusClass) 
                   and (MHS404UniqID is null or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1)))) 
                   or ((PrevLegalStatus in ('04','05','06') and (PrevMHAEndDate = StartDateMHActLegalStatusClass or PrevMHAEndDate = DATE_ADD(StartDateMHActLegalStatusClass,-1)))
                   and (MHS404UniqID is null or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))))) 
         THEN 'B'
         WHEN LegalStatusCode in ('02','03') 
               and ((StartDateMHActLegalStatusClass = PrevMHAEndDate) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) or (StartDateMHActLegalStatusClass = PrevMHAStartDate)))
               and PrevLegalStatus in ('19','20') 
               and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass) or (StartDateHospProvSpell >= PrevMHAStartDate) or (StartDateMHActLegalStatusClass = PrevMHAEndDate)) 
         THEN 'C'
         WHEN  (LegalStatusCode in ('03','09','10','15','16') 
               and ((StartDateMHActLegalStatusClass >= DATE_ADD(EndDateCommTreatOrd,-1) or EndDateCommTreatOrd is null) 
               and CommTreatOrdEndReason = '02') 
               and (StartDateHospProvSpell = StartDateMHActLegalStatusClass or StartDateHospProvSpell >= StartDateCommTreatOrd))
               or (LegalStatusCode = '03' and CommTreatOrdEndReason = '02' and StartDateMHActLegalStatusClass < StartDateHospProvSpell and EndDateCommTreatOrd between DATE_ADD(StartDateHospProvSpell,-1) 
               and DATE_ADD(StartDateHospProvSpell,2))  
         THEN 'D'
         WHEN LegalStatusCode in ('19','20') 
               and (PrevMHAStartDate is null or PrevLegalStatus = '01' or PrevMHAEndDate < StartDateMHActLegalStatusClass) 
         THEN 'E'
         WHEN ((LegalStatusCode in ('04','05','06') 
               and StartDateMHActLegalStatusClass = StartDateHospProvSpell) or (LegalStatusCode in ('05','06') 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell)) 
               and ((PrevLegalStatus  is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass) or PrevLegalStatus = '01' or (PrevLegalStatus in ('02','03') 
               and PrevMHAEndDate = StartDateMHActLegalStatusClass)) 
         THEN 'F'
         WHEN LegalStatusCode IN ('03','07','08','09','10','12','15','16','17','18') 
               AND StartDateMHActLegalStatusClass > StartDateHospProvSpell 
               AND LegalStatusCode = PrevLegalStatus
               and ((StartDateMHActLegalStatusClass BETWEEN PrevMHAEndDate AND DATE_ADD(PrevMHAEndDate,1) or PrevMHAEndDate is null)) 
         THEN 'G'
         WHEN LegalStatusCode IN ('02','03','07','08','09','10','12','13','14','15','16','17','18','31','32','34') 
               and StartDateHospProvSpell > StartDateMHActLegalStatusClass 
               and (MethAdmMHHospProvSpell in ('81','2B','11','12','13') or PrevDischDestCodeHospProvSpell in ('49','51','50','52','53','87')) 
         THEN 'H'
         WHEN LegalStatusCode = '05' 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
               and PrevLegalStatus = '06' 
               and (StartDateMHActLegalStatusClass >= PrevMHAStartDate 
               and (PrevMHAEndDate is null or PrevMHAEndDate <= EndDateMHActLegalStatusClass or EndDateMHActLegalStatusClass is null)) 
         THEN 'J'
         WHEN LegalStatusCode = '10' 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
               and PrevLegalStatus = '07' 
               and (PrevMHAEndDate is null or (PrevMHAEndDate = StartDateMHActLegalStatusClass) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1))) 
         THEN 'K'
         WHEN LegalStatusCode = '03' 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
               and PrevLegalStatus = '02' 
               and (PrevMHAEndDate is null or (PrevMHAEndDate = StartDateMHActLegalStatusClass) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1))) 
         THEN 'L'
         WHEN LegalStatusCode in ('35','36') 
         THEN 'M'
         WHEN LegalStatusCode in ('07','08','09','10','12','13','14','15','16','17','18','31','32','34') 
               and StartDateMHActLegalStatusClass <= StartDateHospProvSpell 
               and (EndDateMHActLegalStatusClass is null or EndDateMHActLegalStatusClass >= StartDateHospProvSpell)
               and MethAdmMHHospProvSpell in ('11','12','13') 
               and (PrevLegalStatus is null or (PrevLegalStatus in ('01','02','03','04','05','06') 
               and (PrevMHAEndDate is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass))) 
               and (MHS404UniqID is null or (EndDateCommTreatOrd is not null 
               and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))) 
         THEN 'P'
         ELSE 'N'
         END as MHA_Logic_Cat_full
     ,d.AgeRepPeriodEnd
     ,COALESCE(d.Age_Band, "UNKNOWN") as Age_Band
     ,COALESCE(d.Age_Group_Higher_Level, "UNKNOWN") as Age_Group_Higher_Level
     ,COALESCE(d.UpperEthnicity, "UNKNOWN") as UpperEthnicity
     ,COALESCE(d.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode
     ,COALESCE(d.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName
     ,COALESCE(d.Der_Gender, "UNKNOWN") as Der_Gender
     ,COALESCE(d.Gender_Description, "UNKNOWN") as Der_Gender_Desc
     ,COALESCE(d.IMD_Decile, "UNKNOWN") as IMD_Decile
     ,D.IC_REC_CCG as CCG_code
     ,D.NAME as CCG_NAME
     ,D.STP_CODE
     ,D.STP_NAME
     ,D.Region_Code
     ,D.Region_Name
     ,COALESCE(d.AutismStatus, "UNKNOWN") as AutismStatus
     ,COALESCE(d.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc
     ,COALESCE(d.LDStatus, "UNKNOWN") as LDStatus
     ,COALESCE(d.LDStatus_desc, "UNKNOWN") as LDStatus_desc
     
 from global_temp.MHA_KP90_2 a 
   left join global_temp.MHA_MHS001MPI_LATEST d on a.Person_ID = d.Person_ID

 ORDER BY 
    a.Person_ID
   ,StartDateMHActLegalStatusClass
   ,StartDateHospProvSpell
   ,HOSP_RANK

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_Final_in_year AS
  
 SELECT a.*
     ,CASE 
         WHEN LegalStatusCode = '01' 
         THEN NULL
         WHEN LegalStatusCode in ('02','03') 
               and StartDateMHActLegalStatusClass = StartDateHospProvSpell 
               and (StartTimeMHActLegalStatusClass <= StartTimeHospProvSpell or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null)
               and ((PrevLegalStatus is null or PrevLegalStatus = '01') or (DATEDIFF(StartDateHospProvSpell,PrevMHAEndDate) > 1)) 
         THEN 'A'
         WHEN LegalStatusCode = '02' 
               AND StartDateHospProvSpell between DATE_ADD(StartDateMHActLegalStatusClass,-5) and StartDateMHActLegalStatusClass 
               and MethAdmMHHospProvSpell = '2A' 
         THEN 'A'
         WHEN LegalStatusCode in ('02','03') 
               and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass 
                   and ((StartTimeHospProvSpell < StartTimeMHActLegalStatusClass) or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null))
                   or StartDateMHActLegalStatusClass > StartDateHospProvSpell)
               and (((PrevLegalStatus is null or PrevMHAEndDate < StartDateMHActLegalStatusClass) 
                   and (MHS404UniqID is null or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1)))) 
                   or ((PrevLegalStatus in ('04','05','06') and (PrevMHAEndDate = StartDateMHActLegalStatusClass or PrevMHAEndDate = DATE_ADD(StartDateMHActLegalStatusClass,-1)))
                   and (MHS404UniqID is null or (EndDateCommTreatOrd is not null and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))))) 
         THEN 'B'
         WHEN LegalStatusCode in ('02','03') 
               and ((StartDateMHActLegalStatusClass = PrevMHAEndDate) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) or (StartDateMHActLegalStatusClass = PrevMHAStartDate)))
               and PrevLegalStatus in ('19','20') 
               and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass) or (StartDateHospProvSpell >= PrevMHAStartDate) or (StartDateMHActLegalStatusClass = PrevMHAEndDate)) 
         THEN 'C'
         WHEN  (LegalStatusCode in ('03','09','10','15','16') 
               and ((StartDateMHActLegalStatusClass >= DATE_ADD(EndDateCommTreatOrd,-1) or EndDateCommTreatOrd is null) 
               and CommTreatOrdEndReason = '02') 
               and (StartDateHospProvSpell = StartDateMHActLegalStatusClass or StartDateHospProvSpell >= StartDateCommTreatOrd))
               or (LegalStatusCode = '03' and CommTreatOrdEndReason = '02' and StartDateMHActLegalStatusClass < StartDateHospProvSpell and EndDateCommTreatOrd between DATE_ADD(StartDateHospProvSpell,-1) 
               and DATE_ADD(StartDateHospProvSpell,2))  
         THEN 'D'
         WHEN LegalStatusCode in ('19','20') 
               and (PrevMHAStartDate is null or PrevLegalStatus = '01' or PrevMHAEndDate < StartDateMHActLegalStatusClass) 
         THEN 'E'
         WHEN ((LegalStatusCode in ('04','05','06') 
               and StartDateMHActLegalStatusClass = StartDateHospProvSpell) or (LegalStatusCode in ('05','06') 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell)) 
               and ((PrevLegalStatus  is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass) or PrevLegalStatus = '01' or (PrevLegalStatus in ('02','03') 
               and PrevMHAEndDate = StartDateMHActLegalStatusClass)) 
         THEN 'F'
         WHEN LegalStatusCode IN ('03','07','08','09','10','12','15','16','17','18') 
               AND StartDateMHActLegalStatusClass > StartDateHospProvSpell 
               AND LegalStatusCode = PrevLegalStatus
               and ((StartDateMHActLegalStatusClass BETWEEN PrevMHAEndDate AND DATE_ADD(PrevMHAEndDate,1) or PrevMHAEndDate is null)) 
         THEN 'G'
         WHEN LegalStatusCode IN ('02','03','07','08','09','10','12','13','14','15','16','17','18','31','32','34') 
               and StartDateHospProvSpell > StartDateMHActLegalStatusClass 
               and (MethAdmMHHospProvSpell in ('81','2B','11','12','13') or PrevDischDestCodeHospProvSpell in ('49','51','50','52','53','87')) 
         THEN 'H'
         WHEN LegalStatusCode = '05' 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
               and PrevLegalStatus = '06' 
               and (StartDateMHActLegalStatusClass >= PrevMHAStartDate 
               and (PrevMHAEndDate is null or PrevMHAEndDate <= EndDateMHActLegalStatusClass or EndDateMHActLegalStatusClass is null)) 
         THEN 'J'
         WHEN LegalStatusCode = '10' 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
               and PrevLegalStatus = '07' 
               and (PrevMHAEndDate is null or (PrevMHAEndDate = StartDateMHActLegalStatusClass) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1))) 
         THEN 'K'
         WHEN LegalStatusCode = '03' 
               and StartDateMHActLegalStatusClass >= StartDateHospProvSpell 
               and PrevLegalStatus = '02' 
               and (PrevMHAEndDate is null or (PrevMHAEndDate = StartDateMHActLegalStatusClass) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1))) 
         THEN 'L'
         WHEN LegalStatusCode in ('35','36') 
         THEN 'M'
         WHEN LegalStatusCode in ('07','08','09','10','12','13','14','15','16','17','18','31','32','34') 
               and StartDateMHActLegalStatusClass <= StartDateHospProvSpell 
               and (EndDateMHActLegalStatusClass is null or EndDateMHActLegalStatusClass >= StartDateHospProvSpell)
               and MethAdmMHHospProvSpell in ('11','12','13') 
               and (PrevLegalStatus is null or (PrevLegalStatus in ('01','02','03','04','05','06') 
               and (PrevMHAEndDate is null or PrevMHAEndDate <= StartDateMHActLegalStatusClass))) 
               and (MHS404UniqID is null or (EndDateCommTreatOrd is not null 
               and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))) 
         THEN 'P'
         ELSE 'N'
         END as MHA_Logic_Cat_full
     ,d.AgeRepPeriodEnd
     ,COALESCE(d.Age_Band, "UNKNOWN") as Age_Band
     ,COALESCE(d.Age_Group_Higher_Level, "UNKNOWN") as Age_Group_Higher_Level
     ,COALESCE(d.UpperEthnicity, "UNKNOWN") as UpperEthnicity
     ,COALESCE(d.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode
     ,COALESCE(d.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName
     ,CASE 
         WHEN d.WhiteBritish = 1 THEN 'White British' 
         WHEN d.NotWhiteBritish = 1 THEN 'Non-white British' 
         ELSE 'Missing/invalid' 
      END as WNW_Ethnicity
     ,COALESCE(d.Der_Gender, "UNKNOWN") as Der_Gender
     ,COALESCE(d.Gender_Description, "UNKNOWN") as Der_Gender_Desc
     ,COALESCE(d.IMD_Decile, "UNKNOWN") as IMD_Decile
     ,COALESCE(d.IMD_Core20, "UNKNOWN") as IMD_Core20
     ,D.CCG_Code as CCG_code
     ,D.CCG_Name as CCG_NAME
     ,D.STP_CODE
     ,D.STP_NAME
     ,D.Region_Code
     ,D.Region_Name
     ,COALESCE(d.AutismStatus, "UNKNOWN") as AutismStatus
     ,COALESCE(d.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc
     ,COALESCE(d.LDStatus, "UNKNOWN") as LDStatus
     ,COALESCE(d.LDStatus_desc, "UNKNOWN") as LDStatus_desc
     
 from global_temp.MHA_KP90_2 a 
   left join global_temp.MHA_MHS001MPI_LATEST_IN_YEAR d on a.Person_ID = d.Person_ID
  
 ORDER BY 
    a.Person_ID
   ,StartDateMHActLegalStatusClass
   ,StartDateHospProvSpell
   ,HOSP_RANK

# COMMAND ----------

# DBTITLE 1,Create MHA_CTO_Final
 %sql

 -- CTO TABLE 

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_CTO_Final AS 

 SELECT 
     A.Person_ID,
     A.UniqMHActEpisodeID as CTO_UniqMHActEpisodeID,
     A.MHS404UniqID,
     A.orgidProv,
     C.NAME as Provider_Name,
     A.StartDateCommTreatOrd,
     A.ExpiryDateCommTreatOrd,
     A.EndDateCommTreatOrd,
     A.CommTreatOrdEndReason,
     B.UniqMHActEpisodeID as MHA_UniqMHActEpisodeID,
     B.LegalStatusCode,
     B.StartDateMHActLegalStatusClass,
     B.ExpiryDateMHActLegalStatusClass,
     B.EndDateMHActLegalStatusClass
     ,d.AgeRepPeriodEnd
     ,COALESCE(d.Age_Band, "UNKNOWN") as Age_Band
     ,COALESCE(d.Age_Group_Higher_Level, "UNKNOWN") as Age_Group_Higher_Level
     ,d.UpperEthnicity
     ,COALESCE(d.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode
     ,COALESCE(d.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName
     ,d.Der_Gender
     ,COALESCE(d.Gender_Description, "UNKNOWN") as Der_Gender_Desc
     ,d.IMD_Decile
     ,D.IC_REC_CCG as CCG_code
     ,D.NAME as CCG_NAME
     ,D.STP_CODE
     ,D.STP_NAME
     ,D.Region_Code
     ,D.Region_Name
     ,COALESCE(d.AutismStatus, "UNKNOWN") as AutismStatus
     ,COALESCE(d.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc
     ,COALESCE(d.LDStatus, "UNKNOWN") as LDStatus
     ,COALESCE(d.LDStatus_desc, "UNKNOWN") as LDStatus_desc
     
 FROM global_temp.MHA_MHS404CTO_Latest_Ranked A
   left join global_temp.MHA_Final B ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID
   LEFT JOIN $db_output.bbrb_org_daily_latest C on  A.orgidProv = C.ORG_CODE
   left join global_temp.MHA_MHS001MPI_LATEST d on a.Person_ID = d.Person_ID

 GROUP BY
     A.Person_ID,
     A.UniqMHActEpisodeID,
     A.MHS404UniqID,
     A.orgidProv,
     C.NAME,
     A.StartDateCommTreatOrd,
     A.ExpiryDateCommTreatOrd,
     A.EndDateCommTreatOrd,
     A.CommTreatOrdEndReason,
     B.UniqMHActEpisodeID,
     B.LegalStatusCode,
     B.StartDateMHActLegalStatusClass,
     B.ExpiryDateMHActLegalStatusClass,
     B.EndDateMHActLegalStatusClass
     ,d.AgeRepPeriodEnd
     ,d.Age_Band
     ,d.Age_Group_Higher_Level
     ,d.UpperEthnicity
     ,d.LowerEthnicityCode
     ,d.LowerEthnicityName
     ,d.Der_Gender
     ,d.Gender_Description
     ,d.IMD_Decile
     ,D.IC_REC_CCG
     ,D.NAME
     ,D.STP_CODE
     ,D.STP_NAME
     ,D.Region_Code
     ,D.Region_Name
     ,D.AutismStatus
     ,D.AutismStatus_desc
     ,D.LDStatus
     ,D.LDStatus_desc

# COMMAND ----------

 %sql
  
 -- CTO TABLE for 12 month rolling data
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_CTO_Final_in_year AS 
  
 SELECT 
     A.Person_ID,
     A.UniqMHActEpisodeID as CTO_UniqMHActEpisodeID,
     A.MHS404UniqID,
     A.orgidProv,
     C.NAME as Provider_Name,
     A.StartDateCommTreatOrd,
     A.ExpiryDateCommTreatOrd,
     A.EndDateCommTreatOrd,
     A.CommTreatOrdEndReason,
     B.UniqMHActEpisodeID as MHA_UniqMHActEpisodeID,
     B.LegalStatusCode,
     B.StartDateMHActLegalStatusClass,
     B.ExpiryDateMHActLegalStatusClass,
     B.EndDateMHActLegalStatusClass
     ,d.AgeRepPeriodEnd
     ,COALESCE(d.Age_Band, "UNKNOWN") as Age_Band
     ,COALESCE(d.Age_Group_Higher_Level, "UNKNOWN") as Age_Group_Higher_Level
     ,d.UpperEthnicity
     ,COALESCE(d.LowerEthnicityCode, "UNKNOWN") as LowerEthnicityCode
     ,COALESCE(d.LowerEthnicityName, "UNKNOWN") as LowerEthnicityName
     ,CASE 
         WHEN d.WhiteBritish = 1 THEN 'White British' 
         WHEN d.NotWhiteBritish = 1 THEN 'Non-white British' 
         ELSE 'Missing/invalid' 
      END as WNW_Ethnicity
     ,d.Der_Gender
     ,COALESCE(d.Gender_Description, "UNKNOWN") as Der_Gender_Desc
     ,COALESCE(d.IMD_Decile, "UNKNOWN") as IMD_Decile
     ,COALESCE(d.IMD_Core20, "UNKNOWN") as IMD_Core20
     ,D.CCG_Code as CCG_code
     ,D.CCG_Name as CCG_NAME
     ,D.STP_CODE
     ,D.STP_NAME
     ,D.Region_Code
     ,D.Region_Name
     ,COALESCE(d.AutismStatus, "UNKNOWN") as AutismStatus
     ,COALESCE(d.AutismStatus_desc, "UNKNOWN") as AutismStatus_desc
     ,COALESCE(d.LDStatus, "UNKNOWN") as LDStatus
     ,COALESCE(d.LDStatus_desc, "UNKNOWN") as LDStatus_desc
     
 FROM global_temp.MHA_MHS404CTO_Latest_Ranked A
   left join global_temp.MHA_Final_in_year B ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID
   LEFT JOIN $db_output.bbrb_org_daily_latest C on  A.orgidProv = C.ORG_CODE
   left join global_temp.MHA_MHS001MPI_LATEST_IN_YEAR d on a.Person_ID = d.Person_ID
  
 GROUP BY
     A.Person_ID,
     A.UniqMHActEpisodeID,
     A.MHS404UniqID,
     A.orgidProv,
     C.NAME,
     A.StartDateCommTreatOrd,
     A.ExpiryDateCommTreatOrd,
     A.EndDateCommTreatOrd,
     A.CommTreatOrdEndReason,
     B.UniqMHActEpisodeID,
     B.LegalStatusCode,
     B.StartDateMHActLegalStatusClass,
     B.ExpiryDateMHActLegalStatusClass,
     B.EndDateMHActLegalStatusClass
     ,d.AgeRepPeriodEnd
     ,d.Age_Band
     ,d.Age_Group_Higher_Level
     ,d.UpperEthnicity
     ,d.LowerEthnicityCode
     ,d.LowerEthnicityName
     ,CASE 
         WHEN d.WhiteBritish = 1 THEN 'White British' 
         WHEN d.NotWhiteBritish = 1 THEN 'Non-white British' 
         ELSE 'Missing/invalid' 
      END
     ,d.Der_Gender
     ,d.Gender_Description
     ,d.IMD_Decile
     ,d.IMD_Core20
     ,D.CCG_Code
     ,D.CCG_Name
     ,D.STP_CODE
     ,D.STP_NAME
     ,D.Region_Code
     ,D.Region_Name
     ,D.AutismStatus
     ,D.AutismStatus_desc
     ,D.LDStatus
     ,D.LDStatus_desc

# COMMAND ----------

 %sql

 INSERT OVERWRITE TABLE $db_output.DETENTIONS_MONTHLY

 SELECT A.*
 FROM  global_temp.MHA_Final A
 INNER JOIN (SELECT UniqMHActEpisodeID, MIN(StartDateHospProvSpell) AS HOSP_START
 			FROM global_temp.MHA_Final
 			GROUP BY UniqMHActEpisodeID) AS B 
         ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START

 WHERE StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'


# COMMAND ----------

 %sql

 INSERT OVERWRITE TABLE $db_output.Short_term_orders_MONTHLY

 SELECT *
 FROM global_temp.MHA_Final
 WHERE StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'

# COMMAND ----------

 %sql

 INSERT OVERWRITE TABLE $db_output.CTO_MONTHLY

 SELECT *
 FROM global_temp.MHA_CTO_Final a
 where StartDateCommTreatOrd between '$rp_startdate' and '$rp_enddate'


# COMMAND ----------

 %sql
  
 INSERT OVERWRITE TABLE $db_output.DETENTIONS_YEARLY
  
 SELECT A.*
 FROM  global_temp.MHA_Final_in_year A
 INNER JOIN (SELECT UniqMHActEpisodeID, MIN(StartDateHospProvSpell) AS HOSP_START
             FROM global_temp.MHA_Final_in_year
             GROUP BY UniqMHActEpisodeID) AS B 
         ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START
  
 WHERE StartDateMHActLegalStatusClass between ADD_MONTHS('$rp_startdate',-11) and '$rp_enddate'

# COMMAND ----------

  %sql
  -- DETENTIONS_YEARLY with filter applied for rates calculation for MHS157a-c, MHS158a-c

  INSERT OVERWRITE TABLE $db_output.DETENTIONS_YEARLY_FILTERED

  SELECT A.*
  FROM  global_temp.MHA_Final_in_year A
  INNER JOIN (SELECT UniqMHActEpisodeID, MIN(StartDateHospProvSpell) AS HOSP_START
  			FROM global_temp.MHA_Final_in_year
  			GROUP BY UniqMHActEpisodeID) AS B 
          ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START

  WHERE StartDateMHActLegalStatusClass between ADD_MONTHS('$rp_startdate',-11) and '$rp_enddate' 
  and MHA_Logic_Cat_full IN ('A','B','C','D','P')

# COMMAND ----------

 %sql
  
 INSERT OVERWRITE TABLE $db_output.CTO_YEARLY
  
 SELECT *
 FROM global_temp.MHA_CTO_Final_in_year a
 where StartDateCommTreatOrd between ADD_MONTHS('$rp_startdate',-11) and '$rp_enddate'

# COMMAND ----------

 %md
 ## MHA New Measures - Jan 2025

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW DETENTIONS_ALL_Temp AS 
  
 SELECT A.*
 FROM  global_temp.MHA_Final A
 INNER JOIN (SELECT UniqMHActEpisodeID, MIN(StartDateHospProvSpell) AS HOSP_START
             FROM global_temp.MHA_Final
             GROUP BY UniqMHActEpisodeID) AS B 
         ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW DETENTIONS_ALL_Temp1 AS
  
 select A.* 
 from global_temp.DETENTIONS_ALL_Temp A
 INNER JOIN (SELECT UniqMHActEpisodeID
             FROM $db_source.MHS401MHActPeriod
             where UniqMonthID = '$end_month_id' ) AS B 
         ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID 

# COMMAND ----------

 %sql

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW DETENTIONS_ALL_Temp2 AS 
  
 Select 
 Person_ID,
 UniqMHActEpisodeID,
 orgidProv,
 Provider_Name,
 CASE WHEN MHA_Logic_Cat_full = 'H' THEN StartDateHospProvSpell
      ELSE StartDateMHActLegalStatusClass END AS StartDateMHActLegalStatusClass,
 StartTimeMHActLegalStatusClass,
 ExpiryDateMHActLegalStatusClass,
 EndDateMHActLegalStatusClass,
 LegalStatusCode,
 MHA_RANK,
 HOSP_RANK,
 UniqHospProvSpellID,
 HOSP_ADM_RANK,
 PrevDischDestCodeHospProvSpell,
 MethAdmMHHospProvSpell,
 StartDateHospProvSpell,
 StartTimeHospProvSpell,
 DischDateHospProvSpell,
 DischTimeHospProvSpell,
 Detention_Cat,
 Detention_DateTime_Cat,
 PrevUniqMHActEpisodeID,
 PrevRecordNumber,
 PrevLegalStatus,
 PrevMHAStartDate,
 PrevMHAEndDate,
 MHS404UniqID,
 CTORecordNumber,
 StartDateCommTreatOrd,
 EndDateCommTreatOrd,
 CommTreatOrdEndReason,
 MHA_Logic_Cat_full,
 AgeRepPeriodEnd,
 Age_Band,
 Age_Group_Higher_Level,
 UpperEthnicity,
 LowerEthnicityCode,
 LowerEthnicityName,
 Der_Gender,
 Der_Gender_Desc,
 IMD_Decile,
 CCG_code,
 CCG_NAME,
 STP_CODE,
 STP_NAME,
 Region_Code,
 Region_Name,
 AutismStatus,
 AutismStatus_desc,
 LDStatus,
 LDStatus_desc,
 Der_EndDateMHActLegalStatusClass
  
 from
 (
 Select B.*,
 CASE WHEN (EndDateMHActLegalStatusClass IS NOT NULL AND EndDateMHActLegalStatusClass <= '$rp_enddate') THEN EndDateMHActLegalStatusClass   --use EndDateMHActLegalStatusClass if recorded and not after rpend
      WHEN ((EndDateMHActLegalStatusClass IS NULL OR EndDateMHActLegalStatusClass > '$rp_enddate') AND                                                              
           (ExpiryDateMHActLegalStatusClass IS NULL OR ExpiryDateMHActLegalStatusClass < StartDateMHActLegalStatusClass OR ExpiryDateMHActLegalStatusClass > '$rp_enddate')) THEN DATE_ADD('$rp_enddate',1)
      ELSE ExpiryDateMHActLegalStatusClass END AS Der_EndDateMHActLegalStatusClass
  
 from global_temp.DETENTIONS_ALL_Temp1 B
 ) A
 WHERE Der_EndDateMHActLegalStatusClass >= '$rp_startdate' 

# COMMAND ----------

 %sql
  
 INSERT OVERWRITE TABLE $db_output.DETENTIONS_ENDED 

 SELECT DISTINCT
 Person_ID,
 UniqMHActEpisodeID,
 orgidProv,
 Provider_Name,
 LegalStatusCode,
 MHA_Logic_Cat_full,
 AgeRepPeriodEnd,
 Age_Band,
 Age_Group_Higher_Level,
 UpperEthnicity,
 LowerEthnicityCode,
 LowerEthnicityName,
 Der_Gender,
 Der_Gender_Desc,
 IMD_Decile,
 CCG_code,
 CCG_NAME,
 STP_CODE,
 STP_NAME,
 Region_Code,
 Region_Name,
 AutismStatus,
 AutismStatus_desc,
 LDStatus,
 LDStatus_desc,
 Detention_days
 FROM
  
 (select a.*,
 datediff(Der_EndDateMHActLegalStatusClass, StartDateMHActLegalStatusClass) AS Detention_days
  
 from global_temp.DETENTIONS_ALL_Temp2 a
 where Der_EndDateMHActLegalStatusClass <= '$rp_enddate' )

# COMMAND ----------

 %sql
  
 INSERT OVERWRITE TABLE  $db_output.DETENTIONS_ACTIVE 

 SELECT DISTINCT
 Person_ID,
 UniqMHActEpisodeID,
 orgidProv,
 Provider_Name,
 LegalStatusCode,
 MHA_Logic_Cat_full,
 AgeRepPeriodEnd,
 Age_Band,
 Age_Group_Higher_Level,
 UpperEthnicity,
 LowerEthnicityCode,
 LowerEthnicityName,
 Der_Gender,
 Der_Gender_Desc,
 IMD_Decile,
 CCG_code,
 CCG_NAME,
 STP_CODE,
 STP_NAME,
 Region_Code,
 Region_Name,
 AutismStatus,
 AutismStatus_desc,
 LDStatus,
 LDStatus_desc,
 Detention_days
 FROM
  
 (select a.*,
 datediff(Der_EndDateMHActLegalStatusClass, StartDateMHActLegalStatusClass) AS Detention_days
  
 from global_temp.DETENTIONS_ALL_Temp2 a
 where Der_EndDateMHActLegalStatusClass > '$rp_enddate')