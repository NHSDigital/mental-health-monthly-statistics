# Databricks notebook source
 %sql
 
 -- $db_output.LDA_Data_1 only contains data for the current run (cleared out here and populated in THIS notebook) - this is clear Person level data
 -- $db_output.LDA_Counts keeps data for all months and both Respite and non-Respite outputs - needs extra filters before output
 
 TRUNCATE TABLE $db_output.LDA_Data_1;
 
 DELETE FROM $db_output.LDA_Counts
 WHERE PRODUCT ='nonRespite' 
 AND PeriodEnd = '$rp_enddate'
 AND SOURCE_DB = '$db_source'

# COMMAND ----------

# DBTITLE 1,LDAflag derivation from SQL - for reference - commented out
# %sql

# /* GBT: this is the Stored Procedure code for LDAFlag from SQL for reference 


# USE [MENH_MHSDS]
# GO

# /****** Object:  StoredProcedure [lve].[sp_IC_LDA_Flag]    Script Date: 6/15/2020 3:35:27 PM ******/
# SET ANSI_NULLS ON
# GO

# SET QUOTED_IDENTIFIER ON
# GO


# CREATE PROCEDURE [lve].[sp_IC_LDA_Flag]

# AS

# BEGIN

# declare @Period_Start date
# declare @Period_End date
# declare @rp int
# declare @ft varchar(7)

# set @rp = (select max(month_id) from lve.MHS000Header)
# set @ft = (select max(file_type) from lve.MHS000Header where Month_ID = @RP )
# set @Period_Start = (select distinct t.Period_Start from menh_ref.dbo.RD_Time_Mn t where t.MONTH_ID = @rp)
# set @Period_End = (select distinct t.Period_End from menh_ref.dbo.RD_Time_Mn t where t.MONTH_ID = @rp)

# IF OBJECT_ID('tempdb..#LD_cohort')	IS NOT NULL
# DROP TABLE #LD_cohort

# IF @ft = 'refresh'
# BEGIN
# UPDATE lve.MHS001MPI
# set IC_LDA_Flag = NULL 
# where lve.MHS001MPI.Month_ID = @RP


# SELECT distinct UniqMHSDSPersID
# into #LD_cohort
# FROM lve.MHS007DisabilityType e where e.DisabCode = '04' 
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from lve.MHS502WardStay f 
# where ((f.WardType = '05' and (f.EndDateWardStay is null or f.EndDateWardStay > @Period_Start)) or ((f.IntendClinCareIntenCodeMH in ('61','62','63') 
# and (f.EndDateWardStay is null or f.EndDateWardStay > @Period_Start))))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from  lve.MHS006MHCareCoord g 
# where  (g.CareProfServOrTeamTypeAssoc in ('B02','C01','E01','E02','E03') 
# and (g.EndDateAssCareCoord is null or g.EndDateAssCareCoord > @Period_Start))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from  lve.MHS102ServiceTypeReferredTo c 
# where (c.ServTeamTypeRefToMH in ('B02','C01','E01','E02','E03') 
# and ((c.ReferClosureDate is null or c.ReferClosureDate > @Period_Start) and (c.ReferRejectionDate is null or c.ReferRejectionDate > @Period_Start)))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from  lve.MHS401MHActPeriod i 
# where (i.MentalCat = 'B' and (i.EndDateMHActLegalStatusClass is null or i.ExpiryDateMHActLegalStatusClass is null 
# or i.EndDateMHActLegalStatusClass > @Period_Start or i.ExpiryDateMHActLegalStatusClass > @Period_Start))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from lve.MHS503AssignedCareProf j 
# where j.TreatFuncCodeMH = '700' and (j.EndDateAssCareProf is null or j.EndDateAssCareProf > @Period_Start)
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from lve.MHS603ProvDiag k 
# where ((k.DiagSchemeInUse = '02' and k.ProvDiag like 'F7%') or (k.DiagSchemeInUse = '02' and k.ProvDiag like 'F84%'))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from lve.MHS604PrimDiag l 
# where ((l.DiagSchemeInUse = '02' and l.PrimDiag like 'F7%') or (l.DiagSchemeInUse = '02' and l.PrimDiag like 'F84%'))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort
# select UniqMHSDSPersID from lve.MHS605SecDiag m 
# where ((m.DiagSchemeInUse = '02' and m.SecDiag like 'F7%') or (m.DiagSchemeInUse = '02' and m.SecDiag like 'F84%'))
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# insert into #LD_cohort 
# select UniqMHSDSPersID from lve.MHS801ClusterTool n 
# where n.ClustCat in ('03','05')
# and IC_Use_Submission_Flag = 'y'
# and Month_ID = @rp

# update menh_mhsds.lve.MHS001MPI
# set menh_mhsds.lve.MHS001MPI.IC_LDA_Flag = 'Y'
# where menh_mhsds.lve.MHS001MPI.UniqMHSDSPersID in (select distinct * from #LD_cohort)   
# and   menh_mhsds.lve.MHS001MPI.month_id = @rp

# drop table #LD_cohort

# end
# end
# GO

# *\

# COMMAND ----------

# DBTITLE 0,Individuals identified within table MHS007
 %sql
 --Individuals identified within table MHS007
 
 CREATE OR REPLACE TEMPORARY VIEW LD_007 AS
 
 SELECT distinct Person_ID, '007' as table
 FROM $db_source.MHS007DisabilityType e where e.DisabCode = '04' 
 and UniqMonthID = $month_id;

# COMMAND ----------

# DBTITLE 0,Individuals identified within table MHS007
 %sql
 --Individuals identified within table MHS502
 
 CREATE OR REPLACE TEMPORARY VIEW LD_502 AS
 
 SELECT distinct Person_ID, '502' as table
 FROM $db_source.MHS502WardStay f 
 where (f.WardType = '05' or f.IntendClinCareIntenCodeMH in ('61','62','63'))
 and  (f.EndDateWardStay is null or f.EndDateWardStay >= '$rp_startdate') 
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS006
 
 CREATE OR REPLACE TEMPORARY VIEW LD_006 AS
 
 select distinct Person_ID, '006' as table
 from  $db_source.MHS006MHCareCoord g 
 where  g.CareProfServOrTeamTypeAssoc in ('B02','C01','E01','E02','E03') 
 and (g.EndDateAssCareCoord is null or g.EndDateAssCareCoord >= '$rp_startdate')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS102
 
 CREATE OR REPLACE TEMPORARY VIEW LD_102 AS
 
 select distinct Person_ID, '102' as table
 from  $db_source.MHS102ServiceTypeReferredTo c 
 where c.ServTeamTypeRefToMH in ('B02','C01','E01','E02','E03') 
 and ((c.ReferClosureDate is null or c.ReferClosureDate >= '$rp_startdate') and (c.ReferRejectionDate is null or c.ReferRejectionDate >= '$rp_startdate'))
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS401
 
 CREATE OR REPLACE TEMPORARY VIEW LD_401 AS
 
 select distinct Person_ID, '401' as table
 from  $db_source.MHS401MHActPeriod i 
 where i.MentalCat = 'B' 
 and (i.EndDateMHActLegalStatusClass is null or i.ExpiryDateMHActLegalStatusClass is null 
 or i.EndDateMHActLegalStatusClass >= '$rp_startdate'or i.ExpiryDateMHActLegalStatusClass >= '$rp_startdate')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS503
 
 CREATE OR REPLACE TEMPORARY VIEW LD_503 AS
 
 select distinct Person_ID, '503' as table
 from $db_source.MHS503AssignedCareProf j 
 where j.TreatFuncCodeMH = '700' 
 and (j.EndDateAssCareProf is null or j.EndDateAssCareProf >= '$rp_startdate')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS603
 
 CREATE OR REPLACE TEMPORARY VIEW LD_603 AS
 
 select distinct Person_ID, '603' as table
 from $db_source.MHS603ProvDiag k 
 where k.DiagSchemeInUse = '02' and (k.ProvDiag like 'F7%' or k.ProvDiag like 'F84%')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS604
 
 CREATE OR REPLACE TEMPORARY VIEW LD_604 AS
 
 select distinct Person_ID, '604' as table
 from $db_source.MHS604PrimDiag l 
 where l.DiagSchemeInUse = '02' and (l.PrimDiag like 'F7%' or l.PrimDiag like 'F84%')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS605
 
 CREATE OR REPLACE TEMPORARY VIEW LD_605 AS
 
 select distinct Person_ID, '605' as table
 from $db_source.MHS605SecDiag m 
 where m.DiagSchemeInUse = '02' and (m.SecDiag like 'F7%' or m.SecDiag like 'F84%')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 --Individuals identified within table MHS801
 
 CREATE OR REPLACE TEMPORARY VIEW LD_801 AS
 
 select distinct Person_ID, '801' as table
 from $db_source.MHS801ClusterTool n 
 where n.ClustCat in ('03','05')
 and UniqMonthID = $month_id;

# COMMAND ----------

 %sql
 
 -- GBT: this cell collects together all the LDA individuals into one table
 -- NB includes duplicate records
 
 CREATE OR REPLACE TEMPORARY VIEW LD_cohort_dups AS
 
 
 SELECT * from LD_007 
 union all
 select * from LD_502
 union all
 select * from LD_006
 union all
 select * from LD_102
 union all
 select * from LD_401
 union all
 select * from LD_503
 union all
 select * from LD_603
 union all
 select * from LD_604
 union all
 select * from LD_605
 union all
 select * from LD_801

# COMMAND ----------

 %sql
 -- GBT: this cell replaces LDAflag
 -- GBT: instead of updating a column in mhs001mpi this populates a new table with just the Person_ID of the LDA cohort in it
 -- i.e. it just uses an equivalent to the temp table created in the original code and joins to this in future code rather than amending the source data table
    
 -- GBT: this creates an ordere, distinct version of the LDA cohort
 -- need to refer to this table as global_temp.LD_cohort
 
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LD_cohort AS
 select distinct Person_ID from LD_cohort_dups
 order by Person_ID

# COMMAND ----------

 %sql
 -- This is similar to the generic prep RD_ORG_DAILY_LATEST, however it is broader and has an extra column. Possible to combine?
 -- in temp_views
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_ORG_DAILY_LATEST_LDA AS
 
 SELECT DISTINCT                          
        ORG_CODE,                          
        NAME,                      
        CASE   WHEN ORG_TYPE_CODE = 'CC' THEN 'CCG'                          
               WHEN ORG_TYPE_CODE = 'CF' THEN 'NHS England'                         
               WHEN ORG_TYPE_CODE = 'LB' THEN 'Welsh Local Authority'                     
               WHEN ORG_TYPE_CODE = 'PT' THEN 'Primary Care Trust (Defunct)'                     
               WHEN ORG_TYPE_CODE = 'CT' THEN 'NHS Trust'                           
               WHEN ORG_TYPE_CODE = 'OU' THEN 'Overseas Patient'                          
               WHEN ORG_TYPE_CODE = 'PH' THEN 'Independent Health Provider'                      
               WHEN ORG_TYPE_CODE = 'NS' THEN 'National Commissioning Group'                     
               WHEN ORG_TYPE_CODE = 'TR' THEN 'NHS Trust'                           
               WHEN ORG_TYPE_CODE = 'HA' THEN 'NHS Stategic Health Authority (Defunct)'                        
               WHEN ORG_TYPE_CODE = 'LA' THEN 'Local Authority'       
               WHEN ORG_TYPE_CODE = 'NN' THEN 'Non-NHS Organisation'                
               END AS ORG_TYPE_CODE                                                
 FROM   db_source.org_daily                
 WHERE  (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)       
        AND BUSINESS_START_DATE <= '$rp_enddate'
        AND ORG_TYPE_CODE in ('CC','CF','LB','PT','CT','OU','NS','TR','HA','LA','PH','NN') 
        AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)  
        AND ORG_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 ------------------- THIS REFERENCE TABLE HAS ALL THE CCGS/TCPS AND COMMISSIONING REGIONS -------------------
 ----------------------- FOR THE RELEVANT REPORTING MONTH, IT IS DYNAMIC AND DEALS WITH NEW ORGS ------------
 -- in temp_views
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW commissioners AS
 select case when O3.NAME like '%HUB%' then 'HUB' 
             when O3.NAME like '%CCG%' then 'CCG' 
             else 'Other' 
             end as CCG_Or_Hub
             ,O3.ORG_CODE
             ,O3.NAME
             ,case when T.NHS_ENGLAND_TCP_CODE Is null then 'No TCP Mapping' 
             else T.NHS_ENGLAND_TCP_CODE 
             end as TCP_Code
             ,T.TCP_NAME
             ,O2.REL_TO_ORG_CODE as Region_code
             ,O4.NAME as Region_name
 from      db_source.ORG_DAILY O3 
 left join db_source.NHSE_CCG_TCP_V01 T 
        on T.CCG_ODS_CODE = O3.org_code 
        and (DSS_RECORD_END_DATE is null or DSS_RECORD_END_DATE >= '$rp_startdate') 
        and DSS_RECORD_START_DATE <= '$rp_startdate'
 inner join db_source.ORG_RELATIONSHIP_DAILY O1 
         on O1.REL_FROM_ORG_CODE = O3.ORG_CODE 
         and O1.REL_FROM_ORG_TYPE_CODE = 'CC' 
         and O1.REL_TYPE_CODE = 'CCCF' 
         and (O1.REL_IS_CURRENT = 1 or o1.REL_CLOSE_DATE >= '$rp_startdate') 
         and O1.REL_OPEN_DATE <= '$rp_startdate'
 inner join db_source.ORG_RELATIONSHIP_DAILY O2 
         on O2.REL_FROM_ORG_CODE = O1.REL_TO_ORG_CODE 
         and O2.REL_TYPE_CODE = 'CFCE' 
         and (O2.REL_IS_CURRENT = 1 or O2.REL_CLOSE_DATE >= '$rp_startdate') 
         and O2.REL_OPEN_DATE <= '$rp_startdate'
 left join db_source.ORG_DAILY O4 
        on O4.ORG_CODE = O2.REL_TO_ORG_CODE 
        and O4.ORG_TYPE_CODE = 'CE' 
        and (O4.ORG_IS_CURRENT = 1 or O4.org_close_DATE >= '$rp_startdate') 
        and O4.ORG_OPEN_DATE <= '$rp_startdate' 
 where  (O3.ORG_IS_CURRENT = 1 OR O3.ORG_CLOSE_DATE >= '$rp_startdate') 
       and O3.ORG_OPEN_DATE <= '$rp_startdate'
 group by case when O3.NAME like '%HUB%' then 'HUB' 
               when O3.NAME like '%CCG%' then 'CCG' 
               else 'Other' 
               end
           ,O3.ORG_CODE
           ,O3.NAME
           ,case when T.NHS_ENGLAND_TCP_CODE Is null then 'No TCP Mapping' 
           else T.NHS_ENGLAND_TCP_CODE 
           end
           ,T.TCP_NAME
           ,O2.REL_TO_ORG_CODE
           ,O4.NAME
 order by ORG_CODE

# COMMAND ----------

 %sql
 ---------- This is to try and speed up the code so that the joins dont have to be done repeatedly on each join. In theory this table holds all of the information required for the LD cohort.
 ---------- The problem to use in some circumstances (eg. Average LOS *No longer included*) is that some Hospital spells have multiple ward stays. This produces mutiple lines per one hospital spell in this table.
 
 ---------- For any new measures where the raw data is not in this table add it in using a left join to the appropriate table. Then add the field required to both the SELECT and GROUP BY.
 ---------- Ensure any new joins have the UniqmonthID in any join (** IC_UseSubmissionFlag previously needed in V3**). 
 
 ---------- In some cases it is easier to have a second raw data table if the calculations used in the measures are particularly complex (eg. MHA)
 -- in temp_views
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_data AS
 
 SELECT  M.Person_ID,
 		M.AgeRepPeriodEnd,
 		M.Gender,
 		M.NHSDEthnicity,
 		M.PatMRecInRP,
 		R.UniqServReqID,
 		R.ReferralRequestReceivedDate,
 		R.ServDischDate,
 		R.OrgIDProv AS REF_OrgCodeProv,
 		R.OrgIDComm,
 		H.UniqHospProvSpellID,
 		H.StartDateHospProvSpell,
 		H.DischDateHospProvSpell,
 		H.MethAdmMHHospProvSpell,
 		H.DestOfDischHospProvSpell,
 		H.OrgIDProv AS HSP_OrgCodeProv,
 		H.PlannedDischDateHospProvSpell,
 		A.UniqWardStayID,
         A.SiteIDOfTreat,
 		A.StartDateWardStay,
 		A.EndDateWardStay,
 		A.WardLocDistanceHome,
 		A.WardSecLevel,
 		A.WardType,
 		DD.StartDateDelayDisch,
 		DD.EndDateDelayDisch,
 		DD.DelayDischReason,
 Case when PVD.ProvDiag = 'Z755' or PRD.PrimDiag = 'Z755' or SED.SecDiag = 'Z755' then 'Y'
 		Else NULL end as RespiteCare,
 		DATEDIFF(case when H.DischDateHospProvSpell IS null then '$rp_enddate' when H.DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else H.DischDateHospProvSpell end, H.StartDateHospProvSpell) as HSP_LOS,
 		DATEDIFF(case when A.EndDateWardStay IS null then '$rp_enddate' when A.EndDateWardStay > '$rp_enddate' then '$rp_enddate'  else A.EndDateWardStay end,A.StartDateWardStay) as WARD_LOS,
         CASE
 			   WHEN H.OrgIDProv is null then R.OrgIDProv
 			   ELSE H.OrgIDProv END as CombinedProvider,
 		CASE 
 			   WHEN HC1.OrgCodeComm IS NULL THEN R.OrgIDComm
 			   WHEN HC1.OrgCodeComm LIKE 'Q%' THEN R.OrgIDComm
 			   ELSE HC1.OrgCodeComm END AS CombinedCommissioner,
                
 --           commented out as this field is included with a generic name as RestrictiveID below 
           
 --                 RES.MHS515UniqId,
                RES.RestrictiveIntType as realrestraintinttype,
              CASE  
 			     WHEN RES.RestrictiveIntType = '15' then '2'	
                  WHEN RES.RestrictiveIntType = '14' then '3'	
                  WHEN RES.RestrictiveIntType = '16' then '4'	
                  WHEN RES.RestrictiveIntType = '17' then '5'	
                  WHEN RES.RestrictiveIntType = '04' then '6'	
                  WHEN RES.RestrictiveIntType = '01' then '7'	
                  WHEN RES.RestrictiveIntType = '12' then '8'
                  WHEN RES.RestrictiveIntType = '08' then '9'
                  WHEN RES.RestrictiveIntType = '11' then '10'
                  WHEN RES.RestrictiveIntType = '10' then '11'
                  WHEN RES.RestrictiveIntType = '07' then '12'
                  WHEN RES.RestrictiveIntType = '09' then '13'
                  WHEN RES.RestrictiveIntType = '13' then '14'
                  WHEN RES.RestrictiveIntType = '05' then '15'
                  WHEN RES.RestrictiveIntType = '06' then '16'
                  ELSE '1' END AS RestrictiveIntType,
                RES.StartDateRestrictiveIntType,
                RES.EndDateRestrictiveIntType,
                          
             CASE  	
                  WHEN RES.RestrictiveIntType = '15' then 'Chemical restraint - Injection (Non Rapid Tranquillisation)'	
                  WHEN RES.RestrictiveIntType = '14' then 'Chemical restraint - Injection (Rapid Tranquillisation)'	
                  WHEN RES.RestrictiveIntType = '16' then 'Chemical restraint - Oral'
                  WHEN RES.RestrictiveIntType = '17' then 'Chemical restraint - Other (not listed)'	
                  WHEN RES.RestrictiveIntType = '04' then 'Mechanical restraint'	
                  WHEN RES.RestrictiveIntType = '01' then 'Physical restraint - Prone'	
                  WHEN RES.RestrictiveIntType = '12' then 'Physical restraint - Kneeling'
                  WHEN RES.RestrictiveIntType = '08' then 'Physical restraint - Restrictive escort'
                  WHEN RES.RestrictiveIntType = '11' then 'Physical restraint - Seated'
                  WHEN RES.RestrictiveIntType = '10' then 'Physical restraint - Side'
                  WHEN RES.RestrictiveIntType = '07' then 'Physical restraint - Standing'
                  WHEN RES.RestrictiveIntType = '09' then 'Physical restraint - Supine'
                  WHEN RES.RestrictiveIntType = '13' then 'Physical restraint - Other (not listed)'
                  WHEN RES.RestrictiveIntType = '05' then 'Seclusion'
                  WHEN RES.RestrictiveIntType = '06' then 'Segregation'
                  ELSE 'No restraint type recorded'
             END AS RestrictiveIntTypeDesc,	
 
         CONCAT(RES.UniqRestrictiveIntIncID, RES.UniqRestrictiveIntTypeID) as RestrictiveID
         
 FROM	$db_source.MHS001MPI M 
 		LEFT JOIN $db_source.MHS101Referral as R ON M.person_ID = R.person_ID AND R.UniqMonthID = '$month_id'    
 		LEFT JOIN $db_source.MHS603ProvDiag as PVD ON R.person_ID = PVD.person_ID AND  PVD.UniqMonthID = '$month_id' AND PVD.provdiag ='Z755' -- and PVD.ic_use_submission_flag = 'y' -- needed for testing  in hue
 		LEFT JOIN $db_source.MHS604PrimDiag as PRD ON R.person_ID = PRD.person_ID AND PRD.UniqMonthID = '$month_id' AND PRD.primdiag ='Z755' -- and PRD.ic_use_submission_flag = 'y' -- needed for testing  in hue
 		LEFT JOIN $db_source.MHS605SecDiag as SED ON R.person_ID = SED.person_ID AND SED.UniqMonthID = '$month_id' AND SED.secdiag ='Z755' -- and SED.ic_use_submission_flag = 'y' -- needed  for testing in hue
 		LEFT JOIN $db_source.MHS501HospProvSpell as H  ON R.person_ID = H.person_ID AND H.UniqMonthID = '$month_id' and h.uniqservreqid = r.uniqservreqid-- and H.ic_use_submission_flag = 'y' -- needed for testing  in hue
 		LEFT JOIN $db_source.MHS502WardStay as A ON H.UniqHospProvSpellID = A.UniqHospProvSpellID AND A.UniqMonthID = '$month_id'-- and A.ic_use_submission_flag = 'y' -- needed  for testing in hue
 		LEFT JOIN $db_source.MHS503AssignedCareProf as ACP ON ACP.UniqHospProvSpellID = H.UniqHospProvSpellID AND ACP.UniqMonthID = '$month_id'-- and ACP.ic_use_submission_flag = 'y' -- needed  for testing in hue
 		LEFT JOIN $db_source.MHS504DelayedDischarge as DD ON H.UniqHospProvSpellID = DD.UniqHospProvSpellID AND DD.UniqMonthID = '$month_id' AND DD.EndDateDelayDisch IS NULL-- and DD.ic_use_submission_flag = 'y' -- needed for testing in hue
         LEFT JOIN 
                   (SELECT HC.UniqHospProvSpellID,
                            H.StartDateHospProvSpell,                       
                            H.DischDateHospProvSpell,  
                            CASE	WHEN O5.ORG_CODE IS NULL THEN O4.ORG_CODE
                            ELSE O5.ORG_CODE END AS OrgCodeComm,
                            StartDateOrgCodeComm,
                            EndDateOrgCodeComm,
                            dense_rank() over (partitiON by HC.UniqHospProvSpellID ORDER BY CASE WHEN ENDDateOrgCodeComm IS NULL THEN 1 ELSE 2 END asc, ENDDateOrgCodeComm DESC, StartDateOrgCodeComm DESC, MHS512UNIQID DESC) AS RANK
                       FROM $db_source.MHS512HospSpellCommAssPer    HC
                  LEFT JOIN $db_source.MHS501HospProvSpell H ON H.UniqHospProvSpellID = HC.UniqHospProvSpellID AND H.UniqMonthID = '$month_id' AND HC.StartDateOrgCodeComm <= H.DischDateHospProvSpell 
                                                        AND (HC.EndDateOrgCodeComm IS NULL OR HC.EndDateOrgCodeComm >= H.DischDateHospProvSpell) -- and H.ic_use_submission_flag = 'y' -- needed for testing  in hue
                  LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O4 ON O4.ORG_CODE = HC.OrgIDComm                       
                  LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O5 ON O5.ORG_CODE = substr(HC.OrgIDComm, 0, 3)  
                      WHERE HC.UniqMonthID = '$month_id')HC1 --and HC.ic_use_submission_flag = 'y' -- needed within where clause for testing  in hue) 
         ON H.UniqHospProvSpellID = HC1.UniqHospProvSpellID AND HC1.rank = 1
 LEFT JOIN $db_source.MHS505RestrictiveInterventInc as RI on RI.UniqHospProvSpellID = H.UniqHospProvSpellID
         LEFT JOIN $db_source.MHS515RestrictiveInterventType as RES ON RES.RestrictiveIntIncID = RI.RestrictiveIntIncID AND RES.person_ID = RI.person_ID 
                     AND ((RES.StartDateRestrictiveIntType BETWEEN '$rp_startdate' AND '$rp_enddate') 
                     OR (RES.EndDateRestrictiveIntType BETWEEN '$rp_startdate' AND '$rp_enddate')
                     OR (RES.EndDateRestrictiveIntType is null)) AND RES.UniqMonthID = '$month_id' -- and RES.ic_use_submission_flag = 'y' -- needed for testing  in hue
         WHERE  M.UniqMonthID = '$month_id' AND M.PatMRecInRP = True 
 --GBT: LDAFlag doesn't exist in v4.1
         --M.LDAFlag = True AND 
         AND M.Person_ID IN
         (SELECT LD.Person_ID from global_temp.LD_cohort LD)
    
 --GBT added in for under 18 output
       -- AND M.AgeRepPeriodEnd < 18 
        
 
 GROUP BY
 		M.Person_ID,
 		M.AgeRepPeriodEnd,
 		M.Gender,
 		M.NHSDEthnicity,
 		M.PatMRecInRP,
 		R.UniqServReqID,
 		R.ReferralRequestReceivedDate,
 		R.ServDischDate,
 		R.OrgIDProv,
 		R.OrgIDComm,
 		H.UniqHospProvSpellID,
 		H.StartDateHospProvSpell,
 		H.DischDateHospProvSpell,
 		H.MethAdmMHHospProvSpell,
 		H.DestOfDischHospProvSpell,
 		H.OrgIDProv,
 		H.PlannedDischDateHospProvSpell,
 		A.UniqWardStayID,
         A.SiteIDOfTreat,
 		A.StartDateWardStay,
 		A.EndDateWardStay,
 		A.WardLocDistanceHome,
 		A.WardSecLevel,
 		A.WardType,
 		DD.StartDateDelayDisch,
 		DD.EndDateDelayDisch,
 		DD.DelayDischReason,
 		CASE	WHEN PVD.ProvDiag = 'Z755' OR PRD.PrimDiag = 'Z755' OR SED.SecDiag = 'Z755' THEN 'Y'
 				ELSE NULL END,
                 
 		DATEDIFF(H.StartDateHospProvSpell, CASE WHEN H.DischDateHospProvSpell IS NULL THEN '$rp_enddate' WHEN H.DischDateHospProvSpell > '$rp_enddate' THEN '$rp_enddate'  ELSE H.DischDateHospProvSpell END),
 		DATEDIFF(A.StartDateWardStay, CASE WHEN A.EndDateWardStay IS NULL THEN '$rp_enddate' WHEN A.EndDateWardStay > '$rp_enddate' THEN '$rp_enddate'  ELSE A.EndDateWardStay END),
 		CASE	WHEN H.OrgIDProv IS NULL THEN R.OrgIDProv
 				ELSE H.OrgIDProv END,
 		CASE	WHEN HC1.OrgCodeComm IS NULL THEN R.OrgIDComm
 				WHEN HC1.OrgCodeComm LIKE 'Q%' THEN R.OrgIDComm
 				ELSE HC1.OrgCodeComm END
                 ,
               CONCAT(RES.UniqRestrictiveIntIncID, RES.UniqRestrictiveIntTypeID),
               RES.RestrictiveIntType,
                RES.StartDateRestrictiveIntType,
                RES.EndDateRestrictiveIntType,
 		CASE  
 			     WHEN RES.RestrictiveIntType = '15' then '2'	
                  WHEN RES.RestrictiveIntType = '14' then '3'	
                  WHEN RES.RestrictiveIntType = '16' then '4'	
                  WHEN RES.RestrictiveIntType = '17' then '5'	
                  WHEN RES.RestrictiveIntType = '04' then '6'	
                  WHEN RES.RestrictiveIntType = '01' then '7'	
                  WHEN RES.RestrictiveIntType = '12' then '8'
                  WHEN RES.RestrictiveIntType = '08' then '9'
                  WHEN RES.RestrictiveIntType = '11' then '10'
                  WHEN RES.RestrictiveIntType = '10' then '11'
                  WHEN RES.RestrictiveIntType = '07' then '12'
                  WHEN RES.RestrictiveIntType = '09' then '13'
                  WHEN RES.RestrictiveIntType = '13' then '14'
                  WHEN RES.RestrictiveIntType = '05' then '15'
                  WHEN RES.RestrictiveIntType = '06' then '16'
                  ELSE '1' END ,
                      
             CASE  	
                  WHEN RES.RestrictiveIntType = '15' then 'Chemical restraint - Injection (Non Rapid Tranquillisation)'	
                  WHEN RES.RestrictiveIntType = '14' then 'Chemical restraint - Injection (Rapid Tranquillisation)'	
                  WHEN RES.RestrictiveIntType = '16' then 'Chemical restraint - Oral'	
                  WHEN RES.RestrictiveIntType = '17' then 'Chemical restraint - Other (not listed)'	
                  WHEN RES.RestrictiveIntType = '04' then 'Mechanical restraint'	
                  WHEN RES.RestrictiveIntType = '01' then 'Physical restraint - Prone'	
                  WHEN RES.RestrictiveIntType = '12' then 'Physical restraint - Kneeling'
                  WHEN RES.RestrictiveIntType = '08' then 'Physical restraint - Restrictive escort'
                  WHEN RES.RestrictiveIntType = '11' then 'Physical restraint - Seated'
                  WHEN RES.RestrictiveIntType = '10' then 'Physical restraint - Side'
                  WHEN RES.RestrictiveIntType = '07' then 'Physical restraint - Standing'
                  WHEN RES.RestrictiveIntType = '09' then 'Physical restraint - Supine'
                  WHEN RES.RestrictiveIntType = '13' then 'Physical restraint - Other (not listed)'
                  WHEN RES.RestrictiveIntType = '05' then 'Seclusion'
                  WHEN RES.RestrictiveIntType = '06' then 'Segregation'
                  ELSE '1 No restraint type recorded'
             END;

# COMMAND ----------

 %sql
 --- THESE TEMP TABLES TRANSFORM THE LIST OF PATIENTS NEEDED FOR REPORTING TO INCLUDE VARIOUS COMMISSIONER FIELDS NEEDED FOR SSRS REPORTING--
 
 -------------- Join the name of the provider onto LDA_Data to make the provider split easier.
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_Data_P AS
 
 SELECT LD.*,O.NAME
         ,O.ORG_TYPE_CODE AS ORG_TYPE_CODE_PROV
 FROM global_temp.LDA_Data LD
 LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O 
 on LD.combinedprovider = O.ORG_CODE;
 
 ----- get the right commissioner in here -----
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_Data_C AS
 select L.*
        ,case WHEN O4.ORG_CODE is null and O5.ORG_CODE is null then 'Unknown'
        WHEN O5.ORG_CODE IS NULL THEN O4.ORG_CODE
        ELSE O5.ORG_CODE 
        END as OrgCode
        ,CASE WHEN O4.NAME is null and O5.NAME is null then 'Unknown'
        WHEN O5.NAME IS NULL THEN O4.NAME
        ELSE O5.NAME 
        END as OrgName
 From global_temp.LDA_Data_P L
 left join global_temp.RD_ORG_DAILY_LATEST_LDA O4 
       on O4.ORG_CODE = (case when L.CombinedCommissioner like 'YDD%' then L.CombinedCommissioner  
       else (left(L.CombinedCommissioner,3)) 
       end)                       
 left join global_temp.RD_ORG_DAILY_LATEST_LDA O5 
       on O5.ORG_CODE = left(L.CombinedCommissioner,3) ;
       
  
 ---- join to the correct TCP/Region -------
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_Data_T AS
 Select L.*
        ,case when TCP_Code IS null then 'No TCP Mapping' 
        else TCP_Code 
        end as TCP_Code
        ,TCP_NAME
        ,case when C.Region_code is null then 'Invalid' 
        else C.Region_code 
        end as Region_code
        ,case when C.Region_name is null then 'Invalid' 
        else C.Region_name 
        end as Region_name
 from global_temp.LDA_Data_C L
 left join global_temp.COMMISSIONERS C
       on C.ORG_CODE = L.OrgCode;
 
 ----- Convert all the commissioner groupings --------
 
 INSERT INTO $db_output.LDA_Data_1
 
 -- CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_Data_1 AS
 
 select L.*,R.ORG_TYPE_CODE as ORG_TYPE_ORG_R, r.ORG_CODE AS ORG_CODE_R,R.NAME AS ORG_NAME_R
       ,case when R.ORG_TYPE_CODE IS NULL then 'Invalid' 
       else R.ORG_TYPE_CODE 
       end as ORG_TYPE_CODE
 from global_temp.LDA_Data_T L
 left join global_temp.RD_ORG_DAILY_LATEST_LDA R 
     on R.ORG_CODE = L.OrgCode
 
 where respitecare is null
   

# COMMAND ----------

 %sql
 -- creates table needed to create prov no ips table
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_data_prov AS
 
 SELECT LD.*,O.NAME
 FROM global_temp.LDA_Data LD
 LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O on LD.combinedprovider = O.ORG_CODE

# COMMAND ----------

 %sql 
 
 ---- This table below creates a list of Provider Orgs that DO NOT have any inpatients  ---------
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ProvNoIPs AS
 
 select REF_OrgCodeProv as OrgCode
 from global_temp.LDA_Data_Prov
 where UniqHospProvSpellID is not null
 group by REF_OrgCodeProv
 order by 1

# COMMAND ----------

 %sql
 -- needed to calculate all hospital spells measures
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW HSP_Spells AS
 
 SELECT 
 UniqHospProvSpellID,
 M.AgeRepPeriodEnd,
 StartDateHospProvSpell,
 DischDateHospProvSpell,
 DATEDIFF(case when DischDateHospProvSpell IS null then '$rp_enddate' when DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else DischDateHospProvSpell end ,StartDateHospProvSpell) as HSP_LOS
 FROM  $db_source.MHS001MPI M
 inner join $db_source.MHS501HospProvSpell H on  M.Person_ID = H.Person_ID  and H.UniqMonthID = '$month_id' --and H.IC_Use_Submission_Flag = 'Y'
 WHERE M.UniqMonthID = '$month_id' 
 --and LD.IC_Use_Submission_Flag = 'Y'
 --and LD.LDAFlag = True 
 --GBT: LDAFlag doesn't exist in v4.1 - replaced with 2 lines below
         
         AND M.Person_ID IN
         (SELECT LD.Person_ID from global_temp.LD_cohort LD)
 and M.PatMRecInRP = True

# COMMAND ----------

 %sql
 -- needed to create all ward spells measures
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW WARD_Spells AS
 
 SELECT 
 UniqWardStayID,
 M.AgeRepPeriodEnd,
 StartDateWardStay,
 EndDateWardStay,
 wardseclevel,
 DATEDIFF(case when EndDateWardStay IS null then '$rp_enddate' when EndDateWardStay > '$rp_enddate' then '$rp_enddate'  else EndDateWardStay end,StartDateWardStay) as WARD_LOS
 FROM 
 $db_source.MHS001MPI M
 inner join $db_source.MHS502WardStay W on  M.Person_ID = W.Person_ID 
 --and W.IC_Use_Submission_Flag = 'Y' 
 and W.UniqMonthID = '$month_id'
 WHERE
 M.UniqMonthID = '$month_id' 
 -- and LD.ic_Use_Submission_Flag = 'Y'  
 --and LD.LDAFlag = True 
 --GBT: LDAFlag doesn't exist in v4.1 - replaced with 2 lines below
         
         AND M.Person_ID IN
         (SELECT LD.Person_ID from global_temp.LD_cohort LD)
 and M.PatMRecInRP = True

# COMMAND ----------

 %sql
 -- needed to create MHA measures
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA AS
 
 SELECT
 M.Person_ID,
 B.UniqHospProvSpellID,
 B.StartDateHospProvSpell,
 B.DischDateHospProvSpell,
 DATEDIFF(case when B.DischDateHospProvSpell IS null then '$rp_enddate' when B.DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else B.DischDateHospProvSpell end,B.StartDateHospProvSpell) as HSP_LOS,
 C.UniqMHActEpisodeID,
 C.MHS401UniqID,
 C.StartDateMHActLegalStatusClass,
 C.EndDateMHActLegalStatusClass,
 c.rank,
 C.NHSDLegalStatus,
 CASE
 WHEN (CD.Person_ID is not null or CTO.Person_ID is not null or CR.Person_ID is not null) AND
 NHSDLegalStatus IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38') THEN 'Informal'
 WHEN C.NHSDLegalStatus IS NULL or NHSDLegalStatus = '98' THEN 'Informal'
 WHEN C.NHSDLegalStatus = '01' THEN 'Informal'
 WHEN C.NHSDLegalStatus in ('02','03') THEN 'Part 2'
 WHEN C.NHSDLegalStatus in ('07','08','10','12','13','14','16','18','34') THEN 'Part 3 no restrictions'
 WHEN C.NHSDLegalStatus in ('09','15','17') THEN 'Part 3 with restrictions'
 WHEN C.NHSDLegalStatus in ('04','05','06','19','20','31','32','35','36','37','38') THEN 'Other'
 WHEN C.NHSDLegalStatus in ('99') THEN 'Informal'
 ELSE 'Informal'
 end as MHA_Group,
 CASE 
        WHEN CD.Person_ID is not null or CTO.Person_ID is not null or CR.Person_ID is not null then 'Y' 
        ELSE null end as Short_Term_Flag
 FROM
 $db_source.MHS001MPI M 
 INNER JOIN $db_source.MHS501HospProvSpell B ON M.Person_ID = B.Person_ID --AND B.IC_Use_Submission_Flag = 'Y' 
 AND B.UniqMonthID = '$month_id'
 LEFT JOIN (SELECT A.Person_ID,
                   A.UniqHospProvSpellID,
                   A.StartDateHospProvSpell,
                   A.DischDateHospProvSpell,
                   UniqMHActEpisodeID,
                   MHS401UniqID,
                   --A.IC_Use_Submission_Flag,
                   StartDateMHActLegalStatusClass,
                   EndDateMHActLegalStatusClass,
                   NHSDLegalStatus,
                   dense_rank() over (partition by A.Person_ID, A.DischDateHospProvSpell order by case when EndDateMHActLegalStatusClass is null then 1 else 2 end asc, EndDateMHActLegalStatusClass DESC, case when NHSDLegalStatus in ('02','03') then 1 else 2 end, StartDateMHActLegalStatusClass DESC, MHS401UNIQID DESC) AS RANK
              FROM $db_source.MHS501HospProvSpell A
              inner join $db_source.MHS401MHActPeriod B on A.Person_ID = B.Person_ID and B.UniqMonthID = '$month_id' 
                         --AND B.IC_Use_Submission_Flag = 'Y' 
                         and ((B.EndDateMHActLegalStatusClass is null and a.DischDateHospProvSpell is null) 
                         or (B.StartDateMHActLegalStatusClass <= A.DischDateHospProvSpell and B.EndDateMHActLegalStatusClass >= A.DischDateHospProvSpell))
              WHERE A.UniqMonthID = '$month_id' --AND A.IC_Use_Submission_Flag = 'Y'
              ) C 
 ON B.Person_ID = C.Person_ID and ((C.EndDateMHActLegalStatusClass is null and B.DischDateHospProvSpell is null) or (C.StartDateMHActLegalStatusClass <= B.DischDateHospProvSpell and C.EndDateMHActLegalStatusClass >= B.DischDateHospProvSpell)) and C.rank = 1
 left join $db_source.MHS403ConditionalDischarge CD on CD.Person_ID = B.Person_ID and CD.UniqMonthID = '$month_id' --and CD.IC_Use_Submission_Flag = 'Y' 
 and ((CD.EndDateMHCondDisch is null and B.DischDateHospProvSpell is null) or (CD.StartDateMHCondDisch <= B.DischDateHospProvSpell and  CD.EndDateMHCondDisch >= B.DischDateHospProvSpell))
 left join $db_source.MHS404CommTreatOrder CTO on CTO.Person_ID = B.Person_ID and CTO.UniqMonthID = '$month_id' --and CTO.IC_Use_Submission_Flag = 'y' 
 and ((CTO.EndDateCommTreatOrd is null and B.DischDateHospProvSpell is null) or (CTO.StartDateCommTreatOrd <= B.DischDateHospProvSpell and  CTO.EndDateCommTreatOrd >= B.DischDateHospProvSpell))
 left join $db_source.MHS405CommTreatOrderRecall CR on CR.Person_ID = B.Person_ID and CR.UniqMonthID = '$month_id' --and CR.IC_Use_Submission_Flag = 'y' 
 and ((CR.EndDateCommTreatOrdRecall is null and B.DischDateHospProvSpell is null) or (CR.StartDateCommTreatOrdRecall <= B.DischDateHospProvSpell and  CR.EndDateCommTreatOrdRecall >= B.DischDateHospProvSpell))
 WHERE
 --A.LDAFlag = True 
 --GBT: LDAFlag doesn't exist in v4.1 - replaced with 2 lines below
         
        M.Person_ID IN
         (SELECT LD.Person_ID from global_temp.LD_cohort LD)
 --AND A.IC_Use_Submission_Flag = 'Y' 
 AND 
 M.PatMRecInRP = True  and M.UniqMonthID = '$month_id'
 GROUP BY
 M.Person_ID,
 B.UniqHospProvSpellID,
 B.StartDateHospProvSpell,
 B.DischDateHospProvSpell,
 DATEDIFF(case when B.DischDateHospProvSpell IS null then '$rp_enddate' when B.DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else b.DischDateHospProvSpell end,B.StartDateHospProvSpell),
 C.UniqMHActEpisodeID,
 C.MHS401UniqID,
 C.StartDateMHActLegalStatusClass,
 C.EndDateMHActLegalStatusClass,
 c.rank,
 C.NHSDLegalStatus,
 CASE
 WHEN (CD.Person_ID is not null or CTO.Person_ID is not null or CR.Person_ID is not null) AND
 NHSDLegalStatus IN ('02', '03', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '18', '31', '32', '37', '38') THEN 'Informal'
 WHEN C.NHSDLegalStatus IS NULL or NHSDLegalStatus = '98' THEN 'Informal'
 WHEN C.NHSDLegalStatus = '01' THEN 'Informal'
 WHEN C.NHSDLegalStatus in ('02','03') THEN 'Part 2'
 WHEN C.NHSDLegalStatus in ('07','08','10','12','13','14','16','18','34') THEN 'Part 3 no restrictions'
 WHEN C.NHSDLegalStatus in ('09','15','17') THEN 'Part 3 with restrictions'
 WHEN C.NHSDLegalStatus in ('04','05','06','19','20','31','32','35','36','37','38') THEN 'Other'
 WHEN C.NHSDLegalStatus in ('99') THEN 'Informal'
 ELSE 'Informal'
 end,
 CASE 
        WHEN CD.Person_ID is not null or CTO.Person_ID is not null or CR.Person_ID is not null then 'Y' 
        ELSE null end

# COMMAND ----------

 %sql
 ------------------ TABLE 1 - NATIONAL TOTAL COUNTS
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table1_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 1 AS TableNumber,
 'Total' AS PrimaryMeasure,
 1 AS PrimaryMeasureNumber,
 'Total' AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
 COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 from  $db_output.LDA_Data_1

# COMMAND ----------

 %sql
 -------------- TABLE 2 - AGE SPLITS
 
 --AGE
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table2_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 2 AS TableNumber,
 'Age' AS PrimaryMeasure,
 
 --GBT: add this bit in for U18 output
 --  AgeRepPeriodEnd AS PrimaryMeasureNumber,
 --  AgeRepPeriodEnd AS PrimarySplit,
 
 --GBT: take this bit out for U18 output
 CASE
        WHEN AgeRepPeriodEnd between 0 and 17 then '1'
 	   WHEN AgeRepPeriodEnd between 18 and 24 then '2'
 	   WHEN AgeRepPeriodEnd between 25 and 34 then '3'
 	   WHEN AgeRepPeriodEnd between 35 and 44 then '4'
 	   WHEN AgeRepPeriodEnd between 45 and 54 then '5'
 	   WHEN AgeRepPeriodEnd between 55 and 64 then '6'
 	   WHEN AgeRepPeriodEnd > 64 then '7'
        ELSE '8'
        END AS PrimaryMeasureNumber,
 CASE
        WHEN AgeRepPeriodEnd between 0 and 17 then 'Under 18'
        WHEN AgeRepPeriodEnd between 18 and 24 then '18-24'
        WHEN AgeRepPeriodEnd between 25 and 34 then '25-34'
 	   WHEN AgeRepPeriodEnd between 35 and 44 then '35-44'
 	   WHEN AgeRepPeriodEnd between 45 and 54 then '45-54'
 	   WHEN AgeRepPeriodEnd between 55 and 64 then '55-64'
 	   WHEN AgeRepPeriodEnd > 64 then '65 and Over'
        ELSE 'Unknown'
        END AS PrimarySplit,
 --GBT: up to here
 
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
 COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY
 
 
 CASE
        WHEN AgeRepPeriodEnd between 0 and 17 then '1'
 	   WHEN AgeRepPeriodEnd between 18 and 24 then '2'
 	   WHEN AgeRepPeriodEnd between 25 and 34 then '3'
 	   WHEN AgeRepPeriodEnd between 35 and 44 then '4'
 	   WHEN AgeRepPeriodEnd between 45 and 54 then '5'
 	   WHEN AgeRepPeriodEnd between 55 and 64 then '6'
 	   WHEN AgeRepPeriodEnd > 64 then '7'
        ELSE '8'
        END,
 CASE
        WHEN AgeRepPeriodEnd between 0 and 17 then 'Under 18'
        WHEN AgeRepPeriodEnd between 18 and 24 then '18-24'
        WHEN AgeRepPeriodEnd between 25 and 34 then '25-34'
 	   WHEN AgeRepPeriodEnd between 35 and 44 then '35-44'
 	   WHEN AgeRepPeriodEnd between 45 and 54 then '45-54'
 	   WHEN AgeRepPeriodEnd between 55 and 64 then '55-64'
 	   WHEN AgeRepPeriodEnd > 64 then '65 and Over'
        ELSE 'Unknown'
        END 

# COMMAND ----------

 %sql
 ---------------- TABLE 3 - GENDER SPLIT
 
 -- GENDER
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table3_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 3 AS TableNumber,
 'Gender' AS PrimaryMeasure,
 CASE
        WHEN Gender = '1' then '1'
        WHEN Gender = '2' then '2'
        WHEN Gender = '9' then '3'
        ELSE '4'
        END AS PrimaryMeasureNumber,
 CASE
        WHEN Gender = '1' then 'Male'
        WHEN Gender = '2' then 'Female'
        WHEN Gender = '9' then 'Not stated'
        ELSE 'Unknown'
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY 
 CASE
        WHEN Gender = '1' then '1'
        WHEN Gender = '2' then '2'
        WHEN Gender = '9' then '3'
        ELSE '4'
        END,
 CASE
        WHEN Gender = '1' then 'Male'
        WHEN Gender = '2' then 'Female'
        WHEN Gender = '9' then 'Not stated'
        ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
 ----------------- TABLE 4 - ETHNICITY SPLIT
 
 --ETHNICITY
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table4_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 4 AS TableNumber,
 'Ethnicity' AS PrimaryMeasure,
 CASE
        WHEN NHSDEthnicity IN ('A','B','C') then '1'
        WHEN NHSDEthnicity IN ('D','E','F','G') then '2'
        WHEN NHSDEthnicity IN ('H','J','K','L') then '3'
        WHEN NHSDEthnicity IN ('M','N','P') then '4'
        WHEN NHSDEthnicity IN ('R','S') then '5'
        WHEN NHSDEthnicity IN ('Z') then '6'
        ELSE '7'
        END AS PrimaryMeasureNumber,
 CASE
        WHEN NHSDEthnicity IN ('A','B','C') then 'White'
        WHEN NHSDEthnicity IN ('D','E','F','G') then 'Mixed'
        WHEN NHSDEthnicity IN ('H','J','K','L') then 'Asian'
        WHEN NHSDEthnicity IN ('M','N','P') then 'Black'
        WHEN NHSDEthnicity IN ('R','S') then 'Other'
        WHEN NHSDEthnicity IN ('Z') then 'Not Stated'
        ELSE 'Unknown'
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY 
 CASE
        WHEN NHSDEthnicity IN ('A','B','C') then '1'
        WHEN NHSDEthnicity IN ('D','E','F','G') then '2'
        WHEN NHSDEthnicity IN ('H','J','K','L') then '3'
        WHEN NHSDEthnicity IN ('M','N','P') then '4'
        WHEN NHSDEthnicity IN ('R','S') then '5'
        WHEN NHSDEthnicity IN ('Z') then '6'
        ELSE '7'
        END,
 CASE
        WHEN NHSDEthnicity IN ('A','B','C') then 'White'
        WHEN NHSDEthnicity IN ('D','E','F','G') then 'Mixed'
        WHEN NHSDEthnicity IN ('H','J','K','L') then 'Asian'
        WHEN NHSDEthnicity IN ('M','N','P') then 'Black'
        WHEN NHSDEthnicity IN ('R','S') then 'Other'
        WHEN NHSDEthnicity IN ('Z') then 'Not Stated'
        ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
 --------- TABLE 5 - DISTANCE FROM HOME SPLIT
 
 -- DISTANCE
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table5_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 5 AS TableNumber,
 'Distance from Home' AS PrimaryMeasure,
 CASE 
        WHEN WardLocDistanceHome between 0 and 10 then '1'
        WHEN WardLocDistanceHome between 11 and 20 then '2'
        WHEN WardLocDistanceHome between 21 and 50 then '3'
        WHEN WardLocDistanceHome between 51 and 100 then '4'
        WHEN WardLocDistanceHome > 100 then '5'  
        ELSE '6'
        END AS PrimaryMeasureNumber,
 CASE 
        WHEN WardLocDistanceHome between 0 and 10 then 'Up to 10km'
        WHEN WardLocDistanceHome between 11 and 20 then '11-20km'
        WHEN WardLocDistanceHome between 21 and 50 then '21-50km'
        WHEN WardLocDistanceHome between 51 and 100 then '51-100km'
        WHEN WardLocDistanceHome > 100 then 'Over 100km'       
        ELSE 'Unknown  '
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY 
 CASE 
        WHEN WardLocDistanceHome between 0 and 10 then '1'
        WHEN WardLocDistanceHome between 11 and 20 then '2'
        WHEN WardLocDistanceHome between 21 and 50 then '3'
        WHEN WardLocDistanceHome between 51 and 100 then '4'
        WHEN WardLocDistanceHome > 100 then '5'  
        ELSE '6'
        END,
 CASE 
        WHEN WardLocDistanceHome between 0 and 10 then 'Up to 10km'
        WHEN WardLocDistanceHome between 11 and 20 then '11-20km'
        WHEN WardLocDistanceHome between 21 and 50 then '21-50km'
        WHEN WardLocDistanceHome between 51 and 100 then '51-100km'
        WHEN WardLocDistanceHome > 100 then 'Over 100km'       
        ELSE 'Unknown  '
        END  

# COMMAND ----------

 %sql
 ----------- TABLE 6 - WARD SECURITY SPLIT
 
 -- WARD SECURITY
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table6_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 6 AS TableNumber,
 'Ward security' AS PrimaryMeasure,
 CASE 
        WHEN WardSecLevel = '0' then '1'
        WHEN WardSecLevel = '1' then '2'
        WHEN WardSecLevel = '2' then '3'
        WHEN WardSecLevel = '3' then '4'
        ELSE '5'
        END AS PrimaryMeasureNumber,
 CASE 
        WHEN WardSecLevel = '0' then 'General'
        WHEN WardSecLevel = '1' then 'Low Secure'
        WHEN WardSecLevel = '2' then 'Medium Secure'
        WHEN WardSecLevel = '3' then 'High Secure'
        ELSE 'Unknown '
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY 
 CASE 
        WHEN WardSecLevel = '0' then '1'
        WHEN WardSecLevel = '1' then '2'
        WHEN WardSecLevel = '2' then '3'
        WHEN WardSecLevel = '3' then '4'
        ELSE '5'
        END,
 CASE 
        WHEN WardSecLevel = '0' then 'General'
        WHEN WardSecLevel = '1' then 'Low Secure'
        WHEN WardSecLevel = '2' then 'Medium Secure'
        WHEN WardSecLevel = '3' then 'High Secure'
        ELSE 'Unknown '
        END

# COMMAND ----------

 %sql
 
 ---------- TABLE 7 - PLANNED DISCHARGE SPLIT
 
 --Planned Discharge
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table7_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 7 AS TableNumber,
 'Planned Discharge Date Present' as PrimaryMeasure,
 CASE
        WHEN PlannedDischDateHospProvSpell is not null then '1'
        WHEN PlannedDischDateHospProvSpell is null then '2' 
        END as PrimaryMeasureNumber,
 CASE
        WHEN PlannedDischDateHospProvSpell is not null then 'Present'
        WHEN PlannedDischDateHospProvSpell is null then 'Not present' END as PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY
 CASE
        WHEN PlannedDischDateHospProvSpell is not null then '1'
        WHEN PlannedDischDateHospProvSpell is null then '2' END,
 CASE
        WHEN PlannedDischDateHospProvSpell is not null then 'Present'
        WHEN PlannedDischDateHospProvSpell is null then 'Not present' END

# COMMAND ----------

 %sql
 ------------------ TABLE 8 -PLANNED DISCHARGE DATE SPLIT
 
 --Planned Discharge
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table8_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 8 AS TableNumber,
 'Time to Planned Discharge' as PrimaryMeasure,
 CASE
        --WHEN PlannedDischDateHospProvSpell is null then '1'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then '2'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '3'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '4'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '5'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '6'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '7'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then '8'  
        END as PrimaryMeasureNumber,
 CASE
        --WHEN PlannedDischDateHospProvSpell is null then 'No planned discharge'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then 'Planned discharge overdue'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '0 to 3 months'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '3 to 6 months'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '6 to 12 months'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '1 to 2 years'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '2 to 5 years'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then 'Over 5 years'  
        END as PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY
 CASE
        --WHEN PlannedDischDateHospProvSpell is null then '1'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then '2'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '3'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '4'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '5'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '6'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '7'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then '8'  
        END,
 CASE
        --WHEN PlannedDischDateHospProvSpell is null then 'No planned discharge'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') < 0 then 'Planned discharge overdue'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 0 and 91 then '0 to 3 months'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 92 and 182 then '3 to 6 months'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 183 and 365 then '6 to 12 months'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 366 and 730 then '1 to 2 years'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') between 731 and 1826 then '2 to 5 years'
        WHEN DATEDIFF(PlannedDischDateHospProvSpell,'$rp_enddate') > 1826 then 'Over 5 years'  
        END

# COMMAND ----------

 %sql
 ------------------- TABLE 9 - RESPITE CARE SPLIT
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table9_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 9 AS TableNumber,
 'Respite care' as PrimaryMeasure,
 CASE
        WHEN RespiteCare = 'Y' then '1'
        ELSE '2'  
        END as PrimaryMeasureNumber,
 CASE
        WHEN RespiteCare = 'Y' then 'Admitted for respite care'
        ELSE 'Not admitted for respite care'  
        END as PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY
 CASE
        WHEN RespiteCare = 'Y' then '1'
        ELSE '2'  
        END,
 CASE
        WHEN RespiteCare = 'Y' then 'Admitted for respite care'
        ELSE 'Not admitted for respite care'  
        END

# COMMAND ----------

 %sql
 -------------------- TABLE 10 - LENGTH OF STAY SPLIT. 
 
 --LOS
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table10_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 10 AS TableNumber,
 'Length of stay' AS PrimaryMeasure,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '1'
        WHEN H.HSP_LOS between 4 and 7 then '2'
        WHEN H.HSP_LOS between 8 and 14 then '3'
        WHEN H.HSP_LOS between 15 and 28 then '4'
        WHEN H.HSP_LOS between 29 and 91 then '5'
        WHEN H.HSP_LOS between 92 and 182 then '6'
        WHEN H.HSP_LOS between 183 and 365 then '7'
        WHEN H.HSP_LOS between 366 and 730 then '8'
        WHEN H.HSP_LOS between 731 and 1826 then '9'
        WHEN H.HSP_LOS between 1827 and 3652 then '10'
        WHEN H.HSP_LOS > 3652 then '11'
        ELSE '12'
        END AS PrimaryMeasureNumber,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
        WHEN H.HSP_LOS > 3652 then '10+ years'
        ELSE 'Unknown'
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 '0' as  WardStaysInCarePreviousMonth,
 '0' as  WardStaysAdmissionsInMonth,
 '0' as  WardStaysDischargedInMonth,
 '0' as  WardStaysAdmittedAndDischargedInMonth,
 '0' as  WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
 global_temp.HSP_Spells H
 GROUP BY 
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '1'
        WHEN H.HSP_LOS between 4 and 7 then '2'
        WHEN H.HSP_LOS between 8 and 14 then '3'
        WHEN H.HSP_LOS between 15 and 28 then '4'
        WHEN H.HSP_LOS between 29 and 91 then '5'
        WHEN H.HSP_LOS between 92 and 182 then '6'
        WHEN H.HSP_LOS between 183 and 365 then '7'
        WHEN H.HSP_LOS between 366 and 730 then '8'
        WHEN H.HSP_LOS between 731 and 1826 then '9'
        WHEN H.HSP_LOS between 1827 and 3652 then '10'
        WHEN H.HSP_LOS > 3652 then '11'
        ELSE '12'
        END,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
        WHEN H.HSP_LOS > 3652 then '10+ years'
        ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
 ------------------- TABLE 11 - DISCHARGE DESTINATION GROUPED SPLIT
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table11_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 11 AS TableNumber,
 'Discharge Destination Grouped' as PrimaryMeasure,
 CASE
        WHEN DestOfDischHospProvSpell in ('19','29','55', '56','66') then '1'
        WHEN DestOfDischHospProvSpell in ('30','48','49','50','51','52','53','84','87') then '2'
        WHEN DestOfDischHospProvSpell in ('37','40', '42')then '3'
        WHEN DestOfDischHospProvSpell in ('88') then '4'
        WHEN DestOfDischHospProvSpell in ('98') then '5'
        WHEN DestOfDischHospProvSpell in ('79') then '6'
        WHEN DestOfDischHospProvSpell in ('89') then '7'
        ELSE '8'
        END as PrimaryMeasureNumber,
        
 CASE
        WHEN DestOfDischHospProvSpell in ('19','29','55', '56','66') then 'Community'
        WHEN DestOfDischHospProvSpell in ('30','48','49','50','51','52','53','84','87') then 'Hospital'
        WHEN DestOfDischHospProvSpell in ('37','40', '42') then 'Penal Establishment / Court'
        WHEN DestOfDischHospProvSpell in ('88') then 'Hospice'  
        WHEN DestOfDischHospProvSpell in ('98') then 'Not Applicable'
        WHEN DestOfDischHospProvSpell in ('79') then 'Patient died'
        WHEN DestOfDischHospProvSpell in ('89') then 'Organisation responsible for forced repatriation'     
        ELSE 'Not Known'
        END as PrimarySplit,       
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY       
 CASE
        WHEN DestOfDischHospProvSpell in ('19','29','55', '56','66') then '1'
        WHEN DestOfDischHospProvSpell in ('30','48','49','50','51','52','53','84','87') then '2'
        WHEN DestOfDischHospProvSpell in ('37','40', '42')then '3'
        WHEN DestOfDischHospProvSpell in ('88') then '4'
        WHEN DestOfDischHospProvSpell in ('98') then '5'
        WHEN DestOfDischHospProvSpell in ('79') then '6'
        WHEN DestOfDischHospProvSpell in ('89') then '7'
        ELSE '8'
        END,
        
 CASE
        WHEN DestOfDischHospProvSpell in ('19','29','55', '56','66') then 'Community'
        WHEN DestOfDischHospProvSpell in ('30','48','49','50','51','52','53','84','87') then 'Hospital'
        WHEN DestOfDischHospProvSpell in ('37','40', '42') then 'Penal Establishment / Court'
        WHEN DestOfDischHospProvSpell in ('88') then 'Hospice'
        WHEN DestOfDischHospProvSpell in ('98') then 'Not Applicable'
        WHEN DestOfDischHospProvSpell in ('79') then 'Patient died'
        WHEN DestOfDischHospProvSpell in ('89') then 'Organisation responsible for forced repatriation'       
        ELSE 'Not Known'
        END

# COMMAND ----------

 %sql
 ------------------- TABLE 12 - WARD TYPE SPLIT
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table12_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 12 AS TableNumber,
 'Ward Type' as PrimaryMeasure,
 CASE
        WHEN WardType = '01' then '1'
        WHEN WardType = '02' then '2'
        WHEN WardType = '03' then '3'
        WHEN WardType = '04' then '4'
        WHEN WardType = '05' then '5'
        WHEN WardType = '06' then '6'
        else '7'
        END as PrimaryMeasureNumber,
 CASE
        WHEN WardType = '01' then 'Child and adolescent mental health ward'
        WHEN WardType = '02' then 'Paediatric ward'
        WHEN WardType = '03' then 'Adult mental health ward'
        WHEN WardType = '04' then 'Non mental health ward'
        WHEN WardType = '05' then 'Learning disabilities ward'
        WHEN WardType = '06' then 'Older peoples mental health ward'
        ELSE 'Unknown   '
        END as PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 GROUP BY
 CASE
        WHEN WardType = '01' then '1'
        WHEN WardType = '02' then '2'
        WHEN WardType = '03' then '3'
        WHEN WardType = '04' then '4'
        WHEN WardType = '05' then '5'
        WHEN WardType = '06' then '6'
        else '7'
        END,
 CASE
        WHEN WardType = '01' then 'Child and adolescent mental health ward'
        WHEN WardType = '02' then 'Paediatric ward'
        WHEN WardType = '03' then 'Adult mental health ward'
        WHEN WardType = '04' then 'Non mental health ward'
        WHEN WardType = '05' then 'Learning disabilities ward'
        WHEN WardType = '06' then 'Older peoples mental health ward'
        ELSE 'Unknown   '
        END

# COMMAND ----------

 %sql
 ------------ TABLE 13 - MHA
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table13_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 13 AS TableNumber,
 'Mental Health Act' as PrimaryMeasure,
 CASE
 WHEN MHA_Group = 'Informal' then '1'
 WHEN MHA_Group = 'Part 2' then '2'
 WHEN MHA_Group = 'Part 3 no restrictions' then '3'
 WHEN MHA_Group = 'Part 3 with restrictions' then '4'
 WHEN MHA_Group = 'Other' then '5'
 ELSE '1'
 end as PrimaryMeasureNumber,
 CASE
 WHEN MHA_Group = 'Informal' then 'Informal'
 WHEN MHA_Group = 'Part 2' then 'Part 2'
 WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
 WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
 WHEN MHA_Group = 'Other' then 'Other sections'
 ELSE 'Informal'
 end as PrimaryMeasureSplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 '0' as  WardStaysInCarePreviousMonth,
 '0' as  WardStaysAdmissionsInMonth,
 '0' as  WardStaysDischargedInMonth,
 '0' as  WardStaysAdmittedAndDischargedInMonth,
 '0' as  WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM global_temp.MHA
 GROUP BY
 CASE
 WHEN MHA_Group = 'Informal' then '1'
 WHEN MHA_Group = 'Part 2' then '2'
 WHEN MHA_Group = 'Part 3 no restrictions' then '3'
 WHEN MHA_Group = 'Part 3 with restrictions' then '4'
 WHEN MHA_Group = 'Other' then '5'
 ELSE '1'
 end,
 CASE
 WHEN MHA_Group = 'Informal' then 'Informal'
 WHEN MHA_Group = 'Part 2' then 'Part 2'
 WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
 WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
 WHEN MHA_Group = 'Other' then 'Other sections'
 ELSE 'Informal'
 end

# COMMAND ----------

 %sql
 
 ------------ TABLE 14 - Delayed Discharges----------------------------
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table14_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' AS OrgCode,
 'National' as OrgName,
 14 AS TableNumber,
 'Delayed Discharges' as PrimaryMeasure,
 CASE
        when DelayDischReason = 'A2' then '1'
        when DelayDischReason = 'B1' then '2'
        when DelayDischReason = 'C1' then '3'
        when DelayDischReason = 'D1' then '4'
        when DelayDischReason = 'D2' then '5'
        when DelayDischReason = 'E1' then '6'
        when DelayDischReason = 'F2' then '7'
        when DelayDischReason = 'G2' then '8'
        when DelayDischReason = 'G3' then '9'
        when DelayDischReason = 'G4' then '10'
        when DelayDischReason = 'G5' then '11'
        when DelayDischReason = 'G6' then '12'
        when DelayDischReason = 'G7' then '13'
        when DelayDischReason = 'G8' then '14'
        when DelayDischReason = 'G9' then '15'
        when DelayDischReason = 'G10' then '16'
        when DelayDischReason = 'G11' then '17'
        when DelayDischReason = 'G12' then '18'
        when DelayDischReason = 'H1' then '19'
        when DelayDischReason = 'I2' then '20'
        when DelayDischReason = 'I3' then '21'
        when DelayDischReason = 'J2' then '22'
        when DelayDischReason = 'K2' then '23'
        when DelayDischReason = 'L1' then '24'
        when DelayDischReason = 'M1' then '25'
        when DelayDischReason = 'N1' then '26'
        when DelayDischReason = 'P1' then '27'
        when DelayDischReason = 'Q1' then '28'
        when DelayDischReason = 'R1' then '29'
        when DelayDischReason = 'R2' then '30'
        when DelayDischReason = 'S1' then '31'
        when DelayDischReason = 'T1' then '32'
        when DelayDischReason = 'T2' then '33'
        when DelayDischReason = '98' then '34'       
 	   when DelayDischReason is null then '35'
        else '35'
        End as PrimaryMeasureNumber,
 CASE
  WHEN DelayDischReason = 'A2' then 'Awaiting care coordinator allocation'                        
     WHEN DelayDischReason = 'B1' then 'Awaiting public funding'                   
     WHEN DelayDischReason = 'C1' then 'Awaiting further non-acute (including community and mental health) NHS care'                        
     WHEN DelayDischReason = 'D1' then 'Awaiting Care Home Without Nursing placement or availability'                       
     WHEN DelayDischReason = 'D2' then 'Awaiting Care Home With Nursing placement or availability'                       
     WHEN DelayDischReason = 'E1' then 'Awaiting care package in own home'                     
     WHEN DelayDischReason = 'F2' then 'Awaiting community equipment, telecare and/or adaptations'                        
     WHEN DelayDischReason = 'G2' then 'Patient or Family choice (reason not stated by patient or family)'                       
     WHEN DelayDischReason = 'G3' then 'Patient or Family choice - non-acute (including community and mental health) NHS care'                      
     WHEN DelayDischReason = 'G4' then 'Patient or Family choice - Care Home Without Nursing placement'                    
     WHEN DelayDischReason = 'G5' then 'Patient or Family choice - Care Home With Nursing placement'                    
     WHEN DelayDischReason = 'G6' then 'Patient or Family choice - care package in own home'                        
     WHEN DelayDischReason = 'G7' then 'Patient or Family choice - community equipment, telecare and/or adaptations'                    
     WHEN DelayDischReason = 'G8' then 'Patient or Family Choice - general needs housing/private landlord acceptance'                        
     WHEN DelayDischReason = 'G9' then 'Patient or Family choice - supported accommodation'                        
     WHEN DelayDischReason = 'G10' then 'Patient or Family choice - emergency accommodation from the local luthority under the housing act'                   
     WHEN DelayDischReason = 'G11' then 'Patient or Family choice - child or young person awaiting social care or family placement'                   
     WHEN DelayDischReason = 'G12' then 'Patient or Family choice - ministry of justice agreement/permission of proposed placement'                       
     WHEN DelayDischReason = 'H1' then 'Disputes'                      
     WHEN DelayDischReason = 'I2' then 'Housing - awaiting availability of general needs housing/private landlord accommodation acceptance'                      
     WHEN DelayDischReason = 'I3' then 'Housing - single homeless patients or asylum seekers NOT covered by care act'                        
     WHEN DelayDischReason = 'J2' then 'Housing - awaiting supported accommodation'                        
     WHEN DelayDischReason = 'K2' then 'Housing - awaiting emergency accommodation from the local authority under the housing act'                      
     WHEN DelayDischReason = 'L1' then 'Child or young person awaiting social care or family placement'                    
     WHEN DelayDischReason = 'M1' then 'Awaiting ministry of justice agreement/permission of proposed placement'                       
     WHEN DelayDischReason = 'N1' then 'Awaiting outcome of legal requirements (mental capacity/mental health legislation)'
     when DelayDischReason = 'P1' then 'Awaiting residential special school or college placement or availability'
     when DelayDischReason = 'Q1' then 'Lack of local education support'
     when DelayDischReason = 'R1' then 'Public safety concern unrelated to clinical treatment need (care team)'
     when DelayDischReason = 'R2' then 'Public safety concern unrelated to clinical treatment need (Ministry of Justice)'
     when DelayDischReason = 'S1' then 'No lawful community care package available'
     when DelayDischReason = 'T1' then 'Lack of health care service provision'
     when DelayDischReason = 'T2' then 'Lack of social care support'
     when DelayDischReason = '98' then 'No reason given'
 	WHEN DelayDischReason is null then 'Unknown'    
        else 'Unknown'
        End as PrimaryMeasureSplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 '0' as HospitalSpellsInCarePreviousMonth,
 '0' as HospitalSpellsAdmissionsInMonth,
 '0' as HospitalSpellsDischargedInMonth,
 '0' as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 '0' as  WardStaysInCarePreviousMonth,
 '0' as  WardStaysAdmissionsInMonth,
 '0' as  WardStaysDischargedInMonth,
 '0' as  WardStaysAdmittedAndDischargedInMonth,
 '0' as  WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 where StartDateDelayDisch is not null
 Group By
 CASE
        when DelayDischReason = 'A2' then '1'
        when DelayDischReason = 'B1' then '2'
        when DelayDischReason = 'C1' then '3'
        when DelayDischReason = 'D1' then '4'
        when DelayDischReason = 'D2' then '5'
        when DelayDischReason = 'E1' then '6'
        when DelayDischReason = 'F2' then '7'
        when DelayDischReason = 'G2' then '8'
        when DelayDischReason = 'G3' then '9'
        when DelayDischReason = 'G4' then '10'
        when DelayDischReason = 'G5' then '11'
        when DelayDischReason = 'G6' then '12'
        when DelayDischReason = 'G7' then '13'
        when DelayDischReason = 'G8' then '14'
        when DelayDischReason = 'G9' then '15'
        when DelayDischReason = 'G10' then '16'
        when DelayDischReason = 'G11' then '17'
        when DelayDischReason = 'G12' then '18'
        when DelayDischReason = 'H1' then '19'
        when DelayDischReason = 'I2' then '20'
        when DelayDischReason = 'I3' then '21'
        when DelayDischReason = 'J2' then '22'
        when DelayDischReason = 'K2' then '23'
        when DelayDischReason = 'L1' then '24'
        when DelayDischReason = 'M1' then '25'
        when DelayDischReason = 'N1' then '26'
        when DelayDischReason = 'P1' then '27'
        when DelayDischReason = 'Q1' then '28'
        when DelayDischReason = 'R1' then '29'
        when DelayDischReason = 'R2' then '30'
        when DelayDischReason = 'S1' then '31'
        when DelayDischReason = 'T1' then '32'
        when DelayDischReason = 'T2' then '33'
        when DelayDischReason = '98' then '34'  
 	   when DelayDischReason is null then '35'
        else '35'
        End,
 CASE
     WHEN DelayDischReason = 'A2' then 'Awaiting care coordinator allocation'                        
     WHEN DelayDischReason = 'B1' then 'Awaiting public funding'                   
     WHEN DelayDischReason = 'C1' then 'Awaiting further non-acute (including community and mental health) NHS care'                        
     WHEN DelayDischReason = 'D1' then 'Awaiting Care Home Without Nursing placement or availability'                       
     WHEN DelayDischReason = 'D2' then 'Awaiting Care Home With Nursing placement or availability'                       
     WHEN DelayDischReason = 'E1' then 'Awaiting care package in own home'                     
     WHEN DelayDischReason = 'F2' then 'Awaiting community equipment, telecare and/or adaptations'                        
     WHEN DelayDischReason = 'G2' then 'Patient or Family choice (reason not stated by patient or family)'                       
     WHEN DelayDischReason = 'G3' then 'Patient or Family choice - non-acute (including community and mental health) NHS care'                      
     WHEN DelayDischReason = 'G4' then 'Patient or Family choice - Care Home Without Nursing placement'                    
     WHEN DelayDischReason = 'G5' then 'Patient or Family choice - Care Home With Nursing placement'                    
     WHEN DelayDischReason = 'G6' then 'Patient or Family choice - care package in own home'                        
     WHEN DelayDischReason = 'G7' then 'Patient or Family choice - community equipment, telecare and/or adaptations'                    
     WHEN DelayDischReason = 'G8' then 'Patient or Family Choice - general needs housing/private landlord acceptance'                        
     WHEN DelayDischReason = 'G9' then 'Patient or Family choice - supported accommodation'                        
     WHEN DelayDischReason = 'G10' then 'Patient or Family choice - emergency accommodation from the local luthority under the housing act'                   
     WHEN DelayDischReason = 'G11' then 'Patient or Family choice - child or young person awaiting social care or family placement'                   
     WHEN DelayDischReason = 'G12' then 'Patient or Family choice - ministry of justice agreement/permission of proposed placement'                       
     WHEN DelayDischReason = 'H1' then 'Disputes'                      
     WHEN DelayDischReason = 'I2' then 'Housing - awaiting availability of general needs housing/private landlord accommodation acceptance'                      
     WHEN DelayDischReason = 'I3' then 'Housing - single homeless patients or asylum seekers NOT covered by care act'                        
     WHEN DelayDischReason = 'J2' then 'Housing - awaiting supported accommodation'                        
     WHEN DelayDischReason = 'K2' then 'Housing - awaiting emergency accommodation from the local authority under the housing act'                      
     WHEN DelayDischReason = 'L1' then 'Child or young person awaiting social care or family placement'                    
     WHEN DelayDischReason = 'M1' then 'Awaiting ministry of justice agreement/permission of proposed placement'                       
     WHEN DelayDischReason = 'N1' then 'Awaiting outcome of legal requirements (mental capacity/mental health legislation)'
     when DelayDischReason = 'P1' then 'Awaiting residential special school or college placement or availability'
     when DelayDischReason = 'Q1' then 'Lack of local education support'
     when DelayDischReason = 'R1' then 'Public safety concern unrelated to clinical treatment need (care team)'
     when DelayDischReason = 'R2' then 'Public safety concern unrelated to clinical treatment need (Ministry of Justice)'
     when DelayDischReason = 'S1' then 'No lawful community care package available'
     when DelayDischReason = 'T1' then 'Lack of health care service provision'
     when DelayDischReason = 'T2' then 'Lack of social care support'
     when DelayDischReason = '98' then 'No reason given'
 	WHEN DelayDischReason is null then 'Unknown'     
       else 'Unknown'
        End
   

# COMMAND ----------

 %sql
 ---TABLE 15 -------------------------------------restraints------------------------------------------------------
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table15_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 15 AS TableNumber,
 'Restraints' AS PrimaryMeasure,
 RestrictiveIntType AS PrimaryMeasureNumber,
 restrictiveinttypedesc AS PrimaryMeasureSplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
 COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
 '0' as HospitalSpellsInCarePreviousMonth,
 '0' as HospitalSpellsAdmissionsInMonth,
 '0' as HospitalSpellsDischargedInMonth,
 '0' as HospitalSpellsAdmittedAndDischargedInMonth,
 '0' as HospitalSpellsOpenAtEndOfMonth,
 '0' as WardStaysInCarePreviousMonth,
 '0' as WardStaysAdmissionsInMonth,
 '0' as WardStaysDischargedInMonth,
 '0' as WardStaysAdmittedAndDischargedInMonth,
 '0' as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1
 
 
 GROUP BY 
 RestrictiveIntType,
 restrictiveinttypedesc

# COMMAND ----------

 %sql
 ---- TABLE 50 - LOS by MHA
 
 --LOS
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table50_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'National' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 50 AS TableNumber,
 'Mental Health Act' AS PrimaryMeasure,
 CASE
 WHEN MHA_Group = 'Informal' then '1'
 WHEN MHA_Group = 'Part 2' then '2'
 WHEN MHA_Group = 'Part 3 no restrictions' then '3'
 WHEN MHA_Group = 'Part 3 with restrictions' then '4'
 WHEN MHA_Group = 'Other' then '5'
 ELSE '1'
 end as PrimaryMeasureNumber,
 CASE
 WHEN MHA_Group = 'Informal' then 'Informal'
 WHEN MHA_Group = 'Part 2' then 'Part 2'
 WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
 WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
 WHEN MHA_Group = 'Other' then 'Other sections'
 ELSE 'Informal'
 end AS PrimarySplit,
 'Length of stay' as SecondaryMeasure,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '1'
        WHEN H.HSP_LOS between 4 and 7 then '2'
        WHEN H.HSP_LOS between 8 and 14 then '3'
        WHEN H.HSP_LOS between 15 and 28 then '4'
        WHEN H.HSP_LOS between 29 and 91 then '5'
        WHEN H.HSP_LOS between 92 and 182 then '6'
        WHEN H.HSP_LOS between 183 and 365 then '7'
        WHEN H.HSP_LOS between 366 and 730 then '8'
        WHEN H.HSP_LOS between 731 and 1826 then '9'
        WHEN H.HSP_LOS between 1827 and 3652 then '10'
        WHEN H.HSP_LOS > 3652 then '11'
        ELSE '12'
        END AS SecondaryMeasureNumber,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
        WHEN H.HSP_LOS > 3652 then '10+ years'
        ELSE 'Unknown'
        END as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 '0' as HospitalSpellsInCarePreviousMonth,
 '0' as HospitalSpellsAdmissionsInMonth,
 '0' as HospitalSpellsDischargedInMonth,
 '0' as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 '0' as  WardStaysInCarePreviousMonth,
 '0' as  WardStaysAdmissionsInMonth,
 '0' as  WardStaysDischargedInMonth,
 '0' as  WardStaysAdmittedAndDischargedInMonth,
 '0' as  WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
 global_temp.MHA H
 GROUP BY 
 CASE
 WHEN MHA_Group = 'Informal' then '1'
 WHEN MHA_Group = 'Part 2' then '2'
 WHEN MHA_Group = 'Part 3 no restrictions' then '3'
 WHEN MHA_Group = 'Part 3 with restrictions' then '4'
 WHEN MHA_Group = 'Other' then '5'
 ELSE '1'
 end,
 CASE
 WHEN MHA_Group = 'Informal' then 'Informal'
 WHEN MHA_Group = 'Part 2' then 'Part 2'
 WHEN MHA_Group = 'Part 3 no restrictions' then 'Part 3 no restrictions'
 WHEN MHA_Group = 'Part 3 with restrictions' then 'Part 3 with restrictions'
 WHEN MHA_Group = 'Other' then 'Other sections'
 ELSE 'Informal'
 end,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '1'
        WHEN H.HSP_LOS between 4 and 7 then '2'
        WHEN H.HSP_LOS between 8 and 14 then '3'
        WHEN H.HSP_LOS between 15 and 28 then '4'
        WHEN H.HSP_LOS between 29 and 91 then '5'
        WHEN H.HSP_LOS between 92 and 182 then '6'
        WHEN H.HSP_LOS between 183 and 365 then '7'
        WHEN H.HSP_LOS between 366 and 730 then '8'
        WHEN H.HSP_LOS between 731 and 1826 then '9'
        WHEN H.HSP_LOS between 1827 and 3652 then '10'
        WHEN H.HSP_LOS > 3652 then '11'
        ELSE '12'
        END,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
        WHEN H.HSP_LOS > 3652 then '10+ years'
        ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
 ---- TABLE 51 - Restraints by Age-----------------------------------------------------------
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table51_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Total data as submitted' AS Geography,
 'National' as OrgCode,
 'National' as OrgName,
 51 AS TableNumber,
 'Restraints' AS PrimaryMeasure,
 RestrictiveIntType AS PrimaryMeasureNumber,
 RestrictiveIntTypedesc AS PrimaryMeasureSplit,
 'Age' as SecondaryMeasure,
 
 CASE
        WHEN R.AgeRepPeriodEnd between 0 and 17 then '1'
           WHEN R.AgeRepPeriodEnd between 18 and 24 then '2'
           WHEN R.AgeRepPeriodEnd between 25 and 34 then '3'
           WHEN R.AgeRepPeriodEnd between 35 and 44 then '4'
           WHEN R.AgeRepPeriodEnd between 45 and 54 then '5'
           WHEN R.AgeRepPeriodEnd between 55 and 64 then '6'
           WHEN R.AgeRepPeriodEnd > 64 then '7'
        ELSE '8'
       END AS SecondaryMeasureNumber,
 CASE
        WHEN R.AgeRepPeriodEnd between 0 and 17 then 'Under 18'
        WHEN R.AgeRepPeriodEnd between 18 and 24 then '18-24'
        WHEN R.AgeRepPeriodEnd between 25 and 34 then '25-34'
        WHEN R.AgeRepPeriodEnd between 35 and 44 then '35-44'
        WHEN R.AgeRepPeriodEnd between 45 and 54 then '45-54'
        WHEN R.AgeRepPeriodEnd between 55 and 64 then '55-64'
        WHEN R.AgeRepPeriodEnd > 64 then '65 and Over'
       ELSE 'Unknown'
      END AS SecondarySplit,
      
 COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
 COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
 '0' as HospitalSpellsInCarePreviousMonth,
 '0' as HospitalSpellsAdmissionsInMonth,
 '0' as HospitalSpellsDischargedInMonth,
 '0' as HospitalSpellsAdmittedAndDischargedInMonth,
 '0' as HospitalSpellsOpenAtEndOfMonth,
 
 '0' as WardStaysInCarePreviousMonth,
 '0' as WardStaysAdmissionsInMonth,
 '0' as WardStaysDischargedInMonth,
 '0' as WardStaysAdmittedAndDischargedInMonth,
 '0' as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
  $db_output.LDA_Data_1 R
 GROUP BY 
 RestrictiveIntType,
 RestrictiveIntTypedesc,
 
 CASE
        WHEN R.AgeRepPeriodEnd between 0 and 17 then '1'
           WHEN R.AgeRepPeriodEnd between 18 and 24 then '2'
           WHEN R.AgeRepPeriodEnd between 25 and 34 then '3'
           WHEN R.AgeRepPeriodEnd between 35 and 44 then '4'
           WHEN R.AgeRepPeriodEnd between 45 and 54 then '5'
           WHEN R.AgeRepPeriodEnd between 55 and 64 then '6'
           WHEN R.AgeRepPeriodEnd > 64 then '7'
        ELSE '8'
       END,
 CASE
        WHEN R.AgeRepPeriodEnd between 0 and 17 then 'Under 18'
        WHEN R.AgeRepPeriodEnd between 18 and 24 then '18-24'
        WHEN R.AgeRepPeriodEnd between 25 and 34 then '25-34'
        WHEN R.AgeRepPeriodEnd between 35 and 44 then '35-44'
        WHEN R.AgeRepPeriodEnd between 45 and 54 then '45-54'
        WHEN R.AgeRepPeriodEnd between 55 and 64 then '55-64'
        WHEN R.AgeRepPeriodEnd > 64 then '65 and Over'
       ELSE 'Unknown'
      END

# COMMAND ----------

 %sql
 ------------ TABLE 70 - PROVIDER TOTALS SPLIT
 
 -- This table is different in that the OrgCode and OrgName fields also have data in the them. The case statement here breaks the measures down by Provider.
 -- Provider cross tabs are done by including the OrgCode and OrgName in the groupings as well as the PrimaryMeasure.
 
 --PROVIDERS
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table70_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Provider' AS Geography,
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE l.CombinedProvider END AS OrgCode,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END as OrgName,
 70 AS TableNumber,
 'Total' as PrimaryMeasure,
 1 as PrimaryMeasureNumber,
 'Total' as PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
 --COUNT(distinct CASE when RestrictiveID is not null then Person_ID + RestrictiveIntType else null end) as RestraintsCountOfPeople,
 COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 ORG_TYPE_CODE_PROV AS ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM 
 
  $db_output.LDA_Data_1 L
  
 GROUP BY
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END,
 ORG_TYPE_CODE_PROV

# COMMAND ----------

 %sql
 ---- TABLE 71 LOS by provider
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table71_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Provider' AS Geography,
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END AS OrgCode,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END as OrgName,
 71 AS TableNumber,
 'Length of stay' AS PrimaryMeasure,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '1'
        WHEN H.HSP_LOS between 4 and 7 then '2'
        WHEN H.HSP_LOS between 8 and 14 then '3'
        WHEN H.HSP_LOS between 15 and 28 then '4'
        WHEN H.HSP_LOS between 29 and 91 then '5'
        WHEN H.HSP_LOS between 92 and 182 then '6'
        WHEN H.HSP_LOS between 183 and 365 then '7'
        WHEN H.HSP_LOS between 366 and 730 then '8'
        WHEN H.HSP_LOS between 731 and 1826 then '9'
        WHEN H.HSP_LOS between 1827 and 3652 then '10'
        WHEN H.HSP_LOS > 3652 then '11'
        --ELSE '12'
        END AS PrimaryMeasureNumber,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
        WHEN H.HSP_LOS > 3652 then '10+ years'
        --ELSE 'Unknown'
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell >= '$rp_startdate') then L.UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell > '$rp_enddate') then L.UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then L.UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then L.UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when L.DischDateHospProvSpell is null or L.DischDateHospProvSpell > '$rp_enddate' then L.UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 '0' as  WardStaysInCarePreviousMonth,
 '0' as  WardStaysAdmissionsInMonth,
 '0' as  WardStaysDischargedInMonth,
 '0' as  WardStaysAdmittedAndDischargedInMonth,
 '0' as  WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 
 FROM 
  $db_output.LDA_Data_1 L
 left join global_temp.HSP_Spells H on L.UniqHospProvSpellID = H.UniqHospProvSpellID
 left join global_temp.ProvNoIPs P on P.OrgCode = L.REF_OrgCodeProv
 where P.OrgCode is not null
 GROUP BY 
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '1'
        WHEN H.HSP_LOS between 4 and 7 then '2'
        WHEN H.HSP_LOS between 8 and 14 then '3'
        WHEN H.HSP_LOS between 15 and 28 then '4'
        WHEN H.HSP_LOS between 29 and 91 then '5'
        WHEN H.HSP_LOS between 92 and 182 then '6'
        WHEN H.HSP_LOS between 183 and 365 then '7'
        WHEN H.HSP_LOS between 366 and 730 then '8'
        WHEN H.HSP_LOS between 731 and 1826 then '9'
        WHEN H.HSP_LOS between 1827 and 3652 then '10'
        WHEN H.HSP_LOS > 3652 then '11'
        --ELSE '12'
        END,
 CASE   
        WHEN H.HSP_LOS between 0 and 3 then '0-3 days'
        WHEN H.HSP_LOS between 4 and 7 then '4-7 days'
        WHEN H.HSP_LOS between 8 and 14 then '1-2 weeks'
        WHEN H.HSP_LOS between 15 and 28 then '2-4 weeks'
        WHEN H.HSP_LOS between 29 and 91 then '1-3 months'
        WHEN H.HSP_LOS between 92 and 182 then '3-6 months'
        WHEN H.HSP_LOS between 183 and 365 then '6-12 months'
        WHEN H.HSP_LOS between 366 and 730 then '1-2 years'
        WHEN H.HSP_LOS between 731 and 1826 then '2-5 years'
        WHEN H.HSP_LOS between 1827 and 3652 then '5-10 years'
        WHEN H.HSP_LOS > 3652 then '10+ years'
        --ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
 -- TABLE 72 - WARD TYPE BY PROVIDER
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table72_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Provider' AS Geography,
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END AS OrgCode,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END as OrgName,
 72 AS TableNumber,
 'Ward Type' AS PrimaryMeasure,
 CASE
        WHEN WardType = '01' then '1'
        WHEN WardType = '02' then '2'
        WHEN WardType = '03' then '3'
        WHEN WardType = '04' then '4'
        WHEN WardType = '05' then '5'
        WHEN WardType = '06' then '6'
        else '7'
        END as PrimaryMeasureNumber,
 CASE
        WHEN WardType = '01' then 'Child and adolescent mental health ward'
        WHEN WardType = '02' then 'Paediatric ward'
        WHEN WardType = '03' then 'Adult mental health ward'
        WHEN WardType = '04' then 'Non mental health ward'
        WHEN WardType = '05' then 'Learning disabilities ward'
        WHEN WardType = '06' then 'Older peoples mental health ward'
        ELSE 'Unknown'
        END as PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell >= '$rp_startdate') then L.UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell > '$rp_enddate') then L.UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then L.UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then L.UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when L.DischDateHospProvSpell is null or L.DischDateHospProvSpell > '$rp_enddate' then L.UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 
 FROM 
  $db_output.LDA_Data_1 L
 left join global_temp.ProvNoIPs P on P.OrgCode = L.REF_OrgCodeProv
 where P.OrgCode is not null
 GROUP BY 
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END,
 CASE
        WHEN WardType = '01' then '1'
        WHEN WardType = '02' then '2'
        WHEN WardType = '03' then '3'
        WHEN WardType = '04' then '4'
        WHEN WardType = '05' then '5'
        WHEN WardType = '06' then '6'
        else '7'
        END,
 CASE
        WHEN WardType = '01' then 'Child and adolescent mental health ward'
        WHEN WardType = '02' then 'Paediatric ward'
        WHEN WardType = '03' then 'Adult mental health ward'
        WHEN WardType = '04' then 'Non mental health ward'
        WHEN WardType = '05' then 'Learning disabilities ward'
        WHEN WardType = '06' then 'Older peoples mental health ward'
        ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
 
 ---- TABLE 73 WARD SECURITY LEVEL BY PROVIDER
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table73_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Provider' AS Geography,
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END AS OrgCode,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END as OrgName,
 73 AS TableNumber,
 'Ward security' AS PrimaryMeasure,
 CASE 
        WHEN WardSecLevel = '0' then '1'
        WHEN WardSecLevel = '1' then '2'
        WHEN WardSecLevel = '2' then '3'
        WHEN WardSecLevel = '3' then '4'
        ELSE '5'
        END AS PrimaryMeasureNumber,
 CASE 
        WHEN WardSecLevel = '0' then 'General'
        WHEN WardSecLevel = '1' then 'Low Secure'
        WHEN WardSecLevel = '2' then 'Medium Secure'
        WHEN WardSecLevel = '3' then 'High Secure'
        ELSE 'Unknown'
        END AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell >= '$rp_startdate') then L.UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (L.DischDateHospProvSpell IS null OR L.DischDateHospProvSpell > '$rp_enddate') then L.UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell < '$rp_startdate' and (L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then L.UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when L.StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and L.DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then L.UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when L.DischDateHospProvSpell is null or L.DischDateHospProvSpell > '$rp_enddate' then L.UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 
 FROM 
  $db_output.LDA_Data_1 L
 left join global_temp.ProvNoIPs P on P.OrgCode = L.REF_OrgCodeProv
 where P.OrgCode is not null
 GROUP BY 
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END,
 CASE 
        WHEN WardSecLevel = '0' then '1'
        WHEN WardSecLevel = '1' then '2'
        WHEN WardSecLevel = '2' then '3'
        WHEN WardSecLevel = '3' then '4'
        ELSE '5'
        END,
 CASE 
        WHEN WardSecLevel = '0' then 'General'
        WHEN WardSecLevel = '1' then 'Low Secure'
        WHEN WardSecLevel = '2' then 'Medium Secure'
        WHEN WardSecLevel = '3' then 'High Secure'
        ELSE 'Unknown'
        END

# COMMAND ----------

 %sql
  -----------table 74 Restraints by Provider-----------------------------------------------------------------------------------------------------------------
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table74_LDA AS
 SELECT
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Provider' AS Geography,
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END AS OrgCode,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END as OrgName,
 74 AS TableNumber,
 'Restraints' AS PrimaryMeasure,
 RestrictiveIntType AS PrimaryMeasureNumber,
 RestrictiveIntTypeDesc AS PrimaryMeasureSplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 COUNT(distinct CASE when RestrictiveID is not null then Person_ID else null end) as RestraintsCountOfPeople,
 COUNT(distinct CASE when RestrictiveID is not null then RestrictiveID else null end) as RestraintsCountOfRestraints,
 '0' as HospitalSpellsInCarePreviousMonth,
 '0' as HospitalSpellsAdmissionsInMonth,
 '0' as HospitalSpellsDischargedInMonth,
 '0' as HospitalSpellsAdmittedAndDischargedInMonth,
 '0' as HospitalSpellsOpenAtEndOfMonth,
 '0' as WardStaysInCarePreviousMonth,
 '0' as WardStaysAdmissionsInMonth,
 '0' as WardStaysDischargedInMonth,
 '0' as WardStaysAdmittedAndDischargedInMonth,
 '0' as WardStaysOpenAtEndOfMonth,
 '0' as OpenReferralsPreviousMonth,
 '0' as ReferralsStartingInTheMonth,
 '0' as ReferralsEndingInTheMonth,
 '0' as ReferralsStartingAndEndingInTheMonth,
 '0' as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 
 FROM 
  $db_output.LDA_Data_1 L
 left join global_temp.ProvNoIPs p on P.OrgCode = L.REF_OrgCodeProv
 where P.OrgCode is not null
 GROUP BY 
 CASE
        WHEN NAME is null then 'Invalid'
        ELSE L.CombinedProvider END,
 CASE 
        WHEN NAME is null then 'Invalid'
        ELSE NAME END, 
 RestrictiveIntType,
 RestrictiveIntTypeDesc

# COMMAND ----------

 %sql
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table80_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Commissioner Groupings' AS Geography,
 ORG_TYPE_CODE  as OrgCode,  -------  to do? ----
 ORG_TYPE_CODE as OrgName, ------- to do ----
 80 AS TableNumber,
 'Total' AS PrimaryMeasure,
 1 AS PrimaryMeasureNumber,
 'Total' AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM
  $db_output.LDA_Data_1 R  
 
 group by 
 ORG_TYPE_CODE

# COMMAND ----------

 %sql
 ----- TABLE 90 - Commissioner
 
 -- The two joins to ORG_DAILY temp table are done as some of the Commissioner codes end 00 but are valid as the 3 digit version. Therefore the join is done on the full org code and the SUBSTR( 0, 3) version.
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table90_LDA AS
 SELECT 
 '$PreviousMonthEnd' as PreviousMonthEnd,
 '$rp_startdate' as PeriodStart,
 '$rp_enddate' as PeriodEnd,
 'Commissioner' AS Geography,
 OrgCode,
 OrgName,
 90 AS TableNumber,
 'Total' AS PrimaryMeasure,
 1 AS PrimaryMeasureNumber,
 'Total' AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM
  $db_output.LDA_Data_1 R 
 
 GROUP BY
 OrgCode,
 OrgName,
 ORG_TYPE_CODE

# COMMAND ----------

 %sql
 --TCP-----
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table100_LDA AS
 SELECT 
 	  '$PreviousMonthEnd' as PreviousMonthEnd,
 	  '$rp_startdate' as PeriodStart,
 	  '$rp_enddate' as PeriodEnd,
       'TCP Region' AS Geography
       ,case when REGION_code IS NULL THEN 'Invalid' ELSE REGION_code end as REGION_code
       ,case when Region_name IS NULL THEN 'Invalid' ELSE Region_name end as Region_name
       ,100 AS TableNumber,
 'Total' AS PrimaryMeasure,
 1 AS PrimaryMeasureNumber,
 'Total' AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 FROM  $db_output.LDA_Data_1 O
 
  --and OrgCode = '13Y'
 --AND NHS_ENGLAND_TCP_CODE <> 'Invalid'
 
 GROUP BY REGION_code
       ,Region_name

# COMMAND ----------

 %sql
 --TCP 101-----
 
 INSERT INTO $db_output.LDA_Counts
 
 -- CREATE OR REPLACE TEMP VIEW Table101_LDA AS
 SELECT 
 	  '$PreviousMonthEnd' as PreviousMonthEnd,
 	  '$rp_startdate' as PeriodStart,
 	  '$rp_enddate' as PeriodEnd,
       'TCP' AS Geography
       ,case when TCP_code IS NULL THEN 'Invalid' ELSE TCP_code end as OrgCode
       ,case when TCP_name IS NULL THEN 'Invalid' ELSE TCP_name end as OrgName
       ,101 AS TableNumber,
 'Total' AS PrimaryMeasure,
 1 AS PrimaryMeasureNumber,
 'Total' AS PrimarySplit,
 '' as SecondaryMeasure,
 '' as SecondaryMeasureNumber,
 '' as SecondarySplit,
 '0' as RestraintsCountOfPeople,
 '0' as RestraintsCountOfRestraints,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell >= '$rp_startdate') then UniqHospProvSpellID else null end) as HospitalSpellsInCarePreviousMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and (DischDateHospProvSpell IS null OR DischDateHospProvSpell > '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsAdmissionsInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell < '$rp_startdate' and (DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate') then UniqHospProvSpellID else null end) as HospitalSpellsDischargedInMonth,
 COUNT (distinct CASE when StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' and DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'  then UniqHospProvSpellID else null end) as HospitalSpellsAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when DischDateHospProvSpell is null or DischDateHospProvSpell > '$rp_enddate' then UniqHospProvSpellID else null end) as HospitalSpellsOpenAtEndOfMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay IS null OR EndDateWardStay >= '$rp_startdate') then UniqWardStayID else null end) as WardStaysInCarePreviousMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and (EndDateWardStay IS null OR EndDateWardStay > '$rp_enddate') then UniqWardStayID else null end) as WardStaysAdmissionsInMonth,
 COUNT (distinct CASE when StartDateWardStay < '$rp_startdate' and (EndDateWardStay between '$rp_startdate' and '$rp_enddate') then UniqWardStayID else null end) as WardStaysDischargedInMonth,
 COUNT (distinct CASE when StartDateWardStay between '$rp_startdate' and '$rp_enddate' and EndDateWardStay between '$rp_startdate' and '$rp_enddate'  then UniqWardStayID else null end) as WardStaysAdmittedAndDischargedInMonth,
 COUNT (distinct CASE when EndDateWardStay is null or EndDateWardStay > '$rp_enddate' then UniqWardStayID else null end) as WardStaysOpenAtEndOfMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate IS null OR ServDischDate >= '$rp_startdate') then UniqServReqID else null end) as OpenReferralsPreviousMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate')  then UniqServReqID else null end) as ReferralsStartingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate < '$rp_startdate' and (ServDischDate between '$rp_startdate' and '$rp_enddate')  then UniqServReqID else null end) as ReferralsEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate between '$rp_startdate' and '$rp_enddate' and (ServDischDate between '$rp_startdate' and '$rp_enddate') then UniqServReqID else null end) as ReferralsStartingAndEndingInTheMonth,
 COUNT (distinct CASE when ReferralRequestReceivedDate <= '$rp_enddate' and (ServDischDate IS null OR ServDischDate > '$rp_enddate') then UniqServReqID else null end) as ReferralsOpenAtEndOfMonth,
 '' as ORG_TYPE_CODE,
 'nonRespite' as PRODUCT, 
 '$db_source' as SOURCE_DB
  
 
 FROM  $db_output.LDA_Data_1 O
 
  --and OrgCode = '13Y'
 --AND NHS_ENGLAND_TCP_CODE <> 'Invalid'
 
 GROUP BY TCP_code
       ,TCP_name

# COMMAND ----------

# %sql
# --uses union all to combine all temp tables above together as table 101 needs all of this information

# -- CREATE OR REPLACE TEMP VIEW LDA_Counts AS
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

 %sql
 --this creates the monthly output data file
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW lda_nonR AS
 
 Select 
 
 PreviousMonthEnd,
 PeriodStart,
 PeriodEnd,
 Geography,
 OrgCode,
 OrgName,
 TableNumber,
 PrimaryMeasure,
 PrimaryMeasureNumber,
 PrimarySplit,
 SecondaryMeasure,
 SecondaryMeasureNumber,
 SecondarySplit,
 case when RestraintsCountOfPeople is null then 0 else RestraintsCountOfPeople end as RestraintsCountOfPeople,
 case when RestraintsCountOfRestraints is null then 0 else RestraintsCountOfRestraints end as RestraintsCountOfRestraints,
 case when HospitalSpellsInCarePreviousMonth is null then 0 else HospitalSpellsInCarePreviousMonth  end as HospitalSpellsInCarePreviousMonth,
 case when HospitalSpellsAdmissionsInMonth is null then 0 else HospitalSpellsAdmissionsInMonth  end as HospitalSpellsAdmissionsInMonth,
 case when HospitalSpellsDischargedInMonth is null then 0 else HospitalSpellsDischargedInMonth  end as HospitalSpellsDischargedInMonth,
 case when HospitalSpellsAdmittedAndDischargedInMonth is null then 0 else HospitalSpellsAdmittedAndDischargedInMonth  end as HospitalSpellsAdmittedAndDischargedInMonth,
 case when HospitalSpellsOpenAtEndOfMonth is null then 0 else HospitalSpellsOpenAtEndOfMonth  end as HospitalSpellsOpenAtEndOfMonth,
 case when WardStaysInCarePreviousMonth is null then 0 else WardStaysInCarePreviousMonth  end as WardStaysInCarePreviousMonth,
 case when WardStaysAdmissionsInMonth is null then 0 else WardStaysAdmissionsInMonth  end as WardStaysAdmissionsInMonth,
 case when WardStaysDischargedInMonth is null then 0 else WardStaysDischargedInMonth  end as WardStaysDischargedInMonth,
 case when WardStaysAdmittedAndDischargedInMonth is null then 0 else WardStaysAdmittedAndDischargedInMonth  end as WardStaysAdmittedAndDischargedInMonth,
 case when WardStaysOpenAtEndOfMonth is null then 0 else WardStaysOpenAtEndOfMonth  end as WardStaysOpenAtEndOfMonth,
 case when OpenReferralsPreviousMonth is null then 0 else OpenReferralsPreviousMonth  end as OpenReferralsPreviousMonth,
 case when ReferralsStartingInTheMonth is null then 0 else ReferralsStartingInTheMonth  end as ReferralsStartingInTheMonth,
 case when ReferralsEndingInTheMonth is null then 0 else ReferralsEndingInTheMonth  end as ReferralsEndingInTheMonth,
 case when ReferralsStartingAndEndingInTheMonth is null then 0 else ReferralsStartingAndEndingInTheMonth  end as ReferralsStartingAndEndingInTheMonth,
 case when ReferralsOpenAtEndOfMonth is null then 0 else ReferralsOpenAtEndOfMonth  end as ReferralsOpenAtEndOfMonth,
 
 
 
 case 
 when Tablenumber not in ('70','71','72','73','74','90') then null
 when OrgCode = 'Invalid' then 'Invalid'
 when OrgName like '%NHS%' then 'NHS'
 else 'Independent' end as NHS_NHD_SPLIT,
 case
 when Tablenumber <> '90' then null
 else ORG_TYPE_CODE end as ORG_TYPE_CODE
 
 
 from $db_output.LDA_Counts l
 WHERE PRODUCT ='Monthly' 
 AND PeriodEnd = '$rp_enddate'
 AND SOURCE_DB = '$db_source'
 
 order by TableNumber,PrimaryMeasureNumber,SecondaryMeasureNumber