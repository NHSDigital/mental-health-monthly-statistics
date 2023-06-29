# Databricks notebook source
 %md
 # LDA Prep assets:
 - RD_ORG_DAILY_LATEST_LDA
 - Commissioners
 - LDA_Data
 - LDA_Data_P
 - LDA_Data_C
 - LDA_Data_T
 - LDA_Data_1
 - LDA_Data_Prov
 - ProvNoIPs
 - HSP_Spells
 - WARD_Spells
 - MHA

# COMMAND ----------

# DBTITLE 1,RD_ORG_DAILY_LATEST_LDA
 %sql
 /* This is similar to the generic prep RD_ORG_DAILY_LATEST, however it is broader and has an extra column. Possible to combine?*/
 
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

# DBTITLE 1,Commissioners
 %sql
 ------------------- THIS REFERENCE TABLE HAS ALL THE CCGS/TCPS AND COMMISSIONING REGIONS -------------------
 ----------------------- FOR THE RELEVANT REPORTING MONTH, IT IS DYNAMIC AND DEALS WITH NEW ORGS ------------
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW commissioners AS
 select case when O3.NAME like '%HUB%' then 'HUB' 
             when O3.NAME like '%CCG%' then 'CCG' 
             else 'Other' 
             end as CCG_Or_Hub
             ,O3.ORG_CODE
             ,O3.NAME
             ,case when T.NHS_ENGLAND_TCP_CODE Is null then 'No TCP mapping' 
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
           ,case when T.NHS_ENGLAND_TCP_CODE Is null then 'No TCP mapping' 
           else T.NHS_ENGLAND_TCP_CODE 
           end
           ,T.TCP_NAME
           ,O2.REL_TO_ORG_CODE
           ,O4.NAME
 order by ORG_CODE

# COMMAND ----------

# DBTITLE 1,LDA_Data V2
 %sql
 ---------- This is to try and speed up the code so that the joins dont have to be done repeatedly on each join. In theory this table holds all of the information required for the LD cohort.
 ---------- The problem to use in some circumstances (eg. Average LOS *No longer included*) is that some Hospital spells have multiple ward stays. This produces mutiple lines per one hospital spell in this table.
 
 ---------- For any new measures where the raw data is not in this table add it in using a left join to the appropriate table. Then add the field required to both the SELECT and GROUP BY.
 ---------- Ensure any new joins have the UniqmonthID in any join (** IC_UseSubmissionFlag previously needed in V3**). 
 
 ---------- In some cases it is easier to have a second raw data table if the calculations used in the measures are particularly complex (eg. MHA)
 
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
 		H.UniqHospProvSpellNum,
 		H.StartDateHospProvSpell,
 		H.DischDateHospProvSpell,
 		H.AdmMethCodeHospProvSpell,
 		H.DischDestCodeHospProvSpell,
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
 Case when ACP.TreatFuncCodeMH = '319' or PVD.ProvDiag = 'Z755' or PRD.PrimDiag = 'Z755' or SED.SecDiag = 'Z755' then 'Y'
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
                RES.MHS505UniqId,
                RES.RestrictiveIntType as realrestraintinttype,
                RES.StartDateRestrictiveInt,
                RES.EndDateRestrictiveInt,
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
          END  AS RestrictiveIntTypeDesc,
       
       RES.MHS505UniqID as RestrictiveID
 FROM	$db_source.MHS001MPI M 
 		LEFT JOIN $db_source.MHS101Referral as R ON M.person_ID = R.person_ID AND R.UniqMonthID = '$month_id' 
 		LEFT JOIN $db_source.MHS603ProvDiag as PVD ON R.person_ID = PVD.person_ID AND  PVD.UniqMonthID = '$month_id' AND PVD.provdiag ='Z755' 
 		LEFT JOIN $db_source.MHS604PrimDiag as PRD ON R.person_ID = PRD.person_ID AND PRD.UniqMonthID = '$month_id' AND PRD.primdiag ='Z755' 
 		LEFT JOIN $db_source.MHS605SecDiag as SED ON R.person_ID = SED.person_ID AND SED.UniqMonthID = '$month_id' AND SED.secdiag ='Z755' 
 		LEFT JOIN $db_source.MHS501HospProvSpell as H  ON R.person_ID = H.person_ID AND H.UniqMonthID = '$month_id' and h.uniqservreqid = r.uniqservreqid
 		LEFT JOIN $db_source.MHS502WardStay as A ON H.UniqHospProvSpellNum = A.UniqHospProvSpellNum AND A.UniqMonthID = '$month_id'
 		LEFT JOIN $db_source.MHS503AssignedCareProf as ACP ON ACP.UniqHospProvSpellNum = H.UniqHospProvSpellNum AND ACP.UniqMonthID = '$month_id'
 		LEFT JOIN $db_source.MHS504DelayedDischarge as DD ON H.UniqHospProvSpellNum = DD.UniqHospProvSpellNum AND DD.UniqMonthID = '$month_id' AND DD.EndDateDelayDisch IS NULL
         LEFT JOIN 
                   (SELECT HC.UniqHospProvSpellNum,
                            H.StartDateHospProvSpell,                       
                            H.DischDateHospProvSpell,  
                            CASE	WHEN O5.ORG_CODE IS NULL THEN O4.ORG_CODE
                            ELSE O5.ORG_CODE END AS OrgCodeComm,
                            StartDateOrgCodeComm,
                            EndDateOrgCodeComm,
                            dense_rank() over (partition by HC.UniqHospProvSpellNum ORDER BY CASE WHEN ENDDateOrgCodeComm IS NULL THEN 1 ELSE 2 END ASC, ENDDateOrgCodeComm DESC, StartDateOrgCodeComm DESC, MHS512UNIQID DESC) AS RANK
                       FROM $db_source.MHS512HospSpellComm    HC
                  LEFT JOIN $db_source.MHS501HospProvSpell H ON H.UniqHospProvSpellNum = HC.UniqHospProvSpellNum 
                        AND H.UniqMonthID = '$month_id' 
                        AND HC.StartDateOrgCodeComm <= H.DischDateHospProvSpell 
                        AND (HC.EndDateOrgCodeComm IS NULL OR HC.EndDateOrgCodeComm >= H.DischDateHospProvSpell) 
                  LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O4 ON O4.ORG_CODE = HC.OrgIDComm                       
                  LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O5 ON O5.ORG_CODE = substr(HC.OrgIDComm, 0, 3)  
                      WHERE HC.UniqMonthID = '$month_id') HC1 
                         ON H.UniqHospProvSpellNum = HC1.UniqHospProvSpellNum AND HC1.rank = 1
          LEFT JOIN $db_source.MHS505RestrictiveIntervention as RES ON RES.WardStayId = A.WardStayId AND RES.person_ID = A.person_ID 
                        AND ((RES.StartDateRestrictiveInt BETWEEN '$rp_startdate' AND '$rp_enddate') 
                        OR (RES.EndDateRestrictiveInt BETWEEN '$rp_startdate' AND '$rp_enddate'))
                        AND RES.UniqMonthID = '$month_id' 
                      WHERE M.LDAFlag = True AND M.UniqMonthID = '$month_id' AND M.PatMRecInRP = True 
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
 		H.UniqHospProvSpellNum,
 		H.StartDateHospProvSpell,
 		H.DischDateHospProvSpell,
 		H.AdmMethCodeHospProvSpell,
 		H.DischDestCodeHospProvSpell,
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
 		CASE	WHEN ACP.TreatFuncCodeMH = '319' OR PVD.ProvDiag = 'Z755' OR PRD.PrimDiag = 'Z755' OR SED.SecDiag = 'Z755' THEN 'Y'
 				ELSE NULL END,
                 
 		DATEDIFF(H.StartDateHospProvSpell, CASE WHEN H.DischDateHospProvSpell IS NULL THEN '$rp_enddate' WHEN H.DischDateHospProvSpell > '$rp_enddate' THEN '$rp_enddate'  ELSE H.DischDateHospProvSpell END),
 		DATEDIFF(A.StartDateWardStay, CASE WHEN A.EndDateWardStay IS NULL THEN '$rp_enddate' WHEN A.EndDateWardStay > '$rp_enddate' THEN '$rp_enddate'  ELSE A.EndDateWardStay END),
 		CASE	WHEN H.OrgIDProv IS NULL THEN R.OrgIDProv
 				ELSE H.OrgIDProv END,
 		CASE	WHEN HC1.OrgCodeComm IS NULL THEN R.OrgIDComm
 				WHEN HC1.OrgCodeComm LIKE 'Q%' THEN R.OrgIDComm
 				ELSE HC1.OrgCodeComm END,
                 
           RES.MHS505UniqId,
           RES.RestrictiveIntType,
           RES.StartDateRestrictiveInt,
           RES.EndDateRestrictiveInt,
           
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
 			 ELSE '1' END,
              
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
              END,
 		RES.MHS505UniqID ;

# COMMAND ----------

# DBTITLE 1,LDA Data - Transformed Tables
 %sql
 --- THESE TEMP TABLES TRANSFORM THE LIST OF PATIENTS NEEDED FOR REPORTING TO INCLUDE VARIOUS COMMISSIONER FIELDS NEEDED FOR SSRS REPORTING--
 
 -------------- Join the name of the provider onto LDA_Data to make the provider split easier.
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_Data_P AS
 
 SELECT LD.*
         ,O.name,o.ORG_TYPE_CODE AS ORG_TYPE_CODE_PROV
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
     
 TRUNCATE TABLE $db_output.LDA_Data_1;
 
 INSERT INTO $db_output.LDA_Data_1 
 (select L.*,R.ORG_TYPE_CODE AS ORG_TYPE_ORG_R,R.ORG_CODE AS ORG_CODE_R, R.NAME AS ORG_NAME_R
       ,case when R.ORG_TYPE_CODE IS NULL then 'Invalid' 
       else R.ORG_TYPE_CODE 
       end as ORG_TYPE_CODE
 from global_temp.LDA_Data_T L
 left join global_temp.RD_ORG_DAILY_LATEST_LDA R 
     on R.ORG_CODE = L.OrgCode);
     
     

# COMMAND ----------

# DBTITLE 1,LDA_Data_prov 
 %sql
 -- creates table needed to create prov no ips table
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW LDA_data_prov AS
 
 SELECT LD.*,O.NAME
 FROM global_temp.LDA_Data LD
 LEFT JOIN global_temp.RD_ORG_DAILY_LATEST_LDA O on LD.combinedprovider = O.ORG_CODE

# COMMAND ----------

# DBTITLE 1,ProvNoIPs
 %sql 
 
 ---- This table below creates a list of Provider Orgs that DO NOT have any inpatients  ---------
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ProvNoIPs AS
 
 select REF_OrgCodeProv as OrgCode
 from global_temp.LDA_Data_Prov
 where UniqHospProvSpellNum is not null
 group by REF_OrgCodeProv
 order by 1

# COMMAND ----------

# DBTITLE 1,HSP_Spells
 %sql
 -- needed to calculate all hospital spells measures
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW HSP_Spells AS
 
 SELECT 
 UniqHospProvSpellNum,
 LD.AgeRepPeriodEnd,
 StartDateHospProvSpell,
 DischDateHospProvSpell,
 DATEDIFF(case when DischDateHospProvSpell IS null then '$rp_enddate' when DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else DischDateHospProvSpell end ,StartDateHospProvSpell) as HSP_LOS
 FROM  $db_source.MHS001MPI LD
 inner join $db_source.MHS501HospProvSpell H on  LD.Person_ID = H.Person_ID  and H.UniqMonthID = '$month_id'
 WHERE LD.UniqMonthID = '$month_id' 
 and LD.LDAFlag = True 
 and LD.PatMRecInRP = True

# COMMAND ----------

# DBTITLE 1,WARD_Spells
 %sql
 -- needed to create all ward spells measures
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW WARD_Spells AS
 
 SELECT 
 UniqWardStayID,
 LD.AgeRepPeriodEnd,
 StartDateWardStay,
 EndDateWardStay,
 wardseclevel,
 DATEDIFF(case when EndDateWardStay IS null then '$rp_enddate' when EndDateWardStay > '$rp_enddate' then '$rp_enddate'  else EndDateWardStay end,StartDateWardStay) as WARD_LOS
 FROM 
 $db_source.MHS001MPI LD
 inner join $db_source.MHS502WardStay W on  LD.Person_ID = w.Person_ID 
 and W.UniqMonthID = '$month_id'
 WHERE LD.UniqMonthID = '$month_id' 
 and LD.LDAFlag = True 
 and LD.PatMRecInRP = True

# COMMAND ----------

# DBTITLE 1,MHA
 %sql
 -- needed to create MHA measures
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA AS
 
 SELECT
 A.Person_ID,
 B.UniqHospProvSpellNum,
 B.StartDateHospProvSpell,
 B.DischDateHospProvSpell,
 DATEDIFF(case when B.DischDateHospProvSpell IS null then '$rp_enddate' when B.DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else B.DischDateHospProvSpell end,B.StartDateHospProvSpell) as HSP_LOS,
 C.UniqMHActEpisodeID,
 C.MHS401UniqID,
 C.StartDateMHActLegalStatusClass,
 C.EndDateMHActLegalStatusClass,
 C.rank,
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
 $db_source.MHS001MPI A 
 INNER JOIN $db_source.MHS501HospProvSpell B ON A.Person_ID = B.Person_ID  
 AND B.UniqMonthID = '$month_id'
 LEFT JOIN (SELECT A.Person_ID,
                   A.UniqHospProvSpellNum,
                   A.StartDateHospProvSpell,
                   A.DischDateHospProvSpell,
                   UniqMHActEpisodeID,
                   MHS401UniqID,
                   StartDateMHActLegalStatusClass,
                   EndDateMHActLegalStatusClass,
                   NHSDLegalStatus,
                   dense_rank() over (partition by A.Person_ID, A.DischDateHospProvSpell order by case when EndDateMHActLegalStatusClass is null then 1 else 2 end ASC, EndDateMHActLegalStatusClass DESC, case when NHSDLegalStatus in ('02','03') then 1 else 2 end, StartDateMHActLegalStatusClass DESC, MHS401UNIQID DESC) AS RANK
              FROM $db_source.MHS501HospProvSpell A
              inner join $db_source.MHS401MHActPeriod B on A.Person_ID = B.Person_ID and B.UniqMonthID = '$month_id' 
                         and ((B.EndDateMHActLegalStatusClass is null and a.DischDateHospProvSpell is null) 
                         or (B.StartDateMHActLegalStatusClass <= A.DischDateHospProvSpell and B.EndDateMHActLegalStatusClass >= A.DischDateHospProvSpell))
              WHERE A.UniqMonthID = '$month_id' 
              ) C 
 ON B.Person_ID = C.Person_ID and ((C.EndDateMHActLegalStatusClass is null and B.DischDateHospProvSpell is null) or (C.StartDateMHActLegalStatusClass <= B.DischDateHospProvSpell and C.EndDateMHActLegalStatusClass >= B.DischDateHospProvSpell)) and C.rank = 1
 left join $db_source.MHS403ConditionalDischarge CD on CD.Person_ID = B.Person_ID and CD.UniqMonthID = '$month_id'  
 and ((CD.EndDateMHCondDisch is null and B.DischDateHospProvSpell is null) or (CD.StartDateMHCondDisch <= B.DischDateHospProvSpell and  CD.EndDateMHCondDisch >= B.DischDateHospProvSpell))
 left join $db_source.MHS404CommTreatOrder CTO on CTO.Person_ID = B.Person_ID and CTO.UniqMonthID = '$month_id'  
 and ((CTO.EndDateCommTreatOrd is null and B.DischDateHospProvSpell is null) or (CTO.StartDateCommTreatOrd <= B.DischDateHospProvSpell and  CTO.EndDateCommTreatOrd >= B.DischDateHospProvSpell))
 left join $db_source.MHS405CommTreatOrderRecall CR on CR.Person_ID = B.Person_ID and CR.UniqMonthID = '$month_id'  
 and ((CR.EndDateCommTreatOrdRecall is null and B.DischDateHospProvSpell is null) or (CR.StartDateCommTreatOrdRecall <= B.DischDateHospProvSpell and  CR.EndDateCommTreatOrdRecall >= B.DischDateHospProvSpell))
 WHERE A.LDAFlag = True  
 AND A.PatMRecInRP = True  
 AND A.UniqMonthID = '$month_id'
 GROUP BY
 A.Person_ID,
 B.UniqHospProvSpellNum,
 B.StartDateHospProvSpell,
 B.DischDateHospProvSpell,
 DATEDIFF(case when B.DischDateHospProvSpell IS null then '$rp_enddate' when B.DischDateHospProvSpell > '$rp_enddate' then '$rp_enddate'  else b.DischDateHospProvSpell end,B.StartDateHospProvSpell),
 C.UniqMHActEpisodeID,
 C.MHS401UniqID,
 C.StartDateMHActLegalStatusClass,
 C.EndDateMHActLegalStatusClass,
 C.rank,
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