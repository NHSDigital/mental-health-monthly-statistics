# Databricks notebook source
dbutils.widgets.text("MONTH_ID", "1490")
dbutils.widgets.text("MSDS_15", "$mat_1.5")
dbutils.widgets.text("MSDS_2", "$maternity")
dbutils.widgets.text("MHSDS", "$mhsds")
dbutils.widgets.text("RP_STARTDATE", "2023-06-01")
dbutils.widgets.text("RP_ENDDATE", "2024-05-31")
dbutils.widgets.text("personal_db", "")
dbutils.widgets.text("prev_months", "2024-04-01")
dbutils.widgets.text("FY_START", "2024-04-01")

# COMMAND ----------

 %sql
 ----PMH07a
 SELECT DISTINCT REF.UniqMonthID, PERI.Person_ID_Mother, UniqPregID, StartDate, EndDate, EndDate12m, EndDate24m, REF.UniqServReqID, REF.MH, REF.CAMHS, HSP.UniqHospProvSpellID, WST.UniqWardStayID, WST.SiteIDOfTreat, WSD.SiteIDOfWard
 FROM $personal_db.MHSDSPerinatalPeriodMH_DF_NEW
 AS PERI
 LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
 AS REF
 ON PERI.Person_ID = REF.Person_ID
 INNER JOIN $MHSDS.MHS501HospProvSpell
 AS HSP
 ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate12m AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

 INNER JOIN $MHSDS.MHS502WardStay
 AS WST
 ON HSP.UniqHospProvSpellID = WST.UniqHospProvSpellID  --  'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' v5 20/10/21 /*** Amended 2 instances to v5 GF ***/
 AND (WST.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
 AND WST.StartDateWardStay <= '$RP_ENDDATE'
 AND (WST.EndDateWardStay IS NULL OR WST.EndDateWardStay >= '$RP_STARTDATE') 
 AND WST.StartDateWardStay <= PERI.EndDate12m 
 AND (WST.EndDateWardStay  IS NULL OR WST.EndDateWardStay  >= PERI.StartDate) 
 AND (WST.RecordEndDate IS NULL OR WST.RecordEndDate >= '$RP_ENDDATE') 
 AND WST.RecordStartDate <= '$RP_ENDDATE'

 LEFT JOIN $MHSDS.MHS903WardDetails 
 AS WSD
 ON WST.UniqWardCode = WSD.UniqWardCode
 AND WSD.SiteIDOfWard in
 ('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
 'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
 'RV505','RRE3K','RW119')

 WHERE PERI.StartDate <= '$RP_ENDDATE'
 AND PERI.EndDate12M >= '$RP_STARTDATE'
 AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
 AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
 AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
 AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
 AND REF.ReferralRequestReceivedDate <= PERI.EndDate12m
 AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
 AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
 AND PERI.AgeAtBookingMother >= 16
 AND
 (SiteIDOfTreat in
 ('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
 'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
 'RV505','RRE3K','RW119') OR WSD.UniqWardCode is not null)

# COMMAND ----------

 %sql
 describe $mhsds.mhs502wardstay

# COMMAND ----------

 %sql
 ----PMH21a
 SELECT COALESCE (COUNT (DISTINCT PERI.UniqPregID), 0)
 FROM $personal_db.MHSDSPerinatalPeriodMH_DF_NEW
 AS PERI

 LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
 AS REF
 ON PERI.Person_ID = REF.Person_ID

 INNER JOIN $MHSDS.MHS501HospProvSpell
 AS HSP
 ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate12M AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

 INNER JOIN $MHSDS.MHS502WardStay
 AS WST
 ON HSP.UniqHospProvSpellID = WST.UniqHospProvSpellID  --'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' v5 20/10/21 /*** Amended 2 instances to v5 GF ***/
 AND (WST.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
 AND WST.StartDateWardStay <= '$RP_ENDDATE' 
 AND (WST.EndDateWardStay IS NULL OR WST.EndDateWardStay >= '$RP_STARTDATE') 
 AND WST.StartDateWardStay <= PERI.EndDate12M 
 AND (WST.EndDateWardStay  IS NULL OR WST.EndDateWardStay  >= PERI.StartDate) 
 AND (WST.RecordEndDate IS NULL OR WST.RecordEndDate >= '$RP_ENDDATE') 
 AND WST.RecordStartDate <= '$RP_ENDDATE' 

 INNER JOIN $MHSDS.MHS903WardDetails 
 AS WSD
 ON WST.UniqWardCode = WSD.UniqWardCode
 AND WSD.SiteIDOfWard in
 ('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
 'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
 'RV505','RRE3K','RW119')

 WHERE PERI.StartDate <= '$RP_ENDDATE'
 AND PERI.EndDate12M >= '$RP_STARTDATE'
 AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
 AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
 AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
 AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
 AND REF.ReferralRequestReceivedDate <= PERI.EndDate12M
 AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
 AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
 AND PERI.AgeAtBookingMother >= 16

# COMMAND ----------

 %sql
 /**CODE TO SPLIT THE DIFFERENT SERVICE AREAS FOR WARD STAYS**/

 --CREATES THE DIFFERENT SERVICE AREAS NON DISTINCT

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list_v6 AS

 SELECT  DISTINCT CASE --WHEN CAMHSTier IN ('4','9') THEN 'Y'
                           WHEN WardIntendedClinCareMH IN ('61', '62', '63') THEN NULL
                           WHEN TreatFuncCodeMH = '700' THEN NULL
                           WHEN WardType = '05' THEN NULL 
                           
                           WHEN TreatFuncCodeMH = '711' THEN 'Y'
                           WHEN WardType IN ('01', '02') THEN 'Y'
                           WHEN WardAge IN ('10','11','12') THEN 'Y'
                           
                           --WHEN PRCNT_U18 > 50 THEN 'Y' ---prior v6 
                           WHEN SRV.ServTeamIntAgeGroup = '02' THEN 'Y' ---v6
                           
                           ELSE NULL END AS CAMHS
                           
             ,CASE WHEN WardIntendedClinCareMH IN ('61', '62', '63') THEN 'Y'
                   WHEN TreatFuncCodeMH = '700' THEN 'Y'
                   WHEN WardType = '05' THEN 'Y'
                   ELSE NULL END AS LD
                   
             ,CASE WHEN WardType IN ('01', '02', '05') THEN NULL 
                   WHEN WardIntendedClinCareMH in ('61', '62', '63') THEN NULL
                   --WHEN CAMHSTier IN ('4','9') THEN NULL 
                   WHEN TreatFuncCodeMH IN ('700', '711') THEN NULL
                   WHEN WardAge IN ('10', '11', '12') THEN NULL
                   
                   WHEN SRV.ServTeamIntAgeGroup = '02' THEN NULL ---v6
                   --WHEN PRCNT_U18 > 50 THEN NULL ---prior 6
                   
                   WHEN WardAge IN ('13', '14', '15') THEN 'Y'
                   WHEN WardIntendedClinCareMH IN ('51', '52', '53') THEN 'Y'
                   WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') THEN 'Y'
                   WHEN WardType IN ('03', '04', '06') THEN 'Y'
                   
                   --WHEN PRCNT_U18 <= 50 THEN 'Y' ---prior v6
                   
                   ELSE 'Y' END AS MH
 ,UniqWardStayID

 FROM $MHSDS.MHS001MPI
   AS MPI
   
 INNER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF_NEW
   AS PERI
   ON MPI.Person_ID = PERI.Person_ID
   
 INNER JOIN $MHSDS.MHS502WardStay
   AS WRD
   ON MPI.Person_ID = WRD.Person_ID AND (WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID')  --- TU has this been amended in reference to cohort 2 ---- DF - need to see if this actually does anything given we have record start and end dates
   AND WRD.StartDateWardStay <= '$RP_ENDDATE' 
   AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE') 
   AND WRD.StartDateWardStay <= PERI.EndDate24m
   AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate) 
   AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'

 LEFT OUTER JOIN $MHSDS.MHS503AssignedCareProf
   AS APF
   ON WRD.UniqHospProvSpellID = APF.UniqHospProvSpellID AND (APF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND APF.StartDateAssCareProf <= '$RP_ENDDATE' AND (APF.EndDateAssCareProf IS NULL OR APF.EndDateAssCareProf >= '$RP_STARTDATE') AND APF.StartDateAssCareProf <= PERI.EndDate24m AND (APF.EndDateAssCareProf IS NULL OR APF.EndDateAssCareProf >= PERI.StartDate) AND (APF.RecordEndDate IS NULL OR APF.RecordEndDate >= '$RP_ENDDATE') AND APF.RecordStartDate <= '$RP_ENDDATE'
     -- line 72 V5 will change from 'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' 20/10/21 /*** Amended 2 instances in line 72 GF***/
 LEFT OUTER JOIN $MHSDS.MHS101Referral
   AS REF
   ON WRD.UniqServReqID = REF.UniqServReqID AND (REF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE') AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate) AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'
   
  
 LEFT OUTER JOIN $personal_db.ServiceTeamType
   AS SRV
   ON REF.UniqServReqID = SRV.UniqServReqID AND (SRV.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= '$RP_STARTDATE') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= '$RP_STARTDATE')) AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND  ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= PERI.StartDate) AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= PERI.StartDate)) AND (SRV.RecordEndDate IS NULL OR SRV.RecordEndDate >= '$RP_ENDDATE') AND SRV.RecordStartDate <= '$RP_ENDDATE'

 WHERE
 MPI.UniqMonthID = '$MONTH_ID'
 AND MPI.PatMRecInRP = True
 AND (MPI.RecordEndDate IS NULL OR MPI.RecordEndDate >= '$RP_ENDDATE') AND MPI.RecordStartDate <= '$RP_ENDDATE'

# COMMAND ----------

 %sql
 /**CODE TO SPLIT THE DIFFERENT SERVICE AREAS FOR WARD STAYS**/

 --CREATES THE DIFFERENT SERVICE AREAS NON DISTINCT

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list_v5 AS

 SELECT DISTINCT 
 CASE 
 WHEN CAMHSTier IN ('4','9')
 THEN 'Y'
 WHEN TreatFuncCodeMH = '711' --code same
 THEN 'Y'
 WHEN WardType IN ('01', '02')
 THEN 'Y'
 WHEN WardAge IN ('10','11','12')
 THEN 'Y'
 ELSE NULL
 END
 AS CAMHS
 ,CASE 
 WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63') -- code same
 THEN 'Y'
 WHEN TreatFuncCodeMH = '700'
 THEN 'Y'
 WHEN WardType = '05'
 THEN 'Y'
 ELSE NULL
 END
 AS LD
 ,CASE 
 WHEN WardType IN ('01', '02', '05')
 THEN NULL 
 WHEN IntendClinCareIntenCodeMH in ('61', '62', '63') --code same
 THEN NULL
 WHEN CAMHSTier IN ('4','9')
 THEN NULL 
 WHEN TreatFuncCodeMH IN ('700', '711') --code same
 THEN NULL
 WHEN WardAge IN ('10', '11', '12')
 THEN NULL
 WHEN WardAge IN ('13', '14', '15')
 THEN 'Y'
 WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53') --code same
 THEN 'Y'
 WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727') -- v4.1 724 Perinatal psychaiatry, v5 Perinatal Mental Health Service 20/10/21
 THEN 'Y'
 WHEN WardType IN ('03', '04', '06')
 THEN 'Y'
 ELSE 'Y'
 END
 AS MH
 ,UniqWardStayID

 FROM $mhsds.MHS001MPI
   AS MPI
   
 INNER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF_NEW
   AS PERI
   ON MPI.Person_ID = PERI.Person_ID
   
 INNER JOIN $mhsds.MHS502WardStay
   AS WRD
   ON MPI.Person_ID = WRD.Person_ID AND (WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID')  --- TU has this been amended in reference to cohort 2 ---- DF - need to see if this actually does anything given we have record start and end dates
   AND WRD.StartDateWardStay <= '$RP_ENDDATE' 
   AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE') 
   AND WRD.StartDateWardStay <= PERI.EndDate24m
   AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate) 
   AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'

 LEFT OUTER JOIN $mhsds.MHS503AssignedCareProf
   AS APF
   ON WRD.UniqHospProvSpellID = APF.UniqHospProvSpellID AND (APF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND APF.StartDateAssCareProf <= '$RP_ENDDATE' AND (APF.EndDateAssCareProf IS NULL OR APF.EndDateAssCareProf >= '$RP_STARTDATE') AND APF.StartDateAssCareProf <= PERI.EndDate24m AND (APF.EndDateAssCareProf IS NULL OR APF.EndDateAssCareProf >= PERI.StartDate) AND (APF.RecordEndDate IS NULL OR APF.RecordEndDate >= '$RP_ENDDATE') AND APF.RecordStartDate <= '$RP_ENDDATE'
     -- line 72 V5 will change from 'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' 20/10/21 /*** Amended 2 instances in line 72 GF***/
 LEFT OUTER JOIN $mhsds.MHS101Referral
   AS REF
   ON WRD.UniqServReqID = REF.UniqServReqID AND (REF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE') AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate) AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'
   
 LEFT OUTER JOIN $mhsds.MHS102ServiceTypeReferredTo
   AS SRV
   ON REF.UniqServReqID = SRV.UniqServReqID AND (SRV.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= '$RP_STARTDATE') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= '$RP_STARTDATE')) AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND  ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= PERI.StartDate) AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= PERI.StartDate)) AND (SRV.RecordEndDate IS NULL OR SRV.RecordEndDate >= '$RP_ENDDATE') AND SRV.RecordStartDate <= '$RP_ENDDATE'

 WHERE
 MPI.UniqMonthID = '$MONTH_ID'
 AND MPI.PatMRecInRP = True
 AND (MPI.RecordEndDate IS NULL OR MPI.RecordEndDate >= '$RP_ENDDATE') AND MPI.RecordStartDate <= '$RP_ENDDATE'

# COMMAND ----------

 %sql
 select CAMHS, LD, MH, count(*) from global_temp.ward_type_list_v5 group by CAMHS, LD, MH order by CAMHS, LD, MH

# COMMAND ----------

 %sql
 select CAMHS, LD, MH, count(*) from global_temp.ward_type_list_v6 group by CAMHS, LD, MH order by CAMHS, LD, MH

# COMMAND ----------

 %sql
 /**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/

 --CREATES THE DIFFERENT SERVICE AREAS NON-DISTINCT

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list_v6 AS
 SELECT    CASE WHEN WCT.LD = 'Y' THEN 'Y'
                  WHEN SRV.ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN 'Y'
                  ELSE NULL END AS LD
                  
             ,CASE WHEN WCT.CAMHS = 'Y' THEN 'Y'
             
                   WHEN WCT.LD = 'Y' THEN NULL
                   WHEN SRV.ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04' , 'B02', 'C01') THEN NULL
  
                   --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN 'Y'
                   WHEN SRV.ServTeamTypeRefToMH in ('C05', 'C06', 'C07') THEN 'Y'
                   
                   --WHEN PRCNT_U18 > 50 THEN 'Y' ---prior v6 
                   WHEN SRV.ServTeamIntAgeGroup = '02' THEN 'Y' ---v6
                   
                   ELSE NULL END AS CAMHS
                   
             ,CASE WHEN SRV.ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01', 'C05', 'C06', 'C07') THEN NULL 
                   WHEN WCT.LD = 'Y' THEN NULL 
                   WHEN WCT.CAMHS = 'Y' THEN NULL 
                   WHEN WCT.MH = 'Y' THEN 'Y'
                  
                   WHEN ReasonOAT IN ('10','11','12','13','14','15') THEN 'Y'
                   --WHEN CAMHSTier IN ('1', '2', '3', '4','9') THEN NULL
                   
                   --WHEN PRCNT_U18 > 50 THEN NULL
                   WHEN SRV.ServTeamIntAgeGroup = '02' THEN NULL ---v6
                   
 --                   WHEN ServTeamTypeRefToMH IN vc.ValidValue THEN 'Y'
                   WHEN SRV.ServTeamTypeRefToMH 
                   IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'B01', 'C02', 'C03', 'C04', 'C08', 'D01', 'D02', 'D03', 'D04', 'Z01', 'Z02') --'A03 & A04' wil become 'A02' in V5 20/10/21
                   THEN 'Y'
                   WHEN SRV.ServTeamTypeRefToMH IN ('E01', 'E02', 'E03','E04', 'B02', 'C01', 'C05', 'C06', 'C07','C09')
                   THEN NULL
                   --WHEN PRCNT_U18 <= 50 THEN 'Y'
                   ELSE 'Y' END AS MH
                   
             ,REF.UniqServReqID

 FROM $MHSDS.MHS101Referral
   AS REF

 INNER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF_New
   AS PERI
   ON REF.Person_ID = PERI.Person_ID

 LEFT OUTER JOIN $MHSDS.MHS001MPI
   AS MPI
   ON MPI.Person_ID = REF.Person_ID AND MPI.PatMRecInRP = True AND (MPI.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (MPI.RecordEndDate IS NULL OR MPI.RecordEndDate >= '$RP_ENDDATE') AND MPI.RecordStartDate <= '$RP_ENDDATE'

 LEFT OUTER JOIN $MHSDS.MHS501HospProvSpell
   AS HSP
   ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate24m AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

 LEFT OUTER JOIN $MHSDS.MHS502WardStay
   AS WRD
   ON HSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID AND (WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND WRD.StartDateWardStay <= '$RP_ENDDATE' AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE') AND WRD.StartDateWardStay <= PERI.EndDate24m AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate) AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'
 -- V5 will change from 'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' 20/10/21 /*** Amended 2 instances in line 582 GF***/
 LEFT OUTER JOIN global_temp.ward_stay_cats_DF
   AS WCT
   ON WRD.UniqWardStayID = WCT.UniqWardStayID

 LEFT OUTER JOIN $personal_db.ServiceTeamType
   AS SRV
   ON REF.UniqServReqID = SRV.UniqServReqID AND (SRV.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= '$RP_STARTDATE') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= '$RP_STARTDATE')) AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= PERI.StartDate) AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= PERI.StartDate)) AND (SRV.RecordEndDate IS NULL OR SRV.RecordEndDate >= '$RP_ENDDATE') AND SRV.RecordStartDate <= '$RP_ENDDATE'

 WHERE
 REF.UniqMonthID = '$MONTH_ID' -- This was previously REF.UniqMont BETWEEN '$MONTH_ID' - 11 AND REF.Month_ID - changed to be consistent as not sure REF.Month_ID is correct.
 AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
 AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
 AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'

# COMMAND ----------

 %sql
 /**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/

 --CREATES THE DIFFERENT SERVICE AREAS NON-DISTINCT

 CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list_v5 AS
 SELECT 
 CASE 
   WHEN WCT.LD = 'Y'
   THEN 'Y'
   WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01') -- code same
   THEN 'Y'
   ELSE NULL
   END
   AS LD
 ,CASE 
   WHEN WCT.CAMHS = 'Y'
   THEN 'Y'
   WHEN CAMHSTier IN ('1', '2', '3', '4','9')
   THEN 'Y'
   WHEN ServTeamTypeRefToMH in ('C05', 'C06', 'C07','C09')
   THEN 'Y'
   ELSE NULL
   END
   AS CAMHS
 ,CASE 
   WHEN WCT.MH = 'Y'
   THEN 'Y'
   WHEN ReasonOAT IN ('10','11','12','13','14','15')
   THEN 'Y'
   WHEN CAMHSTier IN ('1', '2', '3', '4','9')
   THEN NULL
   WHEN ServTeamTypeRefToMH IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'B01', 'C02', 'C03', 'C04', 'C08', 'D01', 'D02', 'D03', 'D04', 'Z01', 'Z02') --'A03 & A04' wil become 'A02' in V5 20/10/21
   THEN 'Y'
   WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03','E04', 'B02', 'C01', 'C05', 'C06', 'C07','C09')
   THEN NULL
   ELSE 'Y'
   END
   AS MH
 ,REF.UniqServReqID

 FROM $mhsds.MHS101Referral
   AS REF

 INNER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF_New
   AS PERI
   ON REF.Person_ID = PERI.Person_ID

 LEFT OUTER JOIN $mhsds.MHS001MPI
   AS MPI
   ON MPI.Person_ID = REF.Person_ID AND MPI.PatMRecInRP = True AND (MPI.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (MPI.RecordEndDate IS NULL OR MPI.RecordEndDate >= '$RP_ENDDATE') AND MPI.RecordStartDate <= '$RP_ENDDATE'

 LEFT OUTER JOIN $mhsds.MHS501HospProvSpell
   AS HSP
   ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate24m AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

 LEFT OUTER JOIN $mhsds.MHS502WardStay
   AS WRD
   ON HSP.UniqHospProvSpellID = WRD.UniqHospProvSpellID AND (WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND WRD.StartDateWardStay <= '$RP_ENDDATE' AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE') AND WRD.StartDateWardStay <= PERI.EndDate24m AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate) AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'
 -- V5 will change from 'UNIQHOSPPROVSPELLNUM' TO 'UniqHospProvSpellID' 20/10/21 /*** Amended 2 instances in line 582 GF***/
 LEFT OUTER JOIN global_temp.ward_stay_cats_DF
   AS WCT
   ON WRD.UniqWardStayID = WCT.UniqWardStayID

 LEFT OUTER JOIN $mhsds.MHS102ServiceTypeReferredTo
   AS SRV
   ON REF.UniqServReqID = SRV.UniqServReqID AND (SRV.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= '$RP_STARTDATE') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= '$RP_STARTDATE')) AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= PERI.StartDate) AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= PERI.StartDate)) AND (SRV.RecordEndDate IS NULL OR SRV.RecordEndDate >= '$RP_ENDDATE') AND SRV.RecordStartDate <= '$RP_ENDDATE'

 WHERE
 REF.UniqMonthID = '$MONTH_ID'-- This was previously REF.UniqMont BETWEEN '$MONTH_ID' - 11 AND REF.Month_ID - changed to be consistent as not sure REF.Month_ID is correct.
 AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
 AND REF.ReferralRequestReceivedDate <= PERI.EndDate24m AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
 AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'

# COMMAND ----------

 %sql
 select CAMHS, LD, MH, count(*) from global_temp.referral_list_v5 group by CAMHS, LD, MH order by CAMHS, LD, MH

# COMMAND ----------

 %sql
 select CAMHS, LD, MH, count(*) from global_temp.referral_list_v6 group by CAMHS, LD, MH order by CAMHS, LD, MH

# COMMAND ----------

