# Databricks notebook source
# DBTITLE 1,CYP02 CCG and National
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CYP02_prep AS
      SELECT MPI.Person_ID
             ,MPI.IC_Rec_CCG
             ,MPI.NAME
        FROM $db_output.MHS001MPI_latest_month_data MPI
  INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
             ON MPI.Person_ID = REF.Person_ID 
  INNER JOIN $db_output.MHS701CPACareEpisode_latest AS CPAE
             ON MPI.Person_ID = CPAE.Person_ID 
       WHERE REF.CYPServiceRefEndRP_temp = true

# COMMAND ----------

# DBTITLE 1,MHS30d Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS30d_Prep AS
 SELECT A.* 
 ,MPI.PatMRecInRP
 FROM global_temp.mhs29_prep AS A  
 INNER JOIN  $db_output.MHS001MPI_latest_month_data AS MPI
             ON A.Person_ID = MPI.Person_ID
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.PERSON_ID = REF.PERSON_ID
             AND REF.UniqMonthID = '$month_id'
             AND REF.UniqServReqID = A.UniqServReqID
 WHERE       REF.AgeServReferRecDate  BETWEEN 0 AND 18
             AND A.AttendOrDNACode IN ('5','6')

# COMMAND ----------

# DBTITLE 1,CYP32 CCG AND NATIONAL
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CYP32_prep AS
   SELECT   REF.UniqServReqID 
            ,REF.AgeServReferRecDate
            ,MPI.IC_Rec_CCG
            ,MPI.NAME
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN global_temp.MHS101Referral_service_area_RPstart AS REF
            ON MPI.Person_ID = REF.Person_ID
      WHERE --REF.ReferralRequestReceivedDate >= '$rp_startdate'
 	       --AND REF.ReferralRequestReceivedDate <= '$rp_enddate'
            --AND REF.UniqMonthID = '$month_id'
 	       --AND 
            CYPServiceRefStartRP_temp = true;

# COMMAND ----------

# DBTITLE 1,CYP32 Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW CYP32_prep_prov AS
   SELECT   UniqServReqID 
            ,AgeServReferRecDate
            ,OrgIDProv
       FROM global_temp.MHS101Referral_service_area_RPstart REF
      WHERE --REF.ReferralRequestReceivedDate >= '$rp_startdate'
 	       --AND REF.ReferralRequestReceivedDate <= '$rp_enddate'
            --AND REF.UniqMonthID = '$month_id'
 	       --AND 
            CYPServiceRefStartRP_temp  = true;

# COMMAND ----------

# DBTITLE 1,MHS32a/b National and CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MH32_prep AS
   SELECT   REF.UniqServReqID 
            ,REF.SourceOfReferralMH
            ,MPI.IC_Rec_CCG
            ,MPI.NAME
            ,CASE WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing'
                 ELSE COALESCE(LEFT(vc.ValidValue, 1), 'Invalid')
                 END AS Referral_Source
            ,CASE WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing'
                 ELSE COALESCE(rd.Referral_Description, 'Invalid')
                 END AS Referral_Description 
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
       INNER JOIN $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID
       LEFT JOIN $db_output.validcodes as vc
             ON vc.table = 'mhs101referral' 
             and vc.field = 'SourceOfReferralMH' 
             and vc.Measure = 'MHS32' 
             and vc.type = 'include' 
             and REF.SourceOfReferralMH = vc.ValidValue
             and '$month_id' >= vc.FirstMonth 
             and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
       LEFT JOIN $db_output.referral_dim as rd
             ON LEFT(vc.ValidValue, 1) = rd.Referral_Source
             and '$month_id' >= rd.FirstMonth and (rd.LastMonth is null or '$month_id' <= rd.LastMonth)
       WHERE REF.UniqMonthID = '$month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate' 	 
         AND REF.AgeServReferRecDate BETWEEN 0 AND 18 
 	  

# COMMAND ----------

# DBTITLE 1,MHS32a/b Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MH32_prep_prov AS
   SELECT   REF.UniqServReqID
            ,REF.SourceOfReferralMH
            ,REF.OrgIDProv
            ,CASE WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing'
                 ELSE COALESCE(LEFT(vc.ValidValue, 1), 'Invalid')
                 END AS Referral_Source
            ,CASE WHEN REF.SourceOfReferralMH IS NULL THEN 'Missing'
                 ELSE COALESCE(rd.Referral_Description, 'Invalid')
                 END AS Referral_Description 
        FROM $db_source.MHS101Referral AS REF
        LEFT JOIN $db_output.validcodes as vc
             ON vc.table = 'mhs101referral' and vc.field = 'SourceOfReferralMH' and vc.Measure = 'MHS32' and vc.type = 'include' and REF.SourceOfReferralMH = vc.ValidValue
             and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)
       LEFT JOIN $db_output.referral_dim as rd
             ON LEFT(vc.ValidValue, 1) = rd.Referral_Source
             and '$month_id' >= rd.FirstMonth and (rd.LastMonth is null or '$month_id' <= rd.LastMonth)
       WHERE REF.UniqMonthID = '$month_id' 
         AND REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND '$rp_enddate' 	 
         AND REF.AgeServReferRecDate BETWEEN 0 AND 18 

# COMMAND ----------

# DBTITLE 1,MHS38/39 National and CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS3839_prep AS      
      SELECT REF.Person_ID 
            ,REF.ReferralRequestReceivedDate
            ,REF.AgeServReferRecDate 
            ,REF.UniqServReqID
            ,MPI.IC_Rec_CCG 
            ,MPI.NAME
            ,CASE WHEN REF.ServDischDate BETWEEN '$rp_startdate' and '$rp_enddate'
                  then REF.AgeServReferDischDate   
                  when REF.ServDischDate is null or REF.ServDischDate>'$rp_enddate'
                  then MPI.AgeRepPeriodEnd 
                  END AS Age
            
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_source.MHS101Referral AS REF
         ON MPI.Person_ID=REF.Person_ID 
      WHERE REF.UniqMonthID = '$month_id'
      

# COMMAND ----------

# DBTITLE 1,MHS38/39 Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS3839_prep_prov AS       
     SELECT  REF.Person_ID
            ,REF.ReferralRequestReceivedDate
            ,REF.AgeServReferRecDate 
            ,REF.UniqServReqID
            ,MPI.OrgIDProv
            ,CASE WHEN REF.ServDischDate BETWEEN '$rp_startdate' and '$rp_enddate'
                  then REF.AgeServReferDischDate   
                  when REF.ServDischDate is null or REF.ServDischDate>'$rp_enddate'
                  then MPI.AgeRepPeriodEnd 
                  END AS Age
            
       FROM $db_source.MHS001MPI AS MPI
 INNER JOIN $db_source.MHS101Referral AS REF
         ON MPI.Person_ID=REF.Person_ID 
        AND MPI.OrgIDProv=REF.OrgIDProv
      WHERE REF.UniqMonthID = '$month_id'
            AND MPI.UniqMonthID  = '$month_id'; 

# COMMAND ----------

# DBTITLE 1,MHS40/41/42 National and CCG
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS404142_prep AS        
     SELECT MPI.Person_ID
            ,MPI.IC_Rec_CCG 
            ,MPI.NAME
            ,PAT.LACStatus
            ,PAT.CPP
            ,PAT.YoungCarer
       FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_source.MHS101Referral AS REF
            ON MPI.Person_ID =REF.Person_ID
            AND REF.UniqMonthID = '$month_id' 
 INNER JOIN $db_source.MHS005PatInd as PAT 
            ON MPI.Person_ID= PAT.Person_ID
            AND PAT.UniqMonthID = '$month_id' 
      WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate' AND  '$rp_enddate'
            AND REF.AgeServReferRecDate BETWEEN 0 AND 18; 
           

# COMMAND ----------

# DBTITLE 1,MHS40/41/42 Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS404142_prep_prov AS     
     SELECT MPI.Person_ID
            ,MPI.OrgIDProv
            ,PAT.LACStatus
            ,PAT.CPP
            ,PAT.YoungCarer
       FROM $db_source.MHS001MPI AS MPI
 INNER JOIN $db_source.MHS101Referral AS REF
           ON MPI.Person_ID = REF.Person_ID
           AND MPI.OrgIDProv=REF.OrgIDProv
           AND REF.UniqMonthID = '$month_id' 
 INNER JOIN $db_source.MHS005PatInd as PAT 
           ON MPI.Person_ID =PAT.Person_ID
           AND PAT.UniqMonthID = '$month_id' 
           AND MPI.OrgIDProv=PAT.OrgIDProv
     WHERE REF.ReferralRequestReceivedDate BETWEEN  '$rp_startdate' AND  '$rp_enddate'
           AND REF.AgeServReferRecDate BETWEEN 0 AND 18
           AND MPI.UniqMonthID  = '$month_id'; 
           

# COMMAND ----------

# DBTITLE 1,MHS56a Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS56a_Prep AS
     SELECT   MPI.Person_ID
             ,MPI.IC_Rec_CCG
             ,MPI.NAME
             ,REF.OrgIDProv
     FROM    $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID
             AND REF.UniqMonthID = '$month_id'
 INNER JOIN  $db_source.MHS204IndirectActivity AS IND     
             ON REF.UniqServReqID = IND.UniqServReqID 
             AND IND.UniqMonthID = '$month_id'
 WHERE       REF.AgeServReferRecDate BETWEEN 0 AND 18
             AND IND.IndirectActDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,MHS57a Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS57a_Prep AS
     SELECT   MPI.Person_ID
             ,MPI.IC_Rec_CCG
             ,MPI.NAME
             ,REF.OrgIDProv
     FROM    $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN  $db_source.MHS101Referral AS REF
             ON MPI.Person_ID = REF.Person_ID 
             AND REF.UniqMonthID = '$month_id' 
 WHERE       REF.ServDischDate between '$rp_startdate' AND '$rp_enddate'
             AND REF.AgeServReferRecDate BETWEEN 0 AND 18

# COMMAND ----------

# DBTITLE 1,MHS58a Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS58a_Prep AS
     SELECT   B.AttendOrDNACode
             ,B.DNA_Reason
             ,B.UniqCareContID
             ,B.IC_Rec_CCG
             ,B.NAME
             ,B.OrgIDProv
             ,A.AgeServReferRecDate
     FROM    global_temp.MHS3839_prep AS A
 INNER JOIN  global_temp.MHS29_prep AS B
             ON A.UniqServReqID = B.UniqServReqID
 WHERE       A.AgeServReferRecDate BETWEEN 0 AND 18 
 AND         B.AttendOrDNACode IN (2, 3, 4, 7)
             

# COMMAND ----------

# DBTITLE 1,MHS61  National Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS61_prep AS
      SELECT CC.UniqServReqID
             ,min(CC.CareContDate) AS FirstCareContDate
      FROM $db_source.MHS201CareContact as CC
      INNER JOIN $db_source.MHS101Referral as REF
           ON CC.UniqServReqID=REF.UniqServReqID 
           AND REF.UniqMonthID = '$month_id' 
           AND REF.ReferralRequestReceivedDate >= '2016-01-01' 
           AND (REF.ServDischDate is null or REF.ServDischDate>='$rp_startdate')
                 
 WHERE  CC.CareContDate >= '2016-01-01' 
       AND CC.AttendOrDNACode in ('5','6')
       AND REF.AgeServReferRecDate between 0 and 18
       group by CC.UniqServReqID;

# COMMAND ----------

# DBTITLE 1,MHS61a CCG/Provider
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS61a_CCG_Prov AS
               SELECT A.UniqServReqID
                      ,CCG.IC_Rec_CCG
                      ,CCG.NAME
                      ,CC.OrgIDProv
                 FROM global_temp.MHS61_Prep as A
           INNER JOIN $db_source.MHS201CareContact AS CC 
           ON         A.UniqServReqID = CC.UniqServReqID 
           AND A.FirstCareContDate = CC.CareContDate 
           AND CC.UniqMOnthID = '$month_id' 
           INNER JOIN $db_source.MHS001MPI AS MPI
           ON         CC.Person_ID = MPI.Person_ID
           AND        MPI.UniqMonthID = '$month_id' 
          LEFT JOIN global_temp.CCG AS CCG
                 ON MPI.Person_ID = CCG.Person_ID
              WHERE A.FirstCareContDate between '$rp_startdate' and '$rp_enddate'
           GROUP BY CCG.IC_REC_CCG,
                    CCG.NAME,
                    CC.orgidprov,
                    A.uniqservreqid;

# COMMAND ----------

# DBTITLE 1,MHS61b National Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS61b AS
 SELECT a.uniqcarecontid
        ,a.uniqservreqid
        ,a.carecontdate
        ,row_number() over (partition by a.uniqservreqid order by a.carecontdate asc, a.MHS201UniqID asc) as rn  --Azeez
      --,row_number() over (partition by a.uniqservreqid order by a.carecontdate asc, a.uniqcarecontid asc) as rn  -- Old code
 FROM $db_source.MHS201carecontact as a
 INNER JOIN $db_source.MHS101referral as b
 on a.uniqservreqid = b.uniqservreqid
 and b.uniqmonthid = '$month_id'
 and b.referralrequestreceiveddate>='2016-01-01'
 and (b.servdischdate is null or b.servdischdate>='$rp_startdate')
 where a.carecontdate >= '2016-01-01'
 and a.attendordnacode in (5,6)
 and b.ageservreferrecdate between 0 and 18
 group by a.uniqservreqid, a.carecontdate, a.uniqcarecontid, a.MHS201UniqID;
 -- a.MHS201UniqID is new

# COMMAND ----------

# DBTITLE 1,MHS61b CCG Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW mhs61b_ccg AS
 SELECT a.uniqcarecontid, ccg.ic_rec_ccg, ccg.name, b.ConsMechanismMH
 FROM global_temp.MHS61b as a
 INNER JOIN $db_source.mhs201carecontact as b
 ON a.uniqcarecontid = b.uniqcarecontid and a.carecontdate = b.carecontdate and b.uniqmonthid = '$month_id' 
 INNER JOIN $db_source.MHS001MPI as c
 ON b.person_id = c.person_id and c.uniqmonthid = '$month_id' 
 LEFT JOIN global_temp.ccg as ccg
 ON c.person_id = ccg.person_id
 WHERE a.carecontdate BETWEEN '$rp_startdate' and '$rp_enddate'
 and a.rn = 1
 GROUP BY ccg.ic_rec_ccg, ccg.name, a.uniqcarecontid, b.ConsMechanismMH;

# COMMAND ----------

# DBTITLE 1,MHS61b Prov Prep
 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW mhs61b_prov as
 SELECT a.uniqcarecontid, b.ConsMechanismMH, b.orgidprov
 FROM global_temp.MHS61b as a
 INNER JOIN $db_source.mhs201carecontact as b
 on a.uniqcarecontid = b.uniqcarecontid and a.carecontdate = b.carecontdate and b.uniqmonthid = '$month_id' --and b.ic_use_submission_flag = 'Y'
 WHERE a.carecontdate between '$rp_startdate' and '$rp_enddate' and a.rn =1
 GROUP BY a.uniqcarecontid, b.ConsMechanismMH, b.orgidprov;

# COMMAND ----------

# DBTITLE 1,MHS68 CCG and National
 %sql
 /*** MHS68 - Referrals with any SNOMED Codes and valid PERS score from MH Assess Scale Current View completed in RP, aged 0-18***/
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS68_prep AS
       SELECT REF.UniqServReqID
              ,MPI.IC_Rec_CCG
              ,MPI.NAME
              ,MPI.OrgIDProv
         FROM $db_output.MHS001MPI_latest_month_data AS MPI
   INNER JOIN $db_source.MHS101Referral AS REF
 			 ON MPI.Person_ID = REF.Person_ID 
              AND REF.UniqmonthID = '$month_id' 
   INNER JOIN $db_source.MHS606CodedScoreAssessmentRefer	AS CSA
 			 ON REF.UniqServReqID = CSA.UniqServReqID
        WHERE CSA.UniqMonthID = '$month_id' 
 		     AND REF.ReferralRequestReceivedDate <= '$rp_enddate' -- New Referrals
              
 		     AND  -- Needs pre-v5 column (AssToolCompDate) now mapped to v5 column AssToolCompTimestamp
 
               (csa.AssToolCompTimestamp between '$rp_startdate' and '$rp_enddate')
   
              AND REF.AgeServReferRecDate BETWEEN 0 AND 18 -- Age at Ref
              AND CSA.PersScore IN ('0', '1', '2', '3', '888', '999') -- Coded Assess valid Score`
              AND CSA.CodedAssToolType IN
               ('987251000000103','987261000000100','987271000000107','987281000000109','987291000000106'
               ,'987301000000105','987311000000107','987321000000101','987331000000104','987341000000108'
               ,'987351000000106','987361000000109','987371000000102','987381000000100','987391000000103'
               ,'987401000000100','987411000000103','987421000000109','987431000000106','987441000000102'
               ,'987451000000104','987461000000101','987471000000108','987481000000105','987491000000107'
               ,'987501000000101','987511000000104','987521000000105','987531000000107','987541000000103'
               ,'987191000000101','987201000000104','987211000000102','987221000000108','987231000000105'
               ,'987241000000101')  ;

# COMMAND ----------

