# Databricks notebook source

%sql
CREATE OR REPLACE GLOBAL TEMP VIEW Cont AS
SELECT        c.UniqMonthID
              ,c.OrgIDProv
              ,c.Person_ID
              ,c.UniqServReqID
              ,c.RecordNumber
              ,c.UniqCareContID AS ContID
              ,c.CareContDate AS ContDate
              ,c.ConsMechanismMH
FROM          $db_source.MHS201CareContact c
LEFT JOIN $db_output.validcodes as vc
  ON vc.table = 'mhs201carecontact' 
  and vc.field = 'ConsMechanismMH' 
  and vc.Measure = '72HOURS' 
  and vc.type = 'exclude' 
  and c.ConsMechanismMH = vc.ValidValue 
  and c.UniqMonthID >= vc.FirstMonth 
  and (vc.LastMonth is null or c.UniqMonthID <= vc.LastMonth)
WHERE         c.UniqMonthID <= $month_id 
              AND c.AttendOrDNACode IN ('5','6') 
              AND vc.Measure is null -- no right hand side if ConsMechanismMH matches nothing in exclude list

union all

SELECT        i.UniqMonthID 
              ,i.OrgIDProv
              ,i.Person_ID
              ,i.UniqServReqID
              ,i.RecordNumber
              ,CAST(i.MHS204UniqID AS string) AS ContID
              ,i.IndirectActDate AS ContDate
              ,NULL AS ConsMechanismMH
FROM          $db_source.MHS204IndirectActivity i
WHERE         i.UniqMonthID <= $month_id

# COMMAND ----------

%sql
CREATE OR REPLACE GLOBAL TEMP VIEW hosp AS
SELECT      h.uniqmonthid
            ,h.orgidprov
            ,h.person_id
            ,h.uniqservreqid
            ,h.uniqhospprovspellid
            ,h.recordnumber
            ,h.startdatehospprovspell
            ,h.DischDateHospProvSpell
            ,h.inacttimehps
            ,h.methofdischmhhospprovspell
            ,h.destofdischhospprovspell
            ,row_number() over(partition by h.person_id, h.uniqservreqid, h.uniqhospprovspellid order by h.uniqmonthid desc) as HospRN
FROM        $db_source.mhs501hospprovspell h
INNER JOIN  $db_source.mhs001mpi m on h.recordnumber = m.recordnumber and (m.ladistrictauth like 'E%' OR ladistrictauth is null OR ladistrictauth = '')
WHERE       h.uniqmonthid between $month_id-1 and $month_id

# COMMAND ----------

%sql

CREATE OR REPLACE GLOBAL TEMP VIEW Bed AS
SELECT      w.UniqMonthID
            ,w.Person_ID
            ,w.UniqServReqID
            ,w.uniqhospprovspellid
            ,w.UniqWardStayID
            ,w.HospitalBedTypeMH
            ,ROW_NUMBER() OVER(PARTITION BY w.Person_ID, w.UniqServReqID, w.uniqhospprovspellid ORDER BY w.UniqMonthID DESC, w.InactTimeWS DESC, w.EndDateWardStay DESC, w.MHS502UniqID DESC) AS BedRN     
FROM        $db_source.MHS502WardStay w
WHERE       w.UniqMonthID BETWEEN $month_id-1 and $month_id

# COMMAND ----------

%sql
CREATE OR REPLACE GLOBAL TEMP VIEW Follow AS
SELECT      c.Person_ID
            ,c.OrgIdProv
            ,c.ContDate
FROM        global_temp.Cont c
INNER JOIN $db_output.validcodes as vc
  ON vc.table = 'mhs201carecontact' 
  and vc.field = 'ConsMechanismMH' 
  and vc.Measure = '72HOURS_FOLLOWUP' 
  and vc.type = 'include' 
  and c.ConsMechanismMH = vc.ValidValue 
  and c.UniqMonthID >= vc.FirstMonth 
  and (vc.LastMonth is null or c.UniqMonthID <= vc.LastMonth)

Union all

SELECT      h.Person_ID
            ,h.OrgIdProv
            ,h.StartDateHospProvSpell AS ContDate
FROM        global_temp.Hosp h
WHERE       h.HospRN = 1

# COMMAND ----------

%sql

CREATE OR REPLACE GLOBAL TEMP VIEW Onward AS
SELECT DISTINCT   o.Person_ID
                  ,o.UniqServReqID
                  ,o.OnwardReferDate
                  ,o.OrgIdProv
                  ,CASE 
                      WHEN map.OrgIDProvider IS NULL THEN NULL
                      WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN o.OrgIDReceiving 
                      ELSE LEFT(o.OrgIDReceiving,3) 
                  END AS OrgIDReceiving
FROM              $db_source.MHS105OnwardReferral o
LEFT JOIN 
          (
          SELECT DISTINCT h.OrgIDProvider
          FROM             $db_source.MHS000Header h
          WHERE            h.UniqMonthID BETWEEN $month_id - 1 and $month_id
          ) map 
          ON CASE WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN o.OrgIDReceiving ELSE LEFT(o.OrgIDReceiving,3) END = map.OrgIDProvider
WHERE     CASE WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN OrgIDProv ELSE LEFT(o.OrgIDReceiving,3) END <> o.OrgIdProv

# COMMAND ----------

# DBTITLE 1,CCG_prep_2months for 2 month period
%sql

-- Renamed this view as it referenced EIP which is not relevant - the time period is the relevant part
-- suspect that the limit to most recent good record WITH CCG is not consistent with other places - likely to be reducing the number of UNKNOWNs

CREATE OR REPLACE GLOBAL TEMP VIEW CCG_prep_2months AS
SElECT DISTINCT    a.Person_ID
				   ,max(a.RecordNumber) as recordnumber	
FROM               $db_source.MHS001MPI a
LEFT JOIN          $db_source.MHS002GP b 
		           on a.Person_ID = b.Person_ID 
                   and a.UniqMonthID = b.UniqMonthID
		           and a.recordnumber = b.recordnumber
		           and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
		           and b.EndDateGMPRegistration is null
LEFT JOIN          $db_output.RD_CCG_LATEST c on  
                      CASE WHEN a.UNIQMONTHID <= 1467 THEN a.OrgIDCCGRes 
                      ELSE a.OrgIDSubICBLocResidence
                      END = c.ORG_CODE

LEFT JOIN          $db_output.RD_CCG_LATEST e on 
                      CASE WHEN b.UNIQMONTHID <= 1467 THEN b.OrgIDCCGGPPractice
                      ELSE b.OrgIDSubICBLocGP
                      END = e.ORG_CODE
                      
WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)

                   and a.uniqmonthid between $month_id - 1 AND $month_id
GROUP BY           a.Person_ID

# COMMAND ----------

%sql
CREATE OR REPLACE GLOBAL TEMP VIEW CCG_LATEST_2months AS
select distinct    a.Person_ID,
				   CASE 
                     WHEN b.UNIQMONTHID <= 1467 and (OrgIDCCGGPPractice is not null and e.ORG_CODE is not null) then OrgIDCCGGPPractice
                     WHEN b.UNIQMONTHID > 1467 and (OrgIDSubICBLocGP is not null and e.ORG_CODE is not null) then OrgIDSubICBLocGP 
                     WHEN a.UNIQMONTHID <= 1467 and (OrgIDCCGRes is not null and c.ORG_CODE is not null) then OrgIDCCGRes 
                     WHEN a.UNIQMONTHID > 1467 and (OrgIDSubICBLocResidence is not null and c.ORG_CODE is not null) then OrgIDSubICBLocResidence 
						ELSE 'UNKNOWN' 
                        END AS IC_Rec_CCG		
FROM               $db_source.mhs001MPI a
LEFT JOIN          $db_source.MHS002GP b 
                   on a.Person_ID = b.Person_ID 
                   and a.UniqMonthID = b.UniqMonthID  
                   and a.recordnumber = b.recordnumber
                   and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                   --and b.OrgIDGPPrac <> '-1' -- clause removed, check the methodology page of the publication for further details.
                   and b.EndDateGMPRegistration is null
INNER JOIN         global_temp.CCG_prep_2months ccg on a.recordnumber = ccg.recordnumber

LEFT JOIN          $db_output.RD_CCG_LATEST c on  
                      CASE WHEN a.UNIQMONTHID <= 1467 THEN a.OrgIDCCGRes 
                      ELSE a.OrgIDSubICBLocResidence
                      END = c.ORG_CODE

LEFT JOIN          $db_output.RD_CCG_LATEST e on 
                      CASE WHEN b.UNIQMONTHID <= 1467 THEN b.OrgIDCCGGPPractice
                      ELSE b.OrgIDSubICBLocGP
                      END = e.ORG_CODE
                      
WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                   and a.uniqmonthid between $month_id - 1 AND $month_id

# COMMAND ----------

%sql

CREATE OR REPLACE GLOBAL TEMP VIEW Disch AS
SELECT DISTINCT       
       h.UniqMonthID
      ,h.OrgIDProv
      ,COALESCE(o.OrgIDReceiving,h.OrgIDProv) AS ResponsibleProv
      ,h.Person_ID
      ,h.UniqServReqID
      ,h.uniqhospprovspellid
      ,h.RecordNumber
      ,h.DischDateHospProvSpell
      ,CASE WHEN h.DischDateHospProvSpell between date_add('$rp_startdate',-4) and date_add('$rp_enddate',-4)  
            THEN 1 ELSE 0 END AS DischFlag
      ,CASE WHEN h.InactTimeHPS < '$rp_enddate' 
            THEN 1 ELSE 0 END AS InactiveFlag
      ,CASE WHEN (MethOfDischMHHospProvSpell NOT IN ('4','5') OR MethOfDischMHHospProvSpell is null)
            AND ((DestOfDischHospProvSpell is not null AND  vc.Measure is null) OR DestOfDischHospProvSpell IS NULL) --Updated to include null discharge destinations.
            AND DischDateHospProvSpell between date_add('$rp_startdate',-4) and date_add('$rp_enddate',-4)        
            THEN 1 ELSE 0 
            END AS ElgibleDischFlag
      ,CASE WHEN b.HospitalBedTypeMH IN ('10', '11', '12', '16', '17', '18') 
            THEN 1 ELSE 0 END AS AcuteBed
      ,CASE WHEN b.HospitalBedTypeMH IN ('13', '14', '15', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34') 
            THEN 1 ELSE 0 END AS OtherBed
      ,CASE WHEN b.HospitalBedTypeMH NOT IN ('10', '11', '12', '16', '17', '18', '13', '14', '15', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34') 
            THEN 1 ELSE 0 END AS InvalidBed
      ,CASE WHEN b.HospitalBedTypeMH IS NULL 
            THEN 1 ELSE 0 END AS MissingBed
      ,CASE WHEN DATEDIFF(m.PersDeathDate, h.DischDateHospProvSpell) <= 3  
            THEN 1 ELSE 0 END AS DiedBeforeFollowUp
      ,CASE WHEN vc1.Measure is not null
            THEN 1 ELSE 0 END AS PrisonCourtDischarge
FROM  global_temp.Hosp h
LEFT  JOIN global_temp.Onward o 
           ON h.Person_ID = o.Person_ID 
           AND h.UniqServReqID = o.UniqServReqID 
           AND OnwardReferDate between h.StartDateHospProvSpell and h.DischDateHospProvSpell
 
LEFT JOIN $db_output.validcodes as vc
  ON vc.table = 'mhs501hospprovspell' 
  and vc.field = 'DestOfDischHospProvSpell' 
  and vc.Measure = '72HOURS' 
  and vc.type = 'exclude' 
  and h.DestOfDischHospProvSpell = vc.ValidValue 
  and h.UniqMonthID >= vc.FirstMonth 
  and (vc.LastMonth is null or h.UniqMonthID <= vc.LastMonth)       
           
LEFT JOIN $db_output.validcodes as vc1
  ON vc1.table = 'mhs501hospprovspell' 
  and vc1.field = 'DestOfDischHospProvSpell' 
  and vc1.Measure = '72HOURS' 
  and vc1.type = 'include' 
  and h.DestOfDischHospProvSpell = vc1.ValidValue 
  and h.UniqMonthID >= vc1.FirstMonth 
  and (vc1.LastMonth is null or h.UniqMonthID <= vc1.LastMonth)           
           
LEFT  JOIN global_temp.Bed b 
           ON b.Person_ID = h.Person_ID 
           AND b.UniqServReqID = h.UniqServReqID 
           AND b.uniqhospprovspellid = h.uniqhospprovspellid 
           AND b.BedRN = 1
LEFT  JOIN $db_source.MHS001MPI m 
           ON m.Person_ID = h.Person_ID 
           AND m.OrgIDProv = COALESCE(o.OrgIDReceiving,h.OrgIDProv) 
           AND m.PersDeathDate IS NOT NULL 
           AND (m.UniqMonthID = h.UniqMonthID OR m.UniqMonthID = h.UniqMonthID+1)
WHERE h.HospRN = 1                      

# COMMAND ----------

%sql

CREATE OR REPLACE GLOBAL TEMP VIEW Fup AS
SELECT d.UniqMonthID
      ,d.OrgIDProv
      ,CASE WHEN d.ElgibleDischFlag = 1 AND AcuteBed = 1 
            THEN d.ResponsibleProv 
            ELSE d.OrgIDProv 
            END AS ResponsibleProv
      ,d.Person_ID
      ,d.UniqServReqID
      ,d.uniqhospprovspellid
      ,d.RecordNumber
      ,d.DischDateHospProvSpell
      ,d.DischFlag
      ,d.InactiveFlag
      ,d.ElgibleDischFlag
      ,d.AcuteBed
      ,d.OtherBed
      ,d.InvalidBed
      ,d.MissingBed
      ,d.DiedBeforeFollowUp
      ,d.PrisonCourtDischarge
      ,c.FirstCont
      ,DATEDIFF(c.FirstCont, d.DischDateHospProvSpell) as diff
      ,CASE WHEN DATEDIFF(c.FirstCont, d.DischDateHospProvSpell)  <= 3
            THEN 1 ELSE 0 
            END AS FollowedUp3Days
      ,CASE WHEN c.FirstCont IS NULL 
            THEN 1 ELSE 0 
            END AS NoFollowUp
FROM  global_temp.Disch d
LEFT  JOIN
(
   SELECT       h.Person_ID
                ,h.UniqServReqID
                ,h.uniqhospprovspellid
                ,MIN(c.ContDate) AS FirstCont

   FROM         global_temp.Disch h
   INNER JOIN 
   (
     SELECT       c.Person_ID
     ,c.OrgIdProv
     ,c.ContDate
     FROM         global_temp.Follow c) c 
     ON c.Person_ID = h.Person_ID 
     AND c.ContDate > h.DischDateHospProvSpell 
     AND c.OrgIdProv = h.ResponsibleProv
     GROUP BY     h.Person_ID,  h.UniqServReqID, h.uniqhospprovspellid
) AS c 
ON d.Person_ID = c.Person_ID 
AND d.UniqServReqID = c.UniqServReqID 
AND d.uniqhospprovspellid = c.uniqhospprovspellid