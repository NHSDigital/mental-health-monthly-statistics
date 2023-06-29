-- Databricks notebook source
%py
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output
db_source=dbutils.widgets.get("db_source")
print(db_source)
assert db_source
status=dbutils.widgets.get("status")
print(status)
assert status
rp_startdate=dbutils.widgets.get("rp_startdate")
print(rp_startdate)
assert rp_startdate
rp_enddate=dbutils.widgets.get("rp_enddate")
print(rp_enddate)
assert rp_enddate
database = dbutils.widgets.get("database")
print(database)
assert(database)

-- COMMAND ----------

-- DBTITLE 1,Create MHA_RD_ORG_DAILY_LATEST 
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_RD_ORG_DAILY_LATEST AS

SELECT DISTINCT	ORG_CODE
    , NAME
    , CASE 				
        WHEN ORG_TYPE_CODE IN ('CT','TR') THEN 'NHS TRUST'
        WHEN ORG_TYPE_CODE IN ('PH','LA','NN') THEN 'INDEPENDENT HEALTH PROVIDER'
      END AS ORG_TYPE_CODE								
FROM	$database.ORG_DAILY	
WHERE			(BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)	
			AND BUSINESS_START_DATE <= '$rp_enddate' AND ORG_TYPE_CODE in ('CC','CF','LB','PT','CT','OU','NS','TR','HA','LA','PH','NN')	
			AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)	
			AND ORG_OPEN_DATE <= '$rp_enddate'


-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS001MPI_LATEST
-- Get latest MPI records for the year.

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_MHS001MPI_LATEST AS

SELECT			 
				 B.UniqMonthID
				,B.orgidProv
				,B.Person_ID
				,B.Gender
				,B.NHSDEthnicity as EthnicCategory
				,B.AgeRepPeriodEnd
                ,A.IC_REC_CCG
                ,A.NAME
FROM			$db_source.MHS001MPI AS B                     
                LEFT JOIN $db_output.CCG A ON A.PERSON_ID = b.PERSON_ID
				where (b.RecordEndDate is null or b.RecordEndDate >= '$rp_enddate') and b.RecordStartDate <= '$rp_enddate' AND b.RecordStartDate >= ADD_MONTHS('$rp_enddate',-12)

-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS401MHA_Latest
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
				

-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS403_CD_Latest
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
				

-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS404CTO_Latest
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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS404CTO_Latest_Ranked
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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS405CTO_Recall_Latest
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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_Revoked_CTO

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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS501HospSpell_Latest

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

-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS501_Ranked
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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_HOSP_ADM
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_HOSP_ADM AS

Select distinct HOSP_ADM_RANK, (HOSP_ADM_RANK - 1) AS PREV_HOSP_ADM_RANK
FROM global_temp.MHA_MHS501_Ranked


-- COMMAND ----------

-- DBTITLE 1,Create MHA_MHS501HospSpell_Latest_Ranked
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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_KP90

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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_KP90a

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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_HOSP_RANK
/*
A table is created to link the curernt MHA episode to any exisiting previous one. This link is done using PersonID and Hospital Spell Number. 
The current MHA episode is selected using = @RANK and the join uses = @RANK -1.
*/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_HOSP_RANK AS 

SELECT DISTINCT HOSP_RANK, (HOSP_RANK - 1) AS PREV_HOSP_RANK
FROM global_temp.MHA_KP90a


-- COMMAND ----------

-- DBTITLE 1,Create MHA_KP90_1

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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_KP90_2

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
    ,B.NAME
    ,B.ORG_TYPE_CODE
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
  left join global_temp.MHA_RD_ORG_DAILY_LATEST b on a.orgidProv = b.ORG_CODE 
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
  ,B.ORG_TYPE_CODE 
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


-- COMMAND ----------

-- DBTITLE 1,Create MHA_Final

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
              and (StartTimeHospProvSpell < StartTimeMHActLegalStatusClass) or StartTimeHospProvSpell is null or StartTimeMHActLegalStatusClass is null) or StartDateMHActLegalStatusClass > StartDateHospProvSpell)
              and (PrevLegalStatus is null or PrevMHAEndDate < StartDateMHActLegalStatusClass 
              and (MHS404UniqID is null or (EndDateCommTreatOrd is not null 
              and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))) Or (PrevLegalStatus in ('04','05','06') 
              and (PrevMHAEndDate = StartDateMHActLegalStatusClass or PrevMHAEndDate = DATE_ADD(StartDateMHActLegalStatusClass,-1)))) 
              and (MHS404UniqID is null or (EndDateCommTreatOrd is not null 
              and EndDateCommTreatOrd < DATE_ADD(StartDateMHActLegalStatusClass,-1))) 
        THEN 'B'
        WHEN LegalStatusCode in ('02','03') 
              and ((StartDateMHActLegalStatusClass = PrevMHAEndDate) or (StartDateMHActLegalStatusClass = DATE_ADD(PrevMHAEndDate,1) or (StartDateMHActLegalStatusClass = PrevMHAStartDate)))
              and PrevLegalStatus in ('19','20') 
              and ((StartDateHospProvSpell = StartDateMHActLegalStatusClass) or (StartDateHospProvSpell >= PrevMHAStartDate) or (StartDateMHActLegalStatusClass = PrevMHAEndDate)) 
        THEN 'C'
        WHEN LegalStatusCode in ('03','09','10','15','16') 
              and ((StartDateMHActLegalStatusClass >= DATE_ADD(EndDateCommTreatOrd,-1) or EndDateCommTreatOrd is null) 
              and CommTreatOrdEndReason = '02') 
              and (StartDateHospProvSpell = StartDateMHActLegalStatusClass or StartDateHospProvSpell >= StartDateCommTreatOrd)
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
    ,d.EthnicCategory
    ,d.Gender
    ,D.IC_REC_CCG
     ,D.NAME as CCG_NAME
    
from global_temp.MHA_KP90_2 a 
  left join global_temp.MHA_MHS001MPI_LATEST d on a.Person_ID = d.Person_ID and a.orgidProv = d.orgidProv

ORDER BY 
   a.Person_ID
  ,StartDateMHActLegalStatusClass
  ,StartDateHospProvSpell
  ,HOSP_RANK

-- COMMAND ----------

-- DBTITLE 1,Create MHA_CTO_Final

-- CTO TABLE 

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHA_CTO_Final AS 

SELECT 
    A.Person_ID,
    A.UniqMHActEpisodeID as CTO_UniqMHActEpisodeID,
    A.MHS404UniqID,
    A.orgidProv
    ,CASE
        WHEN A.orgidProv = 'RRD' THEN 'NORTH ESSEX PARTNERSHIP UNIVERSITY NHS FOUNDATION TRUST'
        WHEN A.orgidProv = 'TAE' THEN 'MANCHESTER MENTAL HEALTH AND SOCIAL CARE TRUST'
        WHEN a.orgidProv = 'RJX' THEN 'CALDERSTONES PARTNERSHIP NHS FOUNDATION TRUST'
        ELSE c.NAME END as NAME
    ,CASE
        WHEN A.orgidProv = 'RRD' THEN 'NHS Trust'
        WHEN A.orgidProv = 'TAE' THEN 'NHS Trust'
        WHEN A.orgidProv = 'RJX' THEN 'NHS Trust'
        ELSE c.ORG_TYPE_CODE END as ORG_TYPE_CODE,
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
    ,d.EthnicCategory
    ,d.Gender
    ,D.IC_REC_CCG
    ,D.NAME as CCG_NAME
    
FROM global_temp.MHA_MHS404CTO_Latest_Ranked A
  left join global_temp.MHA_Final B ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID
  LEFT JOIN global_temp.MHA_RD_ORG_DAILY_LATEST C ON A.orgidProv = C.ORG_CODE
  left join global_temp.MHA_MHS001MPI_LATEST d on a.Person_ID = d.Person_ID AND A.ORGIDPROV = D.ORGIDPROV

GROUP BY
    A.Person_ID,
    A.UniqMHActEpisodeID,
    A.MHS404UniqID,
    A.orgidProv
    ,CASE
        WHEN A.orgidProv = 'RRD' THEN 'NORTH ESSEX PARTNERSHIP UNIVERSITY NHS FOUNDATION TRUST'
        WHEN A.orgidProv = 'TAE' THEN 'MANCHESTER MENTAL HEALTH AND SOCIAL CARE TRUST'
        WHEN a.orgidProv = 'RJX' THEN 'CALDERSTONES PARTNERSHIP NHS FOUNDATION TRUST'
        ELSE c.NAME END
    ,CASE
        WHEN A.orgidProv = 'RRD' THEN 'NHS Trust'
        WHEN A.orgidProv = 'TAE' THEN 'NHS Trust'
        WHEN A.orgidProv = 'RJX' THEN 'NHS Trust'
        ELSE c.ORG_TYPE_CODE END,
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
    ,d.EthnicCategory
    ,d.Gender
    ,D.IC_REC_CCG
    ,D.NAME

-- COMMAND ----------


TRUNCATE TABLE $db_output.DETENTIONS_MONTHLY

-- COMMAND ----------


INSERT INTO $db_output.DETENTIONS_MONTHLY

SELECT A.*
FROM  global_temp.MHA_Final A
INNER JOIN (SELECT UniqMHActEpisodeID, MIN(StartDateHospProvSpell) AS HOSP_START
			FROM global_temp.MHA_Final
			GROUP BY UniqMHActEpisodeID) AS B 
        ON A.UniqMHActEpisodeID = B.UniqMHActEpisodeID AND A.StartDateHospProvSpell = B.HOSP_START

WHERE StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'


-- COMMAND ----------

TRUNCATE TABLE $db_output.SHORT_TERM_ORDERS_MONTHLY


-- COMMAND ----------


INSERT INTO $db_output.Short_term_orders_MONTHLY

SELECT *
FROM global_temp.MHA_Final
WHERE StartDateMHActLegalStatusClass between '$rp_startdate' and '$rp_enddate'

-- COMMAND ----------

TRUNCATE TABLE $db_output.CTO_MONTHLY

-- COMMAND ----------


INSERT INTO $db_output.CTO_MONTHLY

SELECT *
FROM global_temp.MHA_CTO_Final a
where StartDateCommTreatOrd between '$rp_startdate' and '$rp_enddate'
