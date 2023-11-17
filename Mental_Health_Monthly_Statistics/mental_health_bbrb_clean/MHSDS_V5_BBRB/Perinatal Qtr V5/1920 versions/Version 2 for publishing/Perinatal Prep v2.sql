-- Databricks notebook source
-- CREATE WIDGET TEXT MONTH_ID DEFAULT "1449";
-- CREATE WIDGET TEXT MSDS_15 DEFAULT "$mat15_database";
-- CREATE WIDGET TEXT MSDS_2 DEFAULT "mat_pre_clear";
-- CREATE WIDGET TEXT MHSDS DEFAULT "$mhsds_database";
-- CREATE WIDGET TEXT RP_STARTDATE DEFAULT "2020-01-01";
-- CREATE WIDGET TEXT RP_ENDDATE DEFAULT "2020-12-31";
-- CREATE WIDGET TEXT personal_db DEFAULT "$user_id";
-- CREATE WIDGET TEXT prev_months DEFAULT "12";

-- COMMAND ----------

%md

##MSDS V1.5 Cohort

-- COMMAND ----------

/** RECONCILE ALL BOOKING APPT RECORDS UP TO AND INCLUDING SEPT 16 LOAD**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT101Booking_LATEST_DF_15 AS 


SELECT        BAPT.AgeAtBookingMother
                     ,BAPT.AntenatalAppDate
                     ,BAPT.EDDAgreed
                     ,BAPT.Person_ID_Mother
                     ,BAPT.UniqPregID
                     ,BAPT.Month_ID
                     ,BAPT.RecordNumber

FROM          $MSDS_15.MAT101Booking
                           AS BAPT
                     INNER JOIN    (
                                         SELECT        BAPT.UniqPregID
                                                              ,MAX (BAPT.Month_ID)
                                                                     AS Month_ID

                                         FROM          $MSDS_15.MAT101Booking
                                                                    AS BAPT

                                         WHERE         BAPT.IC_USE_BOOKING_APP_FLAG = 'Y'
                                                       AND BAPT.Month_ID <= '$MONTH_ID'

                                         GROUP BY      BAPT.UniqPregID
                                         )
                           AS A
                           ON BAPT.UniqPregID = A.UniqPregID AND BAPT.Month_ID = A.Month_ID

WHERE         BAPT.IC_USE_BOOKING_APP_FLAG = 'Y'


-- COMMAND ----------

/**WHEN A UNIQUEPREGID HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE EARLIEST ANTENATALAPPDATE**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT101Booking_FIRSTAPP_DF_15 AS 

SELECT        BAPT.AgeAtBookingMother
                     ,BAPT.AntenatalAppDate
                     ,BAPT.EDDAgreed
                     ,BAPT.Person_ID_Mother
                     ,BAPT.UniqPregID
                     ,BAPT.Month_ID
                     ,BAPT.RecordNumber

FROM          $MSDS_15.MAT101Booking
                           AS BAPT
                     INNER JOIN    (
                                         SELECT        BAPT.UniqPregID
                                                              ,BAPT.Month_ID
                                                              ,MIN (BAPT.AntenatalAppDate)
                                                                     AS AntenatalAppDate

                                         FROM          global_temp.MAT101Booking_LATEST_DF_15
                                                                     AS BAPT

                                         GROUP BY      BAPT.UniqPregID
                                                              ,BAPT.Month_ID
                                         )
                           AS A
                           ON BAPT.UniqPregID = A.UniqPregID AND BAPT.Month_ID = A.Month_ID AND BAPT.AntenatalAppDate = A.AntenatalAppDate

WHERE         BAPT.IC_USE_BOOKING_APP_FLAG = 'Y'

-- COMMAND ----------

/**WHEN A UNIQUEPREGID, ANTENATALAPPDATE COMBINATION HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE MAX RECORDNUMBER AS A TIEBREAKER**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT101Booking_UNIQUE_DF_15 AS

SELECT        BAPT.AgeAtBookingMother
                     ,BAPT.AntenatalAppDate
                     ,BAPT.EDDAgreed
                     ,BAPT.Person_ID_Mother
                     ,BAPT.UniqPregID
                     ,BAPT.Month_ID
                     ,BAPT.RecordNumber

FROM          $MSDS_15.MAT101Booking
                           AS BAPT
                     INNER JOIN    (
                                         SELECT        BAPT.UniqPregID
                                                              ,BAPT.Month_ID
                                                              ,BAPT.AntenatalAppDate
                                                              ,MAX (BAPT.RecordNumber)
                                                                     AS RecordNumber

                                         FROM          global_temp.MAT101Booking_FIRSTAPP_DF_15
                                                                     AS BAPT

                                         GROUP BY      BAPT.UniqPregID
                                                              ,BAPT.Month_ID
                                                              ,BAPT.AntenatalAppDate
                                         )
                           AS A
                           ON BAPT.UniqPregID = A.UniqPregID AND BAPT.Month_ID = A.Month_ID AND BAPT.AntenatalAppDate = A.AntenatalAppDate AND BAPT.RecordNumber = A.RecordNumber

WHERE         BAPT.IC_USE_BOOKING_APP_FLAG = 'Y'

-- COMMAND ----------

/**SELECT FETUS OUTCOMES FOR THE SAME BOOKING APPT RECORDNUMBER USED**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT501FetusOutcome_LATEST_DF_15 AS

SELECT        FOUT.FetusOutcome
                     ,FOUT.FetusOutcomeDate
                     ,FOUT.FetusOutcomeOrder
                     ,FOUT.Person_ID_Mother
                     ,FOUT.UniqPregID

FROM          $MSDS_15.MAT501FetusOutcome
                           AS FOUT
                     INNER JOIN global_temp.MAT101Booking_UNIQUE_DF_15
                           AS BAPT
                           ON BAPT.RecordNumber = FOUT.RecordNumber


-- COMMAND ----------

/**SELECT LOWEST VALUE FETUS OUTCOME FROM ALL BIRTHS (TO ACCOUNT FOR MULTIPLE BIRTHS, SELECTING LIVE BIRTH IF THERE IS ONE)**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT501FetusOutcome_UNIQUE_DF_15 AS

SELECT        FOUT.UniqPregID
                     ,FOUT.FetusOutcome
                     ,MAX (FOUT.FetusOutcomeDate)
                           AS FetusOutcomeDate

FROM          global_temp.MAT501FetusOutcome_LATEST_DF_15
                           AS FOUT
                     INNER JOIN    (
                                         SELECT        FOUT.UniqPregID
                                                              ,MIN (FOUT.FetusOutcome)
                                                                     AS FetusOutcome

                                         FROM          global_temp.MAT501FetusOutcome_LATEST_DF_15
                                                                     AS FOUT

                                         GROUP BY      FOUT.UniqPregID
                                         )
                           AS A
                           ON FOUT.UniqPregID = A.UniqPregID AND FOUT.FetusOutcome = A.FetusOutcome

GROUP BY      FOUT.UniqPregID
                     ,FOUT.FetusOutcome


-- COMMAND ----------

/**NEW 2017/18 SELECT MOTHERS DEMOGRAPHICS for ethnicity**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_LATEST_DF_15 AS


SELECT        MDEM.Person_ID_Mother
                     ,MDEM.EthnicCategoryMother
                     ,MDEM.UniqPregID
                     ,MDEM.Month_ID
                     ,MDEM.MAT001_ID

FROM          $MSDS_15.MAT001MotherDemog
                           AS MDEM


                           --AS BAPT
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MAX (MDEM.Month_ID)
                                                                     AS Month_ID

                                         FROM          $MSDS_15.MAT001MotherDemog
                                                                     AS MDEM

                                         WHERE         MDEM.Month_ID <= '$MONTH_ID'

                                         GROUP BY      MDEM.Person_ID_Mother, MDEM.UniqPregID
                                         )
                           AS A
                           ON MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.Month_ID = A.Month_ID and A.UniqPregID = MDEM.UniqPregID

-- COMMAND ----------

/**WHEN A MSDS_ID_Mother HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE MAX RECORDNUMBER AS A TIEBREAKER**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_UNIQUE_DF_15 AS

SELECT        
                     MDEM.Person_ID_Mother
                     ,MDEM.UniqPregID
                     ,MDEM.Month_ID
                     ,MDEM.MAT001_ID
                     ,MDEM.EthnicCategoryMother

FROM          $MSDS_15.MAT001MotherDemog
                           AS MDEM
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MDEM.Month_ID
                                                              ,MAX (MDEM.MAT001_ID)
                                                                     AS MAT001_ID

                                         FROM          global_temp.MAT001MotherDemog_LATEST_DF_15

                                                                     AS MDEM

                                         GROUP BY      MDEM.Person_ID_Mother
                                                       ,MDEM.Month_ID
                                                       ,MDEM.UniqPregID
                                                              
                                         )
                           AS A
                           ON MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.Month_ID = A.Month_ID 
                              AND MDEM.MAT001_ID = A.MAT001_ID and  MDEM.UniqPregID =  A.UniqPregID


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_UNIQUE_RANKED_DF_15 AS
select Person_ID_Mother
       ,UniqPregID
       ,Month_ID
       ,MAT001_ID
       ,EthnicCategoryMother
       ,ROW_NUMBER() OVER (PARTITION BY Person_ID_Mother ORDER BY Month_ID DESC,MAT001_ID DESC ) as rnk

from global_temp.MAT001MotherDemog_UNIQUE_DF_15

-- COMMAND ----------

/* Get MAT003 GP data at max month*/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW max_month_GP_latest_DF_15 AS

SELECT 
GP.Person_ID_Mother, 
GP.UniqPregID, 
GP.Month_ID, 
GP.EndDateGMPRegistration,
GP.OrgCodeGMPMother, 
GP.OrgCodeCCG_GPP,
GP.RecordNumber,
GP.MAT003_ID
FROM 
$MSDS_15.MAT003GP as GP
inner join (select 
             Person_ID_Mother,
             UniqPregID,
             MAX(Month_ID) AS Month_ID
             FROM $MSDS_15.MAT003GP
             where Month_ID <= '$MONTH_ID'

             group by Person_ID_Mother, UniqPregID) 
            AS MGP
            on GP.UniqPregID = MGP.UniqPregID and GP.Month_ID = MGP.Month_ID
where 
OrgCodeGMPMother NOT IN ('V81999','V81998','V81997')
--and OrgCodeCCG_GPP <> '-1' 
and EndDateGMPRegistration is null
     


-- COMMAND ----------

/* Get data at mat003 ID */

CREATE OR REPLACE GLOBAL TEMPORARY VIEW max_RN_GP_latest_DF_15 AS 

select distinct 
GP.Person_ID_Mother, 
GP.UniqPregID, 
GP.Month_ID,
GP.EndDateGMPRegistration,
GP.OrgCodeCCG_GPP,
GP.RecordNumber, 
GP.MAT003_ID

from $MSDS_15.MAT003GP as GP
inner join 
(                    select Person_ID_Mother,
                     UniqPregID,
                     Month_ID,
                     max (MAT003_ID) AS MAT003_ID
                     from global_temp.max_month_GP_latest_DF_15
                     group by 
                     Person_ID_Mother,
                     UniqPregID, 
                     Month_ID) as MGP

on GP.UniqPregID = MGP.UniqPregID and GP.Month_ID = MGP.Month_ID and GP.MAT003_ID = MGP.MAT003_ID

-- COMMAND ----------

/* Get data at max MAT003_ID  */

CREATE OR REPLACE GLOBAL TEMPORARY VIEW GP_Prac_unique_DF_15 AS

select distinct 
GP.Person_ID_Mother, 
GP.UniqPregID, 
GP.Month_ID, 
GP.EndDateGMPRegistration, 
GP.OrgCodeCCG_GPP,
GP.RecordNumber


from $MSDS_15.MAT003GP as GP
inner join 
                     (select distinct
                     Person_ID_Mother,
                     UniqPregID,
                     Month_ID,
                     RecordNumber,
                     max (MAT003_ID) AS MAT003_ID
                     FROM         global_temp.max_RN_GP_latest_DF_15
                     where Month_ID <= '$MONTH_ID'
                     and OrgCodeCCG_GPP <> '-1' 
                     and EndDateGMPRegistration is null
                     group by 
                     Person_ID_Mother, 
                     UniqPregID,
                     Month_ID,
                     RecordNumber
                     ) as MGP

on GP.UniqPregID = MGP.UniqPregID and GP.Month_ID = MGP.Month_ID 
and GP.RecordNumber = MGP.RecordNumber and GP.MAT003_ID = MGP.MAT003_ID

where OrgCodeGMPMother NOT IN ('V81999','V81998','V81997')
--and OrgCodeCCG_GPP <> '-1' 
and EndDateGMPRegistration is null



-- COMMAND ----------

-- unique maternity demographics

/**NEW 2017/18 SELECT MOTHERS DEMOGRAPHICS for CCG**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MD_CCG_LATEST_DF_15 AS

SELECT        MDEM.Person_ID_Mother        
                     ,MDEM.UniqPregID
                     ,MDEM.Month_ID
                     ,MDEM.RecordNumber
                     ,MDEM.MAT001_ID

FROM         $MSDS_15.MAT001MotherDemog
                           AS MDEM

                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MAX (MDEM.Month_ID)
                                                                     AS Month_ID

                                         FROM          $MSDS_15.MAT001MotherDemog
                                                                     AS MDEM

                                         WHERE         MDEM.Month_ID <= '$MONTH_ID'

                                         GROUP BY      MDEM.UniqPregID, MDEM.Person_ID_Mother
                                         )
                           AS A
                           ON MDEM.UniqPregID = A.UniqPregID and MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.Month_ID = A.Month_ID



-- COMMAND ----------

--G Select the demographics with max record number

/**WHEN A MSDS_ID_Mother HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE MAX RECORDNUMBER AS A TIEBREAKER**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MD_CCG_UNIQUE_DF_15 AS 
(
SELECT        
                     MDEM.Person_ID_Mother
                     ,MDEM.UniqPregID
                     ,MDEM.Month_ID
                     ,MDEM.RecordNumber
                     ,MDEM.OrgCodeCCGRes
                     ,MDEM.MAT001_ID


FROM          $MSDS_15.MAT001MotherDemog
                           AS MDEM
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MDEM.Month_ID
                                                              ,MAX (MDEM.MAT001_ID)
                                                                     AS MAT001_ID

                                         FROM          global_temp.MD_CCG_LATEST_DF_15

                                                                     AS MDEM

                                         GROUP BY      MDEM.UniqPregID,
                                                       MDEM.Person_ID_Mother,
                                                       MDEM.Month_ID
                                                              
                                         )
                           AS A
                           ON  MDEM.UniqPregID = A.UniqPregID  and MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.Month_ID = A.Month_ID 
                            AND MDEM.MAT001_ID = A.MAT001_ID

)

-- COMMAND ----------

--Gets the latest GP practice
-- This query gets the latest CCG codes and names from a reference table so shouldn't need changing

CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_DF AS
(
SELECT               ORG_CODE
                           ,NAME
						   ,ROW_NUMBER() OVER (PARTITION BY ORG_CODE 
												ORDER BY 
												CASE WHEN BUSINESS_END_DATE is null then '2040-01-01' ELSE BUSINESS_END_DATE END DESC,
												CASE WHEN ORG_CLOSE_DATE is null then '2040-01-01' ELSE ORG_CLOSE_DATE END DESC) as rnk

FROM                 $reference_data.ORG_DAILY -- this is a reference table of all the ccg names

WHERE                /*(BUSINESS_END_DATE >= @RP_ENDDATE OR BUSINESS_END_DATE IS NULL)
                     AND*/ BUSINESS_START_DATE <= '$RP_ENDDATE'
                     AND ORG_TYPE_CODE = 'CC'
                     --AND (ORG_CLOSE_DATE >= @RP_ENDDATE OR ORG_CLOSE_DATE IS NULL)
                     AND ORG_OPEN_DATE <= '$RP_ENDDATE'
                     AND NAME NOT LIKE '%HUB'
                     AND NAME NOT LIKE '%NATIONAL%')

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_LATEST_DF AS

SELECT 
ORG_CODE,
NAME

FROM global_temp.RD_CCG_DF
WHERE RNK = 1

-- COMMAND ----------

-- assigns OrgcodeCCG_GP and when this is NULL uses OrgCodeCCGRes

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MD_UNIQUE_CCG_DF_15 AS

                     select 
                     a.Person_ID_Mother,
                     a.UniqPregID,
                     a.Month_ID,
                     a.RecordNumber
                     ,case when c.OrgCodeCCG_GPP IS not null 
                           then c.OrgCodeCCG_GPP 
                     else a.OrgCodeCCGRes 
                           end as IC_Rec_CCG


                     from global_temp.MD_CCG_UNIQUE_DF_15 as a
                     left join global_temp.GP_Prac_unique_DF_15 c on a.Person_ID_Mother = c.Person_ID_Mother 
                      and a.UniqPregID = c.UniqPregID
                     and a.RecordNumber = c.RecordNumber
                     where a.Month_ID <= '$MONTH_ID'


-- COMMAND ----------

-- create final CCG table

CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_Final_DF_15 AS 

select   
MD.Person_ID_Mother,
MD.UniqPregID, 
RecordNumber,
case 
  when a.ORG_CODE is null then 'UNKNOWN'
  else a.ORG_CODE 
  end as IC_Rec_CCG,
case 
  when NAME IS null then 'UNKNOWN'
  else NAME 
  end as NAME

from  global_temp.MD_UNIQUE_CCG_DF_15  as MD
left join global_temp.RD_CCG_LATEST_DF as a 
       on MD.IC_Rec_CCG = a.ORG_CODE
          

-- COMMAND ----------


CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_Final_RANKED_DF_15 AS
select 
Person_ID_Mother
,UniqPregID
,IC_Rec_CCG
,NAME
,recordnumber
,ROW_NUMBER() OVER (PARTITION BY Person_ID_Mother ORDER BY recordnumber DESC) as rnk

from global_temp.CCG_Final_DF_15

-- COMMAND ----------

/**CREATE PERINATAL PERIOD WITH MSDS IDS**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW PerinatalPeriod_15_DF AS

SELECT        BAPT.Person_ID_Mother
                     ,BAPT.UniqPregID
                     ,BAPT.AntenatalAppDate
                           AS StartDate
                     ,CASE  WHEN FOUT.FetusOutcome IS NULL
                                         THEN ADD_MONTHS (BAPT.EDDAgreed,'$prev_months')
                                  WHEN FOUT.FetusOutcome = '10'
                                         THEN ADD_MONTHS (FOUT.FetusOutcomeDate,'$prev_months')
                                         ELSE FOUT.FetusOutcomeDate
                                  END
                           AS EndDate
                     ,BAPT.AgeAtBookingMother
                     
                     ,CASE  WHEN  MDEM.EthnicCategoryMother IS NULL
                                         THEN 'Unspecified'
                                  WHEN  MDEM.EthnicCategoryMother in ('A','B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K','L','M','N','P','R','S','Z','99')
                                then MDEM.EthnicCategoryMother
                                         ELSE 'Invalid data supplied'
                                  END
                           AS EthnicCategoryMother
                     ,CASE  WHEN  MDEM.EthnicCategoryMother IS NULL
                                         THEN 'Unspecified'
                                when MDEM.EthnicCategoryMother  = 'A'
                                         then 'British'
                                                       when MDEM.EthnicCategoryMother  ='B'
                                         then 'Irish'
                                                       when MDEM.EthnicCategoryMother  ='C'
                                         then 'Any other White background'
                                                       when MDEM.EthnicCategoryMother  = 'D'
                                         then 'White and Black Caribbean'
                                                       when MDEM.EthnicCategoryMother  = 'E' 
                                         then 'White and Black African'
                                                       when MDEM.EthnicCategoryMother  = 'F'
                                         then 'White and Asian'
                                                       when MDEM.EthnicCategoryMother  = 'G' 
                                         then 'Any other mixed background'
                                                       when MDEM.EthnicCategoryMother  = 'H'
                                         then 'Indian'
                                                       when MDEM.EthnicCategoryMother  = 'J' 
                                         then 'Pakistani'
                                                       when MDEM.EthnicCategoryMother  = 'K'
                                         then 'Bangladeshi'
                                                       when MDEM.EthnicCategoryMother  = 'L'
                                         then 'Any other Asian background'
                                                       when MDEM.EthnicCategoryMother = 'M'
                                         then 'Caribbean'
                                                       when MDEM.EthnicCategoryMother  = 'N'
                                         then 'African'
                                                       when MDEM.EthnicCategoryMother  = 'P'
                                         then 'Any other Black background'
                                                       when MDEM.EthnicCategoryMother  = 'R'
                                         then 'Chinese'
                                                       when MDEM.EthnicCategoryMother  = 'S'
                                         then 'Any other ethnic group'
                                                       when MDEM.EthnicCategoryMother  = 'Z'
                                         then 'Not stated'
                                                       when MDEM.EthnicCategoryMother  = '99'
                                         then 'Not known'
                                         ELSE 'Invalid data supplied'
                                  END
                           AS EthnicCategoryMother_DESCRIPTION      
                     
                     --,MDEM.EthnicCategoryMother             
                     ,c.IC_Rec_CCG
                     , c.NAME    
 , MDEM.rnk                                           -- Ethnicity added

FROM          global_temp.MAT101Booking_UNIQUE_DF_15
                           AS BAPT
              LEFT OUTER JOIN global_temp.MAT501FetusOutcome_UNIQUE_DF_15
                           AS FOUT
                           ON BAPT.UniqPregID = FOUT.UniqPregID
              LEFT OUTER JOIN global_temp.MAT001MotherDemog_UNIQUE_RANKED_DF_15
   --#MAT001MotherDemog_UNIQUE                     -- Link to demographics table to get ethnicity
                           AS MDEM
                           on BAPT.Person_ID_Mother = MDEM.Person_ID_Mother and BAPT.UniqPregID = MDEM.UniqPregID
              LEFT OUTER JOIN global_temp.CCG_Final_DF_15 as c
                           on BAPT.Person_ID_Mother = c.Person_ID_Mother and BAPT.UniqPregID = c.UniqPregID

-- COMMAND ----------

%md

##MSDS V2 Cohort

-- COMMAND ----------

/** RECONCILE ALL BOOKING APPT RECORDS UP TO AND INCLUDING SEPT 16 LOAD**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT101Booking_LATEST_DF AS 
SELECT        BAPT.AgeAtBookingMother
                     ,BAPT.AntenatalAppDate
                     ,BAPT.EDDAgreed
                     ,BAPT.Person_ID_mother
                     ,BAPT.UniqPregID
                     ,BAPT.RPStartDate
                     ,BAPT.RecordNumber

FROM          $MSDS_2.MSD101PregnancyBooking
                           AS BAPT
                     INNER JOIN    (
                                         SELECT        BAPT.UniqPregID
                                                              ,MAX (BAPT.RPStartDate)
                                                                     AS Max_RPStart

                                         FROM          $MSDS_2.MSD101PregnancyBooking
                                                                    AS BAPT

                                         WHERE         --BAPT.IC_USE_BOOKING_APP_FLAG = 'Y'
                                                       --AND 
                                                       BAPT.RPStartDate <= '$RP_ENDDATE'

                                         GROUP BY      BAPT.UniqPregID
                                         )
                           AS A
                           ON BAPT.UniqPregID = A.UniqPregID AND BAPT.RPStartDate = A.Max_RPStart



-- COMMAND ----------

/**WHEN A UNIQUEPREGID HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE EARLIEST ANTENATALAPPDATE**/
--IF                   OBJECT_ID ('tempdb..#MAT101Booking_FIRSTAPP') IS NOT NULL
--DROP TABLE    #MAT101Booking_FIRSTAPP

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT101Booking_FIRSTAPP_DF AS

SELECT        BAPT.AgeAtBookingMother
                     ,BAPT.AntenatalAppDate
                     ,BAPT.EDDAgreed
                     ,BAPT.Person_ID_mother
                     ,BAPT.UniqPregID
                     ,BAPT.RPStartDate
                     ,BAPT.RecordNumber

--INTO          #MAT101Booking_FIRSTAPP

FROM          $MSDS_2.MSD101PregnancyBooking
                           AS BAPT
                     INNER JOIN    (
                                         SELECT        BAPT.UniqPregID
                                                              ,BAPT.RPStartDate
                                                              ,MIN (BAPT.AntenatalAppDate)
                                                                     AS AntenatalAppDate

                                         FROM         global_temp.MAT101Booking_LATEST_DF
                                                                     AS BAPT

                                         GROUP BY      BAPT.UniqPregID
                                                              ,BAPT.RPStartDate
                                         )
                           AS A
                           ON BAPT.UniqPregID = A.UniqPregID AND BAPT.RPStartDate = A.RPStartDate AND BAPT.AntenatalAppDate = A.AntenatalAppDate


-- COMMAND ----------

/**WHEN A UNIQUEPREGID, ANTENATALAPPDATE COMBINATION HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE MAX RECORDNUMBER AS A TIEBREAKER**/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT101Booking_UNIQUE_DF AS

SELECT        BAPT.AgeAtBookingMother
                     ,BAPT.AntenatalAppDate
                     ,BAPT.EDDAgreed
                     ,BAPT.Person_ID_Mother
                     ,BAPT.UniqPregID AS UNIQPREGID
                     ,BAPT.RPStartDate
                     ,BAPT.RecordNumber

FROM          $MSDS_2.MSD101PregnancyBooking
                           AS BAPT
                     INNER JOIN    (
                                         SELECT        BAPT.UniqPregID
                                                              ,BAPT.RPStartDate
                                                              ,BAPT.AntenatalAppDate
                                                              ,MAX (BAPT.RecordNumber)
                                                                     AS RecordNumber

                                         FROM          global_temp.MAT101Booking_FIRSTAPP_DF
                                                                     AS BAPT

                                         GROUP BY      BAPT.UniqPregID
                                                              ,BAPT.RPStartDate
                                                              ,BAPT.AntenatalAppDate
                                         )
                           AS A
                           ON BAPT.UniqPregID = A.UniqPregID AND BAPT.RPStartDate = A.RPStartDate AND BAPT.AntenatalAppDate = A.AntenatalAppDate AND BAPT.RecordNumber = A.RecordNumber

-- COMMAND ----------

/**SELECT FETUS OUTCOMES FOR THE SAME BOOKING APPT RECORDNUMBER USED**/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT501FetusOutcome_LATEST_DF AS

SELECT        FOUT.PregOutcome
                     --,FOUT.FetusOutcomeDate -- What to use instead?
                     ,PersonBirthDateBaby as FetusOutcomeDate
                     --,FOUT.FetusOutcomeOrder --Not sure what this is? Looks like an ordering of events in old table
                     ,FOUT.Person_ID_mother
                     ,FOUT.UniqPregID

FROM          $MSDS_2.msd401babydemographics
                           AS FOUT
                     INNER JOIN global_temp.MAT101Booking_UNIQUE_DF
                           AS BAPT
                           ON BAPT.RecordNumber = FOUT.RecordNumber


-- COMMAND ----------

/**SELECT LOWEST VALUE FETUS OUTCOME FROM ALL BIRTHS (TO ACCOUNT FOR MULTIPLE BIRTHS, SELECTING LIVE BIRTH IF THERE IS ONE)**/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT501FetusOutcome_UNIQUE_DF AS

SELECT        FOUT.UniqPregID
                     ,FOUT.PregOutcome
                     ,MAX (FOUT.FetusOutcomeDate)
                           AS FetusOutcomeDate

FROM          global_temp.MAT501FetusOutcome_LATEST_DF
                           AS FOUT
                     INNER JOIN    (
                                         SELECT        FOUT.UniqPregID
                                                              ,MIN (FOUT.PregOutcome)
                                                                     AS PregOutcome

                                         FROM          global_temp.MAT501FetusOutcome_LATEST_DF
                                                                     AS FOUT

                                         GROUP BY      FOUT.UniqPregID
                                         )
                           AS A
                           ON FOUT.UniqPregID = A.UniqPregID AND FOUT.PregOutcome = A.PregOutcome

GROUP BY      FOUT.UniqPregID
                     ,FOUT.PregOutcome


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_LATEST_PREP_DF AS 

SELECT DISTINCT       MDEM.Person_ID_mother
                     ,MDEM.EthnicCategoryMother
                     ,MDEM.RPStartDate
                     ,MDEM.MSD001_ID
                     ,BOOK.UniqPregID
                     ,MDEM.OrgIDResidenceResp
                     ,MDEM.CCGResidenceMother
                     ,MDEM.RecordNumber

FROM          $MSDS_2.msd001motherdemog
                           AS MDEM

INNER JOIN global_temp.MAT101Booking_UNIQUE_DF
                          AS BOOK
                          ON MDEM.Person_ID_Mother = BOOK.Person_ID_Mother and MDEM.RecordNumber = BOOK.RecordNumber

-- COMMAND ----------

/**NEW 2017/18 SELECT MOTHERS DEMOGRAPHICS for ethnicity**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_LATEST_DF AS 

SELECT        MDEM.Person_ID_mother
                     ,MDEM.EthnicCategoryMother
                     ,MDEM.UniqPregID
                     ,MDEM.RPStartDate
                     ,MDEM.MSD001_ID

FROM          global_temp.MAT001MotherDemog_LATEST_PREP_DF
                           AS MDEM


                           --AS BAPT
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MAX (MDEM.RPStartDate)
                                                                     AS RPStartDate

                                         FROM          global_temp.MAT001MotherDemog_LATEST_PREP_DF
                                                                     AS MDEM

                                         WHERE         MDEM.RPStartDate <= '$RP_ENDDATE'

                                         GROUP BY      MDEM.Person_ID_mother, MDEM.UniqPregID
                                         )
                           AS A
                           ON MDEM.Person_ID_mother = A.Person_ID_mother AND MDEM.RPStartDate = A.RPStartDate and a.UniqPregID = mdem.UniqPregID

-- COMMAND ----------

/**WHEN A MSDS_ID_Mother HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE MAX RECORDNUMBER AS A TIEBREAKER**/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_UNIQUE_DF AS 

SELECT        
                     MDEM.Person_ID_Mother
                     ,MDEM.UniqPregID
                     ,MDEM.RPStartDate
                     ,MDEM.MSD001_ID
                     ,MDEM.EthnicCategoryMother

FROM          global_temp.MAT001MotherDemog_LATEST_PREP_DF
                           AS MDEM
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MDEM.RPStartDate
                                                              ,MAX (MDEM.MSD001_ID)
                                                                     AS MSD001_ID

                                         FROM          global_temp.MAT001MotherDemog_LATEST_DF

                                                                     AS MDEM

                                         GROUP BY      MDEM.Person_ID_Mother
                                                       ,MDEM.RPStartDate
                                                       ,MDEM.UniqPregID
                                                              
                                         )
                           AS A
                           ON MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.RPStartDate = A.RPStartDate 
                            AND MDEM.MSD001_ID = A.MSD001_ID and  MDEM.UniqPregID =  a.UniqPregID

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MAT001MotherDemog_UNIQUE_RANKED_DF AS 
select                Person_ID_Mother
                     ,UniqPregID
                     ,RPStartDate
                     ,MSD001_ID
                     ,EthnicCategoryMother
                     ,ROW_NUMBER() OVER (PARTITION BY Person_ID_Mother ORDER BY RPStartDate DESC,MSD001_ID DESC ) as rnk
from global_temp.MAT001MotherDemog_UNIQUE_DF

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW GP_DATA AS

SELECT distinct
GP.Person_ID_Mother, 
BOOK.UniqPregID, 
GP.RPStartDate, 
GP.EndDateGMPReg,
GP.OrgCodeGMPMother, 
GP.CCGResponsibilityMother,
GP.RecordNumber,
GP.MSD002_ID

FROM $MSDS_2.msd002gp 
              AS GP

inner join global_temp.MAT101Booking_UNIQUE_DF 
                        AS BOOK
                        ON GP.PERSON_ID_MOTHER = BOOK.PERSON_ID_MOTHER AND GP.RECORDNUMBER = BOOK.RECORDNUMBER
            

-- COMMAND ----------

-- Create unique GP table for CCG
-- Selecting unique MAT003GP data

--/* Get MAT003 GP data at max month*/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW max_month_GP_latest_DF AS
SELECT 
GP.Person_ID_Mother, 
GP.UniqPregID, 
GP.RPStartDate, 
GP.EndDateGMPReg,
GP.OrgCodeGMPMother, 
GP.CCGResponsibilityMother,
GP.RecordNumber,
GP.MSD002_ID
FROM 
global_temp.GP_DATA AS GP
inner join 
                    (select 
                     Person_ID_Mother,
                     UniqPregID,
                     max(RPStartDate) AS RPStartDate
                     FROM          
                     global_temp.GP_DATA
                     where RPStartDate <= '$RP_ENDDATE'
                     group by 
                     Person_ID_Mother, 
                     UniqPregID
						) AS MGP
                          on GP.UniqPregID = MGP.UniqPregID and GP.RPStartDate = MGP.RPStartDate
where 
	OrgCodeGMPMother NOT IN ('V81999','V81998','V81997')
	--and OrgCodeCCG_GPP <> '-1' -- doesnt exist in the new tables
	and EndDateGMPReg is null

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW max_RN_GP_latest_DF AS
(
select 
distinct 
GP.Person_ID_Mother, 
GP.UniqPregID, 
GP.RPStartDate, 
GP.EndDateGMPReg, 
GP.CCGResponsibilityMother,
GP.RecordNumber, 
GP.MSD002_ID
from  global_temp.GP_DATA as GP
inner join 
(                    select 
                     Person_ID_Mother,
                     UniqPregID,
                     RPStartDate,
                     max (MSD002_ID) AS MSD002_ID
                     from global_temp.max_month_GP_latest_DF
                     group by 
                     Person_ID_Mother,
                     UniqPregID, 
                     RPStartDate) as MGP
on GP.UniqPregID = MGP.UniqPregID and GP.RPStartDate = MGP.RPStartDate and GP.MSD002_ID = MGP.MSD002_ID
)

-- COMMAND ----------

/* Get data at max MAT003_ID  */
CREATE OR REPLACE GLOBAL TEMPORARY VIEW GP_Prac_unique_DF AS
(
select 
distinct 
GP.Person_ID_Mother, 
GP.UniqPregID, 
GP.RPStartDate, 
GP.EndDateGMPReg, 
GP.CCGResponsibilityMother,
GP.RecordNumber
from global_temp.GP_DATA as GP
inner join 
                     (select 
                     distinct
                     Person_ID_Mother,
                     UniqPregID,
                     RPStartDate,
                     RecordNumber,
                     max (MSD002_ID) AS MSD002_ID
                     FROM 
                     global_temp.max_RN_GP_latest_DF
                     where 
                     RPStartDate <= '$RP_ENDDATE'
                     --and OrgCodeCCG_GPP <> '-1' 
                     and EndDateGMPReg is null
                     group by 
                     Person_ID_Mother, 
                     UniqPregID,
                     RPStartDate,
                     RecordNumber
                     ) as MGP

on GP.UniqPregID = MGP.UniqPregID and GP.RPStartDate = MGP.RPStartDate 
and GP.RecordNumber = MGP.RecordNumber and GP.MSD002_ID = MGP.MSD002_ID

where OrgCodeGMPMother NOT IN ('V81999','V81998','V81997')
--and OrgCodeCCG_GPP <> '-1' 
and EndDateGMPReg is null
)

-- COMMAND ----------

/**NEW 2017/18 SELECT MOTHERS DEMOGRAPHICS for CCG**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MD_CCG_LATEST_DF AS
SELECT        MDEM.Person_ID_Mother        
                     ,MDEM.UniqPregID
                     ,MDEM.RPStartDate
                     ,MDEM.RecordNumber
                     ,MDEM.MSD001_ID
FROM          global_temp.MAT001MotherDemog_LATEST_PREP_DF
                           AS MDEM
                           
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MAX (MDEM.RPStartDate)
                                                                     AS RPStartDate

                                         FROM          global_temp.MAT001MotherDemog_LATEST_PREP_DF
                                                                     AS MDEM

                                         WHERE         MDEM.RPStartDate <= '$RP_ENDDATE'

                                         GROUP BY      MDEM.UniqPregID, 
                                                       MDEM.Person_ID_Mother
                                         )
                           AS A
                           ON MDEM.UniqPregID = A.UniqPregID and MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.RPStartDate = A.RPStartDate

-- COMMAND ----------

--G Select the demographics with max record number

/**WHEN A MSDS_ID_Mother HAS MORE THAN ONE RECORD IN THE MOST RECENT MONTH IT FLOWED, USE THE ONE WITH THE MAX RECORDNUMBER AS A TIEBREAKER**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MD_CCG_UNIQUE_DF AS
SELECT        
                     MDEM.Person_ID_Mother
                     ,MDEM.UniqPregID
                     ,MDEM.RPStartDate
                     ,MDEM.RecordNumber
                     ,MDEM.OrgIDResidenceResp
                     ,MDEM.MSD001_ID
                     ,MDEM.CCGResidenceMother

FROM          global_temp.MAT001MotherDemog_LATEST_PREP_DF
                           AS MDEM
                     INNER JOIN    (
                                         SELECT        MDEM.UniqPregID
                                                              ,MDEM.Person_ID_Mother
                                                              ,MDEM.RPStartDate
                                                              ,MAX (MDEM.MSD001_ID)
                                                                     AS MSD001_ID

                                         FROM          global_temp.MD_CCG_LATEST_DF

                                                                     AS MDEM

                                         GROUP BY      MDEM.UniqPregID,
                                                       MDEM.Person_ID_Mother,
                                                       MDEM.RPStartDate
                                                              
                                         )
                           AS A
                           ON  MDEM.UniqPregID = A.UniqPregID  and MDEM.Person_ID_Mother = A.Person_ID_Mother AND MDEM.RPStartDate = A.RPStartDate 
                            AND MDEM.MSD001_ID = A.MSD001_ID

-- COMMAND ----------

--Gets the latest GP practice
-- This query gets the latest CCG codes and names from a reference table so shouldn't need changing


CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_DF AS
SELECT               ORG_CODE
                           ,NAME
						   ,ROW_NUMBER() OVER (PARTITION BY ORG_CODE 
												ORDER BY 
												CASE WHEN BUSINESS_END_DATE is null then '2040-01-01' ELSE BUSINESS_END_DATE END DESC,
												CASE WHEN ORG_CLOSE_DATE is null then '2040-01-01' ELSE ORG_CLOSE_DATE END DESC) as rnk

FROM                 $reference_data.org_daily -- this is a reference table of all the ccg names

WHERE                /*(BUSINESS_END_DATE >= @RP_ENDDATE OR BUSINESS_END_DATE IS NULL)
                     AND*/ BUSINESS_START_DATE <= '$RP_ENDDATE'   
                     AND ORG_TYPE_CODE = 'CC'
                     --AND (ORG_CLOSE_DATE >= @RP_ENDDATE OR ORG_CLOSE_DATE IS NULL)
                     AND ORG_OPEN_DATE <= '$RP_ENDDATE'  
                     AND NAME NOT LIKE '%HUB'
                     AND NAME NOT LIKE '%NATIONAL%'

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW RD_CCG_LATEST_DF AS
SELECT 
ORG_CODE,
NAME
FROM global_temp.RD_CCG_DF
WHERE RNK = 1

-- COMMAND ----------

-- assigns OrgcodeCCG_GP and when this is NULL uses OrgCodeCCGRes

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MD_UNIQUE_CCG_DF AS

                     select 
                     a.Person_ID_Mother,
                     a.UniqPregID,
                     a.RPStartDate,
                     a.RecordNumber
                     ,case 
                       when c.CCGResponsibilityMother IS not null 
                       then c.CCGResponsibilityMother 
                       else a.CCGResidenceMother 
                       end as IC_Rec_CCG
                     from global_temp.MD_CCG_UNIQUE_DF as a
                     left join global_temp.GP_Prac_unique_DF c on a.Person_ID_Mother = c.Person_ID_Mother 
                      and a.UniqPregID = c.UniqPregID
                     and a.RecordNumber = c.RecordNumber
                     where a.RPStartDate <= '$RP_ENDDATE'

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_Final_DF AS
select   
MD.Person_ID_Mother,
MD.UniqPregID, 
RecordNumber,
case 
  when a.ORG_CODE is null then 'UNKNOWN'
  else a.ORG_CODE 
  end as IC_Rec_CCG,
case when NAME IS null then 'UNKNOWN'
  else NAME 
  end as NAME
       
FROM  global_temp.MD_UNIQUE_CCG_DF  as MD
LEFT JOIN global_temp.RD_CCG_LATEST_DF as a 
       on MD.IC_Rec_CCG = a.ORG_CODE
          

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_Final_RANKED_DF AS 
select 
Person_ID_Mother
,UniqPregID
,IC_Rec_CCG
,NAME
,recordnumber
,ROW_NUMBER() OVER (PARTITION BY Person_ID_Mother ORDER BY recordnumber DESC) as rnk
from global_temp.CCG_Final_DF

-- COMMAND ----------

/**CREATE PERINATAL PERIOD WITH MSDS IDS**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW PerinatalPeriod_2_DF AS 

SELECT        
BAPT.Person_ID_Mother
,BAPT.UniqPregID
,BAPT.AntenatalAppDate AS StartDate
,CASE  
  WHEN FOUT.PregOutcome IS NULL THEN add_months(BAPT.EDDAgreed, '$prev_months')
  WHEN FOUT.PregOutcome = '01' THEN add_months(FOUT.FetusOutcomeDate, '$prev_months')
  WHEN FOUT.FetusOutcomeDate IS NULL THEN add_months(BAPT.EDDAgreed, '$prev_months')
  ELSE FOUT.FetusOutcomeDate
  END AS EndDate
,BAPT.AgeAtBookingMother
,CASE  
  WHEN  MDEM.EthnicCategoryMother IS NULL THEN 'Unspecified'
  WHEN  MDEM.EthnicCategoryMother in ('A','B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K','L','M','N','P','R','S','Z','99') then MDEM.EthnicCategoryMother
  ELSE 'Invalid data supplied'
  END AS EthnicCategoryMother
,CASE  
  WHEN MDEM.EthnicCategoryMother IS NULL THEN 'Unspecified'
  when MDEM.EthnicCategoryMother  = 'A' then 'British'
  when MDEM.EthnicCategoryMother  = 'B' then 'Irish'
  when MDEM.EthnicCategoryMother  = 'C' then 'Any other White background'
  when MDEM.EthnicCategoryMother  = 'D' then 'White and Black Caribbean'
  when MDEM.EthnicCategoryMother  = 'E' then 'White and Black African'
  when MDEM.EthnicCategoryMother  = 'F' then 'White and Asian'
  when MDEM.EthnicCategoryMother  = 'G' then 'Any other mixed background'
  when MDEM.EthnicCategoryMother  = 'H' then 'Indian'
  when MDEM.EthnicCategoryMother  = 'J' then 'Pakistani'
  when MDEM.EthnicCategoryMother  = 'K' then 'Bangladeshi'
  when MDEM.EthnicCategoryMother  = 'L' then 'Any other Asian background'
  when MDEM.EthnicCategoryMother  = 'M' then 'Caribbean'
  when MDEM.EthnicCategoryMother  = 'N' then 'African'
  when MDEM.EthnicCategoryMother  = 'P' then 'Any other Black background'
  when MDEM.EthnicCategoryMother  = 'R' then 'Chinese'
  when MDEM.EthnicCategoryMother  = 'S' then 'Any other ethnic group'
  when MDEM.EthnicCategoryMother  = 'Z' then 'Not stated'
  when MDEM.EthnicCategoryMother  = '99' then 'Not known'
  ELSE 'Invalid data supplied'
  END AS EthnicCategoryMother_DESCRIPTION      
,c.IC_Rec_CCG
,c.NAME    
,MDEM.rnk                                           -- Ethnicity added

FROM          global_temp.MAT101Booking_UNIQUE_DF
                           AS BAPT
LEFT OUTER JOIN global_temp.MAT501FetusOutcome_UNIQUE_DF
                           AS FOUT
                           ON BAPT.UniqPregID = FOUT.UniqPregID
LEFT OUTER JOIN  global_temp.MAT001MotherDemog_UNIQUE_RANKED_DF
                           AS MDEM
                           on BAPT.Person_ID_Mother = MDEM.Person_ID_Mother and BAPT.UniqPregID = MDEM.UniqPregID
LEFT OUTER JOIN global_temp.CCG_Final_DF as c
                           on BAPT.Person_ID_Mother = c.Person_ID_Mother
                           and BAPT.UniqPregID = c.UniqPregID


-- COMMAND ----------

%md
## Join v1.5 and v2

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PerinatalPeriod_DF_comb AS

SELECT 
DISTINCT 
'2' as Version,
*
FROM global_temp.PerinatalPeriod_2_DF

UNION  

SELECT 
DISTINCT
'1' as Version,
* 
FROM global_temp.PerinatalPeriod_15_DF

-- COMMAND ----------

TRUNCATE TABLE $personal_db.perinatalperiod_df

-- COMMAND ----------

INSERT INTO $personal_db.PerinatalPeriod_DF

SELECT 
Version, 
Person_ID_Mother, 
UniqPregID,
StartDate,
EndDate, 
AgeAtBookingMother,
EthnicCategoryMother,
EthnicCategoryMother_Description,
IC_Rec_CCG, 
NAME,
dense_rank() over (partition by Person_ID_Mother order by version DESC, EndDate DESC, StartDate ASC, UniqPregID DESC) AS rnk,
dense_rank() over (partition by UniqPregID order by version DESC, EndDate DESC, StartDate ASC, UniqPregID DESC) AS Preg_rnk  
FROM
global_temp.PerinatalPeriod_DF_comb
WHERE
EndDate is not null

-- COMMAND ----------

%md

##Linkage here - NEEDS INVESTIGATING

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHSDS_Index_DF AS
SELECT
DISTINCT
A.Person_ID
FROM
$MHSDS.MHS001MPI A
INNER JOIN $MHSDS.MHS101REFERRAL B 
                    ON A.PERSON_ID = B.PERSON_ID  
WHERE
((A.RecordEndDate IS NULL OR A.RecordEndDate >= '$RP_ENDDATE') AND A.RecordStartDate <= '$RP_ENDDATE')
AND ((B.RecordEndDate IS NULL OR B.RecordEndDate >= '$RP_ENDDATE') AND B.RecordStartDate <= '$RP_ENDDATE')

-- COMMAND ----------

TRUNCATE TABLE $personal_db.MHSDSPerinatalPeriodMH_DF

-- COMMAND ----------

/**ADD MHSDS ID TO PERINATAL PERIOD**/
INSERT INTO $personal_db.MHSDSPerinatalPeriodMH_DF

SELECT        MH_MAT_LINK.Person_ID
                     ,PERI.Person_ID_Mother
                     ,PERI.UniqPregID
                     ,PERI.StartDate
                     ,PERI.EndDate
                     ,PERI.AgeAtBookingMother
                     ,PERI.EthnicCategoryMother 
                     ,PERI.EthnicCategoryMother_DESCRIPTION  
                     ,PERI.IC_Rec_CCG
                     ,PERI.NAME                               -- ethnicity added
                     ,PERI.rnk
                     ,PERI.preg_rnk

FROM         $personal_db.PerinatalPeriod_DF
                           AS PERI
                     INNER JOIN global_temp.MHSDS_Index_DF
                           AS MH_MAT_LINK
                           ON PERI.Person_ID_Mother = MH_MAT_LINK.Person_ID


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_Rank_DF AS
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY Person_ID_Mother ORDER BY EndDate DESC) as CCG_rnk
FROM 
$personal_db.MHSDSPerinatalPeriodMH_DF

-- COMMAND ----------

%md

##MH Code

-- COMMAND ----------

/**CODE TO SPLIT THE DIFFERENT SERVICE AREAS FOR WARD STAYS**/

--CREATES THE DIFFERENT SERVICE AREAS NON DISTINCT

CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_type_list_DF AS

SELECT DISTINCT 
CASE 
WHEN CAMHSTier IN ('4','9')
THEN 'Y'
WHEN TreatFuncCodeMH = '711'
THEN 'Y'
WHEN WardType IN ('01', '02')
THEN 'Y'
WHEN WardAge IN ('10','11','12')
THEN 'Y'
ELSE NULL
END
AS CAMHS
,CASE 
WHEN IntendClinCareIntenCodeMH IN ('61', '62', '63')
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
WHEN IntendClinCareIntenCodeMH in ('61', '62', '63')
THEN NULL
WHEN CAMHSTier IN ('4','9')
THEN NULL 
WHEN TreatFuncCodeMH IN ('700', '711')
THEN NULL
WHEN WardAge IN ('10', '11', '12')
THEN NULL
WHEN WardAge IN ('13', '14', '15')
THEN 'Y'
WHEN IntendClinCareIntenCodeMH IN ('51', '52', '53')
THEN 'Y'
WHEN TreatFuncCodeMH IN ('710', '712', '713', '715', '720', '721', '722', '723', '724', '725', '726', '727')
THEN 'Y'
WHEN WardType IN ('03', '04', '06')
THEN 'Y'
ELSE 'Y'
END
AS MH
,UniqWardStayID

FROM $MHSDS.MHS001MPI
  AS MPI
  
INNER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF
  AS PERI
  ON MPI.Person_ID = PERI.Person_ID
  
INNER JOIN $MHSDS.MHS502WardStay
  AS WRD
  ON MPI.Person_ID = WRD.Person_ID AND (WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
  AND WRD.StartDateWardStay <= '$RP_ENDDATE' 
  AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE') 
  AND WRD.StartDateWardStay <= PERI.EndDate 
  AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate) 
  AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'
  
LEFT OUTER JOIN $MHSDS.MHS503AssignedCareProf
  AS APF
  ON WRD.UniqHospProvSpellNum = APF.UniqHospProvSpellNum AND (APF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND APF.StartDateAssCareProf <= '$RP_ENDDATE' AND (APF.EndDateAssCareProf IS NULL OR APF.EndDateAssCareProf >= '$RP_STARTDATE') AND APF.StartDateAssCareProf <= PERI.EndDate AND (APF.EndDateAssCareProf IS NULL OR APF.EndDateAssCareProf >= PERI.StartDate) AND (APF.RecordEndDate IS NULL OR APF.RecordEndDate >= '$RP_ENDDATE') AND APF.RecordStartDate <= '$RP_ENDDATE'
  
LEFT OUTER JOIN $MHSDS.MHS101Referral
  AS REF
  ON WRD.UniqServReqID = REF.UniqServReqID AND (REF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE') AND REF.ReferralRequestReceivedDate <= PERI.EndDate AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate) AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'
  
LEFT OUTER JOIN $MHSDS.MHS102ServiceTypeReferredTo
  AS SRV
  ON REF.UniqServReqID = SRV.UniqServReqID AND (SRV.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= '$RP_STARTDATE') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= '$RP_STARTDATE')) AND REF.ReferralRequestReceivedDate <= PERI.EndDate AND  ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= PERI.StartDate) AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= PERI.StartDate)) AND (SRV.RecordEndDate IS NULL OR SRV.RecordEndDate >= '$RP_ENDDATE') AND SRV.RecordStartDate <= '$RP_ENDDATE'

WHERE
(MPI.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID')
AND MPI.PatMRecInRP = True
AND (MPI.RecordEndDate IS NULL OR MPI.RecordEndDate >= '$RP_ENDDATE') AND MPI.RecordStartDate <= '$RP_ENDDATE'

-- COMMAND ----------

--CREATES A DISTINCT VERSION OF THE SERVICE AREA BREAKDOWNS

CREATE OR REPLACE GLOBAL TEMPORARY VIEW ward_stay_cats_DF AS
SELECT DISTINCT UniqWardStayID
,MIN (LD)
AS LD
,MIN (CAMHS)
AS CAMHS
,MIN (MH)
AS MH

FROM global_temp.ward_type_list_DF

GROUP BY UniqWardStayID

-- COMMAND ----------

/**CREATES THE SERVICE AREA BREAKDOWN FOR REFERRALS**/

--CREATES THE DIFFERENT SERVICE AREAS NON-DISTINCT

CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_list_DF AS
SELECT 
CASE 
  WHEN WCT.LD = 'Y'
  THEN 'Y'
  WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04', 'B02', 'C01')
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
  WHEN ServTeamTypeRefToMH IN ('A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'B01', 'C02', 'C03', 'C04', 'C08', 'D01', 'D02', 'D03', 'D04', 'Z01', 'Z02')
  THEN 'Y'
  WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03','E04', 'B02', 'C01', 'C05', 'C06', 'C07','C09')
  THEN NULL
  ELSE 'Y'
  END
  AS MH
,REF.UniqServReqID

FROM $MHSDS.MHS101Referral
  AS REF

INNER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF
  AS PERI
  ON REF.Person_ID = PERI.Person_ID

LEFT OUTER JOIN $MHSDS.MHS001MPI
  AS MPI
  ON MPI.Person_ID = REF.Person_ID AND MPI.PatMRecInRP = True AND (MPI.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (MPI.RecordEndDate IS NULL OR MPI.RecordEndDate >= '$RP_ENDDATE') AND MPI.RecordStartDate <= '$RP_ENDDATE'

LEFT OUTER JOIN $MHSDS.MHS501HospProvSpell
  AS HSP
  ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

LEFT OUTER JOIN $MHSDS.MHS502WardStay
  AS WRD
  ON HSP.UniqHospProvSpellNum = WRD.UniqHospProvSpellNum AND (WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND WRD.StartDateWardStay <= '$RP_ENDDATE' AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE') AND WRD.StartDateWardStay <= PERI.EndDate AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate) AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'

LEFT OUTER JOIN global_temp.ward_stay_cats_DF
  AS WCT
  ON WRD.UniqWardStayID = WCT.UniqWardStayID

LEFT OUTER JOIN $MHSDS.MHS102ServiceTypeReferredTo
  AS SRV
  ON REF.UniqServReqID = SRV.UniqServReqID AND (SRV.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= '$RP_STARTDATE') AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= '$RP_STARTDATE')) AND REF.ReferralRequestReceivedDate <= PERI.EndDate AND ((SRV.ReferClosureDate IS NULL OR SRV.ReferClosureDate >= PERI.StartDate) AND (SRV.ReferRejectionDate IS NULL OR SRV.ReferRejectionDate >= PERI.StartDate)) AND (SRV.RecordEndDate IS NULL OR SRV.RecordEndDate >= '$RP_ENDDATE') AND SRV.RecordStartDate <= '$RP_ENDDATE'

WHERE
(REF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') -- This was previously REF.UniqMont BETWEEN '$MONTH_ID' - 11 AND REF.Month_ID - changed to be consistent as not sure REF.Month_ID is correct.
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'

-- COMMAND ----------

--CREATES A DISTINCT VERSION OF THE SERVICE AREA BREAKDOWNS

CREATE OR REPLACE GLOBAL TEMPORARY VIEW referral_cats_DF AS
SELECT DISTINCT UniqServReqID
,MIN (LD)
AS LD
,MIN (CAMHS)
AS CAMHS
,MIN (MH)
AS MH

FROM global_temp.referral_list_DF

GROUP BY UniqServReqID

-- COMMAND ----------

/**CREATES THE TEMP WARDSTAYS TABLE**/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS502WardStay_service_area_peri_in_rp_DF AS
SELECT WRD.*
,WCT.CAMHS
,WCT.LD
,WCT.MH 

FROM $MHSDS.MHS502WardStay
AS WRD
LEFT OUTER JOIN global_temp.ward_stay_cats_DF
AS WCT
ON WRD.UniqWardStayID = WCT.UniqWardStayID
LEFT OUTER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI
ON PERI.Person_ID = WRD.Person_ID

WHERE
(WRD.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID')
AND WRD.StartDateWardStay <= '$RP_ENDDATE' AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= '$RP_STARTDATE')
AND WRD.StartDateWardStay <= PERI.EndDate AND (WRD.EndDateWardStay IS NULL OR WRD.EndDateWardStay >= PERI.StartDate)
AND (WRD.RecordEndDate IS NULL OR WRD.RecordEndDate >= '$RP_ENDDATE') AND WRD.RecordStartDate <= '$RP_ENDDATE'


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHS101Referral_service_area_peri_in_rp_DF AS 

SELECT REF.*
,RCT.CAMHS
,RCT.LD
,RCT.MH

FROM $MHSDS.MHS101Referral
  AS REF
LEFT OUTER JOIN global_temp.referral_cats_DF
  AS RCT
  ON REF.UniqServReqID = RCT.UniqServReqID
LEFT OUTER JOIN $personal_db.MHSDSPerinatalPeriodMH_DF
  AS PERI
  ON PERI.Person_ID = REF.Person_ID

WHERE
(REF.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') -- This was previously REF.UniqMont BETWEEN '$MONTH_ID' - 11 AND REF.Month_ID - changed to be consistent as not sure REF.Month_ID is correct.
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE' AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') AND REF.RecordStartDate <= '$RP_ENDDATE'


-- COMMAND ----------

%md 
##Measure Prep

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH01b_DF AS

SELECT 
Version,
Person_ID_mother, 
UniqPregID,
StartDate,
EndDate,
AgeAtBookingMother, 
EthnicCategoryMother, 
EthnicCategoryMother_Description,
IC_REC_CCG,
NAME,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk
FROM
$personal_db.PerinatalPeriod_DF
AS PERI

WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND PERI.AgeAtBookingMother >= 16

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH02b_DF AS

SELECT 
PERI.Person_ID_mother, 
PERI.AgeAtBookingMother,
PERI.EthnicCategoryMother,
PERI.EthnicCategoryMother_Description,
PERI.IC_REC_CCG,
PERI.NAME,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk,
REF.OrgIDProv
FROM $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI
LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH06a_DF AS

SELECT 
PERI.Person_ID_Mother,
PERI.AgeAtBookingMother,
PERI.EthnicCategoryMother,
PERI.EthnicCategoryMother_Description,
REF.OrgIDProv,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk,
PERI.IC_REC_CCG,
PERI.NAME

FROM $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI

LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

LEFT OUTER JOIN $MHSDS.MHS501HospProvSpell
AS HSP
ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

LEFT OUTER JOIN $MHSDS.MHS502WardStay
AS WST
ON HSP.UniqHospProvSpellNum = WST.UniqHospProvSpellNum 
AND (WST.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
AND WST.StartDateWardStay <= '$RP_ENDDATE'
AND (WST.EndDateWardStay IS NULL OR WST.EndDateWardStay >= '$RP_STARTDATE') 
AND WST.StartDateWardStay <= PERI.EndDate 
AND (WST.EndDateWardStay  IS NULL OR WST.EndDateWardStay  >= PERI.StartDate) 
AND (WST.RecordEndDate IS NULL OR WST.RecordEndDate >= '$RP_ENDDATE') 
AND WST.RecordStartDate <= '$RP_ENDDATE' 
AND WST.SiteIDOfTreat in
('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
'RV505','RRE3K','RW119')

LEFT OUTER JOIN $MHSDS.MHS201CareContact
AS CC
ON CC.UniqServReqID = REF.UniqServReqID AND (CC.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (CC.CareContDate BETWEEN PERI.StartDate AND PERI.EndDate)

LEFT OUTER JOIN $MHSDS.MHS102ServiceTypeReferredTo
AS SRV
ON CC.UniqCareProfTeamID = SRV.UniqCareProfTeamID AND CC.UniqServReqID = SRV.UniqServReqID AND SRV.UniqMonthID = CC.UniqMonthID AND SRV.ServTeamTypeRefToMH = 'C02'

WHERE
PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16
--AND (APF.UniqHospProvSpellNum IS NOT NULL OR SRV.UniqServReqID IS NOT NULL)
AND (WST.UniqHospProvSpellNum IS NOT NULL OR SRV.UniqServReqID IS NOT NULL)

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH08a_DF AS

SELECT 
PERI.Person_ID_Mother,
PERI.AgeAtBookingMother,
PERI.EthnicCategoryMother,
PERI.EthnicCategoryMother_Description,
REF.OrgIDProv,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk,
PERI.IC_REC_CCG,
PERI.NAME

FROM $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI

LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

INNER JOIN $MHSDS.MHS201CareContact
AS CC
ON CC.UniqServReqID = REF.UniqServReqID AND (CC.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (CC.CareContDate BETWEEN PERI.StartDate AND PERI.EndDate)

INNER JOIN $MHSDS.MHS102ServiceTypeReferredTo
AS SRV
ON CC.UniqCareProfTeamID = SRV.UniqCareProfTeamID AND CC.UniqServReqID = SRV.UniqServReqID AND SRV.UniqMonthID = CC.UniqMonthID AND SRV.ServTeamTypeRefToMH = 'C02'


WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH19a_DF AS

SELECT 
PERI.UniqPregID,
PERI.AgeAtBookingMother,
PERI.EthnicCategoryMother,
PERI.EthnicCategoryMother_Description,
REF.OrgIDProv,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk,  
PERI.IC_Rec_CCG,
PERI.NAME
FROM $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI

LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH20a_DF AS

SELECT 
PERI.UniqPregID,
PERI.AgeAtBookingMother,
PERI.EthnicCategoryMother,
PERI.EthnicCategoryMother_Description,
REF.OrgIDProv,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk, 
PERI.IC_Rec_CCG,
PERI.NAME

FROM $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI

LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

LEFT OUTER JOIN $MHSDS.MHS501HospProvSpell
AS HSP
ON HSP.UniqServReqID = REF.UniqServReqID AND (HSP.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID')AND HSP.StartDateHospProvSpell <= '$RP_ENDDATE' AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= '$RP_STARTDATE') AND HSP.StartDateHospProvSpell <= PERI.EndDate AND (HSP.DischDateHospProvSpell IS NULL OR HSP.DischDateHospProvSpell >= PERI.StartDate) AND (HSP.RecordEndDate IS NULL OR HSP.RecordEndDate >= '$RP_ENDDATE') AND HSP.RecordStartDate <= '$RP_ENDDATE'

LEFT OUTER JOIN $MHSDS.MHS502WardStay
AS WST
ON HSP.UniqHospProvSpellNum = WST.UniqHospProvSpellNum 
AND (WST.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') 
AND WST.StartDateWardStay <= '$RP_ENDDATE'
AND (WST.EndDateWardStay IS NULL OR WST.EndDateWardStay >= '$RP_STARTDATE') 
AND WST.StartDateWardStay <= PERI.EndDate 
AND (WST.EndDateWardStay  IS NULL OR WST.EndDateWardStay  >= PERI.StartDate) 
AND (WST.RecordEndDate IS NULL OR WST.RecordEndDate >= '$RP_ENDDATE') 
AND WST.RecordStartDate <= '$RP_ENDDATE' 
AND WST.SiteIDOfTreat in
('RVNPA','RXTD3','RV312','RXM54','RDYGA','RWK62',
'R1LAH','RXVM8','RWRA9','RGD05','RX4E2','RHARA',
'RV505','RRE3K','RW119')

LEFT OUTER JOIN $MHSDS.MHS201CareContact
AS CC
ON CC.UniqServReqID = REF.UniqServReqID AND (CC.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (CC.CareContDate BETWEEN PERI.StartDate AND PERI.EndDate)

LEFT OUTER JOIN $MHSDS.MHS102ServiceTypeReferredTo
AS SRV
ON CC.UniqCareProfTeamID = SRV.UniqCareProfTeamID AND CC.UniqServReqID = SRV.UniqServReqID AND SRV.UniqMonthID = CC.UniqMonthID AND SRV.ServTeamTypeRefToMH = 'C02'


WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16
AND (WST.UniqHospProvSpellNum IS NOT NULL OR SRV.UniqServReqID IS NOT NULL)


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW PMH22a_DF AS 

SELECT 
PERI.UniqPregID,
PERI.AgeAtBookingMother,
PERI.EthnicCategoryMother,
PERI.EthnicCategoryMother_Description,
REF.OrgIDProv,
dense_rank() over (partition by PERI.Person_ID_Mother order by PERI.rnk ASC) AS rnk,
dense_rank() over (partition by PERI.UniqPregID order by PERI.Preg_rnk ASC) AS Preg_rnk,
PERI.IC_Rec_CCG,
PERI.NAME

FROM $personal_db.MHSDSPerinatalPeriodMH_DF
AS PERI

LEFT OUTER JOIN global_temp.MHS101Referral_service_area_peri_in_rp_DF
AS REF
ON PERI.Person_ID = REF.Person_ID

INNER JOIN $MHSDS.MHS201CareContact
AS CC
ON CC.UniqServReqID = REF.UniqServReqID AND (CC.UniqMonthID BETWEEN '$MONTH_ID' - 23 AND '$MONTH_ID') AND (CC.CareContDate BETWEEN PERI.StartDate AND PERI.EndDate)

INNER JOIN $MHSDS.MHS102ServiceTypeReferredTo
AS SRV
ON CC.UniqCareProfTeamID = SRV.UniqCareProfTeamID AND CC.UniqServReqID = SRV.UniqServReqID AND SRV.UniqMonthID = CC.UniqMonthID AND SRV.ServTeamTypeRefToMH = 'C02'


WHERE PERI.StartDate <= '$RP_ENDDATE'
AND PERI.EndDate >= '$RP_STARTDATE'
AND REF.RecordStartDate <= '$RP_ENDDATE' AND (REF.RecordEndDate IS NULL OR REF.RecordEndDate >= '$RP_ENDDATE') -- LAST VERSION OF RECORD DURING RP
AND (((REF.ServDischDate IS NULL OR REF.ServDischDate > '$RP_ENDDATE') AND REF.UniqMonthID = '$MONTH_ID') OR REF.ServDischDate <= '$RP_ENDDATE') -- MAKE SURE THAT THE REFERRAL EITHER CLOSED DOWN DURING THE RP, OR IF OPEN AT END RP WE HAVE RECORD FOR FINAL MONTH IN RP
AND REF.ReferralRequestReceivedDate <= '$RP_ENDDATE'
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= '$RP_STARTDATE')
AND REF.ReferralRequestReceivedDate <= PERI.EndDate
AND (REF.ServDischDate IS NULL OR REF.ServDischDate >= PERI.StartDate)
AND (REF.MH = 'Y' OR REF.CAMHS = 'Y')
AND PERI.AgeAtBookingMother >= 16