# Databricks notebook source
%md
# Following tables/views are created which are used across EIP metric in aggregation queries
- providers_between_rp_start_end_dates
- MHS001_CCG_LATEST  # this table is no longer created here... created here: notebooks/00_common_objects/01_prep_common_objects
- MHS006MHCareCoord_LATEST
- MHS101Referral_LATEST
- EIP_MHS101Referral_LATEST
- MHS102ServiceTypeReferredTo_LATEST
- EIP_MHS102ServiceTypeReferredTo_LATEST
- earliest_care_contact_dates_by_service_request_id
- earliest_care_assessment_dates_by_service_request_id
- earliest_care_assessment_dates_by_service_request_id_Prov
- earliest_care_assessment_dates_by_service_request_id_any_team
- Distinct_Referral_UniqServReqIDs
- Distinct_Referral_UniqServReqIDs_any_month
- EIP01_common
- EIP01_common_prov
- EIP23a_common  --Materialised
- EIP23a_common_Prov  --Materialised
- EIP23d_common
- EIP23d_common_Prov
- EIP23g_common
- EIP23h_common
- EIP23h_common_Prov
- EIP32_ED32_common
- EIP63_common
- earliest_care_contact_dates_by_service_request_id_any_prof
- EIP64abc_common --Materialised

# COMMAND ----------

# DBTITLE 1,providers_between_dates (EIP)
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* providers_between_rp_start_end_dates: This is a view used in a lot of the the Provider breakdowns        

  This returns the OrgIDProvider codes between the reporting period dates from MHS000Header
                                                                                 */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW providers_between_rp_start_end_dates AS 
SELECT DISTINCT OrgIDProvider as ORG_CODE, x.NAME as NAME
           FROM $db_source.MHS000Header as Z
                LEFT OUTER JOIN $db_output.RD_ORG_DAILY_LATEST AS X
					ON Z.OrgIDProvider = X.ORG_CODE
             WHERE	UniqMonthID BETWEEN '$month_id' -2 AND '$month_id'
            

# COMMAND ----------

# DBTITLE 1,MHS006MHCareCoord_LATEST (EIP)
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* MHS006MHCareCoord_LATEST: This is a view used in a lot of EIP metrics breakdowns        

  This provides the latest MSH006CareCoord Assessment care coordination dates for a given provider between 
  the reporting period dates
                                                                                 */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW MHS006MHCareCoord_LATEST AS 
     SELECT DISTINCT c.CareProfServOrTeamTypeAssoc, 
            c.UniqMonthID,
            c.OrgIDProv,
            c.StartDateAssCareCoord,
            c.Person_ID,
            c.EndDateAssCareCoord
       FROM $db_source.MHS006MHCareCoord as c
      WHERE ((c.RecordEndDate IS NULL OR c.RecordEndDate >= '$rp_enddate') AND c.RecordStartDate <= '$rp_enddate');

# COMMAND ----------

# DBTITLE 1,EIP_MHS101Referral_LATEST (EIP)

%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* MHS101Referral_LATEST: Extension of MHS101Referral_LATEST with additional filters which are used across 
  EIP metrics
                                                              */
  /* ---------------------------------------------------------------------------------------------------------*/


CREATE OR REPLACE GLOBAL TEMP VIEW EIP_MHS101Referral_LATEST AS
SELECT DISTINCT r.Person_ID,
           r.UniqServReqID,
           r.UniqMonthID,
           r.OrgIDProv,
           r.ReferralRequestReceivedDate,
           r.PrimReasonReferralMH,
           r.ServDischDate,
           r.AgeServReferRecDate,
           E.NHSDEthnicity,
           CCG.IC_Rec_CCG
      FROM $db_source.MHS101Referral AS r
      left join global_temp.MHS001MPI_PATMRECINRP_FIX AS E 
          ON r.Person_ID = E.Person_ID 
          AND E.UniqMonthID = r.UniqMonthID 
          AND E.PatMRecInRP_FIX = True
        LEFT JOIN 
       $db_output.MHS001_CCG_LATEST as CCG
          ON CCG.Person_ID = E.Person_ID
     WHERE ((r.RecordEndDate IS NULL OR r.RecordEndDate >= '$rp_enddate') AND r.RecordStartDate <= '$rp_enddate')
     and r.ReferralRequestReceivedDate >= '2016-01-01'
           AND r.PrimReasonReferralMH = '01'

# COMMAND ----------

# DBTITLE 1,MHS102ServiceTypeReferredTo_LATEST (EIP)
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* MHS102ServicetypeReferredTo_LATEST: This is a view used in a lot of EIP metrics breakdowns        
  This provides the latest ServTeamTypeReftoMH for the Uniq IDs between the reporting period dates from  
  MSH102ServiceTypeReferredTo                                                                */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW MHS102ServiceTypeReferredTo_LATEST AS
     SELECT S.UniqMonthID
            ,S.OrgIDProv
            ,S.ReferClosureDate
            ,S.ReferRejectionDate
            ,S.ServTeamTypeRefToMH
            ,S.UniqCareProfTeamID
            ,S.Person_ID
            ,S.UniqServReqID
       FROM $db_source.MHS102ServiceTypeReferredTo s
      WHERE (( s.RecordEndDate IS NULL OR s.RecordEndDate >= '$rp_enddate') AND s.RecordStartDate <= '$rp_enddate' );

# COMMAND ----------

# DBTITLE 1,EIP_MHS102ServiceTypeReferredTo_LATEST
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP_MHS102ServicetypeReferredTo_LATEST: Extension of MHS102ServiceTypeReferredTo with additional filters 
  which are used across other EIP mterics                                                                               */ 
/* ---------------------------------------------------------------------------------------------------------*/


CREATE OR REPLACE GLOBAL TEMP VIEW EIP_MHS102ServiceTypeReferredTo_LATEST AS
    SELECT s.UniqMonthID,
           s.OrgIDProv,
           s.ReferClosureDate,
           s.ReferRejectionDate,
           s.ServTeamTypeRefToMH,
           s.UniqCareProfTeamID,
           s.Person_ID,
           s.UniqServReqID
      FROM global_temp.MHS102ServiceTypeReferredTo_LATEST s
     WHERE s.ServTeamTypeRefToMH = 'A14'
           AND 
           (
             (
               (
                 (s.ReferClosureDate IS NULL OR s.ReferClosureDate > '$rp_enddate') AND 
                 (s.ReferRejectionDate IS NULL OR s.ReferRejectionDate > '$rp_enddate')
               ) 
             ) OR s.ReferClosureDate <= '$rp_enddate' OR s.ReferRejectionDate <= '$rp_enddate'
           );

# COMMAND ----------

# DBTITLE 1,earliest_care_contact_dates_by_service_request_id
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* earliest_care_contact_dates_by_service_request_id: This view is used as sub query in many EIP metrics.
  It returns the earliest care contact dates for each service request ID.                                                                               */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW earliest_care_contact_dates_by_service_request_id AS 
     SELECT a.UniqServReqID,
            MIN(CareContDate) AS CareContDate
       FROM $db_source.MHS201CareContact AS a
       
       INNER JOIN $db_output.validcodes as vc
            ON vc.table = 'mhs201carecontact' and vc.field = 'ConsMechanismMH' and vc.Measure = 'AWT' and vc.type = 'include' and a.ConsMechanismMH = vc.ValidValue
            and a.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or a.UniqMonthID <= vc.LastMonth)
            
  LEFT JOIN global_temp.EIP_MHS101Referral_LATEST b
            ON a.UniqServReqID = b.UniqServReqID
  LEFT JOIN $db_source.MHS102ServiceTypeReferredTo AS x
            ON a.UniqCareProfTeamID = x.UniqCareProfTeamID
            AND a.UniqServReqID = x.UniqServReqID
            AND x.UniqMonthID <= $month_id
      WHERE a.AttendOrDNACode IN ('5', '6')
            AND x.ServTeamTypeRefToMH = 'A14'
            AND a.CareContDate >= b.ReferralRequestReceivedDate
            AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = $month_id) 
                    OR b.ServDischDate <= '$rp_enddate')
            AND a.UniqMonthId <= $month_id
   GROUP BY a.UniqServReqID;

# COMMAND ----------

# DBTITLE 1,earliest_care_assessment_dates_by_service_request_id
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* earliest_care_assessment_dates_by_service_request_id: This view is used as sub query in many EIP metrics.
  It returns the earliest care care assessment date for each service request ID.                                                                                */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW earliest_care_assessment_dates_by_service_request_id AS 
    SELECT B.UniqServReqID,
           MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord
      FROM global_temp.MHS006MHCareCoord_LATEST AS a
 LEFT JOIN global_temp.EIP_MHS101Referral_LATEST AS B
           ON A.Person_ID = B.Person_ID
     WHERE A.CareProfServOrTeamTypeAssoc = 'A14'
	       AND A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
	       AND ((B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') OR A.StartDateAssCareCoord <= B.ServDischDate)
           AND (((A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') AND A.UniqMonthID = $month_id) OR A.EndDateAssCareCoord <= '$rp_enddate')
           AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = $month_id) OR b.ServDischDate <= '$rp_enddate')
  GROUP BY B.UniqServReqID

# COMMAND ----------

# DBTITLE 1,earliest_care_assessment_dates_by_service_request_id_Prov
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* earliest_care_assessment_dates_by_service_request_id_Prov: This view is used as sub query in many EIP 
  metrics Provider breakdowns.
  It returns the earliest care care assessment date for each service request ID, however it is also joined on
  Provider as both occurnaces must have been in the same provider. */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW earliest_care_assessment_dates_by_service_request_id_Prov AS 
    SELECT B.UniqServReqID,
           B.OrgIDProv,
           MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord
      FROM global_temp.MHS006MHCareCoord_LATEST AS a
 LEFT JOIN global_temp.EIP_MHS101Referral_LATEST AS B
           ON A.Person_ID = B.Person_ID
     WHERE A.CareProfServOrTeamTypeAssoc = 'A14'
           AND A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
           AND ((B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') OR A.StartDateAssCareCoord <= B.ServDischDate)
           AND (((A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') AND A.UniqMonthID = $month_id) OR A.EndDateAssCareCoord <= '$rp_enddate')
           AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = $month_id) OR b.ServDischDate <= '$rp_enddate')
  GROUP BY B.UniqServReqID,
           B.OrgIDProv

# COMMAND ----------

# DBTITLE 1,earliest_care_assessment_dates_by_service_request_id_any_team
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* earliest_care_assessment_dates_by_service_request_id_all_teams: This view is used as sub query in EIP64.
  It returns the earliest care care assessment date for each service request ID where the team type is not restricted to A14.*/ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW earliest_care_assessment_dates_by_service_request_id_any_team AS 
    SELECT B.UniqServReqID,
           MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord
      FROM global_temp.MHS006MHCareCoord_LATEST AS a
 LEFT JOIN global_temp.EIP_MHS101Referral_LATEST AS B
           ON A.Person_ID = B.Person_ID
     WHERE 
           A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
	       AND (
                 (B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') 
               OR A.StartDateAssCareCoord <= B.ServDischDate
               )
           AND (
                 (
                    (A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') 
                    AND A.UniqMonthID = $month_id
                 ) 
               OR A.EndDateAssCareCoord <= '$rp_enddate'
               )
           AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = $month_id) OR b.ServDischDate <= '$rp_enddate')
  GROUP BY B.UniqServReqID

# COMMAND ----------

# DBTITLE 1,Distinct_Referral_UniqServReqIDs
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* Distinct_Referral_UniqSerReqIDs - this is used in EIP63 and returns a distinct list of UniqServReqIDs 
after joining the referral intermediate tables.                                                                         */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW Distinct_Referral_UniqServReqIDs AS
select distinct a.UniqServReqID
           from global_temp.EIP_MHS101Referral_LATEST a 
      left join global_temp.MHS102ServiceTypeReferredTo_LATEST b 
                ON A.UniqServReqID = B.UniqServReqID 
                and a.UniqMonthID = b.UniqMonthID
          where B.ServTeamTypeRefToMH = 'A14'

# COMMAND ----------

# DBTITLE 1,Distinct_Referral_UniqServReqIDs_any_month
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* Distinct_Referral_UniqSerReqIDs - this is used in EIP63 and returns a distinct list of UniqServReqIDs 
after joining the referral intermediate tables.                                                                        */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW Distinct_Referral_UniqServReqIDs_any_month AS
select distinct a.UniqServReqID
           from global_temp.EIP_MHS101Referral_LATEST a 
      left join global_temp.MHS102ServiceTypeReferredTo_LATEST b 
                ON A.UniqServReqID = B.UniqServReqID 
          where B.ServTeamTypeRefToMH = 'A14'

# COMMAND ----------

# DBTITLE 1,EIP01_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP01_common: This is the core calculation for the EIP01 metric, used for National and CCG      

  This has been denormalised across age groups and so returns the UniqServReqID and "clock stop" for the age
  groupings.
  Old code description: FEP REFERRALS ON FEP PATHWAY IN TREATMENT AT END OF REPORTING PERIOD
*/ 
/* ---------------------------------------------------------------------------------------------------------*/

TRUNCATE TABLE $db_output.EIP01_common;

INSERT INTO TABLE $db_output.EIP01_common
SELECT	CASE	WHEN E.AgeRepPeriodEnd BETWEEN 0 and 17 THEN '00-17' --chunks the calculation into age groupings
				WHEN E.AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18-34' 
				WHEN E.AgeRepPeriodEnd >= 35 THEN '35-120'
				END as AGE_GROUP,        
		A.UniqServReqID,
        CCG.IC_Rec_CCG,
        CASE WHEN E.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
              WHEN E.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
              WHEN E.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
              WHEN E.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
              WHEN E.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
              WHEN E.NHSDEthnicity = 'Z' THEN 'Not Stated'
              WHEN E.NHSDEthnicity = '99' THEN 'Unknown'
              Else 'Unknown'
              END AS NHSDEthnicity,  
		CASE	WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
				ELSE D.StartDateAssCareCoord
				END AS CLOCK_STOP
FROM global_temp.EIP_MHS101Referral_LATEST AS A
	INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS B
		ON A.UniqServReqID = B.UniqServReqID
	INNER JOIN	global_temp.earliest_care_contact_dates_by_service_request_id AS C
		ON A.UniqServReqID = C.UniqServReqID
	INNER JOIN	global_temp.earliest_care_assessment_dates_by_service_request_id AS D
		ON A.UniqServReqID = D.UniqServReqID
     INNER JOIN global_temp.MHS001MPI_PATMRECINRP_FIX AS E
          ON A.Person_ID = E.Person_ID 
         AND E.UniqMonthID = A.UniqMonthID 
         AND E.PatMRecInRP_FIX = True
     LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
           ON CCG.Person_ID = A.Person_ID
WHERE CASE	WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
			ELSE D.StartDateAssCareCoord
			END BETWEEN '2016-01-01' AND '$rp_enddate'
  AND ((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = '$month_id') 
  AND B.ServTeamTypeRefToMH = 'A14' 
  AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')) AND B.UniqMonthID = A.UniqMonthID)
       OR B.ReferClosureDate <= '$rp_enddate' 
       OR B.ReferRejectionDate <= '$rp_enddate')            

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP01_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP01_common'))

# COMMAND ----------

# DBTITLE 1,EIP01_common_Prov
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP01_common_Prov: This is the core calculation for the EIP01 metric Provider breakdown.      

  This has been denormalised across age groups and so returns the UniqServReqID and "clock stop" for the age
  groupings.
  Old code description: FEP REFERRALS ON FEP PATHWAY IN TREATMENT AT END OF REPORTING PERIOD
                                                                                */ 
/* ---------------------------------------------------------------------------------------------------------*/

TRUNCATE TABLE $db_output.EIP01_common_prov;

INSERT INTO TABLE $db_output.EIP01_common_prov
SELECT	CASE	WHEN E.AgeRepPeriodEnd BETWEEN 0 and 17 THEN '00-17'
				WHEN E.AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18-34' 
				WHEN E.AgeRepPeriodEnd >= 35 THEN '35-120'
				END as AGE_GROUP,
				A.UniqServReqID,
				A.OrgIDProv,
                CASE WHEN E.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN E.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN E.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN E.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN E.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN E.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN E.NHSDEthnicity = '99' THEN 'Unknown'
                    Else 'Unknown'
                END AS NHSDEthnicity,  
				CASE	WHEN C.CareContDate > D.StartDateAssCareCoord
							THEN C.CareContDate
							ELSE D.StartDateAssCareCoord
						END
					AS CLOCK_STOP
FROM			global_temp.EIP_MHS101Referral_LATEST	AS A
				INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS B 
					ON A.UniqServReqID = B.UniqServReqID
				INNER JOIN	global_temp.earliest_care_contact_dates_by_service_request_id AS C 
					ON A.UniqServReqID = C.UniqServReqID
				INNER JOIN	global_temp.earliest_care_assessment_dates_by_service_request_id_Prov AS D 
					ON A.UniqServReqID = D.UniqServReqID 
                    AND A.OrgIDProv = D.OrgIDProv
				INNER JOIN global_temp.MHS001MPI_PATMRECINRP_FIX AS E
                    ON A.Person_ID = E.Person_ID 
                    AND A.OrgIDProv = E.OrgIDProv
                    AND E.UniqMonthID = A.UniqMonthID
WHERE	    CASE	WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
						ELSE D.StartDateAssCareCoord
						END	BETWEEN '2016-01-01' AND '$rp_enddate'
			AND ((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = $month_id)
			AND B.ServTeamTypeRefToMH = 'A14'  
			AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')) AND B.UniqMonthID = A.UniqMonthID) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP01_common_prov'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP01_common_prov'))

# COMMAND ----------

# DBTITLE 1,EIP23a_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23a_common: This view is used to do most of the calculation of EIP23a metric. This is later used 
  in the National and CCG breakdowns for all age groupings. The age groups are all calculated together to
  save effort 
                                                                                 */ 
/* ---------------------------------------------------------------------------------------------------------*/


TRUNCATE TABLE $db_output.EIP23a_common;

INSERT INTO $db_output.EIP23a_common
      SELECT CASE WHEN AgeServReferRecDate BETWEEN 0 and 17 THEN '00-17'
                  WHEN AgeServReferRecDate BETWEEN 18 AND 34 THEN '18-34' 
                  WHEN AgeServReferRecDate >= 35 THEN '35-120' 
                  END As AGE_GROUP, 
             A.UniqServReqID,
             CCG.IC_Rec_CCG,
             CASE WHEN A.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                WHEN A.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                WHEN A.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                WHEN A.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                WHEN A.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                WHEN A.NHSDEthnicity = 'Z' THEN 'Not Stated'
                WHEN A.NHSDEthnicity = '99' THEN 'Unknown'
                Else 'Unknown'
             END AS NHSDEthnicity, 
             DATEDIFF (
                      CASE WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
                      ELSE D.StartDateAssCareCoord
                      END,
                      A.ReferralRequestReceivedDate
                      ) days_between_ReferralRequestReceivedDate --Clock Stop
        FROM global_temp.EIP_MHS101Referral_LATEST AS A
   INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS B
             ON A.UniqServReqID = B.UniqServReqID 
   INNER JOIN global_temp.earliest_care_contact_dates_by_service_request_id AS C
             ON A.UniqServReqID = C.UniqServReqID
   INNER JOIN global_temp.earliest_care_assessment_dates_by_service_request_id AS D
             ON A.UniqServReqID = D.UniqServReqID
     LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
           ON CCG.Person_ID = A.Person_ID
       WHERE CASE 
         WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
         ELSE D.StartDateAssCareCoord
         END 
        BETWEEN '$rp_startdate_quarterly' AND '$rp_enddate'
             AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = $month_id) OR A.ServDischDate <= '$rp_enddate')
             AND B.ServTeamTypeRefToMH = 'A14'
			AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')) AND B.UniqMonthID = A.UniqMonthID) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP23a_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP23a_common'))

# COMMAND ----------

# DBTITLE 1,EIP23a_common_Prov
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23a_common_Prov: This view is used to do most of the calculation of EIP23a metric Provider breakdown. It needs
  its own prep table due to the use of earliest_care_assessment_dates_by_service_request_id_Prov. 
  The age groups are all calculated together to save effort.                                                                               */ 
/* ---------------------------------------------------------------------------------------------------------*/

TRUNCATE table $db_output.EIP23a_common_Prov;

INSERT INTO $db_output.EIP23a_common_Prov
     SELECT CASE WHEN AgeServReferRecDate BETWEEN 0 and 17 THEN '00-17'
                 WHEN AgeServReferRecDate BETWEEN 18 AND 34 THEN '18-34' 
                 WHEN AgeServReferRecDate >= 35 THEN '35-120' 
                 END AS AGE_GROUP,
            a.UniqServReqID as UniqServReqID,
			A.OrgIDProv as OrgIDPRov, 
            CASE WHEN A.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                WHEN A.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                WHEN A.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                WHEN A.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                WHEN A.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                WHEN A.NHSDEthnicity = 'Z' THEN 'Not Stated'
                WHEN A.NHSDEthnicity = '99' THEN 'Unknown'
                Else 'Unknown'
             END AS NHSDEthnicity, 
            DATEDIFF (
                     CASE WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
                     ELSE D.StartDateAssCareCoord
                     END,
                     A.ReferralRequestReceivedDate
                     ) days_between_ReferralRequestReceivedDate --Clock Stop
       FROM global_temp.EIP_MHS101Referral_LATEST AS A
  INNER JOIN global_temp.EIP_MHS102ServiceTypeReferredTo_LATEST AS B
            ON A.UniqServReqID = B.UniqServReqID 
            AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')) 
				AND B.UniqMonthID = A.UniqMonthID) 
			OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate')
  INNER JOIN global_temp.earliest_care_contact_dates_by_service_request_id AS C
            ON A.UniqServReqID = C.UniqServReqID
  INNER JOIN global_temp.earliest_care_assessment_dates_by_service_request_id_Prov AS D
            ON A.UniqServReqID = D.UniqServReqID 
            AND A.OrgIDProv = D.OrgIDProv 
      WHERE C.CareContDate IS NOT NULL 
             AND D.StartDateAssCareCoord IS NOT NULL
             AND CASE WHEN C.CareContDate > D.StartDateAssCareCoord 
                      THEN C.CareContDate
                      ELSE D.StartDateAssCareCoord
                      END BETWEEN '$rp_startdate_quarterly' AND '$rp_enddate'
             AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = $month_id) OR A.ServDischDate <= '$rp_enddate') 

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP23a_common_Prov'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP23a_common_Prov'))

# COMMAND ----------

# DBTITLE 1,EIP23d_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23d_common: This view is used as do most of the calculation of EIP23d metric. This is later used 
  in the National and CCG breakdowns for all age groupings. The age groups are all calculated together to
  save effort. */ 
/* ---------------------------------------------------------------------------------------------------------*/

TRUNCATE TABLE $db_output.EIP23d_common;

INSERT INTO TABLE $db_output.EIP23d_common
     SELECT CASE WHEN AgeServReferRecDate BETWEEN 0 and 17 THEN '00-17'
                 WHEN AgeServReferRecDate BETWEEN 18 AND 34 THEN '18-34' 
                 WHEN AgeServReferRecDate >= 35 THEN '35-120' 
                 END AS AGE_GROUP,
            a.UniqServReqID,
            a.IC_Rec_CCG,
            CASE WHEN a.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
              WHEN a.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
              WHEN a.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
              WHEN a.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
              WHEN a.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
              WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
              WHEN a.NHSDEthnicity = '99' THEN 'Unknown'
              Else 'Unknown'
              END AS NHSDEthnicity,               
            DATEDIFF ('$rp_enddate',a.ReferralRequestReceivedDate) AS days_between_endate_ReferralRequestReceivedDate
       FROM global_temp.EIP_MHS101Referral_LATEST AS a
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS b
            ON a.UniqServReqID = b.UniqServReqID 
  LEFT JOIN global_temp.earliest_care_contact_dates_by_service_request_id AS c
            ON a.UniqServReqID = c.UniqServReqID
  LEFT JOIN global_temp.earliest_care_assessment_dates_by_service_request_id AS d
            ON a.UniqServReqID = d.UniqServReqID
   LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
             ON CCG.Person_ID = A.Person_ID
      WHERE (c.CareContDate IS NULL OR d.StartDateAssCareCoord IS NULL)
      		AND B.ServTeamTypeRefToMH = 'A14'
            AND (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = $month_id) OR a.ServDischDate <= '$rp_enddate') 
            AND (a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate')
            AND (
					(
						(
							(B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
						) AND B.UniqMonthID = a.UniqMonthID
					) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
				)

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP23d_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP23d_common'))

# COMMAND ----------

# DBTITLE 1,EIP23d_common_Prov
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23d_common_Prov: This view is used to do most of the calculation of EIP23d metric Provider breakdown. It needs
  its own prep table due to the use of earliest_care_assessment_dates_by_service_request_id_Prov. 
  The age groups are all calculated together to save effort. */ 
/* ---------------------------------------------------------------------------------------------------------*/

TRUNCATE TABLE $db_output.EIP23d_common_Prov;

INSERT INTO TABLE $db_output.EIP23d_common_Prov
     SELECT CASE WHEN AgeServReferRecDate BETWEEN 0 and 17 THEN '00-17'
                 WHEN AgeServReferRecDate BETWEEN 18 AND 34 THEN '18-34' 
                 WHEN AgeServReferRecDate >= 35 THEN '35-120' 
                 END AS AGE_GROUP,
            A.UniqServReqID,
			A.OrgIDProv,
            CASE WHEN A.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                WHEN A.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                WHEN A.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                WHEN A.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                WHEN A.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                WHEN A.NHSDEthnicity = 'Z' THEN 'Not Stated'
                WHEN A.NHSDEthnicity = '99' THEN 'Unknown'
                Else 'Unknown'
             END AS NHSDEthnicity, 
            DATEDIFF('$rp_enddate',a.ReferralRequestReceivedDate) as days_between_endate_ReferralRequestReceivedDate
       FROM global_temp.EIP_MHS101Referral_LATEST AS A
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS B
            ON A.UniqServReqID = B.UniqServReqID 
  LEFT JOIN global_temp.earliest_care_contact_dates_by_service_request_id AS C
            ON A.UniqServReqID = C.UniqServReqID
  LEFT JOIN global_temp.earliest_care_assessment_dates_by_service_request_id_Prov AS D
            ON A.UniqServReqID = D.UniqServReqID 
            AND A.OrgIDProv = D.OrgIDProv 
      WHERE (C.CareContDate IS NULL OR D.StartDateAssCareCoord IS NULL)
      		AND B.ServTeamTypeRefToMH = 'A14'
            AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = $month_id) OR A.ServDischDate <= '$rp_enddate') 
            AND (A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate')
            AND (
					(
						(
							(B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
						) AND B.UniqMonthID = a.UniqMonthID
					) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
				)

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP23d_common_Prov'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP23d_common_Prov'))

# COMMAND ----------

# DBTITLE 1,EIP23g_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23g_common: This view is used to do most of the calculation of EIP23a metric National, Provider and CCG
  breakdowns. This is different to the other metrics because it doesnt use 
  earliest_care_assessment_dates_by_service_request_id. The age groups are all calculated together to
  save effort. */ 
/* ---------------------------------------------------------------------------------------------------------*/


CREATE OR REPLACE GLOBAL TEMP VIEW EIP23g_common AS
     SELECT a.UniqServReqID,
            IC_Rec_CCG,
            a.OrgIDProv,
            c.CareContDate
       FROM global_temp.EIP_MHS101Referral_LATEST AS a
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS b -- DO NOT USE EIP_MHS102ServiceTypeReferredTo_Latest!
            ON a.UniqServReqID = b.UniqServReqID 
  INNER JOIN global_temp.earliest_care_contact_dates_by_service_request_id AS c
            ON a.UniqServReqID = c.UniqServReqID
      WHERE c.CareContDate IS NOT NULL
        AND c.CareContDate BETWEEN '$rp_startdate_quarterly' AND '$rp_enddate'
        AND (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = $month_id) OR a.ServDischDate <= '$rp_enddate')            
        AND B.ServTeamTypeRefToMH = 'A14'
        AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') 
                AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
              )
              AND B.UniqMonthID = A.UniqMonthID -- <-- this is why EIP_MHS102ServiceTypeReferredTo_Latest could not be used
             ) 
             OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
            );

# COMMAND ----------

# DBTITLE 1,EIP23h_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23h_common: This view is used to do most of the calculation of EIP23h metric. This is later used 
  in the National and CCG breakdowns for all age groupings. The age groups are all calculated together to
  save effort. */ 
/* ---------------------------------------------------------------------------------------------------------*/


CREATE OR REPLACE GLOBAL TEMP VIEW EIP23h_common AS
     SELECT a.UniqServReqID,
           IC_Rec_CCG,
            d.StartDateAssCareCoord
       FROM global_temp.EIP_MHS101Referral_LATEST AS a
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS b
            ON a.UniqServReqID = b.UniqServReqID 
  INNER JOIN global_temp.earliest_care_assessment_dates_by_service_request_id AS d
            ON a.UniqServReqID = d.UniqServReqID
      WHERE d.StartDateAssCareCoord IS NOT NULL
        AND D.StartDateAssCareCoord	BETWEEN '$rp_startdate_quarterly' AND '$rp_enddate'
        AND ((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate')
        AND a.UniqMonthId = '$month_id' OR a.ServDischDate <= '$rp_enddate') 
                    -- below would have been contained in EIP_MHS102ServiceTypeReferredTo_Latest
        AND B.ServTeamTypeRefToMH = 'A14'
        AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') 
                AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
              )
              AND B.UniqMonthID = A.UniqMonthID -- <-- this is why EIP_MHS102ServiceTypeReferredTo_Latest could not be used
             ) 
             OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
            );

# COMMAND ----------

# DBTITLE 1,EIP23h_common_Prov
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP23h_common_Prov: This view is used to do most of the calculation of EIP23h metric Provider breakdown. It needs
  its own prep table due to the use of earliest_care_assessment_dates_by_service_request_id_Prov. 
  The age groups are all calculated together to save effort.                                                                               */ 
/* ---------------------------------------------------------------------------------------------------------*/

CREATE OR REPLACE GLOBAL TEMP VIEW EIP23h_common_Prov AS
     SELECT a.UniqServReqID,
			A.OrgIDProv,
            d.StartDateAssCareCoord
       FROM global_temp.EIP_MHS101Referral_LATEST AS a
  INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS b
            ON a.UniqServReqID = b.UniqServReqID 
  INNER JOIN global_temp.earliest_care_assessment_dates_by_service_request_id_Prov AS d 
            ON a.UniqServReqID = d.UniqServReqID 
            AND A.OrgIDProv = D.OrgIDProv 
      WHERE d.StartDateAssCareCoord IS NOT NULL
            AND D.StartDateAssCareCoord	BETWEEN '$rp_startdate_quarterly' AND '$rp_enddate'
            AND ((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') 
        AND a.UniqMonthId = '$month_id' OR a.ServDischDate <= '$rp_enddate')
                    -- below would have been contained in EIP_MHS102ServiceTypeReferredTo_Latest
        AND B.ServTeamTypeRefToMH = 'A14'
        AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
              )
              AND B.UniqMonthID = A.UniqMonthID -- <-- this is why EIP_MHS102ServiceTypeReferredTo_Latest could not be used
             ) 
             OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
            );

# COMMAND ----------

# DBTITLE 1,EIP32_ED32_common
%sql
/* EIP32/ED32_common - this is used by both metrics simply by applying a different filter prior to aggregation.
  These metrics do not have age breakdowns */
  
  
TRUNCATE TABLE $db_output.EIP32_ED32_common;

INSERT INTO TABLE $db_output.EIP32_ED32_common
SELECT	A.UniqServReqID,
		A.OrgIDProv,
		CCG.IC_Rec_CCG,
        PrimReasonReferralMH,
        AgeServReferRecDate 
FROM global_temp.MHS101Referral_LATEST AS A	
LEFT OUTER JOIN global_temp.MHS001MPI_PATMRECINRP_FIX AS E
		ON A.Person_ID = E.Person_ID 
        AND E.UniqMonthID = A.UniqMonthID
LEFT OUTER JOIN (SELECT m.Person_ID,
                    CCG.IC_Rec_CCG AS IC_Rec_CCG
               FROM global_temp.MHS001MPI_PATMRECINRP_FIX AS m
               INNER JOIN $db_output.MHS001_CCG_LATEST AS CCG
                   ON CCG.Person_ID = m.Person_ID) AS CCG
		ON CCG.Person_ID = E.Person_ID					
WHERE	ReferralRequestReceivedDate >= '$rp_startdate_quarterly'
		AND ReferralRequestReceivedDate <= '$rp_enddate'
		AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') 
              AND A.UniqMonthID = $month_id) 
            OR A.ServDischDate <= '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP32_ED32_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP32_ED32_common'))

# COMMAND ----------

# DBTITLE 1,EIP63_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP63_common: This view is used to do most of the calculation of EIP63 metric National, Provider and CCG
  breakdowns. This is different to the other metrics because it doesnt use 
  earliest_care_assessment_dates_by_service_request_id. The age groups are all calculated together to
  save effort                                                                            */ 
/* ---------------------------------------------------------------------------------------------------------*/

TRUNCATE TABLE $db_output.EIP63_common;

INSERT INTO TABLE $db_output.EIP63_common
SELECT	A.UniqServReqID,
		A.OrgIDProv,
        IC_Rec_CCG,
           CASE   WHEN A.NHSDEthnicity = '99' THEN 'Unknown'
         WHEN A.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
              WHEN A.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
              WHEN A.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
              WHEN A.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
              WHEN A.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
              WHEN A.NHSDEthnicity = 'Z' THEN 'Not Stated'
              Else 'Unknown'
        END AS NHSDEthnicity, 
		CASE WHEN AgeServReferRecDate BETWEEN 0 and 17 THEN '00-17'
             WHEN AgeServReferRecDate BETWEEN 18 AND 34 THEN '18-34' 
             WHEN AgeServReferRecDate >= 35 THEN '35-120' 
             END As AGE_GROUP
FROM 	global_temp.EIP_MHS101Referral_LATEST AS A		
		LEFT JOIN global_temp.Distinct_Referral_UniqServReqIDs as f
			ON a.UniqServReqID = F.UniqServReqID
WHERE	f.UniqServReqID is NULL
        AND (A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = '$month_id' 

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP63_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP63_common'))

# COMMAND ----------

# DBTITLE 1,earliest_care_contact_dates_by_service_request_id_any_prof
%sql
--/* This is used in EIP64abc, and is similar to earliest_care_contact_dates_by_service_request_id however the join is different */

CREATE OR REPLACE GLOBAL TEMP VIEW earliest_care_contact_dates_by_service_request_id_any_prof AS
SELECT	A.UniqServReqID,
		MIN (CareContDate) AS CareContDate
FROM	$db_source.MHS201CareContact AS A

INNER JOIN $db_output.validcodes as vc
            ON vc.table = 'mhs201carecontact' and vc.field = 'ConsMechanismMH' and vc.Measure = 'AWT' and vc.type = 'include' and A.ConsMechanismMH = vc.ValidValue
            and A.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or A.UniqMonthID <= vc.LastMonth)
            
		LEFT OUTER JOIN global_temp.EIP_MHS101Referral_LATEST AS B
			ON A.UniqServReqID = B.UniqServReqID
		LEFT JOIN $db_source.MHS102ServiceTypeReferredTo as x 
            ON a.UniqServReqID = x.UniqServReqID
WHERE	A.AttendOrDNACode IN ('5', '6')
        AND A.CareContDate >= B.ReferralRequestReceivedDate
GROUP BY	A.UniqServReqID

# COMMAND ----------

# DBTITLE 1,EIP64abc_common
%sql
/* ---------------------------------------------------------------------------------------------------------*/
/* EIP64abc_common: This view is used to do most of the calculation of EIP64abc metric National, Provider and CCG
  breakdowns (EIP64b becomes EIP65 and EIP64c becomes EIP66). This is different to the other metrics because it doesnt use 
  earliest_care_assessment_dates_by_service_request_id_prov. The age groups are all calculated together to
  save effort.                                                                            */ 
/* ---------------------------------------------------------------------------------------------------------*/



TRUNCATE table $db_output.EIP64abc_common;

INSERT INTO $db_output.EIP64abc_common
SELECT	CASE WHEN AgeServReferRecDate BETWEEN 0 and 17 THEN '00-17'
			WHEN AgeServReferRecDate BETWEEN 18 AND 34 THEN '18-34' 
			WHEN AgeServReferRecDate >= 35 THEN '35-120' 
			END As AGE_GROUP,
            
         CASE WHEN A.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
              WHEN A.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
              WHEN A.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
              WHEN A.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
              WHEN A.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
              WHEN A.NHSDEthnicity = 'Z' THEN 'Not Stated'
              WHEN A.NHSDEthnicity = '99' THEN 'Unknown'
              Else 'Unknown'
              END AS NHSDEthnicity,
              
		A.OrgIDProv, -- will be used for Prov
		A.UniqServReqID,
		IC_Rec_CCG, -- will also be used for CCG
		a.ReferralRequestReceivedDate, -- used for EIP66 and EIP65
		GREATEST(C.CareContDate, D.StartDateAssCareCoord) AS CLOCK_STOP
FROM	global_temp.EIP_MHS101Referral_LATEST as A
		INNER JOIN global_temp.MHS102ServiceTypeReferredTo_LATEST AS B
			ON A.UniqServReqID = B.UniqServReqID and a.UniqMonthID = b.UniqMonthID
		INNER JOIN	global_temp.earliest_care_contact_dates_by_service_request_id_any_prof AS C
			ON A.UniqServReqID = C.UniqServReqID
		INNER JOIN	global_temp.earliest_care_assessment_dates_by_service_request_id_any_team AS D
			ON A.UniqServReqID = D.UniqServReqID
		LEFT JOIN global_temp.Distinct_Referral_UniqServReqIDs_any_month as F
			ON a.UniqServReqID = F.UniqServReqID
WHERE	f.UniqServReqID IS NULL
		AND GREATEST(C.CareContDate, D.StartDateAssCareCoord) BETWEEN '$rp_startdate_quarterly' AND '$rp_enddate'
         AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = $month_id) OR A.ServDischDate <= '$rp_enddate')

# COMMAND ----------

import os

db_output = dbutils.widgets.get("db_output")

if os.environ['env'] == 'prod':
  spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='EIP64abc_common'))

spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='EIP64abc_common'))