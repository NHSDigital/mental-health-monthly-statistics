-- Databricks notebook source
-- DBTITLE 1,EIP01 national age groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP01_common 
 to make the national EIP01a, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN,
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP01c'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP01b'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP01a'
                 END AS METRIC,
           COALESCE(COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP01_common as A
   GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP01 national national
 %sql

 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 national national aggregation - this counts the number of distinct UniqServReqIDs in EIP01_common 
 to make the national EIP01 (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* --------------------------------------------MHS69-------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted
   SELECT  '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'EIP01' as METRIC,
 		  COALESCE(COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP01_common as A

-- COMMAND ----------

-- DBTITLE 1,EIP01 CCG age groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 CCG age groups aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP01_common to make CCG EIP01a, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP01c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP01b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP01a'
                      END AS METRIC,
 				COALESCE(COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP01_common AS A
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP01 CCG national
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 CCG national aggregation - this counts the number of distinct UniqServReqIDs per CCG in 
 EIP01_common to make the CCG EIP01 metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT        '$rp_startdate' AS REPORTING_PERIOD_START, 
               '$rp_enddate' AS REPORTING_PERIOD_END,
               '$status' AS STATUS,
               'CCG - GP Practice or Residence' AS BREAKDOWN,
               COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
               NULL as LEVEL_DESCRIPTION,
               'EIP01' as METRIC,
               COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
               '$db_source' AS SOURCE_DB,
               'NONE' AS SECONDARY_LEVEL, 
               'NONE' as SECONDARY_LEVEL_DESCRIPTION
               
 FROm          $db_output.EIP01_common as A
 GROUP BY      A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP01 Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP01_common to make the provider EIP01a, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP01c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP01b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP01a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP01_common_prov AS A
 GROUP BY		 A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP01 Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP01_common to make the provider EIP01 metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP01' as METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP01_common_prov AS A
 GROUP BY		A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP01 National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP01_Common to make the metrics (for each Ethnicity), and then inserts this into the
 AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-01-25 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'England; Ethnicity' AS BREAKDOWN,
 				'England' AS LEVEL,
                 'England' AS LEVEL_DESCRIPTION,
                 'EIP01' AS METRIC,
 				COALESCE(COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP01_Common AS A
 GROUP BY		A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP01 CCG national - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per CCG in 
 EIP01_common to make the CCG EIP01 metrics based on Ethnicity, and then inserts this into the
 AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT        '$rp_startdate' AS REPORTING_PERIOD_START, 
               '$rp_enddate' AS REPORTING_PERIOD_END,
               '$status' AS STATUS,
               'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
               COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
               NULL as LEVEL_DESCRIPTION,
               'EIP01' as METRIC,
               COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
               '$db_source' AS SOURCE_DB,
               A.NHSDEthnicity AS SECONDARY_LEVEL, 
               A.NHSDEthnicity AS SECONDARY_LEVEL_DESCRIPTION
               
 FROm          $db_output.EIP01_common as A
 GROUP BY      A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP01 Provider National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP01 provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in 
 EIP01_common to make the provider EIP01 metric, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP01' as METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity AS SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP01_common_prov AS A
 GROUP BY		A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23a National Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP23a_common 
 to make the national EIP23aa, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23ac'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23ab'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23aa'
                 END AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
    GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP23a National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a national national aggregation - this counts the number of distinct UniqServReqIDs in EIP01_common 
 to make the national EIP23a (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-13                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP23a' AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A

-- COMMAND ----------

-- DBTITLE 1,EIP23a CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a CCG age groups aggregation - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23aa, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23ac'
                       WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23ab'
                       WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23aa'
                 END AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common A
 GROUP BY A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23a CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a CCG national aggregation - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23a metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23a' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common as  A
 GROUP BY A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23a Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a Provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23aa, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23ac'
                  WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23ab'
                  WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23aa'
                  END AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
    GROUP BY A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23a Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a Provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23a metric (age agnostic), and then inserts this into the
 AWT_unformatted table.

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23a' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
    GROUP BY A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23a National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP23a_Common to make the metrics, and then inserts this into the AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-12 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL,
           'England' AS LEVEL_DESCRIPTION,
           'EIP23a' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION               
           
    FROM  $db_output.EIP23a_common A
    GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23a CCG National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23a metric, and then inserts this into the AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-12 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23a' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common as  A
 GROUP BY A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23a Provider National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23a Provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23a metric, and then inserts this into the AWT_unformatted table.

 Bhabani Sahoo changed dated 2021-02-12 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider; Ethnicity' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23a' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             A.NHSDEthnicity AS SECONDARY_LEVEL, 
             A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
    GROUP BY A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23b National Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP23a_common 
 to make the national EIP23ba, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23bc'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23bb'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ba'
                 END AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
    WHERE A.days_between_ReferralRequestReceivedDate <= 14
    GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP23b National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b national national aggregation - this counts the number of distinct UniqServReqIDs in EIP23a_common 
 to make the national EIP23b metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP23b' AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
    WHERE A.days_between_ReferralRequestReceivedDate <= 14

-- COMMAND ----------

-- DBTITLE 1,EIP23b CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b CCG age groups aggregation - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23ba, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23bc'
                       WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23bb'
                       WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ba'
                 END AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common as A
 WHERE A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23b CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b CCG national aggregation - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23b metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT    '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'CCG - GP Practice or Residence' AS BREAKDOWN,
           COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'EIP23b' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
 FROM      $db_output.EIP23a_common A
 WHERE     A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY  A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23b Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b Provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23ba, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23bc'
                  WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23bb'
                  WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ba'
                  END AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS  METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
       WHERE A.days_between_ReferralRequestReceivedDate <= 14   
    GROUP BY A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23b Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b Provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23b metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23b' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
       WHERE A.days_between_ReferralRequestReceivedDate <= 14   
    GROUP BY A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23b National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP23a_Common to make the metrics (for each Ethnicity), and then inserts this into the
 AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-16 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP23b' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
    WHERE A.days_between_ReferralRequestReceivedDate <= 14
    GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23b CCG National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23b metric, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                          */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT    '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
           COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'EIP23b' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
 FROM      $db_output.EIP23a_common A
 WHERE     A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY  A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23b Provider National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23b Provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23b metric, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                          */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider; Ethnicity' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23b' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             A.NHSDEthnicity AS SECONDARY_LEVEL, 
             A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
       WHERE A.days_between_ReferralRequestReceivedDate <= 14   
    GROUP BY A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23c National Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP23a_common 
 to make the national EIP23ca, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23cc'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23cb'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ca'
                 END AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
   WHERE A.days_between_ReferralRequestReceivedDate > 14
  GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP23c National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c national national aggregation - this counts the number of distinct UniqServReqIDs in EIP23a_common 
 to make the national EIP23c metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP23c' AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
   WHERE A.days_between_ReferralRequestReceivedDate > 14

-- COMMAND ----------

-- DBTITLE 1,EIP23c CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c CCG age groups aggregation - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23ca, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23cc'
                       WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23cb'
                       WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ca'
                 END AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common as A
 WHERE A.days_between_ReferralRequestReceivedDate > 14
 GROUP BY A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23c CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c CCG national aggregation - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23c metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23c' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common as A
 WHERE A.days_between_ReferralRequestReceivedDate > 14
 GROUP BY A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23c Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c Provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23ca, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23cc'
                  WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23cb'
                  WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ca'
                  END AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
       WHERE A.days_between_ReferralRequestReceivedDate > 14   
    GROUP BY A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23c Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c Provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23c metric (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23c' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
       WHERE A.days_between_ReferralRequestReceivedDate > 14   
    GROUP BY A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23c National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP23a_Common to make the metrics (for each Ethnicity), and then inserts this into the
 AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-16 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP23c' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
    FROM  $db_output.EIP23a_common A
   WHERE A.days_between_ReferralRequestReceivedDate > 14
  GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23c CCG National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per CCG in EIP23a_common 
 to make the CCG EIP23c metric, and then inserts this into the AWT_unformatted table. 

 Bhabani Sahoo - 2021-03-31                                                                              */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23c' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM $db_output.EIP23a_common as A
 WHERE A.days_between_ReferralRequestReceivedDate > 14
 GROUP BY A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23c Provider National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c Provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in EIP23a_common 
 to make the provider EIP23c metric, and then inserts this into the AWT_unformatted table. 

  Bhabani Sahoo - 2021-03-31                                                                             */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider; Ethnicity' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23c' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             A.NHSDEthnicity AS SECONDARY_LEVEL, 
             A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.EIP23a_common_Prov A
       WHERE A.days_between_ReferralRequestReceivedDate > 14   
    GROUP BY A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23d National Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP23d_common 
 to make the national EIP23ca, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23dc'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23db'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23da'
                 END AS METRIC,
          COALESCE(COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
       GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP23d National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23c national national aggregation - this counts the number of distinct UniqServReqIDs in EIP23d_common 
 to make the national EIP23c metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP23d' AS METRIC,
        COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A

-- COMMAND ----------

-- DBTITLE 1,EIP23d CCG Age Groups
 %sql

 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d CCG age groups aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP23d_common to make CCG EIP23da, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23dc'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23db'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23da'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23d CCG National
 %sql

 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d CCG national aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP23d_common to make CCG EIP23d metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23d' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 GROUP BY		A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23d Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23da, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23dc'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23db'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23da'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common_prov AS A
 GROUP BY		 A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23d Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23d metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23d' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common_prov AS A
 GROUP BY		 A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23d National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP23a_Common to make the metrics (for each Ethnicity), and then inserts this into the
 AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-16 */ 
 /* ---------------------------------------------------------------------------------------------------------*/
           
 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN, 
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP23d' AS METRIC,
           COALESCE(COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
       GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23d CCG National - Ethnicity
 %sql

 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP23d_common to make CCG EIP23d metrics, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23d' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 GROUP BY		A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23d Provider National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23d metrics, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                              */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23d' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common_prov AS A
 GROUP BY		 A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23e  National Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP23d_common 
 to make the national EIP23ea, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23ec'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23eb'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ea'
                 END AS METRIC,
        COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
       WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14      
       GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP23e  National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e national national aggregation - this counts the number of distinct UniqServReqIDs in EIP23d_common 
 to make the national EIP23e metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP23e' AS METRIC,
       COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
       WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14      

-- COMMAND ----------

-- DBTITLE 1,EIP23e CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e CCG age groups aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP23d_common to make CCG EIP23ea, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23ec'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23eb'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ea'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23e CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e CCG national aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP23d_common to make CCG EIP23e metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23e' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14
 GROUP BY A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23e Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23ea, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
       SELECT    '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23ec'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23eb'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23ea'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
       FROM			$db_output.EIP23d_common_prov AS A
       WHERE     A.days_between_endate_ReferralRequestReceivedDate <= 14
       GROUP BY  A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23e Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23e metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
       SELECT    '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23e' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
       FROM			$db_output.EIP23d_common_prov AS A
       WHERE     A.days_between_endate_ReferralRequestReceivedDate <= 14
       GROUP BY  A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23e National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP23d_Common to make the metrics (for each Ethnicity), and then inserts this into the
 AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-17 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN, 
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP23e' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
       WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14      
       GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23e CCG National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP23d_common to make CCG EIP23e metrics and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                             */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23e' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14
 GROUP BY A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23e Provider National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23e provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23e metrics, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                             */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
       SELECT    '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23e' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity AS SECONDARY_LEVEL_DESCRIPTION
                 
       FROM			$db_output.EIP23d_common_prov AS A
       WHERE     A.days_between_endate_ReferralRequestReceivedDate <= 14
       GROUP BY  A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23f National Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f national age groups aggregation - this counts the number of distinct UniqServReqIDs in EIP23d_common 
 to make the national EIP23fa, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP23fc'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23fb'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23fa'
                 END AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
        WHERE A.days_between_endate_ReferralRequestReceivedDate > 14     
        GROUP BY A.AGE_GROUP
        

-- COMMAND ----------

-- DBTITLE 1,EIP23f National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f national national aggregation - this counts the number of distinct UniqServReqIDs in EIP23d_common 
 to make the national EIP23f metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP23f' AS METRIC,
          COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
        WHERE A.days_between_endate_ReferralRequestReceivedDate > 14     

-- COMMAND ----------

-- DBTITLE 1,EIP23f CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f CCG age groups aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP01_common to make CCG EIP23fa, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23fc'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23fb'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23fa'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 WHERE           A.days_between_endate_ReferralRequestReceivedDate > 14
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23f CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f CCG national aggregation - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP01_common to make CCG EIP23f metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23f' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 WHERE           A.days_between_endate_ReferralRequestReceivedDate > 14
 GROUP BY		A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23f Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f provider age groups aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23fa, b and c metrics (for each age bracket), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
     SELECT		'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP23fc'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP23fb'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP23fa'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
       FROM	    $db_output.EIP23d_common_prov AS A
       WHERE     A.days_between_endate_ReferralRequestReceivedDate > 14
       GROUP BY  A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23f Provider Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23f metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
     SELECT		'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23f' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
       FROM	    $db_output.EIP23d_common_prov AS A
       WHERE     A.days_between_endate_ReferralRequestReceivedDate > 14
       GROUP BY  A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23f National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f Ethnicity aggregation - this counts the number of distinct UniqServReqIDs for each Ethnicity in 
 EIP23d_Common to make the metrics (for each Ethnicity), and then inserts this into the
 AWT_unformatted table. 

 Bhabani Sahoo changed dated 2021-02-17 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN, 
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP23f' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
        FROM $db_output.EIP23d_common A
        WHERE A.days_between_endate_ReferralRequestReceivedDate > 14     
        GROUP BY A.NHSDEthnicity
        

-- COMMAND ----------

-- DBTITLE 1,EIP23f CCG National - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f CCG national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs for each CCG in 
 EIP01_common to make CCG EIP23f metrics, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                            */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23f' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP23d_common AS A
 WHERE           A.days_between_endate_ReferralRequestReceivedDate > 14
 GROUP BY		A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23f Provider Groups - Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23f provider national aggregation based on Ethnicity - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23d_common to make the provider EIP23f metrics, and then inserts this into the AWT_unformatted table. 

   Bhabani Sahoo - 2021-03-31                                                                            */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
     SELECT		'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23f' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
       FROM	    $db_output.EIP23d_common_prov AS A
       WHERE     A.days_between_endate_ReferralRequestReceivedDate > 14
       GROUP BY  A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP23g National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23g national national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23g_common to make the provider EIP23g metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
        SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'England' AS BREAKDOWN,
 				'England' as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23g' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
         FROM  global_temp.EIP23g_common A

-- COMMAND ----------

-- DBTITLE 1,EIP23g Provider
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23g provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23g_common to make the provider EIP23g metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23g' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM  global_temp.EIP23g_common A
    GROUP BY A.OrgIDProv
         

-- COMMAND ----------

-- DBTITLE 1,EIP23g CCG
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23g provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23g_common to make the provider EIP23g metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'CCG - GP Practice or Residence' AS BREAKDOWN,
 			COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23g' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM  global_temp.EIP23g_common as A
    GROUP BY A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP23h National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23h national national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23h_common to make the provider EIP23h metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
        SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'England' AS BREAKDOWN,
 				'England' as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP23h' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
         FROM  global_temp.EIP23h_common A

-- COMMAND ----------

-- DBTITLE 1,EIP23h Provider
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23h provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23h_common to make the provider EIP23h metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'Provider' AS BREAKDOWN,
 			A.OrgIDProv as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23h' AS METRIC,          
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM global_temp.EIP23h_common_Prov A
    GROUP BY A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP23h CCG
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23h provider national aggregation - this counts the number of distinct UniqServReqIDs per provider in 
 EIP23h_common to make the provider EIP23h metrics (age agnostic), and then inserts this into the
 AWT_unformatted table. 

   Sam Hollings - 2018-03-12                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
             '$status' AS STATUS,
 			'CCG - GP Practice or Residence' AS BREAKDOWN,
 			COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
             NULL as LEVEL_DESCRIPTION,
             'EIP23h' AS METRIC,
             COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM global_temp.EIP23h_common A
    GROUP BY A.IC_Rec_CCG
    

-- COMMAND ----------

-- DBTITLE 1,EIP23i National, Provider, CCG
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23i - the proportion of referrals on EIP pathway entering treatment within two weeks. This returns 
 national, CCG, and provider-level figures
 EIP23i represents EIP23b metric divided by EIP23a metric, where:
     - EIP23b metric = numhber of referrals on EIP pathway entering treatment within two weeks
     - EIP23a metric = total referrals on EIP pathway entering treatment)
 This is then inserted into the AWT_unformatted table. 

   Laura Markendale - 2019-05-01                                                                           
   Bhabani Sahoo    - 2021-04-01 : Updated the code to fix Ethnicity fix                                     */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             'EIP23i' AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             A.SECONDARY_LEVEL, 
             A.SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND A.METRIC = 'EIP23a' 
             AND B.METRIC = 'EIP23b'

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP23i Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23i age groups - the national proportion of referrals on EIP pathway entering treatment within two weeks.
 This represents three EIP23ba age metrics divided by the equivalent EIP23a age metrics, where:
     - EIP23b age metric = referrals on EIP pathway entering treatment within two weeks
     - EIP23a age metric = total referrals on EIP pathway entering treatment)
 This is then inserted into the AWT_unformatted table. 

   Laura Markendale - 2019-05-01                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             CASE
               WHEN A.METRIC = 'EIP23aa' THEN 'EIP23ia'
               WHEN A.METRIC = 'EIP23ab' THEN 'EIP23ib'
               WHEN A.METRIC = 'EIP23ac' THEN 'EIP23ic'
                 END AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
              AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND (
                   (A.METRIC = 'EIP23aa' AND B.METRIC = 'EIP23ba')
                OR (A.METRIC = 'EIP23ab' AND B.METRIC = 'EIP23bb')
                OR (A.METRIC = 'EIP23ac' AND B.METRIC = 'EIP23bc')
                  )

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP23i : Ethnicity
 %py

 # %sql
 # /* ---------------------------------------------------------------------------------------------------------*/
 # /* EIP23i Ethnicity - the national proportion of referrals on EIP pathway entering treatment within two weeks.
 # This represents EIP23b ethnicity metrics divided by the equivalent EIP23 ethnicity metrics, where:
 #     - EIP23b ethnicity metric = referrals on EIP pathway entering treatment within two weeks
 #     - EIP23a ethnicity metric = total referrals on EIP pathway entering treatment)
 # This is then inserted into the AWT_unformatted table. 

 #   Bhabani Sahoo - 2021-02-18                                                                           
 #   Bhabani Sahoo - 2021-04-01 : Commented this code as it was merged into the main code above                */ 
 # /* ---------------------------------------------------------------------------------------------------------*/

 # /*
 # Insert into $db_output.AWT_unformatted 
 #      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
 #             '$rp_enddate' AS REPORTING_PERIOD_END,
 # 			'$status' AS STATUS,
 # 			A.BREAKDOWN,
 # 			A.LEVEL,
 #             A.LEVEL_DESCRIPTION,
 #             'EIP23i' AS METRIC,
 #             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
 #             '$db_source' AS SOURCE_DB,
 #             A.SECONDARY_LEVEL, 
 #             A.SECONDARY_LEVEL_DESCRIPTION
             
 #        FROM $db_output.AWT_unformatted A
 #  INNER JOIN $db_output.AWT_unformatted B
 #             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
 #             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
 #             AND A.STATUS = B.STATUS
 #             AND A.BREAKDOWN = B.BREAKDOWN
 #             AND A.LEVEL = B.LEVEL
 #             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
 #             AND (A.METRIC = 'EIP23a' AND B.METRIC = 'EIP23b')
 #             AND A.BREAKDOWN = 'England; Ethnicity'
 #             AND A.METRIC = 'EIP23a' 
 #             AND B.METRIC = 'EIP23b'

 # WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
 #             AND A.REPORTING_PERIOD_END = '$rp_enddate'
 #             AND A.STATUS = '$status'
 #             --AND A.BREAKDOWN = 'England; Ethnicity'; */

-- COMMAND ----------

-- DBTITLE 1,EIP23j National, Provider, CCG
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23j - the proportion of referrals on EIP pathway waiting for treatment for more than two weeks. This returns 
 national, CCG, and provider-level figures
 EIP23j represents EIP23f metric divided by EIP23d metric, where:
     - EIP23f metric = numhber of open referrals on EIP pathway at end of reporting period waiting for treatment for over two weeks
     - EIP23d metric = number of open referrals at end of reporting period
 This is then inserted into the AWT_unformatted table. 

   Laura Markendale - 2019-05-01                                                                           
   Bhabani Sahoo    - 2021-04-01 : Updated the code to fix Ethnicity fix                                     */  
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             'EIP23j' AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             A.SECONDARY_LEVEL, 
             A.SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND A.METRIC = 'EIP23d' 
             AND B.METRIC = 'EIP23f'

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP23j Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23j age groups - the proportion of referrals on EIP pathway entering treatment within two weeks.
 EIP23j represents EIP23f metric divided by EIP23d metric, where:
     - EIP23f metric = numhber of referrals on EIP pathway waiting for treatment for more than two weeks
     - EIP23d metric = open referrals at end of reporting period
 This is then inserted into the AWT_unformatted table. 

   Laura Markendale - 2019-05-01                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             CASE
               WHEN A.METRIC = 'EIP23da' THEN 'EIP23ja'
               WHEN A.METRIC = 'EIP23db' THEN 'EIP23jb'
               WHEN A.METRIC = 'EIP23dc' THEN 'EIP23jc'
                 END AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND (
                   (A.METRIC = 'EIP23da' AND B.METRIC = 'EIP23fa')
                OR (A.METRIC = 'EIP23db' AND B.METRIC = 'EIP23fb')
                OR (A.METRIC = 'EIP23dc' AND B.METRIC = 'EIP23fc')
                  )

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP23j : Ethnicity
 %py
 # %sql
 # /* ---------------------------------------------------------------------------------------------------------*/
 # /* EIP23j ethnicity groups - the proportion of referrals on EIP pathway entering treatment within two weeks.
 # EIP23j represents EIP23f metric divided by EIP23d metric, where:
 #     - EIP23f metric = number of referrals on EIP pathway waiting for treatment for more than two weeks
 #     - EIP23d metric = open referrals at end of reporting period
 # This is then inserted into the AWT_unformatted table. 

 #   Bhabani Sahoo - 2021-02-18                                                                          
 #   Bhabani Sahoo - 2021-04-01 : Commented this code as it was merged into the main code above                */ 
 # /* ---------------------------------------------------------------------------------------------------------*/
 # /*
 # Insert into $db_output.AWT_unformatted 
 #      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
 #             '$rp_enddate' AS REPORTING_PERIOD_END,
 # 			'$status' AS STATUS,
 # 			A.BREAKDOWN,
 # 			A.LEVEL,
 #             A.LEVEL_DESCRIPTION,
 #             'EIP23j' AS METRIC,
 #             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
 #             '$db_source' AS SOURCE_DB,
 #             A.SECONDARY_LEVEL, 
 #             A.SECONDARY_LEVEL_DESCRIPTION
             
 #        FROM $db_output.AWT_unformatted A
 #  INNER JOIN $db_output.AWT_unformatted B
 #             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
 #             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
 #             AND A.STATUS = B.STATUS
 #             AND A.BREAKDOWN = B.BREAKDOWN
 #             AND A.LEVEL = B.LEVEL
 #             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
 #             AND (A.METRIC = 'EIP23d' AND B.METRIC = 'EIP23f')
 #             AND A.BREAKDOWN = 'England; Ethnicity'
 #             AND A.METRIC = 'EIP23d' 
 #             AND B.METRIC = 'EIP23f'

 # WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
 #             AND A.REPORTING_PERIOD_END = '$rp_enddate'
 #             AND A.STATUS = '$status'
 #             --AND A.BREAKDOWN = 'England; Ethnicity'

 # */

-- COMMAND ----------

-- DBTITLE 1,ED32 National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP32 national aggregation - this aggregates the EIP32_ED32_common table based on the requirements of EIP32
   across the whole country.

   Sam Hollings - 2018-03-20                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
        SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'England' AS BREAKDOWN,
 				'England' as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'ED32' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
         FROM  $db_output.EIP32_ED32_common A
         WHERE PrimReasonReferralMH = '12'
               AND (AgeServReferRecDate <=18 AND AgeServReferRecDate >=0)

-- COMMAND ----------

-- DBTITLE 1,ED32 CCG
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP32 CCG - this aggregates the EIP32_ED32_common table based on the requirements of EIP32
   for each CCG.

   Sam Hollings - 2018-03-20                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'CCG - GP Practice or Residence' AS BREAKDOWN,
           COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'ED32' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
     FROM  $db_output.EIP32_ED32_common A
     WHERE PrimReasonReferralMH = '12'
           AND (AgeServReferRecDate <=18 AND AgeServReferRecDate >=0)
     GROUP BY A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,ED32 Provider
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP32 Provider - this aggregates the EIP32_ED32_common table based on the requirements of EIP32
   for each Provider.

   Sam Hollings - 2018-03-20                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'Provider' AS BREAKDOWN,
           A.OrgIDProv as LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'ED32' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE,
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
     FROM  $db_output.EIP32_ED32_common A
     WHERE PrimReasonReferralMH = '12'
           AND (AgeServReferRecDate <=18 AND AgeServReferRecDate >=0)
     GROUP BY A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP32 National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* ED32 national aggregation - this aggregates the EIP32_ED32_common table based on the requirements of ED32
   across the whole country.

   Sam Hollings - 2018-03-20                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
        SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'England' AS BREAKDOWN,
 				'England' as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP32' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE,
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
         FROM  $db_output.EIP32_ED32_common A
         WHERE PrimReasonReferralMH = '01'

-- COMMAND ----------

-- DBTITLE 1,EIP32 CCG
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* ED32 CCG - this aggregates the EIP32_ED32_common table based on the requirements of ED32
   for each CCG.

   Sam Hollings - 2018-03-20                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'CCG - GP Practice or Residence' AS BREAKDOWN,
           COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'EIP32' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE,
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
     FROM  $db_output.EIP32_ED32_common A
     WHERE PrimReasonReferralMH = '01'
     GROUP BY A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP32 Provider
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* ED32 Provider - this aggregates the EIP32_ED32_common table based on the requirements of ED32
   for each Provider.

   Sam Hollings - 2018-03-20                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS,
           'Provider' AS BREAKDOWN,
           A.OrgIDProv as LEVEL,
           NULL as LEVEL_DESCRIPTION,
           'EIP32' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
     FROM  $db_output.EIP32_ED32_common A
     WHERE PrimReasonReferralMH = '01'
     GROUP BY A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,MHS32 National
 %sql

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START
 				,'$rp_enddate' AS REPORTING_PERIOD_END
 				,'$status' AS STATUS
 				,'England' AS BREAKDOWN
 				,'England' AS LEVEL
 				, NULL AS LEVEL_DESCRIPTION
                 
 				,'MHS32' AS METRIC
 --				,CAST (ISNULL (COUNT (DISTINCT A.UniqServReqID), 0) AS NVARCHAR) AS METRIC_VALUE
                 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
                 ,'$db_source' AS SOURCE_DB
                 , 'NONE' AS SECONDARY_LEVEL
                 , 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			global_temp.MHS101Referral_LATEST AS A				
 WHERE			ReferralRequestReceivedDate >= '$rp_startdate'
 			AND ReferralRequestReceivedDate <= '$rp_enddate'
 			AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = $month_id) OR A.ServDischDate <= '$rp_enddate')

-- COMMAND ----------

-- DBTITLE 1,MHS32 CCG
 %sql

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START
 				,'$rp_enddate' AS REPORTING_PERIOD_END
 				,'$status' AS STATUS
 				,'CCG - GP Practice or Residence' AS BREAKDOWN
 				,COALESCE(B.IC_Rec_CCG, 'UNKNOWN') as LEVEL
 				, NULL AS LEVEL_DESCRIPTION
                 
 				,'MHS32' AS METRIC
 --				,CAST (ISNULL (COUNT (DISTINCT A.UniqServReqID), 0) AS NVARCHAR) AS METRIC_VALUE
                 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
                 ,'$db_source' AS SOURCE_DB
                 , 'NONE' AS SECONDARY_LEVEL
                 , 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			global_temp.MHS101Referral_LATEST AS A		
       LEFT JOIN $db_output.MHS001_CCG_LATEST AS B ON A.PERSON_ID = B.PERSON_ID
 WHERE			ReferralRequestReceivedDate >= '$rp_startdate'
 			AND ReferralRequestReceivedDate <= '$rp_enddate'
 			AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = $month_id) OR A.ServDischDate <= '$rp_enddate')
 GROUP BY        B.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,MHS32 Provider
 %sql

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START
 				, '$rp_enddate' AS REPORTING_PERIOD_END
 				, '$status' AS STATUS
 				, 'Provider' AS BREAKDOWN
                 , A.OrgIDProv as LEVEL
 				, NULL AS LEVEL_DESCRIPTION                
 				,'MHS32' AS METRIC
 --				,CAST (ISNULL (COUNT (DISTINCT A.UniqServReqID), 0) AS NVARCHAR) AS METRIC_VALUE
                 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
                 ,'$db_source' AS SOURCE_DB
                 , 'NONE' AS SECONDARY_LEVEL
                 , 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			global_temp.MHS101Referral_LATEST AS A	
 WHERE			ReferralRequestReceivedDate >= '$rp_startdate'
 			AND ReferralRequestReceivedDate <= '$rp_enddate'
 			AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = $month_id) OR A.ServDischDate <= '$rp_enddate')
 GROUP BY        A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP63 National age groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 National Age Groups - Denormalised the EIP63 metric data to 
 also include CCG fields - this can be used for the CCG extract and summed to provide national extract.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP63c'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP63b'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP63a'
                 END AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP63_common as A
   GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP63 National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 National National - Denormalised the EIP63 metric data to 
 also include CCG fields - this can be used for the CCG extract and the national extract.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP63' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP63_common as A

-- COMMAND ----------

-- DBTITLE 1,EIP63 CCG age groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 CCG Age Groups - this is the aggregate calculation of EIP63 CCG breakdown. Denormalised the EIP63 metric data to 
 also include CCG fields - this can be used for the CCG extract and summed to provide national extract.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP63c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP63b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP63a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP63_common AS A
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP63 CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 CCG National - this is the aggregate calculation of EIP63 CCG breakdown. Denormalised the EIP63 metric data to 
 also include CCG fields - this can be used for the CCG extract and summed to provide national extract.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP63' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP63_common AS A
 GROUP BY		A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP63 Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 Provider Age groups - this is the aggregate calculation of EIP63 Prov breakdown for the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP63c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP63b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP63a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP63_common AS A
 GROUP BY		 A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP63 Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 Provider National - this is the aggregate calculation of EIP63 Prov breakdown for the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP63' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP63_common AS A
 GROUP BY	    A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP63 National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 National Ethnicity Groups

    Bhabani Sahoo changed dated 2021-02-24 */ 
 /* ---------------------------------------------------------------------------------------------------------*/
  
 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP63' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP63_common as A
   GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP63 CCG National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 CCG National based on ethnicity - this is the aggregate calculation of EIP63 CCG breakdown. 
    Denormalised the EIP63 metric data to also include CCG fields - this can be used for the CCG extract 
    and summed to provide national extract.

   Sam Hollings - 2018-03-6
   Bhabani Sahoo- 2021-04-08                                                                                 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP63' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP63_common AS A
 GROUP BY		A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP63 Provider National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP63 Provider National - this is the aggregate calculation of EIP63 Prov breakdown based on Ethnicity

   Sam Hollings - 2018-03-6
   Bhabani Sahoo- 2021-04-08                                                                                 */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP63' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP63_common AS A
 GROUP BY	    A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP64 National Age Group
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64a/64 National Age Group - This aggregates the EIP64abc_common table to to produce the EIP64 national
   metric  for each age group.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP64c'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP64b'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP64a'
                 END AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP64 National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64a/64 National National - This aggregates the EIP64abc_common table to to produce the EIP64 national
   metric over all age groups.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP64' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A

-- COMMAND ----------

-- DBTITLE 1,EIP64 CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 CCG Age Groups - this is the aggregate calculation of EIP64 CCG breakdown from EIP64abc_common.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP64c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP64b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP64a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP64 CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 CCG National - this is the aggregate calculation of EIP64 CCG breakdown from EIP64abc_common. Across
   all age groups.
   
   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP64' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 GROUP BY		A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP64 Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 Provider Age groups - this is the aggregate calculation of EIP64 Prov breakdown for the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP64c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP64b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP64a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 GROUP BY		 A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP64 Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 Provider National - this is the aggregate calculation of EIP64 Prov breakdown across the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP64' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 GROUP BY		A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP64 National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 National Ethnicity - This aggregates the EIP64abc_common table to to produce the EIP64 national
   metric for each ethnic group.

   Bhabani Sahoo changed dated 2021-02-24                                                                    */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL, 
           'England' as LEVEL_DESCRIPTION,
           'EIP64' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP64 CCG National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 CCG National - this is the aggregate calculation of EIP64 CCG breakdown from EIP64abc_common. Across
   all age groups.
   
   Sam Hollings - 2018-03-6    
   Bhabani Sahoo - 2021-04-08                                                                                */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP64' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 GROUP BY		A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP64 Provider National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64 Provider National - this is the aggregate calculation of EIP64 Prov breakdown based on Ethnicity

   Sam Hollings - 2018-03-6 
   Bhabani Sahoo - 2021-04-08                                                                                */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP64' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 GROUP BY		A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP64b/65 National Age Group
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 National Age Group - This aggregates the EIP64abc_common table to to produce the EIP65 national
   metric for each age group.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select
           '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN,
           'England' AS LEVEL,
            NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP65c'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP65b'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP65a'
                 END AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
            'NONE' AS SECONDARY_LEVEL, 
            'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
   GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP65 National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 National National - This aggregates the EIP64abc_common table to to produce the EIP65 national
   metric over all age groups.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP65' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14

-- COMMAND ----------

-- DBTITLE 1,EIP65 CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 CCG Age Groups - this is the aggregate calculation of EIP64b/65 CCG breakdown from EIP64abc_common.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP65c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP65b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP65a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP65 CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 CCG Age Groups - this is the aggregate calculation of EIP64b/65 CCG breakdown from EIP64abc_common.
   This is across all age groups.
   
   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP65' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
 GROUP BY		A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP65 Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 Provider Age groups - this is the aggregate calculation of EIP65 Prov breakdown for the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP65c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP65b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP65a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
 GROUP BY		 A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP65 Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 Provider National - this is the aggregate calculation of EIP65 Prov breakdown across the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP65' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
 GROUP BY	    A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP65 National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP65 National Ethnicity - This aggregates the EIP64abc_common table to to produce the EIP65 national
   metric for each ethnic group.

   Bhabani Sahoo changed dated 2021-02-24                                                                    */ 
 /* ---------------------------------------------------------------------------------------------------------*/
   
 Insert into $db_output.AWT_unformatted 
   select
           '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL,
           'England' as LEVEL_DESCRIPTION,
           'EIP65' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
   GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP65 CCG National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP65 CCG based on Ethnicity - this is the aggregate calculation of EIP65 CCG breakdown from EIP64abc_common.
     
   Sam Hollings - 2018-03-6    
   Bhabani Sahoo - 2021-04-09                                                                                */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP65' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
 GROUP BY		A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP65 Provider National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64b/65 Provider National - this is the aggregate calculation of EIP65 Prov breakdown across the age groups

   Sam Hollings - 2018-03-6
   Bhabani Sahoo - 2021-04-08                                                                                */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP65' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) > 14
 GROUP BY	    A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP64c/66 National Age Group
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64c/66 National Age Group - This aggregates the EIP64abc_common table to to produce the EIP66 national
   metric for each age group.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           CASE  WHEN A.AGE_GROUP = '35-120' THEN 'EIP66c'
                 WHEN A.AGE_GROUP = '18-34'  THEN 'EIP66b'
                 WHEN A.AGE_GROUP = '00-17'  THEN 'EIP66a'
                 END AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
   GROUP BY A.AGE_GROUP

-- COMMAND ----------

-- DBTITLE 1,EIP64c/66 National National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64c/66 National National - This aggregates the EIP64abc_common table to to produce the EIP66 national
   metric over all age groups.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England' AS BREAKDOWN, 
           'England' AS LEVEL, 
           NULL as LEVEL_DESCRIPTION,
           'EIP66' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           'NONE' AS SECONDARY_LEVEL, 
           'NONE' as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14

-- COMMAND ----------

-- DBTITLE 1,EIP64c/66 CCG Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64c/66 CCG Age Groups - this is the aggregate calculation of EIP64c/66 CCG breakdown from EIP64abc_common.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP66c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP66b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP66a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
 GROUP BY		A.AGE_GROUP, A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP64c/66 CCG National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64c/66 CCG National - this is the aggregate calculation of EIP64c/66 CCG breakdown from EIP64abc_common,
   across all age groups.

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP66' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
 GROUP BY		A.IC_Rec_CCG

-- COMMAND ----------

-- DBTITLE 1,EIP64c/66 Provider Age Groups
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64c/66 Provider Age groups - this is the aggregate calculation of EIP66 Prov breakdown for the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 CASE WHEN A.AGE_GROUP = '35-120' THEN 'EIP66c'
                      WHEN A.AGE_GROUP = '18-34'  THEN 'EIP66b'
                      WHEN A.AGE_GROUP = '00-17'  THEN 'EIP66a'
                      END AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
 GROUP BY		 A.AGE_GROUP, A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP64c/66 Provider National
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP64c/66 Provider Natinal - this is the aggregate calculation of EIP66 Prov breakdown across the age groups

   Sam Hollings - 2018-03-6                                                                           */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP66' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 'NONE' AS SECONDARY_LEVEL, 
                 'NONE' as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
 GROUP BY		A.OrgIDProv

-- COMMAND ----------

-- DBTITLE 1,EIP66 National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP66 National Ethnicity - This aggregates the EIP64abc_common table to to produce the EIP66 national
   metric for each ethnic group.

   Bhabani Sahoo changed dated 2021-02-24                                                                    */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
   select '$rp_startdate' AS REPORTING_PERIOD_START, 
           '$rp_enddate' AS REPORTING_PERIOD_END,
           '$status' AS STATUS, 
           'England; Ethnicity' AS BREAKDOWN,
           'England' AS LEVEL, 
           'England' AS LEVEL_DESCRIPTION,
           'EIP66' AS METRIC,
           COALESCE (COUNT (DISTINCT A.UniqServReqID),0) as METRIC_VALUE, 
           '$db_source' AS SOURCE_DB,
           A.NHSDEthnicity AS SECONDARY_LEVEL, 
           A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
           
   FROM $db_output.EIP64abc_common as A
   WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
   GROUP BY A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP66 CCG National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP66 CCG National - this is the aggregate calculation of EIP64c/66 CCG breakdown from EIP64abc_common,
   based on Ethnicity.

   Sam Hollings - 2018-03-6  
   Bhabani Sahoo - 2021-04-08                                                                                */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'CCG - GP Practice or Residence; Ethnicity' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'UNKNOWN') as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP66' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
 GROUP BY		A.IC_Rec_CCG, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP66 Provider National : Ethnicity
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP66 Provider Natinal - this is the aggregate calculation of EIP66 Prov breakdown across Ethnicity

   Sam Hollings - 2018-03-6                                                                           
   Bhabani Sahoo - 2021-04-08                                                                                */ 
 /* ---------------------------------------------------------------------------------------------------------*/
 Insert into $db_output.AWT_unformatted 
 SELECT			'$rp_startdate' AS REPORTING_PERIOD_START, 
                 '$rp_enddate' AS REPORTING_PERIOD_END,
 				'$status' AS STATUS,
 				'Provider; Ethnicity' AS BREAKDOWN,
 				A.OrgIDProv as LEVEL,
                 NULL as LEVEL_DESCRIPTION,
                 'EIP66' AS METRIC,
 				COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE, 
                 '$db_source' AS SOURCE_DB,
                 A.NHSDEthnicity AS SECONDARY_LEVEL, 
                 A.NHSDEthnicity as SECONDARY_LEVEL_DESCRIPTION
                 
 FROM			$db_output.EIP64abc_common AS A
 WHERE ABS(DATEDIFF (CLOCK_STOP,A.ReferralRequestReceivedDate)) <= 14
 GROUP BY		A.OrgIDProv, A.NHSDEthnicity

-- COMMAND ----------

-- DBTITLE 1,EIP67 - All
 %sql

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             'EIP67' AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             A.SECONDARY_LEVEL, 
             A.SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A 
        JOIN $db_output.AWT_unformatted B 
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND A.METRIC = 'EIP64' 
             AND B.METRIC = 'EIP66'

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP67 - Ethnicity
 %py
 # %sql

 # /* Bhabani Sahoo - 2021-04-08 : Commented this code as it was merged into the main code above */

 # /*
 # Insert into $db_output.AWT_unformatted 
 #      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
 #             '$rp_enddate' AS REPORTING_PERIOD_END,
 # 			'$status' AS STATUS,
 # 			A.BREAKDOWN,
 # 			A.LEVEL,
 #             A.LEVEL_DESCRIPTION,
 #             'EIP67' AS METRIC,
 #             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
 #             '$db_source' AS SOURCE_DB,
 #             A.SECONDARY_LEVEL, 
 #             A.SECONDARY_LEVEL_DESCRIPTION
             
 #        FROM $db_output.AWT_unformatted A 
 #        JOIN $db_output.AWT_unformatted B 
 #             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
 #             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
 #             AND A.STATUS = B.STATUS
 #             AND A.BREAKDOWN = B.BREAKDOWN
 #             AND A.LEVEL = B.LEVEL
 #             AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
 #             AND A.METRIC = 'EIP64' 
 #             AND B.METRIC = 'EIP66'
 #             AND A.BREAKDOWN = 'England; Ethnicity'

 # WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
 #             AND A.REPORTING_PERIOD_END = '$rp_enddate'
 #             AND A.STATUS = '$status'
   
 # */

-- COMMAND ----------

-- DBTITLE 1,EIP67a - All
 %sql

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             'EIP67a' AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
              AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND A.METRIC = 'EIP64a' 
             AND B.METRIC = 'EIP66a'

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP67b - All
 %sql

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             'EIP67b' AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
              AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND A.METRIC = 'EIP64b' 
             AND B.METRIC = 'EIP66b'

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'

-- COMMAND ----------

-- DBTITLE 1,EIP67c - All
 %sql

 Insert into $db_output.AWT_unformatted 
      SELECT '$rp_startdate' AS REPORTING_PERIOD_START, 
             '$rp_enddate' AS REPORTING_PERIOD_END,
 			'$status' AS STATUS,
 			A.BREAKDOWN,
 			A.LEVEL,
             A.LEVEL_DESCRIPTION,
             'EIP67c' AS METRIC,
             (CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE, 
             '$db_source' AS SOURCE_DB,
             'NONE' AS SECONDARY_LEVEL, 
             'NONE' as SECONDARY_LEVEL_DESCRIPTION
             
        FROM $db_output.AWT_unformatted A
  INNER JOIN $db_output.AWT_unformatted B
             ON A.REPORTING_PERIOD_START = B.REPORTING_PERIOD_START
             AND A.REPORTING_PERIOD_END = B.REPORTING_PERIOD_END
             AND A.STATUS = B.STATUS
             AND A.BREAKDOWN = B.BREAKDOWN
             AND A.LEVEL = B.LEVEL
              AND A.SECONDARY_LEVEL = B.SECONDARY_LEVEL
             AND A.METRIC = 'EIP64c' 
             AND B.METRIC = 'EIP66c'

 WHERE       A.REPORTING_PERIOD_START = '$rp_startdate'
             AND A.REPORTING_PERIOD_END = '$rp_enddate'
             AND A.STATUS = '$status'
             AND A.SOURCE_DB = '$db_source'
             AND B.SOURCE_DB = '$db_source'