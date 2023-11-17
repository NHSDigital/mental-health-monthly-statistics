-- Databricks notebook source
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