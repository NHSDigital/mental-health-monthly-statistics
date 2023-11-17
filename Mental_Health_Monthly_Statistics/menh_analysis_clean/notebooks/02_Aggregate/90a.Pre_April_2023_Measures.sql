-- Databricks notebook source
%sql
-- Outpatient-Other measures
INSERT INTO $db_output.Main_monthly_metric_values VALUES 
    ('CCR70', 'New Emergency Referrals to Crisis Care teams in the Reporting Period')
  , ('CCR70a', 'New Emergency Referrals to Crisis Care teams in the Reporting Period, Aged 18 and over')
  , ('CCR70b', 'New Emergency Referrals to Crisis Care teams in the Reporting Period, Aged under 18')
  , ('CCR71', 'New Urgent Referrals to Crisis Care teams in the Reporting Period')
  , ('CCR71a', 'New Urgent Referrals to Crisis Care teams in the Reporting Period, Aged 18 and over')
  , ('CCR71b', 'New Urgent Referrals to Crisis Care teams in the Reporting Period, Aged under 18')
  , ('CCR72', 'New Emergency Referrals to Crisis Care teams in the Reporting Period with first face to face contact')
  , ('CCR72a', 'New Emergency Referrals to Crisis Care teams in the Reporting Period, with first face to face contact. Aged 18 and over')
  , ('CCR72b', 'New Emergency Referrals to Crisis Care teams in the Reporting Period, with first face to face contact. Aged under 18')
  , ('CCR73', 'New Urgent Referrals to Crisis Care teams in the Reporting Period with first face to face contact')
  , ('CCR73a', 'New Urgent Referrals to Crisis Care teams in the Reporting Period, with first face to face contact. Aged 18 and over')
  , ('CCR73b', 'New Urgent Referrals to Crisis Care teams in the Reporting Period, with first face to face contact. Aged under 18');

-- COMMAND ----------

%sql
--CCR70/CCR71 Intermediate National/CCG
CREATE OR REPLACE GLOBAL TEMP VIEW CCR7071_prep AS
             SELECT MPI.IC_Rec_CCG 
                    ,MPI.NAME
                    ,REF.ClinRespPriorityType
                    ,CASE WHEN REF.AgeServReferRecDate BETWEEN 0 and 17 THEN '0-17'
                          WHEN REF.AgeServReferRecDate >=18 THEN '18 and over' 
                          END AS AGE_GROUP
                     ,REF.UniqServReqID
                FROM $db_output.MHS001MPI_latest_month_data AS MPI
          INNER JOIN $db_source.MHS101Referral AS REF 
                     ON MPI.Person_ID = REF.Person_ID
                     AND REF.UniqMonthID = '$month_id' 
          INNER JOIN $db_source.MHS102ServiceTypeReferredTo AS REFTO
                     ON REF.UniqServReqID = REFTO.UniqServReqID 
                     AND REFTO.UniqMonthID = '$month_id' 
                     
          INNER JOIN $db_output.validcodes as vc
                     ON vc.tablename = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'CCR7071_prep' and vc.type = 'include' and REFTO.ServTeamTypeRefToMH = vc.ValidValue 
                      and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
                      
               WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
--                      AND REFTO.ServTeamTypeRefToMH IN ('A02','A03');

-- COMMAND ----------

%sql
--CCR70/CCR71 Intermediate Provider
CREATE OR REPLACE GLOBAL TEMP VIEW CCR7071_prep_prov AS
             SELECT REF.OrgIDProv
                    ,REF.ClinRespPriorityType
                    ,CASE WHEN REF.AgeServReferRecDate BETWEEN 0 and 17 THEN '0-17'
                          WHEN REF.AgeServReferRecDate >=18 THEN '18 and over' 
                          END AS AGE_GROUP
                     ,REF.UniqServReqID
                FROM $db_source.MHS101Referral AS REF 
          
          INNER JOIN $db_source.MHS102ServiceTypeReferredTo AS REFTO
                     ON REF.UniqServReqID = REFTO.UniqServReqID 
                     AND REFTO.UniqMonthID = '$month_id' 
                     AND REF.OrgIDProv = REFTO.OrgIDProv
                     
         INNER JOIN $db_output.validcodes as vc
                     ON vc.tablename = 'MHS102ServiceTypeReferredTo' and vc.field = 'ServTeamTypeRefToMH' and vc.Measure = 'CCR7071_prep' and vc.type = 'include' and REFTO.ServTeamTypeRefToMH = vc.ValidValue 
                      and $month_id >= vc.FirstMonth and (vc.LastMonth is null or $month_id <= vc.LastMonth)
                      
               WHERE REF.ReferralRequestReceivedDate BETWEEN '$rp_startdate'  AND '$rp_enddate'
--                      AND REFTO.ServTeamTypeRefToMH IN ('A02','A03')
                     AND REF.UniqMonthID = '$month_id' ;
                     

-- COMMAND ----------

%sql
--CCR72/CCR73 Intermediate National/CCG
CREATE OR REPLACE GLOBAL TEMP VIEW CCR7273_prep AS
             SELECT CCR.IC_Rec_CCG 
                    ,CCR.NAME
                    ,CCR.ClinRespPriorityType
                    ,CCR.AGE_GROUP
                    ,CCR.UniqServReqID
                FROM global_temp.CCR7071_prep AS CCR
          INNER JOIN $db_source.MHS201CareContact CON 
                     ON CCR.UniqServReqID = CON.UniqServReqID 
                     AND CON.UniqMonthID = '$month_id'

               WHERE CON.CareContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
                     AND CON.AttendOrDNACode in ('5','6')
                     AND CON.ConsMechanismMH = '01';                  

-- COMMAND ----------

%sql
--CCR72/CCR73 Intermediate Provider
CREATE OR REPLACE GLOBAL TEMP VIEW CCR7273_prep_prov AS 
             SELECT CCR.OrgIDProv
                    ,CCR.ClinRespPriorityType
                    ,CCR.AGE_GROUP
                    ,CCR.UniqServReqID
                FROM global_temp.CCR7071_prep_prov AS CCR
          INNER JOIN $db_source.MHS201CareContact CON 
                     ON CCR.UniqServReqID = CON.UniqServReqID 
                     AND CON.UniqMonthID = '$month_id'
                     AND CCR.OrgIDProv = CON.OrgIDProv 
               WHERE CON.CareContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
                     AND CON.AttendOrDNACode in ('5','6')
                     AND CON.ConsMechanismMH = '01';      

-- COMMAND ----------

%sql
--CCR70 National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR70' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

-- COMMAND ----------

%sql
--CCR70a National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR70a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--       WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '18 and over';

-- COMMAND ----------

%sql
--CCR70b National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR70b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

            WHERE AGE_GROUP = '0-17';

-- COMMAND ----------

%sql
--CCR71 National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR71' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '2';

-- COMMAND ----------

%sql
--CCR71a National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR71a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '18 and over';
            

-- COMMAND ----------

%sql
--CCR71b National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR71b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '0-17';

-- COMMAND ----------

%sql
--CCR72 National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR72' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

-- COMMAND ----------

%sql
--CCR72a National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR72a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

            WHERE AGE_GROUP = '18 and over';

-- COMMAND ----------

%sql
--CCR72b National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR72b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

            WHERE AGE_GROUP = '0-17';

-- COMMAND ----------

%sql
--CCR73 National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR73' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep
      WHERE ClinRespPriorityType = '2';

-- COMMAND ----------

%sql
--CCR73a National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR73a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '18 and over';

-- COMMAND ----------

%sql
--CCR73b National
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
			,'$status' AS STATUS
			,'England' AS BREAKDOWN
			,'England' AS PRIMARY_LEVEL
			,'England' AS PRIMARY_LEVEL_DESCRIPTION
			,'NONE'	AS SECONDARY_LEVEL
			,'NONE'	AS SECONDARY_LEVEL_DESCRIPTION
			,'CCR73b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '0-17';

-- COMMAND ----------

%sql
--CCR70 CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR70' AS METRIC
		   ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
      FROM  global_temp.CCR7071_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--        WHERE ClinRespPriorityType = '1'
  GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql
--CCR70a CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR70a' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7071_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--        WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '18 and over'
   GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql
--CCR70b CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR70b' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7071_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--         WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '0-17'
   GROUP BY IC_Rec_CCG, NAME;  
          

-- COMMAND ----------

%sql
--CCR71 CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR71' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '2'
   GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql
--CCR71a CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR71a' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '18 and over'
   GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql
--CCR71b CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR71b' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7071_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '0-17'
   GROUP BY IC_Rec_CCG, NAME;      

-- COMMAND ----------

%sql
--CCR72 CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR72' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7273_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--           WHERE ClinRespPriorityType = '1'
   GROUP BY IC_Rec_CCG, NAME;    

-- COMMAND ----------

%sql
--CCR72a CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR72a' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
      FROM global_temp.CCR7273_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--           WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '18 and over'
   GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql
--CCR72b CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR72b' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
      FROM global_temp.CCR7273_prep prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--       WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '0-17'
   GROUP BY IC_Rec_CCG, NAME;

-- COMMAND ----------

%sql
--CCR73 CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR73' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7273_prep
      WHERE ClinRespPriorityType = '2'
   GROUP BY IC_Rec_CCG, NAME;    

-- COMMAND ----------

%sql
--CCR73a CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR73a' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7273_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '18 and over'
   GROUP BY IC_Rec_CCG, NAME;    

-- COMMAND ----------

%sql
--CCR73b CCG
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
           ,'$rp_enddate' AS REPORTING_PERIOD_END
	       ,'$status' AS STATUS
	       ,'CCG - GP Practice or Residence' AS BREAKDOWN
	       ,IC_Rec_CCG AS PRIMARY_LEVEL
	       ,NAME AS PRIMARY_LEVEL_DESCRIPTION
	       ,'NONE' AS SECONDARY_LEVEL
	       ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
		   ,'CCR73b' AS METRIC
           ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
           ,'$db_source' AS SOURCE_DB
           
       FROM global_temp.CCR7273_prep
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '0-17'
   GROUP BY IC_Rec_CCG, NAME;     

-- COMMAND ----------

%sql
--CCR70 Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR70' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep_prov prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--         WHERE ClinRespPriorityType = '1'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR70a Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR70a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep_prov prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--         WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '18 and over'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR70b Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR70b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep_prov prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--         WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '0-17'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR71 Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR71' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep_prov
      WHERE ClinRespPriorityType = '2'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR71a Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR71a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep_prov
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '18 and over'  
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR71b Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR71b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7071_prep_prov
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '0-17'  
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR72 Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR72' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep_prov prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--         WHERE ClinRespPriorityType = '1'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR72a Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR72a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep_prov prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--         WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '18 and over'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR72b Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR72b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep_prov prep
       
       INNER JOIN $db_output.validcodes as vc
        ON vc.tablename = 'mhs101referral' and vc.field = 'ClinRespPriorityType' and vc.Measure = 'CCR70_72' and vc.type = 'include' and prep.ClinRespPriorityType = vc.ValidValue 
        and '$month_id' >= vc.FirstMonth and (vc.LastMonth is null or '$month_id' <= vc.LastMonth)

--        WHERE ClinRespPriorityType = '1'
            WHERE AGE_GROUP = '0-17'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR73 Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR73' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep_prov
      WHERE ClinRespPriorityType = '2'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR73a Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR73a' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep_prov
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '18 and over'
   GROUP BY OrgIDProv;

-- COMMAND ----------

%sql
--CCR73b Provider
INSERT INTO $db_output.Main_monthly_unformatted
    SELECT '$rp_startdate' AS REPORTING_PERIOD_START
            ,'$rp_enddate' AS REPORTING_PERIOD_END
		    ,'$status' AS STATUS
		    ,'Provider' AS BREAKDOWN
            ,OrgIDProv	AS PRIMARY_LEVEL
            ,'NONE' AS PRIMARY_LEVEL_DESCRIPTION
		    ,'NONE' AS SECONDARY_LEVEL
            ,'NONE' AS SECONDARY_LEVEL_DESCRIPTION
            ,'CCR73b' AS METRIC
            ,COUNT(DISTINCT UniqServReqID) as METRIC_VALUE
            ,'$db_source' AS SOURCE_DB
            
       FROM global_temp.CCR7273_prep_prov
      WHERE ClinRespPriorityType = '2'
            AND AGE_GROUP = '0-17'
   GROUP BY OrgIDProv;