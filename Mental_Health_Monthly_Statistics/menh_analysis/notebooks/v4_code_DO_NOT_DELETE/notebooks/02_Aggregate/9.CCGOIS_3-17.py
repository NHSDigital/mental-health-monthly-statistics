# Databricks notebook source
 %md 
 # CCGOIS 3.17
  

# COMMAND ----------

 %python
 assert dbutils.widgets.get('db_output')
 assert dbutils.widgets.get('db_source')
 assert dbutils.widgets.get('month_id')
 assert dbutils.widgets.get('rp_enddate')
 assert dbutils.widgets.get('rp_startdate')
 assert dbutils.widgets.get('status')

# COMMAND ----------

 %sql

 INSERT INTO $db_output.CCGOIS_unformatted

 --create or replace temp view CCGOIS as
 (
 SELECT			'$rp_startdate' AS REPORTING_PERIOD
 				,'$status' AS STATUS
 				,CCG.IC_Rec_CCG AS CCG
 				,AgeRepPeriodEnd
 				,CASE	WHEN Gender = '1' THEN 'Male'
 						WHEN Gender = '2' THEN 'Female'
 						ELSE 'Other'
 						END AS Gender
 				,CASE	WHEN CC.AMHCareClustCodeFin IN ('0', '00') 	THEN '0'
 						WHEN CC.AMHCareClustCodeFin IN ('01', '1')  THEN '1'
 						WHEN CC.AMHCareClustCodeFin IN ('02', '2')	THEN '2'
 						WHEN CC.AMHCareClustCodeFin IN ('03', '3')  THEN '3'
 						WHEN CC.AMHCareClustCodeFin IN ('04', '4')	THEN '4'
 						WHEN CC.AMHCareClustCodeFin IN ('05', '5') 	THEN '5'
 						WHEN CC.AMHCareClustCodeFin IN ('06', '6')  THEN '6'
 						WHEN CC.AMHCareClustCodeFin IN ('07', '7') 	THEN '7'
 						WHEN CC.AMHCareClustCodeFin IN ('08', '8') 	THEN '8'
 						WHEN CC.AMHCareClustCodeFin IN ('10')       THEN '10'
 						WHEN CC.AMHCareClustCodeFin IN ('11')      	THEN '11'
 						WHEN CC.AMHCareClustCodeFin IN ('12')       THEN '12'
 						WHEN CC.AMHCareClustCodeFin IN ('13')      	THEN '13'
 						WHEN CC.AMHCareClustCodeFin IN ('14')    	THEN '14'
 						WHEN CC.AMHCareClustCodeFin IN ('15') 		THEN '15'
 						WHEN CC.AMHCareClustCodeFin IN ('16') 		THEN '16'
 						WHEN CC.AMHCareClustCodeFin IN ('17')    	THEN '17'
 						WHEN CC.AMHCareClustCodeFin IN ('18') 		THEN '18'
 						WHEN CC.AMHCareClustCodeFin IN ('19') 		THEN '19'
 						WHEN CC.AMHCareClustCodeFin IN ('20')    	THEN '20'
 						WHEN CC.AMHCareClustCodeFin IN ('21') 		THEN '21'
 						ELSE 'Unknown'
 						END AS CLUSTER
 				,CASE	WHEN CC.AMHCareClustCodeFin IN ('1', '01', '2', '02', '3', '03', '4', '04', '5', '05', '6', '06', '7', '07', '8', '08') THEN 'Non-Psychotic'
 						WHEN CC.AMHCareClustCodeFin IN ('10', '11', '12', '13', '14', '15', '16', '17')             							THEN 'Psychosis'
 						WHEN CC.AMHCareClustCodeFin IN ('18', '19', '20', '21')                                             					THEN 'Organic'
 						WHEN CC.AMHCareClustCodeFin IN ('0','00')    						                                                    THEN 'Variance'
 						ELSE 'Unknown' 
                         END AS SUPER_CLUSTER
 				,CASE	WHEN EmployStatus IN ('1', '01')    THEN 'Employed'
 						WHEN EmployStatus IS NULL   		THEN 'Not Recorded'
 						ELSE 'Other'
 						END AS EMPLOYMENT_STATUS
 				,COALESCE (COUNT (DISTINCT PRSN.person_id), 0) AS METRIC_VALUE
                 ,'$db_source' AS SOURCE_DB
 FROM			
                 (				
                 SELECT DISTINCT PRSN.person_id
 								,PRSN.AgeRepPeriodEnd
 								,PRSN.Gender
 				FROM		    $db_source.MHS001MPI AS PRSN
 				LEFT JOIN       $db_source.MHS101Referral AS REF
 								ON PRSN.person_id = REF.person_id AND REF.uniqmonthid = '$month_id' 
 				WHERE		    PRSN.uniqmonthid = '$month_id' 
 						        AND (REF.ServDischDate IS NULL OR REF.ServDischDate > '$rp_enddate')
                                 AND PRSN.PatMRecInRP = TRUE 
                                 AND PRSN.AgeRepPeriodEnd >= 18
                                 AND PRSN.AgeRepPeriodEnd <= 69
                 ) AS PRSN
 LEFT OUTER JOIN global_temp.Employment_Latest AS EMP
 				ON PRSN.person_id = EMP.person_id
                 AND RANK = 1
 LEFT OUTER JOIN global_temp.CCG AS CCG
 				ON PRSN.person_id = CCG.person_id
       LEFT JOIN global_temp.MHS803UniqIDMAX mx
                 ON PRSN.person_id = mx.person_id
       LEFT JOIN $db_source.MHS803CareCluster AS CC
 				ON mx.person_id = CC.person_id 
                 AND mx.MHS803UniqIDMAX = CC.MHS803UniqID	
 GROUP BY		CCG.IC_Rec_CCG
 				,AgeRepPeriodEnd
 				,Gender
 				,CASE	WHEN CC.AMHCareClustCodeFin IN ('0', '00') 	THEN '0'
 						WHEN CC.AMHCareClustCodeFin IN ('01', '1')  THEN '1'
 						WHEN CC.AMHCareClustCodeFin IN ('02', '2')	THEN '2'
 						WHEN CC.AMHCareClustCodeFin IN ('03', '3')  THEN '3'
 						WHEN CC.AMHCareClustCodeFin IN ('04', '4')	THEN '4'
 						WHEN CC.AMHCareClustCodeFin IN ('05', '5') 	THEN '5'
 						WHEN CC.AMHCareClustCodeFin IN ('06', '6')  THEN '6'
 						WHEN CC.AMHCareClustCodeFin IN ('07', '7') 	THEN '7'
 						WHEN CC.AMHCareClustCodeFin IN ('08', '8') 	THEN '8'
 						WHEN CC.AMHCareClustCodeFin IN ('10')       THEN '10'
 						WHEN CC.AMHCareClustCodeFin IN ('11')      	THEN '11'
 						WHEN CC.AMHCareClustCodeFin IN ('12')       THEN '12'
 						WHEN CC.AMHCareClustCodeFin IN ('13')      	THEN '13'
 						WHEN CC.AMHCareClustCodeFin IN ('14')    	THEN '14'
 						WHEN CC.AMHCareClustCodeFin IN ('15') 		THEN '15'
 						WHEN CC.AMHCareClustCodeFin IN ('16') 		THEN '16'
 						WHEN CC.AMHCareClustCodeFin IN ('17')    	THEN '17'
 						WHEN CC.AMHCareClustCodeFin IN ('18') 		THEN '18'
 						WHEN CC.AMHCareClustCodeFin IN ('19') 		THEN '19'
 						WHEN CC.AMHCareClustCodeFin IN ('20')    	THEN '20'
 						WHEN CC.AMHCareClustCodeFin IN ('21') 		THEN '21'
 						ELSE 'Unknown'
 						END
 				,CASE	WHEN CC.AMHCareClustCodeFin IN ('1', '01', '2', '02', '3', '03', '4', '04', '5', '05', '6', '06', '7', '07', '8', '08') THEN 'Non-Psychotic'
 						WHEN CC.AMHCareClustCodeFin IN ('10', '11', '12', '13', '14', '15', '16', '17')             							THEN 'Psychosis'
 						WHEN CC.AMHCareClustCodeFin IN ('18', '19', '20', '21')                                             					THEN 'Organic'
 						WHEN CC.AMHCareClustCodeFin IN ('0','00')    						                                                    THEN 'Variance'
 						ELSE 'Unknown' 
                         END
 				,CASE	WHEN EmployStatus IN ('1', '01')    THEN 'Employed'
 						WHEN EmployStatus IS NULL   		THEN 'Not Recorded'
 						ELSE 'Other'
 						END
 )