-- Databricks notebook source
 %py
 # dbutils.widgets.removeAll()

-- COMMAND ----------

-- create widget text db_output default "menh_analysis";
-- create widget text db_source default "mh_pre_pseudo_d1";
-- create widget text month_id default "1434";
-- create widget text rp_startdate default "2019-09-01";
-- create widget text rp_enddate default "2019-09-30";
-- create widget text status default "Performance";

-- CREATE WIDGET TEXT rp_startdate_quarterly DEFAULT "2019-07-01";

-- COMMAND ----------

-- DBTITLE 1,Get widget variables
 %py
 db_output = dbutils.widgets.get("db_output")
 db_source  = dbutils.widgets.get("db_source")
 month_id = dbutils.widgets.get("month_id")
 rp_enddate = dbutils.widgets.get("rp_enddate")
 rp_startdate = dbutils.widgets.get("rp_startdate")
 status = dbutils.widgets.get("status")
 rp_startdate_quarterly = dbutils.widgets.get("rp_startdate_quarterly")
 
 params = {'db_output': db_output, 'db_source': db_source, 'month_id': month_id, 'rp_enddate': rp_enddate, 'rp_startdate': rp_startdate, 'rp_startdate_quarterly': rp_startdate_quarterly, 'status': status}
 
 print(params)

-- COMMAND ----------

-- DBTITLE 1,Calculate remaining dates from current month widget value
 %py
 
 # I've moved the creation of these parameters into the notebook above as I *think* that parameters created in python code can't be used by SQL code in the same notebook...
 
 # from datetime import datetime
 # from dateutil.relativedelta import relativedelta
 
 # params['month_id_1'] = int(params['month_id']) - 2
 # params['month_id_2'] = int(params['month_id']) - 1
 
 # params['rp_startdate_m1'] = rp_startdate_quarterly
 # params['rp_startdate_m2'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-1)).strftime('%Y-%m-%d')
 
 # params['rp_enddate_m1'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(months=-1,days=-1)).strftime('%Y-%m-%d')
 # params['rp_enddate_m2'] = (datetime.strptime(params['rp_startdate'], '%Y-%m-%d') + relativedelta(days=-1)).strftime('%Y-%m-%d')
 
 # print(params)

-- COMMAND ----------

 %md
 Table 1 Prep

-- COMMAND ----------

TRUNCATE TABLE $db_output.AMH03e_prep;

INSERT INTO TABLE $db_output.AMH03e_prep

        SELECT   PRSN.Person_ID
                ,PRSN.IC_Rec_CCG
                ,PRSN.NAME
                ,Region_code
                ,Region_description
                ,STP_code
                ,STP_description
           FROM $db_output.MHS001MPI_latest_month_data AS PRSN 
     INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
                ON PRSN.Person_ID = REF.Person_ID 
      LEFT JOIN $db_output.STP_Region_mapping_post_2018 stp ON
                prsn.IC_Rec_CCG = stp.CCG_code
          WHERE PRSN.AgeRepPeriodEnd >= 18
			    AND PRSN.AgeRepPeriodEnd <= 69
                AND AMHServiceRefEndRP_temp = TRUE

-- COMMAND ----------

 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH03e_prep'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH03e_prep'))

-- COMMAND ----------

TRUNCATE TABLE $db_output.AMH13e_14e_prep;

INSERT INTO TABLE $db_output.AMH13e_14e_prep
        
    SELECT PREP.Person_ID
           ,PREP.IC_Rec_CCG
           ,PREP.NAME
           ,Region_code
           ,Region_description
           ,STP_code
           ,STP_description 
           ,AccommodationStatusDate
           ,SettledAccommodationInd
           ,rank
      FROM $db_output.AMH03e_prep AS PREP
INNER JOIN global_temp.Accomodation_latest AS ACC
		   ON PREP.Person_ID = ACC.Person_ID
     WHERE ACC.AccommodationStatusDate >= DATE_ADD(ADD_MONTHS('$rp_enddate', -12),1)
		   AND ACC.AccommodationStatusDate <= '$rp_enddate'

-- COMMAND ----------

 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH13e_14e_prep'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH13e_14e_prep'))

-- COMMAND ----------

TRUNCATE TABLE $db_output.AMH16e_17e_prep;

INSERT INTO TABLE $db_output.AMH16e_17e_prep
        
    SELECT MPI.Person_ID
           ,MPI.IC_Rec_CCG
           ,MPI.NAME
           ,Region_code
           ,Region_description
           ,STP_code
           ,STP_description
           ,EMP.EmployStatusRecDate
           ,EmployStatus
           ,RANK
      FROM $db_output.MHS001MPI_latest_month_data AS MPI
INNER JOIN $db_output.MHS101Referral_open_end_rp AS REF
		   ON MPI.Person_ID = REF.Person_ID 
INNER JOIN global_temp.EMPLOYMENT_LATEST AS EMP
		   ON MPI.Person_ID = EMP.Person_ID
LEFT JOIN $db_output.STP_Region_mapping_post_2018 AS stp 
           ON MPI.IC_Rec_CCG = stp.CCG_code
           --AND (EMP.EmployStatus = '01' OR EMP.EmployStatus = '1') 
           --AND EMP.RANK = '1'
     WHERE REF.AMHServiceRefEndRP_temp = TRUE
		   AND MPI.AgeRepPeriodEnd >= 18
		   AND MPI.AgeRepPeriodEnd <= 69

-- COMMAND ----------

 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AMH16e_17e_prep'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AMH16e_17e_prep'))

-- COMMAND ----------

 %md
 
 Table 2 Prep

-- COMMAND ----------

-- updated following the advice - 2nd contact date should be used to determine quarter not first contact date...
-- change made is to exclude the CASE statement

CREATE OR REPLACE GLOBAL TEMPORARY VIEW ContPer_Quarterly AS
    SELECT s.Person_ID,
           s.UniqServReqID,
           s.OrgIDProv,
           s.OrgIDComm,
           s.UniqMonthID,
           s.ContID,
           s.ContDate,
           s.AgeCareContDate,
           s.RN1,
           f.ContDate AS ContDate2,
           f.AgeCareContDate AS AgeCareContDate2,   
           s.UniqMonthID
--            CASE WHEN MONTH(f.contDate) BETWEEN 4 AND 6 AND f.UniqMonthID BETWEEN '$month_id'-2 and '$month_id' THEN 'Q1'
--                 WHEN MONTH(f.contDate) BETWEEN 7 AND 9 AND f.UniqMonthID BETWEEN '$month_id'-2 and '$month_id' THEN 'Q2'
--                 WHEN MONTH(f.contDate) BETWEEN 10 AND 12 AND f.UniqMonthID BETWEEN '$month_id'-2 and '$month_id' THEN 'Q3'
--                 WHEN MONTH(f.contDate) BETWEEN 1 AND 3 AND f.UniqMonthID BETWEEN '$month_id'-2 and '$month_id' THEN 'Q4' 
--                 END AS Qtr
           FROM global_temp.FirstPersQtr s
INNER JOIN global_temp.first_contacts f 
           ON ((f.UniqServReqID = s.UniqServReqID AND f.Person_ID = s.Person_ID) 
           OR (s.OrgIDProv = 'DFC' AND f.UniqServReqID = s.UniqServReqID))
     WHERE QtrRN=1

-- COMMAND ----------


-- updated following the advice - 2nd contact date should be used to determine quarter not first contact date...
-- change made is to change the WHERE statement

TRUNCATE TABLE $db_output.CYPFinal_2nd_contact_Quarterly;

INSERT INTO TABLE $db_output.CYPFinal_2nd_contact_Quarterly
     SELECT c.Person_ID,
            c.UniqServReqID,
            c.OrgIDProv,
            c.OrgIDComm,
            c.UniqMonthID,
            c.ContID,
            c.ContDate,
            c.AgeCareContDate,
            c.RN1,
--             c.Qtr,
            c.ContDate2,
            c.AgeCareContDate2
       FROM global_temp.ContPer_Quarterly c
       WHERE UniqMonthID BETWEEN ($month_id_1) AND $month_id
--       WHERE Qtr= CASE WHEN MONTH('$rp_startdate') BETWEEN 4 AND 6 THEN 'Q1'
--                       WHEN MONTH('$rp_startdate') BETWEEN 7 AND 9 THEN 'Q2'
--                       WHEN MONTH('$rp_startdate') BETWEEN 10 AND 12 THEN 'Q3'
--                       WHEN MONTH('$rp_startdate') BETWEEN 1 AND 3 THEN 'Q4'
--                       END

-- COMMAND ----------

 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYPFinal_2nd_contact_Quarterly'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYPFinal_2nd_contact_Quarterly'))

-- COMMAND ----------

 %md
 Table 3 Prep

-- COMMAND ----------

/*** CREATES TABLE OF WARD STAYS AND BED DAYS ***/

CREATE OR REPLACE GLOBAL TEMPORARY VIEW wardstay2 AS

SELECT DISTINCT	 
A.UniqMonthID,
UniqWardStayID,
PRSN.Person_ID,
IC_Rec_CCG,
PRSN.AgeRepPeriodEnd,
CASE 
     --MONTH 1
     WHEN A.UniqMonthID = '$month_id_1' and (StartDateWardStay < '$rp_startdate_m1' AND EndDateWardStay > '$rp_enddate_m1')
                            THEN (DATEDIFF('$rp_enddate_m1','$rp_startdate_m1')+1)
	 WHEN A.UniqMonthID = '$month_id_1' and (StartDateWardStay >= '$rp_startdate_m1' AND EndDateWardStay > '$rp_enddate_m1')
                            THEN (DATEDIFF('$rp_enddate_m1',StartDateWardStay)+1)
	 WHEN A.UniqMonthID = '$month_id_1' and (StartDateWardStay < '$rp_startdate_m1' AND (EndDateWardStay BETWEEN '$rp_startdate_m1' AND '$rp_enddate_m1'))
                            THEN (DATEDIFF(EndDateWardStay,'$rp_startdate_m1'))
	 WHEN A.UniqMonthID = '$month_id_1' and (StartDateWardStay >= '$rp_startdate_m1' AND (EndDateWardStay BETWEEN '$rp_startdate_m1' AND '$rp_enddate_m1'))
                            THEN (DATEDIFF(EndDateWardStay,StartDateWardStay))
	 WHEN A.UniqMonthID = '$month_id_1' and (StartDateWardStay < '$rp_startdate_m1' AND EndDateWardStay IS NULL)
                            THEN (DATEDIFF('$rp_enddate_m1','$rp_startdate_m1')+1)
	 WHEN A.UniqMonthID = '$month_id_1' and (StartDateWardStay >= '$rp_startdate_m1' AND EndDateWardStay IS NULL)
                            THEN (DATEDIFF('$rp_enddate_m1',StartDateWardStay)+1)
     --MONTH 2                       
     WHEN A.UniqMonthID = '$month_id_2' and (StartDateWardStay < '$rp_startdate_m2' AND EndDateWardStay > '$rp_enddate_m2')
                            THEN (DATEDIFF('$rp_enddate_m2','$rp_startdate_m2')+1)
	 WHEN A.UniqMonthID = '$month_id_2' and (StartDateWardStay >= '$rp_startdate_m2' AND EndDateWardStay > '$rp_enddate_m2')
                            THEN (DATEDIFF('$rp_enddate_m2',StartDateWardStay)+1)
	 WHEN A.UniqMonthID = '$month_id_2' and (StartDateWardStay < '$rp_startdate_m2' AND (EndDateWardStay BETWEEN '$rp_startdate_m2' AND '$rp_enddate_m2'))
                            THEN (DATEDIFF(EndDateWardStay,'$rp_startdate_m2'))
	 WHEN A.UniqMonthID = '$month_id_2' and (StartDateWardStay >= '$rp_startdate_m2' AND (EndDateWardStay BETWEEN '$rp_startdate_m2' AND '$rp_enddate_m2'))
                            THEN (DATEDIFF(EndDateWardStay,StartDateWardStay))
	 WHEN A.UniqMonthID = '$month_id_2' and (StartDateWardStay < '$rp_startdate_m2' AND EndDateWardStay IS NULL)
                            THEN (DATEDIFF('$rp_enddate_m2','$rp_startdate_m2')+1)
	 WHEN A.UniqMonthID = '$month_id_2' and (StartDateWardStay >= '$rp_startdate_m2' AND EndDateWardStay IS NULL)
                            THEN (DATEDIFF('$rp_enddate_m2',StartDateWardStay)+1)
     --MONTH 3                       
     WHEN A.UniqMonthID = '$month_id' and (StartDateWardStay < '$rp_startdate' AND EndDateWardStay > '$rp_enddate')
                            THEN (DATEDIFF('$rp_enddate','$rp_startdate')+1)
	 WHEN A.UniqMonthID = '$month_id' and (StartDateWardStay >= '$rp_startdate' AND EndDateWardStay > '$rp_enddate')
                            THEN (DATEDIFF('$rp_enddate',StartDateWardStay)+1)
	 WHEN A.UniqMonthID = '$month_id' and (StartDateWardStay < '$rp_startdate' AND (EndDateWardStay BETWEEN '$rp_startdate' AND '$rp_enddate'))
                            THEN (DATEDIFF(EndDateWardStay,'$rp_startdate'))
	 WHEN A.UniqMonthID = '$month_id' and (StartDateWardStay >= '$rp_startdate' AND (EndDateWardStay BETWEEN '$rp_startdate' AND '$rp_enddate'))
                            THEN (DATEDIFF(EndDateWardStay,StartDateWardStay))
	 WHEN A.UniqMonthID = '$month_id' and (StartDateWardStay < '$rp_startdate' AND EndDateWardStay IS NULL)
                            THEN (DATEDIFF('$rp_enddate','$rp_startdate')+1)
	 WHEN A.UniqMonthID = '$month_id' and (StartDateWardStay >= '$rp_startdate' AND EndDateWardStay IS NULL)
                            THEN (DATEDIFF('$rp_enddate',StartDateWardStay)+1)
   
     END 
     AS BED_DAYS

FROM $db_source.MHS502WardStay 
      AS A
      
LEFT OUTER JOIN $db_source.MHS001MPI 
      AS PRSN
      ON A.Person_ID = PRSN.Person_ID 
      AND A.UniqMonthID = PRSN.UniqMonthID
      AND PRSN.UniqMonthID BETWEEN '$month_id_1' AND '$month_id'
      AND PatMRecInRP = True

LEFT OUTER JOIN $db_output.MHS001_CCG_LATEST 
      AS B
      ON A.Person_ID = B.Person_ID
  
WHERE 
A.UniqMonthID BETWEEN '$month_id_1' AND '$month_id'
AND A.WardType IN ('03', '06')
AND PRSN.AgeRepPeriodEnd BETWEEN 0 AND 17

GROUP BY 
A.UniqMonthID,
UniqWardStayID,
PRSN.Person_ID,
IC_Rec_CCG, 
PRSN.AgeRepPeriodEnd, 
StartDateWardStay, 
EndDateWardStay

-- COMMAND ----------

/*** CCG Figures ***/
CREATE OR REPLACE GLOBAL TEMPORARY VIEW Latest_CCG AS

SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY Person_ID, UniqWardStayID ORDER BY UniqMonthID DESC) AS RN

FROM global_temp.wardstay2

ORDER BY 
Person_ID, 
UniqMonthID

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW CCG_CYP AS

select IC_Rec_CCG, count (distinct Person_ID) AS METRIC_VALUE
from global_temp.Latest_CCG
where RN=1
group by IC_Rec_CCG

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW STP_CYP AS

SELECT 
STP_code, 
COUNT (DISTINCT Person_ID) AS METRIC_VALUE

FROM global_temp.Latest_CCG b

LEFT JOIN $db_output.STP_Region_mapping_post_2018 stp 
      ON b.IC_Rec_CCG = stp.CCG_code
      
WHERE RN=1

GROUP BY STP_code

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW Region_CYP AS

SELECT 
Region_code, 
COUNT (DISTINCT Person_ID) AS METRIC_VALUE

from global_temp.Latest_CCG b

LEFT JOIN $db_output.STP_Region_mapping_post_2018 stp 
      ON b.IC_Rec_CCG = stp.CCG_code
      
WHERE RN=1

GROUP BY Region_code