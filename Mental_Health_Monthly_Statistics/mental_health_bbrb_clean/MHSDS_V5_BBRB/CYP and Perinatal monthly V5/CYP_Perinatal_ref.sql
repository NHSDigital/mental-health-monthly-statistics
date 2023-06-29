-- Databricks notebook source
--  %sql
--  DROP TABLE IF EXISTS $db_output.cyp_org_daily;
--  CREATE TABLE $db_output.CYP_ORG_DAILY USING DELTA AS 
--  SELECT DISTINCT ORG_CODE,
--                  NAME,
--                  ORG_TYPE_CODE,
--                  ORG_OPEN_DATE, 
--                  ORG_CLOSE_DATE, 
--                  BUSINESS_START_DATE, 
--                  BUSINESS_END_DATE
--             FROM $ref_database.org_daily
--            WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
--                  AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
--                  AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
--                  AND ORG_OPEN_DATE <= '$rp_enddate' 

-- COMMAND ----------

--  %sql
--  DROP TABLE IF EXISTS $db_output.cyp_org_relationship_daily;
--  CREATE TABLE $db_output.cyp_ORG_RELATIONSHIP_DAILY USING DELTA AS 
--  SELECT 
--  REL_TYPE_CODE,
--  REL_FROM_ORG_CODE,
--  REL_TO_ORG_CODE, 
--  REL_OPEN_DATE,
--  REL_CLOSE_DATE
--  FROM 
--  $ref_database.org_relationship_daily
--  WHERE
--  (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
--  AND REL_OPEN_DATE <= '$rp_enddate'

-- COMMAND ----------

--  %sql
--  DROP TABLE IF EXISTS $db_output.cyp_stp_mapping;
--  CREATE TABLE $db_output.CYP_STP_MAPPING USING DELTA AS 
--  -- CREATE or replace global view CMH_STP_MAPPING AS 
--  SELECT 
--  A.ORG_CODE as STP_CODE, 
--  A.NAME as STP_NAME, 
--  C.ORG_CODE as CCG_CODE, 
--  C.NAME as CCG_NAME,
--  E.ORG_CODE as REGION_CODE,
--  E.NAME as REGION_NAME
--  FROM 
--  $db_output.CYP_ORG_DAILY A
--  LEFT JOIN $db_output.CYP_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
--  LEFT JOIN $db_output.CYP_ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
--  LEFT JOIN $db_output.CYP_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
--  LEFT JOIN $db_output.CYP_ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
--  WHERE
--  A.ORG_TYPE_CODE = 'ST'
--  AND B.REL_TYPE_CODE is not null
--  ORDER BY 1

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.CCG_MAPPING_2021;
CREATE TABLE IF NOT EXISTS $db_output.CCG_MAPPING_2021 

(
CCG_UNMAPPED STRING, 
CCG21CDH STRING, 
CCG21NM STRING, 
STP21CDH STRING, 
STP21NM STRING,  
NHSER21CDH STRING, 
NHSER21NM STRING
) USING DELTA

-- COMMAND ----------

TRUNCATE TABLE $db_output.CCG_MAPPING_2021

-- COMMAND ----------

--  %sql
--  insert into $db_output.CCG_MAPPING_2021
--  (select CCG_CODE as CCG_UNMAPPED
--         ,CCG_CODE as  CCG21CDH
--         ,ccg_name as CCG21NM 
--         ,stp_code as STP21CDH 
--         ,stp_name as STP21NM
--         ,region_code as NHSER21CDH 
--         ,region_name as NHSER21NM  
--  from $db_output.cyp_stp_mapping stp_map
--  order by ccg_code
--  )

-- COMMAND ----------

--  %sql
--  insert into $db_output.CCG_MAPPING_2021 
--  values
--  ('UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN')

-- COMMAND ----------

DROP TABLE IF EXISTS $db_output.MH_ASS;
CREATE TABLE IF NOT EXISTS $db_output.MH_ASS 

(Category STRING
,Assessment_Tool_Name STRING
,Preferred_Term_SNOMED STRING
,Active_Concept_ID_SNOMED BIGINT
,SNOMED_Version STRING
,Lower_Range INT
,Upper_Range INT
,CYPMH STRING
,EIP STRING 
,Rater STRING
) USING DELTA

-- COMMAND ----------

INSERT INTO $db_output.MH_ASS

VALUES
('PROM','Brief Parental Self Efficacy Scale (BPSES)','Brief Parental Self-Efficacy Scale score','961031000000108','v2','5','25','','',''),
('PREM','Child Group Session Rating Score (CGSRS)','Child Group Session Rating Scale score','718431003','v2','0','40','','',''),
('PROM','Child Outcome Rating Scale (CORS)','Child Outcome Rating Scale total score','718458005','v2','0','40','Y','','Self'),
('PREM','Child Session Rating Scale (CSRS)','Child Session Rating Scale score','718762000','v2','0','40','','',''),
('PROM','Childrens Revised Impact of Event Scale (8) (CRIES 8)','Revised Child Impact of Events Scale score','718152002','v2','0','40','','',''),
('CROM','Childrens Global Assessment Scale (CGAS)','Childrens global assessment scale score','860591000000104','v2','1','100','Y','','Clinician'),
('PROM','Clinical Outcomes in Routine Evaluation 10 (CORE 10)','Clinical Outcomes in Routine Evaluation - 10 clinical score','718583008','v2','0','40','Y','','Self'),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 1 score - I feel that the people who have seen my child listened to me','1035681000000101','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 2 score - it was easy to talk to the people who have seen my child','1035711000000102','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 3 score - I was treated well by the people who have seen my child','1035721000000108','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 4 score - my views and worries were taken seriously','1035731000000105','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 5 score - I feel the people here know how to help with the problem I came for','1035741000000101','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 6 score - I have been given enough explanation about the help available here','1035761000000100','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 7 score - I feel that the people who have seen my child are working together to help with the problem(s)','1035771000000107','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 8 score - the facilities here are comfortable (e.g. waiting area)','1035781000000109','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 9 score - the appointments are usually at a convenient time (e.g. do not interfere with work, school)','1035791000000106','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 10 score - it is quite easy to get to the place where the appointments are','1035801000000105','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 11 score - if a friend needed similar help, I would recommend that he or she come here','1035811000000107','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 12 score - overall, the help I have received here is good','1035821000000101','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 1 score - I feel that the people who saw me listened to me','1035861000000109','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 2 score - it was easy to talk to the people who saw me','1035871000000102','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 3 score - I was treated well by the people who saw me','1035881000000100','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 4 score - my views and worries were taken seriously','1035891000000103','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 5 score - I feel the people here know how to help me','1035901000000102','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 6 score - I have been given enough explanation about the help available here','1035911000000100','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 7 score - I feel that the people who have seen me are working together to help me','1035921000000106','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 8 score - the facilities here are comfortable (e.g. waiting area)','1035931000000108','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 9 score - my appointments are usually at a convenient time (e.g. do not interfere with school, clubs, college, work)','1035941000000104','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 10 score - it is quite easy to get to the place where I have my appointments','1035951000000101','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 11 score - if a friend needed this sort of help, I would suggest to them to come here','1035961000000103','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 12 score - overall, the help I have received here is good','1035971000000105','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 1 score - did the people who saw you listen to you?','1035981000000107','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 2 score - was it easy to talk to the people who saw you?','1035991000000109','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 3 score - how were you treated by the people who saw you?','1036001000000108','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 4 score - were your views and worries taken seriously?','1036011000000105','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 5 score - do you feel that the people here know how to help you?','1036021000000104','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 6 score - were you given enough explanation about the help available here?','1036031000000102','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 7 score - do you feel that the people here are working together to help you?','1036041000000106','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 8 score - the facilities here (like the waiting area) are','1036061000000107','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 9 score - the time of my appointments was','1036051000000109','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 10 score - the place where I had my appointments was','1036071000000100','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 11 score - if a friend needed this sort of help, do you think they should come here?','1036081000000103','v2','1','3','','',''),
('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 12 score - has the help you got here been good?','1036091000000101','v2','1','3','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech frequency and duration score','718889002','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech global rating scale score','718918004','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech level of distress score','718894002','v2','0','100','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas frequency and duration score','718885008','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas global rating scale score','718892003','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas level of distress score','718915001','v2','0','100','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities frequency and duration score','718891005','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities global rating scale score','718917009','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities level of distress score','718886009','v2','0','100','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content frequency and duration score','718884007','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content global rating scale score','718888005','v2','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content level of distress score','718887000','v2','0','100','','',''),
('CROM','Current View ','Current View Contextual Problems score - community','987191000000101','v2','0','3','','',''),
('CROM','Current View ','Current View Contextual Problems score - home','987201000000104','v2','0','3','','',''),
('CROM','Current View ','Current View Contextual Problems score - school, work or training','987211000000102','v2','0','3','','',''),
('CROM','Current View ','Current View Contextual Problems score - service engagement','987221000000108','v2','0','3','','',''),
('CROM','Current View ','Current View Education,Employment,Training score - attainment difficulties','987231000000105','v2','0','3','','',''),
('CROM','Current View ','Current View Education,Employment,Training score - attendance difficulties','987241000000101','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 1 score - anxious away from caregivers','987251000000103','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 2 score - anxious in social situations','987261000000100','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 3 score - anxious generally','987271000000107','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 4 score - compelled to do or think things','987281000000109','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 5 score - panics','987291000000106','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 6 score - avoids going out','987301000000105','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 7 score - avoids specific things','987311000000107','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 8 score - repetitive problematic behaviours','987321000000101','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 9 score - depression/low mood','987331000000104','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 10 score - self-harm','987341000000108','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 11 score - extremes of mood','987351000000106','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 12 score - delusional beliefs and hallucinations','987361000000109','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 13 score - drug and alcohol difficulties','987371000000102','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 14 score - difficulties sitting still or concentrating','987381000000100','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 15 score - behavioural difficulties','987391000000103','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 16 score - poses risk to others','987401000000100','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 17 score - carer management of CYP (child or young person) behaviour','987411000000103','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 18 score - doesnt get to toilet in time','987421000000109','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 19 score - disturbed by traumatic event','987431000000106','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 20 score - eating issues','987441000000102','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 21 score - family relationship difficulties','987451000000104','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 22 score - problems in attachment to parent or carer','987461000000101','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 23 score - peer relationship difficulties','987471000000108','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 24 score - persistent difficulties managing relationships with others','987481000000105','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 25 score - does not speak','987491000000107','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 26 score - gender discomfort issues','987501000000101','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 27 score - unexplained physical symptoms','987511000000104','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 28 score - unexplained developmental difficulties','987521000000105','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 29 score - self-care issues','987531000000107','v2','0','3','','',''),
('CROM','Current View ','Current View Provisional Problem Description item 30 score - adjustment to health issues','987541000000103','v2','0','3','','',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 1 score - how satisfied are you with your mental health','1037651000000100','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 2 score - how satisfied are you with your physical health','1037661000000102','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 3 score - how satisfied are you with your job situation','1037671000000109','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 4 score - how satisfied are you with your accommodation','1037681000000106','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 5 score - how satisfied are you with your leisure activities','1037691000000108','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 6 score - how satisfied are you with your friendships','1037701000000108','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 7 score - how satisfied are you with your partner/family','1037711000000105','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 8 score - how satisfied are you with your personal safety','1037721000000104','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 9 score - how satisfied are you with your medication','1037731000000102','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 10 score - how satisfied are you with the practical help you receive','1037741000000106','v2','0','8','','Y',''),
('PROM','DIALOG','DIALOG patient rated outcome measure item 11 score - how satisfied are you with consultations with mental health professionals','1037751000000109','v2','0','8','','Y',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - eating concern subscale score','959601000000103','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - global score','959611000000101','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - restraint subscale score','959621000000107','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - shape concern subscale score','959631000000109','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - weight concern subscale score','959641000000100','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire eating concern subscale score','473345001','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire global score','446826001','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire restraint subscale score','473348004','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire shape concern subscale score','473346000','v2','0','6','','',''),
('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire weight concern subscale score','473347009','v2','0','6','','',''),
('PROM','Genralised Anxiety Disorder 7 (GAD-7)','Generalized anxiety disorder 7 item score','445455005','v2','0','21','Y','','Self'),
('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal score','959951000000108','v2','0','10','','',''),
('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal 1 score','1034351000000101','v2','0','10','Y','','Goals'),
('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal 2 score','1034361000000103','v2','0','10','Y','','Goals'),
('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal 3 score','1034371000000105','v2','0','10','Y','','Goals'),
('PREM','Group Session Rating Scale (GSRS)','Group Session Rating Scale score','718441000','v2','0','40','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 1 score - active disturbance of social behaviour (observable entity)','1052961000000108','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 2 score - self directed injury (observable entity)','1052971000000101','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 3 score - problem drinking or drug use (observable entity)','1052981000000104','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 4 score - cognitive problems (observable entity)','1052991000000102','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 5 score - physical illness or disability problems (observable entity)','1053001000000103','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 6 score - problems associated with hallucinations or delusions or confabulations (observable entity)','1053011000000101','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 7 score - problems with depressive symptoms (observable entity)','1053021000000107','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 8 score - other mental and behavioural problems (observable entity)','1053031000000109','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 9 score - problems with relationships (observable entity)','1053041000000100','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 10 score - problems with activities of daily living (observable entity)','1053051000000102','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 11 score - problems with living conditions (observable entity)','1053061000000104','v2','0','4','','',''),
('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 12 score - problems with activities (observable entity)','1053071000000106','v2','0','4','','',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 1 score - overactive, aggressive, disruptive or agitated behaviour','979641000000103','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 2 score - non-accidental self-injury','979651000000100','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 3 score - problem drinking or drug-taking','979661000000102','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 4 score - cognitive problems','979671000000109','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 5 score - physical illness or disability problems','979681000000106','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 6 score - problems associated with hallucinations and delusions','979691000000108','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 7 score - problems with depressed mood','979701000000108','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 8 score - other mental and behavioural problems','979711000000105','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 9 score - problems with relationships','979721000000104','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 10 score - problems with activities of daily living','979731000000102','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 11 score - problems with living conditions','979741000000106','v2','0','4','','Y',''),
('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 12 score - problems with occupation and activities','979751000000109','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 1 score - behavioural disturbance','980761000000107','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 2 score - non-accidental self-injury','980771000000100','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 3 score - problem-drinking or drug-use','980781000000103','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 4 score - cognitive problems','980791000000101','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 5 score - problems related to physical illness/disability','980801000000102','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 6 score - problems associated with hallucinations and/or delusions (or false beliefs)','980811000000100','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 7 score - problems with depressive symptoms','980821000000106','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 8 score - other mental and behavioural problems','980831000000108','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 9 score - problems with social or supportive relationships','980841000000104','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 10 score - problems with activities of daily living','980851000000101','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 11 score - overall problems with living conditions','980861000000103','v2','0','4','','Y',''),
('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 12 score - problems with work and leisure activities - quality of daytime environment','980871000000105','v2','0','4','','Y',''),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 1 score - disruptive, antisocial or aggressive behaviour','989881000000104','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 2 score - overactivity, attention and concentration','989931000000107','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 3 score - non-accidental self injury','989941000000103','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 4 score - alcohol, substance/solvent misuse','989951000000100','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 5 score - scholastic or language skills','989961000000102','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 6 score - physical illness or disability problems','989971000000109','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 7 score - hallucinations and delusions','989981000000106','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 8 score - non-organic somatic symptoms','989991000000108','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 9 score - emotional and related symptoms','990001000000107','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 10 score - peer relationships','989891000000102','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 11 score - self care and independence','989901000000101','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 12 score - family life and relationships','989911000000104','v2','0','4','Y','Y','Parent'),
('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 13 score - poor school attendance','989921000000105','v2','0','4','Y','Y','Parent'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 1 score - disruptive, antisocial or aggressive behaviour','989751000000102','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 2 score - overactivity, attention and concentration','989801000000109','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 3 score - non-accidental self injury','989811000000106','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 4 score - alcohol, substance/solvent misuse','989821000000100','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 5 score - scholastic or language skills','989831000000103','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 6 score - physical illness or disability problems','989841000000107','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 7 score - hallucinations and delusions','989851000000105','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 8 score - non-organic somatic symptoms','989861000000108','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 9 score - emotional and related symptoms','989871000000101','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 10 score - peer relationships','989761000000104','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 11 score - self care and independence','989771000000106','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 12 score - family life and relationships','989781000000108','v2','0','4','Y','Y','Clinician'),
('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 13 score - poor school attendance','989791000000105','v2','0','4','Y','Y','Clinician'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 1 score - disruptive, antisocial or aggressive behaviour','989621000000101','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 2 score - overactivity, attention and concentration','989671000000102','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 3 score - non-accidental self injury','989681000000100','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 4 score - alcohol, substance/solvent misuse','989691000000103','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 5 score - scholastic or language skills','989701000000103','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 6 score - physical illness or disability problems','989711000000101','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 7 score - hallucinations and delusions','989721000000107','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 8 score - non-organic somatic symptoms','989731000000109','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 9 score - emotional and related symptoms','989741000000100','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 10 score - peer relationships','989631000000104','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 11 score - self care and independence','989641000000108','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 12 score - family life and relationships','989651000000106','v2','0','4','Y','Y','Self'),
('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 13 score - poor school attendance','989661000000109','v2','0','4','Y','Y','Self'),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 1 score - behavioural problems (directed at others)','987711000000106','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 2 score - behavioural problems directed towards self (self-injury)','987811000000101','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3A score - behaviour destructive to property','988261000000101','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3B score - problems with personal behaviours','988271000000108','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3D score - anxiety, phobias, obsessive or compulsive behaviour','988291000000107','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3E score - others','988301000000106','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 4 score - attention and concentration','987831000000109','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 5 score - memory and orientation','987841000000100','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 6 score - communication (problems with understanding)','987851000000102','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 7 score - communication (problems with expression)','987861000000104','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 8 score - problems associated with hallucinations and delusions','987871000000106','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 9 score - problems associated with mood changes','987881000000108','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 10 score - problems with sleeping','987721000000100','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 11 score - problems with eating and drinking','987731000000103','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 12 score - physical problems','987741000000107','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 13 score - seizures','987751000000105','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 14 score - activities of daily living at home','987761000000108','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 15 score - activities of daily living outside the home','987771000000101','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 16 score - level of self-care','987781000000104','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 17 score - problems with relationships','987791000000102','v2','0','4','','',''),
('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 18 score - occupation and activities','987801000000103','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale A score - risk of harm to adults or children','981391000000108','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale B score - risk of self-harm (deliberate or accidental)','981401000000106','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale C score - need for buildings security to prevent escape','981411000000108','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale D score - need for safely staffed living environment','981421000000102','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale E score - need for escort on leave (beyond secure perimeter)','981431000000100','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale F score - risk to individual from others','981441000000109','v2','0','4','','',''),
('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale G score - need for risk management procedures','981451000000107','v2','0','4','','',''),
('PROM ','ODD (Parent)','How Are Things? Behavioural Difficulties (Oppositional Defiant Disorder) - Parent/Carer score','961231000000104','v2','0','8','','',''),
('PROM','Kessler Psychological Distress Scale 10','Kessler Psychological Distress Scale 10 score','720211004','v2','0','40','','',''),
('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire behavioural difficulties score','718459002','v2','0','12','','',''),
('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire score','718562002','v2','0','32','','',''),
('PROM','Me and My Feelings Questionnaire','Me and My Feelings Questionnaire behavioural subscale score','1047101000000106','v2','0','12','','',''),
('PROM','Me and My Feelings Questionnaire','Me and My Feelings Questionnaire emotional subscale score','1047091000000103','v2','0','20','','',''),
('PROM','Outcome Rating Scale (ORS)','Outcome Rating Scale total score','716613004','v2','0','40','Y','','Self'),
('PROM','Patient Health Questionnaire (PHQ-9)','Patient health questionnaire 9 score','720433000','v2','0','27','Y','','Self'),
('PROM','Questionnaire about the Process of Recovery (QPR)','Questionnaire about the Process of Recovery total score','1035441000000106','v2','0','60','','Y',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score','718655001','v2','0','30','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score','718656000','v2','0','18','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score','718657009','v2','0','18','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score','718658004','v2','0','27','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score','718665007','v2','0','30','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score','718666008','v2','0','18','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score','718667004','v2','0','18','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score','718668009','v2','0','27','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score','718669001','v2','0','21','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score','718670000','v2','0','27','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety and Depression score','718672008','v2','0','141','','',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety score','718671001','v2','0','111','','',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score','718659007','v2','0','21','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score','718660002','v2','0','27','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety and Depression score','718662005','v2','0','141','','',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety score','718661003','v2','0','111','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - average total score','720197006','v2','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability average score','718434006','v2','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability total score','720200007','v2','5','25','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties average score','718456009','v2','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties total score','720196002','v2','5','25','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication average score','721955008','v2','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication total score','720594007','v2','5','25','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - total score','718455008','v2','15','75','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - conduct problems - educator score','986241000000103','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - conduct problems - parent score','986311000000109','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - emotional symptoms - educator score','986251000000100','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - emotional symptoms - parent score','986321000000103','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - hyperactivity - educator score','986261000000102','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - hyperactivity - parent score','986331000000101','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - impact - educator score','986271000000109','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - impact - parent score','986341000000105','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - peer problems - educator score','986281000000106','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - peer problems - parent score','986351000000108','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - prosocial - educator score','986291000000108','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - prosocial - parent score','986361000000106','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - total difficulties - educator score','986301000000107','v2','0','40','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - total difficulties - parent score','986371000000104','v2','0','40','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score','986061000000108','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - teacher score','986151000000106','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score','986071000000101','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - teacher score','986161000000109','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score','986081000000104','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - teacher score','986171000000102','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score','986091000000102','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - teacher score','986181000000100','v2','0','10','Y','','Parent'),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score','986101000000105','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - teacher score','986191000000103','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score','986111000000107','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - teacher score','986201000000101','v2','0','10','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - total difficulties - parent score','986121000000101','v2','0','40','','',''),
('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - total difficulties - teacher score','986211000000104','v2','0','40','','',''),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score','718143006','v2','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score','718145004','v2','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score','718482000','v2','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score','718477007','v2','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score','718146003','v2','0','10','','',''),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score','718147007','v2','0','10','','',''),
('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - total difficulties score','718134002','v2','0','40','','',''),
('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 1 score - did you feel listened to','1047381000000103','v2','1','5','','',''),
('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 2 score - did you talk about what you wanted to talk about','1047391000000101','v2','1','5','','',''),
('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 3 score - did you understand the things said in the meeting','1047401000000103','v2','1','5','','',''),
('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 4 score - did you feel the meeting gave you ideas for what to do','1047411000000101','v2','1','5','','',''),
('PREM','Session Rating Scale (SRS)','Session Rating Scale score','720482000','v2','0','40','','',''),
('PROM','Sheffield Learning Disabilities Outcome Measure (SLDOM)','Sheffield learning disabilities outcome measure score','860641000000108','v2','0','40','','',''),
('PROM','Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)','Short Warwick-Edinburgh Mental Well-being Scale score','718466001','v2','7','35','','',''),
('PROM','Warwick-Edinburgh Mental Well-being Scale (WEMWBS)','Warwick-Edinburgh Mental Well-being Scale score','718426000','v2','14','70','','',''),
('PROM','Young Child Outcome Rating Scale (YCORS)','Young Child Outcome Rating Scale score','718461006','v2','1','4','','',''),
('PROM','YP-CORE','Young Persons Clinical Outcomes in Routine Evaluation clinical score','718437004','v2','0','40','','',''),
('PREM','Child Group Session Rating Score (CGSRS)','Child Group Session Rating Scale score','960771000000103','v1','0','40','','',''),
('PROM','Child Outcome Rating Scale (CORS)','Child Outcome Rating Scale total score','960321000000103','v1','0','40','Y','','Self'),
('PREM','Child Session Rating Scale (CSRS)','Child Session Rating Scale score','1036271000000108','v1','0','40','','',''),
('PROM','Childrens Revised Impact of Event Scale (8) (CRIES 8)','Revised Child Impact of Events Scale score','960181000000100','v1','0','40','','',''),
('PROM','Clinical Outcomes in Routine Evaluation 10 (CORE 10)','Clinical Outcomes in Routine Evaluation - 10 clinical score','958051000000104','v1','0','40','Y','','Self'),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech frequency and duration score','1037571000000108','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech global rating scale score','1037561000000101','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech level of distress score','1037581000000105','v1','0','100','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas frequency and duration score','1037591000000107','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas global rating scale score','1037541000000102','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas level of distress score','1037601000000101','v1','0','100','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities frequency and duration score','1037611000000104','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities global rating scale score','1037551000000104','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities level of distress score','1037621000000105','v1','0','100','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content frequency and duration score','1037631000000107','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content global rating scale score','1037531000000106','v1','0','6','','',''),
('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content level of distress score','1037641000000103','v1','0','100','','',''),
('PREM','Group Session Rating Scale (GSRS)','Group Session Rating Scale score','960711000000108','v1','0','40','','',''),
('PROM','Kessler Psychological Distress Scale 10','Kessler Psychological Distress Scale 10 score','963561000000106','v1','0','40','','',''),
('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire behavioural difficulties score','960221000000105','v1','0','12','','',''),
('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire score','960211000000104','v1','0','32','','',''),
('PROM','Outcome Rating Scale (ORS)','Outcome Rating Scale total score','960251000000100','v1','0','40','Y','','Self'),
('PROM','Patient Health Questionnaire (PHQ-9)','Patient health questionnaire 9 score','506701000000100','v1','0','27','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score','958231000000102','v1','0','30','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score','958251000000109','v1','0','18','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score','958261000000107','v1','0','18','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score','958221000000104','v1','0','27','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score','958301000000102','v1','0','30','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score','958311000000100','v1','0','18','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score','958321000000106','v1','0','18','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score','958331000000108','v1','0','27','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score','958341000000104','v1','0','21','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score','958351000000101','v1','0','27','Y','','Parent'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety and Depression score','958361000000103','v1','0','141','','',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety score','958371000000105','v1','0','111','','',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score','958241000000106','v1','0','21','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score','958211000000105','v1','0','27','Y','','Self'),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety and Depression score','958281000000103','v1','0','141','','',''),
('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety score','958271000000100','v1','0','111','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - average total score','960021000000100','v1','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability average score','960131000000104','v1','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability total score','959981000000102','v1','5','25','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties average score','960141000000108','v1','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties total score','959991000000100','v1','5','25','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication average score','960151000000106','v1','1','5','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication total score','960001000000109','v1','5','25','','',''),
('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - total score','959961000000106','v1','15','75','','',''),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score','985991000000105','v1','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score','986001000000109','v1','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score','986011000000106','v1','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score','986051000000105','v1','0','10','Y','','Self'),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score','986021000000100','v1','0','10','','',''),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score','986031000000103','v1','0','10','','',''),
('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - total difficulties score','986041000000107','v1','0','40','','',''),
('PREM','Session Rating Scale (SRS)','Session Rating Scale score','960451000000101','v1','0','40','','',''),
('PROM','Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)','Short Warwick-Edinburgh Mental Well-being Scale score','960481000000107','v1','7','35','','',''),
('PROM','Warwick-Edinburgh Mental Well-being Scale (WEMWBS)','Warwick-Edinburgh Mental Well-being Scale score','885541000000103','v1','14','70','','',''),
('PROM','Young Child Outcome Rating Scale (YCORS)','Young Child Outcome Rating Scale score','960391000000100','v1','1','4','','',''),
('PROM','YP-CORE','Young Persons Clinical Outcomes in Routine Evaluation clinical score','960601000000104','v1','0','40','','',''),
('PROM','PGSI (Problem Gambling Severity Index)','Problem Gambling Severity Index score','492451000000101','v1','0','27','','',''),
('PROM','ReQoL (Recovering Quality of Life 20-item)','Recovering Quality of Life 20-item questionnaire score','1091081000000103','v1','0','80','','',''),
('PROM','ReQoL (Recovering Quality of Life 10-item)','Recovering Quality of Life 10-item questionnaire score','1091071000000100','v1','0','40','','',''),
('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure) ','Clinical Outcomes in Routine Evaluation Outcome Measure Well being score (observable entity)','718492008','v1','0','16','','',''),
('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure) ','Clinical Outcomes in Routine Evaluation Outcome Measure Functioning score (observable entity)','718490000','v1','0','48','','',''),
('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure) ','Clinical Outcomes in Routine Evaluation Outcome Measure Global Distress score (observable entity)','718612000','v1','0','40','','',''),
('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure) ','Clinical Outcomes in Routine Evaluation Outcome Measure Problems symptom score (observable entity)','718613005','v1','0','48','','','')


-- COMMAND ----------

--  %py
--  import json
--  dbutils.notebook.exit(json.dumps({
--    "status": "OK"
--  }))