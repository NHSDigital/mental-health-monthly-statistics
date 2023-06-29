# Databricks notebook source
# DBTITLE 1,Creates python versions of the widgets
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate_1m = dbutils.widgets.get("rp_startdate_1m")
status  = dbutils.widgets.get("status")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.csv_master;
 CREATE TABLE IF NOT EXISTS $db_output.csv_master(
   REPORTING_PERIOD_START string,
   REPORTING_PERIOD_END string,
   STATUS string,
   BREAKDOWN string,
   PRIMARY_LEVEL string,
   PRIMARY_LEVEL_DESCRIPTION string,
   SECONDARY_LEVEL string,
   SECONDARY_LEVEL_DESCRIPTION string,
   MEASURE_ID string
 )

# COMMAND ----------

 %sql
 
 drop table if exists $db_output.ref_master

# COMMAND ----------

 %sql
  
 CREATE TABLE IF NOT EXISTS $db_output.ref_master
 (
     type STRING,
     level STRING,
     level_description STRING
 )
 using delta

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHB_ORG_DAILY;
 
 CREATE TABLE IF NOT EXISTS $db_output.MHB_ORG_DAILY USING DELTA AS
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM dss_corporate.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW ORG_RELATIONSHIP_DAILY AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 DSS_CORPORATE.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW STP_MAPPING AS 
 
 SELECT 
 A.ORG_cODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.MHB_ORG_DAILY A
 LEFT JOIN ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.MHB_ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.MHB_ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

# DBTITLE 1,Reference Table
 %sql
 TRUNCATE TABLE $db_output.ref_master;
 INSERT INTO $db_output.ref_master VALUES
 
 -- England --
 ('England', 'England', 'England'),
 
  -- SubICB - Registration or Residence 
  ('Sub ICB - Registration or Residence','UNKNOWN', 'UNKNOWN'),
 
 -- AGE_HL --
 ('AGE_HL','Under 18', 'People aged under 18'),
 ('AGE_HL','18-64', 'People aged 18 to 64'),
 ('AGE_HL','65+', 'People aged 65 or over'),
 ('AGE_HL','UNKNOWN', 'UNKNOWN'),
 
 -- ATTENDANCE --
 ('ATTENDANCE','5', 'Attended on time or, if late, before the relevant professional was ready to see the patient'),
 ('ATTENDANCE','6', 'Arrived late, after the relevant professional was ready to see the patient, but was seen'),
 ('ATTENDANCE','7', 'Patient arrived late and could not be seen'),
 ('ATTENDANCE','2', 'Appointment cancelled by, or on behalf of the patient'),
 ('ATTENDANCE','3', 'Did not attend, no advance warning given'),
 ('ATTENDANCE','4', 'Appointment cancelled or postponed by the health care provider'),
 ('ATTENDANCE','Invalid', 'Invalid'),
 ('ATTENDANCE','UNKNOWN', 'UNKNOWN'),
 
 --- BED_TYPE ---
 ('BED_TYPE', 'CYP Acute', 'CYP Acute'),
 ('BED_TYPE', 'CYP Specialist', 'CYP Specialist'),
 ('BED_TYPE', 'Adult Acute', 'Adult Acute'),
 ('BED_TYPE', 'Adult Specialist', 'Adult Specialist'),
 ('BED_TYPE', 'Older Adult Acute', 'Older Adult Acute'),
 ('BED_TYPE', 'UNKNOWN', 'UNKNOWN'),
 
 -- CONSULTATION MEDIUM aka Consultant Mechanism--
 ('CONS_MED','01', 'Face to face communication'),
 ('CONS_MED','02', 'Telephone'),
 -- ('CONS_MED','03', 'Telemedicine'),
 ('CONS_MED','04', 'Talk type for a person unable to speak'),
 ('CONS_MED','05', 'Email'),
 -- ('CONS_MED','06', 'Short Message Service (SMS) - Text Messaging'),
 ('CONS_MED','09', 'Text message (asynchronous)'),
 ('CONS_MED','10', 'Instant messaging (synchronous)'),
 ('CONS_MED','11', 'Video consultation'),
 ('CONS_MED','12', 'Message board (asynchronous)'),
 ('CONS_MED','13', 'Chat room (synchronous)'),
 ('CONS_MED','98', 'Other'),
 ('CONS_MED','UNKNOWN', 'UNKNOWN'),
 ('CONS_MED', 'Invalid', 'Invalid'),
 -- ACCOMMODATION TYPE --
 ("ACC_TYPE", "01", "Owner occupier"),             
 ("ACC_TYPE", "02", "Tenant - Local Authority/Arms Length Management Organisation/registered social housing provider"),
 ("ACC_TYPE", "03", "Tenant - private landlord"),
 ("ACC_TYPE", "04", "Living with family"),
 ("ACC_TYPE", "05", "Living with friends"),
 ("ACC_TYPE", "06", "University or College accommodation"),
 ("ACC_TYPE", "07", "Accommodation tied to job (including Armed Forces)"),
 ("ACC_TYPE", "08", "Mobile accommodation"),
 ("ACC_TYPE", "09", "Care home without nursing"), 
 ("ACC_TYPE", "10", "Care home with nursing"),
 ("ACC_TYPE", "11", "Specialist Housing (with suitable adaptations to meet impairment needs and support to live independently)"),
 ("ACC_TYPE", "12", "Rough sleeper"),
 ("ACC_TYPE", "13", "Squatting"),
 ("ACC_TYPE", "14", "Sofa surfing (sleeps on different friends floor each night)"),
 ("ACC_TYPE", "15", "Staying with friends/family as a short term guest"),
 ("ACC_TYPE", "16", "Bed and breakfast accommodation to prevent or relieve homelessness"),
 ("ACC_TYPE", "17", "Sleeping in a night shelter"),
 ("ACC_TYPE", "18", "Hostel to prevent or relieve homelessness"),
 ("ACC_TYPE", "19", "Temporary housing to prevent or relieve homelessness"),
 ("ACC_TYPE", "20", "Admitted patient settings"),
 ("ACC_TYPE", "21", "Criminal justice settings"),
 ("ACC_TYPE", "98", "Other (not listed)"),
 ("ACC_TYPE", "UNKNOWN", "UNKNOWN"),
 
 -- AGE BAND --
 ("AGE", "0 to 5", "0 to 5"),             
 ("AGE", "6 to 10", "6 to 10"),
 ("AGE", "11 to 15", "11 to 15"),
 ("AGE", "16", "16"),
 ("AGE", "17", "17"),
 ("AGE", "18", "18"),
 ("AGE", "19", "19"),
 ("AGE", "20 to 24", "20 to 24"),
 ("AGE", "25 to 29", "25 to 29"),
 ("AGE", "30 to 34", "30 to 34"), 
 ("AGE", "35 to 39", "35 to 39"),
 ("AGE", "40 to 44", "40 to 44"),
 ("AGE", "45 to 49", "45 to 49"),
 ("AGE", "50 to 54", "50 to 54"),
 ("AGE", "55 to 59", "55 to 59"),
 ("AGE", "60 to 64", "60 to 64"),
 ("AGE", "65 to 69", "65 to 69"),
 ("AGE", "70 to 74", "70 to 74"),
 ("AGE", "75 to 79", "75 to 79"),
 ("AGE", "80 to 84", "80 to 84"),
 ("AGE", "85 to 89", "85 to 89"),
 ("AGE", "90 or over", "90 or over"),
 ("AGE", "UNKNOWN", "UNKNOWN"),
 
 -- DISABILITY --
 ("DISAB", "01", "Behaviour and Emotional"),             
 ("DISAB", "02", "Hearing"),
 ("DISAB", "03", "Manual Dexterity"),
 ("DISAB", "04", "Memory or ability to concentrate, learn or understand (Learning Disability)"),
 ("DISAB", "05", "Mobility and Gross Motor"),
 ("DISAB", "06", "Perception of Physical Danger"),
 ("DISAB", "07", "Personal, Self Care and Continence"),
 ("DISAB", "08", "Progressive Conditions and Physical Health (such as HIV, cancer, multiple sclerosis, fits etc)"),
 ("DISAB", "09", "Sight"), 
 ("DISAB", "10", "Speech"),
 ("DISAB", "XX", "Other (not listed)"),
 ("DISAB", "NN", "No Disability"),
 ("DISAB", "ZZ", "Not Stated (Person asked but declined to provide a response)"),
 ("DISAB", "UNKNOWN", "UNKNOWN"),
 
 -- EMPLOYMENT STATUS
 ("EMP", "01", "Employed"),             
 ("EMP", "02", "Unemployed and actively seeking work"),
 ("EMP", "03", "Undertaking full (at least 16 hours per week) or part-time (less than 16 hours per week) education or training as a student and not working or actively seeking work"),
 ("EMP", "04", "Long-term sick or disabled, those receiving government sickness and disability benefits"),
 ("EMP", "05", "Looking after the family or home as a homemaker and not working or actively seeking work"),
 ("EMP", "06", "Not receiving government sickness and disability benefits and not working or actively seeking work"),
 ("EMP", "07", "Unpaid voluntary work and not working or actively seeking work"),
 ("EMP", "08", "Retired"),
 ("EMP", "ZZ", "Not Stated (PERSON asked but declined to provide a response)"),              
 ("EMP", "UNKNOWN", "UNKNOWN"),
 
 -- ETHNICITY --
 ("ETH", "A", "British"),             
 ("ETH", "B", "Irish"),
 ("ETH", "C", "Any Other White Background"),
 ("ETH", "D", "White and Black Caribbean"),
 ("ETH", "E", "White and Black African"),
 ("ETH", "F", "White and Asian"),
 ("ETH", "G", "Any Other mixed background"),
 ("ETH", "H", "Indian"),
 ("ETH", "J", "Pakistani"), 
 ("ETH", "K", "Bangladeshi"),
 ("ETH", "L", "Any Other Asian background"),
 ("ETH", "M", "Caribbean"),
 ("ETH", "N", "African"),
 ("ETH", "P", "Any Other Black background"),
 ("ETH", "R", "Chinese"),
 ("ETH", "S", "Any Other Ethnic Group"),
 ("ETH", "Z", "Not Stated"),
 ("ETH", "99", "Not Known"),
 ("ETH", "UNKNOWN", "UNKNOWN"),
 
 -- GENDER --
 ("GEN", "1", "Male"),             
 ("GEN", "2", "Female"),
 ("GEN", "3", "Non-binary - based on Gender identity field"),
 ("GEN", "4", "Other (not listed) - based on Gender identity field"),
 ("GEN", "9", "Indeterminate - based on Person stated gender field"),            
 ("GEN", "UNKNOWN", "UNKNOWN"),
 
 -- IMD DECILE --
 ("IMD", "01 Most deprived", "01 Most deprived"),             
 ("IMD", "02 More deprived", "02 More deprived"),
 ("IMD", "03 More deprived", "03 More deprived"),
 ("IMD", "04 More deprived", "04 More deprived"),
 ("IMD", "05 More deprived", "05 More deprived"),
 ("IMD", "06 Less deprived", "06 Less deprived"),
 ("IMD", "07 Less deprived", "07 Less deprived"),
 ("IMD", "08 Less deprived", "08 Less deprived"),
 ("IMD", "09 Less deprived", "09 Less deprived"), 
 ("IMD", "10 Least deprived", "10 Least deprived"),
 ("IMD", "UNKNOWN", "UNKNOWN"),
 
 -- SEXUAL ORIENTATION --
 ("SO", "Asexual (not sexually attracted to either gender)", "Asexual (not sexually attracted to either gender)"),             
 ("SO", "Bisexual", "Bisexual"),
 ("SO", "Gay or Lesbian", "Gay or Lesbian"),
 ("SO", "Heterosexual or Straight", "Heterosexual or Straight"),
 ("SO", "Not Stated (Person asked but declined to provide a response)", "Not Stated (Person asked but declined to provide a response)"),            
 ("SO", "Not known (not recorded)", "Not known (not recorded)"),
 ("SO", "Person asked and does not know or is not sure", "Person asked and does not know or is not sure"),            
 ("SO", "UNKNOWN", "UNKNOWN")

# COMMAND ----------

# DBTITLE 1,Add in Provider list
 %sql
 
 INSERT INTO $db_output.ref_master
 SELECT	DISTINCT	'Provider' as type
                     ,HDR.OrgIDProvider as level
 					,ORG.NAME as level_description					
 FROM				$db_source.MHS000Header
 						AS HDR
 					LEFT OUTER JOIN (
                                       SELECT DISTINCT ORG_CODE, 
                                             NAME
                                        FROM $db_output.MHB_ORG_DAILY
                                       WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                                             AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                                             AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN')                 
                                     )
 						AS ORG
 						ON HDR.OrgIDProvider = ORG.ORG_CODE
 WHERE HDR.UniqMonthID = '$end_month_id'

# COMMAND ----------

# DBTITLE 1,Add in CCG list
 %sql
 INSERT INTO $db_output.ref_master
 SELECT               'Sub ICB - Registration or Residence' as type 
                      ,ORG_CODE as level
                            ,NAME as level_description
 FROM                 $db_output.MHB_ORG_DAILY
 WHERE                (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL) 
                      AND ORG_TYPE_CODE = 'CC'
                      AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
                      AND ORG_OPEN_DATE <= '$rp_enddate'
                      AND NAME NOT LIKE '%HUB'
                      AND NAME NOT LIKE '%NATIONAL%'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.measure_level_type_mapping;
 
 CREATE TABLE IF NOT EXISTS $db_output.measure_level_type_mapping
 (
     measure_id STRING,
     breakdown STRING,
     type STRING, -- foreign key rel to ref_master.type
     cross_join_tables_l2 STRING, -- level1 cross join table
     cross_join_tables_l3 STRING -- level2 cross join table
 )

# COMMAND ----------

# DBTITLE 1,Insert Part1 Measures and Breakdowns into table
 %sql
 INSERT INTO TABLE $db_output.measure_level_type_mapping VALUES
 -- MHS01 based measures --
 ('MHS01', 'England', 'England', Null, Null),
 ('MHS01', 'England; Accommodation Type', 'ACC_TYPE', Null, Null),
 ('MHS01', 'England; Age', 'AGE', Null, Null),
 ('MHS01', 'England; Disability', 'DISAB', Null, Null),
 ('MHS01', 'England; Employment Status', 'EMP', Null, Null),
 ('MHS01', 'England; Ethnicity', 'ETH', Null, Null),
 ('MHS01', 'England; Gender', 'GEN', Null, Null),
 ('MHS01', 'England; IMD Decile', 'IMD', Null, Null),
 ('MHS01', 'England; Sexual Orientation', 'SO', Null, Null),
 -- MHS07 based measures --
 ('MHS07', 'England', 'England', Null, Null), 
 ('MHS07', 'England; Accommodation Type', 'ACC_TYPE', Null, Null),
 ('MHS07', 'England; Age', 'AGE', Null, Null),
 ('MHS07', 'England; Disability', 'DISAB', Null, Null),
 ('MHS07', 'England; Employment Status', 'EMP', Null, Null),
 ('MHS07', 'England; Ethnicity', 'ETH', Null, Null),
 ('MHS07', 'England; Gender', 'GEN', Null, Null),
 ('MHS07', 'England; IMD Decile', 'IMD', Null, Null),
 ('MHS07', 'England; Sexual Orientation', 'SO', Null, Null),
 -- MHS23 based measures --
 ('MHS23d', "England", 'England', Null, Null), 
 ('MHS23d', "England; Age Group", 'England', 'AGE_HL', Null), 
 ('MHS23d', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS23d', "Sub ICB - GP Practice or Residence; Age Group", 'Sub ICB - Registration or Residence', 'AGE_HL', Null), 
 ('MHS23d', "Provider", 'Provider', Null, Null), 
 ('MHS23d', "Provider; Age Group", 'Provider', 'AGE_HL', Null),  
 
 
 -- MHS27 based measures -- 
 ('MHS27a', 'England', 'England', Null, Null), 
 ('MHS27a', "England; Bed Type", 'England', 'BED_TYPE', Null), 
 ('MHS27a', "Sub ICB - GP Practice or Residence; Bed Type", 'Sub ICB - Registration or Residence', 'BED_TYPE', Null), 
 ('MHS27a', "Provider; Bed Type", 'Provider', 'BED_TYPE', Null), 
 
 -- MHS29 based measures --
 ('MHS29', 'England', 'England', Null, Null), 
 ('MHS29', 'England; Accommodation Type', 'ACC_TYPE', Null, Null),
 ('MHS29', 'England; Age', 'AGE', Null, Null),
 ('MHS29', 'England; Disability', 'DISAB', Null, Null),
 ('MHS29', 'England; Employment Status', 'EMP', Null, Null),
 ('MHS29', 'England; Ethnicity', 'ETH', Null, Null),
 ('MHS29', 'England; Gender', 'GEN', Null, Null),
 ('MHS29', 'England; IMD Decile', 'IMD', Null, Null),
 ('MHS29', 'England; Sexual Orientation', 'SO', Null, Null),
 
 -- MHS29a
 ('MHS29a', 'England', 'England', Null, Null),--dup?
 ('MHS29a', 'England; Attendance', 'England','ATTENDANCE', Null), 
 ('MHS29a', 'Sub ICB - GP Practice or Residence', 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS29a', 'Sub ICB - GP Practice or Residence; Attendance', 'Sub ICB - Registration or Residence', 'ATTENDANCE', Null),
 ('MHS29a', 'Provider', 'Provider', Null, Null),--dup?
 ('MHS29a', "Provider; Attendance", 'Provider', 'ATTENDANCE', Null),
 
 --MHS29d 
 ('MHS29d', 'England', 'England', Null, Null), 
 ('MHS29d', 'England; Age Group', 'England', 'AGE_HL', Null), 
 ('MHS29d', 'England; Attendance', 'England', 'ATTENDANCE', Null), 
 ('MHS29d', 'Sub ICB - GP Practice or Residence', 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS29d', 'Sub ICB - GP Practice or Residence; Age Group', 'Sub ICB - Registration or Residence', 'AGE_HL', Null),
 ('MHS29d', 'Sub ICB - GP Practice or Residence; Attendance', 'Sub ICB - Registration or Residence', 'ATTENDANCE', Null),
 ('MHS29d', "Provider", 'Provider', Null, Null), 
 ('MHS29d', "Provider; Age Group", 'Provider', 'AGE_HL', Null),  
 ('MHS29d', "Provider; Attendance", 'Provider', 'ATTENDANCE', Null), 
 
 ('MHS29f', "England; Attendance", 'England', 'ATTENDANCE', Null), 
 ('MHS29f', "Sub ICB - GP Practice or Residence; Attendance", 'Sub ICB - Registration or Residence', 'ATTENDANCE', Null), -
 ('MHS29f', "Provider; Attendance", 'Provider', 'ATTENDANCE', Null), 
 
 -- MHS30 based measures --
 ('MHS30f', "England", 'England', Null, Null), 
 ('MHS30f', "England; Age Group", 'England', 'AGE_HL', Null), 
 ('MHS30f', "England; ConsMechanismMH", 'England', 'CONS_MED', Null), 
 ('MHS30f', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS30f', "Sub ICB - GP Practice or Residence; Age Group", 'Sub ICB - Registration or Residence', 'AGE_HL', Null),
 ('MHS30f', "Sub ICB - GP Practice or Residence; ConsMechanismMH", 'Sub ICB - Registration or Residence', 'CONS_MED', Null), 
 ('MHS30f', "Provider", 'Provider', Null, Null), 
 ('MHS30f', "Provider; Age Group", 'Provider', 'AGE_HL', Null),  
 ('MHS30f', "Provider; ConsMechanismMH", 'Provider', 'CONS_MED', Null), 
 
 ('MHS30h', "England; ConsMechanismMH", 'England', 'CONS_MED', Null), 
 ('MHS30h', "Sub ICB - GP Practice or Residence; ConsMechanismMH", 'Sub ICB - Registration or Residence', 'CONS_MED', Null), 
 ('MHS30h', "Provider; ConsMechanismMH", 'Provider', 'CONS_MED', Null), 
 
 --MHS30a new
 ('MHS30a', "England", 'England', Null, Null), 
 ('MHS30a', "England; ConsMechanismMH", 'England', 'CONS_MED', Null),
 ('MHS30a', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS30a', "Sub ICB - GP Practice or Residence; ConsMechanismMH", 'Sub ICB - Registration or Residence', 'CONS_MED', Null), 
 ('MHS30a', "Provider", 'Provider', Null, Null), 
 ('MHS30a', "Provider; ConsMechanismMH", 'Provider', 'CONS_MED', Null), 
 
 -- MHS32 based measures --
 ('MHS32', 'England', 'England', Null, Null), 
 ('MHS32', 'England; Accommodation Type', 'ACC_TYPE', Null, Null),
 ('MHS32', 'England; Age', 'AGE', Null, Null),
 ('MHS32', 'England; Disability', 'DISAB', Null, Null),
 ('MHS32', 'England; Employment Status', 'EMP', Null, Null),
 ('MHS32', 'England; Ethnicity', 'ETH', Null, Null),
 ('MHS32', 'England; Gender', 'GEN', Null, Null),
 ('MHS32', 'England; IMD Decile', 'IMD', Null, Null),
 ('MHS32', 'England; Sexual Orientation', 'SO', Null, Null),
 ('MHS32c', "England", 'England', Null, Null), 
 ('MHS32c', "England; Age Group", 'England', 'AGE_HL', Null), 
 ('MHS32c', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS32c', "Sub ICB - GP Practice or Residence; Age Group", 'Sub ICB - Registration or Residence', 'AGE_HL', Null), 
 ('MHS32c', "Provider", 'Provider', Null, Null), 
 ('MHS32c', "Provider; Age Group", 'Provider', 'AGE_HL', Null),  
 ('MHS32d', "England", 'England', Null, Null), 
 ('MHS32d', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null),
 ('MHS32d', "Provider", 'Provider', Null, Null), 
 
 -- MHS57 based measures --
 ('MHS57b', "England", 'England', Null, Null), 
 ('MHS57b', "England; Age Group", 'England', 'AGE_HL', Null), 
 ('MHS57b', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null),
 ('MHS57b', "Sub ICB - GP Practice or Residence; Age Group", 'Sub ICB - Registration or Residence', 'AGE_HL', Null), 
 ('MHS57b', "Provider", 'Provider', Null, Null), 
 ('MHS57b', "Provider; Age Group", 'Provider', 'AGE_HL', Null),  
 ('MHS57c', "England", 'England', Null, Null), 
 ('MHS57c', "Sub ICB - GP Practice or Residence", 'Sub ICB - Registration or Residence', Null, Null), 
 ('MHS57c', "Provider", 'Provider', Null, Null)

# COMMAND ----------

# DBTITLE 1,Creation of ref asset
mltm_df = spark.table(f'{db_output}.measure_level_type_mapping')
ref_master_df = spark.table(f'{db_output}.ref_master')
# display(ref_master_df)
joined_df = mltm_df.alias('lhs').join(ref_master_df.alias('rhs'), ref_master_df.type == mltm_df.type).select('lhs.measure_id','lhs.breakdown', 'lhs.type','lhs.cross_join_tables_l2','lhs.cross_join_tables_l3', 'rhs.level','rhs.level_description')
joined_df.createOrReplaceTempView("joined_df_v")
mltm_df.createOrReplaceTempView("mltm_df_v")

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.ref_master
 ZORDER BY (type);
 
 DROP TABLE IF EXISTS $db_output.tmp_joined_df;
 
 CREATE TABLE $db_output.tmp_joined_df USING DELTA AS
 SELECT *
 FROM joined_df_v;
 
 DROP TABLE IF EXISTS $db_output.tmp_mltm_df_v;
 
 CREATE TABLE $db_output.tmp_mltm_df_v USING DELTA AS
 SELECT *
 FROM mltm_df_v;
 
 DROP TABLE IF EXISTS $db_output.level1_df_v;
 
 CREATE TABLE $db_output.level1_df_v USING DELTA AS
 SELECT *
 FROM $db_output.tmp_joined_df
 WHERE cross_join_tables_l2 IS NULL AND cross_join_tables_l3 IS NULL;
 
 DROP TABLE IF EXISTS $db_output.level_2_stg1_df_v;
 
 CREATE TABLE $db_output.level_2_stg1_df_v USING DELTA AS
 SELECT *
 FROM $db_output.tmp_mltm_df_v
 WHERE cross_join_tables_l2 IS NOT NULL AND cross_join_tables_l3 IS NULL;

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.csv_master

# COMMAND ----------

qry_stg1 = (f"""
INSERT INTO {db_output}.csv_master
SELECT			 
'{rp_startdate_1m}' AS REPORTING_PERIOD_START
,'{rp_enddate}' AS REPORTING_PERIOD_END
,'{status}' AS STATUS
,breakdown AS BREAKDOWN
,LEVEL AS LEVEL_ONE
,LEVEL_DESCRIPTION AS LEVEL_ONE_DESCRIPTION
,'NONE' AS LEVEL_TWO
,'NONE' AS LEVEL_TWO_DESCRIPTION
,measure_id AS MEASURE_ID
FROM {db_output}.level1_df_v""");
print(qry_stg1)
spark.sql(qry_stg1)


# COMMAND ----------

level_2_stg1_df = spark.sql(f"select * from {db_output}.level_2_stg1_df_v")
print(level_2_stg1_df.count())
for record in level_2_stg1_df.collect():
  print(record)
  level2_qry = f"""
  INSERT INTO {db_output}.csv_master
   SELECT			 
    '{rp_startdate_1m}' AS REPORTING_PERIOD_START
    ,'{rp_enddate}' AS REPORTING_PERIOD_END
    ,'{status}' AS STATUS
    ,l1.breakdown AS BREAKDOWN
    ,L1.level AS LEVEL_ONE
    ,L1.level_description AS LEVEL_ONE_DESCRIPTION
    ,L2.LEVEL AS LEVEL_TWO
    ,L2.LEVEL_DESCRIPTION AS LEVEL_TWO_DESCRIPTION
    ,L1.measure_id AS MEASURE_ID
    FROM 
    (SELECT * FROM {db_output}.tmp_joined_df WHERE type = '{record['type']}' AND measure_id = '{record['measure_id']}' AND breakdown = '{record['breakdown']}') AS L1
    CROSS JOIN (SELECT * FROM {db_output}.ref_master where type = '{record['cross_join_tables_l2']}') AS L2
  """
  
  print(level2_qry)
  spark.sql(level2_qry)


# COMMAND ----------

 %sql
 select * from $db_output.csv_master 

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))