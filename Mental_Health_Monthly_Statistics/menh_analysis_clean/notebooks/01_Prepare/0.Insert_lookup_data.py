# Databricks notebook source
 %md
 
 # Breakdowns & metrics
 
 When the output tables are populated, the unpopulated metrics should still be in the file, but with a zero value. This requires a list of all the possible metrics. That's why as follows, we have tables with the possible breakdowns/metrics for each product. 

# COMMAND ----------

# DBTITLE 1,Collect params for Python
 %python
 
 import os
 
 db_output = dbutils.widgets.get("db_output")
 print(db_output)
 assert db_output
 rp_enddate=dbutils.widgets.get("rp_enddate")
 print(rp_enddate)
 assert rp_enddate
 month_id=int(dbutils.widgets.get("month_id"))
 print(month_id)
 assert month_id

# COMMAND ----------

# DBTITLE 1,STP/Region breakdowns April 2020 onwards
 %sql
 --This code has been copied to menh_publications\notebooks\common_objects\02_load_common_ref_data
 TRUNCATE TABLE $db_output.STP_Region_mapping_post_2020;
 INSERT INTO $db_output.STP_Region_mapping_post_2020 
 
 SELECT 
 A.ORG_cODE as STP_CODE, 
 A.NAME as STP_DESCRIPTION, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_DESCRIPTION,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_DESCRIPTION
 FROM 
 global_temp.org_daily A
 LEFT JOIN global_temp.org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN global_temp.org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN global_temp.org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN global_temp.org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 
 -- added this bit in to get the UNKNOWN row displaying in the output - there may be a better way of doing this!
 UNION ALL
 SELECT DISTINCT
 
 'UNKNOWN' as STP_CODE, 
 'UNKNOWN' as STPD_DESCRIPTION, 
 'UNKNOWN' as CCG_CODE, 
 'UNKNOWN' as CCG_DESCRIPTION,
 'UNKNOWN' as REGION_CODE,
 'UNKNOWN' as REGION_DESCRIPTION
 -- up to here
  
 ORDER BY 1

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='STP_Region_mapping_post_2020'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='STP_Region_mapping_post_2020'))

# COMMAND ----------

# DBTITLE 1,STP/Region breakdowns April 2019 - static table updated for  April 2019 (keep for running old data)
 %sql
 
 TRUNCATE TABLE $db_output.STP_Region_mapping_post_2018;
 INSERT INTO $db_output.STP_Region_mapping_post_2018 VALUES
 ('07L','E54000029','North East London','1','Y56','London'),
 ('07M','E54000028','North Central London','1','Y56','London'),
 ('07N','E54000030','South East London','1','Y56','London'),
 ('07P','E54000027','North West London','1','Y56','London'),
 ('07Q','E54000030','South East London','1','Y56','London'),
 ('07R','E54000028','North Central London','1','Y56','London'),
 ('07T','E54000029','North East London','1','Y56','London'),
 ('07V','E54000031','South West London','1','Y56','London'),
 ('07W','E54000027','North West London','1','Y56','London'),
 ('07X','E54000028','North Central London','1','Y56','London'),
 ('07Y','E54000027','North West London','1','Y56','London'),
 ('08A','E54000030','South East London','1','Y56','London'),
 ('08C','E54000027','North West London','1','Y56','London'),
 ('08D','E54000028','North Central London','1','Y56','London'),
 ('08E','E54000027','North West London','1','Y56','London'),
 ('08F','E54000029','North East London','1','Y56','London'),
 ('08G','E54000027','North West London','1','Y56','London'),
 ('08H','E54000028','North Central London','1','Y56','London'),
 ('08J','E54000031','South West London','1','Y56','London'),
 ('08K','E54000030','South East London','1','Y56','London'),
 ('08L','E54000030','South East London','1','Y56','London'),
 ('08M','E54000029','North East London','1','Y56','London'),
 ('08N','E54000029','North East London','1','Y56','London'),
 ('08P','E54000031','South West London','1','Y56','London'),
 ('08Q','E54000030','South East London','1','Y56','London'),
 ('08R','E54000031','South West London','1','Y56','London'),
 ('08T','E54000031','South West London','1','Y56','London'),
 ('08V','E54000029','North East London','1','Y56','London'),
 ('08W','E54000029','North East London','1','Y56','London'),
 ('08X','E54000031','South West London','1','Y56','London'),
 ('08Y','E54000027','North West London','1','Y56','London'),
 ('09A','E54000027','North West London','1','Y56','London'),
 ('11E','E54000040','Bath, Swindon and Wiltshire','1','Y58','South West'),
 ('11J','E54000041','Dorset','1','Y58','South West'),
 ('11M','E54000043','Gloucestershire','1','Y58','South West'),
 ('11N','E54000036','Cornwall and the Isles of Scilly','1','Y58','South West'),
 ('11X','E54000038','Somerset','1','Y58','South West'),
 ('12D','E54000040','Bath, Swindon and Wiltshire','1','Y58','South West'),
 ('15C','E54000039','Bristol, North Somerset and South Gloucestershire','1','Y58','South West'),
 ('15N','E54000037','Devon','1','Y58','South West'),
 ('99N','E54000040','Bath, Swindon and Wiltshire','1','Y58','South West'),
 ('09C','E54000032','Kent and Medway','1','Y59','South East'),
 ('09D','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09E','E54000032','Kent and Medway','1','Y59','South East'),
 ('09F','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09G','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09H','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09J','E54000032','Kent and Medway','1','Y59','South East'),
 ('09L','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09N','E54000035','Surrey Heartlands','1','Y59','South East'),
 ('09P','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09W','E54000032','Kent and Medway','1','Y59','South East'),
 ('09X','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('09Y','E54000035','Surrey Heartlands','1','Y59','South East'),
 ('10A','E54000032','Kent and Medway','1','Y59','South East'),
 ('10C','E54000034','Frimley Health','1','Y59','South East'),
 ('10D','E54000032','Kent and Medway','1','Y59','South East'),
 ('10E','E54000032','Kent and Medway','1','Y59','South East'),
 ('10J','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('10K','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('10L','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('10Q','E54000044','Buckinghamshire, Oxfordshire and Berkshire West','1','Y59','South East'),
 ('10R','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('10V','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('10X','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('11A','E54000042','Hampshire and the Isle of Wight','1','Y59','South East'),
 ('14Y','E54000044','Buckinghamshire, Oxfordshire and Berkshire West','1','Y59','South East'),
 ('15A','E54000044','Buckinghamshire, Oxfordshire and Berkshire West','1','Y59','South East'),
 ('15D','E54000034','Frimley Health','1','Y59','South East'),
 ('99H','E54000035','Surrey Heartlands','1','Y59','South East'),
 ('99J','E54000032','Kent and Medway','1','Y59','South East'),
 ('99K','E54000033','Sussex and East Surrey','1','Y59','South East'),
 ('99M','E54000034','Frimley Health','1','Y59','South East'),
 ('03T','E54000013','Lincolnshire','1','Y60','Midlands'),
 ('03V','E54000020','Northamptonshire','1','Y60','Midlands'),
 ('03W','E54000015','Leicester, Leicestershire and Rutland','1','Y60','Midlands'),
 ('04C','E54000015','Leicester, Leicestershire and Rutland','1','Y60','Midlands'),
 ('04D','E54000013','Lincolnshire','1','Y60','Midlands'),
 ('04E','E54000014','Nottinghamshire','1','Y60','Midlands'),
 ('04G','E54000020','Northamptonshire','1','Y60','Midlands'),
 ('04H','E54000014','Nottinghamshire','1','Y60','Midlands'),
 ('04K','E54000014','Nottinghamshire','1','Y60','Midlands'),
 ('04L','E54000014','Nottinghamshire','1','Y60','Midlands'),
 ('04M','E54000014','Nottinghamshire','1','Y60','Midlands'),
 ('04N','E54000014','Nottinghamshire','1','Y60','Midlands'),
 ('04Q','E54000013','Lincolnshire','1','Y60','Midlands'),
 ('04V','E54000015','Leicester, Leicestershire and Rutland','1','Y60','Midlands'),
 ('04Y','E54000010','Staffordshire','1','Y60','Midlands'),
 ('05A','E54000018','Coventry and Warwickshire','1','Y60','Midlands'),
 ('05C','E54000016','The Black Country','1','Y60','Midlands'),
 ('05D','E54000010','Staffordshire','1','Y60','Midlands'),
 ('05F','E54000019','Herefordshire and Worcestershire','1','Y60','Midlands'),
 ('05G','E54000010','Staffordshire','1','Y60','Midlands'),
 ('05H','E54000018','Coventry and Warwickshire','1','Y60','Midlands'),
 ('05J','E54000019','Herefordshire and Worcestershire','1','Y60','Midlands'),
 ('05L','E54000016','The Black Country','1','Y60','Midlands'),
 ('05N','E54000011','Shropshire and Telford and Wrekin','1','Y60','Midlands'),
 ('05Q','E54000010','Staffordshire','1','Y60','Midlands'),
 ('05R','E54000018','Coventry and Warwickshire','1','Y60','Midlands'),
 ('05T','E54000019','Herefordshire and Worcestershire','1','Y60','Midlands'),
 ('05V','E54000010','Staffordshire','1','Y60','Midlands'),
 ('05W','E54000010','Staffordshire','1','Y60','Midlands'),
 ('05X','E54000011','Shropshire and Telford and Wrekin','1','Y60','Midlands'),
 ('05Y','E54000016','The Black Country','1','Y60','Midlands'),
 ('06A','E54000016','The Black Country','1','Y60','Midlands'),
 ('06D','E54000019','Herefordshire and Worcestershire','1','Y60','Midlands'),
 ('15E','E54000017','Birmingham and Solihull','1','Y60','Midlands'),
 ('15M','E54000012','Derbyshire','1','Y60','Midlands'),
 ('99D','E54000013','Lincolnshire','1','Y60','Midlands'),
 ('04F','E54000024','Milton Keynes, Bedfordshire and Luton','1','Y61','East of England'),
 ('06F','E54000024','Milton Keynes, Bedfordshire and Luton','1','Y61','East of England'),
 ('06H','E54000021','Cambridgeshire and Peterborough','1','Y61','East of England'),
 ('06K','E54000025','Hertfordshire and West Essex','1','Y61','East of England'),
 ('06L','E54000023','Suffolk and North East Essex','1','Y61','East of England'),
 ('06M','E54000022','Norfolk and Waveney','1','Y61','East of England'),
 ('06N','E54000025','Hertfordshire and West Essex','1','Y61','East of England'),
 ('06P','E54000024','Milton Keynes, Bedfordshire and Luton','1','Y61','East of England'),
 ('06Q','E54000026','Mid and South Essex','1','Y61','East of England'),
 ('06T','E54000023','Suffolk and North East Essex','1','Y61','East of England'),
 ('06V','E54000022','Norfolk and Waveney','1','Y61','East of England'),
 ('06W','E54000022','Norfolk and Waveney','1','Y61','East of England'),
 ('06Y','E54000022','Norfolk and Waveney','1','Y61','East of England'),
 ('07G','E54000026','Mid and South Essex','1','Y61','East of England'),
 ('07H','E54000025','Hertfordshire and West Essex','1','Y61','East of England'),
 ('07J','E54000022','Norfolk and Waveney','1','Y61','East of England'),
 ('07K','E54000023','Suffolk and North East Essex','1','Y61','East of England'),
 ('99E','E54000026','Mid and South Essex','1','Y61','East of England'),
 ('99F','E54000026','Mid and South Essex','1','Y61','East of England'),
 ('99G','E54000026','Mid and South Essex','1','Y61','East of England'),
 ('00Q','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('00R','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('00T','E54000007','Greater Manchester','1','Y62','North West'),
 ('00V','E54000007','Greater Manchester','1','Y62','North West'),
 ('00X','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('00Y','E54000007','Greater Manchester','1','Y62','North West'),
 ('01A','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('01C','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01D','E54000007','Greater Manchester','1','Y62','North West'),
 ('01E','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('01F','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01G','E54000007','Greater Manchester','1','Y62','North West'),
 ('01J','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01K','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('01R','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01T','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01V','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01W','E54000007','Greater Manchester','1','Y62','North West'),
 ('01X','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('01Y','E54000007','Greater Manchester','1','Y62','North West'),
 ('02A','E54000007','Greater Manchester','1','Y62','North West'),
 ('02D','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('02E','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('02F','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('02G','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('02H','E54000007','Greater Manchester','1','Y62','North West'),
 ('02M','E54000048','Lancashire and South Cumbria','1','Y62','North West'),
 ('12F','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('14L','E54000007','Greater Manchester','1','Y62','North West'),
 ('99A','E54000008','Cheshire and Merseyside','1','Y62','North West'),
 ('00C','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00D','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00J','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00K','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00L','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00M','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00N','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('00P','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('01H','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('02N','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('02P','E54000009','South Yorkshire and Bassetlaw','1','Y63','North East and Yorkshire'),
 ('02Q','E54000009','South Yorkshire and Bassetlaw','1','Y63','North East and Yorkshire'),
 ('02R','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('02T','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('02W','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('02X','E54000009','South Yorkshire and Bassetlaw','1','Y63','North East and Yorkshire'),
 ('02Y','E54000006','Humber, Coast and Vale','1','Y63','North East and Yorkshire'),
 ('03A','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('03D','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('03E','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('03F','E54000006','Humber, Coast and Vale','1','Y63','North East and Yorkshire'),
 ('03H','E54000006','Humber, Coast and Vale','1','Y63','North East and Yorkshire'),
 ('03J','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('03K','E54000006','Humber, Coast and Vale','1','Y63','North East and Yorkshire'),
 ('03L','E54000009','South Yorkshire and Bassetlaw','1','Y63','North East and Yorkshire'),
 ('03M','E54000006','Humber, Coast and Vale','1','Y63','North East and Yorkshire'),
 ('03N','E54000009','South Yorkshire and Bassetlaw','1','Y63','North East and Yorkshire'),
 ('03Q','E54000006','Humber, Coast and Vale','1','Y63','North East and Yorkshire'),
 ('03R','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('13T','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('15F','E54000005','West Yorkshire','1','Y63','North East and Yorkshire'),
 ('99C','E54000049','Cumbria and North East','1','Y63','North East and Yorkshire'),
 ('UNKNOWN','UNKNOWN','UNKNOWN','1','UNKNOWN','UNKNOWN');

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='STP_Region_mapping_post_2018'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='STP_Region_mapping_post_2018'))

# COMMAND ----------

# DBTITLE 1,1. Main monthly
 %sql
 
 -- MHS26 - Delayed Discharges moved to end of this notebook to prevent additional breakdowns breaking everything else :-/
 
 TRUNCATE TABLE $db_output.Main_monthly_breakdown_values;
 INSERT INTO $db_output.Main_monthly_breakdown_values VALUES
 ('England'),
 ('CCG - GP Practice or Residence'),
 ('Provider'),
 ('CASSR'),
 ('CASSR; Provider');
 
 TRUNCATE TABLE $db_output.Main_monthly_level_values_1;
 INSERT INTO $db_output.Main_monthly_level_values_1
 
 --************************************************************************************************************************
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 
 --************************************************************************************************************************
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 
 
 --************************************************************************************************************************
 union all
 SELECT DISTINCT
   'England' as primary_level, 
   'England' as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'England' as breakdown
 
 
 --************************************************************************************************************************
 union all
 SELECT DISTINCT
   coalesce(CASSR,"UNKNOWN") as primary_level,
   coalesce(CASSR_description,"UNKNOWN") as primary_level_desc,
   ORG_CODE as secondary_level, 
   NAME as secondary_level_desc,
   'CASSR; Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join global_temp.CASSR_mapping 
 
 union all
 SELECT DISTINCT
   coalesce(CASSR,"UNKNOWN") as primary_level, 
   coalesce(CASSR_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CASSR' as breakdown 
 FROM global_temp.CASSR_mapping -- WARNING: The data in this view differs depending on the month_id
 
 ;
 
 --
 --************************************************************************************************************************

# COMMAND ----------

 %sql
 -- Inpatient measures
 TRUNCATE TABLE $db_output.Main_monthly_metric_values;
 INSERT INTO $db_output.Main_monthly_metric_values VALUES 
    ('MHS07', 'People with an open hospital spell at the end of the reporting period'),
   ('MHS07a', 'People with an open hospital spell at the end of the reporting period aged 0 to 18'),
   ('MHS07b', 'People with an open hospital spell at the end of the reporting period aged 19 to 64'),
   ('MHS07c', 'People with an open hospital spell at the end of the reporting period aged 65 and over'),
     ('MHS21', 'Open ward stays at the end of the reporting period')
   , ('MHS21a', 'Open ward stays at the end of the reporting period, aged 0 to 18')
   , ('MHS21b', 'Open ward stays at the end of the reporting period, aged 19 to64')
   , ('MHS21c', 'Open ward stays at the end of the reporting period, aged 65 and over')
   , ('AMH21', 'Open ward stays (adult mental health services) at the end of the reporting period')
   , ('CYP21', "Open ward stays (children and young people's mental health services) at the end of the reporting period")
   , ('AMH21a', 'Open Ward stays, adult acute MH care at end of reporting period')
   , ('AMH21b', 'Open Ward stays, specialised adult MH services at end of reporting period')
   , ('MHS22', 'Open ward stays, distance >=50KM at end of the reporting period')
   , ('MHS22a', 'Open ward stays, distance >=50KM at end of the reporting period, aged 0 to 18')
   , ('MHS22b', 'Open ward stays, distance >=50KM at end of the reporting period, aged 19 to 64')
   , ('MHS22c', 'Open ward stays, distance >=50KM at end of the reporting period, aged 65 and over')
   , ('AMH22a', 'Open ward stays with distance to treatment >50k, adult acute MH care at end RP')
   , ('AMH22b', 'Distance to treatment >50K Open Ward stays where distance to treatment >50K, specialised adult MH services at end RP')
   , ('MHS24', 'Bed days in RP')
   , ('MHS25', 'Bed days less leave in RP')
 --   , ('MHS26', 'Days of delayed discharge in RP')
   , ('MHS27', 'Admissions to hospital in the RP')
   , ('MHS28', 'Discharges from hospital in the RP')
   , ('MHS31', 'AWOL episodes in RP')
   , ('AMH48a', 'Ward stays ending, adult acute MH care, in the Reporting Period')
   , ('AMH59a', 'Open ward stays with distance to treatment 0 to 19km, adult acute MH care at end RP')
   , ('AMH59b', 'Open ward stays with distance to treatment 20 to 49km, adult acute MH care at end RP')
   , ('AMH59c', 'Open ward stays with distance to treatment 50 to 99km, adult acute MH care at end RP')
   , ('AMH59d', 'Open ward stays with distance to treatment 100km and over, adult acute MH care at end RP');

# COMMAND ----------

 %sql
 -- MHA measures
 INSERT INTO $db_output.Main_monthly_metric_values VALUES 
   ('MHS08', 'People subject to the Mental Health Act at the end of the reporting period')
   , ('MHS08a', 'People subject to the Mental Health Act at the end of the reporting period, aged 0 to 17')
   , ('MH08', 'People subject to the Mental Health Act (mental health services) at the end of the reporting period')
   , ('MH08a', 'People subject to the Mental Health Act (mental health services) at the end of the reporting period, age 0 to 17')
   , ('LDA08', 'People subject to the Mental Health Act (learning disability and Autism services) at the end of the reporting period')
   , ('MHS09', 'People subject to detention at the end of the reporting period')
   , ('MH09', 'People subject to detention (mental health services) at the end of the reporting period')
   , ('MH09a', 'People subject to detention (mental health services) at the end of the reporting period, aged 0 to 17')
   , ('MH09b', 'People subject to detention (mental health services) at the end of the reporting period, aged 18 to 64')
   , ('MH09c', 'People subject to detention (mental health services) at the end of the reporting period, aged 65 and over')
   , ('AMH09a', 'People subject to detention, adult acute MH care, at the end of the reporting period')
   , ('LDA09', 'People subject to detention (learning disability and Autism services) at the end of the reporting period')
   , ('MHS10', 'People subject to CTO or conditional discharge at the end of the reporting period ')
   , ('MH10', 'People subject to CTO or conditional discharge (mental health services) at the end of the reporting period')
   , ('MH10a', 'People subject to CTO or conditional discharge (mental health services) at the end of the reporting period, aged 0 to 17')
   , ('LDA10', 'People subject to CTO or conditional discharge (learning disability and Autism services) at the end of the reporting period')
   , ('MHS11', 'People subject to a short term order at the end of the reporting period')
   , ('MHS11a', 'People subject to a short term order at the end of the reporting period, aged 0 to 17')
   , ('MH11', 'People subject to a short term order (mental health services) at the end of the reporting period')
   , ('LDA11', 'People subject to a short term order (learning disability and Autism services) at the end of the reporting period');

# COMMAND ----------

 %sql
 -- Outpatient-Other measures
 INSERT INTO $db_output.Main_monthly_metric_values VALUES 
   ('MHS01', 'People in contact with services at the end of the reporting period'),
   --('MHS02', 'People on CPA  at the end of the reporting period'),
    ('AMH01', 'People in contact with adult mental health services at the end of the reporting period')
   --, ('AMH02', 'People in contact with adult mental health services on CPA  at the end of the reporting period')
   --, ('AMH03', 'People on CPA aged 18 to 69  at the end of the reporting period (adult mental health services only)')
   --, ('AMH04', 'People in contact with adult mental health services CPA at the end of the reporting period with HoNOS recorded')
   , ('CYP01', "People in contact with children and young people's mental health services at the end of the reporting period")
   , ('MH01', 'People in contact with mental health services at the end of the reporting period')
   , ('MH01a', 'People in contact with mental health services aged 0 to 18 at the end of the reporting period')
   , ('MH01b', 'People in contact with mental health services aged 19 to 64 at the end of the reporting period')
   , ('MH01c', 'People in contact with mental health services aged 65 and over at the end of the reporting period')
   , ('LDA01', 'People in contact with Learning Disabilities and Autism services at the end of the reporting period')
   --, ('AMH05', 'People on CPA for 12 months at the end of the reporting period (adult mental health services only)')
   --, ('AMH06', 'People on CPA for 12 months with review at the end of the reporting period (adult mental health services only)')
   , ('MHS13', 'People in contact with services at the end of the reporting period with accommodation status recorded')
   --, ('AMH14', 'People aged 18 to 69 on CPA at the end of the reporting period  in settled accommodation (adult mental health services)')
   --, ('AMH15', 'Proportion of people aged 18 to 69 on CPA at the end of the reporting period in settled accommodation (adult mental health services)')
   , ('MHS16', 'People in contact with services at the end of the reporting period  with employment status recorded')
   --, ('AMH17', 'People aged 18 to 69 on CPA (adult mental health services) at the end of the reporting period  in employment')
   --, ('AMH18', 'Proportion of people aged 18 to 69 on CPA (adult mental health services) at the end of the reporting period  in employment')
   , ('MHS19', 'People with a crisis plan in place at the end of the reporting period')
   , ('MHS20', 'People in contact with services at the end of the reporting period with a diagnosis recorded')
   , ('MHS23', 'Open referrals at the end of the reporting period')
   , ('AMH23', 'Open referrals (adult mental health services) at end of the reporting period')
   , ('CYP23', "Open referrals (children's and young people's mental health services) at end of the reporting period")
   , ('MHS23a', 'Open referrals to perinatal MH team at the end of the Reporting Period')
   , ('MHS23b', 'Open referrals to crisis resolution service or home treatment team at the end of the Reporting Period')
   , ('MHS23c', 'Open referrals to memory services team at the end of the Reporting Period')
 --   , ('MHS23d', 'Open referrals to community mental health services for adult and older adults with severe mental illness')  BITC-4133, BITC-4679
   , ('MHS29', 'Contacts in RP')
   , ('MHS29a', 'Contacts with perinatal MH team in the Reporting Period')
   , ('MHS29b', 'Contacts with crisis resolution service or home treatment team in the Reporting Period')
   , ('MHS29c', 'Contacts with memory services team in the reporting period')
 --   ,('MHS29d', 'Contacts in the RP with community mental health services for adult and older adults with severe mental illness')
   , ('MHS29f', 'Care contacts by Attended / did not attend code')
   , ('MHS30', 'Attended contacts in RP')
   , ('MHS30a', 'Attended contacts with perinatal MH team in Reporting Period')
   , ('MHS30b', 'Attended contacts with crisis resolution service or home treatment team in Reporting Period')
   , ('MHS30c', 'Attended contacts with memory services team in Reporting Period')
 --   , ('MHS30f', 'Attended contacts in the RP with community mental health services for adult and older adults with severe mental illness')
 --   , ('MHS30h', 'Attended contacts in the RP by consultation medium')
   , ('MHS32', 'Referrals starting in RP')
   , ('MHS33', 'People assigned to an adult MH Care Cluster at end of the reporting period')
   , ('MHS57', 'People discharged from a referral in the reporting period')
   , ('MHS58', 'Missed care contacts in the RP')
   , ('CCR70', 'New Emergency Referrals to Crisis Care teams in the Reporting Period')
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

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Main_monthly_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Main_monthly_level_values_1'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Main_monthly_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Main_monthly_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Main_monthly_level_values_1'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Main_monthly_metric_values'))

# COMMAND ----------

# DBTITLE 1,2. Access and Waiting Times # no longer populated by run_notebooks - moved to menh_publications
# %sql

# TRUNCATE TABLE $db_output.AWT_breakdown_values;
# INSERT INTO $db_output.AWT_breakdown_values VALUES
# ('England'),
# ('England; Ethnicity'),
# ('CCG - GP Practice or Residence'),
# ('CCG - GP Practice or Residence; Ethnicity'),
# ('Provider'),
# ('Provider; Ethnicity');

# TRUNCATE TABLE $db_output.AWT_level_values;
# INSERT INTO $db_output.AWT_level_values
# SELECT DISTINCT
#   IC_Rec_CCG as level, 
#   COALESCE(NAME, "UNKNOWN") as level_desc, 
#   --IC_Rec_CCG as primary_level, 
#   --COALESCE(NAME, "UNKNOWN") as primary_level_desc,
#   'NONE' as secondary_level,
#   'NONE' as secondary_level_desc,
#   'CCG - GP Practice or Residence' as breakdown 
# FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
# UNION ALL
# SELECT DISTINCT
#   ORG_CODE as level, 
#   NAME as level_desc, 
#   --ORG_CODE as primary_level, 
#   --NAME as primary_level_desc, 
#   'NONE' as secondary_level,
#   'NONE' as secondary_level_desc,
#   'Provider' as breakdown 
#   FROM global_temp.Provider_list_AWT -- WARNING: The data in this view differs depending on the month_id
# UNION ALL
# SELECT 
#   'England' as Level, 
#   'England' as level_desc, 
#   --'England' as primary_level, 
#   --'England' as primary_level_desc,
#   'NONE' as secondary_level,
#   'NONE' as secondary_level_desc,
#   'England' as breakdown
# -- BS: Added below for Ethnicity only
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'White' as secondary_level, 'White' as secondary_level_desc, 'England; Ethnicity' as breakdown
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'Mixed' as secondary_level, 'Mixed' as secondary_level_desc, 'England; Ethnicity' as breakdown
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'Asian or Asian British' as secondary_level, 'Asian or Asian British' as secondary_level_desc, 'England; Ethnicity' as breakdown
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'Black or Black British' as secondary_level, 'Black or Black British' as secondary_level_desc, 'England; Ethnicity' as breakdown
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'Other Ethnic Groups' as secondary_level, 'Other Ethnic Groups' as secondary_level_desc, 'England; Ethnicity' as breakdown
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'Not Stated' as secondary_level, 'Not Stated' as secondary_level_desc, 'England; Ethnicity' as breakdown
# UNION ALL
# SELECT 'England' as level, 'England' as level_desc, 'Unknown' as secondary_level, 'Unknown' as secondary_level_desc, 'England; Ethnicity' as breakdown

# -- BS: Added below for CCG level breakdown of Ethnicity
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'White' as secondary_level, 'White' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Mixed' as secondary_level, 'Mixed' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Asian or Asian British' as secondary_level, 'Asian or Asian British' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Black or Black British' as secondary_level, 'Black or Black British' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Other Ethnic Groups' as secondary_level, 'Other Ethnic Groups' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Not Stated' as secondary_level, 'Not Stated' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG
# UNION ALL
# SELECT DISTINCT IC_Rec_CCG as level, COALESCE(NAME, "UNKNOWN") as level_desc, 'Unknown' as secondary_level, 'Unknown' as secondary_level_desc, 'CCG - GP Practice or Residence; Ethnicity' as breakdown FROM global_temp.CCG

# -- BS: Added below for Provider level breakdown of Ethnicity
# UNION ALL  
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'White' as secondary_level, 'White' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT
# UNION ALL
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Mixed' as secondary_level, 'Mixed' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT
# UNION ALL
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Asian or Asian British' as secondary_level, 'Asian or Asian British' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT
# UNION ALL
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Black or Black British' as secondary_level, 'Black or Black British' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT
# UNION ALL
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Other Ethnic Groups' as secondary_level, 'Other Ethnic Groups' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT
# UNION ALL
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Not Stated' as secondary_level, 'Not Stated' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT
# UNION ALL
# SELECT DISTINCT ORG_CODE as level, NAME as level_desc, 'Unknown' as secondary_level, 'Unknown' as secondary_level_desc, 'Provider; Ethnicity' as breakdown FROM global_temp.Provider_list_AWT;


# TRUNCATE TABLE $db_output.AWT_metric_values;
# INSERT INTO $db_output.AWT_metric_values VALUES 
#   ('ED32', 'New referrals with eating disorder issues, aged 0 to 18')
#   , ('EIP01', 'Open referrals on EIP pathway in treatment')
#   , ('EIP01a', 'Open referrals on EIP pathway in treatment , Aged 0 to 17')
#   , ('EIP01b', 'Open referrals on EIP pathway in treatment , Aged 18 to 34')
#   , ('EIP01c', 'Open referrals on EIP pathway in treatment , Aged 35 and over')
#   , ('EIP23a', 'Referrals on EIP pathway entering treatment')
#   , ('EIP23aa', 'Referrals on EIP pathway entering treatment , Aged 0 to 17')
#   , ('EIP23ab', 'Referrals on EIP pathway entering treatment , Aged 18 to 34')
#   , ('EIP23ac', 'Referrals on EIP pathway entering treatment , Aged 35 and over')
#   , ('EIP23b', 'Referrals on EIP pathway entering treatment within two weeks')
#   , ('EIP23ba', 'Referrals on EIP pathway entering treatment within two weeks , Aged 0 to 17')
#   , ('EIP23bb', 'Referrals on EIP pathway entering treatment within two weeks , Aged 18 to 34')
#   , ('EIP23bc', 'Referrals on EIP pathway entering treatment within two weeks , Aged 35 and over')
#   , ('EIP23c', 'Referrals on EIP pathway entering treatment more than two weeks')
#   , ('EIP23ca', 'Referrals on EIP pathway entering treatment more than two weeks , Aged 0 to 17')
#   , ('EIP23cb', 'Referrals on EIP pathway entering treatment more than two weeks , Aged 18 to 34')
#   , ('EIP23cc', 'Referrals on EIP pathway entering treatment more than two weeks , Aged 35 and over')
#   , ('EIP23d', 'Open referrals on EIP pathway waiting for treatment')
#   , ('EIP23da', 'Open referrals on EIP pathway waiting for treatment , Aged 0 to 17')
#   , ('EIP23db', 'Open referrals on EIP pathway waiting for treatment , Aged 18 to 34')
#   , ('EIP23dc', 'Open referrals on EIP pathway waiting for treatment , Aged 35 and over')
#   , ('EIP23e', 'Open referrals on EIP pathway waiting for treatment within two weeks')
#   , ('EIP23ea', 'Open referrals on EIP pathway waiting for treatment within two weeks , Aged 0 to 17')
#   , ('EIP23eb', 'Open referrals on EIP pathway waiting for treatment within two weeks , Aged 18 to 34')
#   , ('EIP23ec', 'Open referrals on EIP pathway waiting for treatment within two weeks , Aged 35 and over')
#   , ('EIP23f', 'Open referrals on EIP pathway waiting for treatment more than two weeks')
#   , ('EIP23fa', 'Open referrals on EIP pathway waiting for treatment more than two weeks , Aged 0 to 17')
#   , ('EIP23fb', 'Open referrals on EIP pathway waiting for treatment more than two weeks , Aged 18 to 34')
#   , ('EIP23fc', 'Open referrals on EIP pathway waiting for treatment more than two weeks , Aged 35 and over')
#   , ('EIP23g', 'Referrals on EIP pathway that receive a first contact')
#   , ('EIP23h', 'Referrals on EIP pathway that are assigned to care coordinator')
#   , ('EIP23i', 'Proportion entering treatment waiting two weeks or less')
#   , ('EIP23ia', 'Proportion entering treatment waiting two weeks or less , Aged 0 to 17')
#   , ('EIP23ib', 'Proportion entering treatment waiting two weeks or less , Aged 18 to 34')
#   , ('EIP23ic', 'Proportion entering treatment waiting two weeks or less , Aged 35 and over')
#   , ('EIP23j', 'Proportion waiting more than two weeks (still waiting)')
#   , ('EIP23ja', 'Proportion waiting more than two weeks (still waiting) , Aged 0 to 17')
#   , ('EIP23jb', 'Proportion waiting more than two weeks (still waiting) , Aged 18 to 34')
#   , ('EIP23jc', 'Proportion waiting more than two weeks (still waiting) , Aged 35 and over')
#   , ('EIP32', 'New referrals with a suspected FEP')
#   , ('EIP63', 'Open referrals not on EIP pathway')
#   , ('EIP63a', 'Open referrals not on EIP pathway aged 0 to 17')
#   , ('EIP63b', 'Open referrals not on EIP pathway aged 18 to 34')
#   , ('EIP63c', 'Open referrals not on EIP pathway aged 35 and over')
#   , ('EIP64', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team')
#   , ('EIP64a', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team aged 0 to 17')
#   , ('EIP64b', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team aged 18 to 34')
#   , ('EIP64c', 'Referrals not on EIP pathway, receiving a first contact and assigned a care co-ordinator with any team aged 35 and over')
#   , ('EIP65', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral')
#   , ('EIP65a', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral aged 0 to 17')
#   , ('EIP65b', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral aged 18 to 34')
#   , ('EIP65c', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team more than two weeks after referral aged 35 and over')
#   , ('EIP66', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral')
#   , ('EIP66a', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 0 to 17')
#   , ('EIP66b', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 18 to 34')
#   , ('EIP66c', 'Referrals not on EIP pathway, Receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 35 and over')
#   , ('EIP67', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral')
#   , ('EIP67a', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 0 to 18')
#   , ('EIP67b', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 18 to 34')
#   , ('EIP67c', 'Proportion of referrals not on EIP pathway receiving a first contact and assigned a care co-ordinator with any team two weeks or less after referral aged 35 and over')
#   , ('MHS32', 'New referrals');

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables # no longer populated by run_notebooks - moved to menh_publications
# %python

# if os.environ['env'] == 'prod':
#   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_breakdown_values'))
#   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_level_values'))
#   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='AWT_metric_values'))

# spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AWT_breakdown_values'))
# spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AWT_level_values'))
# spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='AWT_metric_values'))

# COMMAND ----------

# DBTITLE 1,3. CYP 2nd contact
 %sql
 
 TRUNCATE TABLE $db_output.CYP_2nd_contact_breakdown_values;
 INSERT INTO $db_output.CYP_2nd_contact_breakdown_values VALUES
 ('England'),
 ('Provider'),
 ('Commissioning Region'),
 ('STP'),
 ('CCG - GP Practice or Residence'),
 ('CCG - GP Practice or Residence; Provider');
 
 TRUNCATE TABLE $db_output.CYP_2nd_contact_level_values;
 INSERT INTO $db_output.CYP_2nd_contact_level_values
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 union all
 SELECT 
   'England' as primary_level, 
   'England' as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'England' as breakdown
 union all
 SELECT DISTINCT
   IC_Rec_CCG as primary_level, 
   COALESCE(c.NAME, "UNKNOWN") as primary_level_desc,
   ORG_CODE as secondary_level,
   d.NAME as secondary_level_desc,
   'CCG - GP Practice or Residence; Provider' as breakdown
 FROM (
   SELECT DISTINCT IC_Rec_CCG, NAME FROM global_temp.CCG
 ) c
 CROSS JOIN $db_output.Provider_list d
 union all
 SELECT DISTINCT
   Region_code as primary_level, 
   coalesce(Region_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Commissioning Region' as breakdown
 FROM $db_output.STP_Region_mapping_post_2020
 union all
 SELECT DISTINCT
   STP_code as primary_level, 
   coalesce(STP_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'STP' as breakdown
 FROM $db_output.STP_Region_mapping_post_2020;
 
 TRUNCATE TABLE $db_output.CYP_2nd_contact_metric_values;
 INSERT INTO $db_output.CYP_2nd_contact_metric_values VALUES
   ('MHS69', 'The number of children and young people, regardless of when their referral started, receiving at least two contacts (including indirect contacts) and where their first contact occurs before their 18th birthday');

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_2nd_contact_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_2nd_contact_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_2nd_contact_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYP_2nd_contact_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYP_2nd_contact_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYP_2nd_contact_metric_values'))

# COMMAND ----------

# DBTITLE 1,4. CaP
 %sql
 
 TRUNCATE TABLE $db_output.CaP_breakdown_values;
 INSERT INTO $db_output.CaP_breakdown_values VALUES
 ('England'),
 ('CCG - GP Practice or Residence'),
 ('Provider');
 
 -- Level values
 TRUNCATE TABLE $db_output.CaP_level_values;
 INSERT INTO $db_output.CaP_level_values
 SELECT DISTINCT 
   IC_Rec_CCG as level, 
   COALESCE(NAME, "UNKNOWN") as level_desc, 
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 UNION ALL
 SELECT DISTINCT
   ORG_CODE as level, 
   NAME as level_desc, 
   'Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 UNION ALL
 SELECT 
   'England' as level, 
   'England' as level_desc, 
   'England' as breakdown;
 
 -- Cluster values
 TRUNCATE TABLE $db_output.CaP_cluster_values;
 INSERT INTO $db_output.CaP_cluster_values VALUES
 (0),(1),(2),(3),(4),(5),(6),(7),(8),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21);
 
 -- Metric values
 TRUNCATE TABLE $db_output.CaP_metric_values;
 INSERT INTO $db_output.CaP_metric_values VALUES
   --('ACC02', 'People on CPA at the end of the Reporting Period')
   --, ('ACC53', 'Proportion of people at the end of the Reporting Period who are on CPA')
   ('ACC54', 'People at the end of the RP in settled accommodation')
   , ('ACC33', 'People assigned to an adult MH care cluster')
   , ('ACC37', 'Proportion of people assigned to an adult MH care cluster within cluster review period')
   , ('ACC36', 'People assigned to an adult MH care cluster within cluster review period')
   , ('ACC62', 'Proportion of people at the end of the RP in settled accommodation');

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CaP_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CaP_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CaP_cluster_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CaP_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CaP_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CaP_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CaP_cluster_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CaP_metric_values'))

# COMMAND ----------

# DBTITLE 1,ConsMechanismMH (was ConsMediumUsed prior to v5)
 %sql
 TRUNCATE TABLE $db_output.ConsMechanismMH;
 
 INSERT INTO $db_output.ConsMechanismMH 
 SELECT Code, Description
 FROM $db_output.ConsMechanismMH_dim
 WHERE '$month_id' >= FirstMonth and (LastMonth is null or '$month_id' <= LastMonth)

# COMMAND ----------

# DBTITLE 1,5. CYP monthly
 %sql
 
 TRUNCATE TABLE $db_output.CYP_monthly_breakdown_values;
 INSERT INTO $db_output.CYP_monthly_breakdown_values VALUES
 ('England'),
 ('CCG - GP Practice or Residence'),
 ('Provider'),
 ('England; ConsMechanismMH'),
 ('England; DNA Reason'),
 ('England; Referral Source'),
 ('CCG - GP Practice or Residence; ConsMechanismMH'),
 ('CCG - GP Practice or Residence; DNA Reason'),
 ('CCG - GP Practice or Residence; Referral Source'),
 ('Provider; ConsMechanismMH'),
 ('Provider; DNA Reason'),
 ('Provider; Referral Source');
 
 TRUNCATE TABLE $db_output.DNA_Reason;
 INSERT INTO $db_output.DNA_Reason VALUES 
   ('2', 'Appointment cancelled by, or on behalf of the patient'),
   ('3', 'Did not attend, no advance warning given'),
   ('4', 'Appointment cancelled or postponed by the health care provider'),
   ('7', 'Patient arrived late and could not be seen');
   
 TRUNCATE TABLE $db_output.Referral_Source;
 INSERT INTO $db_output.Referral_Source 
 SELECT Referral_Source, Referral_Description
 FROM $db_output.referral_dim
 WHERE '$month_id' >= FirstMonth and (LastMonth is null or '$month_id' <= LastMonth);
   
 TRUNCATE TABLE $db_output.CYP_monthly_level_values;
 INSERT INTO $db_output.CYP_monthly_level_values
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 UNION ALL
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 union all
 SELECT 
   'England' as primary_level, 
   'England' as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'England' as breakdown
 union all
 SELECT 
   'England' as primary_level, 
   'England' as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'England; ConsMechanismMH' as breakdown
 FROM $db_output.ConsMechanismMH
 union all
 SELECT 
   'England' as primary_level, 
   'England' as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'England; DNA Reason' as breakdown
 FROM $db_output.DNA_Reason
 union all
 SELECT 
   'England' as primary_level, 
   'England' as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'England; Referral Source' as breakdown
 FROM $db_output.Referral_Source
 union all
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'CCG - GP Practice or Residence; ConsMechanismMH' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 cross join $db_output.ConsMechanismMH
 union all
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'CCG - GP Practice or Residence; DNA Reason' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 cross join $db_output.DNA_Reason
 union all
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'CCG - GP Practice or Residence; Referral Source' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 cross join $db_output.Referral_Source
 union all
 SELECT 
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'Provider; ConsMechanismMH' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.ConsMechanismMH
 union all
 SELECT 
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'Provider; DNA Reason' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.DNA_Reason
 union all
 SELECT 
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   level as secondary_level,
   level_description as secondary_level_desc,
   'Provider; Referral Source' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.Referral_Source;
 
 TRUNCATE TABLE $db_output.CYP_monthly_metric_values;
 
 --Inpatient measures
 INSERT INTO $db_output.CYP_monthly_metric_values VALUES 
 --  ('MHS07a', 'People with an open hospital spell at the end of the reporting period aged 0 to 18'),
 --   ('CYP21', 'Open ward stays (children and young people''s mental health services) at the end of the reporting period'),
 --    ('MHS21a', 'Open ward stays at the end of the reporting period, aged 0 to 18'),
     ('MHS24a', 'Under 16 bed days on adult wards in reporting period')
   , ('MHS24b', 'Age 16 bed days on adult wards in reporting period')
   , ('MHS24c', 'Age 17 bed days on adult wards in reporting period');
 
 -- Outpatient Other
 INSERT INTO $db_output.CYP_monthly_metric_values VALUES 
 --  ('MH01a', 'People in contact with mental health services aged 0 to 18 at the end of the reporting period'),
 --   ('CYP01', 'People in contact with children and young people''s mental health services at the end of the reporting period'),
 --    ('CYP02', 'People in contact with children and young people''s mental health services on CPA at the end of the reporting period')
 --  , ('CYP23', 'Open referrals (children''s and young people''s mental health services) at end of the reporting period')
   ('MHS30d', 'Attended contacts in the RP, aged 0 to 18')
   , ('MHS30e', 'Attended contacts in the RP, 0 to 18, by consultation medium')
   , ('CYP32', "Referrals to children and young people's mental health services starting in RP")
   , ('CYP32a', "Referrals to children and young people's mental health services starting in RP, 0 to 18")
   , ('MHS32a', 'Referrals starting in RP, aged 0 to 18')
   , ('MHS32b', 'Referrals starting in reporting period, aged 0 to 18, that were self-referrals')
   , ('MHS38a', 'Referrals active at any point in the reporting period, aged 0 to 18')
   , ('MHS38b', 'Referrals active at any point in the Reporting Period, with indirect activity in the RP, aged 0 to 18')
   , ('MHS39a', 'People with a referral starting in the reporting period, aged 0 to 18')
   , ('MHS40', 'Looked after children with a referral starting in the reporting period, aged 0 to 18')
   , ('MHS41', 'Children and young people with a child protection plan with a referral starting in the reporting period, aged 0 to 18')
   , ('MHS42', 'Young carers with a referral starting in the reporting period, aged 0 to 18')
   , ('MHS55a', 'People attending at least one contact in the RP, aged 0 to 18')
   , ('MHS56a', 'People with indirect activity in the RP, aged 0 to 18')
   , ('MHS57a', 'People discharged from a referral in the reporting period, aged 0 to 18')
   , ('MHS58a', 'Missed care contacts in the RP, 0 to 18, by reason')
   , ('MHS61a', 'First attended contacts for referrals open in the RP, aged 0 to 18')
   , ('MHS61b', 'First attended contacts for referrals open in the RP, aged 0 to 18, by consultation medium')
   , ('MHS68', 'All referrals, aged 0 to 18, with any one or more SNOMED Codes and valid PERS score from MH Assessment Scale Current View in RP');

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_monthly_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='ConsMechanismMH'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='DNA_Reason'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Referral_Source'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_monthly_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='CYP_monthly_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYP_monthly_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='ConsMechanismMH'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='DNA_Reason'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Referral_Source'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYP_monthly_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='CYP_monthly_metric_values'))

# COMMAND ----------

# DBTITLE 1,6. LDA - commented out
# %sql
# -- POPULATE $db_output.Categories TO HOLD ALL AVAILABLE OPTIONS FOR THE BREAKDOWNS. 

# -- This temp table provides the reference data for the extract tables to link to. Since all options are present even those where there are 0 records will be shown.

# -- Note that if a category is missed or a new breakdown is produced then the table below will need to be updated. Otherwise the data will not be pulled through.

# TRUNCATE TABLE $db_output.Categories;

# INSERT INTO $db_output.Categories
# SELECT cast('' as string) as Geography,                            
# cast('' as string) as OrgCode,                              
# cast('' as string) as OrgName,                              
# cast(0 as int) as TableNumber,                              
# cast('' as string) as PrimaryMeasure,                             
# cast(0 as int) as PrimaryMeasureNumber,                           
# cast('' as string) as PrimarySplit,                         
# cast('' as string) as SecondaryMeasure,                           
# cast(0 as int) as SecondaryMeasureNumber,                               
# cast('' as string) as SecondarySplit;

# INSERT INTO $db_output.Categories 
# VALUES
# ('National',  'National',   'National', 1,       'Total',      1,     'Total',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 1,     'Under 18',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 2,     '18-24',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 3,     '25-34',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 4,     '35-44',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 5,     '45-54',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 6,     '55-64',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 7,     '65 and Over',  '', '', ''),
# ('National',  'National',   'National', 2,       'Age', 8,     'Unknown',  '', '', ''),
# ('National',  'National',   'National', 3,       'Gender',     1,     'Male',  '', '', ''),
# ('National',  'National',   'National', 3,       'Gender',     2,     'Female',  '', '', ''),
# ('National',  'National',   'National', 3,       'Gender',     3,     'Not stated',  '', '', ''),
# ('National',  'National',   'National', 3,       'Gender',     4,     'Unknown',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  1,     'White',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  2,     'Mixed',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  3,     'Asian',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  4,     'Black',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  5,     'Other',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  6,     'Not Stated',  '', '', ''),
# ('National',  'National',   'National', 4,       'Ethnicity',  7,     'Unknown',  '', '', ''),
# ('National',  'National',   'National', 5, 'Distance from Home',     1,     'Up to 10km',  '', '', ''),
# ('National',  'National',   'National', 5, 'Distance from Home',     2,     '11-20km',  '', '', ''),
# ('National',  'National',   'National', 5, 'Distance from Home',     3,     '21-50km',  '', '', ''),
# ('National',  'National',   'National', 5,       'Distance from Home',      4,     '51-100km',  '', '', ''),
# ('National',  'National',   'National', 5,       'Distance from Home',      5,     'Over 100km',  '', '', ''),
# ('National',  'National',   'National', 5,       'Distance from Home',      6,     'Unknown',  '', '', ''),
# ('National',  'National',   'National', 6,       'Ward security',     1,     'General',  '', '', ''),
# ('National',  'National',   'National', 6,       'Ward security',     2,     'Low Secure',  '', '', ''),
# ('National',  'National',   'National', 6,       'Ward security',     3,     'Medium Secure',  '', '', ''),
# ('National',  'National',   'National', 6,       'Ward security',     4,     'High Secure',  '', '', ''),
# ('National',  'National',   'National', 6,       'Ward security',     5,     'Unknown',  '', '', ''),
# ('National',  'National',   'National', 7,       'Planned Discharge Date Present', 1,     'Present',  '', '', ''),
# ('National',  'National',   'National', 7,       'Planned Discharge Date Present', 2,     'Not present',  '', '', ''),
# --('National',       'National',   'National', 8,       'Time to Planned Discharge',      1,     'No planned discharge',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      2,     'Planned discharge overdue',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      3,     '0 to 3 months',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      4,     '3 to 6 months',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      5,     '6 to 12 months',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      6,     '1 to 2 years',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      7,     '2 to 5 years',  '', '', ''),
# ('National',  'National',   'National', 8,       'Time to Planned Discharge',      8,     'Over 5 years',  '', '', ''),
# ('National',  'National',   'National', 9,       'Respite care',      1,     'Admitted for respite care',  '', '', ''),
# ('National',  'National',   'National', 9,       'Respite care',      2,     'Not admitted for respite care',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    1,     '0-3 days',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    2,     '4-7 days',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    3,     '1-2 weeks',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    4,     '2-4 weeks',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    5,     '1-3 months',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    6,     '3-6months',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    7,     '6-12 months',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    8,     '1-2 years',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    9,     '2-5 years',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    10,    '5-10 years',  '', '', ''),
# ('National',  'National',   'National', 10,      'Length of stay',    11,    '10+ years',  '', '', ''),
# --('National',       'National',   'National', 10,      'Length of stay',    12,    'Unknown',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   1,     'Community',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   2,     'Hospital',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   3,     'Penal establishment / Court',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   4,     'Non-NHS Hospice',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   5,     'Not applicable',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   6,     'Patient died',  '', '', ''),
# ('National',  'National',   'National', 11,      'Discharge destination',   7,     'Not known',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  1,     'Child and adolescent mental health ward',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  2,     'Paediatric ward',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  3,     'Adult mental health ward',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  4,     'Non mental health ward',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  5,     'Learning disabilties ward',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  6,     'Older peoples mental health ward',  '', '', ''),
# ('National',  'National',   'National', 12,      'Ward Type',  7,     'Unknown',  '', '', ''),
# ('National',  'National',   'National', 13,      'Mental Health Act', 1,     'Informal',  '', '', ''),
# ('National',  'National',   'National', 13,      'Mental Health Act', 2,     'Part 2',  '', '', ''),
# ('National',  'National',   'National', 13,      'Mental Health Act', 3,     'Part 3 no restrictions',  '', '', ''),
# ('National',  'National',   'National', 13,      'Mental Health Act', 4,     'Part 3 with restrictions',  '', '', ''),
# ('National',  'National',   'National', 13,      'Mental Health Act', 5,     'Other sections',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   1,    'Awaiting care coordinator allocation',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   2,    'Awaiting public funding',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   3,    'Awaiting further non-acute (including community and mental health) NHS care',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   4,    'Awaiting Care Home Without Nursing placement or availability',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   5,    'Awaiting Care Home With Nursing placement or availability',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   6,    'Awaiting care package in own home',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   7,    'Awaiting community equipment, telecare and/or adaptations',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   8,    'Patient or Family choice (reason not stated by patient or family)',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   9,    'Patient or Family choice - Non-acute (including community and mental health) NHS care',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   10,   'Patient or Family choice - Care Home Without Nursing placement',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   11,   'Patient or Family choice - Care Home With Nursing placement',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   12,   'Patient or Family choice - Care package in own home',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   13,   'Patient or Family choice - Community equipment, telecare and/or adaptations',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   14,   'Patient or Family Choice - general needs housing/private landlord acceptance',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   15,   'Patient or Family choice - Supported accommodation',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   16,   'Patient or Family choice - Emergency accommodation from the Local Authority under the Housing Act',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   17,   'Patient or Family choice - Child or young person awaiting social care or family placement',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   18,   'Patient or Family choice - Ministry of Justice agreement/permission of proposed placement',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   19,   'Disputes',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   20,   'Housing - Awaiting availability of general needs housing/private landlord accommodation acceptance as patient',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   21,   'Housing - Single homeless patients or asylum seekers NOT covered by Care Act',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   22,   'Housing - Awaiting supported accommodation',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   23,   'Housing - Awaiting emergency accommodation from the Local Authority under the Housing Act',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   24,   'Child or young person awaiting social care or family placement',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   25,   'Awaiting Ministry of Justice agreement/permission of proposed placement',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   26,   'Awaiting outcome of legal requirements (mental capacity/mental health legislation)',  '', '', ''),
# ('National',    'National', 'National', 14, 'Delayed Discharges',   27,   'Unknown',  '', '', ''),
# ('National',  'National',   'National', 15, 'Restraints',    1,   'No restraint type entered',  '', '', ''), 
# ('National',  'National',   'National', 15, 'Restraints',    2,   'Chemical restraint',  '', '', ''),    
# ('National',  'National',   'National', 15, 'Restraints',    3,   'Mechanical restraint',  '', '', ''),     
# ('National',  'National',   'National', 15, 'Restraints',    4,   'Physical restraint - Excluding prone',  '', '', ''),     
# ('National',  'National',   'National', 15, 'Restraints',    5,   'Physical restraint - Prone',  '', '', ''),      
# ('National',  'National',   'National', 15, 'Restraints',    6,   'Seclusion',  '', '', ''),   
# ('National',  'National',   'National', 15, 'Restraints',    7,   'Segregation',  '', '', ''),     
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '1', '0-3 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '2', '4-7 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '3', '1-2 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '4', '2-4 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '5', '1-3 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '6', '3-6 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '7', '6-12 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '8', '1-2 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '9', '2-5 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '10', '5-10 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '11', '10+ years'),
# --('National',       'National',   'National', 50,      'Mental Health Act', 1,     'Informal', 'Length of stay', '12', 'Unknown'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '1', '0-3 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '2', '4-7 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '3', '1-2 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '4', '2-4 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '5', '1-3 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '6', '3-6 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '7', '6-12 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '8', '1-2 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '9', '2-5 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '10', '5-10 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '11', '10+ years'),
# --('National',       'National',   'National', 50,      'Mental Health Act', 2,     'Part 2', 'Length of stay', '12', 'Unknown'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '1', '0-3 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '2', '4-7 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '3', '1-2 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '4', '2-4 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '5', '1-3 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '6', '3-6 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '7', '6-12 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '8', '1-2 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '9', '2-5 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '10', '5-10 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '11', '10+ years'),
# --('National',       'National',   'National', 50,      'Mental Health Act', 3,     'Part 3 no restrictions', 'Length of stay', '12', 'Unknown'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '1', '0-3 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '2', '4-7 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '3', '1-2 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '4', '2-4 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '5', '1-3 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '6', '3-6 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '7', '6-12 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '8', '1-2 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '9', '2-5 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '10', '5-10 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '11', '10+ years'),
# --('National',       'National',   'National', 50,      'Mental Health Act', 4,     'Part 3 with restrictions', 'Length of stay', '12', 'Unknown'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '1', '0-3 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '2', '4-7 days'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '3', '1-2 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '4', '2-4 weeks'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '5', '1-3 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '6', '3-6 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '7', '6-12 months'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '8', '1-2 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '9', '2-5 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '10', '5-10 years'),
# ('National',  'National',   'National', 50,      'Mental Health Act', 5,     'Other sections', 'Length of stay', '11', '10+ years'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 1,     'No restraint type entered', 'Age', '7', '65 and Over'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 2,     'Chemical restraint', 'Age', '7', '65 and Over'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 3,     'Mechanical restraint', 'Age', '7', '65 and Over'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 4,     'Physical restraint - Excluding prone', 'Age', '7', '65 and Over'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 5,     'Physical restraint - Prone', 'Age', '7', '65 and Over'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 6,     'Seclusion', 'Age', '7', '65 and Over'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '1', 'Under 18'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '2', '18-24'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '3', '25-34'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '4', '35-44'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '5', '45-54'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '6', '55-64'),
# ('National',  'National',   'National', 51,      'Restraints', 7,     'Segregation', 'Age', '7', '65 and Over');

# -- This table holds all of the options for which provider cross tabs are done. It also includes options for the totals of provider and commissioner.

# TRUNCATE TABLE $db_output.ProviderCrossCategories;

# INSERT INTO $db_output.ProviderCrossCategories
# SELECT cast('' as string) as Geography,                            
# cast('' as string) as OrgCode,                              
# cast('' as string) as OrgName,                              
# cast(0 as int) as TableNumber,                              
# cast('' as string) as PrimaryMeasure,                             
# cast(0 as int) as PrimaryMeasureNumber,                           
# cast('' as string) as PrimarySplit,                         
# cast('' as string) as SecondaryMeasure,                           
# cast(0 as int) as SecondaryMeasureNumber,                               
# cast('' as string) as SecondarySplit;

# INSERT INTO $db_output.ProviderCrossCategories 
# VALUES
# ('Provider',  '',    '', 70,       'Total',      1,     'Total', '', '', ''),
# ('Commissioner Groupings',     '',    '', 80,       'Total',      1,     'Total', '', '', ''),
# ('Commissioner',     '',    '', 90,       'Total',      1,     'Total', '', '', ''),
# ('TCP Region',     '',    '', 100,       'Total',      1,     'Total', '', '', ''),
# ('TCP',     '',    '', 101,       'Total',      1,     'Total', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    1,     '0-3 days', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    2,     '4-7 days', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    3,     '1-2 weeks', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    4,     '2-4 weeks', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    5,     '1-3 months', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    6,     '3-6 months', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    7,     '6-12 months', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    8,     '1-2 years', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    9,     '2-5 years', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    10,    '5-10 years', '', '', ''),
# ('Provider',  '',    '', 71,       'Length of stay',    11,    '10+ years', '', '', ''),
# --('Provider',       '',    '', 71,       'Length of stay',    12,    'Unknown', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  1,     'Child and adolescent mental health ward', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  2,     'Paediatric ward', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  3,     'Adult mental health ward', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  4,     'Non mental health ward', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  5,     'Learning disabilities ward', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  6,     'Older peoples mental health ward', '', '', ''),
# ('Provider',  '',    '', 72,       'Ward Type',  7,     'Unknown', '', '', ''),
# ('Provider',  '',    '', 73,       'Ward security',     1,     'General', '', '', ''),
# ('Provider',  '',    '', 73,       'Ward security',     2,     'Low Secure', '', '', ''),
# ('Provider',  '',    '', 73,       'Ward security',     3,     'Medium Secure', '', '', ''),
# ('Provider',  '',    '', 73,       'Ward security',     4,     'High Secure', '', '', ''),
# ('Provider',  '',    '', 73,       'Ward security',     5,     'Unknown', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     1,     'No restraint type entered', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     2,     'Chemical restraint', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     3,     'Mechanical restraint', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     4,     'Physical restraint - Excluding prone', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     5,     'Physical restraint - Prone', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     6,     'Seclusion', '', '', ''),
# ('Provider',  '',    '', 74,       'Restraints',     7,     'Segregation', '', '', '');

# COMMAND ----------

# DBTITLE 1,Optimize and vacuum tables - commented out
# %python

# if os.environ['env'] == 'prod':
#   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Categories'))
#   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='ProviderCrossCategories'))

# spark.sql('VACUUM {db_output}.{table} RETAIN 0 HOURS'.format(db_output=db_output, table='Categories'))
# spark.sql('VACUUM {db_output}.{table} RETAIN 0 HOURS'.format(db_output=db_output, table='ProviderCrossCategories'))

# COMMAND ----------

# DBTITLE 1,7. Ascof
 %sql
 
 TRUNCATE TABLE $db_output.Ascof_breakdown_values;
 INSERT INTO $db_output.Ascof_breakdown_values VALUES
 ('England'),
 ('Provider'),
 ('CASSR'),
 ('CASSR; Gender'),
 ('CASSR; Provider'),
 ('CASSR; Provider; Gender'),
 ('England; Gender'),
 ('Provider; Gender');
 
 TRUNCATE TABLE $db_output.Ascof_level_values;
 INSERT INTO $db_output.Ascof_level_values
 SELECT DISTINCT
   IC_Rec_CCG as level, 
   COALESCE(NAME, "UNKNOWN") as level_desc, 
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 UNION ALL
 SELECT DISTINCT
   ORG_CODE as level, 
   NAME as level_desc, 
   'Provider' as breakdown 
   FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 UNION ALL
 SELECT 
   'England' as Level, 
   'England' as level_desc, 
   'England' as breakdown;
   
 TRUNCATE TABLE $db_output.Ascof_metric_values;
 INSERT INTO $db_output.Ascof_metric_values VALUES 
   ('1F_NUMERATOR', 'ASCOF_NUMERATOR')
   , ('1H_NUMERATOR', 'ASCOF_NUMERATOR')
   , ('DENOMINATOR', 'ASCOF_DENOMINATOR');

# COMMAND ----------

# DBTITLE 1,Optimize and vaccum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Ascof_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Ascof_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='Ascof_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Ascof_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Ascof_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='Ascof_metric_values'))

# COMMAND ----------

# DBTITLE 1,8. FYFV Dashboard
 %sql
 
 TRUNCATE TABLE $db_output.FYFV_Dashboard_breakdown_values;
 INSERT INTO $db_output.FYFV_Dashboard_breakdown_values VALUES
 ('England'),
 ('Commissioning Region'),
 ('STP'),
 ('CCG - GP Practice or Residence');
 
 
 TRUNCATE TABLE $db_output.FYFV_Dashboard_level_values;
 INSERT INTO $db_output.FYFV_Dashboard_level_values
 SELECT DISTINCT
   IC_Rec_CCG as level, 
   COALESCE(NAME, "UNKNOWN") as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 UNION ALL
 SELECT 
   'England' as Level, 
   'England' as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'England' as breakdown
 union all
 SELECT DISTINCT
   Region_code as primary_level, 
   coalesce(Region_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Commissioning Region' as breakdown
 FROM $db_output.STP_Region_mapping_post_2020
 union all
 SELECT DISTINCT
   STP_code as primary_level, 
   coalesce(STP_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'STP' as breakdown
 FROM $db_output.STP_Region_mapping_post_2020;
 
 TRUNCATE TABLE $db_output.FYFV_Dashboard_metric_values;
 INSERT INTO $db_output.FYFV_Dashboard_metric_values VALUES 
   ('AMH03e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period (AMH03e)')
    , ('AMH13e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period with accommodation status recorded (AMH13e)')
    , ('AMH13e%', 'Proportion of people in contact with adult mental health services aged 18-69 at the end of the reporting period with accommodation status recorded')
    , ('AMH14e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period in settled accommodation (AMH14e)')
    , ('AMH14e%', 'Proportion of people in contact with adult mental health services aged 18-69 at the end of the reporting period in settled accommodation')
    , ('AMH16e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period with employment status recorded (AMH16e)')
    , ('AMH16e%', 'Proportion of people in contact with adult mental health services aged 18-69 at the end of the reporting period with employment status recorded')
    , ('AMH17e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period in employment (AMH17e)')
    , ('AMH17e%', 'Proportion of people in contact with adult mental health services aged 18-69 at the end of the reporting period in employment')
    , ('MHS69', 'The number of children and young people, regardless of when their referral started, receiving at least two contacts (including indirect contacts) and where their first contact occurs before their 18th birthday')
    , ('BED_DAYS','Bed days on adult wards for people aged 0-17')
    , ('CYP_ADULT_WARDS','Number of people aged 0-17 on adult wards');

# COMMAND ----------

 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='FYFV_Dashboard_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='FYFV_Dashboard_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='FYFV_Dashboard_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='FYFV_Dashboard_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='FYFV_Dashboard_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='FYFV_Dashboard_metric_values'))

# COMMAND ----------

# DBTITLE 1,1.1 MHS26 in Main monthly (Delayed Discharge)
 %sql
 
 TRUNCATE TABLE $db_output.DD_breakdown_values;
 INSERT INTO $db_output.DD_breakdown_values VALUES
 ('England'),
 ('England; Delayed discharge attributable to'),
 ('England; Delayed discharge reason'),
 ('CCG - GP Practice or Residence'),
 ('CCG - GP Practice or Residence; Delayed discharge attributable to'),
 ('CCG - GP Practice or Residence; Delayed discharge reason'),
 ('Provider'),
 ('Provider; Delayed discharge attributable to'),
 ('Provider; Delayed discharge reason'),
 ('Local Authority of Responsibility or Residence'),
 ('Local Authority of Responsibility or Residence; Delayed discharge attributable to'),
 ('Local Authority of Responsibility or Residence; Delayed discharge reason');
 
 --************************************************************************************************************************
 
 TRUNCATE TABLE $db_output.DD_level_values;
 INSERT INTO $db_output.DD_level_values
 
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'CCG - GP Practice or Residence' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 
 union all
 --NB data to be added here for secondary_level
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'CCG - GP Practice or Residence; Delayed discharge attributable to' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 cross join $db_output.DelayedDischDim dd
 where key = 'att' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 
 union all
 SELECT DISTINCT 
   IC_Rec_CCG as primary_level, 
   COALESCE(NAME, "UNKNOWN") as primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'CCG - GP Practice or Residence; Delayed discharge reason' as breakdown 
 FROM global_temp.CCG -- WARNING: The data in this view differs depending on each month rp_enddate
 cross join $db_output.DelayedDischDim dd
 where key = 'reason' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 --************************************************************************************************************************
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'Provider; Delayed discharge attributable to' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.DelayedDischDim dd
 where key = 'att' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'Provider; Delayed discharge reason' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.DelayedDischDim dd
 where key = 'reason' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 --************************************************************************************************************************
 union all
 SELECT DISTINCT
   'England' as primary_level, 
   'England' as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'England' as breakdown
 
 union all
 SELECT DISTINCT
   'England' as primary_level, 
   'England' as primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'England; Delayed discharge attributable to' as breakdown
 from $db_output.DelayedDischDim dd
 where key = 'att' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 
 union all
 SELECT DISTINCT
   'England' as primary_level, 
   'England' as primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'England; Delayed discharge reason' as breakdown
   from $db_output.DelayedDischDim dd
   where key = 'reason' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 --************************************************************************************************************************
 
 union all
 SELECT DISTINCT
 LADistrictAuth AS primary_level,
 LADistrictAuthName AS primary_level_desc,
 'NONE' as secondary_level, 
 'NONE' as secondary_level_desc,
 'Local Authority of Responsibility or Residence' as breakdown 
 FROM global_temp.ResponsibleLA_mapping -- WARNING: The data in this view differs depending on the month_id
 
 union all
 SELECT DISTINCT
 LADistrictAuth AS primary_level,
 LADistrictAuthName AS primary_level_desc,
 code as secondary_level,
 description  as secondary_level_desc,
 'Local Authority of Responsibility or Residence; Delayed discharge attributable to' as breakdown 
 FROM global_temp.ResponsibleLA_mapping -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.DelayedDischDim dd
 where key = 'att' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth)
 --
 union all
 SELECT DISTINCT
   LADistrictAuth AS primary_level,
   LADistrictAuthName AS primary_level_desc,
   code as secondary_level,
   description  as secondary_level_desc,
   'Local Authority of Responsibility or Residence; Delayed discharge reason' as breakdown 
 FROM global_temp.ResponsibleLA_mapping -- WARNING: The data in this view differs depending on the month_id
 cross join $db_output.DelayedDischDim dd
 where key = 'reason' and '$month_id' >= dd.FirstMonth and (dd.LastMonth is null or '$month_id' <= dd.LastMonth);
 --
 --************************************************************************************************************************
 -- Inpatient measures
 TRUNCATE TABLE $db_output.DD_metric_values;
 INSERT INTO $db_output.DD_metric_values VALUES 
 
    ('MHS26', 'Days of delayed discharge in RP')
 ;

# COMMAND ----------

# DBTITLE 1,Optimize and vacuum tables
 %python
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='DD_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='DD_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='DD_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='DD_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='DD_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='DD_metric_values'))