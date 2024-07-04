-- Databricks notebook source
-- DBTITLE 1,Breakdown, Level and Metric values
 %sql
 
 TRUNCATE TABLE $db_output.ascof_breakdown_values;
 INSERT INTO $db_output.ascof_breakdown_values VALUES
 ('England'),
 ('CASSR'),  
 ('Provider'),
 ('CASSR;Provider'),   
 ('CASSR;Gender'),
 ('CASSR;Provider;Gender'),
 ('England;Gender'),
 ('Provider;Gender');
 
 TRUNCATE TABLE $db_output.ascof_level_values;
 INSERT INTO $db_output.ascof_level_values
 
 SELECT 
   'England' as Level, 
   'England' as level_desc, 
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'NONE' as third_level,
   'England' as breakdown
 
 union all
 SELECT DISTINCT
   coalesce(CASSR,"UNKNOWN") as primary_level, 
   coalesce(CASSR_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'NONE' as third_level,
   'CASSR' as breakdown 
 FROM global_temp.CASSR_mapping
 
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   'NONE' as secondary_level,
   'NONE' as secondary_level_desc,
   'NONE' as third_level,
   'Provider' as breakdown 
 FROM $db_output.Provider_list
 
 union all
 SELECT DISTINCT
   coalesce(CASSR,"UNKNOWN") as primary_level,
   coalesce(CASSR_description,"UNKNOWN") as primary_level_desc,
   ORG_CODE as secondary_level, 
   NAME as secondary_level_desc,
   'NONE' as third_level,
   'CASSR;Provider' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join global_temp.CASSR_mapping 
 
 union all
 SELECT DISTINCT
   coalesce(CASSR,"UNKNOWN") as primary_level,
   coalesce(CASSR_description,"UNKNOWN") as primary_level_desc,
   'NONE' as secondary_level, 
   'NONE' as secondary_level_desc,
   PrimaryCode as third_level,
   'CASSR;Gender' as breakdown 
 FROM global_temp.CASSR_mapping
 cross join global_temp.GenderCodes 
 
 union all
 SELECT DISTINCT
   coalesce(CASSR,"UNKNOWN") as primary_level,
   coalesce(CASSR_description,"UNKNOWN") as primary_level_desc,
   ORG_CODE as secondary_level, 
   NAME as secondary_level_desc,
   PrimaryCode as third_level,
   'CASSR;Provider;Gender' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join global_temp.CASSR_mapping 
 cross join global_temp.GenderCodes 
 
 union all
 SELECT DISTINCT
   'England' as Level, 
   'England' as level_desc, 
   'NONE' as secondary_level, 
   'NONE' as secondary_level_desc,
   PrimaryCode as third_level,
   'England;Gender' as breakdown 
 FROM global_temp.GenderCodes 
 
 union all
 SELECT DISTINCT
   ORG_CODE as primary_level, 
   NAME as primary_level_desc,
   'NONE' as secondary_level, 
   'NONE' as secondary_level_desc,
   PrimaryCode as third_level,
   'Provider;Gender' as breakdown 
 FROM $db_output.Provider_list -- WARNING: The data in this view differs depending on the month_id
 cross join global_temp.GenderCodes 
 ;
   
 TRUNCATE TABLE $db_output.ascof_metric_values;
 INSERT INTO $db_output.ascof_metric_values VALUES 
   ('AMH03e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period (AMH03e)')
    , ('AMH14e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period in settled accommodation (AMH14e)')
    , ('AMH14e%', 'Proportion of people in contact with adult mental health services aged 18-69 at the end of the reporting period in settled accommodation')
    , ('AMH17e', 'People in contact with adult mental health services aged 18-69 at the end of the reporting period in employment (AMH17e)')
    , ('AMH17e%', 'Proportion of people in contact with adult mental health services aged 18-69 at the end of the reporting period in employment')
   

-- COMMAND ----------

-- DBTITLE 1,Optimize and vaccum tables
 %python
 
 import os
 db_output = dbutils.widgets.get("db_output")
 
 if os.environ['env'] == 'prod':
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='ascof_breakdown_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='ascof_level_values'))
   spark.sql('OPTIMIZE {db_output}.{table}'.format(db_output=db_output, table='ascof_metric_values'))
 
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='ascof_breakdown_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='ascof_level_values'))
 spark.sql('VACUUM {db_output}.{table} RETAIN 8 HOURS'.format(db_output=db_output, table='ascof_metric_values'))