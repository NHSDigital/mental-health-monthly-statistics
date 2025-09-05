# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as df
from dataclasses import dataclass

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_enddate = dbutils.widgets.get("rp_enddate")

# COMMAND ----------

def combine_breakdowns_to_df(bd1: dict, bd2: dict) -> df:
  bd1_df = create_level_dataframe(bd1["level_list"], bd1["lookup_col"])
  bd2_df = create_level_dataframe(bd2["level_list"], bd2["lookup_col"])
  cross_df = bd1_df.crossJoin(bd2_df)
  
  return cross_df

def create_level_dataframe(level_list: list, lookup_columns: list) -> df:
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each necessary measure_id    
    """
    level_df = (
    spark.createDataFrame(
      level_list,
      lookup_columns
    )
    )
    
    return level_df

# COMMAND ----------

total_bd = {
    "level_list": [("Total", )],
    "lookup_col": ["Total"],
    "level_fields" : [
      F.col("Geography").alias("PrimaryMeasure"),
      F.col("Total").alias("PrimaryMeasure"),  
      F.col("Total").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
age_bd  = {
    "level_list" : [            
              ("Under 18", ),
              ("18-24", ),           
              ("25-34", ),             
              ("35-44", ),             
              ("45-54", ),             
              ("55-64", ),              
              ("65 and Over", ),
              ("Unknown", ),
  ],
  "lookup_col" : ["Age_Band"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Age").alias("PrimaryMeasure"),  
      F.col("Age_Band").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
gender_bd  = {
    "level_list" : [        
              ("Male", ),           
              ("Female", ),             
              ("Not stated", ),             
              ("Unknown", ),
  ],
  "lookup_col" : ["Gender"],
    "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Gender").alias("PrimaryMeasure"),  
      F.col("Gender").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
upper_eth_bd  = {
    "level_list" : [            
              ("White", ),
              ("Mixed", ),           
              ("Asian", ),             
              ("Black", ),             
              ("Other", ),             
              ("Not Stated", ),
              ("Unknown", ),
  ],
  "lookup_col" : ["UpperEthnicity"],
    "level_fields" : [
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Ethnicity").alias("PrimaryMeasure"),  
      F.col("UpperEthnicity").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
dist_from_home_bd  = {
    "level_list" : [            
              ("Up to 10km", ),
              ("11-20km", ),           
              ("21-50km", ),             
              ("51-100km", ),             
              ("Over 100km", ), 
              ("Unknown", ),
  ],
  "lookup_col" : ["DistFromHome"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Distance from Home").alias("PrimaryMeasure"),  
      F.col("DistFromHome").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
ward_sec_bd  = {
    "level_list" : [            
              ("General", ),
              ("Low Secure", ),           
              ("Medium Secure", ),             
              ("High Secure", ),  
              ("Unknown", ),
  ],
  "lookup_col" : ["WardSec"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Ward security").alias("PrimaryMeasure"),  
      F.col("WardSec").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
plan_disch_date_bd  = {
    "level_list" : [        
              ("Present", ),           
              ("Not present", ),
  ],
  "lookup_col" : ["PlanDischDate"],
    "level_fields" : [
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Planned Discharge Date Present").alias("PrimaryMeasure"),  
      F.col("PlanDischDate").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
time_to_plan_disch_bd  = {
    "level_list" : [      
              ("Planned discharge overdue", ),           
              ("0 to 3 months", ),             
              ("3 to 6 months", ),             
              ("6 to 12 months", ),             
              ("1 to 2 years", ),
              ("2 to 5 years", ),
              ("Over 5 years", ),
              ("Unknown", ),
  ],
  "lookup_col" : ["TimeToPlanDisch"],
    "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Time to Planned Discharge").alias("PrimaryMeasure"),  
      F.col("TimeToPlanDisch").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
resp_care_bd  = {
    "level_list" : [        
              ("Admitted for respite care", ),           
              ("Not admitted for respite care", ),
  ],
  "lookup_col" : ["RespCare"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Respite care").alias("PrimaryMeasure"),  
      F.col("RespCare").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
los_bd  = {
    "level_list" : [      
              ("0-3 days", ),           
              ("4-7 days", ),             
              ("1-2 weeks", ),             
              ("2-4 weeks", ),             
              ("1-3 months", ),
              ("3-6 months", ),
              ("6-12 months", ),
              ("1-2 years", ),             
              ("2-5 years", ),
              ("5-10 years", ),
              ("10+ years", ),
  ],
  "lookup_col" : ["LengthOfStay"],
    "level_fields" : [
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Length of stay").alias("PrimaryMeasure"),  
      F.col("LengthOfStay").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
disch_dest_bd  = {
    "level_list" : [      
              ("Community", ),           
              ("Hospital", ),             
              ("Penal Establishment / Court", ),             
              ("Hospice", ),      
              ("Not Applicable", ),
              ("Patient died", ),
              ("Organisation responsible for forced repatriation", ),
              ("Not Known", ),  
  ],
  "lookup_col" : ["DischDest"],
    "level_fields" : [
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Discharge Destination Grouped").alias("PrimaryMeasure"),  
      F.col("DischDest").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
ward_type_bd  = {
    "level_list" : [ 
              ("Child and adolescent mental health ward", ),
              ("Paediatric ward", ),           
              ("Adult mental health ward", ),             
              ("Non mental health ward", ),             
              ("Learning disabilities ward", ),             
              ("Older peoples mental health ward", ),
              ("Unknown", ),  
  ],
  "lookup_col" : ["WardType"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Ward Type").alias("PrimaryMeasure"),  
      F.col("WardType").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
mha_bd  = {
    "level_list" : [      
              ("Informal", ),           
              ("Part 2", ),             
              ("Part 3 no restrictions", ),             
              ("Part 3 with restrictions", ),             
              ("Other sections", ),
  ],
  "lookup_col" : ["MentalHealthAct"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Mental Health Act").alias("PrimaryMeasure"),  
      F.col("MentalHealthAct").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
delay_disch_bd  = {  
    "level_list" : [      
              ("Awaiting care coordinator allocation", ),           
              ("Awaiting allocation of community psychiatrist", ),             
              ("Awaiting allocation of social worker", ),             
              ("Awaiting public funding or decision from funding panel", ),             
              ("Awaiting further community or mental health NHS Services not delivered in an acute setting including intermediate care, rehabilitation services, step down service", ),
              ("Awaiting availability of placement in prison or Immigration Removal Centre", ),
              ("Awaiting availability of placement in care home without nursing", ),
              ("Awaiting availability of placement in care home with nursing", ),
              ("Awaiting commencement of care package in usual or temporary place of residence", ),
              ("Awaiting provision of community equipment and/or adaptations to own home", ),
              ("Patient or Family choice", ),
              ("Disputes relating to responsible commissioner for post-discharge care", ),
              ("Disputes relating to post-discharge care pathway between clinical teams and/or care panels", ),
              ("Housing - awaiting availability of private rented accommodation", ),
              ("Housing - awaiting availability of social rented housing via council housing waiting list", ),
              ("Housing - awaiting purchase/sourcing of own home", ),
              ("Housing - Patient NOT eligible for funded care or support", ),
              ("Housing - Awaiting supported accommodation", ),
              ("Housing - Awaiting temporary accommodation from the Local Authority under housing legislation", ),
              ("Awaiting availability of residential children's home (non-secure)", ),
              ("Awaiting availability of secure children's home (welfare or non-welfare)", ),
              ("Awaiting availability of placement in Youth Offender Institution", ),
              ("Child or young person awaiting foster placement", ),
              ("Awaiting Ministry of Justice agreement to proposed placement", ),
              ("Awaiting outcome of legal proceedings under relevant Mental Health legislation", ),
              ("Awaiting Court of Protection proceedings", ),
              ("Awaiting Deprivation of Liberty Safeguards (DOLS) Application", ),
              ("Delay due to consideration of specific court judgements", ),
              ("Awaiting residential special school or college placement", ),
              ("Lack of local education support", ),
              ("Public safety concern unrelated to clinical treatment need (care team and/or ministry of justice)", ),
              ("Highly bespoke housing and/or care arrangements not available in the community", ),
              ("No lawful support available in the community excluding social care", ),
              ("No social care support including social care funded placement", ),
              ("Delays to NHS-led assessments in the community", ),
              ("Hospital staff shortages", ),
              ("Delays to non-NHS led assessments in the community", ),
              ("Reason not known", ),
  ],
  "lookup_col" : ["DelayDisch"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Delayed Discharges").alias("PrimaryMeasure"),  
      F.col("DelayDisch").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
restr_bd  = {
    "level_list" : [      
              ("Chemical restraint - Injection (Non Rapid Tranquillisation)", ),           
              ("Chemical restraint - Injection (Rapid Tranquillisation)", ),             
              ("Chemical restraint - Oral", ),             
              ("Chemical restraint - Other (not listed)", ),             
              ("Mechanical restraint", ),
              ("Physical restraint - Prone", ),
              ("Physical restraint - Kneeling", ),
              ("Physical restraint - Restrictive escort", ),  
              ("Physical restraint - Seated", ),           
              ("Physical restraint - Side", ),             
              ("Physical restraint - Standing", ),             
              ("Physical restraint - Supine", ),             
              ("Physical restraint - Other (not listed)", ),
              ("Seclusion", ),
              ("Segregation", ),
              ("No restraint type recorded", ), 
  ],
  "lookup_col" : ["RestraintType"],
    "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Restraints").alias("PrimaryMeasure"),  
      F.col("RestraintType").alias("PrimarySplit"),  
      F.lit("").alias("SecondaryMeasure"),   
      F.lit("").alias("SecondarySplit")
    ],   
}
mha_los_df = combine_breakdowns_to_df(mha_bd, los_bd)
mha_los_bd = {  
  "level_list" : mha_los_df.collect(),
  "lookup_col": mha_los_df.columns,
  "level_fields" : [ 
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Mental Health Act").alias("PrimaryMeasure"),  
      F.col("MentalHealthAct").alias("PrimarySplit"),  
      F.lit("Length of stay").alias("SecondaryMeasure"),   
      F.col("LengthOfStay").alias("SecondarySplit")
    ],   
}
restr_age_df = combine_breakdowns_to_df(restr_bd, age_bd)
restr_age_bd = { 
  "level_list" : restr_age_df.collect(),
  "lookup_col": restr_age_df.columns,
  "level_fields" : [
      F.col("Geography").alias("PrimaryMeasure"),
      F.lit("Restraints").alias("PrimaryMeasure"),  
      F.col("RestraintType").alias("PrimarySplit"),  
      F.lit("Age").alias("SecondaryMeasure"),   
      F.col("Age_Band").alias("SecondarySplit")
    ],   
}
lda_df = spark.table(f"{db_output}.LDA_Counts").filter((F.col("PeriodEnd") == rp_enddate) & (F.col("SOURCE_DB") == db_source) & (F.col("PRODUCT") == "Monthly"))
comm_df = spark.table(f"{db_output}.STP_Region_mapping_post_2020")
 
prov_level_df = lda_df.select("Geography", "OrgCode", "OrgName").filter(F.col("Geography") == "Provider").distinct()
comm_level_df = lda_df.select("Geography", "OrgCode", "OrgName").filter(F.col("Geography") == "Commissioner").distinct()
comm_group_level_df = lda_df.select("Geography", "OrgCode", "OrgName").filter(F.col("Geography") == "Commissioner Groupings").distinct()
 
icb_level_df = (
  comm_df
  .withColumn("Geography", F.lit("ICB"))
  .select("Geography", "STP_CODE", "STP_DESCRIPTION").distinct()
)
comm_region_level_df = (
  comm_df
  .withColumn("Geography", F.lit("Commissioning Region"))
  .select("Geography", "REGION_CODE", "REGION_DESCRIPTION").distinct()
)

# COMMAND ----------

lda_metadata = {  
"National": {
  "level_list": [("National", "National", "National")],
  "lookup_col": ["Geography", "OrgCode", "OrgName"],
  "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),  
      F.col("OrgCode").alias("PrimarySplit"),  
      F.col("OrgName").alias("SecondaryMeasure"),
    ],   
  "breakdowns": [total_bd, age_bd, gender_bd, upper_eth_bd, dist_from_home_bd, ward_sec_bd, plan_disch_date_bd, time_to_plan_disch_bd, resp_care_bd, los_bd, disch_dest_bd, ward_type_bd, mha_bd, delay_disch_bd, restr_bd, mha_los_bd, restr_age_bd],
}, 
"Commissioner": {
  "level_list": comm_level_df.collect(),
  "lookup_col": comm_level_df.columns,
  "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),  
      F.col("OrgCode").alias("PrimarySplit"),  
      F.col("OrgName").alias("SecondaryMeasure"),
    ], 
  "breakdowns": [total_bd]
},
"Commissioner Groupings": {
  "level_list": comm_group_level_df.collect(),
  "lookup_col": comm_group_level_df.columns,
  "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),  
      F.col("OrgCode").alias("PrimarySplit"),  
      F.col("OrgName").alias("SecondaryMeasure"),
    ], 
  "breakdowns": [total_bd]
},
"Provider": {
  "level_list": prov_level_df.collect(),
  "lookup_col": prov_level_df.columns,
  "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),  
      F.col("OrgCode").alias("PrimarySplit"),  
      F.col("OrgName").alias("SecondaryMeasure"),
    ], 
  "breakdowns": [total_bd, los_bd, mha_bd, restr_bd, ward_sec_bd, ward_type_bd]}, 
"ICB": {
  "level_list": icb_level_df.collect(),
  "lookup_col": icb_level_df.columns,
  "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),  
      F.col("STP_CODE").alias("PrimarySplit"),  
      F.col("STP_DESCRIPTION").alias("SecondaryMeasure"),
    ], 
  "breakdowns": [total_bd]
},
"Commissioning Region": {
  "level_list": comm_region_level_df.collect(),
  "lookup_col": comm_region_level_df.columns,
  "level_fields" : [  
      F.col("Geography").alias("PrimaryMeasure"),  
      F.col("REGION_CODE").alias("PrimarySplit"),  
      F.col("REGION_DESCRIPTION").alias("SecondaryMeasure"),
    ], 
  "breakdowns": [total_bd]
},
}

# COMMAND ----------

@dataclass
class LDAMeasureLevel():
  db_output: str
  geography: str
  level_list: list
  level_fields: list
  lookup_columns: list
  
  def create_level_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each necessary measure_id    
    """
    level_df = (
    spark.createDataFrame(
      self.level_list,
      self.lookup_columns
    )
    )
    
    return level_df
  
  def insert_level_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each measure_id using the create_level_dataframe() function
    and then inserts into the $db_output.bbrb_level_values table
    """
    level_df = self.create_level_dataframe()    
    geog_level_df = (
      level_df
      .withColumn("Geography", F.lit(self.geography))
    ) 
    insert_df = geog_level_df.select(self.level_fields)   
    insert_df.write.insertInto(f"{self.db_output}.lda_level_values")
    
  def insert_geography_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each measure_id using the create_level_dataframe() function
    and then inserts into the $db_output.bbrb_level_values table
    """
    level_df = self.create_level_dataframe()
    insert_df = level_df.select(self.level_fields)
    insert_df.write.insertInto(f"{self.db_output}.lda_geography_values")
  
def insert_lda_lookup_values(db_output: str, metadata: dict):
  """  This function is the main function which creates the 
  main mh_monthly_level_values table for each measure_id and breakdown.

  This table is then used as the "skeleton" in which mh_raw values are joined.
  This is to ensure that we have every possible breakdown value for each
  measure and breakdown in the final output.
  i.e. a provider may have submitted to the MHS000Header Table but not the MHS101Referral Table in
  month, therefore, they wouldn't appear in the MHS01 "Provider" breakdown raw output.
  The "skeleton" table ensures cases like this aren't missed in the final output

  Example:
  jan22perf = MHRunParameters("mh_clear_collab", "mhsds_database", "2022-01-31")
  insert_mh_monthly_lookup_values(jan22perf.db_output, jan22perf.db_source, jan22perf.rp_enddate, jan22perf.end_month_id, common_measure_ids)
  >> all breakdown and primary/secondary level for each measure inserted into the mh_monthly_level_values table
  """

  spark.sql(f"TRUNCATE TABLE {db_output}.lda_csv_lookup")
  column_order = ["Geography","OrgCode","OrgName","PrimaryMeasure","PrimarySplit","SecondaryMeasure","SecondarySplit"] 

  for geography in lda_metadata:
    geog_info = lda_metadata[geography]
    geog_list = geog_info["level_list"]
    geog_fields = geog_info["level_fields"]
    geog_cols = geog_info["lookup_col"]
    geog = LDAMeasureLevel(db_output, geography, geog_list, geog_fields, geog_cols)
    geog.insert_geography_dataframe() #all org_code and org_names inserted for the geography
    breakdowns = geog_info["breakdowns"]
    for breakdown in breakdowns:
      lvl_list = breakdown["level_list"]
      lvl_fields = breakdown["level_fields"]
      lvl_cols = breakdown["lookup_col"]
      #set up MHMeasureLevel class
      lvl = LDAMeasureLevel(db_output, geography, lvl_list, lvl_fields, lvl_cols)
      #create level_df to insert into mh_monthly_level_values
      lvl.insert_level_dataframe()

  geog_df = spark.table(f"{db_output}.lda_geography_values")
  lvl_df = spark.table(f"{db_output}.lda_level_values")
  geog_lvl_df = geog_df.join(lvl_df, on="Geography")
  geog_lvl_df.write.insertInto(f"{db_output}.lda_csv_lookup")

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.lda_geography_values;
 TRUNCATE TABLE $db_output.lda_level_values;
 TRUNCATE TABLE $db_output.lda_csv_lookup;

# COMMAND ----------

insert_lda_lookup_values(db_output, lda_metadata)

# COMMAND ----------

import pandas as pd
import io
import numpy as np
 
data = """
TableNumber,PrimaryMeasure,PrimaryMeasureNumber,PrimarySplit,SecondaryMeasure,SecondaryMeasureNumber,SecondarySplit
1,Total,1,Total,,,
2,Age,1,Under 18,,,
2,Age,2,18-24,,,
2,Age,3,25-34,,,
2,Age,4,35-44,,,
2,Age,5,45-54,,,
2,Age,6,55-64,,,
2,Age,7,65 and Over,,,
2,Age,8,Unknown,,,
3,Gender,1,Male,,,
3,Gender,2,Female,,,
3,Gender,3,Not stated,,,
3,Gender,4,Unknown,,,
4,Ethnicity,1,White,,,
4,Ethnicity,2,Mixed,,,
4,Ethnicity,3,Asian,,,
4,Ethnicity,4,Black,,,
4,Ethnicity,5,Other,,,
4,Ethnicity,6,Not Stated,,,
4,Ethnicity,7,Unknown,,,
5,Distance from Home,1,Up to 10km,,,
5,Distance from Home,2,11-20km,,,
5,Distance from Home,3,21-50km,,,
5,Distance from Home,4,51-100km,,,
5,Distance from Home,5,Over 100km,,,
5,Distance from Home,6,Unknown,,,
6,Ward security,1,General,,,
6,Ward security,2,Low Secure,,,
6,Ward security,3,Medium Secure,,,
6,Ward security,4,High Secure,,,
6,Ward security,5,Unknown,,,
7,Planned Discharge Date Present,1,Present,,,
7,Planned Discharge Date Present,2,Not present,,,
8,Time to Planned Discharge,1,No planned discharge,,,
8,Time to Planned Discharge,2,Planned discharge overdue,,,
8,Time to Planned Discharge,3,0 to 3 months,,,
8,Time to Planned Discharge,4,3 to 6 months,,,
8,Time to Planned Discharge,5,6 to 12 months,,,
8,Time to Planned Discharge,6,1 to 2 years,,,
8,Time to Planned Discharge,7,2 to 5 years,,,
8,Time to Planned Discharge,8,Over 5 years,,,
9,Respite care,1,Admitted for respite care,,,
9,Respite care,2,Not admitted for respite care,,,
10,Length of stay,1,0-3 days,,,
10,Length of stay,10,5-10 years,,,
10,Length of stay,11,10+ years,,,
10,Length of stay,12,Unknown,,,
10,Length of stay,2,4-7 days,,,
10,Length of stay,3,1-2 weeks,,,
10,Length of stay,4,2-4 weeks,,,
10,Length of stay,5,1-3 months,,,
10,Length of stay,6,3-6 months,,,
10,Length of stay,7,6-12 months,,,
10,Length of stay,8,1-2 years,,,
10,Length of stay,9,2-5 years,,,
11,Discharge Destination Grouped,1,Community,,,
11,Discharge Destination Grouped,2,Hospital,,,
11,Discharge Destination Grouped,3,Penal Establishment / Court,,,
11,Discharge Destination Grouped,4,Hospice,,,
11,Discharge Destination Grouped,5,Not Applicable,,,
11,Discharge Destination Grouped,6,Patient died,,,
11,Discharge Destination Grouped,7,Organisation responsible for forced repatriation,,,
11,Discharge Destination Grouped,8,Not Known,,,
12,Ward Type,1,Child and adolescent mental health ward,,,
12,Ward Type,2,Paediatric ward,,,
12,Ward Type,3,Adult mental health ward,,,
12,Ward Type,4,Non mental health ward,,,
12,Ward Type,5,Learning disabilities ward,,,
12,Ward Type,6,Older peoples mental health ward,,,
12,Ward Type,7,Unknown,,,
13,Mental Health Act,1,Informal,,,
13,Mental Health Act,2,Part 2,,,
13,Mental Health Act,3,Part 3 no restrictions,,,
13,Mental Health Act,4,Part 3 with restrictions,,,
13,Mental Health Act,5,Other sections,,,
14,Delayed Discharges,1,Awaiting care coordinator allocation,,,
14,Delayed Discharges,2,Awaiting allocation of community psychiatrist,,,
14,Delayed Discharges,3,Awaiting allocation of social worker,,,
14,Delayed Discharges,4,Awaiting public funding or decision from funding panel,,,
14,Delayed Discharges,5,"Awaiting further community or mental health NHS Services not delivered in an acute setting including intermediate care, rehabilitation services, step down service",,,
14,Delayed Discharges,6,Awaiting availability of placement in prison or Immigration Removal Centre,,,
14,Delayed Discharges,7,Awaiting availability of placement in care home without nursing,,,
14,Delayed Discharges,8,Awaiting availability of placement in care home with nursing,,,
14,Delayed Discharges,9,Awaiting commencement of care package in usual or temporary place of residence,,,
14,Delayed Discharges,10,Awaiting provision of community equipment and/or adaptations to own home,,,
14,Delayed Discharges,11,Patient or Family choice,,,
14,Delayed Discharges,12,Disputes relating to responsible commissioner for post-discharge care,,,
14,Delayed Discharges,13,Disputes relating to post-discharge care pathway between clinical teams and/or care panels,,,
14,Delayed Discharges,14,Housing - awaiting availability of private rented accommodation,,,
14,Delayed Discharges,15,Housing - awaiting availability of social rented housing via council housing waiting list,,,
14,Delayed Discharges,16,Housing - awaiting purchase/sourcing of own home,,,
14,Delayed Discharges,17,Housing - Patient NOT eligible for funded care or support,,,
14,Delayed Discharges,18,Housing - Awaiting supported accommodation,,,
14,Delayed Discharges,19,Housing - Awaiting temporary accommodation from the Local Authority under housing legislation,,,
14,Delayed Discharges,20,Awaiting availability of residential children's home (non-secure),,,
14,Delayed Discharges,21,Awaiting availability of secure children's home (welfare or non-welfare),,,
14,Delayed Discharges,22,Awaiting availability of placement in Youth Offender Institution,,,
14,Delayed Discharges,23,Child or young person awaiting foster placement,,,
14,Delayed Discharges,24,Awaiting Ministry of Justice agreement to proposed placement,,,
14,Delayed Discharges,25,Awaiting outcome of legal proceedings under relevant Mental Health legislation,,,
14,Delayed Discharges,26,Awaiting Court of Protection proceedings,,,
14,Delayed Discharges,27,Awaiting Deprivation of Liberty Safeguards (DOLS) Application,,,
14,Delayed Discharges,28,Delay due to consideration of specific court judgements,,,
14,Delayed Discharges,29,Awaiting residential special school or college placement,,,
14,Delayed Discharges,30,Lack of local education support,,,
14,Delayed Discharges,31,Public safety concern unrelated to clinical treatment need (care team and/or ministry of justice),,,
14,Delayed Discharges,32,Highly bespoke housing and/or care arrangements not available in the community,,,
14,Delayed Discharges,33,No lawful support available in the community excluding social care,,,
14,Delayed Discharges,34,No social care support including social care funded placement,,,
14,Delayed Discharges,35,Delays to NHS-led assessments in the community,,,
14,Delayed Discharges,36,Hospital staff shortages,,,
14,Delayed Discharges,37,Delays to non-NHS led assessments in the community,,,
14,Delayed Discharges,38,Reason not known,,,
15,Restraints,1,No restraint type recorded,,,
15,Restraints,10,Physical restraint - Seated,,,
15,Restraints,11,Physical restraint - Side,,,
15,Restraints,12,Physical restraint - Standing,,,
15,Restraints,13,Physical restraint - Supine,,,
15,Restraints,14,Physical restraint - Other (not listed),,,
15,Restraints,15,Seclusion,,,
15,Restraints,16,Segregation,,,
15,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),,,
15,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),,,
15,Restraints,4,Chemical restraint - Oral,,,
15,Restraints,5,Chemical restraint - Other (not listed),,,
15,Restraints,6,Mechanical restraint,,,
15,Restraints,7,Physical restraint - Prone,,,
15,Restraints,8,Physical restraint - Kneeling,,,
15,Restraints,9,Physical restraint - Restrictive escort,,,
50,Mental Health Act,1,Informal,Length of stay,1,0-3 days
50,Mental Health Act,1,Informal,Length of stay,10,5-10 years
50,Mental Health Act,1,Informal,Length of stay,11,10+ years
50,Mental Health Act,1,Informal,Length of stay,12,Unknown
50,Mental Health Act,1,Informal,Length of stay,2,4-7 days
50,Mental Health Act,1,Informal,Length of stay,3,1-2 weeks
50,Mental Health Act,1,Informal,Length of stay,4,2-4 weeks
50,Mental Health Act,1,Informal,Length of stay,5,1-3 months
50,Mental Health Act,1,Informal,Length of stay,6,3-6 months
50,Mental Health Act,1,Informal,Length of stay,7,6-12 months
50,Mental Health Act,1,Informal,Length of stay,8,1-2 years
50,Mental Health Act,1,Informal,Length of stay,9,2-5 years
50,Mental Health Act,2,Part 2,Length of stay,1,0-3 days
50,Mental Health Act,2,Part 2,Length of stay,10,5-10 years
50,Mental Health Act,2,Part 2,Length of stay,11,10+ years
50,Mental Health Act,2,Part 2,Length of stay,2,4-7 days
50,Mental Health Act,2,Part 2,Length of stay,3,1-2 weeks
50,Mental Health Act,2,Part 2,Length of stay,4,2-4 weeks
50,Mental Health Act,2,Part 2,Length of stay,5,1-3 months
50,Mental Health Act,2,Part 2,Length of stay,6,3-6 months
50,Mental Health Act,2,Part 2,Length of stay,7,6-12 months
50,Mental Health Act,2,Part 2,Length of stay,8,1-2 years
50,Mental Health Act,2,Part 2,Length of stay,9,2-5 years
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,10,5-10 years
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,11,10+ years
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,2,4-7 days
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,4,2-4 weeks
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,5,1-3 months
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,6,3-6 months
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,7,6-12 months
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,8,1-2 years
50,Mental Health Act,3,Part 3 no restrictions,Length of stay,9,2-5 years
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,1,0-3 days
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,10,5-10 years
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,11,10+ years
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,2,4-7 days
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,3,1-2 weeks
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,4,2-4 weeks
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,5,1-3 months
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,6,3-6 months
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,7,6-12 months
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,8,1-2 years
50,Mental Health Act,4,Part 3 with restrictions,Length of stay,9,2-5 years
50,Mental Health Act,5,Other sections,Length of stay,1,0-3 days
50,Mental Health Act,5,Other sections,Length of stay,10,5-10 years
50,Mental Health Act,5,Other sections,Length of stay,11,10+ years
50,Mental Health Act,5,Other sections,Length of stay,2,4-7 days
50,Mental Health Act,5,Other sections,Length of stay,3,1-2 weeks
50,Mental Health Act,5,Other sections,Length of stay,4,2-4 weeks
50,Mental Health Act,5,Other sections,Length of stay,5,1-3 months
50,Mental Health Act,5,Other sections,Length of stay,6,3-6 months
50,Mental Health Act,5,Other sections,Length of stay,7,6-12 months
50,Mental Health Act,5,Other sections,Length of stay,8,1-2 years
50,Mental Health Act,5,Other sections,Length of stay,9,2-5 years
51,Restraints,1,No restraint type recorded,Age,1,Under 18
51,Restraints,1,No restraint type recorded,Age,2,18-24
51,Restraints,1,No restraint type recorded,Age,3,25-34
51,Restraints,1,No restraint type recorded,Age,4,35-44
51,Restraints,1,No restraint type recorded,Age,5,45-54
51,Restraints,1,No restraint type recorded,Age,6,55-64
51,Restraints,1,No restraint type recorded,Age,7,65 and Over
51,Restraints,1,No restraint type recorded,Age,8,Unknown
51,Restraints,10,Physical restraint - Seated,Age,1,Under 18
51,Restraints,10,Physical restraint - Seated,Age,2,18-24
51,Restraints,10,Physical restraint - Seated,Age,3,25-34
51,Restraints,10,Physical restraint - Seated,Age,4,35-44
51,Restraints,10,Physical restraint - Seated,Age,5,45-54
51,Restraints,10,Physical restraint - Seated,Age,6,55-64
51,Restraints,10,Physical restraint - Seated,Age,7,65 and Over
51,Restraints,11,Physical restraint - Side,Age,1,Under 18
51,Restraints,11,Physical restraint - Side,Age,2,18-24
51,Restraints,11,Physical restraint - Side,Age,3,25-34
51,Restraints,11,Physical restraint - Side,Age,4,35-44
51,Restraints,11,Physical restraint - Side,Age,5,45-54
51,Restraints,11,Physical restraint - Side,Age,6,55-64
51,Restraints,11,Physical restraint - Side,Age,7,65 and Over
51,Restraints,12,Physical restraint - Standing,Age,1,Under 18
51,Restraints,12,Physical restraint - Standing,Age,2,18-24
51,Restraints,12,Physical restraint - Standing,Age,3,25-34
51,Restraints,12,Physical restraint - Standing,Age,4,35-44
51,Restraints,12,Physical restraint - Standing,Age,5,45-54
51,Restraints,12,Physical restraint - Standing,Age,6,55-64
51,Restraints,12,Physical restraint - Standing,Age,7,65 and Over
51,Restraints,13,Physical restraint - Supine,Age,1,Under 18
51,Restraints,13,Physical restraint - Supine,Age,2,18-24
51,Restraints,13,Physical restraint - Supine,Age,3,25-34
51,Restraints,13,Physical restraint - Supine,Age,4,35-44
51,Restraints,13,Physical restraint - Supine,Age,5,45-54
51,Restraints,13,Physical restraint - Supine,Age,6,55-64
51,Restraints,13,Physical restraint - Supine,Age,7,65 and Over
51,Restraints,14,Physical restraint - Other (not listed),Age,1,Under 18
51,Restraints,14,Physical restraint - Other (not listed),Age,2,18-24
51,Restraints,14,Physical restraint - Other (not listed),Age,3,25-34
51,Restraints,14,Physical restraint - Other (not listed),Age,4,35-44
51,Restraints,14,Physical restraint - Other (not listed),Age,5,45-54
51,Restraints,14,Physical restraint - Other (not listed),Age,6,55-64
51,Restraints,15,Seclusion,Age,1,Under 18
51,Restraints,15,Seclusion,Age,2,18-24
51,Restraints,15,Seclusion,Age,3,25-34
51,Restraints,15,Seclusion,Age,4,35-44
51,Restraints,15,Seclusion,Age,5,45-54
51,Restraints,15,Seclusion,Age,6,55-64
51,Restraints,16,Segregation,Age,1,Under 18
51,Restraints,16,Segregation,Age,2,18-24
51,Restraints,16,Segregation,Age,3,25-34
51,Restraints,16,Segregation,Age,4,35-44
51,Restraints,16,Segregation,Age,5,45-54
51,Restraints,16,Segregation,Age,6,55-64
51,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),Age,1,Under 18
51,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),Age,2,18-24
51,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),Age,3,25-34
51,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),Age,4,35-44
51,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),Age,5,45-54
51,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),Age,6,55-64
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,1,Under 18
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,2,18-24
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,3,25-34
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,4,35-44
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,5,45-54
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,6,55-64
51,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),Age,7,65 and Over
51,Restraints,4,Chemical restraint - Oral,Age,1,Under 18
51,Restraints,4,Chemical restraint - Oral,Age,2,18-24
51,Restraints,4,Chemical restraint - Oral,Age,3,25-34
51,Restraints,4,Chemical restraint - Oral,Age,4,35-44
51,Restraints,4,Chemical restraint - Oral,Age,5,45-54
51,Restraints,4,Chemical restraint - Oral,Age,6,55-64
51,Restraints,4,Chemical restraint - Oral,Age,7,65 and Over
51,Restraints,5,Chemical restraint - Other (not listed),Age,2,18-24
51,Restraints,5,Chemical restraint - Other (not listed),Age,3,25-34
51,Restraints,5,Chemical restraint - Other (not listed),Age,4,35-44
51,Restraints,5,Chemical restraint - Other (not listed),Age,6,55-64
51,Restraints,6,Mechanical restraint,Age,2,18-24
51,Restraints,6,Mechanical restraint,Age,3,25-34
51,Restraints,6,Mechanical restraint,Age,4,35-44
51,Restraints,7,Physical restraint - Prone,Age,1,Under 18
51,Restraints,7,Physical restraint - Prone,Age,2,18-24
51,Restraints,7,Physical restraint - Prone,Age,3,25-34
51,Restraints,7,Physical restraint - Prone,Age,4,35-44
51,Restraints,7,Physical restraint - Prone,Age,5,45-54
51,Restraints,7,Physical restraint - Prone,Age,6,55-64
51,Restraints,7,Physical restraint - Prone,Age,7,65 and Over
51,Restraints,8,Physical restraint - Kneeling,Age,1,Under 18
51,Restraints,8,Physical restraint - Kneeling,Age,2,18-24
51,Restraints,8,Physical restraint - Kneeling,Age,3,25-34
51,Restraints,8,Physical restraint - Kneeling,Age,4,35-44
51,Restraints,8,Physical restraint - Kneeling,Age,6,55-64
51,Restraints,9,Physical restraint - Restrictive escort,Age,1,Under 18
51,Restraints,9,Physical restraint - Restrictive escort,Age,2,18-24
51,Restraints,9,Physical restraint - Restrictive escort,Age,3,25-34
51,Restraints,9,Physical restraint - Restrictive escort,Age,4,35-44
51,Restraints,9,Physical restraint - Restrictive escort,Age,5,45-54
51,Restraints,9,Physical restraint - Restrictive escort,Age,6,55-64
51,Restraints,9,Physical restraint - Restrictive escort,Age,7,65 and Over
70,Total,1,Total,,,
71,Length of stay,1,0-3 days,,,
71,Length of stay,10,5-10 years,,,
71,Length of stay,11,10+ years,,,
71,Length of stay,12,Unknown,,,
71,Length of stay,2,4-7 days,,,
71,Length of stay,3,1-2 weeks,,,
71,Length of stay,4,2-4 weeks,,,
71,Length of stay,5,1-3 months,,,
71,Length of stay,6,3-6 months,,,
71,Length of stay,7,6-12 months,,,
71,Length of stay,8,1-2 years,,,
71,Length of stay,9,2-5 years,,,
72,Ward Type,1,Child and adolescent mental health ward,,,
72,Ward Type,2,Paediatric ward,,,
72,Ward Type,3,Adult mental health ward,,,
72,Ward Type,4,Non mental health ward,,,
72,Ward Type,5,Learning disabilities ward,,,
72,Ward Type,6,Older peoples mental health ward,,,
72,Ward Type,7,Unknown,,,
73,Ward security,1,General,,,
73,Ward security,2,Low Secure,,,
73,Ward security,3,Medium Secure,,,
73,Ward security,4,High Secure,,,
73,Ward security,5,Unknown,,,
74,Restraints,1,No restraint type recorded,,,
74,Restraints,10,Physical restraint - Seated,,,
74,Restraints,11,Physical restraint - Side,,,
74,Restraints,12,Physical restraint - Standing,,,
74,Restraints,13,Physical restraint - Supine,,,
74,Restraints,14,Physical restraint - Other (not listed),,,
74,Restraints,15,Seclusion,,,
74,Restraints,16,Segregation,,,
74,Restraints,2,Chemical restraint - Injection (Non Rapid Tranquillisation),,,
74,Restraints,3,Chemical restraint - Injection (Rapid Tranquillisation),,,
74,Restraints,4,Chemical restraint - Oral,,,
74,Restraints,5,Chemical restraint - Other (not listed),,,
74,Restraints,6,Mechanical restraint,,,
74,Restraints,7,Physical restraint - Prone,,,
74,Restraints,8,Physical restraint - Kneeling,,,
74,Restraints,9,Physical restraint - Restrictive escort,,,
80,Total,1,Total,,,
90,Total,1,Total,,,
100,Total,1,Total,,,
101,Total,1,Total,,,
"""
df = pd.read_csv(io.StringIO(data), header=0, delimiter=',')
df["SecondaryMeasureNumber"] = df["SecondaryMeasureNumber"].fillna(-1)
df["SecondaryMeasureNumber"] = df["SecondaryMeasureNumber"].astype(int)
df["SecondaryMeasureNumber"] = df["SecondaryMeasureNumber"].astype(str)
df["SecondaryMeasureNumber"] = df["SecondaryMeasureNumber"].replace('-1', np.nan)
df2 = df.fillna("")
spark_df = spark.createDataFrame(df2)
spark.createDataFrame(df2).write.insertInto(f"{db_output}.lda_measure_number_ref",overwrite=True)