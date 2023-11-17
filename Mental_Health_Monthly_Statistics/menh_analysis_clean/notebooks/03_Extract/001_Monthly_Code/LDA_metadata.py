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
              ("Unknown", ),
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
              ("Non-NHS Hospice", ),
              ("Not Applicable", ),
              ("Patient died", ),
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
              ("Awaiting public funding", ),             
              ("Awaiting further non-acute (including community and mental health) NHS care", ),             
              ("Awaiting Care Home Without Nursing placement or availability", ),             
              ("Awaiting Care Home With Nursing placement or availability", ),
              ("Awaiting care package in own home", ),
              ("Awaiting community equipment, telecare and/or adaptations", ),
              ("Patient or Family choice - (reason not stated by patient or family)", ),  
              ("Patient or Family choice - non-acute (including community and mental health) NHS care", ),           
              ("Patient or Family choice - Care Home Without Nursing placement", ),             
              ("Patient or Family choice - Care Home With Nursing placement", ),             
              ("Patient or Family choice - care package in own home", ),             
              ("Patient or Family choice - community equipment, telecare and/or adaptations", ),
              ("Patient or Family Choice - general needs housing/private landlord acceptance", ),
              ("Patient or Family choice - supported accommodation", ),
              ("Patient or Family choice - emergency accommodation from the local authority under the housing act", ),
              ("Patient or Family choice - child or young person awaiting social care or family placement", ),           
              ("Patient or Family choice - ministry of justice agreement/permission of proposed placement", ),             
              ("Disputes", ),             
              ("Housing - awaiting availability of general needs housing/private landlord accommodation acceptance", ),             
              ("Housing - single homeless patients or asylum seekers NOT covered by care act", ),
              ("Housing - awaiting supported accommodation", ),
              ("Housing - awaiting emergency accommodation from the local authority under the housing act", ),           
              ("Child or young person awaiting social care or family placement", ),             
              ("Awaiting ministry of justice agreement/permission of proposed placement", ),             
              ("Awaiting outcome of legal requirements (mental capacity/mental health legislation)", ),             
              ("Awaiting residential special school or college placement or availability", ),
              ("Lack of local education support", ),
              ("Public safety concern unrelated to clinical treatment need (care team)", ),
              ("Public safety concern unrelated to clinical treatment need (Ministry of Justice)", ),           
              ("No lawful community care package available", ),             
              ("Lack of health care service provision", ),             
              ("Lack of social care support", ),             
              ("No reason given", ), 
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
  jan22perf = MHRunParameters("mh_clear_collab", "$mhsds_database", "2022-01-31")
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