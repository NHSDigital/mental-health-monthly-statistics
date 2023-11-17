# Databricks notebook source
from dataclasses import dataclass, field
import pyspark.sql.types as T
import pyspark.sql.functions as F
import json
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta
from pyspark.sql.column import Column
from pyspark.sql import DataFrame as df

# COMMAND ----------

def str2dt(date: str) -> T.DateType():
  """  This function converts a string into a datetime in the 
  format datetime(YYYY, M, D, H, M)
  
  Example:
  str2dt("2021-09-01")
  >> datetime.datetime(2021, 9, 1, 0, 0)
  """
  date_format = '%Y-%m-%d'
  date_conv = datetime.strptime(date, date_format)
  
  return date_conv
 
def dt2str(date: T.DateType()) -> str:
  """
  This function converts a datetime to a string in the 
  format "YYYY-MM-DD"
  
  Example:
  dt2str(datetime(2022, 1, 1, 0, 0))
  >> "2022-01-01"
  """ 
  date_format = '%Y-%m-%d'
  date_conv = date.strftime(date_format)
  
  return date_conv
 
def first_day(date: T.DateType()) -> T.DateType():
  """  This function gets the first day of the month
  for any given date
  
  Example:
  get_first_day(datetime(2021, 1, 15, 0, 0))
  >> datetime(2021, 1, 1, 0, 0)
  """
  first_day = date.replace(day=1) ##first day
  
  return first_day

def last_day(date: T.DateType()) -> T.DateType():
  """  This function gets the last day of the month
  for any given date
  
  Example:
  get_last_day(datetime(2021, 1, 15, 0, 0))
  >> datetime(2021, 1, 31, 0, 0)
  """
  last_day_params = calendar.monthrange(date.year, date.month)[1]
  last_day = date.replace(day=last_day_params) ##last day
  
  return last_day
 
def add_months(date: T.DateType(), rp_length_num: int) -> T.DateType():
  """ This functions adds required amount of 
  months from a given date  
  
  Example:
  add_months(datetime(2021, 1, 15, 0, 0), 3)
  >> datetime(2020, 10, 15, 0, 0)
  """
  new_month = date + relativedelta(months=rp_length_num) ##minus rp_length_num months
  
  return new_month
 
def minus_months(date: T.DateType(), rp_length_num: int) -> T.DateType():
  """ This functions minuses required amount of 
  months from a given date  
  
  Example:
  minus_months(datetime(2021, 1, 15, 0, 0), 3)
  >> datetime(2020, 10, 15, 0, 0)
  """
  new_month = date - relativedelta(months=rp_length_num) ##minus rp_length_num months
  
  return new_month

def is_numeric(s):
    try:
        float(s)
        return 1
    except ValueError:
        return 0
spark.udf.register("is_numeric", is_numeric)

# COMMAND ----------

def get_rp_enddate(rp_startdate: str) -> str:
  """ This function gets the end of the month from
  the reporting period start date
  
  Example:
  get_rp_enddate("2021-10-01")
  >> "2021-10-31"
  """
  rp_startdate_dt = str2dt(rp_startdate)
  rp_enddate_dt = last_day(rp_startdate_dt)
  rp_enddate = dt2str(rp_enddate_dt)
  
  return rp_enddate  

def get_pub_month(rp_startdate: str, status: str) -> str:
  """ This function gets the Publication year and month
  in the format YYYYMM from the reporting period start date 
  and submission window
  
  Example:
  get_pub_month("2021-10-01", "Performance")
  >> "202201"
  """ 
  pub_month = str2dt(rp_startdate)
  
  if status == "Provisional":
    pub_month = add_months(pub_month, 2)
  elif status == "Performance" or status == "Final":
    pub_month = add_months(pub_month, 3)
  else:
    return ValueError("Invalid submission window name inputted")

  pub_month = dt2str(pub_month)
  pub_month = pub_month[0:4] + pub_month[5:7]
  return pub_month
 
def get_qtr_startdate(rp_startdate: str) -> str:
  """  This functions gets the ReportingPeriodStartDate of a
  Quarterly Reporting Period
  
  Example:
  get_qtr_startdate("2020-03-01")
  >> "2020-01-01"
  """
  rp_startdate_dt = str2dt(rp_startdate) ##to datetime
  rp_qtr_startdate_dt = minus_months(rp_startdate_dt, 2) ##minus 2 months
  rp_qtr_startdate = dt2str(rp_qtr_startdate_dt) ##to string
  
  return rp_qtr_startdate
 
def get_12m_startdate(rp_startdate: str) -> str:
  """  This functions gets the ReportingPeriodStartDate of a
  12-month Reporting Period
  
  Example:
  get_12m_startdate("2020-03-01")
  >> "2019-04-01"
  """
  rp_startdate_dt = str2dt(rp_startdate) ##to datetime
  rp_12m_startdate_dt = minus_months(rp_startdate_dt, 11) ##minus 11 months
  rp_12m_startdate = dt2str(rp_12m_startdate_dt) ##to string
  
  return rp_12m_startdate

def get_month_ids(rp_startdate: str) -> int:
  """  This function gets the end_month_id and start_month_id 
  parameters from the rp_startdate. This assumes a reporting 
  period of 12 months maximum
  
  Example:
  get_month_ids("2021-09-01")
  >> 1458, 1447
  """
  rp_startdate_dt = str2dt(rp_startdate)
  start_date_dt = datetime(1900, 4, 1)
  time_diff = relativedelta(rp_startdate_dt, start_date_dt)
  end_month_id = int(time_diff.years * 12 + time_diff.months + 1)
  start_month_id = end_month_id - 11
  
  return end_month_id, start_month_id
 
def get_financial_yr_start(rp_startdate: str) -> str:
    """ This function returns the date of the start
    of the financial year using the start of the
    reporting period
    
    Example:
    get_financial_yr_start("2022-05-01")
    >> "2022-04-01"
    """
    rp_startdate_dt = str2dt(rp_startdate)
    if rp_startdate_dt.month > 3:
      financial_year_start = datetime(rp_startdate_dt.year,4,1)
    else:
      financial_year_start = datetime(rp_startdate_dt.year-1,4,1)
      
    return dt2str(financial_year_start)

def get_year_of_count(rp_startdate):
  '''
  This function returns the year_of_count which should be used to extract data from $reference_data.ONS_POPULATION_V2.  
  If the financial_yr_start is greater than the existing max(current_year) in $reference_data.ONS_POPULATION_V2 then use
  current_year = max(current_year).
  '''
  current_year = get_financial_yr_start(rp_startdate)[0:4]
  max_year_of_count = spark.sql(f"select max(year_of_count) AS year_of_count from $reference_data.ONS_POPULATION_V2")
  max_year_of_count_value = max_year_of_count.first()["year_of_count"]
  year_of_count = current_year
  if (year_of_count > max_year_of_count_value):
    year_of_count = max_year_of_count_value
 
  return year_of_count 

# COMMAND ----------

@dataclass
class MHRunParameters:
  db_output: str
  db_source: str
  status: str  
  rp_startdate: str
  product: str
  pub_month: str = field(init=False) 
  rp_enddate: str = field(init=False)   
  end_month_id: int = field(init=False)
  start_month_id: int = field(init=False)  
  rp_startdate_1m: str = field(init=False)  
  rp_startdate_qtr: str = field(init=False)  
  rp_startdate_12m: str = field(init=False)
  financial_year_start: str = field(init=False)
  year_of_count: int = field(init=False)
  $reference_data: str = "$reference_data"
   
  def __post_init__(self):
    self.pub_month = get_pub_month(self.rp_startdate, self.status)
    self.rp_enddate = get_rp_enddate(self.rp_startdate)
    self.end_month_id, self.start_month_id = get_month_ids(self.rp_startdate)
    self.rp_startdate_1m = self.rp_startdate
    self.rp_startdate_qtr = get_qtr_startdate(self.rp_startdate)
    self.rp_startdate_12m = get_12m_startdate(self.rp_startdate)
    self.financial_year_start = get_financial_yr_start(self.rp_startdate)
    self.year_of_count = get_year_of_count(self.rp_startdate)
  
  def as_dict(self):
    json_dump = json.dumps(self, sort_keys=False, default=lambda o: o.__dict__)
    return json.loads(json_dump)
  
  def run_pub():
    return None

# COMMAND ----------

def mh_freq_to_rp_startdate(mh_run_params: object, freq: str) -> str:
  """  This function gets the corresponding rp_startdate for a measure_id
  depending on the "freq" key value in the metadata
  
  Current Frequency values:
  ["M", "Q", "12M"]
  
  Example:
  mh_freq_to_rp_startdate("2022-03-31", "M")
  >> "2022-01-01"
  """
  if freq == "12M":
    rp_startdate = mh_run_params.rp_startdate_12m
  elif freq == "Q":
    rp_startdate = mh_run_params.rp_startdate_qtr
  else: ##Monthly most common frequency
    rp_startdate = mh_run_params.rp_startdate_1m
    
  return rp_startdate

def produce_agg_df(
  db_output: str, 
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    agg_df = (
            prep_df
            .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc)
            .agg(aggregation_field)
            .withColumn("REPORTING_PERIOD_START", F.lit(rp_startdate))
            .withColumn("REPORTING_PERIOD_END", F.lit(rp_enddate))                    
            .withColumn("BREAKDOWN", F.lit(breakdown))
            .withColumn("STATUS", F.lit(status))
            .withColumn("PRIMARY_LEVEL", primary_level)
            .withColumn("PRIMARY_LEVEL_DESCRIPTION", primary_level_desc)
            .withColumn("SECONDARY_LEVEL", secondary_level)
            .withColumn("SECONDARY_LEVEL_DESCRIPTION", secondary_level_desc)
            .withColumn("MEASURE_ID", F.lit(measure_id))
            .withColumn("MEASURE_NAME", F.lit(measure_name))
            .select(*column_order)
   )
    
    return agg_df  

def access_filter_for_breakdown(prep_df: df, breakdown_name: str) -> df:
  """  This function filters the access-related prep table with the relevant
  access ROW_NUMBER() field depending on the breakdown being aggregated i.e. AccessEngRN
  to be equal to 1
  
  NOTE: This function is only currently required for the CMH and CYP Access prep tables
  """ 
  if breakdown_name == "Provider":
    rn_field = "AccessProvRN"
  elif breakdown_name == "CCG of Residence":
    rn_field = "AccessCCGRN"
  elif breakdown_name == "STP of Residence":
    rn_field = "AccessSTPRN"
  elif breakdown_name == "Commissioning Region":
    rn_field = "AccessRegionRN"
  else:
    rn_field = "AccessEngRN"
    
  filt_df = (
    prep_df
    .filter(F.col(rn_field) == 1)
  )
  
  return filt_df

def produce_filter_agg_df(
  db_output: str, 
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    prep_filter_df = (
      prep_df.filter(filter_clause)
    )
    
    agg_df = (
            prep_filter_df
            .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc)
            .agg(aggregation_field)
            .withColumn("REPORTING_PERIOD_START", F.lit(rp_startdate))
            .withColumn("REPORTING_PERIOD_END", F.lit(rp_enddate))                    
            .withColumn("BREAKDOWN", F.lit(breakdown))
            .withColumn("STATUS", F.lit(status))
            .withColumn("PRIMARY_LEVEL", primary_level)
            .withColumn("PRIMARY_LEVEL_DESCRIPTION", primary_level_desc)
            .withColumn("SECONDARY_LEVEL", secondary_level)
            .withColumn("SECONDARY_LEVEL_DESCRIPTION", secondary_level_desc)
            .withColumn("MEASURE_ID", F.lit(measure_id))
            .withColumn("MEASURE_NAME", F.lit(measure_name))
            .select(*column_order)
   )
    
    return agg_df
  
def produce_access_filter_agg_df(
  db_output: str, 
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """     
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    prep_access_df = access_filter_for_breakdown(prep_df, breakdown)
    
    prep_filter_df = (
      prep_access_df.filter(filter_clause)
    )
    
    agg_df = (
            prep_filter_df
            .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc)
            .agg(aggregation_field)
            .withColumn("REPORTING_PERIOD_START", F.lit(rp_startdate))
            .withColumn("REPORTING_PERIOD_END", F.lit(rp_enddate))                    
            .withColumn("BREAKDOWN", F.lit(breakdown))
            .withColumn("STATUS", F.lit(status))
            .withColumn("PRIMARY_LEVEL", primary_level)
            .withColumn("PRIMARY_LEVEL_DESCRIPTION", primary_level_desc)
            .withColumn("SECONDARY_LEVEL", secondary_level)
            .withColumn("SECONDARY_LEVEL_DESCRIPTION", secondary_level_desc)
            .withColumn("MEASURE_ID", F.lit(measure_id))
            .withColumn("MEASURE_NAME", F.lit(measure_name))
            .select(*column_order)
   )
    
    return agg_df
  
def produce_pop_rate_agg_df(
  db_output: str, 
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    agg_df = (
            prep_df
            .filter(F.col("breakdown") == breakdown)
            .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc)
            .agg(aggregation_field)
            .withColumn("REPORTING_PERIOD_START", F.lit(rp_startdate))
            .withColumn("REPORTING_PERIOD_END", F.lit(rp_enddate))                    
            .withColumn("BREAKDOWN", F.lit(breakdown))
            .withColumn("STATUS", F.lit(status))
            .withColumn("PRIMARY_LEVEL", primary_level)
            .withColumn("PRIMARY_LEVEL_DESCRIPTION", primary_level_desc)
            .withColumn("SECONDARY_LEVEL", secondary_level)
            .withColumn("SECONDARY_LEVEL_DESCRIPTION", secondary_level_desc)
            .withColumn("MEASURE_ID", F.lit(measure_id))
            .withColumn("MEASURE_NAME", F.lit(measure_name))
            .select(*column_order)
        )
    
    return agg_df
  
def insert_unsup_agg(agg_df: df, db_output: str, unsup_columns: list, output_table: str) -> None:
  """
  This function uses the aggregation dataframe produced in the different aggregation functions 
  and selects certain columns and inserts them into the required unsuppressed table in measure metadata
  
  Example:
  
  """
  unsup_agg_df = (
    agg_df
    .withColumn("MEASURE_VALUE", F.coalesce(F.col("MEASURE_VALUE"), F.lit(0)))
    .select(*unsup_columns)
  )
  
  unsup_agg_df.write.insertInto(f"{db_output}.{output_table}")
  
def insert_sup_agg(agg_df: df, db_output: str, measure_name: str, sup_columns: list, output_table: str) -> None:
  """
  This function uses the aggregation dataframe produced in the different aggregation functions 
  and selects certain columns and inserts them into the required suppressed table in measure metadata
  
  Example:
  
  """
  sup_agg_df = (
    agg_df
    .withColumn("MEASURE_NAME", F.lit(measure_name))
    .select(*sup_columns)
  )
  
  sup_agg_df.write.insertInto(f"{db_output}.{output_table}")

# COMMAND ----------

@dataclass
class MHMeasureLevel():
  db_output: str
  measure_id: str
  freq: str
  breakdown: str
  level_list: list
  level_fields: list
  lookup_columns: list
  
  def create_level_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each necessary measure_id    
    """
    if self.breakdown in ("CCG - GP Practice or Residence", "CCG of Residence"):
      prep_df = spark.table(f"{self.db_output}.mhsds_stp_mapping")
      level_df = prep_df.select("CCG_CODE", "CCG_NAME").distinct()
      unknown_list = spark.createDataFrame(data=[("UNKNOWN","UNKNOWN") ],schema=["CCG_CODE", "CCG_NAME"])
      level_df = level_df.union(unknown_list).distinct()
      
    elif self.breakdown == "Provider" and self.freq == "M":
      level_df = spark.table(f"{self.db_output}.bbrb_org_daily_latest_mhsds_providers")
    
    elif self.breakdown == "Provider" and self.freq == "Q":
      level_df = spark.table(f"{self.db_output}.bbrb_org_daily_past_quarter_mhsds_providers")
      
    elif self.breakdown == "Provider" and self.freq == "12M":
      level_df = spark.table(f"{self.db_output}.bbrb_org_daily_past_12_months_mhsds_providers")
      
    elif self.breakdown in ("STP of GP Practice or Residence", "STP of Residence"):
      prep_df = spark.table(f"{self.db_output}.mhsds_stp_mapping")
      level_df = prep_df.select("STP_CODE", "STP_NAME").distinct()
      unknown_list = spark.createDataFrame(data=[("UNKNOWN","UNKNOWN") ],schema=["STP_CODE", "STP_NAME"])
      level_df = level_df.union(unknown_list).distinct()
      
    elif self.breakdown == "Commissioning Region":
      prep_df = spark.table(f"{self.db_output}.mhsds_stp_mapping")
      level_df = prep_df.select("REGION_CODE", "REGION_NAME").distinct()
      unknown_list = spark.createDataFrame(data=[("UNKNOWN","UNKNOWN") ],schema=["REGION_CODE", "REGION_NAME"])
      level_df = level_df.union(unknown_list).distinct()
      
    elif self.breakdown == "CCG; Provider":
      prep_df = spark.table(f"{self.db_output}.firstcont_final")
      level_df = prep_df.select("CCG_Code", "CCG_Name", "OrgIDProv", "Provider_Name").distinct()
      
    else:
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
    insert_df = level_df.select(self.level_fields)
    
    insert_df.write.insertInto(f"{self.db_output}.bbrb_level_values")

def insert_bbrb_lookup_values(mh_run_params: object, metadata: dict):
  """  This function is the main function which creates the 
  main mh_monthly_level_values table for each measure_id and breakdown.
  
  This table is then used as the "skeleton" in which mh_raw values are joined.
  This is to ensure that we have every possible breakdown value for each
  measure and breakdown in the final output.
  i.e. a provider may have submitted to the MHS000Header Table but not the MHS101Referral Table in
  month, therefore, they wouldn't appear in the MHS01 "Provider" breakdown raw output.
  The "skeleton" table ensures cases like this aren't missed in the final output
  
  Example:
  jan22perf = MHRunParameters("mh_clear_collab", "$reference_data", "2022-01-31")
  insert_mh_monthly_lookup_values(jan22perf.db_output, jan22perf.db_source, jan22perf.rp_enddate, jan22perf.end_month_id, common_measure_ids)
  >> all breakdown and primary/secondary level for each measure inserted into the mh_monthly_level_values table
  """
  spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.bbrb_csv_lookup")
  column_order = [ 
      "REPORTING_PERIOD_START",
      "REPORTING_PERIOD_END",
      "STATUS",
      "BREAKDOWN",
      "PRIMARY_LEVEL",
      "PRIMARY_LEVEL_DESCRIPTION",
      "SECONDARY_LEVEL",
      "SECONDARY_LEVEL_DESCRIPTION",
      "MEASURE_ID",
      "MEASURE_NAME"
  ] 
  for measure_id in metadata:    
    measure_name = metadata[measure_id]["name"]
    freq = metadata[measure_id]["freq"]
    rp_startdate = mh_freq_to_rp_startdate(mh_run_params, freq)
    breakdowns = metadata[measure_id]["breakdowns"]
    
    for breakdown in breakdowns:        
      breakdown_name = breakdown["breakdown_name"]
      lvl_list = breakdown["level_list"]
      lvl_fields = breakdown["level_fields"]
      lvl_cols = breakdown["lookup_col"]
      print(f"{measure_id}: {breakdown_name}")
      #set up MHMeasureLevel class
      lvl = MHMeasureLevel(mh_run_params.db_output, measure_id, freq, breakdown_name, lvl_list, lvl_fields, lvl_cols)
      #create level_df to insert into mh_monthly_level_values
      lvl.insert_level_dataframe() #all breakdowns and primary levels added for the measure_id
      #add measure_id to dataframe and insert into mh_monthly_lookup
      lvl_df = spark.table(f"{db_output}.bbrb_level_values")
      lvl_measure_df = (
        lvl_df
        .withColumn("REPORTING_PERIOD_START", F.lit(rp_startdate)) #rp_startdate based off frequency
        .withColumn("REPORTING_PERIOD_END", F.lit(mh_run_params.rp_enddate))
        .withColumn("STATUS", F.lit(mh_run_params.status))
        .withColumn("MEASURE_ID", F.lit(measure_id))
        .withColumn("MEASURE_NAME", F.lit(measure_name))
        .select(*column_order)
      )
      #write into final ref table
      lvl_measure_df.write.insertInto(f"{mh_run_params.db_output}.bbrb_csv_lookup")
      #reset level_values table for next measure_id
      spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.bbrb_level_values")                              
      #restart loop
    ##loop ends with full populated levels and breakdowns for each measure id in metadata

# COMMAND ----------

def count_suppression(x: int, base=5) -> str:  
  """  The function has the logic for suppression of MHSDS count measures
  i.e. "denominator" == 0 in measure_metadata
  
  Examples:
  count_suppression(254)
  >> 255
  count_suppression(3)
  >> *
  """
  if x < 5:
    return '*'
  else:
    return str(int(base * round(float(x)/base)))
  
  
def mhsds_suppression(df: df, suppression_type: str, breakdown: str, measure_id: str, rp_enddate: str, status: str, numerator_id: str) -> df:
  """  The function has the logic for suppression of MHSDS count and percent measures
  if numerator_id value of percentage = "*" then percentage value = "*"
  else round to nearest whole number
  
  Example:
  """
  supp_method = F.udf(lambda z: count_suppression(z))
  
  if suppression_type == "count":
    supp_df = (
      df
      .filter(
        (F.col("MEASURE_ID") == measure_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("REPORTING_PERIOD_END") == rp_enddate)
        & (F.col("STATUS") == status)
      )
      .withColumn("MEASURE_VALUE", supp_method(F.col("MEASURE_VALUE")))
    )  
  
  else:
    perc_values = (
      df
      .filter(
        (F.col("MEASURE_ID") == measure_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("REPORTING_PERIOD_END") == rp_enddate)
        & (F.col("STATUS") == status)
      )
    )

    num_values = (
      df
      .filter(
        (F.col("MEASURE_ID") == numerator_id)
        & (F.col("BREAKDOWN") == breakdown)
      )
      .select(
        F.col("BREAKDOWN"), F.col("PRIMARY_LEVEL"), F.col("SECONDARY_LEVEL"), F.col("MEASURE_ID").alias("NUMERATOR_ID"), F.col("MEASURE_VALUE").alias("NUMERATOR_VALUE")
      )
    )

    perc_num_comb = (
      num_values
      .join(perc_values, ["BREAKDOWN", "PRIMARY_LEVEL", "SECONDARY_LEVEL"])
    )

    #percentage suppression logic
    perc_supp_logic = (
    F.when(F.col("NUMERATOR_VALUE_SUPP") == "*", F.lit("*"))
     .otherwise(F.round(F.col("MEASURE_VALUE"), 0)) #round to nearest whole number (0dp)
    )

    supp_df = (
      perc_num_comb
      .withColumn("NUMERATOR_VALUE_SUPP", supp_method(F.col("NUMERATOR_VALUE")))
      .withColumn("MEASURE_VALUE", perc_supp_logic)
    )
    
  return supp_df

# COMMAND ----------

eng_bd = {
    "breakdown_name": "England",
    "primary_level": F.lit("England"),
    "primary_level_desc": F.lit("England"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "level_list": [("England", )],
    "lookup_col": ["England"],
    "level_fields" : [  
      F.col("England").alias("breakdown"),
      F.col("England").alias("primary_level"),  
      F.col("England").alias("primary_level_desc"),  
      F.lit("NONE").alias("secondary_level"),   
      F.lit("NONE").alias("secondary_level_desc")
    ],   
}
age_band_0_19_to_70_bd = {
    "breakdown_name": "England; Age Group",
    "primary_level": F.col("Age_Band"),
    "primary_level_desc": F.col("Age_Band"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [            
              ("0 to 19", ),
              ("20 to 29", ),           
              ("30 to 39", ),             
              ("40 to 49", ),             
              ("50 to 59", ),             
              ("60 to 69", ),              
              ("70 and over", ),
              ("UNKNOWN", ),
  ],
  "lookup_col" : ["Age_Band"],
  "level_fields" : [
    F.lit("England; Age Group").alias("breakdown"),
    F.col("Age_Band").alias("primary_level"),
    F.col("Age_Band").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
gender_bd = {
    "breakdown_name": "England; Gender",
    "primary_level": F.col("Der_Gender"),
    "primary_level_desc": F.col("Der_Gender_Desc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [
              ("1", "Male"),             
              ("2", "Female"),
              ("3", "Non-binary"),
              ("4", "Other (not listed)"),
              ("9", "Indeterminate"),            
              ("UNKNOWN", "UNKNOWN"),
  ],
  "lookup_col" : ["Der_Gender", "Der_Gender_Desc"],
  "level_fields" : [
    F.lit("England; Gender").alias("breakdown"),
    F.col("Der_Gender").alias("primary_level"),
    F.col("Der_Gender_Desc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],  
}
lower_eth_bd = {
    "breakdown_name": "England; Lower Ethnicity",
    "primary_level": F.col("LowerEthnicityCode"),
    "primary_level_desc": F.col("LowerEthnicityName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [
              ("A", "British"),             
              ("B", "Irish"),
              ("C", "Any Other White Background"),
              ("D", "White and Black Caribbean"),
              ("E", "White and Black African"),
              ("F", "White and Asian"),
              ("G", "Any Other mixed background"),
              ("H", "Indian"),
              ("J", "Pakistani"), 
              ("K", "Bangladeshi"),
              ("L", "Any Other Asian background"),
              ("M", "Caribbean"),
              ("N", "African"),
              ("P", "Any Other Black background"),
              ("R", "Chinese"),
              ("S", "Any Other Ethnic Group"),
              ("Z", "Not Stated"),
              ("99", "Not Known"),
              ("UNKNOWN", "UNKNOWN"),
  ],
  "lookup_col" : ["Ethnic_Code", "Ethnic_Name"],
  "level_fields" : [
    F.lit("England; Lower Ethnicity").alias("breakdown"),
    F.col("Ethnic_Code").alias("primary_level"),
    F.col("Ethnic_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")    
  ]
}
upper_eth_bd = {
    "breakdown_name": "England; Upper Ethnicity",
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [            
              ("White", ),
              ("Mixed", ),           
              ("Asian or Asian British", ),             
              ("Black or Black British", ),             
              ("Other Ethnic Groups", ),             
              ("Not Stated", ),              
              ("Not Known", ),
              ("UNKNOWN", ),
  ],
  "lookup_col" : ["UpperEthnicity"],
  "level_fields" : [
    F.lit("England; Upper Ethnicity").alias("breakdown"),
    F.col("UpperEthnicity").alias("primary_level"),
    F.col("UpperEthnicity").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
wnw_eth_bd = {
    "breakdown_name": "England; Ethnicity (White British/Non-White British)",
    "primary_level": F.col("WNW_Ethnicity"),
    "primary_level_desc": F.col("WNW_Ethnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [            
              ("White British", ),
              ("Non-white British", ),           
              ("Missing/invalid", ), 
  ],
  "lookup_col" : ["WNW_Ethnicity"],
  "level_fields" : [
    F.lit("England; Ethnicity (White British/Non-White British)").alias("breakdown"),
    F.col("WNW_Ethnicity").alias("primary_level"),
    F.col("WNW_Ethnicity").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
imd_decile_bd = {
    "breakdown_name": "England; IMD Decile",
    "primary_level": F.col("IMD_Decile"),
    "primary_level_desc": F.col("IMD_Decile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [
              ("01 Most deprived", ),             
              ("02 More deprived", ),
              ("03 More deprived", ),
              ("04 More deprived", ),
              ("05 More deprived", ),
              ("06 Less deprived", ),
              ("07 Less deprived", ),
              ("08 Less deprived", ),
              ("09 Less deprived", ), 
              ("10 Least deprived", ),
              ("UNKNOWN", ),
  ],
  "lookup_col" : ["IMD_Decile_Name"],
  "level_fields" : [
    F.lit("England; IMD Decile").alias("breakdown"),
    F.col("IMD_Decile_Name").alias("primary_level"),
    F.col("IMD_Decile_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
prov_bd = {
    "breakdown_name": "Provider",
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [
    F.lit("Provider").alias("breakdown"),  
    F.col("ORG_CODE").alias("primary_level"),
    F.col("NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
ccg_res_bd = {
    "breakdown_name": "CCG of Residence",
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence").alias("breakdown"),
    F.col("CCG_CODE").alias("primary_level"),
    F.col("CCG_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
ccg_prac_res_bd = {
    "breakdown_name": "CCG - GP Practice or Residence",
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - GP Practice or Residence").alias("breakdown"),
    F.col("CCG_CODE").alias("primary_level"),
    F.col("CCG_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
ccg_prov_bd = {
    "breakdown_name": "CCG; Provider",
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("OrgIDProv"),
    "secondary_level_desc": F.col("Provider_Name"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG; Provider").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("OrgIDProv").alias("secondary_level"), 
    F.col("Provider_Name").alias("secondary_level_desc")
    ],
}
stp_res_bd = {
    "breakdown_name": "STP of Residence",
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
stp_prac_res_bd = {
    "breakdown_name": "STP of GP Practice or Residence",
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of GP Practice or Residence").alias("breakdown"),
    F.col("STP_CODE").alias("primary_level"),
    F.col("STP_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
comm_region_bd = {
    "breakdown_name": "Commissioning Region",
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region").alias("breakdown"),
    F.col("REGION_CODE").alias("primary_level"),
    F.col("REGION_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}
pop_rate_eng_bd = {
    "breakdown_name": "England",
    "primary_level": F.col("level_code"),
    "primary_level_desc": F.col("level_name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list": [("England", )],
    "lookup_col": ["England"],
    "level_fields" : [  
      F.col("England").alias("breakdown"),
      F.col("England").alias("primary_level"),  
      F.col("England").alias("primary_level_desc"),  
      F.lit("NONE").alias("secondary_level"),   
      F.lit("NONE").alias("secondary_level_desc")
    ],   
}
pop_rate_age_band_0_19_to_70_bd = {
    "breakdown_name": "England; Age Group",
    "primary_level": F.col("level_code"),
    "primary_level_desc": F.col("level_name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [            
              ("0 to 19", ),
              ("20 to 29", ),           
              ("30 to 39", ),             
              ("40 to 49", ),             
              ("50 to 59", ),             
              ("60 to 69", ),              
              ("70 and over", ),
              ("UNKNOWN", ),
  ],
  "lookup_col" : ["Age_Band"],
  "level_fields" : [
    F.lit("England; Age Group").alias("breakdown"),
    F.col("Age_Band").alias("primary_level"),
    F.col("Age_Band").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
pop_rate_gender_bd = {
    "breakdown_name": "England; Gender",
    "primary_level": F.col("level_code"),
    "primary_level_desc": F.col("level_name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [
              ("1", "Male"),             
              ("2", "Female"),
              ("3", "Non-binary"),
              ("4", "Other (not listed)"),
              ("9", "Indeterminate"),            
              ("UNKNOWN", "UNKNOWN"),
  ],
  "lookup_col" : ["Der_Gender", "Der_Gender_Desc"],
  "level_fields" : [
    F.lit("England; Gender").alias("breakdown"),
    F.col("Der_Gender").alias("primary_level"),
    F.col("Der_Gender_Desc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],  
}
pop_rate_upper_eth_bd = {
    "breakdown_name": "England; Upper Ethnicity",
    "primary_level": F.col("level_code"),
    "primary_level_desc": F.col("level_name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [            
              ("White", ),
              ("Mixed", ),           
              ("Asian or Asian British", ),             
              ("Black or Black British", ),             
              ("Other Ethnic Groups", ),             
              ("Not Stated", ),              
              ("Not Known", ),
              ("UNKNOWN", ),
  ],
  "lookup_col" : ["UpperEthnicity"],
  "level_fields" : [
    F.lit("England; Upper Ethnicity").alias("breakdown"),
    F.col("UpperEthnicity").alias("primary_level"),
    F.col("UpperEthnicity").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),     
  ],
}
pop_rate_imd_decile_bd = {
    "breakdown_name": "England; IMD Decile",
    "primary_level": F.col("level_code"),
    "primary_level_desc": F.col("level_name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [
              ("01 Most deprived", ),             
              ("02 More deprived", ),
              ("03 More deprived", ),
              ("04 More deprived", ),
              ("05 More deprived", ),
              ("06 Less deprived", ),
              ("07 Less deprived", ),
              ("08 Less deprived", ),
              ("09 Less deprived", ), 
              ("10 Least deprived", ),
              ("UNKNOWN", ),
  ],
  "lookup_col" : ["IMD_Decile_Name"],
  "level_fields" : [
    F.lit("England; IMD Decile").alias("breakdown"),
    F.col("IMD_Decile_Name").alias("primary_level"),
    F.col("IMD_Decile_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
  ],
}
pop_rate_ccg_res_bd = {
    "breakdown_name": "CCG of Residence",
    "primary_level": F.col("level_code"),
    "primary_level_desc": F.col("level_name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"),
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence").alias("breakdown"),
    F.col("CCG_CODE").alias("primary_level"),
    F.col("CCG_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc")
    ],
}

# COMMAND ----------

mhsds_measure_metadata = {
'MHS110' : {
  'freq': 'M',
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts',  
  'source_table': 'cyp_master',     
  "filter_clause": "",
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS110',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and any assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                       
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_ANY = 1 THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111a' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and any assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_ANY = 1 THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111b' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                       
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_SR IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111b',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111c' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_SR IS NOT NULL THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111b',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111d' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                       
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_PR IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111d',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111e' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_PR IS NOT NULL THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111d',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111f' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                       
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_CR IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111f',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS111g' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective assessment',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Assessment_CR IS NOT NULL THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS111f',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS112' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and any perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Paired_ANY = 1 THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS112a' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and any perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Paired_ANY = 1 THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS112b' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Paired_SR IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112b',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS112c' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Paired_SR IS NOT NULL THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112b',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS112d' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Paired_PR IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112d',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS112e' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Paired_PR IS NOT NULL THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112d',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS112f' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN Paired_CR IS NOT NULL THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112f',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS112g' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL AND Paired_CR IS NOT NULL THEN UniqServReqID ELSE NULL END)/COUNT(DISTINCT CASE WHEN Contact2 IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS112f',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS113a' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score that showed measurable improvement',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("SUM(COALESCE(Improvement_SR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS113a',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS113b' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score that showed measurable improvement',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(Improvement_SR,0))/COUNT(DISTINCT CASE WHEN Paired_SR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS113a',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS113c' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score that showed measurable deterioration',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                             
  'aggregate_field': (F.expr("SUM(COALESCE(Deter_SR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS113c',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS113d' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score that showed measurable deterioration',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(Deter_SR,0))/COUNT(DISTINCT CASE WHEN Paired_SR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS113c',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS113e' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score that showed no measurable change',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("SUM(COALESCE(NoChange_SR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS113e',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS113f' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a self-rated perspective paired score that showed no measurable change',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(NoChange_SR,0))/COUNT(DISTINCT CASE WHEN Paired_SR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS113e',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS114a' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score that showed measurable improvement',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("SUM(COALESCE(Improvement_PR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS114a',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': "count",
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS114b' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score that showed measurable improvement',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(Improvement_PR,0))/COUNT(DISTINCT CASE WHEN Paired_PR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS114a',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS114c' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score that showed measurable deterioration',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("SUM(COALESCE(Deter_PR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS114c',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS114d' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score that showed measurable deterioration',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(Deter_PR,0))/COUNT(DISTINCT CASE WHEN Paired_PR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS114c',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS114e' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score that showed no measurable change',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("SUM(COALESCE(NoChange_PR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS114e',
  "denominator": 0,
  "related_to": "referrals",
  "suppression": "count",
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS114f' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a parent-rated perspective paired score that showed no measurable change',
  'freq': 'M',
  'source_table': 'cyp_master',
  "filter_clause": "",                
  'date_column': 'ReportingPeriodStartDate',                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(NoChange_PR,0))/COUNT(DISTINCT CASE WHEN Paired_PR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS114e',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS115a' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score that showed measurable improvement',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("SUM(COALESCE(Improvement_CR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS115a',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS115b' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score that showed measurable improvement',
  'freq': 'M',
  'source_table': 'cyp_master',       
  "filter_clause": "",                    
  'date_column': 'ReportingPeriodStartDate',                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(Improvement_CR,0))/COUNT(DISTINCT CASE WHEN Paired_CR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS115a',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
}, 
'MHS115c' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score that showed measurable deterioration',
  'freq': 'M',
  'source_table': 'cyp_master',       
  "filter_clause": "",                    
  'date_column': 'ReportingPeriodStartDate',                              
  'aggregate_field': (F.expr("SUM(COALESCE(Deter_CR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS115c',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS115d' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score that showed measurable deterioration',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(Deter_CR,0))/COUNT(DISTINCT CASE WHEN Paired_CR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS115c',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS115e' : {
  'name': 'Number of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score that showed no measurable change',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                            
  'aggregate_field': (F.expr("SUM(COALESCE(NoChange_CR,0))").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS115e',
  "denominator": 0,
  "related_to": "referrals",
  'suppression': 'count',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS115f' : {
  'name': 'Percentage of closed referrals for children and young people aged between 0 and 17 with at least 2 contacts and a clinician-rated perspective paired score that showed no measurable change',
  'freq': 'M',
  'source_table': 'cyp_master',                    
  "filter_clause": "",                              
  'aggregate_field': (F.expr("ROUND((SUM(COALESCE(NoChange_CR,0))/COUNT(DISTINCT CASE WHEN Paired_CR IS NOT NULL THEN UniqServReqID ELSE NULL END))*100, 1)").alias("MEASURE_VALUE")),
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS115e',
  "denominator": 1,
  "related_to": "referrals",
  'suppression': 'percent',
  'breakdowns': [eng_bd, prov_bd, stp_res_bd, comm_region_bd],
  "output_table": "cyp_outcomes_monthly"
},
'MHS116' : {
  'name': 'Number of referrals that accessed Individual Placement Support (IPS) in the reporting period',
  'freq': '12M',
  'source_table': 'ips_master',                                
  'aggregate_field': (F.expr("COUNT(DISTINCT CASE WHEN AccessFlag = 1 THEN UniqServReqID ELSE NULL END)").alias("MEASURE_VALUE")),
  'filter_clause': '',
  'aggregate_function': produce_agg_df,
  'numerator_id': 'MHS116', 
  'denominator': 0,
  'related_to': 'referrals',
  'suppression': 'count',
  'breakdowns': [eng_bd, age_band_0_19_to_70_bd, gender_bd, upper_eth_bd, imd_decile_bd, prov_bd, ccg_res_bd],
  'output_table': 'ips_monthly'
},
'MHS116a' : {
  'name': 'Rate of referrals that accessed Individual Placement Support (IPS) in the reporting period per 100,000 of the population',
  'freq': '12M',
  'source_table': 'ips_master_rates',                                  
  'aggregate_field': (F.expr("(COALESCE(SUM(REF_COUNT)/SUM(POPULATION_COUNT), 0))*100000").alias("MEASURE_VALUE")),
  'filter_clause': '',
  'aggregate_function': produce_pop_rate_agg_df,
  'numerator_id': 'MHS116',  
  'denominator': 1,
  'related_to': 'referrals',
  'suppression': 'percent',
  'breakdowns': [pop_rate_eng_bd, pop_rate_age_band_0_19_to_70_bd, pop_rate_gender_bd, pop_rate_upper_eth_bd, pop_rate_imd_decile_bd, pop_rate_ccg_res_bd],
  'output_table': 'ips_monthly'
},
"CCR70": {
  "freq": "M", 
  "name": "New Emergency Referrals to Crisis Care teams in the reporting period",
  "source_table": "community_crisis_master",
  "filter_clause": (F.col("ClinRespPriorityType") == "1"),
  "aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
  "aggregate_function": produce_filter_agg_df,
  "numerator_id": "CCR70",
  "denominator": 0,
  "related_to": "referrals",
  "suppression": "count",
  "breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
  "output_table": "uec_monthly"
},
"CCR70a": {
"freq": "M", 
"name": "New Emergency Referrals to Crisis Care teams in the reporting period, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "1") & (F.col("AGE_GROUP") == "18 and over"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR70a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR70b": {
"freq": "M", 
"name": "New Emergency Referrals to Crisis Care teams in the reporting period, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "1") & (F.col("AGE_GROUP") == "0-17"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR70b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR117": {
  "freq": "M", 
  "name": "New Very Urgent Referrals to Crisis Care teams in the reporting period",
  "source_table": "community_crisis_master",
  "filter_clause": (F.col("ClinRespPriorityType") == "4"),
  "aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
  "aggregate_function": produce_filter_agg_df,
  "numerator_id": "CCR117",
  "denominator": 0,
  "related_to": "referrals",
  "suppression": "count",
  "breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
  "output_table": "uec_monthly"
},
"CCR117a": {
"freq": "M", 
"name": "New Very Urgent Referrals to Crisis Care teams in the reporting period, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("AGE_GROUP") == "18 and over"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR117a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR117b": {
"freq": "M", 
"name": "New Very Urgent Referrals to Crisis Care teams in the reporting period, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("AGE_GROUP") == "0-17"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR117b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR71": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR71",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR71a": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("AGE_GROUP") == "18 and over"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR71a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR71b": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("AGE_GROUP") == "0-17"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR71b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR118": {
  "freq": "M", 
  "name": "New Very Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact",
  "source_table": "community_crisis_master",
  "filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("UniqCareContID").isNotNull()),
  "aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
  "aggregate_function": produce_filter_agg_df,
  "numerator_id": "CCRx1",
  "denominator": 0,
  "related_to": "referrals",
  "suppression": "count",
  "breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
  "output_table": "uec_monthly"
},
"CCR118a": {
"freq": "M", 
"name": "New Very Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("AGE_GROUP") == "18 and over") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCRx1a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR118b": {
"freq": "M", 
"name": "New Very Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("AGE_GROUP") == "0-17") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCRx1b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR73": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR73",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR73a": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("AGE_GROUP") == "18 and over") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR73a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR73b": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("AGE_GROUP") == "0-17") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR73b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR119": {
  "freq": "M", 
  "name": "New Very Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact within 4 hours of referral",
  "source_table": "community_crisis_master",
  "filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,240)),
  "aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
  "aggregate_function": produce_filter_agg_df,
  "numerator_id": "CCR119",
  "denominator": 0,
  "related_to": "referrals",
  "suppression": "count",
  "breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
  "output_table": "uec_monthly"
},
"CCR119a": {
"freq": "M", 
"name": "New Very Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact within 4 hours of referral, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("AGE_GROUP") == "18 and over") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,240)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR119a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR119b": {
"freq": "M", 
"name": "New Very Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact within 4 hours of referral, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "4") & (F.col("AGE_GROUP") == "0-17") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,240)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR119b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR120": {
  "freq": "M", 
  "name": "New Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact within 24 hours of referral",
  "source_table": "community_crisis_master",
  "filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,1440)),
  "aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
  "aggregate_function": produce_filter_agg_df,
  "numerator_id": "CCR120",
  "denominator": 0,
  "related_to": "referrals",
  "suppression": "count",
  "breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
  "output_table": "uec_monthly"
},
"CCR120a": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact within 24 hours of referral, Aged 18 and over",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("AGE_GROUP") == "18 and over") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,1440)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCR120a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"CCR120b": {
"freq": "M", 
"name": "New Urgent Referrals to Crisis Care teams in the reporting period with first face to face contact within 24 hours of referral, Aged under 18",
"source_table": "community_crisis_master",
"filter_clause": (F.col("ClinRespPriorityType") == "2") & (F.col("AGE_GROUP") == "0-17") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,1440)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "CCRx3b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS121": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("UniqServReqID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS121",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS121a": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period, Aged 18 and over",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("AGE_GROUP") == "18 and over"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS121a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS121b": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period, Aged under 18",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("AGE_GROUP") == "0-17"),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS121b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS122": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period with first face to face contact",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS122",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS122a": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period with first face to face contact, Aged 18 and over",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("AGE_GROUP") == "18 and over") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS122a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS122b": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period with first face to face contact, Aged under 18",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("AGE_GROUP") == "0-17") & (F.col("UniqCareContID").isNotNull()),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS122b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS123": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period with first face to face contact within 1 hour",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,60)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS123",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS123a": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period with first face to face contact within 1 hour, Aged 18 and over",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("AGE_GROUP") == "18 and over") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,60)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS123a",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
"PLS123b": {
"freq": "M", 
"name": "New referrals to liaison psychiatry teams from A&E in the reporting period with first face to face contact within 1 hour, Aged under 18",
"source_table": "liason_psychiatry_master",
"filter_clause": (F.col("AGE_GROUP") == "0-17") & (F.col("UniqCareContID").isNotNull()) & (F.col("minutes_between_ref_and_first_cont").between(0,60)),
"aggregate_field": (F.expr("CAST(COUNT(DISTINCT UniqServReqID) as INT)").alias("MEASURE_VALUE")),
"aggregate_function": produce_filter_agg_df,
"numerator_id": "PLS3b",
"denominator": 0,
"related_to": "referrals",
"suppression": "count",
"breakdowns": [eng_bd, prov_bd, ccg_prac_res_bd],
"output_table": "uec_monthly"
},
}

# COMMAND ----------

unsup_breakdowns = ["England", "England; Age Group", "England; Gender", "England; Upper Ethnicity", "England; IMD Decile"]
output_columns = ["REPORTING_PERIOD_START", "REPORTING_PERIOD_END", "STATUS", "BREAKDOWN", "PRIMARY_LEVEL", "PRIMARY_LEVEL_DESCRIPTION", "SECONDARY_LEVEL", "SECONDARY_LEVEL_DESCRIPTION", "MEASURE_ID", "MEASURE_NAME", "MEASURE_VALUE"]