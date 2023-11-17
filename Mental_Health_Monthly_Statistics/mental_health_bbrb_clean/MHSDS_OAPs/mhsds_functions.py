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

# DBTITLE 1,Common Functions
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
    except TypeError:
        return 0
spark.udf.register("is_numeric", is_numeric)

# COMMAND ----------

# DBTITLE 1,Parameter Functions
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
  max_year_of_count = spark.sql(f"select max(year_of_count) AS year_of_count from $reference_data.ONS_POPULATION_V2 where GEOGRAPHIC_GROUP_CODE = 'E38'")
  max_year_of_count_value = max_year_of_count.first()["year_of_count"]
  year_of_count = current_year
  if (year_of_count > max_year_of_count_value):
    year_of_count = max_year_of_count_value
 
  return year_of_count  

# COMMAND ----------

# DBTITLE 1,Parameter Data Class
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

# DBTITLE 1,Aggregation Functions
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

# DBTITLE 1,CSV Lookup Table Data Class and Functions
@dataclass
class MHMeasureLevel():
  db_output: str
  measure_id: str
  breakdown: str
  level_list: list
  level_fields: list
  lookup_columns: list
  
  def create_level_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each necessary measure_id    
    """
    if self.breakdown in ("CCG of GP Practice or Residence", "CCG of Residence"):
      prep_df = spark.table(f"{self.db_output}.OAPs_STP_Region_mapping")
      level_df = prep_df.select("CCG_CODE", "CCG_NAME").distinct()
      
    elif self.breakdown == "Provider":
      level_df = spark.table(f"{self.db_output}.oaps_org_daily_latest_mhsds_providers")
      
    elif self.breakdown in ("STP of GP Practice or Residence", "STP of Residence"):
      prep_df = spark.table(f"{self.db_output}.OAPs_STP_Region_mapping")
      level_df = prep_df.select("STP_CODE", "STP_NAME").distinct()
      
    elif self.breakdown == "Commissioning Region":
      prep_df = spark.table(f"{self.db_output}.OAPs_STP_Region_mapping")
      level_df = prep_df.select("REGION_CODE", "REGION_NAME").distinct()
      
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
    and then inserts into the $db_output.oaps_level_values table
    """
    level_df = self.create_level_dataframe()
    insert_df = level_df.select(self.level_fields)
    
    insert_df.write.insertInto(f"{self.db_output}.oaps_level_values")

def insert_oaps_lookup_values(mh_run_params: object, metadata: dict):
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
  spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.oaps_csv_lookup")
  column_order = [ 
      "REPORTING_PERIOD_START",
      "REPORTING_PERIOD_END",
      "STATUS",
      "BREAKDOWN",
      "PRIMARY_LEVEL",
      "PRIMARY_LEVEL_DESCRIPTION",
      "SECONDARY_LEVEL",
      "SECONDARY_LEVEL_DESCRIPTION",
      "METRIC",
      "METRIC_DESCRIPTION"
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
      lvl = MHMeasureLevel(mh_run_params.db_output, measure_id, breakdown_name, lvl_list, lvl_fields, lvl_cols)
      #create level_df to insert into mh_monthly_level_values
      lvl.insert_level_dataframe() #all breakdowns and primary levels added for the measure_id
      #add measure_id to dataframe and insert into mh_monthly_lookup
      lvl_df = spark.table(f"{db_output}.oaps_level_values")
      lvl_measure_df = (
        lvl_df
        .withColumn("REPORTING_PERIOD_START", F.lit(rp_startdate)) #rp_startdate based off frequency
        .withColumn("REPORTING_PERIOD_END", F.lit(mh_run_params.rp_enddate))
        .withColumn("STATUS", F.lit(mh_run_params.status))
        .withColumn("METRIC", F.lit(measure_id))
        .withColumn("METRIC_DESCRIPTION", F.lit(measure_name))
        .select(*column_order)
      )
      #write into final ref table
      lvl_measure_df.write.insertInto(f"{mh_run_params.db_output}.oaps_csv_lookup")
      #reset level_values table for next measure_id
      spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.oaps_level_values")                              
      #restart loop
    ##loop ends with full populated levels and breakdowns for each measure id in metadata

# COMMAND ----------

 %md
 Test MHSDS Data

# COMMAND ----------

# DBTITLE 1,Test Functions - Parameters to Output checks
def test_ReportingPeriodEndParamMatch(df, param_json):
  """
  This tests that the reporting period end in the output match in the parameter JSON
  """
  #Output ReportingPeriodEndDate list
  end_rp = list(df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END'])
  output_rp_enddate = str(end_rp[0])
  
  #Parameter ReportingPeriodEndDate
  param_rp_enddate = param_json['rp_enddate']
  
  assert param_rp_enddate in output_rp_enddate, "Reporting Period End does not match to Parameter"
  print(f"{test_ReportingPeriodEndParamMatch.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Test Functions - Output Schema checks
def test_AssertOutputSchema(df):
  """
  This asserts MH Run output is equivalent in datatypes to the MHRunOutput class
  """
  
def test_NumberOfColumnsUnsup(df):
  """
  This tests whether the number of columns in the unsuppressed output is equal to 11 (PRIMARY, SECONDARY)
  """
  assert len(df.columns) == 11, "Unsuppressed Output does not contain expected amount of columns"
  print(f"{test_NumberOfColumnsUnsup.__name__}: PASSED")
  
def test_NumberOfColumnsSup(df):
  """
  This tests whether the number of columns in the suppressed output is equal to 11 (PRIMARY, SECONDARY)
  """
  assert len(df.columns) == 11, "Suppressed Output does not contain expected amount of columns"
  print(f"{test_NumberOfColumnsSup.__name__}: PASSED")
  
def test_SameColumnNamesUnsup(df):
  """
  This asserts that all columns in the dataframe contain the expected column names for an MHSDS unsuppressed output
  """
  expt_cols = ['REPORTING_PERIOD_START',
 'REPORTING_PERIOD_END',
 'STATUS',
 'BREAKDOWN',
 'PRIMARY_LEVEL',
 'PRIMARY_LEVEL_DESCRIPTION',
 'SECONDARY_LEVEL',
 'SECONDARY_LEVEL_DESCRIPTION',
 'MEASURE_ID',
 'MEASURE_NAME',
 'MEASURE_VALUE']
  
  assert expt_cols == df.columns, "Unsuppressed Output does not contain expected column names"
  print(f"{test_SameColumnNamesUnsup.__name__}: PASSED")
  
def test_SameColumnNamesSup(df):
  """
  This asserts that all columns in the dataframe contain the expected column names for an MHSDS suppressed output
  """
  expt_cols = ['REPORTING_PERIOD_START',
 'REPORTING_PERIOD_END',
 'STATUS',
 'BREAKDOWN',
 'PRIMARY_LEVEL',
 'PRIMARY_LEVEL_DESCRIPTION',
 'SECONDARY_LEVEL',
 'SECONDARY_LEVEL_DESCRIPTION',
 'MEASURE_ID',
 'MEASURE_NAME',
 'MEASURE_VALUE']
  
  assert expt_cols == df.columns, "Suppressed Output does not contain expected column names"
  print(f"{test_SameColumnNamesSup.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Test Functions - Reporting Period checks
def test_SingleReportingPeriodEnd(df):
  """
  This tests that there is only 1 reporting period end month in the output
  """
  #ReportingPeriodEndDate Series
  end_rp = df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END']
  
  assert len(end_rp) == 1, "more than a single month in ReportingPeriodEndDate"
  print(f"{test_SingleReportingPeriodEnd.__name__}: PASSED")
 
def test_Quart_ReportingPeriod(df):
  """
  This tests that the rp_startdate for the quarter rp_startdate are correct in relation to the rp_enddate
  """
  
  #ReportingPeriodEndDate Series
  end_rp = df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END']
  out_enddate = str(end_rp[0]) ##this is dependent on test_SingleReportingPeriodEnd PASSING first
  
  #date format and transform ReportingPeriodEndDateto timestamp
  date_format = '%Y-%m-%d'
  rp_enddate = datetime.strptime(out_enddate, date_format)  
  
  #expected quarter start date from rp_enddate
  exp_qtr_enddate = rp_enddate - relativedelta(months=2) ##minus 3 months
  exp_qtr_startdate = exp_qtr_enddate.replace(day=1) ##first day
    
  #expected start dates list
  exp_start_rps = []
  exp_start_rps.append(exp_qtr_startdate)
  
  #start dates in output list
  pre_output_start_rps = list(df.select('REPORTING_PERIOD_START').distinct().toPandas()['REPORTING_PERIOD_START'])
  output_start_rps = [datetime.strptime(str(i), date_format) for i in pre_output_start_rps]
  
  assert (set(exp_start_rps) == set(output_start_rps)), "ReportingPeriodStartDate is incorrect for Quarterly measure according to ReportingPeriodEndDate"
  print(f"{test_Quart_ReportingPeriod.__name__}: PASSED")
  
def test_12month_ReportingPeriod(df):
  """
  This tests that the rp_startdate for the 12-month rolling rp_startdate are correct in relation to the rp_enddate
  """
  #ReportingPeriodEndDate Series
  end_rp = df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END']
  out_enddate = str(end_rp[0]) ##this is dependent on test_SingleReportingPeriodEnd first
  
  #date format and transform ReportingPeriodEndDateto timestamp
  date_format = '%Y-%m-%d'
  rp_enddate = datetime.strptime(out_enddate, date_format)  
  
  #expected rolling 12-month start date from rp_enddate
  exp_12m_enddate = rp_enddate - relativedelta(months=11) ##minus 11 months
  exp_12m_startdate = exp_12m_enddate.replace(day=1) ##first day 
  
  #expected start dates list
  exp_start_rps = []
  exp_start_rps.append(exp_12m_startdate)
  
  #start dates in output list
  pre_output_start_rps = list(df.select('REPORTING_PERIOD_START').distinct().toPandas()['REPORTING_PERIOD_START'])
  output_start_rps = [datetime.strptime(str(i), date_format) for i in pre_output_start_rps]
  
  assert (set(exp_start_rps) == set(output_start_rps)), "ReportingPeriodStartDate is incorrect for 12-month according to ReportingPeriodEndDate"
  print(f"{test_12month_ReportingPeriod.__name__}: PASSED")
 
def test_1month_ReportingPeriod(df):
  """
  This tests that the rp_startdate for the 12-month rolling rp_startdate are correct in relation to the rp_enddate
  """
  #ReportingPeriodEndDate Series
  end_rp = df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END']
  out_enddate = str(end_rp[0]) ##this is dependent on test_SingleReportingPeriodEnd PASSING first
  
  #date format and transform ReportingPeriodEndDateto timestamp
  date_format = '%Y-%m-%d'
  rp_enddate = datetime.strptime(out_enddate, date_format)  
  
  #expected rolling 12-month start date from rp_enddate
  exp_1m_startdate = rp_enddate.replace(day=1) ##first day 
  
  #expected start dates list
  exp_start_rps = []
  exp_start_rps.append(exp_1m_startdate)
  
  #start dates in output list
  pre_output_start_rps = list(df.select('REPORTING_PERIOD_START').distinct().toPandas()['REPORTING_PERIOD_START'])
  output_start_rps = [datetime.strptime(str(i), date_format) for i in pre_output_start_rps]
  
  assert (set(exp_start_rps) == set(output_start_rps)), "ReportingPeriodStartDate is incorrect for 12-month according to ReportingPeriodEndDate"
  print(f"{test_1month_ReportingPeriod.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Test Functions - Breakdown totals
def test_breakdown_activity_count(df):
  '''
  This function tests that all totals for counts of activity at each breakdown are the same i.e. standard dev of TOTAL column has to be equal to 0
  '''
  breakdown_totals = list(df.select(stddev(F.col("TOTAL")).alias("STD")).toPandas()["STD"])
  
  assert breakdown_totals[0] == 0, "Breakdown totals are not all equal"
  print(f"{test_breakdown_activity_count.__name__}: PASSED")
  
def test_breakdown_people_count(df):
  '''
  This function tests that all totals for counts of apeople at each breakdown are greater than or equal to the National total
  '''
  eng_breakdown = df[df["BREAKDOWN"] == "England"]
  eng_total = list(eng_breakdown.select("TOTAL").toPandas()["TOTAL"])
  eng_value = eng_total[0]
 
  other_totals = list(df.select("TOTAL").toPandas()["TOTAL"])
  for total in other_totals:
    assert total >= eng_value, "Breakdowns are not equal to or greater than National value"
  
  print(f"{test_breakdown_people_count.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Metric Functions - General
def single_measure_df(df, measure):
  '''
  This function filters a dataframe to a single measure
  '''
  df_measure = df[df["MEASURE_ID"] == measure]
  
  return df_measure
 
def get_distinct_measure_ids(df):
  '''
  This function returns a distinct list of all the measure_ids in a dataframe
  '''
  measure_ids = list(df.select('MEASURE_ID').distinct().toPandas()['MEASURE_ID'])
  
  return sorted(measure_ids)

# COMMAND ----------

# DBTITLE 1,Metric Functions - Reporting Periods
def get_measure_reporting_periods(df, measure):
  '''
  This function returns a dataframe of distinct REPORTING_PERIOD_START and REPORTING_PERIOD_END for a single measure
  '''
  df_measure = single_measure_df(df, measure)
  rps = df_measure.select("MEASURE_ID", "REPORTING_PERIOD_START", "REPORTING_PERIOD_END").distinct()
  
  return rps
 
def assess_measure_reporting_periods(df, measure_metadata):
  '''
  This function assess the distinct reporting periods based on the frequency key in the measure metadata
  '''
  measure_value = df.first()["MEASURE_ID"]
  if measure_metadata[measure_value]["freq"] == "Q":
    print(measure_value)
    test_SingleReportingPeriodEnd(df)
    test_Quart_ReportingPeriod(df)
  elif measure_metadata[measure_value]["freq"] == "12M":
    print(measure_value)
    test_SingleReportingPeriodEnd(df)
    test_12month_ReportingPeriod(df)
  elif measure_metadata[measure_value]["freq"] == "M":
    print(measure_value)
    test_SingleReportingPeriodEnd(df)
    test_1month_ReportingPeriod(df)
  else:
    print(f"{measure_value}: Invalid code")
    
def test_measure_reporting_periods(df, measure_metadata):
  '''
  This function combines the reporting period functions into one test for better readability
  '''
  measure_ids = get_distinct_measure_ids(df)
  for measure_id in measure_ids:
    rps = get_measure_reporting_periods(df, measure_id)
    assess_measure_reporting_periods(rps, measure_metadata)


# COMMAND ----------

# DBTITLE 1,Metric Functions - Breakdowns
def get_measure_breakdown_totals(df, measure, measure_metadata):
  '''
  This function gets an aggregate figure for MEASURE VALUE at each breakdown for a single measure
  '''
  df_measure = single_measure_df(df, measure)
  if measure_metadata[measure]["denominator"] == 0:
    breakdowns = df_measure.groupBy("MEASURE_ID", "BREAKDOWN").agg(sum("MEASURE_VALUE").alias("TOTAL"))  ###sum of a count of activity across all breakdowns should equal the same total
  else:
    data = [{"MEASURE_ID": measure, "BREAKDOWN": "NO_BREAKDOWNS_TESTED", "TOTAL": 0}]
    breakdowns = spark.createDataFrame(data)
  
  return breakdowns
 
def assess_measure_breakdown_totals(df, measure_metadata):
  '''
  This function assess the distinct breakdown totals
  '''
  measure_value = df.first()["MEASURE_ID"]
  ### count of referrals, hospital spells, care contact related measures
  if (measure_metadata[measure_value]["related_to"] != "people") & (measure_metadata[measure_value]["denominator"] == 0):
    print(measure_value)
    test_breakdown_activity_count(df)
  ### count of people related measures
  elif (measure_metadata[measure_value]["related_to"] == "people") & (measure_metadata[measure_value]["denominator"] == 0):
    print(measure_value)
    test_breakdown_people_count(df)
  else:
    print(f"Breakdown Test for {measure_value} is not yet implemented")
    
def test_measure_breakdown_figures(df, measure_metadata):
  '''
  This function combines the reporting period functions into one test for better readability
  '''
  measure_ids = get_distinct_measure_ids(df)
  for measure_id in measure_ids:
    breakdowns = get_measure_breakdown_totals(df, measure_id, measure_metadata)
    assess_measure_breakdown_totals(breakdowns, measure_metadata)


# COMMAND ----------

# DBTITLE 1,Metric Functions - Reference data - Provider
def get_distinct_provider_codes(df, measure, measure_metadata):
  '''
  This function returns a list of distinct Provider codes if the Provider breakdown exists for the measure_id 
  else it returns a blank list
  '''
  df_measure = single_measure_df(df, measure)
  breakdown_list = measure_metadata[measure]["primary_breakdowns"]
  if breakdown_list.count("Provider") == 1:
    prov_df = df_measure[df_measure['BREAKDOWN'].startswith('Provider')]
    df_prov_list  = list(prov_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  else:
    df_prov_list = []
    
  return df_prov_list
 
def get_expected_provider_codes(params_json):
  '''
  This function returns a list of expected distinct Provider codes based on the rp_enddate
  '''
  rp_enddate = params_json['rp_enddate']
  exp_prov_df = spark.sql(f"""
  SELECT DISTINCT ORG_CODE
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('{rp_enddate}', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('{rp_enddate}', 1)    
                AND (ORG_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(ORG_CLOSE_DATE))              
                AND ORG_OPEN_DATE <= '{rp_enddate}' 
  """)
  exp_prov_list = list(exp_prov_df.select('ORG_CODE').toPandas()['ORG_CODE'])
  
  return exp_prov_list
 
def test_ProviderCodes(df, measure_metadata, params_json):
  """
  This tests Provider codes in output against the latest valid codes 
  """
  measure_ids = get_distinct_measure_ids(df)
  exp_prov_list = get_expected_provider_codes(params_json)
  for measure_id in measure_ids:
    prov_codes = get_distinct_provider_codes(df, measure_id, measure_metadata)
    if len(prov_codes) == 0:
      print(measure_id)
      print("Provider breakdown does not exist for this measure_id")
    else:
      check = all(item in exp_prov_list for item in prov_codes)
  
  if check is True:
    print(f"{test_ProviderCodes.__name__}: PASSED")    
  else :
    ValueError(print("provider codes in output are not in expected provider codes")) 


# COMMAND ----------

# DBTITLE 1,Metric Functions - Reference data - CCG
def get_distinct_ccg_codes(df, measure, measure_metadata):
  '''
  This function returns a list of distinct CCG codes if the CCG breakdown exists for the measure_id 
  else it returns a blank list
  '''
  df_measure = single_measure_df(df, measure)
  breakdown_list = measure_metadata[measure]["primary_breakdowns"]
  if breakdown_list.count("CCG") == 1:
    ccg_df = df_measure[df_measure['BREAKDOWN'].startswith('CCG')]
    df_ccg_list  = list(ccg_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  else:
    df_ccg_list = []
    
  return df_ccg_list
 
def get_expected_ccg_codes(params_json):
  '''
  This function returns a list of expected distinct Provider codes based on the rp_enddate
  '''
  rp_enddate = params_json['rp_enddate']  
 
  exp_ccg_df = spark.sql(f"""
  SELECT DISTINCT ORG_CODE
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('{rp_enddate}', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('{rp_enddate}', 1)    
                AND ORG_TYPE_CODE = 'CC'
                AND (ORG_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(ORG_CLOSE_DATE))
                AND ORG_OPEN_DATE <= '{rp_enddate}'
                AND NAME NOT LIKE '%HUB'
                AND NAME NOT LIKE '%NATIONAL%'
  """)
  exp_ccg_list = list(exp_ccg_df.select('ORG_CODE').toPandas()['ORG_CODE'])
  
  return exp_ccg_list
 
def test_CCGCodes(df, measure_metadata, params_json):
  """
  This tests CCG codes in output against the latest valid codes 
  """
  measure_ids = get_distinct_measure_ids(df)
  exp_ccg_list = get_expected_ccg_codes(params_json)
  for measure_id in measure_ids:
    ccg_codes = get_distinct_ccg_codes(df, measure_id, measure_metadata)
    if len(ccg_codes) == 0:
      print(measure_id)
      print("CCG breakdown does not exist for this measure_id")
    else:
      assert set(exp_ccg_list) == set(ccg_codes), "CCG codes in output do not match expected ccg codes"
      print(f"{test_CCGCodes.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Metric Functions - Reference data - STP
def get_distinct_stp_codes(df, measure, measure_metadata):
  '''
  This function returns a list of distinct STP codes if the STP breakdown exists for the measure_id 
  else it returns a blank list
  '''
  df_measure = single_measure_df(df, measure)
  breakdown_list = measure_metadata[measure]["primary_breakdowns"]
  if breakdown_list.count("STP") == 1:
    stp_df = measure_df[measure_df['BREAKDOWN'].startswith('STP')]
    df_stp_list  = list(stp_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  else:
    df_ccg_list = []
    
  return df_ccg_list
 
def get_expected_stp_codes(params_json):
  '''
  This function returns a list of expected distinct Provider codes based on the rp_enddate
  '''
  rp_enddate = params_json['rp_enddate']  
 
  exp_stp_df1 = spark.sql(f"""
  SELECT DISTINCT 
                ORG_CODE,
                NAME,
                ORG_TYPE_CODE,
                ORG_OPEN_DATE, 
                ORG_CLOSE_DATE, 
                BUSINESS_START_DATE, 
                BUSINESS_END_DATE
           FROM $reference_data.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('{rp_enddate}', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('{rp_enddate}', 1)    
                AND (ORG_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(ORG_CLOSE_DATE))              
                AND ORG_OPEN_DATE <= '{rp_enddate}'
  """)
  exp_stp_df1.createOrReplaceGlobalTempView("exp_org_daily")
  exp_stp_df2 = spark.sql(f"""
  SELECT 
                REL_TYPE_CODE,
                REL_FROM_ORG_CODE,
                REL_TO_ORG_CODE, 
                REL_OPEN_DATE,
                REL_CLOSE_DATE
            FROM $reference_data.org_relationship_daily
            WHERE (REL_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(REL_CLOSE_DATE))              
                   AND REL_OPEN_DATE <= '{rp_enddate}'
  """)
  exp_stp_df2.createOrReplaceGlobalTempView("exp_org_relationship_daily")
  
  exp_stp_df = spark.sql(f"""
  SELECT 
                A.ORG_CODE as STP_CODE, 
                A.NAME as STP_NAME, 
                C.ORG_CODE as CCG_CODE, 
                C.NAME as CCG_NAME,
                E.ORG_CODE as REGION_CODE,
                E.NAME as REGION_NAME
            FROM global_temp.exp_org_daily A
            LEFT JOIN global_temp.exp_org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
            LEFT JOIN global_temp.exp_org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
            LEFT JOIN global_temp.exp_org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
            LEFT JOIN global_temp.exp_org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
            WHERE A.ORG_TYPE_CODE = 'ST' AND B.REL_TYPE_CODE is not null
  """)
  
  exp_stp_list = list(exp_stp_df.select('STP_CODE').toPandas()['STP_CODE'])
  
  return exp_ccg_list
 
def test_CCGCodes(df, measure_metadata, params_json):
  """
  This tests CCG codes in output against the latest valid codes 
  """
  measure_ids = get_distinct_measure_ids(df)
  exp_ccg_list = get_expected_ccg_codes(params_json)
  for measure_id in measure_ids:
    ccg_codes = get_distinct_ccg_codes(df, measure_id, measure_metadata)
    if len(ccg_codes) == 0:
      print(measure_id)
      print("CCG breakdown does not exist for this measure_id")
    else:
      assert set(exp_ccg_list) == set(ccg_codes), "CCG codes in output do not match expected ccg codes"
      print(f"{test_CCGCodes.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Metric Functions - Reference data - Region
def test_RegionCodes(df):
  """
  This tests Commissioning Region codes in output against the latest valid codes 
  """
  #df commissioning region codes excluding UNKNOWN value
  comm_region_df = df[(df['BREAKDOWN'].endswith('Region')) & (df['PRIMARY_LEVEL'] != 'UNKNOWN')]
  df_regions_list  = list(comm_region_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  
  #expected commissioning region codes
  comm_region_od = spark.sql("select * from $reference_data.org_daily where org_type_code = 'CE'")
  latest_comm_region = comm_region_od.filter("ORG_CLOSE_DATE is null and BUSINESS_END_DATE is null")
  expt_regions_list = list(latest_comm_region.select('ORG_CODE').toPandas()['ORG_CODE'])  
 
  assert set(df_regions_list) == set(expt_regions_list), "region codes in output do not match expected region codes"
  print(f"{test_RegionCodes.__name__}: PASSED")


# COMMAND ----------

# DBTITLE 1,Suppression functions - added to do one round of suppression at the end of the process, rather than one per product
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