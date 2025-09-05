# Databricks notebook source
from dataclasses import dataclass, field
import pyspark.sql.types as T
import pyspark.sql.functions as F
import json
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta
from pyspark.sql.column import Column
from functools import reduce
from pyspark.sql import DataFrame as df

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

# DBTITLE 1,Common Functions
def timenow():
  return datetime.now().strftime("%Y%m%d %X")
 
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
 
def parent_breakdown(whole_breakdown: str) -> str:
  return whole_breakdown.split(";")[0]
 
def child_breakdown(whole_breakdown: str) -> str:
  return whole_breakdown.split(";")[-1]
 
def get_pyspark_column_name(col_expr: Column) -> str:
  s = col_expr._jc.toString()
  if s.startswith("`") and s.endswith("`"):
      return s[1:-1]
  
  return s
 
def get_unique_pyspark_column_list(col_list: list) -> list:
  seen_exprs = set()
  unique_columns = []
  for col in col_list:
      expr_str = str(col._jc.toString())  # low-level Spark column string
      if expr_str not in seen_exprs:
          seen_exprs.add(expr_str)
          unique_columns.append(col)
  
  return unique_columns  
 
def add_alias(pyspark_column_name: Column, alias: str) -> Column:
  '''
  This function returns a column (based on a string of a defined pyspark column 
  that may change based on a parameter) with an alias in front of it, to enable 
  joins between 2 tables and allow the user to choose which table to select the colunn from.
  '''
  pyspark_column_alias = F.col(f"{alias}.{pyspark_column_name}")
  
  return pyspark_column_alias     

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
  elif status in ["Performance", "Final", "Adhoc"]:
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
  This function returns the year_of_count which should be used to extract data from reference_data.ONS_POPULATION_V2.  
  If the financial_yr_start is greater than the existing max(current_year) in reference_data.ONS_POPULATION_V2 then use
  current_year = max(current_year).
  '''
  current_year = get_financial_yr_start(rp_startdate)[0:4]
  max_year_of_count = spark.sql(f"select max(year_of_count) AS year_of_count from reference_data.ONS_POPULATION_V2 where GEOGRAPHIC_GROUP_CODE = 'E38'")
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
  reference_data: str = "reference_data"
   
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
def mh_freq_to_rp_startdate(freq: str, rp_startdate: str) -> str:
  """  This function gets the corresponding rp_startdate for a measure_id
  depending on the "freq" key value in the metadata
  
  Current Frequency values:
  ["M", "Q", "12M"]
  
  Example:
  mh_freq_to_rp_startdate("2022-03-31", "M")
  >> "2022-01-01"
  """
  if freq == "12M":
    rp_startdate = get_12m_startdate(rp_startdate)
  elif freq == "Q":
    rp_startdate = get_qtr_startdate(rp_startdate)
  else: ##Monthly most common frequency
    rp_startdate = rp_startdate
    
  return rp_startdate
 
def create_agg_df(
  df: df,
  db_source: str,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  measure_name: str,
  column_order: list) -> df:
  """
  
  """
  aggregation_field = (F.expr(aggregation_field).alias("MEASURE_VALUE"))
  
  agg_df = (
            df
            .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc)
            .agg(aggregation_field)
            .select(
              "*",
              F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
              F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
              F.lit(breakdown).alias("BREAKDOWN"),
              F.lit(status).alias("STATUS"),
              primary_level.alias("PRIMARY_LEVEL"),
              primary_level_desc.alias("PRIMARY_LEVEL_DESCRIPTION"),
              secondary_level.alias("SECONDARY_LEVEL"),
              secondary_level_desc.alias("SECONDARY_LEVEL_DESCRIPTION"),              
              F.lit(measure_id).alias("MEASURE_ID"),
              F.lit(measure_name).alias("MEASURE_NAME"),
              F.lit(db_source).alias("SOURCE_DB"),
            )
            .select(*column_order)
  )
  
  return agg_df 
 
def produce_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    agg_df = create_agg_df(prep_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df  
 
def access_filter_for_breakdown(prep_df: df, breakdown_name: str) -> df:
  """  This function filters the access-related prep table with the relevant
  access ROW_NUMBER() field depending on the breakdown being aggregated i.e. AccessEngRN
  to be equal to 1
  
  NOTE: This function is only currently required for the CMH and CYP Access prep tables
  """ 
  if breakdown_name == "Provider" or parent_breakdown(breakdown_name) == "Provider":
    rn_field = "AccessProvRN"
  elif (breakdown_name == "CCG of GP Practice or Residence" or parent_breakdown(breakdown_name) == "CCG of GP Practice or Residence") and breakdown_name != "CCG of GP Practice or Residence; Provider":
    rn_field = "AccessCCGRN"
  elif (breakdown_name == "CCG - Registration or Residence" or parent_breakdown(breakdown_name) == "CCG - Registration or Residence") and breakdown_name != "CCG - Registration or Residence; Provider":
    rn_field = "AccessCCGRN"
  elif breakdown_name == "STP of GP Practice or Residence" or parent_breakdown(breakdown_name) == "STP of GP Practice or Residence":
    rn_field = "AccessSTPRN"
  elif breakdown_name == "Commissioning Region" or parent_breakdown(breakdown_name) == "Commissioning Region":
    rn_field = "AccessRegionRN"
  elif breakdown_name == "LA/UA":
    rn_field = "AccessLARN"
  elif breakdown_name == "CCG of GP Practice or Residence; Provider":
    rn_field = "AccessCCGProvRN"  
  else:
    rn_field = "AccessEngRN"
    
  filt_df = (
    prep_df
    .filter(F.col(rn_field) == 1)
  )
  
  return filt_df
 
def fy_access_filter_for_breakdown(prep_df: df, breakdown_name: str) -> df:
  """  This function filters the access-related prep table with the relevant
  access ROW_NUMBER() field depending on the breakdown being aggregated i.e. AccessEngRN
  to be equal to 1
  
  NOTE: This function is only currently required for the CMH and CYP Access prep tables
  """ 
  if breakdown_name == "Provider":
    rn_field = "FYAccessRNProv"
  elif breakdown_name == "CCG of GP Practice or Residence":
    rn_field = "FYAccessCCGRN"
  elif breakdown_name == "STP of GP Practice or Residence":
    rn_field = "FYAccessSTPRN"
  elif breakdown_name == "Commissioning Region":
    rn_field = "FYAccessRegionRN"
  else:
    rn_field = "FYAccessEngRN"
    
  filt_df = (
    prep_df
    .filter(F.col(rn_field) == "1")
  )
  
  return filt_df
 
def produce_filter_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
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
        
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df
  
def produce_access_filter_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
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
    
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df
  
def produce_fy_access_filter_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,   
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:    
  """  This function produces the aggregation output dataframe from a defined preparation table
  for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
  """     
  prep_df = spark.table(f"{db_output}.{table_name}")
 
  prep_access_df = fy_access_filter_for_breakdown(prep_df, breakdown)
 
  prep_filter_df = (
    prep_access_df.filter(filter_clause)
  )
 
  agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                         primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                         aggregation_field, breakdown, status, measure_id, measure_name, column_order)
 
  return agg_df 

def produce_date_filter_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    prep_filter_df = (
      prep_df
      .filter(
      (F.col("ReportingPeriodEndDate") == rp_enddate)
      )
    )
        
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df
  
def get_numerator_df(df: df, numerator_id: str, breakdown: str, status: str, db_source: str, rp_enddate: str) -> df:
  numerator_df = (
    df
    .filter(
      (F.col("MEASURE_ID") == numerator_id)
      & (F.col("BREAKDOWN") == breakdown)
      & (F.col("STATUS") == status)
      & (F.col("SOURCE_DB") == db_source)
      & (F.col("REPORTING_PERIOD_END") == rp_enddate)
    )
    .select(
      df.BREAKDOWN, 
      df.PRIMARY_LEVEL, df.PRIMARY_LEVEL_DESCRIPTION,
      df.SECONDARY_LEVEL, df.SECONDARY_LEVEL_DESCRIPTION,
      df.MEASURE_VALUE.alias("NUMERATOR_COUNT")    
    ).distinct()
  )
  
  return numerator_df
  
def get_denominator_df(df: df, denominator_id: str, breakdown: str, status: str, db_source: str, rp_enddate: str) -> df: 
    
  denominator_df = (
      df
      .filter(
        (F.col("MEASURE_ID") == denominator_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("STATUS") == status)        
        & (F.col("SOURCE_DB") == db_source)
        & (F.col("REPORTING_PERIOD_END") == rp_enddate)
      )
      .select(
        df.BREAKDOWN, 
        df.PRIMARY_LEVEL, df.PRIMARY_LEVEL_DESCRIPTION,
        df.SECONDARY_LEVEL, df.SECONDARY_LEVEL_DESCRIPTION,
        df.MEASURE_VALUE.alias("DENOMINATOR_COUNT")    
      ).distinct()
    )
  
  return denominator_df
 
def create_crude_rate_prep_df(numerator_df: df, denominator_df: df) -> df:    
  crude_rate_prep_df = (
    numerator_df
    .join(denominator_df,
                  (numerator_df.BREAKDOWN == denominator_df.BREAKDOWN) &
                  (numerator_df.PRIMARY_LEVEL == denominator_df.PRIMARY_LEVEL) &   
                  (numerator_df.SECONDARY_LEVEL == denominator_df.SECONDARY_LEVEL),
                  how="left")
    .select(
      numerator_df.BREAKDOWN, 
      numerator_df.PRIMARY_LEVEL, numerator_df.PRIMARY_LEVEL_DESCRIPTION,
      numerator_df.SECONDARY_LEVEL, numerator_df.SECONDARY_LEVEL_DESCRIPTION,
      numerator_df.NUMERATOR_COUNT,
      denominator_df.DENOMINATOR_COUNT
    )
  )  
  
  return crude_rate_prep_df
  
def produce_crude_rate_agg_df(
  db_output: str,
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    insert_df = spark.table(f"{db_output}.{table_name}")
    
    numerator_df = get_numerator_df(insert_df, numerator_id, breakdown, status, db_source, rp_enddate)
 
    denominator_df = get_denominator_df(insert_df, denominator_id, breakdown, status, db_source, rp_enddate)
 
    crude_rate_prep_df = create_crude_rate_prep_df(numerator_df, denominator_df)
    
    aggregation_field = (F.expr(aggregation_field).alias("MEASURE_VALUE"))
    
    agg_df = (
        crude_rate_prep_df
        .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc)
        .agg(aggregation_field)
        .select(
              "*",
              F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
              F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
              F.lit(breakdown).alias("BREAKDOWN"),
              F.lit(status).alias("STATUS"),             
              F.lit(measure_id).alias("MEASURE_ID"),
              F.lit(measure_name).alias("MEASURE_NAME"),
              F.lit(db_source).alias("SOURCE_DB"),
            )
        .select(*column_order)
    )
    
    return agg_df
  
def produce_filter_oaps_bed_days_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    if "Bed Type" in breakdown_name:
      if measure_id == "OAP02aM" or measure_id == "OAP02aQ" or measure_id == "OAP02aY":
        filter_clause = (F.col("ReasonOAT") == "10") & (F.col("Submitted_In_RP") == 1) & (F.col("Acute_Bed") == "Y")
        aggregation_field = aggregation_field.replace("_HS", "_WS")
      else:
        filter_clause = (F.col("ReasonOAT") == "10") & (F.col("Submitted_In_RP") == 1)
        aggregation_field = aggregation_field.replace("_HS", "_WS")
      
    
    prep_filter_df = (
      prep_df.filter(filter_clause)
    )
        
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df
  
def produce_filter_bed_days_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    if "Bed Type" in breakdown_name:
      if measure_id[-2] == "a": ###all oaps measures in acute beds exclusively have 'a' after measure i.e. OAP02 is bed days OAP02a is bed days in Adult Acute Beds
        filter_clause = (F.col("Acute_Bed") == "Y")
        aggregation_field = aggregation_field.replace("_HS", "_WS")
      else:
        filter_clause = (F.col("UniqHospProvSpellID").isNotNull())
        aggregation_field = aggregation_field.replace("_HS", "_WS")
      
    
    prep_filter_df = (
      prep_df.filter(filter_clause)
    )
        
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df
  
def join_standardisation_groupings(df1: df, df2: df, join_list: list, unique_fields: list) -> df:
  #alias each dataframe
  df1 = df1.alias("a")
  df2 = df2.alias("b")
  
  #for each pyspark Column in the list change to column name string
  join_cols = [get_pyspark_column_name(col) for col in join_list]
  
  #create conditions for join (this assumes you are joining on each column provided in join_list - both dataframes must have these columns)
  join_conditions = [F.col(f"a.{col}") == F.col(f"b.{col}") for col in join_cols]
  
  #inner join dataframes based on conditions
  joined_df = df1.join(df2, on=join_conditions, how="inner")  
  
  #create list of columns which exist in both dataframes and in join_list
  #this will always prefer columns from df1 where column appears in both dataframes  
  common_columns = [F.col(f"a.{col}") for col in join_cols]
  
  #select common_columns and unique_fields - which allows you to bring in fields which are unique to each dataframe
  final_df = (
    joined_df
    .select(
      *common_columns,
      *unique_fields
    )            
  )
  
  return final_df
 
def produce_direct_standardised_rate_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:    
  """  This function produces the age and gender direct standardisation rate and
  confidence interval range for a given breakdown aggregation output dataframe from 
  a defined preparation table which produces the count based on the relevant breakdown,
  a population table is selected for all standardisation rate and confidence interval 
  measures and breakdowns according to the measure_metadata dictionary.
  
  This is the same function as below EXCEPT it doens't do the final step that produces 
  agg_df. This is because the data produced through this function is not the final data
  but instead feeds the absolute mean deviation function to produces measures MHS95d and 
  MHS95e.
  
  Current measures
  ----------------
  MHS95d, MHS95e
  """  
  #all columns for direct standardisation by age and gender
  columns_for_standardisation = [primary_level, primary_level_desc, secondary_level, secondary_level_desc, F.col("Age_Band"), F.col("Der_Gender")]
  unique_columns_for_standardisation = get_unique_pyspark_column_list(columns_for_standardisation)
  
  #columns for breakdown, this is done after we have joined population at age/gender/breakdown
  columns_for_breakdown = [primary_level, primary_level_desc, secondary_level, secondary_level_desc]
  unique_columns_for_breakdown = get_unique_pyspark_column_list(columns_for_breakdown)
  
  #get prep table needed for counts
  prep_df = spark.table(f"{db_output}.{table_name}")
  
  #putting this clause in now because CYP Access is the most complex measure in terms of aggregation, but might need amending as we add more complex aggregations
  if numerator_id == "MHS95": 
    prep_df = access_filter_for_breakdown(prep_df, breakdown)
  
  #apply filter to prep_df if "filter_clause" is populated in measure_metadata
  if isinstance(filter_clause, Column):
    prep_df = (
      prep_df
      .filter(filter_clause)
    )
    
  #cant think of another way to get the logic for this as we need the aggregate_field to differentiate between DSR and CI in the aggregate field in measure_metadata
  count_aggregation_logic = standardisation_counts_metadata[numerator_id]["aggregate_field"]
  count_aggregation_field = (F.expr(count_aggregation_logic).alias("MEASURE_VALUE"))
    
  #aggregate counts by age x gender x breakdown
  counts_df = (
    prep_df
    .where( (F.col("Age_Band") != "UNKNOWN") & (F.col("Der_Gender") != "UNKNOWN") )
    .groupBy(*unique_columns_for_standardisation)
    .agg(count_aggregation_field)
  )
  
  #select correct population table based on breakdown (will need updating if we ever add more breakdowns for direct standardisation)
  if "IMD" in child_breakdown(breakdown):
      pop_df = spark.table(f"{db_output}.age_gender_std_imd_pop")
  elif "Ethnicity" in child_breakdown(breakdown):
      pop_df = spark.table(f"{db_output}.age_gender_std_eth_pop")
  else:
      raise ValueError("Invalid breakdown for standardisation rates")
      
  #look at age band value of counts_df and rename same column in pop_df to age band (for joining)
  #this is needed because in prep_df the relevant age band is always called "Age_Band" but in pop_df there are many age bands
  counts_age_values = set([row["Age_Band"] for row in counts_df.select("Age_Band").distinct().collect()])
  pop_age_columns = [col for col in pop_df.columns if col.startswith("Age_Group")]
  
  for pop_age_col in pop_age_columns:
    pop_age_values = set([row[pop_age_col] for row in pop_df.select(pop_age_col).distinct().collect()])
    if all(age in pop_age_values for age in counts_age_values):            
      matching_age_col = pop_age_col            
      break
      
  #rename matching_age_col to Age_Band in pop_df
  pop_df = pop_df.withColumnRenamed(matching_age_col, "Age_Band")
  
  #aggregate population by columns for standardisation align schemas for joining (pick only necessary columns)
  pop_df = (
    pop_df
    .groupBy(*unique_columns_for_standardisation)
    .agg(F.sum("POPULATION_COUNT").alias("POPULATION"))
  )
  
  #join counts_df and pop_df to get counts and population by age x gender x breakdown
  joined_df = join_standardisation_groupings(counts_df, pop_df, unique_columns_for_standardisation, [F.col("MEASURE_VALUE"), F.col("POPULATION")])
    
  #calculate total standard population by breakdown
  total_pop_df = (
    joined_df
    .groupBy(*unique_columns_for_breakdown)
    .agg(F.sum("POPULATION").alias("TOTAL_POPULATION"))
  )
  
  #join joined_df and total_pop_df to get counts, population and total population by breakdown
  std_pop_df = join_standardisation_groupings(joined_df, total_pop_df, unique_columns_for_breakdown, [F.col("MEASURE_VALUE"), F.col("POPULATION"), F.col("TOTAL_POPULATION")])
  
  #calculate weights and standardised rates
  weight_df = (
    std_pop_df
    .withColumn("WEIGHT", F.col("POPULATION") / F.col("TOTAL_POPULATION")) #need to use withColumn as WEIGHT is needed in the next calculation
    .withColumn("WEIGHTED_RATE", (F.col("MEASURE_VALUE") / F.col("POPULATION")) * F.col("WEIGHT"))
  )
  
  #calculate directly standardised rate and variance (needed for calculating confidence intervals)
  dsr_df = (
    weight_df
    .groupby(*unique_columns_for_breakdown)
    .agg(
      F.sum("WEIGHTED_RATE").alias("STANDARDISED_RATE"),
      F.sum((F.col("WEIGHT") ** 2) * (F.col("MEASURE_VALUE") / (F.col("POPULATION") ** 2))).alias("VARIANCE")
    )
  )
  
  #calculate confidence interval range 1.96 * SQRT(VARIANCE)
  dsr_ci_df = (
    dsr_df
    .select(
      "*",
      (1.96 * F.sqrt(F.col("VARIANCE"))).alias("CI_RANGE")
    )
  )
  
  return weight_df, dsr_ci_df, total_pop_df
 
def produce_direct_standardised_rate_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:    
  """  This function produces the age and gender direct standardisation rate and
  confidence interval range for a given breakdown aggregation output dataframe from 
  a defined preparation table which produces the count based on the relevant breakdown,
  a population table is selected for all standardisation rate and confidence interval 
  measures and breakdowns according to the measure_metadata dictionary.
  
  Current measures
  ----------------
  MHS95b, MHS95c
  """  
  weight_df, dsr_ci_df, total_pop_df = produce_direct_standardised_rate_df(
  db_output, db_source, table_name, filter_clause,rp_startdate, rp_enddate, 
  primary_level, primary_level_desc, secondary_level, secondary_level_desc,
  aggregation_field, breakdown, status, measure_id, numerator_id, denominator_id,
  measure_name, column_order)
 
  agg_groupby_cols = [primary_level.alias("PRIMARY_LEVEL"),            
                      primary_level_desc.alias("PRIMARY_LEVEL_DESCRIPTION"),              
                      secondary_level.alias("SECONDARY_LEVEL"),              
                      secondary_level_desc.alias("SECONDARY_LEVEL_DESCRIPTION")]
  
  aggregation_field = (F.expr(aggregation_field).alias("MEASURE_VALUE"))
  
  agg_df = (    
    dsr_ci_df
    .groupBy(*agg_groupby_cols)
    .agg(aggregation_field)
    .select(
      "*",
      F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
      F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
      F.lit(breakdown).alias("BREAKDOWN"),
      F.lit(status).alias("STATUS"),         
      F.lit(measure_id).alias("MEASURE_ID"),
      F.lit(measure_name).alias("MEASURE_NAME"),
      F.lit(db_source).alias("SOURCE_DB"),
    )
    .select(*column_order)
  )
  
  return agg_df
 
def produce_absolute_mean_deviation_white_vs_non_white_upper_ethnicites_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:    
  """ This function uses the produce_direct_standardisation_rate_df function to produce
  age and gender direct standardisation rate for a given breakdown aggregation output dataframe.
  This function then produces an absolute mean deviation OR weighted absolute mean deviation 
  (determined in the measure_metadata) between the direct standardised rate of each non-White 
  upper ethnicity and the White upper ethnicity.
  
  Current measures
  ----------------
  MHS95d, MHS95e
  """  
  weight_df, dsr_ci_df, total_pop_df = produce_direct_standardised_rate_df(
  db_output, db_source, table_name, filter_clause,rp_startdate, rp_enddate, 
  primary_level, primary_level_desc, secondary_level, secondary_level_desc,
  aggregation_field, breakdown, status, measure_id, numerator_id, denominator_id,
  measure_name, column_order)
    
  primary_level_name = get_pyspark_column_name(primary_level)
  secondary_level_name = get_pyspark_column_name(secondary_level)
  secondary_level_desc_name = get_pyspark_column_name(secondary_level_desc)
  
  if secondary_level_name == "NONE":
 
    weight_df_subset = total_pop_df.filter(primary_level != "White or White British").agg(F.sum(F.col("TOTAL_POPULATION")).alias("Non-WhitePop"))
    weight_df_subset2 = dsr_ci_df.filter(primary_level == "White or White British").agg(F.sum(F.col("STANDARDISED_RATE")).alias("WhiteStanRate"))
 
    dev_weights_df = dsr_ci_df.alias("a").join(total_pop_df.alias("b"), (add_alias(primary_level_name, "a") == add_alias(primary_level_name, "b")), 'left') \
                                         .select(add_alias(primary_level_name, "a"), F.col("a.STANDARDISED_RATE"), F.col("b.TOTAL_POPULATION")).alias("a") \
                                         .crossJoin(F.broadcast(weight_df_subset)) \
                                         .crossJoin(F.broadcast(weight_df_subset2))
 
    count_ethnicities = dev_weights_df.agg(F.count(F.col("*")).alias("NumofEth"))
 
    count_eth_join = dev_weights_df.alias("a").crossJoin(F.broadcast(count_ethnicities)) \
                                              .select("a.*", (F.col("NumofEth") - 1).alias("NumofEth"))
 
    dev_weights_df2 = count_eth_join.withColumn("PopWeight", F.col("TOTAL_POPULATION") / F.col("Non-WhitePop")) \
                                    .withColumn("DiffFromWhite", F.col("STANDARDISED_RATE") - F.col("WhiteStanRate")) \
                                    .withColumn("AbsoluteDiffFromWhite", F.abs(F.col("DiffFromWhite"))) \
                                    .withColumn("AbsoluteDiffWeighted", F.col("AbsoluteDiffFromWhite") * F.col("PopWeight"))
 
    mean_deviation = dev_weights_df2.groupBy(F.col("NumofEth")).agg((F.sum(F.col("AbsoluteDiffFromWhite")) / F.col("NumofEth")).alias("MeanAbsoluteDeviation"),
                                                                     F.sum(F.col("AbsoluteDiffWeighted")).alias("WeightedMeanAbsoluteDeviation")) \
                                    .drop("NumofEth")
 
  else: 
 
    weight_df_subset = total_pop_df.filter(primary_level != "White or White British").groupBy(secondary_level).agg(F.sum(F.col("TOTAL_POPULATION")).alias("Non-WhitePop"))
    weight_df_subset2 = dsr_ci_df.filter(primary_level == "White or White British").groupBy(secondary_level).agg(F.sum(F.col("STANDARDISED_RATE")).alias("WhiteStanRate"))
 
    dev_weights_df = dsr_ci_df.alias("a").join(total_pop_df.alias("b"), ((add_alias(primary_level_name, "a") == add_alias(primary_level_name, "b")) & (add_alias(secondary_level_name, "a") == add_alias(secondary_level_name, "b"))), 'left') \
                                         .select(add_alias(primary_level_name, "a"), add_alias(secondary_level_name, "a"), add_alias(secondary_level_desc_name, "a"), F.col("a.STANDARDISED_RATE"), F.col("b.TOTAL_POPULATION")).alias("a") \
                                         .join(weight_df_subset.alias("c"), add_alias(secondary_level_name, "a") == add_alias(secondary_level_name, "c"), 'left') \
                                         .join(weight_df_subset2.alias("d"), add_alias(secondary_level_name, "a") == add_alias(secondary_level_name, "d"), 'left') \
                                         .select("a.*", F.col("c.Non-WhitePop"), F.col("d.WhiteStanRate"))
 
    count_ethnicities = dev_weights_df.groupBy(secondary_level).agg(F.count(F.col("*")).alias("NumofEth"))
 
    count_eth_join = dev_weights_df.alias("a").join(count_ethnicities.alias("b"), add_alias(secondary_level_name, "a") == add_alias(secondary_level_name, "b"), 'left') \
                                              .select("a.*", (F.col("NumofEth") - 1).alias("NumofEth"))
 
    dev_weights_df2 = count_eth_join.withColumn("PopWeight", F.col("TOTAL_POPULATION") / F.col("Non-WhitePop")) \
                                    .withColumn("DiffFromWhite", F.col("STANDARDISED_RATE") - F.col("WhiteStanRate")) \
                                    .withColumn("AbsoluteDiffFromWhite", F.abs(F.col("DiffFromWhite"))) \
                                    .withColumn("AbsoluteDiffWeighted", F.col("AbsoluteDiffFromWhite") * F.col("PopWeight"))
 
    mean_deviation = dev_weights_df2.groupBy(secondary_level, secondary_level_desc, F.col("NumofEth")).agg((F.sum(F.col("AbsoluteDiffFromWhite")) / F.col("NumofEth")).alias("MeanAbsoluteDeviation"),
                                                                                      F.sum(F.col("AbsoluteDiffWeighted")).alias("WeightedMeanAbsoluteDeviation")) \
                                    .drop("NumofEth")
  
  agg_groupby_cols = [secondary_level.alias("SECONDARY_LEVEL"),              
                      secondary_level_desc.alias("SECONDARY_LEVEL_DESCRIPTION")]
 
  aggregation_field = (F.expr(aggregation_field).alias("MEASURE_VALUE"))
  
  agg_df = (    
    mean_deviation
    .groupBy(*agg_groupby_cols)
    .agg(aggregation_field)
    .select(
      F.when(F.col("SECONDARY_LEVEL") == "NONE", F.lit("England")).otherwise(F.col("SECONDARY_LEVEL")).alias("PRIMARY_LEVEL"),
      F.when(F.col("SECONDARY_LEVEL") == "NONE", F.lit("England")).otherwise(F.col("SECONDARY_LEVEL_DESCRIPTION")).alias("PRIMARY_LEVEL_DESCRIPTION"),
      "*",
      F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
      F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
      F.lit(breakdown).alias("BREAKDOWN"),
      F.lit(status).alias("STATUS"),         
      F.lit(measure_id).alias("MEASURE_ID"),
      F.lit(measure_name).alias("MEASURE_NAME"),
      F.lit(db_source).alias("SOURCE_DB"),
    )
    .withColumn("SECONDARY_LEVEL", F.lit("NONE"))
    .withColumn("SECONDARY_LEVEL_DESCRIPTION", F.lit("NONE"))
    .select(*column_order)
  )
  
  return agg_df
 
def produce_absolute_difference_between_two_standardised_rates_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  aggregation_field: str,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str,  
  denominator_id: str,
  measure_name: str,
  column_order: list
) -> df:    
 
  weight_df, dsr_ci_df, total_pop_df = produce_direct_standardised_rate_df(
  db_output, db_source, table_name, filter_clause,rp_startdate, rp_enddate, 
  primary_level, primary_level_desc, secondary_level, secondary_level_desc,
  aggregation_field, breakdown, status, measure_id, numerator_id, denominator_id,
  measure_name, column_order)
 
  secondary_level_name = get_pyspark_column_name(secondary_level)
 
  if "Ethnicity" in breakdown:
    main_group = "White British"
 
  else:
    main_group = "Most deprived quintile"
 
  if secondary_level_name == "NONE":
 
    rate_subset = dsr_ci_df.filter(primary_level == main_group).agg(F.sum(F.col("STANDARDISED_RATE")).alias("MainRate"))
 
    main_rate = dsr_ci_df.crossJoin(rate_subset)
 
    main_rate2 = main_rate.withColumn("DiffFromMain", F.col("STANDARDISED_RATE") - F.col("MainRate")) \
                          .withColumn("AbsoluteDiffFromMain", F.abs(F.col("DiffFromMain")))
 
    diff_from_main = main_rate2.filter(primary_level != main_group).select(F.col("AbsoluteDiffFromMain"))
 
  else: 
 
    rate_subset = dsr_ci_df.filter(primary_level == main_group).groupBy(secondary_level).agg(F.sum(F.col("STANDARDISED_RATE")).alias("MainRate"))
 
    main_rate = dsr_ci_df.alias("a").join(rate_subset.alias("b"), add_alias(secondary_level_name, "a") == add_alias(secondary_level_name, "b"), 'left') \
                                    .select("a.*", F.col("b.MainRate"))
 
    main_rate2 = main_rate.withColumn("DiffFromMain", F.col("STANDARDISED_RATE") - F.col("MainRate")) \
                          .withColumn("AbsoluteDiffFromMain", F.abs(F.col("DiffFromMain")))
 
    diff_from_main = main_rate2.filter(primary_level != main_group).select(secondary_level, secondary_level_desc, F.col("AbsoluteDiffFromMain"))
  
  agg_groupby_cols = [secondary_level.alias("SECONDARY_LEVEL"),              
                      secondary_level_desc.alias("SECONDARY_LEVEL_DESCRIPTION")]
 
  aggregation_field = (F.expr(aggregation_field).alias("MEASURE_VALUE"))
  
  agg_df = (    
    diff_from_main
    .groupBy(*agg_groupby_cols)
    .agg(aggregation_field)
    .select(
      F.when(F.col("SECONDARY_LEVEL") == "NONE", F.lit("England")).otherwise(F.col("SECONDARY_LEVEL")).alias("PRIMARY_LEVEL"),
      F.when(F.col("SECONDARY_LEVEL") == "NONE", F.lit("England")).otherwise(F.col("SECONDARY_LEVEL_DESCRIPTION")).alias("PRIMARY_LEVEL_DESCRIPTION"),
      "*",
      F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
      F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
      F.lit(breakdown).alias("BREAKDOWN"),
      F.lit(status).alias("STATUS"),         
      F.lit(measure_id).alias("MEASURE_ID"),
      F.lit(measure_name).alias("MEASURE_NAME"),
      F.lit(db_source).alias("SOURCE_DB"),
    )
    .withColumn("SECONDARY_LEVEL", F.lit("NONE"))
    .withColumn("SECONDARY_LEVEL_DESCRIPTION", F.lit("NONE"))
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

# DBTITLE 1,Suppression functions - added to do one round of suppression at the end of the process, rather than one per product
def mhsds_suppression_mean_dev_and_stan_rates_diff(df: df, suppression_type: str, breakdown: str, measure_id: str, rp_enddate: str, status: str, numerator_id: str, db_source: str) -> df:
  """ The function has the logic for suppression of MHSDS measures
  relating to mean deviation, specifically of non-White upper ethnicities
  from White British, and difference in standardised rates between 2
  groups, specifically of white vs non-white and IMD core 20 and other
  IMD quintiles.
  If numerator_id value for ANY numerator group = "*" then mean deviation 
  value or difference in rates value = "*", else round to nearest whole number.
  
  Example:
  """
  supp_method = F.udf(lambda z: count_suppression(z))  
  
  perc_values = (
    df
    .filter(
      (F.col("MEASURE_ID") == measure_id)
      & (F.col("BREAKDOWN") == breakdown)
      & (F.col("REPORTING_PERIOD_END") == rp_enddate)
      & (F.col("STATUS") == status)
      & (F.col("SOURCE_DB") == db_source)
    )
  )
    
  if child_breakdown(breakdown) == " Mean Deviation of Upper Ethnicity":
    breakdown_2 = "; Upper Ethnicity"
  elif "Most Deprived Quintile" in breakdown:
    breakdown_2 = "; IMD Core20"
  else:
    breakdown_2 = "; Ethnicity (White British/Non-White British)"
    
  num_values = (
    df
    .filter(
      (F.col("MEASURE_ID") == numerator_id)
      & (F.col("BREAKDOWN") == F.concat(F.lit(parent_breakdown(breakdown)), F.lit(breakdown_2)))
    & (F.col("REPORTING_PERIOD_END") == rp_enddate)
    & (F.col("STATUS") == status)
    & (F.col("SOURCE_DB") == db_source)
  )
  .select(
    F.col("BREAKDOWN"), F.col("PRIMARY_LEVEL"), F.when(F.col("SECONDARY_LEVEL") == "NONE", F.lit("England")).otherwise(F.col("SECONDARY_LEVEL")).alias("SECONDARY_LEVEL"), F.col("MEASURE_ID").alias("NUMERATOR_ID"), F.col("MEASURE_VALUE").alias("NUMERATOR_VALUE")
  )
)
 
  if parent_breakdown(breakdown) == "England":
 
    perc_num_comb = perc_values.alias("a").join(num_values.alias("b"), F.col("a.PRIMARY_LEVEL") == F.col("b.SECONDARY_LEVEL"), 'left').select("a.*", F.col("NUMERATOR_ID"), F.col("NUMERATOR_VALUE"))
 
  else:
 
    perc_num_comb = perc_values.alias("a").join(num_values.alias("b"), F.col("a.PRIMARY_LEVEL") == F.col("b.PRIMARY_LEVEL"), 'left').select("a.*", F.col("NUMERATOR_ID"), F.col("NUMERATOR_VALUE"))
 
  #percentage suppression logic
  perc_supp_logic = (
  F.when(F.col("NUMERATOR_VALUE_SUPP") == "*", F.lit("*"))
   .otherwise(F.round(F.col("MEASURE_VALUE"), 0)) #round to nearest whole number (0dp)
  )
 
  perc_num_comb_supp_df = (
    perc_num_comb
    .withColumn("NUMERATOR_VALUE_SUPP", supp_method(F.col("NUMERATOR_VALUE")))
    .withColumn("MEASURE_VALUE", perc_supp_logic)
  )
 
  perc_num_comb_flag = perc_num_comb_supp_df.withColumn("Suppression_Flag", F.when(F.col("MEASURE_VALUE") == "*", F.lit(1)).otherwise(F.lit(0)))
 
  exclude_cols = ["NUMERATOR_ID", "NUMERATOR_VALUE", "NUMERATOR_VALUE_SUPP", "Suppression_Flag"]
  group_cols = [col for col in perc_num_comb_flag.columns if col not in exclude_cols]
  perc_num_comb_part = perc_num_comb_flag.groupBy(group_cols).agg(F.max("Suppression_Flag").alias("Any_Suppressed")) # how to group by on all except one column
 
  supp_df = perc_num_comb_part.withColumn("MEASURE_VALUE", F.when(F.col("Any_Suppressed") == 1, F.lit("*")).otherwise(F.col("MEASURE_VALUE"))).drop("Any_Suppressed")                 
  
  return supp_df

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
  
  
def mhsds_suppression(df: df, suppression_type: str, breakdown: str, measure_id: str, rp_enddate: str, status: str, numerator_id: str, db_source: str) -> df:
  """  The function has the logic for suppression of MHSDS count and percent measures
  if numerator_id value of percentage = "*" then percentage value = "*"
  else round to nearest whole number
  
  Example:
  """
  supp_method = F.udf(lambda z: count_suppression(z))
  
  if child_breakdown(breakdown) == " Mean Deviation of Upper Ethnicity" or "Standardised Rate Difference from" in breakdown:
    supp_df = mhsds_suppression_mean_dev_and_stan_rates_diff(df, suppression_type, breakdown, measure_id, rp_enddate, status, numerator_id, db_source)
  
  else:
    if suppression_type == "count":
      supp_df = (
        df
        .filter(
          (F.col("MEASURE_ID") == measure_id)
          & (F.col("BREAKDOWN") == breakdown)
          & (F.col("REPORTING_PERIOD_END") == rp_enddate)
          & (F.col("STATUS") == status)
          & (F.col("SOURCE_DB") == db_source)
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
          & (F.col("SOURCE_DB") == db_source)
        )
      )
 
      num_values = (
        df
        .filter(
          (F.col("MEASURE_ID") == numerator_id)
          & (F.col("BREAKDOWN") == breakdown)
          & (F.col("REPORTING_PERIOD_END") == rp_enddate)
          & (F.col("STATUS") == status)
          & (F.col("SOURCE_DB") == db_source)
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

# DBTITLE 1,Reference data functions
def mapage(x):
  x = x.lower()
  lx = x.split(" ")
  if len(lx) == 1 or " to " in x:
      ly = [i for i in range(int(lx[0]), 1 + int(lx[-1]))]
  elif "under " in x:
      ly = [i for i in range(int(lx[1]))]
  elif "over " in x:
      ly = [i for i in range(int(lx[1]), 125)]
  elif " or over" in x or " and over" in x:
      ly = [i for i in range(int(lx[0]), 125)]
  elif " or under" in x or " and under" in x:
      ly = [i for i in range(1 + int(lx[0]))]
  try:
    return ly
  except:
    print(">>>>> Warning >>>>> \n", x)
    return x

# COMMAND ----------

# DBTITLE 1,Breakdown functions
def list_dataframes():
    from pyspark.sql import DataFrame
    return [[k] for (k, v) in globals().items() if isinstance(v, DataFrame)]
 
def flatlist(list1):
  # change [[1,2,3],[4,5],...] to [1,2,3,4,5,...]
  return [item for sublist in list1 for item in sublist]  
 
def cross_join_level_listD(list1):
  ''' input list of 1 to 5 dictionaries from metadata to get cross-join'''
  
  len1 = len(list1)
  listU = flatlist([list1[i]["lookup_col"] for i in range(len1)])
  listC = [[[x[0],x[1]] if len(x) == 2 else [x[0]] for x in  list1[i]["level_list"]]
           for i in range(len1)]
  if len1 == 1: listLL = [x0 for x0 in listC[0]]
  if len1 == 2: listLL = [x0 + x1 for x0 in listC[0] for x1 in listC[1]]
  if len1 == 3: listLL = [x0 + x1 + x2 for x0 in listC[0] for x1 in listC[1] for x2 in listC[2]]
  if len1 == 4: listLL = [x0 + x1 + x2 + x3 for x0 in listC[0] for x1 in listC[1] for x2 in listC[2] for x3 in listC[3]]
  if len1 == 5: listLL = [x0 + x1 + x2 + x3 + x4 for x0 in listC[0] for x1 in listC[1]
                         for x2 in listC[2] for x3 in listC[3] for x4 in listC[4]]
 
  if 1 <= len1 and len1 <= 5:
    ''' dataframe option for output, commented out as returning dictionary instead
    schema1 = ", ".join([f"{x} string" for x in listU])
    print(schema1)
    
    df1 = spark.createDataFrame(listLL, schema = schema1)
    cols = [x for x in listU]
    return df1.select(cols)
    '''
    d1 = {"lookup_col": listU, "level_list": listLL}
    return d1
  
def cross_dict(list1):
  # input list of up to 4 single level dictionaries -> output cross join dictionary
  # please do not use double / triple level dictionaries as inputs
  len1 = len(list1)
  lln = [d["primary_level"] for d in list1]
  lld = [d["primary_level_desc"] for d in list1]
  llf = [d["level_fields"] for d in list1]
 
  listU = flatlist([list1[i]["lookup_col"] for i in range(len1)])
  listC = [[[x[0],x[1]] if len(x) == 2 else [x[0]] for x in  list1[i]["level_list"]]
           for i in range(len1)]
  if len1 == 1: listLL = [x0 for x0 in listC[0]]
  if len1 == 2: listLL = [x0 + x1 for x0 in listC[0] for x1 in listC[1]]
  if len1 == 3: listLL = [x0 + x1 + x2 for x0 in listC[0] for x1 in listC[1] for x2 in listC[2]]
  if len1 == 4: listLL = [x0 + x1 + x2 + x3 for x0 in listC[0] for x1 in listC[1] for x2 in listC[2] for x3 in listC[3]]
  if len1 == 5: listLL = [x0 + x1 + x2 + x3 + x4 for x0 in listC[0] for x1 in listC[1]
                         for x2 in listC[2] for x3 in listC[3] for x4 in listC[4]]
 
  if 1 <= len1 and len1 <= 5:
    cross_cols = {"lookup_col": listU, "level_list": listLL}
 
  breakdown_name = "; ".join([d["breakdown_name"] for d in list1])
  breakdown_name = breakdown_name.replace("England; ", "")
  
  final_lvl_tables = [list1[0]["level_tables"][0], list1[1]["level_tables"][0]]
 
  dict1 = {
    "breakdown_name": breakdown_name,
    "level_tier": 2,
    "level_tables": final_lvl_tables,
    "primary_level": lln[0],
    "primary_level_desc": lld[0],
    "secondary_level": lln[1],
    "secondary_level_desc": lld[1],     
    "level_list" : cross_cols["level_list"],
    "lookup_col" : cross_cols["lookup_col"],
    "level_fields" : [
    F.lit(breakdown_name).alias("breakdown"),
    list1[0]["level_fields"][1],
    list1[0]["level_fields"][2],
    list1[1]["level_fields"][1].alias("secondary_level"), 
    list1[1]["level_fields"][2].alias("secondary_level_desc")]
  }  
  return dict1
 
def pop_cross_dict(list1):
  # input list of up to 4 single level dictionaries -> output cross join dictionary
  # please do not use double / triple level dictionaries as inputs
  len1 = len(list1)
  lln = [d["primary_level"] for d in list1]
  lld = [d["primary_level_desc"] for d in list1]
  llf = [d["source_table"] for d in list1]
 
  breakdown_name = "; ".join([d["breakdown_name"] for d in list1])
  breakdown_name = breakdown_name.replace("England; ", "")
 
  dict1 = {
    "breakdown_name": breakdown_name,
    "source_table": llf[1],
    "primary_level": lln[0],
    "primary_level_desc": lld[0],
    "secondary_level": lln[1],
    "secondary_level_desc": lld[1],
    "aggregate_field": "SUM(POPULATION_COUNT)"
  }
  
  return dict1

# COMMAND ----------

# DBTITLE 1,CSV Skeleton functions
def get_var_name(variable):   # pass in a variable, get its name in text format
    for name in globals():
        if eval(name) is variable:
            return name
          
def unionAll(*dfs):
    return reduce(df.unionAll, dfs)
   
def convert_df_to_dictionary(db_output, table, cols):
  d1 = {}
  if type(table) == str:
    df = spark.sql(f"select * from {db_output}.{table}")
    df = df.select(*cols).distinct()
    d1["lookup_col"] = [df.columns]
    d1["level_list"] = [[x[0], x[1]] for x in df.collect()]
  return d1
 
def check_substring_exists_in_list(lst, string):
  checklist = []
  for name in lst:
    if name in string:
      checklist.append(True)
      
  check = any(checklist)
  return check
 
def createbreakdowndf(db_output, end_month_id, breakdown, freq):
  # this function returns a dataframe from the level_tables, level_list, level)fields and lookup_col
    
  breakdown_name = breakdown["breakdown_name"]
  level_lists = []
  lookup_cols = []
  lvl_tier = breakdown["level_tier"]
  lvl_tables = breakdown["level_tables"]
  lvl_list = breakdown["level_list"]
  lvl_fields = breakdown["level_fields"]
  lvl_cols = breakdown["lookup_col"]
  
  prov_geog_check = check_substring_exists_in_list(provider_parent_breakdowns, breakdown_name)
  oaps_prov_geog_check = check_substring_exists_in_list(oaps_provider_parent_breakdowns, breakdown_name)
  
  if oaps_prov_geog_check == True:
      lvl_tables = ["oaps_year" if x == "oaps_prov_placeholder" else x for x in lvl_tables]
  
  elif prov_geog_check == True and freq == "M":
      lvl_tables = ["bbrb_org_daily_latest_mhsds_providers" if x == "prov_placeholder" else x for x in lvl_tables]
    
  elif prov_geog_check == True and freq == "Q":
      lvl_tables = ["bbrb_org_daily_past_quarter_mhsds_providers" if x == "prov_placeholder" else x for x in lvl_tables]
      
  elif prov_geog_check == True and freq == "12M":
      lvl_tables = ["bbrb_org_daily_past_12_months_mhsds_providers" if x == "prov_placeholder" else x for x in lvl_tables]   
 
  if lvl_tier == 0:
    table = lvl_tables[0]
    df1 = spark.table(f"{db_output}.{table}")      
    df1 = df1.filter(        
      (F.col("FirstMonth") <= end_month_id) &        
      ((F.col("LastMonth") >= end_month_id) | (F.col("LastMonth").isNull()))
    )      
    df1 = df1.select(*lvl_fields).distinct()
    # create dataframe from level_list
#     schema = ", ".join(f"{col} string" for col in breakdown["lookup_col"])
 
#     try:
#       df1 = spark.table(f"{db_output}.{lvl_tables}")
#       df1 = df1.filter(
#         (F.col("FirstMonth") <= end_month_id) &
#         (F.col("LastMonth") >= end_month_id | F.col("LastMonth").isNull())
#       )
#       df1 = df1.select(*lvl_fields).distinct()
#     except:
#       print("mismatch between number of columns in lookup cols and number of columns level list")
#       print(breakdown["lookup_col"])
#       print(breakdown["level_list"])
    
  if lvl_tier == 1:
    # create dataframe from level_tables and add Unknown row
    table = lvl_tables[0]
    df1 = spark.sql(f"select * from {db_output}.{table}")
    df1 = df1.select(*lvl_fields).distinct()
    
  if lvl_tier >= 2:
    # create dataframe form level_tables, convert any dataframes to lists, cross join all lists, then produce dataframe
    # example
    # ccg_prac_res_gender_age_band_lower_chap1_bd = {  
    # "breakdown_name": "CCG - Registration or Residence; Gender; Age Group (Lower Level)",  
    # "level_tier": 3,
    # "level_tables": ["stp_region_mapping", gender_bd, age_band_chap1_bd],    .....
    
    for ix, table in enumerate(lvl_tables):     # ix is the counter / postion in lvl_tables
      
      if type(table) == str:
        if ix == 0: cols = [lvl_fields[1], lvl_fields[2]]
        if ix == 1: cols = [lvl_fields[3], lvl_fields[4]]
        if ix == 2: cols = [lvl_fields[5], lvl_fields[6]]
        if ix == 3: cols = [lvl_fields[7], lvl_fields[8]]
        try:
          df1 = spark.table(f"{db_output}.{table}")      
          df1 = df1.filter(        
            (F.col("FirstMonth") <= end_month_id) &        
            ((F.col("LastMonth") >= end_month_id) | (F.col("LastMonth").isNull()))
          )      
          df1 = df1.select(*cols).distinct() 
        except:
          df1 = spark.sql(f"select * from {db_output}.{table}").select(*cols).distinct()
        lookup_cols += [df1.columns]
        
        if "Provider; " not in breakdown["breakdown_name"] and breakdown["breakdown_name"] != "Provider":
          level_lists += [[[x[0], x[1]] for x in df1.collect()]  + [["UNKNOWN", "UNKNOWN"]]]
        else: 
          level_lists += [[[x[0], x[1]] for x in df1.collect()]]
        
      if type(table) == dict:
        if ix == 0: cols = ["primary_level", "primary_level_desc"]
        if ix == 1: cols = ["secondary_level", "secondary_level_desc"]
        lookup_cols += [cols]
        listC = [[x[0],x[1]] if len(x) == 2 else [x[0], x[0]] for x in  table["level_list"]]
        level_lists += [listC]
        
    len1 = len(level_lists) 
    listLL = [[breakdown_name] + x0 + x1 for x0 in level_lists[0] for x1 in level_lists[1]]    
    listU = ["breakdown", "primary_level", "primary_level_desc", "secondary_level", "secondary_level_desc"]
    schema = ", ".join(f"{col} string" for col in listU)
    df1 = spark.createDataFrame(listLL, schema = schema)
    
  # put it all together
  bd_name = get_var_name(breakdown)
  df1 = df1.select(
    F.lit(freq).alias("freq"),
    F.lit(bd_name).alias("bd_name"), 
    "*"
  )
  df1 = df1.distinct()
  try:
    return df1
  except:
    print("failed to create df")