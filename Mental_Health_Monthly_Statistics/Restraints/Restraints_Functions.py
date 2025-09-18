# Databricks notebook source
from pyspark.sql import types as T
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta

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

def last_day(date: str) -> str:
  """  This function gets the first day of the month
  for any given date
  
  Example:
  get_last_day("2021-01-15")
  >> "2021-01-31"
  """
  date_dt = str2dt(date)
  last_day_var = calendar.monthrange(date_dt.year, date_dt.month)
  last_day_dt = date_dt.replace(day=last_day_var[1]) ##last_day_var is a tuple of first and last day (1, 28/29/30/31) get last day by indexing
  last_day = dt2str(last_day_dt)
  
  return last_day

def add_months(date: str, rp_length_num: int) -> T.DateType():
  """ This functions adds required amount of 
  months from a given date  
  
  Example:
  add_months("2021-01-15", 3)
  >> "2021-04-15"
  """
  month_dt = str2dt(date)
  new_month_dt = month_dt + relativedelta(months=rp_length_num) #plus rp_length_num months
  new_month = dt2str(new_month_dt)
  
  return new_month

# COMMAND ----------

def get_restr_db_from_status(status: str) -> str:
  if status == "Final":
    db_source = "mhsds_database"
  else:
    db_source = "mhsds_database"
    
  return db_source  