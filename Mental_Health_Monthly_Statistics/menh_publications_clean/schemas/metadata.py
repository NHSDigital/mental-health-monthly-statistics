# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
update_metadata = dbutils.widgets.get("update_metadata")
assert db_output
print("db_output", db_output)
print("update_metadata", update_metadata)

# COMMAND ----------

# DBTITLE 1,create metadata table if not exist
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.metadata(
   
   product string,
   module string,
   notebook_path string,
   seq int,
   stage string,
   restart_root string
 )
 using DELTA;

# COMMAND ----------

# DBTITLE 1,insert data 
if update_metadata == 'true':
  print("updating metadata")
  spark.sql(f"DELETE FROM {db_output}.metadata")
  spark.sql(
    f"""
            INSERT INTO {db_output}.metadata VALUES 
            ('72HOURS', 'init_schemas', '02_72HOURS/create_tables', 201, 'create_schema', null),
            ('72HOURS', 'run_notebooks', '02_72HOURS/00_Master/DeletePrevRunRecords', 210, 'transform', null),
            ('72HOURS', 'run_notebooks', '02_72HOURS/01_Prepare/PrepViews', 211, 'transform', null),
            ('72HOURS', 'run_notebooks', '02_72HOURS/02_Aggregate/72hours_Aggregate', 212, 'transform', null),
            
            ('RestrictiveInterventions', 'init_schemas', '03_RestrictiveInterventions/1_create_tables', 301, 'create_schema', null),
            ('RestrictiveInterventions', 'init_schemas', '03_RestrictiveInterventions/2_create_views', 302, 'create_schema', null),
            ('RestrictiveInterventions', 'init_schemas', '03_RestrictiveInterventions/3_load_ref_data', 303, 'create_schema', null),
            ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/00_Master/DeleteFromTarget', 304, 'transform', null),
          
            ('CYP_ED_WaitingTimes_preFY2324', 'init_schemas', '04_CYP_ED_WaitingTimes/create_tables', 401, 'create_schema', null),
            ('CYP_ED_WaitingTimes_preFY2324', 'run_notebooks', '04_CYP_ED_WaitingTimes/00_Master/DeletePrevRunRecords', 410, 'transform', null),
            ('CYP_ED_WaitingTimes_preFY2324', 'run_notebooks', '04_CYP_ED_WaitingTimes/01_Prepare/LoadRefData', 411, 'transform', null),
            ('CYP_ED_WaitingTimes_preFY2324', 'run_notebooks', '04_CYP_ED_WaitingTimes/01_Prepare/PrepViews', 412, 'transform', null),
            ('CYP_ED_WaitingTimes_preFY2324', 'run_notebooks', '04_CYP_ED_WaitingTimes/02_Aggregate/01_CYP_ED_WT_Aggregate', 413, 'transform', null),
          
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04a_CYP_ED_WaitingTimes/00_Master/DeletePrevRunRecords', 460, 'transform', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04a_CYP_ED_WaitingTimes/01_Prepare/LoadRefData', 461, 'transform', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04a_CYP_ED_WaitingTimes/01_Prepare/PrepViews', 462, 'transform', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04a_CYP_ED_WaitingTimes/02_Aggregate/01_CYP_ED_WT_Aggregate', 463, 'transform', null),
            ('CYP_ED_WaitingTimes_12m', 'run_notebooks', '04a_CYP_ED_WaitingTimes/01_Prepare/LoadRefData', 464, 'transform', null),
            ('CYP_ED_WaitingTimes_12m', 'run_notebooks', '04a_CYP_ED_WaitingTimes/01_Prepare/PrepViews', 465, 'transform', null),
            ('CYP_ED_WaitingTimes_12m', 'run_notebooks', '04a_CYP_ED_WaitingTimes/02_Aggregate/01_CYP_ED_WT_Aggregate', 466, 'transform', null),
            
            ('MHA_Monthly', 'init_schemas', '05_MHA_Monthly/1_create_tables', 501, 'create_schema', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/00_Master/DeletePrevRunRecords', 502, 'transform', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/01_Prepare/00_LoadRefData', 503, 'transform', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/01_Prepare/01_CreateViews', 504, 'transform', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/02_Aggregate/01_MHS81', 505, 'transform', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/02_Aggregate/02_MHS82', 506, 'transform', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/02_Aggregate/03_MHS83', 507, 'transform', null),
            ('MHA_Monthly', 'run_notebooks', '05_MHA_Monthly/02_Aggregate/04_MHS84', 508, 'transform', null),
            
            ('CYP_Outcome_Measures', 'init_schemas', '06_CYP_Outcome_Measures/create_tables', 601, 'create_schema', null),
            
            ('EIP', 'init_schemas', '07_EIP/create_tables', 701, 'create_schema', null),
            ('EIP', 'run_notebooks', '07_EIP/00_Master/DeletePrevRunRecords', 702, 'transform', null),
            ('EIP', 'run_notebooks', '07_EIP/01_Prepare/01_EIP_prep', 703, 'transform', null),
            ('EIP', 'run_notebooks', '07_EIP/01_Prepare/02_Insert_lookup_data', 704, 'transform', null),
            ('EIP', 'run_notebooks', '07_EIP/02_Aggregate/01_EIP_Aggregate_ethnicity', 705, 'transform', null),
            ('EIP', 'run_notebooks', '07_EIP/02_Aggregate/01_EIP_Aggregate_non_ethnicity', 706, 'transform', null),
            ('EIP', 'run_notebooks', '07_EIP/02_Aggregate/02_EIP_68-69b', 707, 'transform', null),
            
            ('ASCOF', 'init_schemas', '08_ASCOF/create_tables', 801, 'create_schema', null),
            ('ASCOF', 'run_notebooks', '08_ASCOF/00_Master/DeletePrevRunRecords', 802, 'transform', null),
            ('ASCOF', 'run_notebooks', '08_ASCOF/01_Prepare/00_LoadRefData', 803, 'transform', null),
            ('ASCOF', 'run_notebooks', '08_ASCOF/01_Prepare/01_ASCOF_prep', 804, 'transform', null),
            ('ASCOF', 'run_notebooks', '08_ASCOF/02_Aggregate/01_ASCOF_agg', 805, 'transform', null)
     """
  )
         
  print("metadata upload complete")
  

          
  # RestrictiveInterventions measures temporarily suspended in this codebase (until redevelopment for MHSDS v5.0 data) - October 2021
#   ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/01_Prepare/DeriveRaw', 305, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/1.National/PeopleTotal', 306, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/1.National/PeopleBreakdown', 307, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/2.Provider/PeopleBreakdown', 308, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/3.ProviderType/PeopleBreakdown', 309, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/4.Suppress/PeopleSuppress', 310, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/1.National/CountTotal', 311, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/1.National/CountBreakdown', 312, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/2.Provider/CountBreakdown', 313, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/3.ProviderType/CountBreakdown', 314, 'transform', null),
#             ('RestrictiveInterventions', 'run_notebooks', '03_RestrictiveInterventions/02_Aggregate/4.Suppress/CountSuppress', 315, 'transform', null),

# CYP_Outcome_Measures excluded from run - these have never fully matched the analyst produced measures, haven't been updated for v5 and are causing delays in ICB updates
# these measures are being run by Babbage as part of BBRB and are being integrated as part of menh_bbrb (due to a lack of communication and awareness) 
# it makes sense to get these measures right in a single place, menh_bbrb is likely to be this place BUT these notebooks still exist here just in case.

#             ('CYP_Outcome_Measures', 'run_notebooks', '06_CYP_Outcome_Measures/00_Master/DeletePrevRunRecords', 602, 'transform', null),
#             ('CYP_Outcome_Measures', 'run_notebooks', '06_CYP_Outcome_Measures/01_Prepare/LoadRefData', 603, 'transform', null),
#             ('CYP_Outcome_Measures', 'run_notebooks', '06_CYP_Outcome_Measures/01_Prepare/PrepViews', 604, 'transform', null),
#             ('CYP_Outcome_Measures', 'run_notebooks', '06_CYP_Outcome_Measures/02_Aggregate/MHS92-94', 605, 'transform', null),
#             ('CYP_Outcome_Measures', 'run_notebooks', '06_CYP_Outcome_Measures/02_Aggregate/MHS91', 606, 'transform', null),
#             ('CYP_Outcome_Measures', 'run_notebooks', '06_CYP_Outcome_Measures/02_Aggregate/MHS95', 607, 'transform', null),
            

# COMMAND ----------

 %sql
 select * from $db_output.metadata order by seq