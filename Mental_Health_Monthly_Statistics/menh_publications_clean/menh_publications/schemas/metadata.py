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
          
            ('CYP_ED_WaitingTimes', 'init_schemas', '04_CYP_ED_WaitingTimes/create_tables', 401, 'create_schema', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04_CYP_ED_WaitingTimes/00_Master/DeletePrevRunRecords', 410, 'transform', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04_CYP_ED_WaitingTimes/01_Prepare/LoadRefData', 411, 'transform', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04_CYP_ED_WaitingTimes/01_Prepare/PrepViews', 412, 'transform', null),
            ('CYP_ED_WaitingTimes', 'run_notebooks', '04_CYP_ED_WaitingTimes/02_Aggregate/01_CYP_ED_WT_Aggregate', 413, 'transform', null),
            
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
           

# COMMAND ----------

%sql
select * from $db_output.metadata