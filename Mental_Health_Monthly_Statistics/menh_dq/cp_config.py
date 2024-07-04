# Databricks notebook source
 %md
 # Code Promotion Job Configuration
 ### This notebook sets some parameters for Databricks jobs wrapping the top-level entry point notebooks.
 Only simple setting of variables is allowed in this notebook.

# COMMAND ----------

# DBTITLE 1,Global settings
 %md
 spark_version can be either "6.6.x-scala2.11" (spark 2) or "9.1.x-scala2.12" (spark 3).   
 This applies to all jobs created

# COMMAND ----------

# spark_version = "6.6.x-scala2.11"
spark_version = "9.1.x-scala2.12"


# COMMAND ----------

# DBTITLE 1,init_schemas
 %md
 Currently, no parameter can be set for the job wrapping the *init_schemas* notebook.

# COMMAND ----------

# DBTITLE 1,run_notebooks
 %md
 Available parameters:
  - **concurrency**: Integer between 1 and 10. Allows you to run multiple *run_notebooks* jobs at the same time. 
  - **extra_parameters**: Dictionary(String, String) that maps *parameter names* to *default values*. These parameters are added to the list of parameters for the job.

# COMMAND ----------

# Example:
# run_notebooks = {
#   "concurrency": 5,
#   "extra_parameters": {
#     "month_id": "",
#     "end_of_year_report": "no",
#   },
# }
# schedule updated to run DAILY at 23:25 (advice is not to schedule jobs to start on the hour or half hour to reduce risk of resource contention)
run_notebooks = {
  "concurrency": 1,
  "extra_parameters": {}
,"schedule": "0 25 23 1/1 * ? *"
}

# COMMAND ----------

# DBTITLE 1,tool_config
 %md
 Currently, no parameter can be set for the job wrapping the *tool_config* notebook.