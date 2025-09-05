# Databricks notebook source
provider_parent_breakdowns = ["Provider"]
oaps_provider_parent_breakdowns = ["Sending Provider", "Receiving Provider"]
multiple_geogs = ["CCG of Residence; Provider", "Sending Provider; Receiving Provider", "CCG of Residence; Receiving Provider", "CCG of GP Practice or Residence; Receiving Provider", "STP of Residence; Receiving Provider", "STP of GP Practice or Residence; Receiving Provider", "Commissioning Region; Receiving Provider"]

# COMMAND ----------

unsup_breakdowns = ['England',
                    'England; Age Band',
                    'England; Bed Type',
                    'England; Ethnicity',
                    'England; Lower Ethnicity',
                    'England; Upper Ethnicity',
                    'England; Gender',
                    'England; Primary reason for referral',
                    'England; Sexual Orientation',
                    'England; Ethnicity (White British/Non-White British)', 
                    'England; IMD Decile',
                    'England; Service or Team Type Referred To',
                    'England; Age; Under 18 or 18 and over',
                    'England; Autism Status',
                    'England; LD Status',
                    'England; IMD Quintile',
                    'England; IMD Core20',
                    'England; Mean Deviation of Upper Ethnicity',
                    'England; Ethnicity - Standardised Rate Difference from White British',
                    'England; IMD - Standardised Rate Difference from Most Deprived Quintile'
                   ]

# COMMAND ----------

output_columns = ['REPORTING_PERIOD_START',
                  'REPORTING_PERIOD_END',
                  'STATUS',
                  'BREAKDOWN',
                  'PRIMARY_LEVEL',
                  'PRIMARY_LEVEL_DESCRIPTION',
                  'SECONDARY_LEVEL',
                  'SECONDARY_LEVEL_DESCRIPTION',
                  'MEASURE_ID',
                  'MEASURE_NAME',
                  'MEASURE_VALUE',
                  'SOURCE_DB']

# COMMAND ----------

csv_lookup_columns = ['REPORTING_PERIOD_START',
                  'REPORTING_PERIOD_END',
                  'STATUS',
                  'BREAKDOWN',
                  'PRIMARY_LEVEL',
                  'PRIMARY_LEVEL_DESCRIPTION',
                  'SECONDARY_LEVEL',
                  'SECONDARY_LEVEL_DESCRIPTION',
                  'MEASURE_ID',
                  'MEASURE_NAME',
                  'SOURCE_DB']