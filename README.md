# Mental Health Services Monthly Statistics

**_Warning - this repository is a snapshot of a repository internal to NHS England. This means that links to videos and some URLs may not work._**

**Repository owner:** Analytical Services: Community and Mental Health

**Email:** mh.analysis@nhs.net

To contact us raise an issue on Github or via email and we will respond promptly.

## Introduction

This codebase is used in the creation of the Mental Health Services Monthly Statistics publication. There are three separate pipelines (menh_analysis, Menh_publications and mental_health_bbrb) which contribute to the production of the Mental Health Monthly Statistics. These are all included in the repository. This publication uses the Mental Health Services Dataset (MHSDS), further information about the dataset can be found at https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set.

The full publication series can be found at https://digital.nhs.uk/data-and-information/publications/statistical/mental-health-services-monthly-statistics.

Other associated metadata which includes pseudo code and descriptions and a full list of the measures produced by NHS Digital across the mental health publications can be found at https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/statistics-and-reports.

Other Mental Health related publications and dashboards can be found at the Mental Health Data Hub: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub 

## Folder structure

The repository is structured as follows:

```bash
│   README.md
│
└───Mental_Health_Monthly_Statistics
    ├───menh_analysis_clean
    │   │   init_schemas.py
    │   │   run_notebooks.py
    │   │
    │   ├───notebooks
    │   │   │   run_notebooks_master.py
    │   │   │
    │   │   ├───00_Master
    │   │   │       1.Run_Main_monthly.py
    │   │   │       2.Run_AWT.py
    │   │   │       3.Run_CYP_2nd_contact.py
    │   │   │       4.Run_CAP.py
    │   │   │       5.Run_CYP_monthly.py
    │   │   │       6.Run_LDA.py
    │   │   │       7.Run_Ascof.py
    │   │   │       8.Run_FYFV.py
    │   │   │       9.Run_CCGOIS.py
    │   │   │
    │   │   ├───01_Prepare
    │   │   │       0.5_VersionChanges.py
    │   │   │       0.Generic_prep.py
    │   │   │       0.Insert_lookup_data.py
    │   │   │       1.Main_monthly_prep.py
    │   │   │       2.AWT_prep.py
    │   │   │       3.CYP_2nd_contact_prep.py
    │   │   │       4.CAP_prep.py
    │   │   │       5.CYP_monthly_prep.py
    │   │   │       6.LDA_Prep.py
    │   │   │       8.FYFV_prep.sql
    │   │   │       prepare_all_products.py
    │   │   │
    │   │   ├───02_Aggregate
    │   │   │   │   2.AWT_agg.sql
    │   │   │   │   2.AWT_agg_ethnicity.sql
    │   │   │   │   2.AWT_agg_not_ethnicity.sql
    │   │   │   │   3.CYP_2nd_contact_agg.py
    │   │   │   │   4.CAP_agg.py
    │   │   │   │   6.LDA.py
    │   │   │   │   7.Ascof_agg.py
    │   │   │   │   8.FYFV_agg.sql
    │   │   │   │   90.Pre_April_2021_Measures.py
    │   │   │   │   91.List_possible_metrics.py
    │   │   │   │   92.Expand_output.py
    │   │   │   │   93.Clean_formatted_tables.py
    │   │   │   │   94.1.Cache_output_CCG_fix_Apr_2020.py
    │   │   │   │   94.Cache_output.py
    │   │   │   │   95.Round_output.py
    │   │   │   │   96.Update_raw_outputs_ICB.py
    │   │   │   │
    │   │   │   ├───1.Main_monthly_agg
    │   │   │   │   ├───1.National
    │   │   │   │   │       Inpatient measures.py
    │   │   │   │   │       MHA measures.py
    │   │   │   │   │       MHS07_Outpatient.py
    │   │   │   │   │       Outpatient-Other measures.py
    │   │   │   │   │
    │   │   │   │   ├───2.CCG
    │   │   │   │   │       Inpatient measures.py
    │   │   │   │   │       MHA measures.py
    │   │   │   │   │       Outpatient-Other measures.py
    │   │   │   │   │
    │   │   │   │   ├───3.Provider
    │   │   │   │   │       Inpatient measures.py
    │   │   │   │   │       MHA measures.py
    │   │   │   │   │       Outpatient-Other measures.py
    │   │   │   │   │
    │   │   │   │   └───4.LA-CASSR
    │   │   │   │           Inpatient measures.py
    │   │   │   │           Outpatient-Other measures.py
    │   │   │   │
    │   │   │   └───5.CYP_monthly_agg
    │   │   │       ├───1.National
    │   │   │       │       Inpatient measures.py
    │   │   │       │       Outpatient Other_v5.py
    │   │   │       │
    │   │   │       ├───2.CCG
    │   │   │       │       Inpatient measures.py
    │   │   │       │       Outpatient Other_v5.py
    │   │   │       │
    │   │   │       └───3.Provider
    │   │   │               Inpatient measures.py
    │   │   │               Outpatient Other_v5.py
    │   │   │
    │   │   └───03_Extract
    │   │       │   01_menh_analysis_csvs_perf_prov.sql
    │   │       │   02_menh_analysis_csvs_perf.sql
    │   │       │   03_LDA_Monthly_Outputs.py
    │   │       │   04_menh_analysis_csvs_quarter.sql
    │   │       │
    │   │       └───001_Monthly_Code
    │   │               LDA_child_non-respite_v4.1.py
    │   │               LDA_child_non-respite_v5.py
    │   │               LDA_child_v4.1.py
    │   │               LDA_child_v5.py
    │   │
    │   └───schemas
    │       │   schemas_master.py
    │       │
    │       ├───00_DB
    │       │       db_upgrade_1.sql
    │       │       db_upgrade_10.py
    │       │       db_upgrade_11.py
    │       │       db_upgrade_11a.py
    │       │       db_upgrade_12.py
    │       │       db_upgrade_3.sql
    │       │       db_upgrade_3a.py
    │       │       db_upgrade_3b.py
    │       │       db_upgrade_4.sql
    │       │       db_upgrade_5.sql
    │       │       db_upgrade_6.py
    │       │       db_upgrade_7.sql
    │       │       db_upgrade_8.py
    │       │       db_upgrade_9.py
    │       │
    │       └───99_update_testdata_db
    │               00_add_ICB_derivations.py
    │
    ├───menh_publications
    │       │   init_schemas.py
    │       │   run_notebooks.py
    │       │
    │       ├───notebooks
    │       │   │   91_List_possible_metrics.py
    │       │   │   92_Expand_output.py
    │       │   │   93_Cache_output.py
    │       │   │   94_Round_output.py
    │       │   │   96.Update_raw_outputs_ICB.py
    │       │   │   run_notebooks_master.py
    │       │   │
    │       │   ├───00_common_objects
    │       │   │       00_version_change_tables.py
    │       │   │       01_prep_common_objects.py
    │       │   │       02_load_common_ref_data.py
    │       │   │
    │       │   ├───02_72HOURS
    │       │   │   ├───00_Master
    │       │   │   │       DeletePrevRunRecords.py
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       PrepViews.py
    │       │   │   │
    │       │   │   └───02_Aggregate
    │       │   │           72hours_Aggregate.py
    │       │   │
    │       │   ├───03_RestrictiveInterventions
    │       │   │   ├───00_Master
    │       │   │   │       DeleteFromTarget.py
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       DeriveRaw.py
    │       │   │   │
    │       │   │   ├───02_Aggregate
    │       │   │   │   ├───1.National
    │       │   │   │   │       CountBreakdown.py
    │       │   │   │   │       CountTotal.py
    │       │   │   │   │       PeopleBreakdown.py
    │       │   │   │   │       PeopleTotal.py
    │       │   │   │   │
    │       │   │   │   ├───2.Provider
    │       │   │   │   │       CountBreakdown.py
    │       │   │   │   │       PeopleBreakdown.py
    │       │   │   │   │
    │       │   │   │   ├───3.ProviderType
    │       │   │   │   │       CountBreakdown.py
    │       │   │   │   │       PeopleBreakdown.py
    │       │   │   │   │
    │       │   │   │   └───4.Suppress
    │       │   │   │           CountSuppress.py
    │       │   │   │           PeopleSuppress.py
    │       │   │   │
    │       │   │   └───03_Extract
    │       │   │           CountExtract.py
    │       │   │           PeopleExtract.py
    │       │   │
    │       │   ├───04_CYP_ED_WaitingTimes
    │       │   │   ├───00_Master
    │       │   │   │       DeletePrevRunRecords.sql
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       LoadRefData.sql
    │       │   │   │       PrepViews.sql
    │       │   │   │
    │       │   │   └───02_Aggregate
    │       │   │           01_CYP_ED_WT_Aggregate.sql
    │       │   │
    │       │   ├───05_MHA_Monthly
    │       │   │   ├───00_Master
    │       │   │   │       DeletePrevRunRecords.sql
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       00_LoadRefData.sql
    │       │   │   │       01_CreateViews.sql
    │       │   │   │       02_PrepareMonthlyData.sql
    │       │   │   │
    │       │   │   └───02_Aggregate
    │       │   │           01_MHS81.sql
    │       │   │           02_MHS82.sql
    │       │   │           03_MHS83.sql
    │       │   │           04_MHS84.sql
    │       │   │
    │       │   ├───06_CYP_Outcome_Measures
    │       │   │   ├───00_Master
    │       │   │   │       DeletePrevRunRecords.sql
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       LoadRefData.sql
    │       │   │   │       PrepViews.sql
    │       │   │   │
    │       │   │   └───02_Aggregate
    │       │   │           MHS91.sql
    │       │   │           MHS92-94.sql
    │       │   │           MHS95.sql
    │       │   │
    │       │   ├───07_EIP
    │       │   │   ├───00_Master
    │       │   │   │       DeletePrevRunRecords.py
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       01_EIP_prep.py
    │       │   │   │       02_Insert_lookup_data.py
    │       │   │   │
    │       │   │   └───02_Aggregate
    │       │   │           01_EIP_Aggregate_ethnicity.sql
    │       │   │           01_EIP_Aggregate_non_ethnicity.sql
    │       │   │           02_EIP_68-69b.py
    │       │   │
    │       │   ├───08_ASCOF
    │       │   │   ├───00_Master
    │       │   │   │       DeletePrevRunRecords.py
    │       │   │   │
    │       │   │   ├───01_Prepare
    │       │   │   │       00_LoadRefData.sql
    │       │   │   │       01_ASCOF_prep.sql
    │       │   │   │
    │       │   │   └───02_Aggregate
    │       │   │           01_ASCOF_agg.sql
    │       │   │
    │       │   └───99_Extract
    │       │           menh_publications_perf.sql
    │       │           menh_publications_perf_prov.sql
    │       │
    │       └───schemas
    │           │   metadata.py
    │           │   schema_master.py
    │           │
    │           ├───00_create_common_objects
    │           │       01_create_common_tables.py
    │           │       5.0_create_version_change_tables.py
    │           │       99_alter_existing_tables.py
    │           │
    │           ├───02_72HOURS
    │           │       create_tables.py
    │           │
    │           ├───03_RestrictiveInterventions
    │           │       1_create_tables.py
    │           │       2_create_views.py
    │           │       3_load_ref_data.py
    │           │
    │           ├───04_CYP_ED_WaitingTimes
    │           │       create_tables.sql
    │           │
    │           ├───05_MHA_Monthly
    │           │       1_create_tables.sql
    │           │
    │           ├───06_CYP_Outcome_Measures
    │           │       create_tables.sql
    │           │
    │           ├───07_EIP
    │           │       create_tables.py
    │           │
    │           └───08_ASCOF
    │                   create_tables.py
    │
    └───mental_health_bbrb_clean
        ├───MHSDS_OAPs
        │       MHSDS OAPS - 0 MASTER.py
        │       MHSDS OAPS - 1 Create Tables.py
        │       MHSDS OAPS - 2 Generic Prep.py
        │       MHSDS OAPS - 3 Create Measures.py
        │       MHSDS OAPS - 3a OAP01.py
        │       MHSDS OAPS - 3b OAP02.py
        │       MHSDS OAPS - 3c OAP03.py
        │       MHSDS OAPS - 3d OAP04.py
        │       MHSDS OAPS - 90 Outputs.py
        │
        └───MHSDS_V5_BBRB
            │   Adult Acute LOS - rolling quarter (v5).sql
            │
            ├───Additional Breakdowns
            │       MHSDS Monthly - Additional Breakdowns - Generic Prep.py
            │       MHSDS Monthly - Additional Breakdowns - MASTER.py
            │       MHSDS Monthly - Additional Breakdowns - MHS01.py
            │       MHSDS Monthly - Additional Breakdowns - MHS07.py
            │       MHSDS Monthly - Additional Breakdowns - MHS23.py
            │       MHSDS Monthly - Additional Breakdowns - MHS27.sql
            │       MHSDS Monthly - Additional Breakdowns - MHS29.py
            │       MHSDS Monthly - Additional Breakdowns - MHS30.py
            │       MHSDS Monthly - Additional Breakdowns - MHS32.py
            │       MHSDS Monthly - Additional Breakdowns - MHS57.py
            │       MHSDS Monthly - Additional Breakdowns - Outputs.py
            │       MHSDS Monthly - Additional Breakdowns - Ref Data Load.py
            │       MHSDS Monthly - Additional Breakdowns - Single Run.py
            │
            ├───CMH_v5
            │   └───CMH NHS D Code
            │       │   CMH_Master.py
            │       │
            │       ├───CMH_Agg
            │       │       CMH_Access_Agg.py
            │       │       CMH_Admissions_Agg.py
            │       │
            │       └───CMH_Prep
            │               Acute_Admissions_with_or_without_contact.py
            │               CMH Access Reduced.py
            │               Create_STP_Region_Mapping.py
            │               NHSE_Pre_Processing_Tables.py
            │
            ├───CYP and Perinatal monthly V5
            │       AdultsMHS109.sql
            │       CYP Access v2.sql
            │       CYP Outcomes.sql
            │       CYP Perinatal - Master.py
            │       CYP_Perinatal_ref.sql
            │       CYP_Perinatal_tables.sql
            │       Perinatal final.sql
            │
            ├───EIP_CaseloadSNoMED
            │       Create_SNoMED_FSN_Ref.sql
            │       EIP_CaseloadSNoMED_FINAL.py
            │
            ├───New measures from April 23
            │   │   Agg_Functions.py
            │   │   Master.py
            │   │   NHSE_Pre_Processing_Tables.py
            │   │   Reference_Tables.py
            │   │
            │   └───Prep
            │           CYPOutcomes_Prep.py
            │           IPS_Prep.py
            │           UEC_Prep.py
            │
            ├───Perinatal Qtr V5
            │   └───21_22 version V5
            │           Perinatal_Master_v2.sql
            │           Perinatal_output.sql
            │           Perinatal_Prep_v4.sql
            │           Perinatal_Table.sql
            │           Table_1_measures_and_breakdowns.sql
            │           Table_2_measures_and_breakdowns.sql
            │           Table_3_measures_and_breakdowns.sql
            │           Table_4_measures_and_breakdowns.sql
            │
            └───Restraints_v2_FINAL_v5
                │   Create_Restraints_Tables.py
                │   Restraints_Prep.py
                │   Run_Restraints_Process.py
                │
                ├───Restraints_Published_CSV_Build
                │       Average_Minutes_of_Restraint.py
                │       Bed_Days_Demographics_Agg.py
                │       Bed_Days_Duration_Agg.py
                │       Bed_Days_Provider_Agg.py
                │       Bed_Days_Summary_Agg.py
                │       Maximum_Minutes_of_Restraint.py
                │       Number_of_People_Demographics_Agg.py
                │       Number_of_People_Duration_Agg.py
                │       Number_of_People_Provider_Agg.py
                │       Number_of_People_Summary_Agg.py
                │       Number_of_Restraints_Demographics_Agg.py
                │       Number_of_Restraints_Duration_Agg.py
                │       Number_of_Restraints_Provider_Agg.py
                │       Number_of_Restraints_Summary_Agg.py
                │       Perc_RIPeople_in_Hosp.py
                │       Restr_per_1000BD_Demographics_Agg.py
                │       Restr_per_1000BD_Duration_Agg.py
                │       Restr_per_1000BD_Provider_Agg.py
                │       Restr_per_1000BD_Summary_Agg.py
                │
                └───Restraints_Suppression
                        Published_CSV_Suppression.py
```

The menh_analysis pipeline contains most high level measures such as MHS01, People in contact with mental health services. Menh_publications contains metrics such as EIP, 72 hour follow ups and numbers of detentions under the Mental Health Act. The mental_health_bbrb code contains newer metrics such as Children and Young Peoples Access, Perinatal Access and Children and Young Peoples Outcomes.


## Installation and running
Please note that the code included in this project is designed to be run on Databricks within the NHS England systems. As such some of the code included here may not run on other MHSDS assets. The logic and methods used to produce the metrics included in this codebase remain the same though. 

## Understanding the Mental Health Services Dataset

MHSDS is collected on a monthly basis from providers of secondary mental health services. On average around 210 million rows of data flow into the dataset on a monthly basis. More information on the data quality of the dataset, including the numbers of providers submitting data and the volumes of data flowing to each table can be found in the Data Quality Dashboard: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub/data-quality/mental-health-services-dataset---data-quality-dashboard 

The MHSDS tables and fields used within the code are all documented within the MHSDS tools and guidance. This guidance can be found here: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance

Within the guidance are three key documents:

1) MHSDS Technical Output Specification - This provides technical details of all of the fields and tables contained within the dataset. It also contains details of the validations applied to specific tables and fields. The specification also includes details of derivations and how they are constructed.
2) MHSDS Data Model - This details all of the tables and fields within the dataset and how they relate to each other.
3) MHSDS User Guidance - This document provides details of all of the tables and fields within the dataset and gives examples of how a user might populate the data in certain scenarios.

Additionally, users might want to consult the Data Dictionary for specific fields within the dataset: https://www.datadictionary.nhs.uk/ 

## Appendix and Notes

In places the notebooks above use some acronyms. The main ones used are as follows:

- MHSDS: Mental Health Services Dataset
- CCG: Clinical Commissioning Group. These were replaced by Sub Integrated Care Boards (ICBs) in July 2022.
- ICB: Integrated Care Board. These came into effect on July 1st 2022. Further information can be found at https://www.kingsfund.org.uk/publications/integrated-care-systems-explained#development.
- Provider: The organisation is providing care. This is also the submitter of MHSDS data
-LA: Local Authority


## Support
If you have any questions or issues regarding the constructions or code within this repository please contact mh.analysis@nhs.net

## Authors and acknowledgment
Community and Mental Health Team, NHS England
mh.analysis@nhs.net

## License
The menh_bbrb codebase is released under the MIT License.
The documentation is © Crown copyright and available under the terms of the [Open Government 3.0] (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.

