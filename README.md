# Mental Health Services Monthly Statistics

**_Warning - this repository is a snapshot of a repository internal to NHS England. This means that links to videos and some URLs may not work._**

**Repository owner:** Analytical Services: Community and Mental Health

**Email:** england.mentalhealthanalysis@nhs.net

To contact us raise an issue on Github or via email and we will respond promptly.

## Introduction

This codebase is used in the creation of the Mental Health Services Monthly Statistics publication. There are three separate pipelines (menh_analysis, Menh_publications and mental_health_bbrb) which contribute to the production of the Mental Health Monthly Statistics. These are all included in the repository. This publication uses the Mental Health Services Dataset (MHSDS), further information about the dataset can be found at https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set.

The full publication series can be found at https://digital.nhs.uk/data-and-information/publications/statistical/mental-health-services-monthly-statistics.

Other associated metadata which includes pseudo code and descriptions and a full list of the measures produced by NHS Digital across the mental health publications can be found at https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/statistics-and-reports.

Other Mental Health related publications and dashboards can be found at the Mental Health Data Hub: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub 

## Folder structure

The repository is structured with three main folders. These are as follows: 

- The menh_analysis pipeline contains most high level measures such as MHS01, People in contact with mental health services. 

- Menh_publications contains metrics such as EIP, 72 hour follow ups and numbers of detentions under the Mental Health Act. 

- The menh_bbrb code contains newer metrics such as Children and Young Peoples Access, Perinatal Access and Children and Young Peoples Outcomes. This folder also contains the methodology used to create the OAPS data.
  
- The menh_dq code contains the code used to create the coverage and VODIM files that form part of the publication.


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
If you have any questions or issues regarding the constructions or code within this repository please contact england.mentalhealthanalysis@nhs.net

## Authors and acknowledgment
Community and Mental Health Team, NHS England
mh.analysis@nhs.net

## License
The menh_bbrb codebase is released under the MIT License.
The documentation is © Crown copyright and available under the terms of the [Open Government 3.0] (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.

