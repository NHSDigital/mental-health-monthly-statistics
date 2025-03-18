# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.cyp_outcomes_ass;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_outcomes_ass 
 (
 Assessment_Tool_Name STRING,
 Active_Concept_ID_SNOMED STRING,
 Preferred_Term_SNOMED STRING,
 SNOMED_Version STRING,
 Der_Rater STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.rcads_transform_score;
 CREATE TABLE IF NOT EXISTS $db_output.rcads_transform_score 
 (
 Gender STRING,
 Age STRING,
 AssName STRING,
 Sample_Mean DECIMAL(20,2),
 Sample_SD DECIMAL(20,2)
 ) USING DELTA

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.rcads_transform_score
 VALUES
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.25, 4.09),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.25, 4.09),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.98, 3.36),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.98, 3.36),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.15, 3.2),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.15, 3.2),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.15),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.15),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.87, 3.93),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.87, 3.93),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 9.77, 4.51),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 9.77, 4.51),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.74, 4.75),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.74, 4.75),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.77, 3.77),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.77, 3.77),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 7.62, 3.68),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 7.62, 3.68),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 6.51, 4.73),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 6.51, 4.73),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 7.05, 4.31),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 7.05, 4.31),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.61, 4.98),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.61, 4.98),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.07, 3.64),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.07, 3.64),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.44, 3.13),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.44, 3.13),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.01, 3.26),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.01, 3.26),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.06, 3.6),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.06, 3.6),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.2, 3.05),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.2, 3.05),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.3, 4.75),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.3, 4.75),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.64, 4.1),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.64, 4.1),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.01, 3.68),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.01, 3.68),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.39, 3.46),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.39, 3.46),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.3),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.3),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.74, 3.78),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.74, 3.78),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.92, 5.21),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.92, 5.21),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 6.71, 3.64),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 6.71, 3.64),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.2, 3.14),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.2, 3.14),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.22, 3.4),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.22, 3.4),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.62, 3.36),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.62, 3.36),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.26, 2.47),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.26, 2.47),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.05, 4.74),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.05, 4.74),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.89, 3.91),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.89, 3.91),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.42, 3.16),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.42, 3.16),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.12, 3.34),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.12, 3.34),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.03, 3.92),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.03, 3.92),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3, 2.72),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3, 2.72),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 13.01, 4.94),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 13.01, 4.94),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.44, 4.1),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.44, 4.1),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.07, 2.93),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.07, 2.93),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.65, 2.89),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.65, 2.89),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.76, 3.21),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.76, 3.21),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.5, 2.46),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.5, 2.46),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.68, 4.74),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.68, 4.74),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.65, 3.68),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.65, 3.68),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.28, 3.44),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.28, 3.44),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.12, 2.79),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.12, 2.79),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.18, 3.07),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.18, 3.07),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.34, 2.23),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.34, 2.23),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.27, 5),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.27, 5),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.32, 3.81),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.32, 3.81),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.76, 3.44),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.76, 3.44),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.18, 3.12),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.18, 3.12),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.79, 2.71),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.79, 2.71),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 1.9, 2.03),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 1.9, 2.03),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.67, 4.49),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.67, 4.49),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 9.36, 4.45),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 9.36, 4.45),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.49, 3.71),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.49, 3.71),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.48, 3.82),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.48, 3.82),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.26, 4.28),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.26, 4.28),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.05, 2.57),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.05, 2.57),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.85, 4.98),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.85, 4.98),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.71, 2.93),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.71, 2.93),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.11, 3),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.11, 3),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.04, 2.43),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.04, 2.43),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.91, 1.9),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.91, 1.9),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.29, 3),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.29, 3),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.44, 3.88),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.44, 3.88),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.25, 3.58),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.25, 3.58),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4, 2.87),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4, 2.87),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.63),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.63),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.87, 2.61),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.87, 2.61),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.2, 3),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.2, 3),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.01, 3.87),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.01, 3.87),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.62, 2.87),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.62, 2.87),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.74, 2.49),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.74, 2.49),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.31),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.31),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.64, 1.84),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.64, 1.84),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.85, 2.79),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.85, 2.79),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.71, 3.94),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.71, 3.94),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.75, 3.63),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.75, 3.63),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.18, 3.18),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.18, 3.18),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.03, 2.65),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.03, 2.65),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.79, 2.3),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.79, 2.3),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 3.46, 2.95),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 3.46, 2.95),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.94, 5.16),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.94, 5.16),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.54, 3.18),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.54, 3.18),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.26, 2.6),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.26, 2.6),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.62, 1.98),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.62, 1.98),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.61, 1.56),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.61, 1.56),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.97, 2.21),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.97, 2.21),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.59, 4.31),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.59, 4.31),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.6, 3.37),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.6, 3.37),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.23, 2.54),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.23, 2.54),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.41, 1.94),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.41, 1.94),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.82, 1.98),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.82, 1.98),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.08, 2.33),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.08, 2.33),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.62, 4.65),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.62, 4.65),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 5.21, 3.51),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 5.21, 3.51),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.73, 2.75),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.73, 2.75),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.58, 3.03),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.58, 3.03),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.19, 2.34),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.19, 2.34),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.69, 1.89),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.69, 1.89),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.39, 4.19),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.39, 4.19),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.97, 3.25),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.97, 3.25),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.46, 3.02),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.46, 3.02),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.89, 2.57),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.89, 2.57),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.83, 2.13),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.83, 2.13),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.91, 2.49),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.91, 2.49),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.83, 4.73),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.83, 4.73),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.94, 3.88),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.94, 3.88),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.22, 2.5),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.22, 2.5),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.11, 1.96),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.11, 1.96),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.5, 1.69),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.5, 1.69),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.15, 1.55),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.15, 1.55),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.32, 3.69),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.32, 3.69),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 4.91, 3.17),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 4.91, 3.17),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.76, 2.28),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.76, 2.28),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.8, 2.34),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.8, 2.34),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.04, 2.27),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.04, 2.27),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.92, 1.98),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.92, 1.98),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.35, 4.38),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.35, 4.38)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_reliable_change_thresholds;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_reliable_change_thresholds
 (
 AssName STRING,
 Threshold DECIMAL(20,2)
 ) USING DELTA

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_reliable_change_thresholds
 VALUES
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 22.87), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 18.30), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 24.06), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 40.93), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 28),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 16.63),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score', 4), ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score', 4),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score', 4), ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score', 3), ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score', 3), --- derived from SDQ norms / Goodman (2001) paper
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score', 3), --- derived from SDQ norms / Goodman (2001) paper
 ('Child Outcome Rating Scale total score', 10),  ---- CYP IAPT 2015
 ('Clinical Outcomes in Routine Evaluation - 10 clinical score', 5 ), ---- CYP IAPT 2015
 ('Generalized anxiety disorder 7 item score', 4),  ---- CYP IAPT 2015
 ('Outcome Rating Scale total score', 6.60),  ---- CYP IAPT 2015
 ('Patient health questionnaire 9 score', 6),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 17.73), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 14.91), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 16.35), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 18.29),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 22.95),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 13.99),  ---- CYP IAPT 2015
 ('SCORE Index of Family Function and Change - 15 - total score', 10), -- from Stratton et al. (2014)
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score', 4),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score', 4),  ---- CYP IAPT 2015Br
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score', 4),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score', 3),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score', 5), --- derived from SDQ norms / Goodman (2001) paper 
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score', 4), --- derived from SDQ norms / Goodman (2001) paper 
 ("Young Persons Clinical Outcomes in Routine Evaluation clinical score", 8),  ---- CYP IAPT 2015
 ('Goal-Based Outcomes tool goal progress chart - goal 1 score', 3), ---added AT BITC-5078
 ('Goal-Based Outcomes tool goal progress chart - goal 2 score', 3), ---added AT BITC-5078
 ('Goal-Based Outcomes tool goal progress chart - goal 3 score', 3), ---added AT BITC-5078
 ('Goal Progress Chart - Child/Young Person - goal score', 3),
 ('Goal Progress Chart - Child/Young Person - goal 1 score', 3),
 ('Goal Progress Chart - Child/Young Person - goal 2 score', 3),
 ('Goal Progress Chart - Child/Young Person - goal 3 score', 3),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 1 score - disruptive, antisocial or aggressive behaviour', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 2 score - overactivity, attention and concentration', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 3 score - non-accidental self injury', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 4 score - alcohol, substance/solvent misuse', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 5 score - scholastic or language skills', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 6 score - physical illness or disability problems', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 7 score - hallucinations and delusions', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 8 score - non-organic somatic symptoms', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 9 score - emotional and related symptoms', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 10 score - peer relationships', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 11 score - self care and independence', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 12 score - family life and relationships', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 13 score - poor school attendance', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 1 score - disruptive, antisocial or aggressive behaviour', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 2 score - overactivity, attention and concentration', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 3 score - non-accidental self injury', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 4 score - alcohol, substance/solvent misuse', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 5 score - scholastic or language skills', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 6 score - physical illness or disability problems', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 7 score - hallucinations and delusions', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 8 score - non-organic somatic symptoms', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 9 score - emotional and related symptoms', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 10 score - peer relationships', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 11 score - self care and independence', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 12 score - family life and relationships', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 13 score - poor school attendance', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 1 score - disruptive, antisocial or aggressive behaviour', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 2 score - overactivity, attention and concentration', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 3 score - non-accidental self injury', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 4 score - alcohol, substance/solvent misuse', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 5 score - scholastic or language skills', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 6 score - physical illness or disability problems', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 7 score - hallucinations and delusions', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 8 score - non-organic somatic symptoms', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 9 score - emotional and related symptoms', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 10 score - peer relationships', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 11 score - self care and independence', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 12 score - family life and relationships', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 13 score - poor school attendance', 2),
 ('Short Warwick-Edinburgh Mental Well-being Scale score', 3), ---from Shah et al. (2018)
 ('Warwick-Edinburgh Mental Well-being Scale score', 9), ---from Maheswaran et al. (2012) 
 ('Childrens global assessment scale score', 11) ---from Bird et al. (1987)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_ass_type_scaling;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_ass_type_scaling
 (
 AssType STRING,
 Scale STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_ass_type_scaling
 ---Positive means high score good symptoms
 ---Negative means high score bad symptoms
 VALUES
 ('Brief Parental Self Efficacy Scale (BPSES)', 'Negative'),
 ('CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure)', 'Negative'),
 ('Child Group Session Rating Score (CGSRS)', 'Negative'),
 ('Child Outcome Rating Scale (CORS)', 'Positive'),
 ('Child Session Rating Scale (CSRS)', 'Negative'),
 ('Childrens Global Assessment Scale (CGAS)', 'Positive'),
 ('Childrens Revised Impact of Event Scale (8) (CRIES 8)', 'Negative'),
 ('Clinical Outcomes in Routine Evaluation 10 (CORE 10)', 'Negative'),
 ('Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated', 'Negative'),
 ('Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs', 'Negative'),
 ('Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs', 'Negative'),
 ('Comprehensive Assessment of At-Risk Mental States (CAARMS)', 'Negative'),
 ('Current View', 'Negative'),
 ('DIALOG', 'Negative'),
 ('Eating Disorder Examination Questionnaire (EDE-Q)', 'Negative'),
 ('Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents', 'Negative'),
 ('Genralised Anxiety Disorder 7 (GAD-7)', 'Negative'),
 ('Goal Based Outcomes (GBO)', 'Positive'),
 ('Group Session Rating Scale (GSRS)', 'Negative'),
 ('HoNOS Working Age Adults', 'Negative'),
 ('HoNOS-ABI', 'Negative'),
 ('HoNOS-CA (Child and Adolescent) - Clinician rated', 'Negative'),
 ('HoNOS-CA (Child and Adolescent) - Parent rated', 'Negative'),
 ('HoNOS-CA (Child and Adolescent) - Self rated', 'Negative'),
 ('HoNOS-LD (Learning Disabilities)', 'Negative'),
 ('HoNOS 65+ (Older Persons)', 'Negative'),
 ('HoNOS-Secure', 'Negative'),
 ('Kessler Psychological Distress Scale 10', 'Negative'),
 ('MAMS (Me and My School) Questionnaire', 'Negative'),
 ('Me and My Feelings Questionnaire', 'Negative'),
 ('ODD (Parent)', 'Negative'),
 ('Outcome Rating Scale (ORS)', 'Positive'),
 ('PGSI (Problem Gambling Severity Index)', 'Negative'),
 ('Patient Health Questionnaire (PHQ-9)', 'Negative'),
 ('Questionnaire about the Process of Recovery (QPR)', 'Negative'),
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated', 'Negative'),
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated', 'Negative'),
 ('ReQoL (Recovering Quality of Life 20-item)', 'Negative'),
 ('ReQoL (Recovering Quality of Life 10-item)', 'Negative'),
 ('SCORE-15 Index of Family Functioning and Change', 'Positive'),
 ('Session Feedback Questionnaire (SFQ)', 'Negative'),
 ('Session Rating Scale (SRS)', 'Negative'),
 ('Sheffield Learning Disabilities Outcome Measure (SLDOM)', 'Negative'),
 ('Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)', 'Positive'),
 ('Strengths and Difficulties Questionnaire (SDQ)', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs', 'Negative'),
 ('Warwick-Edinburgh Mental Well-being Scale (WEMWBS)', 'Positive'),
 ('YP-CORE', 'Negative'),
 ('Young Child Outcome Rating Scale (YCORS)', 'Negative')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_closed_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_closed_referrals
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Provider_Code string,
 Provider_Name string,
 ICB_Code string,
 ICB_Name string,
 Region_Code string,
 Region_Name string,
 LADistrictAuth string,
 AgeServReferRecDate bigint,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 ReferRejectionDate date,
 Der_HospSpellCount int,
 Der_Gender string,
 Der_Gender_Desc string,
 Age_Band string,
 UpperEthnicity string,
 IMD_Decile string,
 PrimReasonReferralMH string,
 PrimReasonReferralMHName string,
 ServTeamTypeRefToMH string,
 ServTeamTypeRefToMHDesc string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_closed_contacts;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_closed_contacts
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Provider_Code string,
 Contact1 date,
 Contact2 date
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_all_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_all_assessments
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Provider_Code string,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate date,
 CodedAssToolType string,
 Der_PreferredTermSNOMED string,
 Der_AssessmentToolName string,
 Der_AssessmentCategory string,
 PersScore string,
 Der_ValidScore string,
 Rater string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_ref_cont_out;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_ref_cont_out
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 Provider_Code string,
 Provider_Name string,
 ICB_Code string,
 ICB_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Contact1 date,
 Contact2 date,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate date,
 CodedAssToolType string,
 AssName string,
 AssType string,
 Rater string,
 Der_AssessmentCategory string,
 RawScore float,
 Der_ValidScore string
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_valid_unique_assessments_rcads;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_valid_unique_assessments_rcads
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 Provider_Code string,
 Provider_Name string,
 ICB_Code string,
 ICB_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Contact1 date,
 Contact2 date,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate date,
 CodedAssToolType string,
 AssName string,
 AssType string,
 Rater string,
 Der_AssessmentCategory string,
 RawScore float,
 Der_ValidScore string,
 TScore float
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_partition_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_partition_assessments
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 Provider_Code string,
 Provider_Name string,
 ICB_Code string,
 ICB_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Contact1 date,
 Contact2 date,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate date,
 CodedAssToolType string,
 AssName string,
 AssType string,
 Rater string,
 Der_AssessmentCategory string,
 RawScore float,
 Der_ValidScore string,
 TScore float,
 RN int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_last_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_last_assessments
 (
 Person_ID string,
 UniqServReqID string,
 AssName string,
 max_ass int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_first_and_last_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_first_and_last_assessments
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 Provider_Code string,
 Provider_Name string,
 ICB_Code string,
 ICB_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Contact1 date,
 Contact2 date,
 Der_AssessmentCategory string,
 Rater string,
 AssType string,
 AssName string,
 Der_AssUniqID_first string,
 AssDate_first date,
 Score_first float,
 Der_AssUniqID_last string,
 AssDate_last date,
 Score_last float,
 Score_Change float,
 Threshold float
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_rci;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rci
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 Provider_Code string,
 Provider_Name string,
 ICB_Code string,
 ICB_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Contact1 date,
 Contact2 date,
 Der_AssessmentCategory string,
 Rater string,
 AssType string,
 AssName string,
 Der_AssUniqID_first string,
 AssDate_first date,
 Score_first float,
 Der_AssUniqID_last string,
 AssDate_last date,
 Score_last float,
 Score_Change float,
 Threshold float,
 Reliable_Improvement int,
 Reliable_Deterioration int,
 No_Change int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_rci_referral;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rci_referral
 (
 Person_ID string,
 UniqServReqID string,
 Rater string,
 Contact1 date,
 Contact2 date,
 Assessment int,
 Paired int,
 Improvement int,
 NoChange int,
 Deter int
 ) USING DELTA

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_meaningful_change;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_meaningful_change
 (
 Person_ID string,
 UniqServReqID string,
 Rater string,
 Contact1 date,
 Contact2 date,
 Assessment int,
 Paired int,
 Improvement int,
 NoChange int,
 Deter int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_master;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_master
 (
 Person_ID string,
 UniqServReqID string,
 RecordNumber bigint,
 Der_FY string,
 UniqMonthID bigint,
 ReportingPeriodStartDate date,
 OrgIDProv string,
 Provider_Name string,
 STP_Code string,
 STP_Name string,
 Region_Code string,
 Region_Name string,
 Age int,
 Gender string,
 ReferralRequestReceivedDate date,
 ServDischDate date,
 Der_Gender string,
 Der_Gender_Desc string,
 Age_Band string,
 UpperEthnicity string,
 IMD_Decile string,
 PrimReasonReferralMH string,
 PrimReasonReferralMHName string,
 ServTeamTypeRefToMH string,
 ServTeamTypeRefToMHDesc string,
 Contact1 date,
 Contact2 date,
 Assessment_SR int,
 Paired_SR int,
 Improvement_SR int,
 NoChange_SR int,
 Deter_SR int,
 Assessment_PR int,
 Paired_PR int,
 Improvement_PR int,
 NoChange_PR int,
 Deter_PR int,
 Assessment_CR int,
 Paired_CR int,
 Improvement_CR int,
 NoChange_CR int,
 Deter_CR int,
 Assessment_ANY int,
 Paired_ANY int
 ) USING DELTA