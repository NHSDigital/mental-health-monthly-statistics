# Databricks notebook source
db_output=dbutils.widgets.get("db_output")
print(db_output)
assert db_output

# COMMAND ----------

# DBTITLE 1, RestrictiveIntTypeDim (candidate for REF_DATA)
%sql
TRUNCATE TABLE $db_output.RestrictiveIntTypeDim;
INSERT INTO $db_output.RestrictiveIntTypeDim VALUES
('06','16','Segregation'),
('05', '15', 'Seclusion'),
('13', '14', 'Physical restraint - Other (not listed)'),
('09', '13', 'Physical restraint - Supine'),
('07', '12', 'Physical restraint - Standing'),
('10', '11', 'Physical restraint - Side'),
('11', '10', 'Physical restraint - Seated'),
('08', '9', 'Physical restraint - Restrictive escort'),
('12', '8', 'Physical restraint - Kneeling'),
('01', '7', 'Physical restraint - Prone'),
('04', '6', 'Mechanical restraint'),
('17', '5', 'Chemical restraint - Other (not listed)'),
('16', '4', 'Chemical restraint - Oral'),
('14', '3', 'Chemical restraint - Injection (Rapid Tranquillisation)'),
('15', '2', 'Chemical restraint - Injection (Non Rapid Tranquillisation)');

# COMMAND ----------

# DBTITLE 1,GenderDim (candidate for REF_DATA)
%sql
TRUNCATE TABLE $db_output.GenderDim;
INSERT INTO $db_output.GenderDim VALUES
('1', 'Male'),
('2', 'Female'),
('3', 'Not stated')

# COMMAND ----------

# DBTITLE 1,NHSDEthnicityDim (candidate for REF_DATA)
%sql
TRUNCATE TABLE $db_output.NHSDEthnicityDim;
INSERT INTO $db_output.NHSDEthnicityDim VALUES
('A', '1', 'White'),
('B', '1', 'White'),
('C', '1', 'White'),
('D', '2', 'Mixed'),
('E', '2', 'Mixed'),
('F', '2', 'Mixed'),
('G', '2', 'Mixed'),
('H', '3', 'Asian'),
('J', '3', 'Asian'),
('K', '3', 'Asian'),
('L', '3', 'Asian'),
('M', '4', 'Black'),
('N', '4', 'Black'),
('P', '4', 'Black'),
('R', '5', 'Other'),
('S', '5', 'Other'),
('Z', '6', 'Not Stated')

# COMMAND ----------

# DBTITLE 1,ProviderTypeDim
%sql
TRUNCATE TABLE $db_output.ProviderTypeDim;
INSERT INTO $db_output.ProviderTypeDim VALUES
(1, 'NHS Providers'),
(2, 'Non NHS Providers')

# COMMAND ----------

# DBTITLE 1,AgeBandsDim
%sql
TRUNCATE TABLE $db_output.AgeBandsDim;
INSERT INTO $db_output.AgeBandsDim VALUES
(1, 'Under 18'),
(2, '18-24'),
(3, '25-34'),
(4, '35-44'),
(5, '45-54'),
(6, '55-64'),
(7, '65 or over'),
(8, 'UNKNOWN')