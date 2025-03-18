# Databricks notebook source
 %sql
 CREATE TABLE IF NOT EXISTS $db_output.open_ref_pres_comp
 (
 UniqServReqID string,
 Person_ID string,
 UniqMonthID bigint,
 OrgIDProv string,
 Provider_Name string,
 CCG_Code string,
 CCG_Name string,
  
 FindSchemeInUse string,
 PresComp string,
 PresCompCodSig string,
 PresCompDate date,
 RecordNumber bigint,
 MHS609UniqID bigint,
 RecordStartDate date,
 RecordEndDate date,
 RowNumber bigint
 ) USING DELTA