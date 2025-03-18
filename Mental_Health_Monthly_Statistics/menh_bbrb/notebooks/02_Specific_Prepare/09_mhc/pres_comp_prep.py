# Databricks notebook source
 %sql
  INSERT OVERWRITE TABLE $db_output.open_ref_pres_comp
 SELECT 
 REF.UniqServReqID,
 REF.Person_ID,
 REF.UniqMonthID,
 REF.OrgIDProv,
 E.NAME AS Provider_Name,
 MPI.IC_Rec_CCG AS CCG_code,
 MPI.NAME AS CCG_Name,
  
 PCDGN.FindSchemeInUse,
 PCDGN.PresComp,
 PCDGN.PresCompCodSig,
 PCDGN.PresCompDate,
 PCDGN.RecordNumber,
 PCDGN.MHS609UniqID,
 PCDGN.RecordStartDate,
 PCDGN.RecordEndDate,
 PCDGN.RowNumber
  
 FROM $db_output.MHS001MPI_latest_month_data AS MPI
 INNER JOIN $db_source.MHS101Referral AS REF
            ON MPI.Person_ID = REF.Person_ID AND MPI.OrgIDProv = REF.OrgIDProv
            
 INNER JOIN $db_source.MHS609PresComp PCDGN ON REF.UniqServReqID = PCDGN.UniqServReqID AND REF.UniqMonthID = PCDGN.UniqMonthID
       AND ((PCDGN.RecordEndDate IS NULL OR PCDGN.RecordEndDate >= '$rp_enddate') AND PCDGN.RecordStartDate <= '$rp_enddate')
 LEFT JOIN $db_output.bbrb_org_daily_latest E ON REF.OrgIDProv = E.ORG_CODE
  
 WHERE REF.ServDischDate IS NULL OR REF.ServDischDate >= '$rp_enddate'
 AND REF.UniqMonthID = $end_month_id AND MPI.UniqMonthID = $end_month_id