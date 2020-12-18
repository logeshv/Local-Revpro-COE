==============================
README FOR PATCH INSTALLATION 
==============================

Patch Creation Date          : 02-DEC-2020

Case Number                  : 

Case Description             : To Create Custom objects

Comments                     : Custom patch

Script(s) to Executed in
Schema                       : REVPRO_ATLA_S03

Re-runnable                  : YES

Pre-requisite                : N/A   
        
Execution Time (in minutes)  :  LESS THAN 2 MINUTE
                   
Execute the below files in the given order. 
    
       @RPRO_EXECUTE.sql
       
        1. rpro_cust_bill_idx1.sql            CUSTOM INDEX
        2. rpro_cust_bill_idx2.sql            CUSTOM INDEX                     
        3. rpro_cust_bill_idx3.sql            CUSTOM INDEX
        4. RPRO_ATL_CUSTOM_PKG.pks            CUSTOM PKG SPEC
        5. RPRO_ATL_CUSTOM_PKG.pkb            CUSTOM PKG BODY
        6. PREPROC_RCCOLLECT.sql              PRE/POST Processor
================================================================================
NOTE: PLEASE TAKE THE BACKUP OF THE EXISTING OBJECTS BEFORE DEPLOYING THE PATCH.    
      [NOT APPLICABLE IN CASE OF DATA FIX].
=================================================================================
For Support:
    If you need any assistance, you may contact        << Logesh V >>
         1. Contact phone number                       <<7200552055>>
         2. Email Support at respective Support Alias. <<lvarathan@zuora.com>>
