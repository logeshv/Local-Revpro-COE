==============================
README FOR PATCH INSTALLATION 
==============================

Patch Creation Date          : 21-NOV-2020

Case Number                  : 

Case Description             : To Create Custom objects

Comments                     : Custom patch

Script(s) to Executed in
Schema                       : REVPRO_PTC_SND01

Re-runnable                  : YES

Pre-requisite                : N/A   
        
Execution Time (in minutes)  :  LESS THAN 2 MINUTE
                   
Execute the below files in the given order. 
    
       @RPRO_EXECUTE.sql
       
        1. RPRO_RC_LINE_CBILL_V.sql            CUSTOM BILL AND LINE VIEW
        2. RPRO_CUST_RI_LN_ACCT_SUMM_V.sql     RPRO_CUST_RI_LN_ACCT_SUMM_V                     
        3. RPRO_CUST_ACCT_SUMM_V               CUSTOM ACCT SUMM VIEW
        3. RPRO_RI_CUST_INSIGHT_V.sql          REPORT BASE VIEW
        4. Rep_Script.sql                      CUSTOM REPORT SCRIPT
        5. RPRO_CUST_BILL_IDX_1.sql            CUSTOM INDEX FOR PERFORMANCE
================================================================================
NOTE: PLEASE TAKE THE BACKUP OF THE EXISTING OBJECTS BEFORE DEPLOYING THE PATCH.    
      [NOT APPLICABLE IN CASE OF DATA FIX].
=================================================================================
For Support:
    If you need any assistance, you may contact        << Logesh V >>
         1. Contact phone number                       <<>>
         2. Email Support at respective Support Alias. <<lvarathan@zuora.com>>
