==============================
README FOR PATCH INSTALLATION 
==============================

Patch Creation Date          : 02-AUG-2019

Case Number                  : << TOPS Number>>

Case Description             : Customization

Comments                     : Create customization

Data Fix (Or) Code Fix       : New Customization to be deployed

Script(s) to Executed in
Schema                       : REVPRO_COE

Re-runnable                  : NO

Pre-requisite                : N/A   
        
Execution Time (in minutes)  : << LESS THAN 10 MINUTES >>

Execute script               : @RPRO_EXECUTE.sql 
                  
Execute the below files in the given order. 
        1. RPRO_CUST_WF_TBL                           CUSTOM TABLE FOR REPORT SCRIPT
        2. RPRO_CUST_SEQ                              CUSTOM SCHEDULE SEQUENCE
		3. RPRO_CUST_SCHD_TBL                         CUSTOM SCHEDULE TABLE  
        4. rpro_siemens_custom_pkg_m_org.pks                CUSTOM PKG SPEC
        5. rpro_siemens_custom_pkg_m_org.pkb                CUSTOM PKS BODY
        6. REVPRO_CUSTOM_SUMM_BG_JOB.sql              CUSTOM BG JOB
		7. REP_DEL_SCRIPT.sql                         CUSTOM REPORT DELETE SCRIPT
		8. REP_CREATE_SCRIPT.sql                      CUSTOM REPORT CREATE SCRIPT
================================================================================
NOTE: PLEASE TAKE THE BACKUP OF THE EXISTING OBJECTS BEFORE DEPLOYING THE PATCH.    
      [NOT APPLICABLE IN CASE OF DATA FIX].
=================================================================================
For Support:
    If you need any assistance, you may contact  << Logesh. V >>
         1. Contact phone number <<+91-7200552055>>
         2. Email Support at respective Support Alias. <<lvarathan@zuora.com>>
