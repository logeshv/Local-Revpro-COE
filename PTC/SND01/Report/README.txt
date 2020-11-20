==============================
README FOR PATCH INSTALLATION 
==============================

Patch Creation Date          : 03-NOV-2020

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
       
        1. VIEW_1.sql                          CUSTOM RI ACCT SUMM VIEW
        2. VIEW_2.sql                          CUSTOM ACCT SUMM VIEW
        3. VIEW_3.sql                          REPORT BASE VIEW
        4. Report_Script.sql                   CUSTOM REPORT SCRIPT
================================================================================
NOTE: PLEASE TAKE THE BACKUP OF THE EXISTING OBJECTS BEFORE DEPLOYING THE PATCH.    
      [NOT APPLICABLE IN CASE OF DATA FIX].
=================================================================================
For Support:
    If you need any assistance, you may contact        << Logesh V >>
         1. Contact phone number                       <<>>
         2. Email Support at respective Support Alias. <<lvarathan@zuora.com>>
