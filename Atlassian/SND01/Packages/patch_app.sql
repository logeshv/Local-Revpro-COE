--## 1 = APEX SCHEMA NAME ##--
--## 2 = APPLICATION ID ##--
--## 3 = REVPRO SCHEMA NAME ##--
PROMPT
PROMPT SETTING CURRENT SCHEMA TO APEX
alter session set current_schema = &1;
PROMPT
PROMPT RUNNING THE APPLICATION PATCH FILES
PROMPT
@./AppInstallCMD.sql &2 &3
