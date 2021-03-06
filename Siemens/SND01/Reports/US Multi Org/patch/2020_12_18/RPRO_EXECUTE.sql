spool RPRO_EXECUTE_OUTPUT.txt

SELECT NAME,TO_CHAR(SYSDATE,'DD-Mon-YYYY HH24:MI') TIME_STAMP FROM V$DATABASE;

SET DEFINE OFF;

SET SERVEROUTPUT ON;

PROMPT RPRO_CUST_WF_TBL.sql
@RPRO_CUST_WF_TBL.sql

PROMPT RPRO_CUST_SEQ.sql
@RPRO_CUST_SEQ.sql

PROMPT RPRO_CUST_SCHD_TBL.sql
@RPRO_CUST_SCHD_TBL.sql

PROMPT rpro_siemens_custom_pkg.pks
@rpro_siemens_custom_pkg.pks

PROMPT rpro_siemens_custom_pkg.pkb
@rpro_siemens_custom_pkg.pkb

PROMPT REVPRO_CUSTOM_SUMM_BG_JOB.sql
@REVPRO_CUSTOM_SUMM_BG_JOB.sql

PROMPT REP_DEL_SCRIPT.sql
@REP_DEL_SCRIPT.sql

PROMPT REP_CREATE_SCRIPT.sql
@REP_CREATE_SCRIPT.sql

spool off;
EXIT;


