spool RPRO_EXECUTE_OUTPUT.txt

SELECT NAME,TO_CHAR(SYSDATE,'DD-Mon-YYYY HH24:MI') TIME_STAMP FROM V$DATABASE;

SET DEFINE OFF;

SET SERVEROUTPUT ON;


PROMPT rpro_cust_bill_idx1
@rpro_cust_bill_idx1.sql;

PROMPT rpro_cust_bill_idx2
@rpro_cust_bill_idx2.sql;

PROMPT rpro_cust_bill_idx3
@rpro_cust_bill_idx3.sql;

PROMPT RPRO_ATL_CUSTOM_PKG
@RPRO_ATL_CUSTOM_PKG.pks;

PROMPT RPRO_ATL_CUSTOM_PKG
@RPRO_ATL_CUSTOM_PKG.pkb;

PROMPT PREPROC_RCCOLLECT
@PREPROC_RCCOLLECT.sql;

spool off;
EXIT;


