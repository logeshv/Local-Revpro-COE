spool RPRO_EXECUTE_OUTPUT.txt

SELECT NAME,TO_CHAR(SYSDATE,'DD-Mon-YYYY HH24:MI') TIME_STAMP FROM V$DATABASE;

SET DEFINE OFF;

SET SERVEROUTPUT ON;


PROMPT VIEW_1
@VIEW_1.sql;

PROMPT VIEW_2
@VIEW_2.sql;

PROMPT VIEW_3
@VIEW_3.sql;

PROMPT Report_Script
@Report_Script.sql;

spool off;
EXIT;


