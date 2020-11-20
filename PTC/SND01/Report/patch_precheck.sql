set verify off feedback off
set serveroutput on size 10000
WHENEVER SQLERROR EXIT 1
declare
    v_build number(10):=0;
    v_build_chk number(10):=&&1;
begin
    select build_number into v_build from rpro_common_setup;
    if v_build = v_build_chk then
        dbms_output.put_line(v_build);
    else
    raise_application_error(-20001,'Incorrect Build of RevPro. - '||v_build);
    end if;
end;
/
exit;
