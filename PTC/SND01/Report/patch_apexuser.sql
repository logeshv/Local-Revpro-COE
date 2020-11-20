set pages 0 feedback off verify off
select username from dba_users where
username in (select 'APEX_'||lpad(rpad(replace(substr(VERSION_NO,1,3),'.','0'),5,'0'),6,'0') from apex_release);
exit;
