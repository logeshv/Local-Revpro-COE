col owner for a20
col object_name for a35
col object_type for a45
set lines 150 pages 1000 verify off feedback off

prompt INVALID OBJECTS IN REVPRO
prompt #########################
select owner,object_name,object_type 
from dba_objects
where 	status = 'INVALID'
and	owner  = nvl('&1',owner)
order by 1,2;

prompt 
prompt ALL INVALID OBJECTS
prompt ###################
select owner, count(1)
from dba_objects
where status = 'INVALID'
group by owner;
