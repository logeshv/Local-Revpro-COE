conn / as sysdba
show user
prompt #Compile Invalids#
exec sys.utl_recomp.recomp_serial(upper('&&1'));
