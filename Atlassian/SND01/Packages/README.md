# patch
Patching Framework
------------------

Patch Framework contains following files.

1) patch.sh : Is the executable file for running a patch, which inturn calls patch_rp.sh

<<FULL_PATH>>/patch.sh <<ORACLE_SID>> <<REVPRO_SCHEMA>> 

Execute remotely on the server

ssh oracle@hostname "<<FULL_PATH>>/patch.sh <<ORACLE_SID>> <<REVPRO_SCHEMA>> '<<*FOEX|NOFOEX,*APPS|NOAPPS,*NOORDS|ORDS,*NODEBUG|DEBUG>>'"

2) patch_rp.sh : Is the main shell script which runs the patch by parsing the patch.cmd for execution steps.

3) patch.cmd : Is the command file containing all the actions any patch or deployment needs to perform.

Sample patch.cmd
----------------
POSTINSTALL:INFO:#### COMPLETED 32202497 #####
PREINSTALL:LABEL:32302497:3220,3221,3222,3223,3224|0
INSTALL:OS:ANY:cd ./32302497
PREINSTALL:SQL-RESULT:REVPRO:patch_apexuser.sql 
PREINSTALL:SQL:REVPRO:patch_precheck.sql
PREINSTALL:SQL:REVPRO:patch_invobj.sql
INSTALL:SQL:REVPRO:patch_db.sql vrprouser
INSTALL:SQL:REVPRO:patch_app.sql vpara 511 vrprouser
INSTALL:APPSSQL:APPS:apps_script.sql
POSTINSTALL:SQL:REVPRO:patch_invobj.sql
INSTALL:OS:ANY:cd ..
POSTINSTALL:INFO:#### COMPLETED 32302497 ######

4) patch_db.sql : Sample file executing Database changes.

5) patch_app.sql : Sample file execution apex application changes.

6) patch_invobj.sql : Script to list invalid object pre or post patch.
