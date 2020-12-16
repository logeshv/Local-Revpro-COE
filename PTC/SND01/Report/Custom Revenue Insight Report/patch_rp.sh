#!/bin/bash
#############################
# File Name : patch.sh
# Purpose   : RevPro patch installer script
# Created   : 24-May-2016
# Version   : 31
# Last Upd  : 06-Nov-2019
# 
# Change His: 
# 10/01/18:- Added shell debug option
# 10/01/18:- Removed password validation functions
# 10/01/18:- Check if FOEX APP EXISTS
# 10/01/18:- Check ORDS is Deployed for Schema
# 10/02/18:- Change connection string based on SQL User in Cmd File
# 10/02/18:- Added FOEXSQL,FOEXCMD,ORDSSQL,ORDSCMD to provide more control
# 11/21/18:- Added decode option for version check 
# 01/22/19:- Updates the version check to deal with alphanumeric
# 01/23/19:- Changed --DEBUG to DEBUG & fixed SQL-RESULT BLOCK
# 01/29/19:- Fixed- ORDS apply  
# 01/05/19:- using the patch.cmd parameter
# 01/05/19:- Added found current build output
# 02/13/19:- Fixing the build validation when set to 0
# 02/13/19:- change the sysdba login string
# 02/21/19:- Added exception handling for ORDS check block
# 02/26/19:- Moved some of the initial checks into patch.sh
# 02/27/19:- Added exception handling for RevPro 3 version check block
# 03/18/19:- Added exit status check on OS cmd
# 08/13/19:- Added various connection mode inline with patch.sh
# 09/12/19:- Removed echo for the conenction string
# 11/06/19:- updated RPRO-PATCH-ERROR exit status to 1
#############################
##set -vx

#### VARIABLES
########
if [ $(echo $3|awk -F: '{print $1}') = 'SYSDBA' ]
then
	v_ruser=$(echo $3|awk -F: '{print $2}');

elif [ $(echo $3|awk -F: '{print $1}') = 'RPRO' ]
then
	v_ruser=$(echo $3|awk -F: '{print $2}');

elif [ $(echo $3|awk -F: '{print $1}') = 'ADMIN' ]
then
	v_ruser=$(echo $3|awk -F: '{print $4}');

fi

v_patch_loc=`pwd`
v_utest=0
v_user_mode=0
v_vcnt=0
v_pcnt=1
v_install_mode=" "

if [ "$4 " = "DEBUG " ]
then
{
        v_sqldebug="ON"
	set -vx
}
else
{
        v_sqldebug="OFF"
}
fi
if [ " $5" == " NOORDS" ]
then
	v_ordsapply="N"
else
	v_ordsapply="Y"
fi

if [ " $6" == " NOFOEX" ]
then
	v_foexapply="N"
else
	v_foexapply="Y"
fi

if [ " $7" == " NOAPPS" ]
then
        v_appsapply="N"
else
        v_appsapply="Y"
fi

v_patchcmd="$8"
v_patchver="$9"
##### CHECK IF PATCH VERSION IS PASSED
if [ ${v_patchver} != "NULL" ]
then
{
	##### IF NOT NULL SET THE FLAG
	v_patch_ver_check="Y"
}
fi

declare -a v_avar


########
#### CHECK IF APEX/FOEX APP EXISTS
########
echo "RPRO-PATCH-INFO: Check if Foex App is installed."
if [ $v_foexapply == "Y" ]
then
{
v_foex_appid=`sqlplus -s ${v_connection} <<SQLEND
set verify off feedback off pages 0
${sys_sql}
select trim(application_id)
from   APEX_APPLICATIONS
where  workspace = 'REVPRO'
and    application_name = 'RevPro 3.0'
and    owner = '${v_ruser}' and rownum <= 1
order by owner;
SQLEND`
}
else
{
	echo "RPRO-PATCH-INFO: Check FOEX APP...SKIPPED."
	v_foex_appid=0
}
fi
#### CHECK IF ORDS ENABLED
########
echo "RPRO-PATCH-INFO: Check If ORDS is enabled."
if [ $v_ordsapply == "Y" ]
then
{
v_ords_flag=`sqlplus -s ${v_connection} <<SQLEND
whenever sqlerror exit 1;
set pages 0 verify off feedback off
${sys_sql}
select trim(count(1)) from user_ords_schemas where parsing_schema = '$v_ruser' and status='ENABLED';
SQLEND`
if [ $? == 1 ] 
then 
	v_ords_flag=0
fi 
}
else
{
        echo "RPRO-PATCH-INFO: Check ORDS...SKIPPED."
	v_ords_flag=0
}
fi
#### CHECK IF ORDS ENABLED
########
if [ "${v_patch_loc} " = " " ]
then
{
	v_patch_loc=`pwd`
	echo "RPRO-PATCH-INFO: Patch Location Not provided."
	echo "RPRO-PATCH-INFO: Defaulting to Current Directory."
	echo "RPRO-PATCH-INFO: LOCATION = $v_patch_loc"
}
fi
echo "RPRO-PATCH-INFO: Starting Patch Execution `date`";
if [ -s ${v_patch_loc}/${v_patchcmd} ]
then
{
	cat ${v_patch_loc}/${v_patchcmd}|grep -v ^#|sed "s/vrprouser/${v_ruser}/" |while read cmdline
	do
		
		v_stage=`echo $cmdline|awk -F: '{print $1}'`
		v_type=`echo $cmdline|awk -F: '{print $2}'`
		
		### CHECK IF PATCH IS APPLICABLE
		if [ ${v_type} = 'LABEL' ]
		then
		{
			v_mainbuild=`echo $cmdline|awk -F: '{print $3}'`
			v_prebuild=`echo $cmdline|awk -F: '{print $4}'`
			v_prebuild_30=`echo $v_prebuild|awk -F\| '{print $1}'`
			v_prebuild_24=`echo $v_prebuild|awk -F\| '{print $2}'`
			##### CHECK IF VERSION CHECK IS NEEDED AND IF CURRENT VERSION MATCHES WITH TARGET VERSION
			if [ "${v_patch_ver_check}" = "Y" -a "${v_patchver}" = "${v_mainbuild}" ]
			then
			{
				##### SET EXIT FLAG TO STOP PATCH BEFORE AT NEXT PATCH LABEL
				v_exit_flag="Y"
				echo "SETTING EXIT FLAG ====> Y"
			}
			elif [ "${v_patch_ver_check}" = "Y" -a "${v_exit_flag}" = "Y" ]
			then
			{
				echo "#######################"
				echo "EXITING AS REACHED DESIRED PATCH LEVEL ${v_patchver}"
				echo "#######################"
				exit 0;
			}
			fi
			for i in $(echo $v_prebuild_30|sed "s/,/ /g")
			do
v_build_30=`sqlplus -s ${v_connection} <<SQLEND
whenever sqlerror exit 1;
set feedback off verify off pages 0
${sys_sql}
set serveroutput on size 10000
declare
    v_cnt number(3):=0;
    v_query VARCHAR2(200);
    v_build number(10):=0;
begin
    select count(1) into v_cnt from rpro_setup;
if v_cnt = 1 then
    v_query:='select count(1) from rpro_setup where bld_num in (decode(''${i}'',0,bld_num,''${i}''))';
else
    v_query:='select count(1) from rpro_setup_g where bld_num in (decode(''${i}'',0,bld_num,''${i}''))';
end if;
EXECUTE IMMEDIATE v_query INTO v_build;
dbms_output.put_line(v_build);
end;
/
SQLEND`

if [ $? == 1 ]
then
        v_build_30=0
fi
echo "=============> $v_build_30"
	        	if [ $v_build_30 -ge 1 ]
        		then
v_bldnum=`sqlplus -s ${v_connection} <<SQLEND
set verify off feedback off pages 0
${sys_sql}
select bld_num from rpro_setup_g;
SQLEND`
echo "FOUND THE BUILD $v_bldnum"
                		break;
        		fi;
			done
		if [ ${v_build_30} -ge 1 ]
		then
		{
			v_apply="Y"
			echo "RPRO-PATCH-SUCCESS: RevPro required release found."
		}
		elif [ "${i}" = "0" ] 
		then
		{
			v_apply="Y"
			echo "RPRO-PATCH-SUCCESS: RevPro release validation SKIPPED, applying the patch."		
		}
		else
		{
			v_apply="N"
		}
		fi;
		echo "cmdline       >>>>>>>>>>  $cmdline"
		echo "v_build_30    >>>>>>>>>>  ${v_build_30}"
		echo "v_prebuild_30 >>>>>>>>>>  ${v_prebuild_30}"
		echo "v_build_24    >>>>>>>>>>  ${v_build_24}"
		echo "v_prebuild_24 >>>>>>>>>>  ${v_prebuild_24}"
		echo "v_apply       >>>>>>>>>>  ${v_apply}"
			
		}
		elif [ ${v_type} = "SQL" -a ${v_apply} = "Y" ]
		then
		{
			v_sqluser="`echo $cmdline|awk -F: '{print $3}'`"
                	### Setup SQL User
                	if [ ${v_sqluser} == 'SYSDBA' ]
                	then
                	{
                        	v_conn="sys/oracle as sysdba"
                	}
                	else
                	{
                        	v_conn="$v_ruser/$v_rpuser_pwd_tmp"
                	}
                	fi
			echo "$cmdline"|grep vpara
                        if [ $? -eq 0 ]
			then 
				v_pcheck="Y"; 
			else 
				v_pcheck="N"; 
			fi
			while [ "$v_pcheck" = "Y" ]
			do
				v_cmdline=`echo $cmdline|sed "s/vpara/${v_avar[$v_pcnt]}/"`   ### REPLACE VPARA WITH VALUE
				cmdline="$v_cmdline"
				v_pcnt=`expr $v_pcnt + 1`
				echo "$cmdline"|grep vpara
                        if [ $? -eq 0 ]
			then 
				v_pcheck="Y"; 
			else 
				v_pcheck="N"; 
			fi
			done
			v_dbuser=`echo $cmdline|awk -F: '{print $3}'`
			v_sqlcmd=`echo $cmdline|awk -F: '{print $4}'`
sqlplus ${v_connection} <<SQLEND
set feedback ${v_sqldebug} echo ${v_sqldebug}
${sys_sql}
show user
@${v_sqlcmd} ${v_ruser}
SQLEND
			if [ $? -ne 0 ] 
			then
			{
				echo "RRPO-PATCH-ERROR: Error encountered at $v_stage - $v_sqlcmd"
				exit 1;
			}
			fi
		}
		elif [ ${v_type} = "FOEXSQL" -a ${v_foexapply} = "Y" -a ${v_apply} = "Y" ]
                then
                {
			v_sqluser="`echo $cmdline|awk -F: '{print $3}'`"
                        ### Setup SQL User
                        if [ ${v_sqluser} == 'SYSDBA' ]
                        then
                        {
                                v_conn="\"/ as sysdba\""
                        }
                        else
                        {
                                v_conn="$v_ruser/$v_rpuser_pwd_tmp"
                        }
                        fi
			echo "$cmdline"|grep vpara
                        if [ $? -eq 0 ]
			then 
				v_pcheck="Y"; 
			else 
				v_pcheck="N"; 
			fi
                        while [ "$v_pcheck" = "Y" ]
                        do
                                v_cmdline=`echo $cmdline|sed "s/vpara/${v_avar[$v_pcnt]}/"`   ### REPLACE VPARA WITH VALUE
                                cmdline="$v_cmdline"
                                v_pcnt=`expr $v_pcnt + 1`
                                echo "$cmdline"|grep vpara
                        	if [ $? -eq 0 ] 
				then 
					v_pcheck="Y"; 
				else 
					v_pcheck="N"; 
				fi
                        done
                        v_dbuser=`echo $cmdline|awk -F: '{print $3}'`
                        v_sqlcmd=`echo $cmdline|awk -F: '{print $4}'`
                        echo $v_sqlcmd|egrep '511'
                        if [ $? -eq 0 ]
                        then
                                v_sqlcmd=`echo $v_sqlcmd|sed "s/511/${v_foex_appid}/"`
                                echo "SQLCMD WITH UPDATED APPID ======= > ${v_sqlcmd}"
                        fi;	
sqlplus ${v_connection} <<SQLEND
set feedback ${v_sqldebug} echo ${v_sqldebug}
${sys_sql}
show user
@${v_sqlcmd} ${v_ruser}
SQLEND
		}
		elif [ ${v_type} = "FOEXCMD" -a ${v_foexapply} = "Y" -a ${v_apply} = "Y" ]
                then
                {
                        v_osuser=`echo $cmdline|awk -F: '{print $3}'`
                        v_oscmd=`echo $cmdline|awk -F: '{print $4}'`
                        $v_oscmd	
		}
		elif [ ${v_type} = "ORDSSQL" -a ${v_ords_flag} -eq 1 -a ${v_apply} = "Y" ]
                then
                {
			v_sqluser="`echo $cmdline|awk -F: '{print $3}'`"
                        ### Setup SQL User
                        if [ ${v_sqluser} == 'SYSDBA' ]
                        then
                        {
                                v_conn="\"/ as sysdba\""
                        }
                        else
                        {
                                v_conn="$v_ruser/$v_rpuser_pwd_tmp"
                        }
                        fi
                        echo "$cmdline"|grep vpara
                        if [ $? -eq 0 ]
                        then
                                v_pcheck="Y";
                        else
                                v_pcheck="N";
                        fi
                        while [ "$v_pcheck" = "Y" ]
                        do
                                v_cmdline=`echo $cmdline|sed "s/vpara/${v_avar[$v_pcnt]}/"`   ### REPLACE VPARA WITH VALUE
                                cmdline="$v_cmdline"
                                v_pcnt=`expr $v_pcnt + 1`
                                echo "$cmdline"|grep vpara
                                if [ $? -eq 0 ]
                                then
                                        v_pcheck="Y";
                                else
                                        v_pcheck="N";
                                fi
                        done
                        v_dbuser=`echo $cmdline|awk -F: '{print $3}'`
                        v_sqlcmd=`echo $cmdline|awk -F: '{print $4}'`
sqlplus ${v_connection} <<SQLEND
set feedback ${v_sqldebug} echo ${v_sqldebug}
${sys_sql}
show user
@${v_sqlcmd} ${v_ruser}
SQLEND
                }
		elif [ ${v_type} = "ORDSCMD" -a ${v_ordsapply} = "Y" -a ${v_apply} = "Y" ]
                then
                {
                        v_osuser=`echo $cmdline|awk -F: '{print $3}'`
                        v_oscmd=`echo $cmdline|awk -F: '{print $4}'`
                        $v_oscmd
		}
		elif [ ${v_type} = "SQL-RESULT" -a ${v_apply} = "Y" ]
		then
		{
		        v_dbuser=`echo $cmdline|awk -F: '{print $3}'`
                        v_sqlcmd=`echo $cmdline|awk -F: '{print $4}'`	
			v_vcnt=`expr ${v_vcnt} + 1`
 
                        v_sqluser="`echo $cmdline|awk -F: '{print $3}'`"
                        ### Setup SQL User
                        if [ ${v_sqluser} == 'SYSDBA' ]
                        then
                        {
                                v_conn="\"/ as sysdba\""
                        }
                        else
                        {
                                v_conn="$v_ruser/$v_rpuser_pwd_tmp"
                        }
                        fi
v_avar[$v_vcnt]=`sqlplus -s ${v_connection} <<SQLEND
set feedback off verify off
${sys_sql}
@${v_sqlcmd} ${v_ruser}
SQLEND`
		}
		elif [ ${v_type} = "OS" -a ${v_apply} = "Y" ]
		then
		{
			v_osuser=`echo $cmdline|awk -F: '{print $3}'`
			v_oscmd=`echo $cmdline|awk -F: '{print $4}'`
			$v_oscmd
			if [ $? -ne 0 ]
                        then
                                echo "RPRO-PATCH: O/S Command exit with ERROR."
                        fi
		}
		elif [ ${v_type} = "INFO" -a ${v_apply} = "Y" ]
		then
		{
			echo $cmdline|awk -F: '{print $3}'
		}
		else
		{
			echo "Skipping steps for ${v_mainbuild}....."
		}
		fi
	done
}
else
{
	echo "RPRO-PATCH-ERROR: ${v_patch_loc}/${v_patchcmd} not found or not readable!"
	echo "exiting."
	exit 1;
}
fi
