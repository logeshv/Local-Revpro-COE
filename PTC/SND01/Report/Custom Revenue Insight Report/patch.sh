#############################
# File Name : patch.sh
# Purpose   : RevPro patch installer script
# Created   : 24-May-2016
# Version   : 31
# Last Upd  : 06-NOV-2019
# 
# Change is: 
# 28/Nov/2017 : Remove Oracle Home parameter.
# 23/Jan/2019 : Updated input parameters
# 25/Jan/2019 : Fixed version arguement
# 05/Feb/2019 : Update ORACLE_HOME to not have / at the end
# 06/Feb/2019 : Update usage instruction
# 26/Feb/2019 : Moved Oracle Env, Schema Validate, Temp Password 
#               section from patch_rp.sh to patch.sh
# 27/Feb/2019 : Save the old password into a temp file.
#             : Log file to have schema name suffix
# 06/Mar/2019 : Added PLS- / SP2- in error checking
#             : Upper for user in user$ SQL
# 18/Mar/2019 : Added SID filter for Oracle PMON
#             : Capturing FAIL and ERROR text in logfile
#             : Export v_ruser variable
# 20/Jul/2019 : Updated the user input mode to not reset
#              schema password.
# 06/Nov/2019 : Added Pipestatus
#############################
##set -vx

clear
echo "          +------------------------------------------+"
echo "          |      Weclome to RevPro Patch Installer   |"
echo "          |                                          |"
echo "          |                                          |"
echo "          +------------------------------------------+"
echo " "
echo " "
v_revpro_installer_ver=30

if [ $(echo "$1"|egrep -i '\-v|-version|-ver'|wc -l) -eq 1 ]
then
        echo "  "
        echo "Patch Installer Version : $v_revpro_installer_ver"
        echo "  "
        exit 0;
elif [ $(echo "$1"|egrep -i 'list|listpatch'|wc -l) -eq 1 ]
then
	echo "Listing content of this patch..."
	grep 'LABEL' ./patch.cmd|awk -F: '{print $3}'
	exit 0;
fi;

if [ $# -lt 2 ]
then
        echo "Parameter not provided.."
        echo "Usage Instructions        "
        echo "------------------"
        echo "<<FULL_PATH>>/patch.sh <<ORACLE_SID>> <<REVPRO_SCHEMA>> '<<*FOEX|NOFOEX,*NOORDS|ORDS,*NODEBUG|DEBUG>>' "
	echo "DEFAULT VALUE MARKED WITH ==> * <=="
        echo "OR"
        echo "patch.sh -v [For Installer Version]"
        echo "  "
        echo "Example 1:"
        echo "/home/oracle/Revpro3.3.5.3_Build3353/patch.sh ORCL REVPRO 'FOEX,NOORDS,NODEBUG,patch=>36.001.04"
        echo "  "
        echo "Example 3: Running in a Debug mode"
        echo "/home/oracle/Revpro3.3.5.3_Build3353/patch.sh ORCL REVPRO 'NOFOEX,ORDS,DEBUG'"
        echo "  "
        echo "  "
        echo "Example 4: Running in a Admin mode"
        echo "/home/oracle/Revpro3.3.5.3_Build3353/patch.sh ORCL SYSDBA:RPRO_A/C 'NOFOEX,ORDS,DEBUG' [DEFAULT]"
	echo " OR "
        echo "/home/oracle/Revpro3.3.5.3_Build3353/patch.sh ORCL RPRO_A/C 'NOFOEX,ORDS,DEBUG' [DEFAULT]"
        echo "/home/oracle/Revpro3.3.5.3_Build3353/patch.sh ORCL ADMIN:ADMIN_A/C:ADMIN_A/C_PASSWD:RPROACCOUNT 'NOFOEX,ORDS,DEBUG'"
        echo "/home/oracle/Revpro3.3.5.3_Build3353/patch.sh ORCL RPRO:RPRO_A/C:RPRO_A/C_PASSWD 'NOFOEX,ORDS,DEBUGA'"
        echo "  "
        echo "Exiting..."
        echo "Exiting..."
        echo "  "
        exit 1;
fi

pid=`ps -ef | grep -w ora_pmon_$1 | grep -v grep | awk '{ print $2 }'`
dbs_loc=`pwdx $pid | awk '{print $2}'`
oh_loc=`echo "$dbs_loc" | sed 's/\/dbs//g'`
echo $oh_loc

v_mode=`echo $2|awk -F: '{print $1}'`
if [ "$v_mode" = 'SYSDBA' ]
then
{
	export v_connection="/ as sysdba";
	export v_rpro_user="$(echo $2|awk -F: '{print $2}')";
	export sys_sql="alter session set current_schema = $v_rpro_user;"
}
elif [ "$v_mode" = 'RPRO' ]
then
{
        export v_rpro_user="$(echo $2|awk -F: '{print $2}')";
        export v_conn_user="$(echo $2|awk -F: '{print $2}')";
        export v_conn_userpasswd="$(echo $2|awk -F: '{print $3}')";
        export v_connection="$v_conn_user/$v_conn_userpasswd"
        export sys_sql=" "
}
elif [ "$v_mode" = 'ADMIN' ]
then
{
        export v_conn_user="$(echo $2|awk -F: '{print $2}')";
        export v_conn_userpasswd="$(echo $2|awk -F: '{print $3}')";
        export v_rpro_user="$(echo $2|awk -F: '{print $4}')";
        export v_connection="$v_conn_user[$v_rpro_user]/$v_conn_userpasswd"
        export sys_sql=" "
}
else 
{
        export v_connection="/ as sysdba";
        export v_rpro_user="$(echo $2|awk -F: '{print $1}')";
        export sys_sql="alter session set current_schema = $v_rpro_user;"
}
fi

#v_date_ttt=`date +%d-%m-%Y_%H%M%S`
BASEDIR=$(dirname "$0")
cd $BASEDIR
v_curdir=`pwd|awk -F\/ '{print $NF}'`
echo $v_curdir

if ( test ! -d ../logs)
then
	mkdir ../logs
	if [ $? -ne 0 ];then echo "RPRO-PATCH-ERROR: Failed to create Log directory ../logs exiting." && exit 1;fi
fi

if [ $(echo $3|grep -i NODEBUG|wc -l) -eq 1 ]
then
	v_debug="NODEBUG"
elif [ $(echo $3|grep -i DEBUG|wc -l) -eq 1 ]
then
	v_debug="DEBUG"
else
	v_debug="NODEBUG"
fi

if [ $(echo $3|grep -i NOFOEX|wc -l) -eq 1 ]
then
        v_foex="NOFOEX"
else
	v_foex="FOEX"
fi

if [ $(echo $3|grep -i NOAPPS|wc -l) -eq 1 ]
then
        v_apps="NOAPPS"
else
        v_apps="APPS"
fi

if [ $(echo $3|grep -i NOORDS|wc -l) -eq 1 ]
then
        v_ords="NOORDS"
elif [ $(echo $3|grep -i ORDS|wc -l) -eq 1 ]
then
        v_ords="ORDS"
else
        v_ords="NOORDS"
fi

##### CHECK IF NON DEFAULT PATCH.CMD PASSED
if [ $(echo $3|grep -i .cmd|wc -l) -eq 1 ]
then
	for i in $(echo $3|sed "s/,/ /g")
        do
		if [ $(echo $i|grep -i .cmd|wc -l) -eq 1 ]
		then
			v_patchcmd=$i
			break;
		fi
	done
else
        v_patchcmd="patch.cmd"	####IF NOT PASSED SET DEFAULT
fi

##### CHECK IF PATCH VERSION IS PASSED
if [ $(echo $3|grep -i 'patch=>'|wc -l) -eq 1 ]
then
        for i in $(echo $3|sed "s/,/ /g")
        do
                if [ $(echo $i|grep -i 'patch=>'|wc -l) -eq 1 ]
                then
                        v_patchver=$(echo $i|sed "s/patch=>//g")
                        break;
                fi
        done

	##### Check if supplies patch version is part of patch build
	if [ $(grep 'LABEL' ${v_patchcmd}|awk -F: '{print $3}'|grep "^${v_patchver}$"|wc -l) -gt 0 ]
	then
		echo "PATCH INVENTORY CHECK PASSED."
	else
		echo "PATCH INVENTORY CHECK PASSED."
		echo "PATCH VERSION $v_patchver NOT FOUND IN ${v_patchcmd}"
		echo "Exiting..."
		exit 1;
	fi;
else
        v_patchver="NULL"       ####IF NOT PASSED SET NULL
fi
export v_ruser=$(echo $2|tr '[:lower:]' '[:upper:]')


echo "RPRO-PATCH-INFO: Setup Env Varibles."
#### SETUP ORACLE ENV SETTING
########
export ORACLE_HOME=$oh_loc
export ORACLE_SID=$1
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export PATH=$ORACLE_HOME/bin:$PATH:.
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8

#### VERIFY ORACLE_HOME / ORACLE_SID / CONNECTION
########
echo "RPRO-PATCH-INFO: DB Connection Test."
sqlplus -s /nolog << SQLEND 
whenever sqlerror exit 1;
connect ${v_connection}
set pages 0 lines 100
prompt #########################
select 'REVPRO A/C NAME: '||SYS_CONTEXT ('USERENV', 'SESSION_USER')||chr(10)||'PROXY USER: '||nvl(SYS_CONTEXT ('USERENV', 'PROXY_USER'),'NONE')||chr(10)||'TIMESTAMP: '||to_char(sysdate,'DD-MON-YYYY HH24MI') Check_Timestamp from dual;
prompt #########################
SQLEND

if [ $? == 0 ]; then echo "RRPO-PATCH-SECCESS: DB Connection Test."; else echo "RRPO-PATCH-ERROR: DB Connection Test." && exit 1; fi
#### VERIFY ORACLE_HOME / ORACLE_SID
########

./patch_rp.sh $oh_loc $1 $v_ruser $v_debug $v_ords $v_foex $v_apps $v_patchcmd $v_patchver 2>&1 | tee ../logs/patch_details_${v_curdir}_${v_date_ttt}_${v_ruser}.log
RP_EXIT=${PIPESTATUS[0]}
echo $RP_EXIT
if [ $RP_EXIT -ne 0 ]; then
  exit 1
fi

echo "################ PATCH ERROR ##############"
egrep -i 'ORA-|PLS-|SP2-|FAIL|ERROR'  ../logs/patch_details_${v_curdir}_${v_date_ttt}_${v_ruser}.log|egrep -v -i 'ORA-00955|ORA-01430|NO ERROR'
rm -rf ../logs/${v_ruser}_oldpwd_${v_date_ttt}.sql
