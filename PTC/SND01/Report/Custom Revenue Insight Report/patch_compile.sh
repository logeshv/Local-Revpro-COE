#set -xv
v_compile_flag="N"

v_waittime1=$(( ( RANDOM % 5 )  + 1 ))
echo "PATCH_COMPILE : INITIAL WAIT ${v_waittime1}";
sleep $v_waittime1

while [ ${v_compile_flag} = "N" ]
do
	### CHECK IF ANOTHER COMPILE PROCESS
	if [ ! -s /tmp/patch_compile.ttt ]
	then
	{
		### CRETAE A LOCK FILE
		echo `date` > /tmp/patch_compile.ttt
sqlplus -s "/ as sysdba" <<SQLEND
@./patch_compile.sql $1
SQLEND
		### CLEAN-UP LOCK FILE
		rm -rf /tmp/patch_compile.ttt
		v_compile_flag="Y"
		echo "PATCH_COMPILE: COMPILE COMPLETED."
	}
	else
	{
		v_waittime=$(( ( RANDOM % 10 )  + 1 ))
		echo "PATCH_COMPILE : PROCESS ALREADY RUNNING...WAITING FOR ${v_waittime}"
		sleep ${v_waittime}
	}
	fi
done
