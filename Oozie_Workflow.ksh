#!/bin/ksh

#------------------------------------------------------------------------------
# NAME          : Oozie_workflow.ksh
# Description   : This is the file trigger script 
#
# Author        : DEV
#
# Modified by           Date            Changes
#--------------------- --------------- ----------------------------------------
# 		              				    Original Version
#------------------------------------------------------------------------------


#
# This function kicks off the Oozie workflow in case of new raw files.
# It expects three arguments - poll interval, batch size and env file
#  - poll interval - polling interval for file
#  - batch size - No of files to be process in a single batch
#  - env file - environment file path
# Typically this workflow takes 9 - 10 mins - for all 3 tables. Give 800 s as arg
#
function main
{

set -x

#counter=0
MAX_RUN_WF_THRESHOLD=$BATCH_SIZE

TODAY=`date +'%Y%m%d'`
FILE=vzwiglobal_????????-*.tar.gz

cd ${DATA_DIR}

while true 
do
    x=`ls -1 ${FILE}|wc -l`

    if [ "x$x" != "x" ]
    then

     for FILE_NAME in `ls -1  ${FILE}| head -1`
     do
        FILE_DT=`echo ${FILE_NAME} | awk -F '[_.]' '{print $2}' | awk -F '[-.]' '{print $1}'`
        FILE_DATE=`date -d ${FILE_DT} +'%Y-%m-%d'`
        FILE_HR=`echo ${FILE_NAME} | awk -F '[-.]' '{print $2}' | cut -c 1-2`

        FILE_PREVSIZE=`ls -l "${FILE_NAME}"|awk '{print $5}'`
        sleep $POLL_INTERVAL

        FILE_CURRSIZE=`ls -l "${FILE_NAME}"|awk '{print $5}'`

        if [ "$FILE_PREVSIZE" -eq "$FILE_CURRSIZE" ]
        then
            echo 'Successful, The Workflow can be run now!!' 
  
            NO_OF_RUNNING_WF=`hadoop fs -ls ${HDFS_RUNNING_WKFL_TOUCH_FILE_DIR} | wc -l`;

            echo "NO_OF_RUNNING_WF:$NO_OF_RUNNING_WF, Path: ${HDFS_RUNNING_WKFL_TOUCH_FILE_DIR}"

            echo  "File name is ${FILE_NAME}"
            if [ "$NO_OF_RUNNING_WF" -le "$MAX_RUN_WF_THRESHOLD" ]
              then
     
                 if [ -f "${PROCESS_DIR}/${FILE_NAME}" ]
                     then
                        echo "File found in ${PROCESS_DIR}. Removing the file and processing it again."
                        rm ${PROCESS_DIR}/${FILE_NAME}
                      else
                        echo "File not found."
                      fi

                 scp ${FILE_NAME} sL_abc@fsngl-hdp.tdc.abc.com:/data/dasi/abc/scat 
                 mv ${FILE_NAME} ${PROCESS_DIR}/.   
	         ksh ~/abc/release/${PROCESS_NM_LC}/bin/${PROCESS_NM_LC}_run.ksh $FILE_DT $FILE_HR $FILE_DATE

             fi 
        fi
     done
    fi
    sleep $POLL_INTERVAL
    continue
done

}

set -x

TODAY=`date +'%Y%m%d'`

if [ "$#" != "3" ]
then
    echo "Usage: $0 <poll_interval_in_sec> <batch_size> <env_file>"
    exit 1
fi

POLL_INTERVAL=$1
BATCH_SIZE=$2

file_found=0

	x=`echo ${0%%\.ksh}`
        export SCRIPT=${x##*/}

######################################
# Reading the environment file
######################################
        [ -r ${3} ] || {
            echo "${0} cannot read env file ${3}"
            exit 1
        }
        . ${3}

#--quit without log if script is already running
SCRIPT_RUNNING=`ps -ef|grep $SCRIPT|grep -v grep|grep -v log|grep -v vi|wc -l|awk '{print $1}'`
if [ ${SCRIPT_RUNNING} -gt 1 ]; then 
    echo "${SCRIPT}.ksh is already running"
    return $? 
fi


P='%'
export ds=$(date +${P}Y${P}m${P}d"."${P}H${P}M${P}S)
export LOGFILE=${SCRIPT}_$$.${ds}.log
export LOG_FILE=${LOG_PATH}/${LOGFILE}

hdfs dfs -rm -r -skipTrash /user/svc-omg_abc_pld/abc/scripts/scat_all
hdfs dfs -copyFromLocal /home/sL_abc/abc/scripts/scat_all  /user/svc-omg_abc_pld/abc/scripts/

#hdfs dfs -rm -r -skipTrash /user/svc-disc_abc_dld/abc/scripts/scat_all
#hdfs dfs -copyFromLocal ~/abc/dev/scripts/scat_all /user/svc-disc_abc_dld/abc/scripts/

main  > ${LOG_FILE} 2>&1
EXITCD=$?

	if [ $EXITCD != 0 ] ; then
                echo "investigate `hostname`:${LOG_FILE}" | mailx -s "${PROCESS_NM_UC} error!!" $RECIPIENT_LIST
        fi
echo "All ${PROCESS_NM_UC} files processed successfuly!!"
