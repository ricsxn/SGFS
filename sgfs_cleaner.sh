#!/bin/sh
#
# sgfs_cleaner.sh - Script to clean the Science Gateway File System database and the
#                   related temporary files; it also kills pending lcg-* commands 
#
# Call this script manually or via scheduled cron job executing:
#
# #sgfs_cleaner.sh [info|exec] [cutoff] > $(date +%Y%m%d%H%M%S)_sgfs_cleaner.log
#
# Author: riccardo.bruno@ct.infn.it
#
DB_USERNAME=sgfs_user
DB_PASSWORD=sgfs_password
DB_NAME=sgfs
CUTOFF_DATE='2012-07-14'

STAT_TRANSACTIONS=0
STAT_ACTIONS=0
STAT_BOOKINGS=0
STAT_PROXIES=0
STAT_FILES=0
STAT_KILLS=0

STAT_ERR_TRANSACTIONS=0
STAT_ERR_ACTIONS=0
STAT_ERR_BOOKINGS=0
STAT_ERR_PROXIES=0
STAT_ERR_FILES=0
STAT_ERR_KILLS=0

# !!! 
# !!! EXEC_FLAG - Set this flag to a non-zero value to apply changes on both DB/FS 
# !!!
EXEC_FLAG=0

# Only [exec|info] command line argument can override the script value 
if [ "${1}" != "" ]; then
  if [ $(echo ${1} | awk '{ print tolower($1) }') = "exec" ]; then
    EXEC_FLAG=1
  else
    EXEC_FLAG=0
  fi
fi

# Only [cutoff] command line argiment can override the script value
if [ "${2}" != "" ]; then
  CUTOFF_DATE=${2}
fi

##
## Main code
##
echo "[i]"
echo "[i] Starting at: "$(date)
echo "[i]"
echo "[i] Cutoff date: $CUTOFF_DATE"
echo "[i]"

if [ $EXEC_FLAG -ne 0 ]; then
  echo "[i] !!!"
  echo "[i] !!! EXEC FLAG is active; the script will make changes on both DB/FS"
  echo "[i] !!!"
else
  echo "[i] ---"
  echo "[i] --- EXEC FLAG is off; no changes will be done on both DB/FS"
  echo "[i] ---"
fi

TRANSACTION_IDS=$(mktemp)
QUERY="select transaction_id from sgfs_transactions where transaction_from < '${CUTOFF_DATE}';"
mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY" > $TRANSACTION_IDS
# Loop over all transaction Ids
while read transaction_id
do
  echo "[i] Processing transaction_id: '${transaction_id}'"

  # Each transaction performs several bookings and actions
  ACTION_IDS=$(mktemp)
  QUERY="select action_id from sgfs_actions where transaction_id=${transaction_id};"
  mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY" > $ACTION_IDS
  # Loop over all action Ids
  while  read action_id
  do
    echo "[i]   Processing action_id: '${action_id}'"
    BOOKING_IDS=$(mktemp)
    QUERY="select booking_id from sgfs_bookings where action_id=${action_id} and transaction_id=${transaction_id};"
    mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY" > $BOOKING_IDS
    # Loop over all booking Ids
    while read booking_id
    do
      echo "[i]     Processing booking_id: '${booking_id}'"
      # Check and eventually kill an active downloading process
      QUERY="select download_pid from sgfs_bookings where booking_id=${booking_id}"
      download_pid=$(mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY")
      # check for real lcg- process
      downlad_cmd=$(cat /proc/${downloas_pid}/cmdline | grep "lcg-")
      if [ -d /proc/$download_pid -a "${downlad_cmd}" != "" ]; then
        if [ $EXEC_FLAG -ne 0 ]; then
          kill -9 ${download_pid}
          RES=$?
          if [ $RES -ne 0 ]; then
            echo "ERROR: Unable to kill download_pid: "$download_pid
            STAT_ERR_KILLS=$((STAT_ERR_KILLS+1))
          fi
        else
          echo "    COMMAND: kill -9 ${download_pid}"
        fi
        STAT_KILLS=$((STAT_KILLS+1))
      fi
      # Remove booking entry
      QUERY="delete from sgfs_bookings where booking_id=${booking_id};"
      if [ $EXEC_FLAG -ne 0 ]; then
        mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY" > /dev/null
        RES=$?
        if [ $RES -ne 0 ]; then
          echo "ERROR: Unable to remove booking_id: "$booking_id
          STAT_ERR_BOOKINGS=$((STAT_ERR_BOOKINGS+1))
        fi
      else
        echo "    QUERY: "$QUERY
      fi
      STAT_BOOKINGS=$((STAT_BOOKINGS+1))
    done < $BOOKING_IDS
    rm -f $BOOKING_IDS
    # Remove action files 
    QUERY="select file_name from sgfs_actions where action_id=${action_id} and transaction_id=${transaction_id};"
    file_name=$(mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY") 
    echo "[i]   File Name: '${file_name}'"
    if [ -d "${file_name}" -o -f "${file_name}" ]
    then
      if [ $EXEC_FLAG -ne 0 ]; then
        rm -rf ${file_name}
        RES=$?
        if [ $RES -ne 0 ]; then
          echo "ERROR: Unable to remove file_name: "$file_name
          STAT_ERR_FILES=$((STAT_ERR_FILES+1))
        fi
      else
        echo "  COMMAND: rm -rf ${file_name}"
      fi
      STAT_FILES=$((STAT_FILES+1))
    fi
    # Remove action entry
    QUERY="delete from sgfs_actions where action_id=${action_id};"
    if [ $EXEC_FLAG -ne 0 ]; then
      mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY" > /dev/null
      RES=$?
      if [ $RES -ne 0 ]; then
        echo "ERROR: Unable to remove action_id: "$action_id
        STAT_ERR_ACTIONS=$((STAT_ERR_ACTIONS+1))
      fi
    else
      echo "  QUERY: "$QUERY
    fi
    STAT_ACTIONS=$((STAT_ACTIONS+1))
  done < $ACTION_IDS
  rm -f $ACTION_IDS
  # Remove transactio_id proxy file
  QUERY="select transaction_proxy from sgfs_transactions where transaction_id=${transaction_id};"
  proxy_file=$(mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY")
  echo "[i] Proxy file: '${proxy_file}'"
  if [ -f "${proxy_file}" ]
  then
    if [ $EXEC_FLAG -ne 0 ]; then
      rm -f ${proxy_file}
      RES=$?
      if [ $RES -ne 0 ]; then
        echo "ERROR: Unable to remove proxy_file: "$proxy_file
        STAT_ERR_PROXIES=$((STAT_ERR_PROXIES+1))
      fi
    else
      echo "COMMAND: rm -f ${proxy_file}"
    fi
    STAT_PROXIES=$((STAT_PROXIES+1))
  fi
  # Remove transaction entry
  QUERY="delete from sgfs_transactions where transaction_id=${transaction_id};"
  if [ $EXEC_FLAG -ne 0 ]; then
    mysql -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -s -N -e "$QUERY" > /dev/null
    RES=$?
    if [ $RES -ne 0 ]; then
      echo "ERROR: Unable to remove transaction_id: "$transaction_id
      STAT_ERR_TRANSACTIONS=$((STAT_ERR_TRANSACTIONS+1))
    fi
  else
    echo "QUERY: "$QUERY
  fi
  STAT_TRANSACTIONS=$((STAT_TRANSACTIONS+1))
done < $TRANSACTION_IDS
rm -f $TRANSACTION_IDS

echo "[i]  "
echo "[i] Statistics ..."
echo "[i]  "
echo "[i] Number of deleted bookinngs   : "$STAT_TRANSACTIONS
echo "[i] Number of deleted actions     : "$STAT_ACTIONS
echo "[i] Number of deleted transactions: "$STAT_BOOKINGS
echo "[i] Number of deleted proxies     : "$STAT_PROXIES
echo "[i] Number of deleted files       : "$STAT_FILES
echo "[i] Number of killed  processes   : "$STAT_KILLS
echo "[i]  "
echo "[i] Errors ..."
echo "[i] "
echo "[i] Number of errored bookinngs   : "$STAT_ERR_TRANSACTIONS
echo "[i] Number of errored actions     : "$STAT_ERR_ACTIONS
echo "[i] Number of errored transactions: "$STAT_ERR_BOOKINGS
echo "[i] Number of errored proxies     : "$STAT_ERR_PROXIES
echo "[i] Number of errored files       : "$STAT_ERR_FILES
echo "[i] Number of errored processes   : "$STAT_ERR_KILLS
echo "[i]  "
echo "[i] done at: "$(date)

