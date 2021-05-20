#!/usr/bin/env bash
DEF_MIMER_DATA_DIR=/data
DEF_MIMER_DATABASE=mimerdb

# set up a SIGTERM handler to stop MimerSQL gracefully
cleanup()
{
  echo "Container is stopping, shuting down Mimer SQL"
  mimcontrol -t ${MIMER_DATABASE}
  exit 0
}

trap "cleanup" INT TERM

# Config and start server according to the environmental variable values
config_and_start_mimer()
{
  if [ ! -e ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs ]; then
    mimcontrol -g ${MIMER_DATABASE}
  fi
  if [ "${MIMER_TCP_PORT}" = "" ]; then
    MIMER_TCP_PORT=1360
  fi
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs TCPPort ${MIMER_TCP_PORT}
  if [ "${MIMER_MAX_DBFILES}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Databanks ${MIMER_MAX_DBFILES}
  fi
  if [ "${MIMER_MAX_USERS}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Users ${MIMER_MAX_USERS}
  fi
  if [ "${MIMER_MAX_TABLES}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Tables ${MIMER_MAX_TABLES}
  fi
  if [ "${MIMER_MAX_TRANS}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs ActTrans ${MIMER_MAX_TRANS}
  fi
  if [ "${MIMER_BUFFERPOOL_SIZE}" != "" ]; then
      mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Pages4K $((${MIMER_BUFFERPOOL_SIZE}/2/4))
      mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Pages32K $((${MIMER_BUFFERPOOL_SIZE}/3/32))
      mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Pages128K $((${MIMER_BUFFERPOOL_SIZE}/6/128))
  else
    if [ "${MIMER_PAGES_4K}" != "" ]; then
      mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Pages4K ${MIMER_PAGES_4K}
    fi
    if [ "${MIMER_PAGES_32K}" != "" ]; then
      mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Pages32K ${MIMER_PAGES_32K}
    fi
    if [ "${MIMER_PAGES_128K}" != "" ]; then
      mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs Pages128K ${MIMER_PAGES_128K}
    fi
  fi

  if [ "${MIMER_REQUEST_THREADS}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs RequestThreads ${MIMER_REQUEST_THREADS}
  fi
  if [ "${MIMER_BACKGROUND_THREADS}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs BackgroundThreads ${MIMER_BACKGROUND_THREADS}
  fi
  if [ "${MIMER_TC_FLUSH_THREADS}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs TcFlushThreads ${MIMER_TC_FLUSH_THREADS}
  fi
  if [ "${MIMER_BG_PRIORITY}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs BackgroundPriority ${MIMER_BG_PRIORITY}
  fi


  if [ "${MIMER_INIT_SQLPOOL}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs SQLPool ${MIMER_INIT_SQLPOOL}
  fi
  if [ "${MIMER_MAX_SQLPOOL}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs MaxSQLPool ${MIMER_MAX_SQLPOOL}
  fi


  if [ "${MIMER_DELAYED_COMMIT}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs DelayedCommit ${MIMER_DELAYED_COMMIT}
  fi
  if [ "${MIMER_DELAYED_COMMIT_TIMEOUT}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs DelayedCommitTimeout ${MIMER_DELAYED_COMMIT_TIMEOUT}
  fi
  if [ "${MIMER_GROUP_COMMIT_TIMEOUT}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs GroupCommitTimeout ${MIMER_GROUP_COMMIT_TIMEOUT}
  fi
  if [ "${MIMER_NETWORK_ENCRYPTION}" != "" ]; then
    mimchval ${MIMER_DATA_DIR}/${MIMER_DATABASE}/multidefs NetworkEncryption ${MIMER_NETWORK_ENCRYPTION}
  fi

  echo "Starting database..."
  mimcontrol -s ${MIMER_DATABASE}
}



#Get environment variables and set default values if they are not set
if [ "${MIMER_DATA_DIR}" = "" ]; 
then
  MIMER_DATA_DIR=${DEF_MIMER_DATA_DIR}
fi

if [ "${MIMER_DATABASE}" = "" ]; 
then
  MIMER_DATABASE=${DEF_MIMER_DATABASE}
fi

#Create Mimer database directory if it doesn't exist
if [ ! -e ${MIMER_DATA_DIR}/${MIMER_DATABASE} ];
then
  mkdir -p ${MIMER_DATA_DIR}/${MIMER_DATABASE}
fi

#Check if there is a database in MIMER_DATA_DIR
if [ ! -e ${MIMER_DATA_DIR}/${MIMER_DATABASE} -o ! -e ${MIMER_DATA_DIR}/${MIMER_DATABASE}/sysdb110.dbf ];
then
  CREATE_DATABASE=1
else
  CREATE_DATABASE=0
fi

if [ "${MIMER_SYSADM_PASSWORD}" = "" -a $CREATE_DATABASE = 1 ];
then
  #Generate a new SYSADM password and print it
  SYSADM_PWD=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w12 | head -n1)
  echo "Mimer SQL SYSADM password is generated since none was specified with -e MIMER_SYSADM_PASSWORD=<password>"
else
  SYSADM_PWD=${MIMER_SYSADM_PASSWORD}
fi


# Install license. If one is specified in MIMER_KEY then that is used.
# The key in MIMER_KEY is saved so it will be used in future start of the container
# If MIMER_KEY haven't been specified we look in $MIMER_DATA_DIR/key.mcfg
if [ "${MIMER_KEY}" != "" ];
then
  echo ${MIMER_KEY} > ${MIMER_DATA_DIR}/${MIMER_DATABASE}/my_mimerkey.mcfg
fi

if [ -e ${MIMER_DATA_DIR}/${MIMER_DATABASE}/my_mimerkey.mcfg ];
then
  echo "Install Mimer SQL license"
  MY_KEY=`cat ${MIMER_DATA_DIR}/${MIMER_DATABASE}/my_mimerkey.mcfg`
  mimlicense -n -a ${MY_KEY}
  # report the license status
  mimlicense -c
elif [ -e ${MIMER_DATA_DIR}/key.mcfg ];
then
  echo "Install Mimer SQL license from ${MIMER_DATA_DIR}/key.mcfg"
  mimlicense -n -f ${MIMER_DATA_DIR}/key.mcfg
  # report the license status
  mimlicense -c
fi

#Register the database. We don't want to run "dbinstall" since that starts the database
mimsqlhosts -a -t local ${MIMER_DATABASE} ${MIMER_DATA_DIR}/${MIMER_DATABASE}
mimsqlhosts -d ${MIMER_DATABASE}
#Create the database if it doesn't exist, otherwise start it
if [ $CREATE_DATABASE = 1 ]; 
then
  # create a new, empty database
  echo "Creating a new Mimer SQL database ${MIMER_DATABASE}"
  sdbgen -p ${SYSADM_PWD} ${MIMER_DATABASE}
  config_and_start_mimer
  #Check if a initialization SQL file was specified
  if [ "${MIMER_INIT_FILE}" != "" ];
  then
    echo "Running SQL init script"
    bsql -uSYSADM -p${SYSADM_PWD} < ${MIMER_INIT_FILE}
  fi
else
  # start Mimer SQL
  echo "Starting existing Mimer SQL database ${MIMER_DATABASE}"
  config_and_start_mimer
fi

if [ $CREATE_DATABASE = 1 -a "${MIMER_SYSADM_PASSWORD}" = "" ]; 
then
  echo "Mimer SQL SYSADM password is: ${SYSADM_PWD}" 
  echo "Remember this password since it cannot be recovered later"
fi

# Wait forever
while true
do
  tail -f /dev/null & wait ${!}
done
