#!/usr/bin/env bash
# Copyright (c) 2017 Mimer Information Technology

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
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
  echo "=========================================================="
  echo "Mimer SQL SYSADM password is: ${SYSADM_PWD}" 
  echo "Remember this password since it cannot be recovered later"
  echo "=========================================================="
fi

USE_MIMER_CONTROLLER=`echo ${MIMER_REST_CONTROLLER} | tr '[A-Z]' '[a-z]'`
if [ "$USE_MIMER_CONTROLLER" = "true" ]; 
then
  export MIMER_REST_CONTROLLER_AUTH_FILE=${MIMER_DATA_DIR}/htpasswd

  #If we have created a new database, create a new .htpasswd as well
  if [ $CREATE_DATABASE = 1 ];
  then
    rm -f  ${MIMER_REST_CONTROLLER_AUTH_FILE}
  fi
  if [ ! -e ${MIMER_REST_CONTROLLER_AUTH_FILE} ];
  then

    if [ "${MIMER_REST_CONTROLLER_USER}" = "" ];
    then
      REST_USER=mimadmin
      echo ""
      echo "Mimer SQL REST Controller default user \"mimadmin\" is used since none was specified with -e MIMER_REST_CONTROLLER_USER=<user>"
    else
      REST_USER=${MIMER_REST_CONTROLLER_USER}
    fi

    if [ "${MIMER_REST_CONTROLLER_PASSWORD}" = "" ];
    then
      #Generate a new password and print it
      REST_PWD=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w12 | head -n1)
      echo ""
      echo "Mimer SQL REST Controller password is generated since none was specified with -e MIMER_REST_CONTROLLER_PASSWORD=<password>"
      echo ""
      echo "Generated password: ${REST_PWD}"
      echo "Remember the password, it cannot be recovered, but a new one can be created with \"htpasswd -c ${MIMER_REST_CONTROLLER_AUTH_FILE}\""
    else
      REST_PWD=${MIMER_REST_CONTROLLER_PASSWORD}
    fi

    #Create a .htpasswd file for Mimer SQL Rest controller authtentication
    htpasswd -bc ${MIMER_REST_CONTROLLER_AUTH_FILE} ${REST_USER} ${REST_PWD}

    #Copy the cert.pem and key.pem to ${MIMER_DATA_DIR}/.cert.pem and .key.pem
    if [ ! -e ${MIMER_DATA_DIR}/cert.pem ]; then
      cp mimer_controller/cert.pem ${MIMER_DATA_DIR}/cert.pem 
    fi
    if [ ! -e ${MIMER_DATA_DIR}/key.pem ]; then
      cp mimer_controller/key.pem ${MIMER_DATA_DIR}/key.pem 
    fi
  fi

  if [ "${MIMER_REST_CONTROLLER_PORT}" != "" ];
  then
    MIMER_CONTROLLER_PORT=${MIMER_REST_CONTROLLER_PORT}
  else
    MIMER_CONTROLLER_PORT=5001
  fi
  export FLASK_APP=mimer_controller
  export FLASK_ENV=development
  export FLASK_DEBUG=0
  cd mimer_controller
  echo ""
  echo "Starting Mimer SQL REST controller on port ${MIMER_CONTROLLER_PORT}"
  if [ "${MIMER_REST_CONTROLLER_USE_HTTP}" = "true" ]; then
    python3 -m gunicorn.app.wsgiapp -b 0.0.0.0:${MIMER_CONTROLLER_PORT} --daemon --access-logfile ${MIMER_DATA_DIR}/mimer_controller_access_log_${MIMER_DATABASE}.log --error-logfile ${MIMER_DATA_DIR}/mimer_controller_error_log_${MIMER_DATABASE}.log mimer_controller:app
  else
    python3 -m gunicorn.app.wsgiapp -b 0.0.0.0:${MIMER_CONTROLLER_PORT} --daemon --keyfile ${MIMER_DATA_DIR}/key.pem --certfile ${MIMER_DATA_DIR}/cert.pem --access-logfile ${MIMER_DATA_DIR}/mimer_controller_access_log_${MIMER_DATABASE}.log --error-logfile ${MIMER_DATA_DIR}/mimer_controller_error_log_${MIMER_DATABASE}.log mimer_controller:app &
  fi
fi

echo "Container started"
# Wait forever
while true
do
  tail -f /dev/null & wait ${!}
done
