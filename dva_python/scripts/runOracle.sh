#!/bin/bash

############# Execute custom scripts ##############
function runUserScripts {

  SCRIPTS_ROOT="$1";

  # Check whether parameter has been passed on
  if [ -z "$SCRIPTS_ROOT" ]; then
    echo "$0: No SCRIPTS_ROOT passed on, no scripts will be run";
    exit 1;
  fi;
  
  # Execute custom provided files (only if directory exists and has files in it)
  if [ -d "$SCRIPTS_ROOT" ] && [ -n "$(ls -A $SCRIPTS_ROOT)" ]; then
      
    echo "";
    echo "Executing user defined scripts"
  
    for f in $SCRIPTS_ROOT/*; do
        case "$f" in
            *.sh)     echo "$0: running $f"; . "$f" ;;
            *.sql)    echo "$0: running $f"; echo "exit" | su -p oracle -c "$ORACLE_HOME/bin/sqlplus / as sysdba @$f"; echo ;;
            *)        echo "$0: ignoring $f" ;;
        esac
        echo "";
    done
    
    echo "DONE: Executing user defined scripts"
    echo "";
  
  fi;
  
}


########### SIGTERM handler ############
function _term() {
   echo "Stopping container."
   echo "SIGTERM received, shutting down database!"
  /etc/init.d/oracle-xe stop
}

########### SIGKILL handler ############
function _kill() {
   echo "SIGKILL received, shutting down database!"
   /etc/init.d/oracle-xe stop
}

############# Create DB ################
function createDB {
   # Auto generate ORACLE PWD if not passed on
   export ORACLE_PWD=${ORACLE_PWD:-"`openssl rand -hex 8`"}
   echo "ORACLE PASSWORD FOR SYS AND SYSTEM: $ORACLE_PWD";

   sed -i -e "s|###ORACLE_PWD###|$ORACLE_PWD|g" $ORACLE_BASE/$CONFIG_RSP && \
   /etc/init.d/oracle-xe configure responseFile=$ORACLE_BASE/$CONFIG_RSP

   # Listener 
   echo "# listener.ora Network Configuration File:
         
         SID_LIST_LISTENER = 
           (SID_LIST =
             (SID_DESC =
               (SID_NAME = PLSExtProc)
               (ORACLE_HOME = $ORACLE_HOME)
               (PROGRAM = extproc)
             )
           )
         
         LISTENER =
           (DESCRIPTION_LIST =
             (DESCRIPTION =
               (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC_FOR_XE))
               (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))
             )
           )
         
         DEFAULT_SERVICE_LISTENER = (XE)" > $ORACLE_HOME/network/admin/listener.ora

# TNS Names.ora
   echo "# tnsnames.ora Network Configuration File:
XE =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = XE)
    )
  )
EXTPROC_CONNECTION_DATA =
  (DESCRIPTION =
     (ADDRESS_LIST =
       (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC_FOR_XE))
     )
     (CONNECT_DATA =
       (SID = PLSExtProc)
       (PRESENTATION = RO)
     )
  )
" > $ORACLE_HOME/network/admin/tnsnames.ora

   su -p oracle -c "sqlplus / as sysdba <<EOF
      EXEC DBMS_XDB.SETLISTENERLOCALACCESS(FALSE);
      
      ALTER DATABASE ADD LOGFILE GROUP 4 ('$ORACLE_BASE/oradata/$ORACLE_SID/redo04.log') SIZE 50m;
      ALTER DATABASE ADD LOGFILE GROUP 5 ('$ORACLE_BASE/oradata/$ORACLE_SID/redo05.log') SIZE 50m;
      ALTER DATABASE ADD LOGFILE GROUP 6 ('$ORACLE_BASE/oradata/$ORACLE_SID/redo06.log') SIZE 50m;
      ALTER SYSTEM SWITCH LOGFILE;
      ALTER SYSTEM SWITCH LOGFILE;
      ALTER SYSTEM CHECKPOINT;
      ALTER DATABASE DROP LOGFILE GROUP 1;
      ALTER DATABASE DROP LOGFILE GROUP 2;
      
      ALTER SYSTEM SET db_recovery_file_dest='';
      exit;
EOF"

  # Move database operational files to oradata
  moveFiles;
}

############# MAIN ################

# Set SIGTERM handler
trap _term SIGTERM

# Set SIGKILL handler
trap _kill SIGKILL

/etc/init.d/oracle-xe start | grep -qc "Oracle Database 11g Express Edition is not configured"
if [ "$?" == "0" ]; then
   # Check whether container has enough memory
   if [ `df -k /dev/shm | tail -n 1 | awk '{print $2}'` -lt 1048576 ]; then
      echo "Error: The container doesn't have enough memory allocated."
      echo "A database XE container needs at least 1 GB of shared memory (/dev/shm)."
      echo "You currently only have $((`df -k /dev/shm | tail -n 1 | awk '{print $2}'`/1024)) MB allocated to the container."
      exit 1;
   fi;
   
   # Create database
   createDB;
   
   # Execute custom provided setup scripts
   runUserScripts $ORACLE_BASE/scripts/setup
fi;

echo "#########################"
echo "DATABASE IS READY TO USE!"
echo "#########################"

# Execute custom provided startup scripts
runUserScripts $ORACLE_BASE/scripts/startup

echo "The following output is now a tail of the alert.log:"
tail -f $ORACLE_BASE/diag/rdbms/*/*/trace/alert*.log &
childPID=$!
wait $childPID

