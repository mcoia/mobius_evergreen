#!/bin/bash

fromemail="no-reply@domain.com"
alertemails="alertemail@domain.com"
dbschema="mymig"
profileid="86"
importdir="/mnt/evergreen/_uploads/student_patron_import_files"
logfile="test.log"


ps -a | grep -v grep | grep patron_create > /dev/null
result=$?
# echo "exit code: ${result}"
if [ "${result}" -eq "0" ] ; then
    echo "script is already running"
else

    if [ -z "$(ls -A $importdir)" ]; then
       echo "No files to process"
    else
        fileslist=""
        for filename in "$importdir"/*.csv ; do
            filename="${filename// /\\ }"
            fileslist+=$filename" "
        done
        
        # echo "$fileslist"
        ./patron_create.pl \
        --directory $importdir \
        --schema $dbschema \
        --profileid $profileid \
        --fromemail $fromemail \
        --toemail $alertemails \
        --logfile $logfile
        
        rm -f $importdir/*.csv
        
    fi

fi