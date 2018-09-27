#!/bin/bash

fromemail="no-reply@domain.com"
alertemails="alertemail@domain.com"
dbschema="mymig"
profileid="86"
importdir="/mnt/evergreen/_uploads/student_patron_import_files"
logfile="test.log"

RESULT=`ps -a | sed -n /patron_create/p`

if [ "${RESULT:-null}" = null ]; then
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

else
    echo "script is already running"
fi