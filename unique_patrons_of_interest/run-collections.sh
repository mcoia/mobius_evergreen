#!/bin/bash

DATE=$(date +%Y-%m-%d);
DIR="collections-$DATE"
declare -a LIBS=(SYS1)
USRNAME="yourusername"
PASSWD="yourpassword"
REMOTE_HOST="usernccardinal@example.com:incoming/"

mkdir -p output/$DIR

for lib in "${LIBS[@]}"; do
    ./run-calls.pl /openils/conf/opensrf_core.xml $USRNAME $PASSWD $lib
    mv data-$lib output/$DIR/
done;

tar cvf output/$DIR.tar output/$DIR
gzip output/$DIR.tar

echo "scping $DIR.tar.gz...";
scp output/$DIR.tar.gz $REMOTE_HOST
