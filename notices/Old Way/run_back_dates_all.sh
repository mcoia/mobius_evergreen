#!/bin/bash
#
# You may want to add a -G to the end of each set of these if 
# the files don't alerady exist.
#

GOBACK=5
COUNTER=0
while [ $COUNTER -lt $GOBACK ]; do
  . /etc/profile && cd /openils/notices/scripts/ && ./call_notices.sh -B $COUNTER 
  . /etc/profile && cd /openils/notices/scripts/ && ./call_notices_14.sh -B $COUNTER 
  let COUNTER=COUNTER+1
done
