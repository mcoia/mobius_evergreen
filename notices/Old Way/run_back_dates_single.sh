#!/bin/bash
#
# You may want to add a -G to the end of each set of these if 
# the files don't alerady exist.
#

GOBACK=14
COUNTER=0
while [ $COUNTER -lt $GOBACK ]; do
  ./notices.sh -B $COUNTER -S wcm -D wcm -F wcm-overdue -T -G 
  ./notices_14.sh -B $COUNTER -S wcm -D wcm -F wcm-overdue-14day -T -G
  let COUNTER=COUNTER+1
done
