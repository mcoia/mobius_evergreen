#!/bin/bash
###
# parameter descriptions:
#       -B n            number of days back for the run where 0 is todays run

###
#
# call_notices_14.sh script is used to call the notices_14.sh script - passing desired parameters to determine
#       which site to build notices for. The parameters available to pass are:
#       -B n            this is number of days back for the run where 0 is todays run
#       -S hmmpl        this is shortname of site to create notices for
#       -D hmmpl        this is the directory to copy output to
#       -F hmmpl-overdue-14day        this is file name chunk to make unique file with
#       -G              this parameter if supplied results in creating the xml file by calling genereate_circ_notices_14.sh
#       -T              this is debug (test) to genereate debugging output
while getopts "B:H" optname
  do
    case "$optname" in
      "B")
        DAYSBACK=$OPTARG
        ;;
      "H")
          echo "###############################################################################################################"
          echo "#"
          echo "# call_notices_14.sh requires the following parameter: "
          echo "#"
          echo "#       -B n   (where n is the days back value) "
          echo "#"
          echo "###############################################################################################################"
          exit
        ;;
      "?")
        echo "Unknown option $OPTARG"
        ;;
      ":")
        echo "No argument value for option $OPTARG"
        exit
        ;;
      *)
      # Should not occur
        echo "Unknown error while processing options"
        ;;
    esac
  done

TESTVAL=${DAYSBACK:?Error DAYSBACK is not defined or is empty}
#    NOTE: the -G param causes the creation of the xml file by calling generate_circ_notices.pl
#          This only needs to be called on first execuition of notices_14.sh - all others can use the 
#          file created from the first call

# Create Notices for PBPL - Poplar Bluff 
./notices_14.sh -B $DAYSBACK -S pbpl -D pbpl -F pbpl-overdue-14day -G -T

# Create Notices for Grundy County
./notices_14.sh -B $DAYSBACK -S gcjn -D gcjn -F gcjn-overdue-14day -T

# Create Notices for Marshall Public Library
./notices_14.sh -B $DAYSBACK -S mpl -D mpl -F mpl-overdue-14day -T

# Create Notices for Carrollton Public Library
./notices_14.sh -B $DAYSBACK -S cpl -D cpl -F cpl-overdue-14day -T

# Create Notices for Lebanon
./notices_14.sh -B $DAYSBACK -S leb -D leb -F leb-overdue-14day -T

# Create Notices for Stone county
./notices_14.sh -B $DAYSBACK -S scc -D scc -F scc-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S scg -D scg -F scg-overdue-14day -T

# Create Notices for Webster County
./notices_14.sh -B $DAYSBACK -S wcm -D wcm -F wcm-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S wcf -D wcf -F wcf-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S wcr -D wcr -F wcr-overdue-14day -T

# Create Notices for Doniphan-Ripley
./notices_14.sh -B $DAYSBACK -S drd -D drd -F drd-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S drn -D drn -F drn-overdue-14day -T

# Create Notices for Albany-Carnegie
./notices_14.sh -B $DAYSBACK -S acpl -D acpl -F acpl-overdue-14day -T

# Create notices for Howard County
./notices_14.sh -B $DAYSBACK -S hoco -D hoco -F hoco-overdue-14day -T

###
# Calcualte the DATE so we can send the corresponding XML_FILE to UMS 
#DAYS_BACK=0
#DATE=$(date -d"-$DAYS_BACK day" +%Y-%m-%d);
#XML_FILE="/mnt/evergreen/circ_notices/overdue-$DATE.xml";
# send notices to UMS
#scp $XML_FILE home.unique-mgmt.com:~/
#scp -v $XML_FILE opensrf@sftp.unique-mgmt.com:incoming/
#exit;
