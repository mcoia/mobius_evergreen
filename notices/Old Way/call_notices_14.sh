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

# Create notices for Little Dixie
./notices_14.sh -B $DAYSBACK -S ldxr-hunt -D ldxr -F ldxr-hunt-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S ldxr-mad -D ldxr -F ldxr-mad-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S ldxr-par -D ldxr -F ldxr-par-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S ldxr-mob -D ldxr -F ldxr-mob-overdue-14day -T

# Create Notices for Carthage
./notices_14.sh -B $DAYSBACK -S cgpl -D cgpl -F cgpl-overdue-14day -T

# Create notices for Webb City
./notices_14.sh -B $DAYSBACK -S wbcpl -D wbcpl -F wbcpl-overdue-14day -T

./notices_14.sh -B $DAYSBACK -S srl-bk -D srl_wapl -F srl-bk-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-hr -D srl_wapl -F srl-hr-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-nh -D srl_wapl -F srl-nh-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-ow -D srl_wapl -F srl-ow-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-pc -D srl_wapl -F srl-pc-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-sc -D srl_wapl -F srl-sc-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-un -D srl_wapl -F srl-un-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S srl-wr -D srl_wapl -F srl-wr-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S wpl -D srl_wapl -F wpl-overdue-14day -T

# Create notices for Ozark Regional
./notices_14.sh -B $DAYSBACK -S orl-ir -D orl -F orl-ir-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-an -D orl -F orl-an-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-vi -D orl -F orl-vi-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-fr -D orl -F orl-fr-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-sg -D orl -F orl-sg-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-st -D orl -F orl-st-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-cu -D orl -F orl-cu-overdue-14day -T
./notices_14.sh -B $DAYSBACK -S orl-bo -D orl -F orl-bo-overdue-14day -T

# Create notices for Caruthersville
./notices_14.sh -B $DAYSBACK -S cvpl -D cvp -F cvpl-overdue-14day -T

# Create notices for Sullivan
./notices_14.sh -B $DAYSBACK -S slvnpl -D slvn -F slvnpl-overdue-14day -T

# Create notices for Sikeston
./notices_14.sh -B $DAYSBACK -S skpl -D skpl -F skpl-overdue-14day -T

# Create notices for Marion
./notices_14.sh -B $DAYSBACK -S mcl -D mcl -F mcl-overdue-14day -T


###
# Calcualte the DATE so we can send the corresponding XML_FILE to UMS 
#DAYS_BACK=0
#DATE=$(date -d"-$DAYS_BACK day" +%Y-%m-%d);
#XML_FILE="/mnt/evergreen/circ_notices/overdue-$DATE.xml";
# send notices to UMS
#scp $XML_FILE home.unique-mgmt.com:~/
#scp -v $XML_FILE opensrf@sftp.unique-mgmt.com:incoming/
#exit;
