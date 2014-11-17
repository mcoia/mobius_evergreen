#!/bin/bash
###
# parameter descriptions:
#       -B n            number of days back for the run where 0 is todays run

###
#
# call_notices.sh script is used to call the notices.sh script - passing desired parameters to determine
#       which site to build notices for. The parameters available to pass are:
#       -B n            this is number of days back for the run where 0 is todays run
#       -S hmmpl        this is shortname of site to create notices for
#       -D hmmpl        this is the directory to copy output to
#       -F hmmpl-overdue        this is file name chunk to make unique file with
#       -G              this parameter if supplied results in creating the xml file by calling genereate_circ_notices.sh
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
          echo "# call_notices.sh requires the following parameter: "
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

# Create Notices for PBPL - Poplar Bluff 
#    NOTE: the -G param causes the creation of the xml file by calling generate_circ_notices.pl
#          This only needs to be called on first execuition of notices.sh - all others can use the 
#          file created from the first call
./notices.sh -B $DAYSBACK -S pbpl -D pbpl -F pbpl-overdue -G -T

# Create Notices for Grundy County
./notices.sh -B $DAYSBACK -S gcjn -D gcjn -F gcjn-overdue -T

# Create Notices for Marshall Public Library
./notices.sh -B $DAYSBACK -S mpl -D mpl -F mpl-overdue -T

# Create Notices for Carrollton Public Library
./notices.sh -B $DAYSBACK -S cpl -D cpl -F cpl-overdue -T

# Create Notices for Lebanon
./notices.sh -B $DAYSBACK -S leb -D leb -F leb-overdue -T

# Create Notices for Stone county
./notices.sh -B $DAYSBACK -S scc -D scc -F scc-overdue -T
./notices.sh -B $DAYSBACK -S scg -D scg -F scg-overdue -T

# Create Notices for Webster County
./notices.sh -B $DAYSBACK -S wcm -D wcm -F wcm-overdue -T
./notices.sh -B $DAYSBACK -S wcf -D wcf -F wcf-overdue -T
./notices.sh -B $DAYSBACK -S wcr -D wcr -F wcr-overdue -T

# Create Notices for Doniphan-Ripley
./notices.sh -B $DAYSBACK -S drd -D drd -F drd-overdue -T
./notices.sh -B $DAYSBACK -S drn -D drn -F drn-overdue -T

# Create Notices for Albany-Carnegie
./notices.sh -B $DAYSBACK -S acpl -D acpl -F acpl-overdue -T

# Create Notices for Howard County
./notices.sh -B $DAYSBACK -S hcpl -D hcpl -F hcpl-overdue -T

# Create notices for Little Dixie
./notices.sh -B $DAYSBACK -S ldxr-hunt -D ldxr -F ldxr-hunt-overdue -T
./notices.sh -B $DAYSBACK -S ldxr-mad -D ldxr -F ldxr-mad-overdue -T
./notices.sh -B $DAYSBACK -S ldxr-par -D ldxr -F ldxr-par-overdue -T
./notices.sh -B $DAYSBACK -S ldxr-mob -D ldxr -F ldxr-mob-overdue -T

# Create notices for Carthage
./notices.sh -B $DAYSBACK -S cgpl -D cgpl -F cgpl-overdue -T

# Create notices for Webb City
./notices.sh -B $DAYSBACK -S wbcpl -D wbcpl -F wbcpl-overdue -T

# Create notices for Scenic and Washington
./notices.sh -B $DAYSBACK -S srl-bk -D srl_wapl -F srl-bk-overdue -T
./notices.sh -B $DAYSBACK -S srl-hr -D srl_wapl -F srl-hr-overdue -T
./notices.sh -B $DAYSBACK -S srl-nh -D srl_wapl -F srl-nh-overdue -T
./notices.sh -B $DAYSBACK -S srl-ow -D srl_wapl -F srl-ow-overdue -T
./notices.sh -B $DAYSBACK -S srl-pc -D srl_wapl -F srl-pc-overdue -T
./notices.sh -B $DAYSBACK -S srl-sc -D srl_wapl -F srl-sc-overdue -T
./notices.sh -B $DAYSBACK -S srl-un -D srl_wapl -F srl-un-overdue -T
./notices.sh -B $DAYSBACK -S srl-wr -D srl_wapl -F srl-wr-overdue -T
./notices.sh -B $DAYSBACK -S wpl -D srl_wapl -F wpl-overdue -T

# Create notices for Ozark Regional
./notices.sh -B $DAYSBACK -S orl-ir -D orl -F orl-ir-overdue -T
./notices.sh -B $DAYSBACK -S orl-an -D orl -F orl-an-overdue -T
./notices.sh -B $DAYSBACK -S orl-vi -D orl -F orl-vi-overdue -T
./notices.sh -B $DAYSBACK -S orl-fr -D orl -F orl-fr-overdue -T
./notices.sh -B $DAYSBACK -S orl-sg -D orl -F orl-sg-overdue -T
./notices.sh -B $DAYSBACK -S orl-st -D orl -F orl-st-overdue -T
./notices.sh -B $DAYSBACK -S orl-cu -D orl -F orl-cu-overdue -T
./notices.sh -B $DAYSBACK -S orl-bo -D orl -F orl-bo-overdue -T

# Create notices for Caruthersville
./notices.sh -B $DAYSBACK -S cvpl -D cvp -F cvpl-overdue -T

# Create notices for Sullivan
./notices.sh -B $DAYSBACK -S slvnpl -D slvn -F slvnpl-overdue -T

# Create notices for Sikeston
./notices.sh -B $DAYSBACK -S skpl -D skpl -F skpl-overdue -T

# Create notices for Marion
./notices.sh -B $DAYSBACK -S mcl -D mcl -F mcl-overdue -T


###
# Calcualte the DATE so we can send the corresponding XML_FILE to UMS 
#DAYS_BACK=0
#DATE=$(date -d"-$DAYS_BACK day" +%Y-%m-%d);
#XML_FILE="/mnt/evergreen/circ_notices/overdue-$DATE.xml";
# send notices to UMS
#scp $XML_FILE home.unique-mgmt.com:~/
#scp -v $XML_FILE opensrf@sftp.unique-mgmt.com:incoming/
#exit;
