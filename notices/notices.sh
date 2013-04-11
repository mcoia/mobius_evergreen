#!/bin/bash
###
# parameter descriptions:
#       -B n            number of days back for the run where 0 is todays run
#       -S hmmpl        shortname of site to create notices for
#       -D hmmpl        the name of sub-directory to store results in
#       -F hmmpl-overdue        file name chunk to make unique file with
#       -G              this parameter if supplied results in creating the xml file by calling generate_circ_notices.sh
#       -T              debug (test) to generate debugging output
#       -H              display help for module
#

GENERATE=0
DEBUG=0
while getopts "B:S:D:F:GTH" optname
  do
    case "$optname" in
      "B")
        DAYSBACK=$OPTARG
        ;;
      "S")
        SHORTNAME=$OPTARG
        ;;
      "D")
        DIROUT=$OPTARG
        ;;
      "F")
        FILEBASE=$OPTARG
        ;;
      "G")
        GENERATE=1
        ;;
      "T")
        DEBUG=1
        ;;
      "H")
          echo "###############################################################################################################"
          echo "#"
          echo "# notices.sh requires the following parameters: "
          echo "#"
          echo "#       -B n   (where n is the days back value) "
          echo "#       -S xxxxx (where xxxxx is the shortname of the library)"
          echo "#       -D xxxxx (where xxxxx is the sub-directory to store output in)"
          echo "#       -F xxxxx-overdue (the filename chunk to make unique filename with)"
          echo "#       -G (this parameter if specified results in creating the xml file by calling generate_circ_notices.sh)"
          echo "#"
          echo "###############################################################################################################"
          exit
        ;;
      "?")
        echo "Unknown option $OPTARG"
        ;;
      ":")
        echo "No argument value for option $OPTARG"
        ;;
      *)
      # Should not occur
        echo "Unknown error while processing options"
        ;;
    esac
  done

TESTVAL=${DAYSBACK:?Error DAYSBACK is not defined or is empty}
TESTVAL=${SHORTNAME:?Error SHORTNAME is not defined or is empty}
TESTVAL=${DIROUT:?Error DIROUT is not defined or is empty}
TESTVAL=${FILEBASE:?Error FILEBASE is not defined or is empty}

DATE=$(date -d"-$DAYSBACK day" +%Y-%m-%d);

#if [ "$DEBUG" == "1" ] ; then
#   echo "DAYSBACK $DAYSBACK"
#   echo "SHORTNAME $SHORTNAME"
#   echo "DIROUT $DIROUT"
#   echo "FILEBASE $FILEBASE"
#   echo PRINT_FO_FILE="/var/evergreen/notices/$FILEBASE-$DATE.fo";
#   echo PDF_FILE="/var/evergreen/notices/$FILEBASE-$DATE.pdf";
#   echo GENERATE ="$GENERATE"
#fi

XML_FILE="/mnt/evergreen/circ_notices/overdue-$DATE.xml";

. /etc/profile

if [ "$GENERATE" == "1" ] ; then
	PSQL="psql --tuples-only --no-align -U evergreen -h db2";

	SQL="SELECT array_to_string(array_accum(coalesce(data, '')),'') FROM action_trigger.event_output where id in (select template_output from action_trigger.event where event_def = 101 AND run_time::date = '$DATE');";

	# remove first 2 lines and last line of output
	DATA=$(echo $SQL | $PSQL | sed 's/(1 row)//g');

	echo -e "<?xml version='1.0' encoding='UTF-8'?>\n<file type='notice' date='$DATE'>$DATA</file>" > $XML_FILE
fi

#cd /var/evergreen/scripts

PRINT_FO_FILE="/mnt/evergreen/circ_notices/$FILEBASE-$DATE.fo";
PDF_FILE="/mnt/evergreen/circ_notices/$DIROUT/$FILEBASE-$DATE.pdf";
GENDATE=`date +'%d %b %Y'` && xsltproc --stringparam gendate "${GENDATE}" --stringparam lid $SHORTNAME /openils/notices/xsl/multi-site-notice.xsl $XML_FILE > $PRINT_FO_FILE;
fop $PRINT_FO_FILE $PDF_FILE;
#mv $PDF_FILE /mnt/evergreen/circ_notices/$DIROUT/
#cp $PDF_FILE /tmp/
cd /mnt/evergreen/circ_notices/$DIROUT && ./create-index.sh

exit

