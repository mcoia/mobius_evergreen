#!/usr/bin/perl
use lib qw(../);
use strict;
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
#use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use XML::Simple;
use Getopt::Long;

my $configFile=0;
my $logFile=0;
my $xmlconf = "/openils/conf/opensrf.xml";
our $debug=0;

GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"debug" => \$debug,
)
or die("Error in command line arguments\nYou can specify
--logfile logfilename (required)
--xmlconfig  pathto_opensrf.xml
--debug flag
\n");

if(! -e $xmlconf)
{
    print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
    exit 0;
}
 if(!$logFile)
 {
    print "Please specify a log file\n";
    exit;
 }

    our $mobUtil = new Mobiusutil();
    our $log;
    our $dbHandler;
    our @updatetypes = ('newitems','statusitems','recentreturned','last14daytopcirc','shelvinglocation');

    # These are the 4 types:
    # Newly cataloged items (regardless of age of bib)  (newitems)
    # Newly cataloged items of a copy status (regardless of age of bib)  (statusitems copystatus1,copystatus2)
    # Recently returned (last 100 items returned)   (recentreturned)
    # Last 14 days, top 100 circulated titles (last14daytopcirc)
    # Newest items on shelving locations (shelvinglocation shelfname1,shelfname2,shelfname3,...,...)


my $dt = DateTime->now(time_zone => "local");
my $fdate = $dt->ymd;
my $ftime = $dt->hms;
my $dateString = "$fdate $ftime";
$log = new Loghandler($logFile);
$log->truncFile("");
$log->addLogLine(" ---------------- Script Starting ---------------- ");

my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});

my $query = "SELECT
ID,
(SELECT PARENT_OU FROM ACTOR.ORG_UNIT WHERE ID=(SELECT HOME_OU FROM ACTOR.USR WHERE ID=A.OWNER)),
SPLIT_PART(DESCRIPTION,\$\$ \$\$,1),
regexp_replace(DESCRIPTION,\$\$^[^\\s]*\\s\$\$,\$\$\$\$) FROM CONTAINER.BIBLIO_RECORD_ENTRY_BUCKET A
WHERE SPLIT_PART(DESCRIPTION,\$\$ \$\$,1) IN(";
foreach(@updatetypes){ $query.="\$\$$_\$\$,";}
$query = substr($query,0,-1).")";

$log->addLine($query);
my @results = @{$dbHandler->query($query)};
foreach(@results)
{
    my $row = $_;
    my @row = @{$row};
    my $bucketID=@row[0];
    my $scope=@row[1];
    my $des=@row[2];
    my $des2=@row[3];
    my @args=();
    @args = split(',',$des2) if( length($des2)>1);
    my $ous = getOUs($scope);
    my $inserts="";
    if($des eq 'newitems')
    {
        $inserts = updatebagNewItems($bucketID,$ous);
    }
    elsif($des eq 'statusitems')
    {
        $inserts = updatebagStatusItems($bucketID,$ous,\@args);
    }
    elsif($des eq 'recentreturned')
    {
        $inserts = updatebagRecentReturned($bucketID,$ous);
    }
    elsif($des eq 'last14daytopcirc')
    {
        $inserts = updatebag14daytopcirc($bucketID,$ous);
    }
    elsif($des eq 'shelvinglocation')
    {
        $inserts = updatebagshelvinglocation($bucketID,$ous,\@args);
    }
    if(length($inserts) > 0)
    {
        $log->addLine("refreshing $bucketID");
        $inserts = substr($inserts,0,-1);
        $query = "DELETE FROM CONTAINER.BIBLIO_RECORD_ENTRY_BUCKET_ITEM WHERE BUCKET=$bucketID";
        #$log->addLine($query);
        $dbHandler->update($query);
        $query = "INSERT INTO CONTAINER.BIBLIO_RECORD_ENTRY_BUCKET_ITEM (BUCKET,TARGET_BIBLIO_RECORD_ENTRY)
        VALUES
        $inserts";
        #$log->addLine($query);
        $dbHandler->update($query);
    }
}


$log->addLogLine(" ---------------- Script End ---------------- ");

sub updatebagNewItems
{
    my $id = @_[0];
    my $ous = @_[1];

my $query = "
SELECT * FROM
(
 SELECT DISTINCT B.\"REC\",
 MAX(B.\"IDATE\") \"THEDATE\"
  FROM
(
SELECT (SELECT RECORD FROM ASSET.CALL_NUMBER WHERE ID=A.CALL_NUMBER AND RECORD>0 AND RECORD IS NOT NULL
-- REMOVE SERIALS
AND RECORD IN(SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE
    ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE OWNING_LIB IN($ous))
    AND
    marc !~ \$\$<leader>.......[bs]\$\$
)

) \"REC\",ACTIVE_DATE::DATE \"IDATE\" FROM ASSET.COPY  A WHERE CIRC_LIB IN($ous)
AND LOCATION IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE OWNING_LIB IN($ous) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE AND ID != -1::BIGINT
ORDER BY
ACTIVE_DATE::DATE DESC LIMIT 300
) AS B
GROUP BY B.\"REC\"
) AS C
where C.\"THEDATE\" IS NOT NULL
AND C.\"REC\" IS NOT NULL
ORDER BY C.\"THEDATE\" DESC
LIMIT 200
";

    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $inserts = "";
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        if(length($mobUtil->trim(@row[0])) >0)
        {
            $inserts.="($id,".@row[0]."),";
        }
    }
    return $inserts;
}

sub updatebagStatusItems
{
    my $id = @_[0];
    my $ous = @_[1];
    my @statuses = @{@_[2]};
    my $statuslistforquery = '';
    foreach(@statuses){ $statuslistforquery.="'$_',";}
    #remove final comma
    chop($statuslistforquery);
my $query = "
SELECT * FROM
(
 SELECT DISTINCT B.\"REC\",
 MAX(B.\"IDATE\") \"THEDATE\"
  FROM
(
SELECT (SELECT RECORD FROM ASSET.CALL_NUMBER WHERE ID=A.CALL_NUMBER AND RECORD>0 AND RECORD IS NOT NULL
-- REMOVE SERIALS
AND RECORD IN(SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE
    ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE OWNING_LIB IN($ous))
    AND
    marc !~ \$\$<leader>.......[bs]\$\$
)

) \"REC\",CREATE_DATE::DATE \"IDATE\" FROM ASSET.COPY A JOIN CONFIG.COPY_STATUS AS S ON A.STATUS = S.ID WHERE S.name in ($statuslistforquery) AND CIRC_LIB IN($ous)
AND LOCATION IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE OWNING_LIB IN($ous))
AND A.ID != -1::BIGINT

ORDER BY
CREATE_DATE::DATE DESC LIMIT 300
) AS B
GROUP BY B.\"REC\"
) AS C
where C.\"THEDATE\" IS NOT NULL
AND C.\"REC\" IS NOT NULL
ORDER BY C.\"THEDATE\" DESC
LIMIT 200
";

    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $inserts = "";
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        if(length($mobUtil->trim(@row[0])) >0)
        {
            $inserts.="($id,".@row[0]."),";
        }
    }
    return $inserts;
}

sub updatebagRecentReturned
{
    my $id = @_[0];
    my $ous = @_[1];
    my $query = "
    SELECT DISTINCT \"REC\" FROM
(
SELECT (SELECT RECORD FROM ASSET.CALL_NUMBER WHERE RECORD>0 AND RECORD IS NOT NULL AND
ID=(SELECT CALL_NUMBER FROM ASSET.COPY WHERE ID=A.TARGET_COPY
AND LOCATION IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE OWNING_LIB IN($ous) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE AND ID != -1::BIGINT
)) \"REC\",
CHECKIN_SCAN_TIME::DATE FROM ACTION.CIRCULATION  A
WHERE CIRC_LIB IN($ous) AND
CHECKIN_SCAN_TIME IS NOT NULL AND
TARGET_COPY IN(SELECT ID FROM ASSET.COPY WHERE CALL_NUMBER IN(SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD>0 AND RECORD IS NOT NULL
-- REMOVE SERIALS
AND RECORD IN(SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE
    ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE OWNING_LIB IN($ous))
    AND
    marc !~ \$\$<leader>.......[bs]\$\$
)
)
)
ORDER BY
CHECKIN_SCAN_TIME::DATE DESC LIMIT 200
) AS B
ORDER BY \"REC\" DESC
LIMIT 200";

    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $inserts = "";
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        if(length($mobUtil->trim(@row[0])) >0)
        {
            $inserts.="($id,".@row[0]."),";
        }
    }
    return $inserts;
}


sub updatebag14daytopcirc
{
    my $id = @_[0];
    my $ous = @_[1];
    my $query = "
     SELECT DISTINCT \"REC\",COUNT(*) FROM
 (
SELECT (SELECT RECORD FROM ASSET.CALL_NUMBER WHERE ID=(SELECT CALL_NUMBER FROM ASSET.COPY WHERE ID=A.TARGET_COPY) AND RECORD>0 AND RECORD IS NOT NULL) \"REC\"
,XACT_START::DATE
FROM ACTION.CIRCULATION  A
WHERE CIRC_LIB IN($ous) AND
(TARGET_COPY IN(SELECT ID FROM ASSET.COPY WHERE LOCATION IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE OWNING_LIB IN($ous) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE AND ID != -1::BIGINT
-- REMOVE SERIALS
AND CALL_NUMBER IN(
SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD
IN(SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE
    ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE OWNING_LIB IN($ous))
    AND
    marc !~ \$\$<leader>.......[bs]\$\$
)
)

)) AND
XACT_START > NOW() - \$\$14 DAYS\$\$::INTERVAL
) AS B
GROUP BY \"REC\"
ORDER BY COUNT(*) DESC
LIMIT 100
";

    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $inserts = "";
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        if(length($mobUtil->trim(@row[0])) >0)
        {
            $inserts.="($id,".@row[0]."),";
        }
    }
    return $inserts;
}


sub updatebagshelvinglocation
{
    my $id = @_[0];
    my $ous = @_[1];
    my @shelves = @{@_[2]};
    my $query = "
     SELECT * FROM
(
 SELECT DISTINCT B.\"REC\",
 MAX(B.\"IDATE\") \"THEDATE\"
  FROM
(
SELECT (SELECT RECORD FROM ASSET.CALL_NUMBER WHERE ID=A.CALL_NUMBER AND RECORD>0 AND RECORD IS NOT NULL
-- REMOVE SERIALS
AND RECORD IN(SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE
    ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE OWNING_LIB IN($ous))
    AND
    marc !~ \$\$<leader>.......[bs]\$\$
)

) \"REC\",ACTIVE_DATE::DATE \"IDATE\" FROM ASSET.COPY  A WHERE CIRC_LIB IN($ous)
AND LOCATION IN(SELECT ID FROM ASSET.COPY_LOCATION WHERE OWNING_LIB IN($ous) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE AND NAME IN( \$\$theshelvinglocationlist\$\$ )) AND OPAC_VISIBLE AND HOLDABLE AND CIRCULATE AND ID != -1::BIGINT
ORDER BY
ACTIVE_DATE::DATE DESC LIMIT 300
) AS B
GROUP BY B.\"REC\"
) AS C
where C.\"THEDATE\" IS NOT NULL
AND C.\"REC\" IS NOT NULL
ORDER BY C.\"THEDATE\" DESC
LIMIT 200
";

    my $shelflistforquery = '';
    foreach(@shelves){ $shelflistforquery.="$_\$\$,\$\$";}
    $shelflistforquery = substr($shelflistforquery,0,-5);
    $query =~s/theshelvinglocationlist/$shelflistforquery/g;


    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $inserts = "";
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        if(length($mobUtil->trim(@row[0])) >0)
        {
            $inserts.="($id,".@row[0]."),";
        }
    }
    return $inserts;
}

sub getOUs
{
    my $ret = '';
    my $parentou = @_[0];
    my $query = "SELECT ACTOR.ORG_UNIT_DESCENDANTS($parentou)";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $temp = substr(@row[0],0,index(@row[0],','));
        $temp = substr($temp,1);
        $ret.="$temp,";
        #$log->addLine($ret);
    }
    return $ret.'1';

}

sub gatherDB
{
    my $templateID = @_[0];
    my $date = @_[1];
    my $query = "SELECT array_to_string(array_accum(coalesce(data, '')),'') FROM action_trigger.event_output where id in (select template_output from action_trigger.event where event_def = $templateID AND run_time::date = '$date');";
    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $data="<?xml version='1.0' encoding='UTF-8'?>\n<file type='notice'>".@row[0]."</file>";
        parseXML($data);
    }
}

sub figureDateString
{
    my $daysback=@_[0];
    my $dt = DateTime->now;   # Stores current date and time as datetime object
    my $target = $dt->subtract(days=>$daysback);
    my @ret=($target->ymd,$target->mdy);
    return \@ret;
}

sub getDBconnects
{
    my $openilsfile = @_[0];
    my $xml = new XML::Simple;
    my $data = $xml->XMLin($openilsfile);
    my %conf;
    $conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
    $conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
    $conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
    $conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
    $conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
    ##print Dumper(\%conf);
    return \%conf;

}


exit;