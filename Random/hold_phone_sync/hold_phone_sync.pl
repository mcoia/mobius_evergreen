#!/usr/bin/perl


# Author: Blake Graham-Henderson
# This will update the action.hold_request.phone_notify column with the best phone number from
# actor.usr only when the updated time stamp on the user account is newer than the hold request

# ./hold_phone_sync.pl log.log
#

#use strict; use warnings;

use lib qw(../);
use LWP;
use Data::Dumper;
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;

my $logfile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";
 

if(@ARGV[1])
{
	$xmlconf = @ARGV[1];
}

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script\n";
	exit 0;
}
 if(!$logfile)
 {
	print "Please specify a log file\n";
	print "usage: ./hold_phone_sync.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
	exit;
 }

my $log = new Loghandler($logfile);
#$log->deleteFile();
$log->addLogLine(" ---------------- Script Starting ---------------- ");

my %conf = %{getDBconnects($xmlconf,$log)};
my @reqs = ("dbhost","db","dbuser","dbpass","port"); 
my $valid = 1;
for my $i (0..$#reqs)
{
	if(!$conf{@reqs[$i]})
	{
		$log->addLogLine("Required configuration missing from conf file");
		$log->addLogLine(@reqs[$i]." required");
		$valid = 0;
	}
}
if($valid)
{	
	my $dbHandler;
	
	eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
	if ($@) 
	{
		$log->addLogLine("Could not establish a connection to the database");
		print "Could not establish a connection to the database";
	}
	else
	{
		my $mobutil = new Mobiusutil();
		my $query = "
				select ahr.id,au.usrname,ahr.phone_notify, au.day_phone,au.evening_phone,au.other_phone,
			COALESCE(
			case length(btrim(regexp_replace(au.day_phone,'\\D','','g')))
			when 0 then null else au.day_phone end ,
			case length(btrim(regexp_replace(au.evening_phone,'\\D','','g')))
			when 0 then null else au.evening_phone end ,
			case length(btrim(regexp_replace(au.other_phone,'\\D','','g')))
			when 0 then null else au.other_phone end ,
			null),
			au.last_update_time,ahr.request_time from action.hold_request ahr, actor.usr au
			where
			au.id=ahr.usr and
			au.last_update_time > ahr.request_time and
			ahr.capture_time is null and
			ahr.cancel_time is null and
			length(btrim(regexp_replace(ahr.phone_notify,'\\D','','g')))>0
			and ahr.phone_notify !=COALESCE(
			case length(btrim(regexp_replace(au.day_phone,'\\D','','g')))
			when 0 then null else au.day_phone end ,
			case length(btrim(regexp_replace(au.evening_phone,'\\D','','g')))
			when 0 then null else au.evening_phone end ,
			case length(btrim(regexp_replace(au.other_phone,'\\D','','g')))
			when 0 then null else au.other_phone end ,
			null)";

			my @results = @{$dbHandler->query($query)};	
			if($#results>-1)
			{
				$log->addLine("ahr.id\tau.usrname\tahr.phone_notify\tau.day_phone\tau.evening_phone\tau.other_phone\tnew phone number\tau.last_update_time\tahr.request_time");
			}
			foreach(@results)
			{
				my $row = $_;
				my @row = @{$row};
				my $line="";
				foreach(@row)
				{
					$line.=$_."\t";
				}
				$log->addLine($line);
			}
		my $query = "
update action.hold_request ahr
set phone_notify=
COALESCE(
case length(btrim(regexp_replace(au.day_phone,'\\D','','g')))
when 0 then null else au.day_phone end ,
case length(btrim(regexp_replace(au.evening_phone,'\\D','','g')))
when 0 then null else au.evening_phone end ,
case length(btrim(regexp_replace(au.other_phone,'\\D','','g')))
when 0 then null else au.other_phone end ,
null)
from
actor.usr au
where
au.id=ahr.usr and
au.last_update_time > ahr.request_time and
ahr.capture_time is null and
ahr.cancel_time is null and
length(btrim(regexp_replace(ahr.phone_notify,'\\D','','g')))>0
and ahr.phone_notify !=COALESCE(
case length(btrim(regexp_replace(au.day_phone,'\\D','','g')))
when 0 then null else au.day_phone end ,
case length(btrim(regexp_replace(au.evening_phone,'\\D','','g')))
when 0 then null else au.evening_phone end ,
case length(btrim(regexp_replace(au.other_phone,'\\D','','g')))
when 0 then null else au.other_phone end ,
null)";
		my $results = $dbHandler->update($query);
		$log->addLogLine("Phone Sync results: $results");
		
	}
}


$log->addLogLine(" ---------------- Script Ending ---------------- ");

sub getDBconnects
{
	my $openilsfile = @_[0];
	my $log = @_[1];
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($openilsfile);
	my %conf;
	$conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
	$conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
	$conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
	$conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
	$conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
	#print Dumper(\%conf);
	return \%conf;

}
