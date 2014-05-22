#!/usr/bin/perl


# Author: Blake Graham-Henderson

# ./reingestbibs.pl log.log
#

#use strict; use warnings;

use lib qw(../);
use LWP;
use Data::Dumper;
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;
use DateTime; 
use DateTime::Format::Duration;

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
	print "usage: ./reingestbibs.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
	exit;
 }

my $log = new Loghandler($logfile);
$log->deleteFile();
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
		my $updatecount=0;
		my $originalSetting = getOriginalSetting($dbHandler);
		my $previousTime=DateTime->now;
	# remove the reingest flag to make this faster	
		setReingest($dbHandler,"false");
	
		my $query = "
		select id,tcn_value,btrim(split_part(split_part(marc,'<controlfield tag=\"001\">',2),'<',1)) from biblio.record_entry where tcn_value in
(select btrim(split_part(split_part(marc,'<controlfield tag=\"001\">',2),'<',1)) from biblio.record_entry where
split_part(split_part(marc,'<controlfield tag=\"001\">',2),'<',1)!=tcn_value and deleted is false and marc ~ '<controlfield tag=\"001\">'
)";
		$log->addLogLine("Executing this query which will take some time for sure:\n$query");
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		$log->addLogLine("$total bibs to make tcn_value=001 tag");
		my $completed=1;
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $bibID = @row[0];
			my $zero01tag = @row[2];
			my $tcn_value = @row[1];
			my $duration = calcTimeDiff($previousTime);
			my $speed = $completed / $duration;
			my $eta = ($total - $completed) / $speed / 60;
			$eta = substr($eta,0,index($eta,'.')+3);
			$duration = $duration / 60;
			$duration = substr($duration,0,index($duration,'.')+3);			
			$log->addLogLine("Working on $bibID TCN: $zero01tag\t$completed / $total\telapsed/remaining $duration/$eta");
			makeRoomForTCN($dbHandler,$bibID,$zero01tag,$log);
			$query = "update biblio.record_entry set tcn_value=$zero01tag where id=$bibID";
			$dbHandler->update($query);
			$completed++;
		}
		my $previousTime=DateTime->now;
	# now finally reingest like normal	
		$log->addLogLine("Finished cleaning TCN and now moving onto regular triggered reingest");
		#setReingest($dbHandler,"true");	
		setReingest($dbHandler,$originalSetting==1?"true":"false");
		$query = "select id from biblio.record_entry where deleted is false and id>0";
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		my $completed=1;
		$log->addLogLine("$total bibs to reingest");
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $bibID = @row[0];
			my $duration = calcTimeDiff($previousTime);
			my $speed = $completed / $duration;
			my $eta = ($total - $completed) / $speed / 60;
			$eta = substr($eta,0,index($eta,'.')+3);
			$duration = $duration / 60;
			$duration = substr($duration,0,index($duration,'.')+3);
			$log->addLogLine("Ingesting $bibID\t$completed / $total\telapsed/remaining $duration/$eta");
			$query = "update biblio.record_entry set id=id where id=$bibID";
			$dbHandler->update($query);
			$completed++;
		}
		$log->addLogLine("Reingest is finished!");
		setReingest($dbHandler,$originalSetting==1?"true":"false");	
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


sub makeRoomForTCN
{
	my $dbHandler = @_[0];
	my $ignorebibid = @_[1];
	my $tcn_value = @_[2];
	my $log = @_[3];
	my $query = "select id from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$ and id!=$ignorebibid";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};
		my $bib=@row[0];
		my $current=@row[0];
		$log->addLogLine("Clearing $bib with tcn $tcn_value to make room for $ignorebibid");
		correctTCN($dbHandler,$bib,$tcn_value.'_');
	}
}

sub correctTCN
{
	my $dbHandler = @_[0];
	my $bibid = @_[1];
	my $target_tcn = @_[2];
	my $tcn_value = $target_tcn;
	
	my $count=1;			
	#Alter the tcn until it doesn't collide
	while($count>0)
	{
		my $query = "select count(*) from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my $row = $_;
			my @row = @{$row};
			$count=@row[0];
		}
		$tcn_value.="_";
	}
	#take the last tail off
	$tcn_value=substr($tcn_value,0,-1);	
	$query = "update biblio.record_entry tcn_value = \$\$$tcn_value\$\$  where id=$bibid";
	$dbHandler->update($query);
}

sub getOriginalSetting
{
	my $dbHandler = @_[0];
	my $ret = "";
	my $query = "select enabled from config.internal_flag where name = 'ingest.reingest.force_on_same_marc'";
	my @results = @{$dbHandler->query($query)};
	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$ret=@row[0];
	}
	#print "Orginal setting: $ret\n";
	
	return $ret;
}

sub setReingest
{
	my $dbHandler = @_[0];
	my $setting = @_[1];
	my $ret = "";
	#print "setting to: $setting\n";
	my $query = "update config.internal_flag set enabled = $setting where name = 'ingest.reingest.force_on_same_marc'";
	$dbHandler->update($query);
	
}

 sub calcTimeDiff
 {
	my $previousTime = @_[0];
	my $currentTime=DateTime->now;
	my $difference = $currentTime - $previousTime;#
	my $format = DateTime::Format::Duration->new(pattern => '%M');
	my $minutes = $format->format_duration($difference);
	$format = DateTime::Format::Duration->new(pattern => '%S');
	my $seconds = $format->format_duration($difference);
	my $duration = ($minutes * 60) + $seconds;
	if($duration<.1)
	{
		$duration=.1;
	}
	return $duration;
 }