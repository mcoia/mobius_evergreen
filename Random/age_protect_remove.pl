#!/usr/bin/perl
use lib qw(../);
use File::Path qw(make_path remove_tree);
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

my $logFile='';
my $xmlconf = "/openils/conf/opensrf.xml";



GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig  pathto_opensrf.xml
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
	
  

	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
	$log = new Loghandler($logFile);
	#$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		
	
	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});		
# 6 Month
	my $recordCount=1;
	
	my $loops=0;
	while ($recordCount>0)
	{
		my $query = "select 
		count(*) from asset.copy 
		where 
		age_protect=2 and 
		active_date < now()-\$\$6 months\$\$::interval";
		my @results = @{$dbHandler->query($query)};	
		
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$log->addLine(@row[0]." remaining,$loops,$recordCount");
		}
		my $query = "select 
		barcode,
		active_date,
		active_date-now(),
		age_protect from asset.copy 
		where 
		age_protect=2 and 
		active_date < now()-\$\$6 months\$\$::interval 
		order by active_date-now() desc limit 10000";
		#$log->addLine($query);	
		my $logoutput="";
		my @results = @{$dbHandler->query($query)};	
		$recordCount=$#results+1;
		if($loops==0 && $recordCount>0)
		{
			$logoutput.="Clearing age protection for these:\nbarcode,active_date,active_date_relative_distance,age_protect\n";
		}
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$logoutput.=$mobUtil->makeCommaFromArray(\@row)."\n";
		}
		if($recordCount>0)
		{
			$query = "update asset.copy set age_protect=null
			where 
			id in(select id from asset.copy 
				where 
				age_protect=2 and 
				active_date < now()-\$\$6 months\$\$::interval 
				order by active_date-now() desc limit 10000)";
			$dbHandler->update($query);
		}
		$log->addLine($logoutput);
		$loops++;
	}
	
# 3 Month
	my $recordCount=1;
	
	my $loops=0;
	while ($recordCount>0)
	{
		my $query = "select 
		count(*) from asset.copy 
		where 
		age_protect=1 and 
		active_date < now()-\$\$3 months\$\$::interval";
		my @results = @{$dbHandler->query($query)};	
		
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$log->addLine(@row[0]." remaining,$loops,$recordCount");
		}
		my $query = "select 
		barcode,
		active_date,
		active_date-now(),
		age_protect from asset.copy 
		where 
		age_protect=1 and 
		active_date < now()-\$\$3 months\$\$::interval 
		order by active_date-now() desc limit 10000";
		#$log->addLine($query);	
		my $logoutput="";
		my @results = @{$dbHandler->query($query)};	
		$recordCount=$#results+1;
		if($loops==0 && $recordCount>0)
		{
			$logoutput.="Clearing age protection for these:\nbarcode,active_date,active_date_relative_distance,age_protect\n";
		}
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$logoutput.=$mobUtil->makeCommaFromArray(\@row)."\n";
		}
		if($recordCount>0)
		{
			$query = "update asset.copy set age_protect=null
			where 
			id in(select id from asset.copy 
				where 
				age_protect=1 and 
				active_date < now()-\$\$3 months\$\$::interval 
				order by active_date-now() desc limit 10000)";
			$dbHandler->update($query);
		}
		$log->addLine($logoutput);
		$loops++;
	}
	$log->addLogLine(" ---------------- Script End ---------------- ");	
	


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