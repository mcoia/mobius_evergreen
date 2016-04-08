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

	my $recordCount=1;
	
	my $loops=0;
	while ( $recordCount>0 )
	{
		my $query = "select 
		count(*) from biblio.record_entry where
		 tcn_source~'ebsco-public-library-collection-script'
		and marc!~'referringurl\\?intendedurl'
		";
		my @results = @{$dbHandler->query($query)};	
		
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$recordCount=@row[0];
			$log->addLine(@row[0]." remaining,$loops loops");
		}
		my $query = "update biblio.record_entry
set marc=

regexp_replace(marc,'(.*)(<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">)(.*)(<subfield code=\"u\">)([^<]*)(.*)','\\1\\2\\3\\4/eg/opac/referringurl?intendedurl=\\5&amp;authtype=url,uid\\6')

where 
id in(select id from biblio.record_entry where 
tcn_source~'ebsco-public-library-collection-script'
and marc!~'referringurl\\?intendedurl' limit 100
)";
		#$log->addLine($query);	
		my $logoutput="";
		$dbHandler->update($query);
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