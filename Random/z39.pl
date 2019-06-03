#!/usr/bin/perl


use lib qw(../);
use Data::Dumper;
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;
use DateTime; 
use DateTime::Format::Duration;

my $logfile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";



# Query to get a list of popular UPCS

# select split_part(value,' ',1)||',' from metabib.identifier_field_entry where field=20 and source
# in
# (
# select record from
# (
# select 
# acn.record,count(*)
# from
# asset.call_number acn,
# asset.copy ac,
# action.circulation acirc
# where
# acirc.target_copy=ac.id and
# acn.id=ac.call_number and
# not acn.deleted and
# not ac.deleted
# group by 1
# having count(*) > 140
# ) as b
# limit 100
# )

 
 my $host = 'olc1.ohiolink.edu:210/INNOPAC';
 my $searchType = '1007'; # 4 = title
 my @searchArray = (
 '024543110361',
'733961144772',
'678149191523',
'024543527527',
'025195016674',
'025195004831',
'097368014541',
'053939613926',
'027616861436',
'043396005020',
'043396097452',
'025193328625',
'085391142560',
'025192038365',
'013132154893',
'097363500643',
'012569593503',
'786936213843'
 );

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
	print "usage: ./z39.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
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
        foreach(@searchArray)
        {
            $log->addLine("Searching $_");
            my @res = @{$mobutil->getMarcFromZ3950($host,'@attr 1='.$searchType.' @attr 4=1 @attr 5=1 "'.$_.'"',$log)};
            $log->addLine("No Results") if($#res == -1);
            $log->addLine(Dumper(\@res)) if($#res > -1);
        }
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

