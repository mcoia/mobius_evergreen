#!/usr/bin/perl

use lib qw(../);
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;


our $schema;
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $tables;
our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"schema=s" => \$schema,
"tables=s" => \$tables
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--tables tables within the schema (comma separated)
--schema (eg. m_slmpl)
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
if(!$schema)
{
	print "Please specify an Evergreen DB schema to dump the data to\n";
	exit;
}

	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	my @tables = split(',',$tables);
	
	foreach(@tables)
	{
		my $query = "select * from $schema.$_ limit 1";
		extractSummary($_);
	}
	
	$log->addLogLine(" ---------------- Script End ---------------- ");
	
	
sub extractSummary
{
	my $tablename = @_[0];
	
	my $query = "select count(*) from $schema.$tablename";
	my $tablecount = @{@{$dbHandler->query($query)}[0]}[0];
	print "$tablecount rows in $tablename\n";
	
	my $query = "select * from $schema.$tablename limit 1";
	$dbHandler->query($query);
	my @cols = @{$dbHandler->getColumnNames()};
	$log->addLine($query);
	
	foreach(@cols)
	{
		print "Gathering $tablename $_....";
		my $query = "select $_,count(*) from $schema.$tablename group by $_ order by $_";
		$log->addLine($query);
		my $fileout = new Loghandler("$schema.$tablename.$_.out");
		$fileout->truncFile("");
		my @results = @{$dbHandler->query($query)};
		print $#results." different values for $_\n";
		if($#results > ($tablecount / 2))
		{
			print "Just getting a sample from $_ because it has less than 50% unique values\n";
			my $query = "select $_,count(*) from $schema.$tablename group by $_ order by count(*) limit 200";
			$log->addLine($query);
			@results = @{$dbHandler->query($query)};
		}
		my $output="";
		foreach(@results)
		{	
			my @row = @{$_};
			$output.="\"$_\"," for @row;
			$output=substr($output,0,-1)."\n";
		}
		$fileout->addLine($output);
		undef $fileout;
	}
	$log->addLine($query);
	$dbHandler->update($query);
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