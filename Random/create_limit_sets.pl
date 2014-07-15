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
		my @numbers = (1,2,3,4,5,6,7,8,10,12,25,30,50,75);
		my @circmods = ();
		my $query = "select code from config.circ_modifier";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			push @circmods,@row[0];
			$log->addLine("reading ".@row[0]);
		}
		foreach(@numbers)
		{
			my $number = $_;
			foreach(@circmods)
			{
				my $circmod = $_;
				my $name = $number." ".$circmod;
				$query = "insert into config.circ_limit_set(name,owning_lib,items_out,description) 
				values('$name',1,$number,'$name')";
				$log->addLine($query);
				$dbHandler->update($query);
				$query = "select max(id) from config.circ_limit_set";
				@results = @{$dbHandler->query($query)};
				my $limitID=-1;
				foreach(@results)
				{
					my $row = $_;
					my @row = @{$row};
					$limitID=@row[0];
				}
				$log->addLine($limitID);
				$query = "insert into config.circ_limit_set_circ_mod_map (limit_set,circ_mod)
				values($limitID,'$circmod')";
				$log->addLine($query);
				$dbHandler->update($query);
				
			}
		}
		$log->addLogLine("finished!");
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

