#!/usr/bin/perl
use lib qw(../);
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use DateTime;
use utf8;
use Encode;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use XML::Simple;
use Unicode::Normalize;
use Getopt::Long;



	our $mobUtil = new Mobiusutil();  
	my $xmlconf = "/openils/conf/opensrf.xml";
	our $log;
	our $dbHandler;
	our $jobid=-1;
	our %queries;
	our $baseTemp = "/mnt/evergreen/tmp";
	our @writeMARC = ();

	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
	my $file = "/mnt/evergreen/migration/jeffco/data/checkouts.txt";
	$log = new Loghandler("/mnt/evergreen/migration/jeffco/log/checkouts_extract.log");
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");
	
	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	
	$dbHandler->update("DROP TABLE IF EXISTS m_jeffco.checkouts");
	my $query = "CREATE TABLE m_jeffco.checkouts (userid TEXT, item_barcode TEXT, checkout_date TEXT, due_date TEXT)";
	$dbHandler->update($query);
	my $query = "INSERT INTO m_jeffco.checkouts (userid,item_barcode,checkout_date,due_date)
	VALUES
	";
	my $rfile = new Loghandler($file);
	my @lines = @{$rfile->readFile()};
	my $onblock = 0;
	my $patronidline;
	my $patron;
	foreach(@lines)
	{
		my $line = $_;
		if($onblock) #ignore it if we are not in a block
		{
			if($patronidline) # first line after .block
			{
				my @split = split(' ',$line);
				$patron = @split[$#split]; #the last token
				$patronidline = 0;
				$log->addLine("got $patron");
			}
			else
			{
				if($line =~ m/\.endblock/)  #ending patron block
				{
					$onblock = 0;
				}
				else  # we are in a checkout line
				{
					my @split = split(' ',$line);
					pop @split; #price
					my $duedate = pop @split;
					my $checkoutdate = pop @split;
					my $item = @split[0];
					$query.="(\$\$$patron\$\$,\$\$$item\$\$,\$\$$checkoutdate\$\$,\$\$$duedate\$\$),\n";
				}
				
			}
		}
		else
		{
			if($line =~ m/\.block/)  #starting a new patron block
			{
				$onblock = 1;
				$patronidline = 1;
			}
		}
	}
	$query = substr($query,0,-2);
	$log->addLine($query);
	$dbHandler->update($query);
	
	
	my $afterProcess = DateTime->now(time_zone => "local");
	my $difference = $afterProcess - $dt;
	my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
	my $duration =  $format->format_duration($difference);
	
	$log->addLogLine("Duration: $duration");
	$log->addLogLine(" ---------------- Script Ending ---------------- ");
	

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

 
 