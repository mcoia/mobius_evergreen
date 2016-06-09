#!/usr/bin/perl

use lib qw(../);
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;
use Encode;
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use OpenILS::Application::AppUtils;


our $mobUtil = new Mobiusutil();
our $log;


our %allcomps;
our $unique = new Loghandler("/mnt/evergreen/tmp/unique.csv");
our $duplicates = new Loghandler("/mnt/evergreen/tmp/duplicates.csv");

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
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

	$log = new Loghandler($logFile);
	$log->truncFile('');
	$log->addLogLine(" ---------------- Script Starting ---------------- ");
	
	$unique->truncFile('');
	$duplicates->truncFile('');
	
	my @files = ("/mnt/evergreen/tmp/CSL_ALL.txt");
	$log->addLogLine("Reading files");
	foreach(@files)
	{
		my $file = new Loghandler($_);
		my @lines = @{$file->readFile()};
		foreach(@lines)
		{
			my $line = $_;
			if( $line =~ m/([^,]*)/ )
			{
				my $key = $1;
				# $log->addLine("First number:".$1." second: ".$2);
				if($allcomps{$key})
				{	
					push($allcomps{$key}, $line);
				}
				else
				{
					my @temp = ($line);
					$allcomps{$key} = \@temp;
				}
			}
		}
		$log->addLine("Read $count from ". $file->getFileName());
	}
	#$log->addLine(Dumper(\@allcomps));
	
	my $progress = 1;
	my $uniqueoutput = '';
	my $duplicateoutput = '';
	while ((my $key, my $lines) = each(%allcomps))
	{
		my @lines = @{$lines};
		if($#lines>0)
		{
			foreach(@lines)
			{
				$duplicateoutput.=$_;
				
			}
		}
		else
		{
			foreach(@lines)
			{
				$uniqueoutput.=$_;
			}
		}
		$progress++;
		#if($progress>50){last;}
	}
	$duplicates->appendLine($duplicateoutput);
	$unique->appendLine($uniqueoutput);
	
	$log->addLogLine(" ---------------- Script End ---------------- ");



exit;