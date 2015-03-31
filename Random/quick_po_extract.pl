#!/usr/bin/perl
# 
# evergreen_bib_extract.pl
#
# Usage:
# ./evergreen_bib_extract.pl conf_file.conf [adds or cancels]
# 

 use lib qw(../);
 use strict; 
 use Loghandler;
 use Mobiusutil;
 use DBhandler;
 use evergreenScraper;
 use Data::Dumper;
 use email;
 use DateTime;
 use utf8;
 use Encode;
 use DateTime::Format::Duration;
 use MARC::Record;
 use MARC::File;
 use MARC::File::XML (BinaryEncoding => 'utf8');
 use MARC::File::USMARC;
  use MARC::Batch;

   #If you have weird control fields...
   
    # use MARC::Field;
	# my @files = ('/tmp/temp/evergreen_tempmarc1011.mrc');

    # my $batch = MARC::Batch->new( 'USMARC', @files );
    # while ( my $marc = $batch->next ) {
        # print $marc->subfield(245,"a"), "\n";
		# print $marc->subfield(901,"a"), "\n";
    # }
	# exit;
 my $barcodeCharacterAllowedInEmail=2000;
		 
 my $configFile = @ARGV[0];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }
 
 my $mobUtil = new Mobiusutil(); 
 my $conf = $mobUtil->readConfFile($configFile);
 
 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{
		my $log = new Loghandler($conf->{"logfile"});
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("dbhost","db","dbuser","dbpass","port","fileprefix","marcoutdir","school","alwaysemail","fromemail","queryfile","platform","pathtothis","maxdbconnections");
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
	
	my $query = "select purchase_order,edi from acq.edi_message where purchase_order in(
27,
67,
68,
71,
72,
78,
80,
82,
84,
85,
86,
88,
94,
107,
108,
109,
128,
129,
134,
145,
155,
164,
168,
173,
194,
197,
198,
201
) 
and message_type='ORDERS'";
		
			my $dbHandler;
			my $failString = "Success";
			
			 eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
			 if ($@) {
				$log->addLogLine("Could not establish a connection to the database");
				$failString = "Could not establish a connection to the database";
			 }
			 if($valid)
			 {

				my @results = @{$dbHandler->query($query)};
				foreach(@results)
				{
					my $row = $_;
					my @row = @{$row};
					my $id = @row[0];
					my $edi = @row[1];
					my $logfile = $mobUtil->chooseNewFileName('/mnt/evergreen/tmp/test',"po$id","edi");
					my $writer = new Loghandler($logfile);
					$writer->appendLine($edi);
				}
			}			
		 }
		 $log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
 }
 
 
 sub dirtrav
{
	my @files = @{@_[0]};
	my $pwd = @_[1];
	opendir(DIR,"$pwd") or die "Cannot open $pwd\n";
	my @thisdir = readdir(DIR);
	closedir(DIR);
	foreach my $file (@thisdir) 
	{
		print "$file\n";
		if(($file ne ".") and ($file ne ".."))
		{
			if (-d "$pwd/$file")
			{
				push(@files, "$pwd/$file");
				@files = @{dirtrav(\@files,"$pwd/$file")};
			}
			elsif (-f "$pwd/$file")
			{
				push(@files, "$pwd/$file");
			}
		}
	}
	return \@files;
}

 
 exit;