#!/usr/bin/perl
# 
# Example Configure file:
# 
# logfile = /tmp/log.log
# csvout = /tmp/run/marc_to_tab_extract.csv

 use strict; 
 use Loghandler;
 use Mobiusutil;
 use DBhandler;
 use Data::Dumper;
 use DateTime;
 use utf8;
 use Encode;
 use DateTime::Format::Duration;
 use XML::Simple;
 
 #use warnings;
 #use diagnostics; 
 
 #
 #  REPORT PATH:
 #  TEMPLATEID/REPORTID/SCHEDULEID
our @deletedPaths = ();
our $root='';
our $log;
our $dbHandler;
our $deleteSize = 0;
our @notDeleted = ();
our @deleted = ();
our $ageToDelete=365;
my $xmlconf = "/openils/conf/opensrf.xml";
our $configFile;

GetOptions (
"config=s" => \$configFile,
"xmlconfig=s" => \$xmlconf
)
or die("Error in command line arguments\nYou can specify
--config configfilename (required)
--xmlconfig  pathto_opensrf.xml\n");

 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }
 
 if(! -e $xmlconf)
{
	print "xml config file not found: $xmlconf\nYou can specify the path in the second argument\n";
	exit 0;
}

 our $mobUtil = new Mobiusutil(); 
 our $conf = $mobUtil->readConfFile($configFile);
 
 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{		
		$log = new Loghandler($conf->{"logfile"});
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		if($conf{"age_to_delete"})
		{
			$ageToDelete = $conf{"age_to_delete"});
		}
		my %dbconf = %{getDBconnects($xmlconf)};
		$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});		
		
		my @files = ();
		$root = $conf->{"reportroot"};
		@files = @{dirtrav(\@files,$root)};
		
		foreach(@deleted)
		{
			
		}
		$log->addLine(Dumper(\@notDeleted));
		$log->addLine(Dumper(\@deleted));
		$log->addLine("Space cleaned: $deleteSize");
		 
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile' and 'marcoutdir'\n";
		
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
		if(($file ne ".") and ($file ne ".."))
		{
			my $test = "$pwd/$file";
			$test =~ s/$root\///g;			
			my @s = split('/',$test);
			my $template = @s[0];
			my $keepTraversing = 1;
			if($#s == 1)  #template
			{
				my $query = "select * from reporter.template where id=$template";
				my @results = @{$dbHandler->query($query)};
				if($#results==-1)
				{
					my $age = -M "$pwd/$file";
					if($age > $ageToDelete)
					{
						#Delete the files and the directory, it has been deleted in the DB
						#print "$test no longer in the db\n";
						push(@deleted,"$pwd/$file");
					}
				}
				else
				{
					push(@notDeleted, "$pwd/$file");
				}
			}
			if($#s == 3)
			{	
				my $reportid = @s[1];
				my $scheduleid = @s[2];
				my $query = "select * from reporter.schedule where id=$scheduleid";
				my @results = @{$dbHandler->query($query)};
				if($#results==-1)
				{
					my $age = -M "$pwd/$file";
					if($age > $ageToDelete)
					{
						#Delete the files and the directory, it has been deleted in the DB
						#print "$test no longer in the db\n";
						push(@deleted,"$pwd/$file");
					}
				}
				else
				{
					push(@notDeleted, "$pwd/$file");
				}
				
			}
			if (-d "$pwd/$file")
			{
				push(@files, "$pwd/$file");
				if($keepTraversing)
				{
					@files = @{dirtrav(\@files,"$pwd/$file")};
				}
			}
			elsif (-f "$pwd/$file")
			{
				push(@files, "$pwd/$file");
			}
		}
	}
	return \@files;
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