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
 
 #use warnings;
 #use diagnostics; 
 
 #
 #  REPORT PATH:
 #  TEMPLATENUMBER/REPORTID/SCHEDULEID
our @deletedPaths = ();
 my $configFile = shift;
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
		my @files = ();
		@files = @{dirtrav(\@files,$conf->{"reportroot"});
		
		 
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
			print "$pwd/$file\n";
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