#!/usr/bin/perl

# These Perl modules are required:

use lib qw(../);
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use DateTime;
use utf8;
use Encode;
use File::stat;
use Cwd;

 my $configFile = @ARGV[0];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

 our $mobUtil = new Mobiusutil(); 
 our $conf = $mobUtil->readConfFile($configFile);
 
 our $jobid=-1;
 our $log;
 our $archivefolder;
 our $importSourceName;
 our $importSourceNameDB;
 our $dbHandler;
 our @shortnames;
 
 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		$log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		
		my $cwd = getcwd();
		print $cwd."\n";
		exit;
		
		my @reqs = ("server","login","password","logfile"); 
		my $valid = 1;
		my $errorMessage="";
		for my $i (0..$#reqs)
		{
			if(!$conf{@reqs[$i]})
			{
				$log->addLogLine("Required configuration missing from conf file");
				$log->addLogLine(@reqs[$i]." required");
				$valid = 0;
			}
		}
		
		my $server = $conf->{server};
		my $login = $conf->{login};
		my $password = $conf->{password};
		
		$log->addLogLine("**********FTP starting -> $server with $login and $password");
	
		
		
				my $ftp = Net::FTP->new($server, Debug => 1, Passive=> 1)
				or die $log->addLogLine("Cannot connect to ".$server);
				$ftp->login($login,$password)
				or die $log->addLogLine("Cannot login ".$ftp->message);
				print "Changing Dir\n";
				$ftp->cwd("'edx.wcs.mornt.ftp'");
				
				my @remotefiles = $ftp->ls();
				foreach(@remotefiles)
				{
					my $filename = $_;
					print "Downloading $filename\n";
					$ftp->binary();
					my $worked = $ftp->get($filename,"/mnt/evergreen/tmp/test/$filename"."_attempt_3");
					if($worked)
					{
						print "Downloaded\n";
					}
					else 
					{
						print "Failed\n";
					}
					$log->addLine($filename);
				}
				$ftp->quit
				or die $log->addLogLine("Unable to close FTP connection");
				$log->addLogLine("**********FTP session closed ***************");
		
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}


 exit;

 
 