#!/usr/bin/perl
#1198396
use lib qw(../);
use MARC::Record;
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use DateTime;
use utf8;
use Encode;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;

 my $configFile = @ARGV[0];
 our $jobid=-1;
 $jobid = @ARGV[1];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }
 if(!$jobid)
 {
	print "Please specify a job number to reverse\n";
	exit;
 }

 my $mobUtil = new Mobiusutil(); 
 my $conf = $mobUtil->readConfFile($configFile);
 

 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		my $log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("dbhost","db","dbuser","dbpass","port","logfile"); 
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
		
		if($valid)
		{	
			my $dbHandler;
			eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
			if ($@) 
			{
				$log->addLogLine("Could not establish a connection to the database");					
				$valid = 0;
				$errorMessage = "Could not establish a connection to the database";
			}
			if($valid)
			{	
				my $query = "SELECT * FROM molib2go.job WHERE ID= $jobid";
				my @results = @{$dbHandler->query($query)};
				my $found=0;
				foreach(@results)
				{
					print "Found job id\n";
					$found=1;
				}
				if($found)
				{
					
					print "Running undeleteBiblioRecordEntry\n";
					undeleteBiblioRecordEntry($log,$dbHandler);					
					print "Running moveItemsBack\n";
					moveItemsBack($log,$dbHandler);
					print "Running reDedupe\n";
					reDedupe($log,$dbHandler);					
					print "Running reverseMARCchanges\n";
					reverseMARCchanges($log,$dbHandler);
					print "Running deleteNewRecords\n";
					deleteNewRecords($log,$dbHandler);
				}
				else
				{
					print "Could not find the job in molib2go.job";
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


sub deleteNewRecords
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $query = "DELETE FROM BIBLIO.RECORD_ENTRY WHERE ID IN(SELECT record from molib2go.bib_marc_update WHERE job=$jobid and new_record IS TRUE);";
	$log->addLine($query);
	$dbHandler->update($query);
}

sub moveItemsBack
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $query = "
	update asset.call_number set record=a.oldleadbib
	from molib2go.undedupe a
	where asset.call_number.id=a.moved_call_number and
	asset.call_number.record=a.undeletedbib and
	a.job=$jobid
	;";
	$log->addLine($query);
	$dbHandler->update($query);
	$query = "select copy,prev_bib,target_bib from molib2go.item_reassignment where job=$jobid";
	my @results = @{$dbHandler->query($query)};		
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};		
		my $thiscopy = @row[0];
		my $prev_bib = @row[1];
		my $target_bib = @row[2];
		$query = "update asset.call_number set record=$prev_bib where record=$target_bib and id in(select call_number from asset.copy where id=$thiscopy);";
		$log->addLine($query);
		$dbHandler->update($query);
	}
}

sub reDedupe
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $query = "update biblio.record_entry set deleted=true where id in(select undeletedbib from molib2go.undedupe where job=$jobid);";
	$log->addLine($query);
	$dbHandler->update($query);
}

sub undeleteBiblioRecordEntry
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	
	my $query = "select id,tcn_value from biblio.record_entry where id in(select subbib from molib2go.bib_merge where job=$jobid) and deleted is true;";
	my @results = @{$dbHandler->query($query)};
		
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};		
		my $thisbib = @row[0];
		my $tcn_value = @row[1];
		my $count=1;
		#make sure that when we undelete it, it will not collide its tcn_value 
		while($count>0)
		{
			$query = "select count(*) from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$ and id != $thisbib and deleted is false;";
			$log->addLine($query);
			my @results = @{$dbHandler->query($query)};
			foreach(@results)
			{	
				my $row = $_;
				my @row = @{$row};
				$count=@row[0];
			}
			$tcn_value.="_";
		}
		#take the last tail off
		$tcn_value=substr($tcn_value,0,-1);
		#finally, undelete the bib making it available for the asset.call_number
		$query = "update biblio.record_entry set deleted=false,tcn_source=\$\$reversed\$\$,tcn_value = \$\$$tcn_value\$\$  where id=$thisbib;";
		$log->addLine($query);
		$dbHandler->update($query);
	}
	
}

sub reverseMARCchanges
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $query = "select record,prev_marc,changed_marc from molib2go.bib_marc_update where job=$jobid and new_record is false;";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my @row = @{$_};
		my $bibid = @row[0];
		removeOldCallNumberURI($bibid,$dbHandler,$log);
	}
	$query = "UPDATE BIBLIO.RECORD_ENTRY SET MARC=A.PREV_MARC
	FROM molib2go.bib_marc_update A WHERE A.RECORD=BIBLIO.RECORD_ENTRY.ID AND
	A.job=$jobid and A.new_record is false;";
	$log->addLine($query);
	$dbHandler->update($query);
}
	

sub removeOldCallNumberURI
{
	my $bibid = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE record = $bibid AND label = '##URI##'
	);
	";
	$dbHandler->update($query);
	$log->addLine($query);
	$query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE  record = $bibid AND label = '##URI##'
	);";
	$dbHandler->update($query);
	$log->addLine($query);
	$query = "
	DELETE FROM asset.uri WHERE id not in
	(
		SELECT uri FROM asset.uri_call_number_map
	);";
	$dbHandler->update($query);
	$log->addLine($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = '##URI##';
	";
	$dbHandler->update($query);
	$log->addLine($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = '##URI##';
	";
	$dbHandler->update($query);
	$log->addLine($query);
}

 exit;

 
 