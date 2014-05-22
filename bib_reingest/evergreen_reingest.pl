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
 use threadHandler;
 use Data::Dumper; 
 use DateTime;
 use DateTime::Format::Duration;
 
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
		my @reqs = ("dbhost","db","dbuser","dbpass","port","fileprefix","marcoutdir","school","queryfile","platform","pathtothis","maxdbconnections");
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
			my $pathtothis = $conf{"pathtothis"};
			my $maxdbconnections = $conf{"maxdbconnections"};
			my $queries = $mobUtil->readQueryFile($conf{"queryfile"});
			if($queries)
			{
				my %queries = %{$queries};
				
				my $school = $conf{"school"};
				my $type = @ARGV[1];
				if($type eq "thread")
				{
					thread(\%conf);
				}
				
				my $platform = $conf{"platform"};#ebsco or summon
				
				if(defined($type))		
				{
					if($type eq "adds")
					{
						$valid = 1;
					}
					elsif(($platform eq 'all') && ($type eq "cancels"))
					{
						$valid = 1;
						
					}
					elsif($type eq "cancels")
					{
						$valid = 1;
						
					}
					elsif($type eq "full")
					{
						$valid = 1;
					}
					else
					{
						$valid = 0;
						print "You need to specify the type Example 'full'\n";
					}
				}
				else
				{
					$valid = 0;
					print "You need to specify the type Example 'full'\n";
				}
				if(!defined($platform))
				{
					print "You need to specify the platform in the config file\n";
				}
				my @dbUsers = @{$mobUtil->makeArrayFromComma($conf{"dbuser"})};
				my @dbPasses = @{$mobUtil->makeArrayFromComma($conf{"dbpass"})};
				if(scalar @dbUsers != scalar @dbPasses)
				{
					print "Sorry, you need to provide DB usernames equal to the number of DB passwords\n";
					exit;
				}
				my $dbuser = @dbUsers[0];
				my $dbpass = @dbPasses[0];
				my $remoteDirectory = "/";
			#All inputs are there and we can proceed
				if($valid)
				{
					my $dbHandler;
					my $failString = "Success";
					
					 eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$dbuser,$dbpass,$conf{"port"});};
					 if ($@) {
						$log->addLogLine("Could not establish a connection to the database");
						$failString = "Could not establish a connection to the database";
					 }
					 if($valid)
					 {
						my $selectQuery = $mobUtil->findQuery($dbHandler,$school,$platform,$type,$queries);
						# PREP THE DATABASE
						my $originalSetting = getOriginalSetting($dbHandler);
						# remove the reingest flag to make this faster	
						setReingest($dbHandler,"false");
						prepDBTCN($dbHandler,$log);
						# This ingest cannot be done in threads. So we just have to bite it.
						#ingestBrowseOnly($dbHandler,$selectQuery,$log);
						setReingest($dbHandler,$originalSetting==1?"true":"false");	
						# DONE PREPPING AND NOW STARTING THREADS
						my $dt = DateTime->now(time_zone => "local");
						my $fdate = $dt->ymd;
						my $ftime = $dt->hms;
						my $dateString = "$fdate $ftime";
						my $threadHandler;
						$valid=1;
						
						#print "Path: $pathtothis\n";
						my $gatherTime = DateTime->now(time_zone => "local");
						local $@;
						eval{$threadHandler = new threadHandler($dbHandler,$log,$selectQuery,$type,$conf{"school"},$pathtothis,$configFile,$maxdbconnections);};
						if($@)
						{
							print "Master Thread Failed:\n";
							print $@;
							$valid=0;							
							$log->addLogLine("Reingest Failed. The cron standard output will have more clues.\r\n$selectQuery");
							$failString = "Master Thread Fail/Crash";
						}
						if($valid)
						{
							my $recCount=0;
							my $format = DateTime::Format::Duration->new(
								pattern => '%M:%S' #%e days, %H hours,
							);
							my $gatherTime = calcTimeDiff($gatherTime);
							$gatherTime = $gatherTime / 60;
							my $afterProcess = DateTime->now(time_zone => "local");
							my $difference = $afterProcess - $dt;
							my $duration =  $format->format_duration($difference);
							my $rps;								
							$rps = $threadHandler->getRPS();
							$afterProcess = DateTime->now(time_zone => "local");
							$difference = $afterProcess - $dt;
							$duration =  $format->format_duration($difference);
							$log->addLogLine("$school $platform $type: $recCount Record(s)");
				
							$log->addLogLine("$school $platform $type *ENDING*");
						}
					 }
				 }
			 }
			 
			$log->addLogLine("Reingest is finished!");
			
		 }
		 $log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
 }
 
 sub ingestBrowseOnly
 {
	my $dbHandler = @_[0];
	my $selectQuery = @_[1];
	my $log = @_[2];	
	$selectQuery =~ s/\$recordSearch/ID/gi;
	my @results = @{$dbHandler->query($selectQuery)};
	my $total = $#results+1;
	$log->addLogLine("$total bibs to preform the browse ingest");
	my $previousTime=DateTime->now(time_zone => "local");
	my $completed=1;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibID = @row[0];
		my $duration = calcTimeDiff($previousTime);
		my $speed = $completed / $duration;
		my $eta = ($total - $completed) / $speed / 60;
		$eta = substr($eta,0,index($eta,'.')+3);
		$duration = $duration / 60;
		$duration = substr($duration,0,index($duration,'.')+3);
		$log->addLogLine("Working on $bibID \t$completed / $total\telapsed/remaining $duration/$eta");
		my $query = "SELECT metabib.reingest_metabib_field_entries($bibID, TRUE, FALSE, TRUE)";		
		$dbHandler->update($query);
		$completed++;
	}
 }
 
 sub prepDBTCN
 {
	my $dbHandler = @_[0];
	my $log = @_[1];
	my $previousTime=DateTime->now(time_zone => "local");
	
# This query identifies all of the bibs that have a TCN value of another bib's 001 tag
	my $query = "
	select id,tcn_value,btrim(split_part(split_part(marc,'<controlfield tag=\"001\">',2),'<',1)) from biblio.record_entry where tcn_value in
	(select btrim(split_part(split_part(marc,'<controlfield tag=\"001\">',2),'<',1)) from biblio.record_entry where
	id>0 and split_part(split_part(marc,'<controlfield tag=\"001\">',2),'<',1)!=tcn_value and deleted is false and marc ~ '<controlfield tag=\"001\">'
	)";
	$log->addLogLine("Executing this query which will take some time for sure:\n$query");
	my @results = @{$dbHandler->query($query)};
	my $total = $#results+1;
	$log->addLogLine("$total bibs to make tcn_value=001 tag");
	my $completed=1;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibID = @row[0];
		my $zero01tag = @row[2];
		my $tcn_value = @row[1];
		my $duration = calcTimeDiff($previousTime);
		my $speed = $completed / $duration;
		my $eta = ($total - $completed) / $speed / 60;
		$eta = substr($eta,0,index($eta,'.')+3);
		$duration = $duration / 60;
		$duration = substr($duration,0,index($duration,'.')+3);
		$log->addLogLine("Working on $bibID TCN: $zero01tag\t$completed / $total\telapsed/remaining $duration/$eta");
		makeRoomForTCN($dbHandler,$bibID,$zero01tag,$log);
		$query = "update biblio.record_entry set tcn_value=$zero01tag where id=$bibID";
		$dbHandler->update($query);
		$completed++;
	}
	my $previousTime=DateTime->now(time_zone => "local");
	# now finally reingest like normal	
	#$log->addLogLine("Finished cleaning TCN and now moving onto regular triggered reingest");
	#setReingest($dbHandler,"true");	
 }
 
 sub getOriginalSetting
{
	my $dbHandler = @_[0];
	my $ret = "";
	my $query = "select enabled from config.internal_flag where name = 'ingest.reingest.force_on_same_marc'";
	my @results = @{$dbHandler->query($query)};
	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$ret=@row[0];
	}
	#print "Orginal setting: $ret\n";
	
	return $ret;
}
 
 sub makeRoomForTCN
{
	my $dbHandler = @_[0];
	my $ignorebibid = @_[1];
	my $tcn_value = @_[2];
	my $log = @_[3];
	my $query = "select id from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$ and id!=$ignorebibid";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};
		my $bib=@row[0];
		my $current=@row[0];
		$log->addLogLine("Clearing $bib with tcn $tcn_value to make room for $ignorebibid");
		correctTCN($dbHandler,$bib,$tcn_value.'_');
	}
}

sub correctTCN
{
	my $dbHandler = @_[0];
	my $bibid = @_[1];
	my $target_tcn = @_[2];
	my $tcn_value = $target_tcn;
	
	my $count=1;			
	#Alter the tcn until it doesn't collide
	while($count>0)
	{
		my $query = "select count(*) from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$";
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
	$query = "update biblio.record_entry tcn_value = \$\$$tcn_value\$\$  where id=$bibid";
	$dbHandler->update($query);
}

 sub setReingest
{
	my $dbHandler = @_[0];
	my $setting = @_[1];
	my $ret = "";
	#print "setting to: $setting\n";
	my $query = "update config.internal_flag set enabled = $setting where name = 'ingest.reingest.force_on_same_marc'";
	$dbHandler->update($query);
	
}

 sub calcTimeDiff
 {
	my $previousTime = @_[0];
	my $currentTime=DateTime->now(time_zone => "local");
	my $difference = $currentTime - $previousTime;#
	my $format = DateTime::Format::Duration->new(pattern => '%M');
	my $minutes = $format->format_duration($difference);
	$format = DateTime::Format::Duration->new(pattern => '%S');
	my $seconds = $format->format_duration($difference);
	my $duration = ($minutes * 60) + $seconds;
	if($duration<.1)
	{
		$duration=.1;
	}
	return $duration;
 }
 
 sub thread
 {
	my %conf = %{@_[0]};
	my $previousTime=DateTime->now(time_zone => "local");
	my $rangeWriter = new Loghandler("/tmp/rangepid.pid");
	my $mobUtil = new Mobiusutil();
	my $offset = @ARGV[2];
	my $increment = @ARGV[3];
	my $limit = $increment-$offset;
	my $pid = @ARGV[4];
	my $dbuser = @ARGV[5];
	my $typ = @ARGV[6];
	#print "Type = $typ\n";
	$rangeWriter->addLine("$offset $increment");
	#print "$pid: $offset - $increment $dbuser\n";
	my $dbpass = "";
	my @dbUsers = @{$mobUtil->makeArrayFromComma($conf{"dbuser"})};
	my @dbPasses = @{$mobUtil->makeArrayFromComma($conf{"dbpass"})};	
	my $i=0;
	foreach(@dbUsers)
	{
		if($dbuser eq $_)
		{
			$dbpass=@dbPasses[$i];
		}
		$i++;
	}
	my $pidWriter = new Loghandler($pid);
	my $log = new Loghandler($conf->{"logfile"});
	my $pathtothis = $conf{"pathtothis"};
	my $queries = $mobUtil->readQueryFile($conf{"queryfile"});
	my $school = $conf{"school"};
	my $type = @ARGV[1];
	my $platform = $conf{"platform"};
	my $dbHandler;
	eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$dbuser,$dbpass,$conf{"port"});};
	
	if ($@) {
		$pidWriter->truncFile("none\nnone\nnone\nnone\nnone\nnone\n$dbuser\nnone\n1\n$offset\n$increment");
		$rangeWriter->addLine("$offset $increment DEFUNCT");
		print "******************* I DIED DBHANDLER ********************** $pid\n";
	}
	else
	{
		my $dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$dbuser,$dbpass,$conf{"port"});
		#print "Sending off to get thread query: $school, $platform, $type";
		my $selectQuery = $mobUtil->findQuery($dbHandler,$school,$platform,$typ,$queries);		
		$selectQuery=~s/\$recordSearch/ID/gi;
		$selectQuery.= " AND ID > $offset AND ID <= $increment";
		#print "Thread got this query\n\n$selectQuery\n\n";
		$pidWriter->truncFile("0");	
		#print "Thread started\n offset: $offset\n increment: $increment\n pidfile: $pid\n limit: $limit";
		my $threadHandler;
		local $@;
		#print "Scraping:\n$dbHandler,$log,$selectQuery,$type,".$conf{"school"}.",$pathtothis,$configFile";
		eval{$threadHandler = new threadHandler($dbHandler,$log,$selectQuery,$type,$conf{"school"},$pathtothis,$configFile);};
		if($@)
		{
			print "******************* I DIED SCRAPER ********************** $pid\n";
			print $@;
			$pidWriter->truncFile("none\nnone\nnone\nnone\nnone\nnone\n$dbuser\nnone\n1\n$offset\n$increment");
			$rangeWriter->addLine("$offset $increment DEFUNCT");
			exit;
		}								
		
		my $recordCount = $threadHandler->getRecordCount();
		my $queryTime = $threadHandler->getSpeed();
		my $secondsElapsed = calcTimeDiff($previousTime);
		#print "Writing to thread File:\n$disk\n$recordCount\n$extraInformationOutput\n$couldNotBeCut\n$queryTime\n$limit\n$dbuser\n$secondsElapsed\n";
		my $writeSuccess=0;
		my $trys=0;
		while(!$writeSuccess && $trys<100)
		{
			$writeSuccess = $pidWriter->truncFile("blabla\n$recordCount\nNONE\nNONE\n$queryTime\n$limit\n$dbuser\n$secondsElapsed");
			if(!$writeSuccess)
			{
				print "$pid -  Could not write final thread output, trying again: $trys\n";
			}
			$trys++;
		}
		
	}
	
	exit;
 }
 

 
 exit;