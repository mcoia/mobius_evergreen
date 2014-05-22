#!/usr/bin/perl
#
# threadHandler.pm
# Blake Graham-Henderson 
# MOBIUS
# blake@mobiusconsortium.org
# 2014-5-24

package threadHandler;
 use Loghandler;
 use strict; 
 use Data::Dumper;
 use Mobiusutil;
 use Date::Manip;
 use DateTime::Format::Duration; 
 use Time::HiRes;
 
 sub new   
 {
	my $class = shift;
	my %k=();
	my %d=();
	my %e=();
	my %f=();
	my %g=();
	my %h=();
	my $mobutil = new Mobiusutil();
	my $pidfile = new Loghandler($mobutil->chooseNewFileName('/tmp','reingest_pid','pid'));
    my $self = 
	{
		'dbhandler' => shift,
		'log' => shift,
		'bibids' => shift,
		'mobiusutil' => $mobutil,
		'standard' => \%g,
		'selects' => "",
		'querytime' => 0,
		'query' => "",
		'type' => "",		
		'pidfile' => $pidfile,
		'title' => "",
		'rps' => 0,
		'pathtothis' =>"",
		'conffile' =>"",
		'recordcount' => 0,
		'maxdbconnection' =>3
	};
	
	my $t = shift;
	my $title = shift;
	$self->{'pathtothis'} = shift;
	$self->{'conffile'} = shift;
	my $m = shift;
	if($m)
	{
		$self->{'maxdbconnection'} = $m;
	}
	if($title)
	{
		$pidfile = new Loghandler($mobutil->chooseNewFileName('/tmp',"reingest_pid_$title",'pid'));
		$self->{'pidfile'} = $pidfile;
		$self->{'title'} = $title;
	}
	$pidfile->addLine("starting up....");
	#print "4: $t\n";
	if($t)
	{
		$self->{'type'}=$t;
	}
	bless $self, $class;
	figureSelectStatement($self);
	my $max = findMaxRecordCount($self,$self->{'selects'});
	#print "Max calc: $max\n";
	if(($t) && ($t ne 'thread') )
	{
		gatherDataFromDB_spinThread_Controller($self);
	}	
	elsif(($t) && ($t eq 'thread'))
	{
		my $cou = spinThread($self);
		$self->{'recordcount'} = $cou;
	}	
	$pidfile->deleteFile();
    return $self;
 }
 
 sub threadDone
 {
	my $pidFile = @_[0];
	#print "Starting to read pid file\n";
	my $pidReader = new Loghandler($pidFile);
	my @lines = @{ $pidReader->readFile() };
	#print "Done reading Pid\n";
	undef $pidReader;
	if(scalar @lines >1)
	{
		return 1;
	}
	elsif(scalar @lines ==1)
	{
		my $line =@lines[0];
		$line =~ s/\n//;
		if($line eq "0")
		{
			return 0;
		}
		else
		{
			return 1;
		}
	}
	return 0;
 }
 
 sub spinThread
 {
	my $self = @_[0];
	my $mobUtil = $self->{'mobiusutil'};
	my $dbHandler = $self->{'dbhandler'};
	my $log = $self->{'log'};
	my $selects = $self->{'selects'};
	my $pidfile = $self->{'pidfile'};	
	my $previousTime=DateTime->now(time_zone => "local");
	#print "Thread starting\n";
	$self->{'selects'}  = $self->{'bibids'};
	#print "stuffStandardFields\n";
	stuffStandardFields($self);
	
	my $secondsElapsed = calcTimeDiff($self,$previousTime);
	#print "time = $secondsElapsed\n";
	my %standard = %{$self->{'standard'}};
	my $currentRecordCount = scalar keys %standard;
	
	return $currentRecordCount;
 }
  
 sub gatherDataFromDB_spinThread_Controller
 {
	my $self = @_[0];
	#This file is written to by each of the threads to debug the database ID's selected for each thread
	my $rangeWriter = new Loghandler("/tmp/rangepid.pid");
	$rangeWriter->deleteFile();
	my $dbUserMaxConnection=$self->{'maxdbconnection'};
	my $mobUtil = $self->{'mobiusutil'};
	my $dbHandler = $self->{'dbhandler'};
	my $log = $self->{'log'};
	my $selects = $self->{'selects'};
	my $pidfile = $self->{'pidfile'};
	my $pathtothis = $self->{'pathtothis'};
	my $conffile = $self->{'conffile'};
	my $conf = $mobUtil->readConfFile($conffile);
	my %conf = %{$conf};
	my @dbUsers = @{$mobUtil->makeArrayFromComma($conf{"dbuser"})};
	my %dbUserTrack = ();
	my @recovers;
	my $i=0;
	foreach(@dbUsers)
	{
		$dbUserTrack{$_}=0;
		$i++;
	}
	$dbUserTrack{@dbUsers[0]}=1;
	
	my @cha = split("",$selects);
	my $tselects = "";	
	my $chunks = 0;
	my $zeroAdded = 0;
	my $chunkGoal = 100;
	my $title = $self->{'title'};
	my $masterfile = new Loghandler($mobUtil->chooseNewFileName('/tmp',"master_$title",'pid'));
	my $previousTime=DateTime->now(time_zone => "local");
	foreach(@cha)
	{
		$tselects.=$_;
	}
	
	my $query = "SELECT MIN(ID) FROM BIBLIO.RECORD_ENTRY";
	my @results = @{$dbHandler->query($query)};
	my $min = 0;
	my $max = 1;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$min = @row[0];
	}
	$min--;
	#print "Min: $min\n";
	my $maxQuery = $tselects;
	$maxQuery =~ s/\$recordSearch/COUNT(\*)/gi;
	$max = findMaxRecordCount($self,$maxQuery);
		
	my $finishedRecordCount=0;
	my @threadTracker = ();
	my $userCount = scalar @dbUsers;
	#print Dumper(\@dbUsers);
	#print Dumper(\%dbUserTrack);
	#print "$userCount users\n";
	my $threadsAllowed = $dbUserMaxConnection * (scalar @dbUsers);
	my $threadsAlive=1;
	my $offset = $min;
	my $increment = $min+$chunkGoal;
	my $slowestQueryTime = 0;
	my $rps = 0;
	my $range=0;
	my $recordsCollectedTotalPerLoop=0;
	my $recordsCollectedStale=0;
	while($threadsAlive)
	{
		#print "Starting main Thread loop\n";
		my $workingThreads=0;
		my @newThreads=();
		my $threadJustFinished=0;
		#print Dumper(\@threadTracker);
		#print "Looping through the threads\n";
		$recordsCollectedTotalPerLoop = $finishedRecordCount;
		foreach(@threadTracker)
		{	
			#print "Checking to see if thread $_ is done\n";
			my $done = threadDone($_);
			if($done)
			{
				#print "$_ Thread Finished.... Cleaning up\n";
				$threadJustFinished=1;				
				my $pidReader = new Loghandler($_);				
				my @lines = @{ $pidReader->readFile() };				
				$pidReader->deleteFile();
				
				undef $pidReader;
				if(scalar @lines >6)
				{
					@lines[0] =~ s/\n//; # Output marc file location
					@lines[1] =~ s/\n//; # Total Records Gathered
					@lines[2] =~ s/\n//; # $self->{'toobig'} = $extraInformationOutput;
					@lines[3] =~ s/\n//; # $self->{'toobigtocut'}					
					@lines[4] =~ s/\n//; # Slowest Query Time
					@lines[5] =~ s/\n//; # Chunk Size
					@lines[6] =~ s/\n//; # DB Username
					@lines[7] =~ s/\n//; # Execute Time in Seconds
					my $dbuser = @lines[6];
					if(@lines[8])
					{
						@lines[8] =~ s/\n//;
					}
					if(scalar @lines >8 && @lines[8]==1)
					{
						#print "************************ RECOVERING ************************\n";
						
						#print "This thread died, going to restart it\n";
						#This thread failed, we are going to try again (this is usually due to a database connection)
						@lines[9] =~ s/\n//;
						@lines[10] =~ s/\n//;
						#print Dumper(\@lines);
						my $off = @lines[9];
						my $inc = @lines[10];
						my @add = ($off,$inc);
						push(@recovers,[@add]);
						if($dbUserTrack{$dbuser})
						{
							$dbUserTrack{$dbuser}--;
						}
						my $check = new Loghandler(@lines[0]);
						if($check->fileExists())
						{
							#print "Deleting @lines[0]\n";
							$check->deleteFile();
						}
						#print "Done recovering\n";
					}
					else
					{
						#print "Completed thread success, now cleaning\n";
						if(@lines[1] == 0)
						{
							$zeroAdded++;
							$max = findMaxRecordCount($self,$maxQuery);
							print "Got 0 records $zeroAdded times\n";
							if($zeroAdded>100) #we have looped 100 times with not a single record added to the collection. Time to quit.
							{
								$finishedRecordCount = $max;
							}
						}
						else
						{
							$zeroAdded=0;
						}
						
						$dbUserTrack{$dbuser}--;
						if(@lines[1] !=0)
						{
							$self->{'toobig'}.=@lines[2];
							$self->{'toobigtocut'}.=@lines[3];
							if(@lines[7]<1)
							{
								@lines[7]=1;
							}
							my $trps = @lines[1] / @lines[7];
							#print "Performed math\n";
							if($rps < $trps-1)
							{
								$chunkGoal+=100;
							}
							elsif($rps > $trps+3)
							{
								$chunkGoal-=100;
							}
							if($chunkGoal<1)
							{
								$chunkGoal=200;
							}
							#print "Thread time: ".@lines[4]."\n";
							if(@lines[4] > 280)
							{
								$chunkGoal=200;
							}
							$rps = $trps;
							
							$finishedRecordCount += @lines[1];
							#print "Added dump count to total\n";
							#print Dumper(\@dumpedFiles);
						}
					}
				}
				else
				{
					print "For some reason the thread PID file did not output the expected stuff\n";
				}
				#print "Looping back through the rest of running threads\n";
			}
			else
			{
				#print "Thread not finished, adding it to \"running\"\n";
				$workingThreads++;
				push(@newThreads,$_);
			}
		}
		@threadTracker=@newThreads;
		#print "$workingThreads / $threadsAllowed Threads\n";
		
		#Figure out if total collected records is the same as last time
		#Count the number of times that the number of collected records are the same
		if($finishedRecordCount==$recordsCollectedTotalPerLoop)
		{
			$recordsCollectedStale++;
		}
		else
		{
			$recordsCollectedStale=0;
		}
		
		if($workingThreads<($threadsAllowed-1))
		{
			if(!$threadJustFinished)
			{
				my $pidFileNameStart=int(rand(10000));
				if($finishedRecordCount<$max)
				{
					my $loops=0;
					while ($workingThreads<($threadsAllowed-1))#&& ($finishedRecordCount+($loops*$chunkGoal)<$max))
					{
						$loops++;
						my $thisOffset = $offset;
						my $thisIncrement = $increment;	
						my $choseRecover=0;						
						my $dbuser = "";
						my $keepsearching=1;
						#print "Searching for an available userid\n";
						#print Dumper(\%dbUserTrack);
						while (((my $internal, my $value ) = each(%dbUserTrack)) && $keepsearching)
						{
							if($value<$dbUserMaxConnection)
							{
								$keepsearching=0;
								$dbuser=$internal;
								$dbUserTrack{$dbuser}++;
								#print "$dbuser: $value\n";
							}							
						}
						if($dbuser ne "")
						{	
							if((scalar @recovers) == 0)
							{
								#print "Sending off for range....\n";
								$thisIncrement = calcDBRange($self,$thisOffset,$chunkGoal,$dbHandler,$tselects);								
								#print "Got range: $thisIncrement\n";
							}
							else
							{
								print "There are some threads that died, so we are using those ranges for new threads\n";
								$choseRecover=1;
								$thisOffset = @{@recovers[0]}[0];
								$thisIncrement = @{@recovers[0]}[1];
								my $test = $thisIncrement - $thisOffset;
								if($test<0)
								{
									print "NEGATIVE RANGE:\n$thisOffset\n$thisIncrement\n";
								}
								shift(@recovers);
							}
							$range=$thisIncrement-$thisOffset;
							#print "Starting new thread\n";
							#print "Max: $max   From: $thisOffset To: $thisIncrement\n";
							my $thisPid = $mobUtil->chooseNewFileName("/tmp",$pidFileNameStart,"evergreenpid");
							my $ty = $self->{'type'};
							#print "Spawning: $pathtothis $conffile thread $thisOffset $thisIncrement $thisPid $dbuser $ty\n";
							system("$pathtothis $conffile thread $thisOffset $thisIncrement $thisPid $dbuser $ty &");
							push(@threadTracker,$thisPid);
							#print "Just pushed thread onto stack\n";
							$pidFileNameStart++;
							if(!$choseRecover)
							{
								$offset=$thisIncrement;
								$increment=$thisIncrement;
							}
						}
						else
						{
							#print "Could not find an available db user - going to have to wait\n";
						}
						$workingThreads++;
						#print "End of while loop for $workingThreads< ( $threadsAllowed - 1 )\n";
					}
				}
				else
				{
					print "We have reached our target record count... script is winding down\n";
				}
			}
		}
		
		#stop this nonsense - we have looped 600 times and not increased our records!  600 loops * 2 seconds per loop = 20 minutes
		if($recordsCollectedStale>600)
		{
			$threadsAlive=0;
		}
		
		if($workingThreads==0 && !$threadJustFinished)
		{
			$threadsAlive=0;
		}
		my $secondsElapsed = calcTimeDiff($self,$previousTime);
		my $minutesElapsed = $secondsElapsed / 60;
		my $overAllRPS = $finishedRecordCount / $secondsElapsed;
		my $devideTemp = $overAllRPS;
		if($devideTemp<1)
		{
			$devideTemp=1;
		}
		
		my $remaining = ($max - $finishedRecordCount) / $devideTemp / 60;
		$self->{'rps'}=$overAllRPS;
		$minutesElapsed = substr($minutesElapsed,0,index($minutesElapsed,'.')+3);		
		$remaining = substr($remaining,0,index($remaining,'.')+3);
		
		$masterfile->truncFile($pidfile->getFileName);
		$masterfile->addLine("$rps records/s Per Thread\n$overAllRPS records/s Average\nChunking: $chunkGoal\nRange: $range\n$remaining minutes remaining\n$minutesElapsed minute(s) elapsed\n");
		$masterfile->addLine("Reingest finished: $finishedRecordCount,\nNeed: $max  \n");
		$masterfile->addLine("Loops with no results: $recordsCollectedStale");
		$masterfile->addLine("Database User Utalization:");
		$masterfile->addLine(Dumper(\%dbUserTrack));
		if((scalar @recovers)>0)
		{
			$masterfile->addLine("Recovering these ranges:");
			$masterfile->addLine(Dumper(\@recovers));
		}
		
		#print "$rps records/s Current\n$overAllRPS records/s Average\nChunking: $chunkGoal\nRange: $range\nRecords On disk: $finishedRecordCount,\nNeed: $max  Searching: $offset To: $increment\n";
		sleep(2);
	}
	$masterfile->deleteFile();
 }
 
 sub findMaxRecordCount
 {
	my $self = @_[0];
	my $mm = @_[1];
	my @cha = split("",$mm);
	my $maxQuery;
	foreach(@cha)
	{
		$maxQuery.=$_;
	}
	$maxQuery =~ s/\$recordSearch/COUNT(\*)/gi;
	
	my $dbHandler = $self->{'dbhandler'};
	my $max = 0;
	my @results = @{$dbHandler->query($maxQuery)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$max = @row[0];
	}
	#print "max: $max\n";
	return $max;
 }
 
 sub calcDBRange
 {
	#print "starting rangefinding\n";
	my $self = @_[0];
	my $previousTime=DateTime->now(time_zone => "local");
	my $thisOffset = @_[1];	
	my $chunkGoal = @_[2];
	my $dbHandler = @_[3];
	my $countQ = @_[4];
	my $thisIncrement = $thisOffset;
	
	$countQ =~s/\$recordSearch/COUNT(\*)/gi;
	my $yeild=0;
	if($chunkGoal<1)
	{
		$chunkGoal=1;
	}
	$thisIncrement+=$chunkGoal;
	my $trys = 0;
	while($yeild<$chunkGoal)  ## Figure out how many rows to read into the database to get the goal number of records
	{	
		my $selects = $countQ." AND ID > $thisOffset AND ID <= $thisIncrement";
		#print "$selects\n";
		my @results = @{$dbHandler->query($selects)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$yeild = @row[0];
		}
		#print "Yeild: $yeild\n";
		if($yeild<$chunkGoal)
		{
			$trys++;
			if($trys>20)	#well, 100 * 10 and we didn't get 1000 rows returned, so we are stopping here.
			{
				$yeild=$chunkGoal;
			}
			$thisIncrement+=$chunkGoal+($trys*$chunkGoal);
		}
	}
	my $secondsElapsed = calcTimeDiff($self,$previousTime);
	#print "Range Finding: $secondsElapsed after $trys trys\n";
	
	#print "ending rangefinding\n";
	return $thisIncrement;
 }
 
 sub getRecordCount
 {
	my $self = @_[0];
	return $self->{'recordcount'};
 }
 
 sub getSpeed
 {
	my $self = @_[0];
	return $self->{'querytime'};
 }
 
 sub getRPS
 {
	my $self = @_[0];
	return $self->{'rps'};
 }

 sub stuffStandardFields
 {
	my ($self) = @_[0];
	my $dbHandler = $self->{'dbhandler'};
	my $log = $self->{'log'};
	my $mobUtil = $self->{'mobiusutil'};
	my %standard = %{$self->{'standard'}};
	my $selects = $self->{'selects'};
	my $previousTime=DateTime->now(time_zone => "local");
	my $pidfile = $self->{'pidfile'};
	my $query = "SELECT ID FROM BIBLIO.RECORD_ENTRY A WHERE A.ID IN($selects) ORDER BY ID";
	#print "$query\n";	
	$pidfile->truncFile($query);
	my @results = @{$dbHandler->query($query)};
	updateQueryDuration($self,$previousTime,$query);
	my @records;
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibID = @row[0];
		$query = "SELECT metabib.reingest_metabib_field_entries($bibID, FALSE, TRUE, FALSE)";
		#update biblio.record_entry set id=id where id=$bibID";
		$dbHandler->update($query);
		$standard{$bibID} = 1;
	}
	$self->{'standard'} = \%standard;	
 } 
 
 sub figureSelectStatement
 { 
	my $self = @_[0];
	my $test = $self->{'bibids'};
	my $dbHandler = $self->{'dbhandler'};
	my $results = "";
	my $mobUtil = $self->{'mobiusutil'};
	if(ref $test eq 'ARRAY')
	{
		my @ids = @{$test};
		$results = $mobUtil->makeCommaFromArray(\@ids);
	}
	else
	{
		$results = $test;		
	}
	$self->{'selects'}  = $results;
	
 }
  
 sub updateQueryDuration
 {
	my $self = @_[0];
	my $previousTime=@_[1];
	my $query = @_[2];
	my $duration = calcTimeDiff($self,$previousTime);	
	if($self->{'querytime'}<$duration)
	{
		$self->{'querytime'}=$duration;
		$self->{'query'}=$query;
		#print "New long running query: $duration\n";
	}
	return $duration;
 }
 
 sub calcTimeDiff
 {
	my $self = @_[0];
	my $previousTime = @_[1];
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
 
 1;
 
 