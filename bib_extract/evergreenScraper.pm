#!/usr/bin/perl
#
# evergreenScraper.pm
#
# Requires:
#
# recordItem.pm
# evergreenScraper.pm
# DBhandler.pm
# Loghandler.pm
# Mobiusutil.pm
# MARC::Record (from CPAN)
#
#
# Usage:
# my $log = new Loghandler("path/to/log/file");
# my $dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"});
#
#
# You can get the resulting MARC Records in an array of MARC::Records like this:
#
# my @marc = @{$evergreenScraper->getAllMARC()};
#
# Blake Graham-Henderson
# MOBIUS
# blake@mobiusconsortium.org
# 2013-1-24

package evergreenScraper;
 use MARC::Record;
 use MARC::File;
 use MARC::File::XML (BinaryEncoding => 'utf8');
 use MARC::File::USMARC;
 use Loghandler;
 use strict;
 use Data::Dumper;
 use Mobiusutil;
 use Date::Manip;
 use DateTime::Format::Duration;
 use String::Multibyte;
 use utf8;
 use Encode;
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
    my $pidfile = new Loghandler($mobutil->chooseNewFileName('/tmp','scraper_pid','pid'));
    my $self =
    {
        'dbhandler' => shift,
        'log' => shift,
        'bibids' => shift,
        'mobiusutil' => $mobutil,
        'holdings' =>  \%k,
        'standard' => \%g,
        'selects' => "",
        'querytime' => 0,
        'query' => "",
        'type' => "",
        'diskdump' => "",
        'toobig' => "",
        'toobigtocut' => "",
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
        $pidfile = new Loghandler($mobutil->chooseNewFileName('/tmp',"scraper_pid_$title",'pid'));
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
    if(($t) && ($t ne 'thread') && ($max > 5000))
    {
        gatherDataFromDB_spinThread_Controller($self);
    }
    elsif(($t) && ($t eq 'full'))
    {
        gatherDataFromDB_spinThread_Controller($self);
    }
    elsif(($t) && ($t eq 'thread'))
    {
        my $cou = spinThread($self);
        $self->{'recordcount'} = $cou;
    }
    else
    {
        gatherDataFromDB($self);
    }
    $pidfile->deleteFile();
    return $self;
 }

 sub gatherDataFromDB
 {
    my $self = @_[0];
    my $previousTime = DateTime->now();
    $self->{'selects'} =~ s/\$recordSearch/ID/gi;
    stuffStandardFields($self);
    stuffHoldings($self);
    my $secondsElapsed = calcTimeDiff($self,$previousTime);
    if($secondsElapsed < 1)
    {
        $secondsElapsed = 1;
    }
    my %standard = %{$self->{'standard'}};
    my $recordCount = scalar keys %standard;
    $self->{'rps'} = $recordCount / $secondsElapsed;
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
    my $previousTime=DateTime->now;
    #print "Thread starting\n";
    $self->{'selects'}  = $self->{'bibids'};
    #print "stuffStandardFields\n";
    stuffStandardFields($self);
    stuffHoldings($self);
    my $secondsElapsed = calcTimeDiff($self,$previousTime);
    #print "time = $secondsElapsed\n";
    my %standard = %{$self->{'standard'}};
    my $currentRecordCount = scalar keys %standard;
    #print "currentRecordCount = $currentRecordCount\n";
    my @dumpedFiles = (0);
    #print "Dumping to disk\n";
    @dumpedFiles = @{dumpRamToDisk($self, \@dumpedFiles,1)};
    #print "done Dumping to disk\n";
    $self->{'diskdump'}=\@dumpedFiles;
    #print "Dumped files\n";
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
    my $previousTime=DateTime->now;
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

    my @dumpedFiles = (0);
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
                            #print "Adjusted chunk to $chunkGoal\n";
                            push(@dumpedFiles,@lines[0]);
                            #print "Added dump files to array\n";
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
                                $thisIncrement = $thisOffset + $range if (!$zeroAdded && $range > 0);
                                $thisIncrement = calcDBRange($self,$thisOffset,$chunkGoal,$dbHandler,$tselects) if ($zeroAdded || $range == 0);
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
        $masterfile->truncFile($pidfile->getFileName);
        $masterfile->addLine("$rps records/s Per Thread\n$overAllRPS records/s Average\nChunking: $chunkGoal\nRange: $range\n$remaining minutes remaining\n$minutesElapsed minute(s) elapsed\n");
        $masterfile->addLine("Records On disk: $finishedRecordCount,\nNeed: $max  \n");
        $masterfile->addLine("Loops with no records: $recordsCollectedStale");
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
    $self->{'diskdump'}=\@dumpedFiles;
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
    my $previousTime=DateTime->now;
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
            if($trys>20)    #well, 100 * 10 and we didn't get 1000 rows returned, so we are stopping here.
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

 sub getDiskDump
 {
    my $self = @_[0];
    return $self->{'diskdump'};
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

 sub getSingleStandardFields
 {
    my ($self) = @_[0];
    my $idInQuestion = @_[1];
    my $log = $self->{'log'};
    my %standard = %{$self->{'standard'}};
    if(exists $standard{$idInQuestion})
    {
        #print "It exists\n";
    }
    return \@{$standard{$idInQuestion}};
 }

 sub stuffStandardFields
 {
    my ($self) = @_[0];
    my $dbHandler = $self->{'dbhandler'};
    my $log = $self->{'log'};
    my $mobUtil = $self->{'mobiusutil'};
    my %standard = %{$self->{'standard'}};
    my $selects = $self->{'selects'};
    my $previousTime=DateTime->now;
    my $pidfile = $self->{'pidfile'};
    my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY A WHERE A.ID IN($selects) ORDER BY ID";
    #print "$query\n";
    $pidfile->truncFile($query);
    my @results = @{$dbHandler->query($query)};
    updateQueryDuration($self,$previousTime,$query);
    my @records;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $recordID = @row[0];
        my $memarc = @row[1];
        $memarc =~ s/(<leader>.........)./${1}a/;
        $memarc = MARC::Record->new_from_xml($memarc, 'UTF-8');
        $standard{$recordID} = $memarc;
    }
    $self->{'standard'} = \%standard;

 }

 sub stuffHoldings
{
    my ($self) = @_[0];
    my $dbHandler = $self->{'dbhandler'};
    my $log = $self->{'log'};
    my %holdings = %{$self->{'holdings'}};
    my $mobiusUtil = $self->{'mobiusutil'};
    my $selects = $self->{'selects'};
    my $pidfile = $self->{'pidfile'};
    my $query = "SELECT aou_own.shortname,aou_circ.shortname,AC.PRICE,AC.BARCODE,AC.CIRC_MODIFIER,ACN.LABEL, acl.name, acn.record FROM ASSET.COPY ac,asset.call_number acn, asset.copy_location acl, actor.org_unit aou_own, actor.org_unit aou_circ
                          where
                        ac.CALL_NUMBER =acn.id and
                        acn.record in(".$selects.") and
                        acn.owning_lib=aou_own.id and
                        ac.circ_lib=aou_circ.id and
                        acl.id=ac.location and
                        ac.deleted='f'";
    my $previousTime=DateTime->now;
    my @results = @{$dbHandler->query($query)};
    updateQueryDuration($self,$previousTime,$query);
    $pidfile->truncFile($query);
    foreach(@results)
    {
    #print "Holdings results loop\n";
        my $row = $_;
        my @row = @{$row};
        my $recordID = @row[7];
        my $recordItem;
        if(!exists $holdings{$recordID})
        {
            my @a = ();
            $holdings{$recordID} = \@a;
        }
        #print "Making holdings field\n";
        my $logout='';
        for my $i(0..$#row)
        {
            if(!@row[$i])
            {
                #print "Undefined value $i: '".@row[$i]."'\n";
                @row[$i]='';
            }
            @row[$i]=~s/\\/\\\\/g;
            $logout.="\"".@row[$i]."\",";
        }
        $logout=substr($logout,0,-1);
        #$log->addLine($logout);
        my $field = MARC::Field->new('852','4', ' ', 'a' => @row[0], 'b'=>@row[1], 'c'=>@row[6], 'j'=>@row[5], 'g'=>@row[4], 'p'=>@row[3], 'y'=>"\$".@row[2]  );
        #print "done Making holdings field\n";
        push(@{$holdings{$recordID}},$field);
    }
    $self->{'holdings'} = \%holdings;
}

 sub getSingleMARC
 {
    my ($self) = @_[0];
    my $recID = @_[1];
    my $dbHandler = $self->{'dbhandler'};
    my $log = $self->{'log'};
    my $mobiusUtil = $self->{'mobiusutil'};
    my %holdings = %{$self->{'holdings'}};
    my %standard = %{$self->{'standard'}};
    #print "Single MARC ASSIGNING STANDARD = \$RET\n";
    my $ret = $standard{$recID};
    #print Dumper($ret);
    if($ret)
    {
        if(exists $holdings{$recID})
        {
            my $dbid = $ret->subfield('901','a');
            #print "Adding HOldings\n";
            my @hdings = @{$holdings{$recID}};
            #print Dumper(@hdings);
            if($#hdings>-1)
            {
                #$log->addLine("Adding ".$#hdings." holdings to $dbid\n");
                $ret->insert_fields_ordered( @hdings );
            }
            #print "done Adding HOldings\n";
        }
        else
        {
        #   print "No holdings for this $recID\n";
        }

    }
    #print "Changing to utf-8\n";
    #print Dumper($ret);
    #$log->addLine($ret->as_formatted());
    #print $ret->encoding();
    $ret->encoding( 'UTF-8' );
    #print "Returning single marc\n";
    return $ret;
 }

 sub getAllMARC
 {
    my $self = @_[0];
    my %standard = %{$self->{'standard'}};
    my $dumpedFiles = $self->{'diskdump'};
    my @ret;
    my @marcout;

    #format memory into marc
    while ((my $internal, my $value ) = each(%standard))
    {
        push(@marcout,getSingleMARC($self,$internal));
    }

    push(@ret,[@marcout]);
    if(ref $dumpedFiles eq 'ARRAY')
    {
        my @dumpedFiles = @{$dumpedFiles};
        push(@ret,[@dumpedFiles]);
    }
    return \@ret;
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

 sub calcCheckDigit
 {
    my $seed =@_[1];
    $seed = reverse($seed);
    my @chars = split("", $seed);
    my $checkDigit = 0;
    for my $i (0.. $#chars)
    {
        $checkDigit += @chars[$i] * ($i+2);
    }
    $checkDigit =$checkDigit%11;
    if($checkDigit>9)
    {
        $checkDigit='x';
    }
    return $checkDigit;
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
    my $currentTime=DateTime->now;
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

 sub dumpRamToDisk
 {
    my $self = @_[0];
    my %standard = %{$self->{'standard'}};
    my $log = $self->{'log'};
    my $mobUtil = $self->{'mobiusutil'};
    my $extraInformationOutput = $self->{'toobig'};
    my $couldNotBeCut = $self->{'toobigtocut'};
    my $title=$self->{'title'};
    my @dumpedFiles = @{@_[1]};
    my $threshHold = @_[2];
    my @newDump=@dumpedFiles;
    if(scalar keys %standard >$threshHold)
    {
        #print "Now it's over the threshold\n";
        @newDump=();
        my $recordsInFiles=0;
        if(scalar(@dumpedFiles)>0)
        {
            my $lastElement = scalar(@dumpedFiles);
            $lastElement--;
            for my $i(0..$#dumpedFiles-1)
            {
                push(@newDump,@dumpedFiles[$i]);
            }
            $recordsInFiles=@dumpedFiles[$lastElement];  #The last element contains the total count
            undef @dumpedFiles;
            #print Dumper(@newDump);
        }

        my @try = ('holdings','standard');
        #print "Getting all marc\n";
        my @both = @{getAllMARC($self)};
        #print "done Getting all marc\n";
        my @marc = @{@both[0]};
        my $files = @both[1];
        if(ref $files eq 'ARRAY')
        {
            print "There should not be any files here but there are:\n";
            my @files = @{$files};
            foreach(@files)
            {
                print "$_\n";
            }
        }
        #print Dumper(@marc);
        #print "Got em\n";
        my $output;

        foreach(@marc)
        {
            #print "Loop through marc record";
            my $marc = $_;
            #print "Counting recordsize\n";
            my $count = $mobUtil->marcRecordSize($marc);
            #print "Got size: $count\n";
            my $addThisone=1;
            if($count>99999) #ISO2709 MARC record is limited to 99,999 octets
            {
                print "it's over 99999\n";
                my @re = @{$mobUtil->trucateMarcToFit($marc)};
                $marc = @re[0];
                $addThisone=@re[1];
                if($addThisone)
                {
                    print "Extrainfo Before: $extraInformationOutput\n";
                    $extraInformationOutput.=$marc->subfield('901',"a");
                    print "Extrainfo After: $extraInformationOutput\n";
                }
            }
            if($addThisone) #ISO2709 MARC record is limited to 99,999 octets
            {
                #print "About to set encoding\n";
                #$marc->encoding( 'UTF-8' );
                #print "About to add it to output\n";
                #print Dumper($marc);

                $output.=$marc->as_usmarc();
                #print "Added it\n";
            }
            else
            {
                $couldNotBeCut.=$marc->subfield('901',"a");
            }
        }
        if(length($title)>0)
        {
            $title=$title."_";
        }
        #print "Choosing file name\n";
        my $fileName = $mobUtil->chooseNewFileName("/mnt/evergreen/tmp/temp",$title."tempmarc","mrc");
        #print "Decided on $fileName \n";
        my $marcout = new Loghandler($fileName);
        $marcout->appendLine($output);
        push(@newDump, $fileName);
        my $addedToDisk = scalar keys %standard;
        $recordsInFiles+=$addedToDisk;
        push(@newDump, $recordsInFiles);
        foreach(@try)
        {
            my %n = ();
            undef $self->{$_};
            $self->{$_} = \%n;
        }
    }
    #print Dumper(\@newDump);
    $self->{'toobig'} = $extraInformationOutput;
    $self->{'toobigtocut'} = $couldNotBeCut;
    return \@newDump;
 }

 sub getTooBigList
 {
    my $self = @_[0];
    my @ret = ($self->{'toobig'},$self->{'toobigtocut'});
    return \@ret;
 }

 1;

