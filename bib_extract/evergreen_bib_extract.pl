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
   
    use MARC::Field;
	my @files = ('/tmp/temp/evergreen_tempmarc1011.mrc');

    my $batch = MARC::Batch->new( 'USMARC', @files );
    while ( my $marc = $batch->next ) {
        print $marc->subfield(245,"a"), "\n";
		print $marc->subfield(901,"a"), "\n";
    }
	exit;
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
				my $fileNamePrefix = $conf{"fileprefix"}."_cancels_";
				if(defined($type))		
				{
					if($type eq "adds")
					{
						$valid = 1;
						$fileNamePrefix = $conf{"fileprefix"}."_updates_";
						
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
						$fileNamePrefix = $conf{"fileprefix"}."_full_";
					}
					else
					{
						$valid = 0;
						print "You need to specify the type 'adds' or 'cancels' or 'full'\n";
					}
				}
				else
				{
					$valid = 0;
					print "You need to specify the type 'adds' or 'cancels'\n";
				}
				if(!defined($platform))
				{
					print "You need to specify the platform\n";
				}
				else
				{
					$fileNamePrefix=$platform."_".$fileNamePrefix;
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

						my $dt   = DateTime->now(time_zone => "local"); 	
						my $fdate = $dt->ymd;
						
						my $outputMarcFile = $mobUtil->chooseNewFileName($conf->{"marcoutdir"},$fileNamePrefix.$fdate,"mrc");
												
						if($outputMarcFile ne "0")
						{	
						
							my $dt    = DateTime->now(time_zone => "local");   # Stores current date and time as datetime object
							my $ftime = $dt->hms;
							my $dateString = "$fdate $ftime";
							#print $outputMarcFile."\n";
							my $marcOutFile = $outputMarcFile;
							my $evergreenScraper;
							$valid=1;
							my $selectQuery = $mobUtil->findQuery($dbHandler,$school,$platform,$type,$queries);
							
							#print "Path: $pathtothis\n";
							my $gatherTime = DateTime->now();
							local $@;
							#eval{$evergreenScraper = new evergreenScraper($dbHandler,$log,$selectQuery,$type,$conf{"school"},$pathtothis,$configFile,$maxdbconnections);};
							eval{$evergreenScraper = new evergreenScraper($dbHandler,$log,'select $recordsearch from biblio.record_entry where id=-90',$type,$conf{"school"},$pathtothis,$configFile,$maxdbconnections);};
							if($@)
							{
								print "Master Thread Failed:\n";
								print $@;
								$valid=0;
								#$email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
								#$email->send("RMO $school - $platform $type FAILED - Job # $dateString","There was a failure when trying to get data from the database.\r\n\r\n I have only seen this in the case where an item has more than 1 bib and is in the same subset of records. Check the cron output for more information.\r\n\r\nThis job is over.\r\n\r\n-MOBIUS Perl Squad-\r\n\r\n$selectQuery");
								$log->addLogLine("Sierra scraping Failed. The cron standard output will have more clues.\r\n$selectQuery");
								$failString = "Scrape Fail";
							}
							
							my $recCount=0;
							my $format = DateTime::Format::Duration->new(
								pattern => '%M:%S' #%e days, %H hours,
							);
							my $gatherTime = $evergreenScraper->calcTimeDiff($gatherTime);
							$gatherTime = $gatherTime / 60;
							#$gatherTime = $format->format_duration($gatherTime);
							my $afterProcess = DateTime->now(time_zone => "local");
							my $difference = $afterProcess - $dt;
							my $duration =  $format->format_duration($difference);
							my $extraInformationOutput = "";
							my $couldNotBeCut = "";
							my $rps;
							if($valid)
							{
								my @tm = ();
								#my @all = @{$evergreenScraper->getAllMARC()};
								# my @tall = ('/mnt/evergreen/tmp/temp/evergreen_tempmarc35.mrc');
# '/mnt/evergreen/tmp/temp/evergreen_tempmarc1072.mrc',
# '/mnt/evergreen/tmp/temp/evergreen_tempmarc148.mrc',
# '/mnt/evergreen/tmp/temp/evergreen_tempmarc1077.mrc',
# '/mnt/evergreen/tmp/temp/evergreen_tempmarc829.mrc');
my @tall = ();
								@tall = @{dirtrav(\@tall,"/tmp/temp")};	
								@tall = sort @tall;
								# for my $i(0..$#tall)
								# {
									# if($i!=$#tall)
									# {										
										# if(@tall[$i+1] lt @tall[$i])
										# {
											# print "Shifting\n";
											# my $tempm = @tall[$i+1];
											# @tall[$i+1]=@tall[$i];
											# @tall[$i+1] = $tempm;
											# $i-=2;
											# if($i==-2)
											# {
												# $i++;
											# }
										# }
									# }
								# }
# print "Got: ".$#tall." files\n";
								 my @all = (\@tm,\@tall);
								my @marc = @{@all[0]}; 
								my @tobig = @{$evergreenScraper->getTooBigList()};
								$extraInformationOutput = @tobig[0];
								$couldNotBeCut = @tobig[1];
								my $marcout = new Loghandler($marcOutFile);
								$marcout->deleteFile();
								my $output;
								my $barcodes="";
								# my @back = @{processMARC(\@marc,$platform,$type,$school,$marcout,$log)};
								# print Dumper(@back);
								# $extraInformationOutput.=@back[0];
								# $barcodes.=@back[1];
								# $couldNotBeCut.=@back[2];
								# $recCount+=@back[3];
								
								if(ref @all[1] eq 'ARRAY')
								{
									print "There were some files to process";
									my @dumpedFiles = @{@all[1]};
									foreach(@dumpedFiles)
									{	
										@marc =();
										my $marcfile = $_;
										my $check = new Loghandler($marcfile);
										if($check->fileExists())
										{
											$log->addLine($marcfile);
											my $file = MARC::File::USMARC->in( $marcfile );
											my $r =0;
											while ( my $marc = $file->next() ) 
											{						
												$r++;
												push(@marc,$marc);
											}
											print "Read $r records from $_\n";
											#$check->deleteFile();
										}
										my @back = @{processMARC(\@marc,$platform,$type,$school,$marcout,$log)};
										$extraInformationOutput.=@back[0];
										$barcodes.=@back[1];
										$couldNotBeCut.=@back[2];
										#print "Adding ".@back[3];
										$recCount+=@back[3];
									}
								}
								
								
								if(length($extraInformationOutput)>0)
								{
									$extraInformationOutput="These records were TRUNCATED due to the 100000 size limits: $extraInformationOutput \r\n\r\n";
								}
								if(length($couldNotBeCut)>0)
								{
									$couldNotBeCut="These records were OMITTED due to the 100000 size limits: $couldNotBeCut \r\n\r\n";
								}
								
								if($recCount<1)
								{	
									$marcOutFile = "(none)";
								}
								if($valid)
								{
									my $extraBlurb="";
									$rps = $evergreenScraper->getRPS();
									$afterProcess = DateTime->now(time_zone => "local");
									$difference = $afterProcess - $dt;
									$duration =  $format->format_duration($difference);
									$log->addLogLine("$school $platform $type: $marcOutFile");
									$log->addLogLine("$school $platform $type: $recCount Record(s)");
									#$email = new email($conf{"fromemail"},\@tolist,0,1,\%conf);
									if($recCount>0)
									{	
										$marcOutFile = substr($marcOutFile,rindex($marcOutFile, '/')+1);
									}
									if(length($barcodes)>$barcodeCharacterAllowedInEmail)
									{
										$barcodes = substr($barcodes,0,$barcodeCharacterAllowedInEmail);
									}
									#$email->send("RMO $school - $platform $type Success - Job # $dateString","$extraBlurb \r\nRecord gather duration: $gatherTime\r\nRecords per second: $rps\r\nTotal duration: $duration\r\n\r\nThis process finished without any errors!\r\n\r\nHere is some information:\r\n\r\nOutput File: \t\t$marcOutFile\r\n$recCount Record(s)\r\nFTP location: ".$conf{"ftphost"}."\r\nUserID: ".$conf{"ftplogin"}."\r\nFolder: $remoteDirectory\r\n\r\n$extraInformationOutput $couldNotBeCut -MOBIUS Perl Squad-\r\n\r\n$selectQuery\r\n\r\nThese are the top $barcodeCharacterAllowedInEmail characters included records:\r\n$barcodes");
								}
							}
				#OUTPUT TO THE CSV
							if($conf{"csvoutput"})
							{
								 my $csv = new Loghandler($conf{"csvoutput"});
								 my $csvline = "\"$dateString\",\"$school\",\"$platform\",\"$type\",\"$failString\",\"$marcOutFile\",\"$gatherTime\",\"$rps\",\"$duration\",\"$recCount Record(s)\",\"".$conf{"ftphost"}."\",\"".$conf{"ftplogin"}."\",\"$remoteDirectory\",\"$extraInformationOutput\",\"$couldNotBeCut\"";
								 $csvline=~s/\n//g;
								 $csvline=~s/\r//g;
								 $csvline=~s/\r\n//g;
								 
								 $csv->addLine($csvline);
								 undef $csv;							 
							}
							
							$log->addLogLine("$school $platform $type *ENDING*");
						}
						else
						{
							$log->addLogLine("Output directory does not exist: ".$conf{"marcoutdir"});
						}
						
					 }
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
 
 sub processMARC
 {
	my @marc = @{@_[0]};
	my $platform = @_[1];
	my $type = @_[2];
	my $school = @_[3];
	my $marcout = @_[4];
	my $log = @_[5];
	my $extraInformationOutput='';
	my $barcodes;
	my $couldNotBeCut='';
	my $recCount=0;
	foreach(@marc)
	{
		my $marc = $_;
		#print $marc->encoding();
		$log->addLine("setting encoding");
		$marc->encoding( 'UTF-8' );
		$log->addLine($marc->subfield('901','a'));
		my @count = @{$mobUtil->trucateMarcToFit($marc)};
		#print @count[1]."\n";
		my $addThisone=1;
		if(@count[1]==1)
		{
			$marc = @count[0];
			print "Extrainformation adding: ".$marc->subfield('901',"a");
			$extraInformationOutput.=$marc->subfield('901',"a");
			print "Now it's\n $extraInformationOutput";
		}
		elsif(@count[1]==0)
		{
			$addThisone=0;
		}
		
		if($addThisone) #ISO2709 MARC record is limited to 99,999 octets 
		{
			#print "Adding barcode to string\n";
			$barcodes.=$marc->subfield('901',"a");
			if($marc->subfield('245',"a"))
			{	
				#print "Adding title to string\n";
				$barcodes.=" - ".$marc->subfield('245',"a");
			}
			$barcodes.="\r\n";
			#print "Appending master marc file\n";
			$marcout->appendLine($marc->as_usmarc());
			#print "Done appending master marc file\n";			
			$recCount++;
		}
		else
		{
			$couldNotBeCut.=$marc->subfield('901',"a");
		}
	}
	my @ret=($extraInformationOutput,$barcodes,$couldNotBeCut,$recCount);
	return \@ret;
 }
 
 sub thread
 {
	my %conf = %{@_[0]};
	my $previousTime=DateTime->now;
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
		my $evergreenScraper;
		local $@;
		#print "Scraping:\n$dbHandler,$log,$selectQuery,$type,".$conf{"school"}.",$pathtothis,$configFile";
		eval{$evergreenScraper = new evergreenScraper($dbHandler,$log,$selectQuery,$type,$conf{"school"},$pathtothis,$configFile);};
		if($@)
		{
			print "******************* I DIED SCRAPER ********************** $pid\n";
			print $@;
			$pidWriter->truncFile("none\nnone\nnone\nnone\nnone\nnone\n$dbuser\nnone\n1\n$offset\n$increment");
			$rangeWriter->addLine("$offset $increment DEFUNCT");
			exit;
		}
		
		my $recordCount = $evergreenScraper->getRecordCount();
		my @tobig = @{$evergreenScraper->getTooBigList()};
		my $extraInformationOutput = @tobig[0];
		my $couldNotBeCut = @tobig[1];
		my @diskDump = @{$evergreenScraper->getDiskDump()};
		my $disk =@diskDump[0];
		my $queryTime = $evergreenScraper->getSpeed();
		my $secondsElapsed = $evergreenScraper->calcTimeDiff($previousTime);
		#print "Writing to thread File:\n$disk\n$recordCount\n$extraInformationOutput\n$couldNotBeCut\n$queryTime\n$limit\n$dbuser\n$secondsElapsed\n";
		my $writeSuccess=0;
		my $trys=0;
		while(!$writeSuccess && $trys<100)
		{
			$writeSuccess = $pidWriter->truncFile("$disk\n$recordCount\n$extraInformationOutput\n$couldNotBeCut\n$queryTime\n$limit\n$dbuser\n$secondsElapsed");
			if(!$writeSuccess)
			{
				print "$pid -  Could not write final thread output, trying again: $trys\n";
			}
			$trys++;
		}
		
	}
	
	exit;
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