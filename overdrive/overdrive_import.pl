#!/usr/bin/perl
use lib qw(../);
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use pQuery;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;

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
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		my $log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("server","login","password","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","yearstoscrape","toomanyfilesthreshold"); 
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
		my $archivefolder = $conf{"archivefolder"};
		if(!(-d $archivefolder))
		{
			$valid = 0;
			print "Sorry, the archive folder does not exist: $archivefolder\n";
			$errorMessage = "Sorry, the archive folder does not exist: $archivefolder";
		}
		
		my $finalImport = 0;
		my @info;
		my $count=0;
		my @files;
		if($valid)
		{	
			my $log = new Loghandler($conf{"logfile"});
			my @marcOutputRecords;
			my @shortnames = split(/,/,$conf{"participants"});
			for my $y(0.. $#shortnames)
			{				
				@shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
			}
			@files = @{getmarc($conf{"server"},$conf{"login"},$conf{"password"},$conf{"yearstoscrape"},$archivefolder,$log)};
			if(@files[$#files]!=-1)
			{
				for my $b(0..$#files)
				{
					$log->addLogLine("Parsing: ".$files[$b]);
					my $file = MARC::File::USMARC->in($files[$b]);
					while ( my $marc = $file->next() ) 
					{	
						$marc = add9($marc,\@shortnames);
						push(@marcOutputRecords,$marc);
					}
				}
				my $outputFile = $mobUtil->chooseNewFileName($conf{"tempspace"},"temp","mrc");
				my $marcout = new Loghandler($outputFile);
				$marcout->deleteFile();
				my $output;
				
				foreach(@marcOutputRecords)
				{
					my $marc = $_;
					$output.=$marc->as_usmarc();
					$count++;
				}
				$log->addLogLine("Outputting $count record(s) into $outputFile");
				$marcout->addLine($output);
				my $dbHandler;
				eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
				if ($@) 
				{
					$log->addLogLine("Could not establish a connection to the database");
					$log->addLogLine("Deleting $outputFile");
					$marcout->deleteFile();
					deleteFiles(\@files);
					$valid = 0;
					$errorMessage = "Could not establish a connection to the database";
				}
				if($valid)
				{					
					@info = @{importMARCintoEvergreen($outputFile,$log,$dbHandler)};
					$finalImport = 1;
					print Dumper(\@info);
				}
				$marcout->deleteFile();
			}
			else
			{
				$log->addLogLine("There were some errors during the getmarc function, we are stopping execution. Any partially downloaded files are deleted.");
				foreach(@files)
				{
					$log->addLogLine($_);
				}
			}
		}
		my @worked = @{@info[0]};
		my @notworked = @{@info[1]};
		my $workedCount = $#worked+1;
		my $notWorkedCount = $#notworked+1;
		my $fileCount = $#files;
		my $afterProcess = DateTime->now(time_zone => "local");
		my $difference = $afterProcess - $dt;
		my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
		my $duration =  $format->format_duration($difference);
		my $fileList;
		my $successTitleList;
		my $failedTitleList;
		foreach(@files)
		{
			$fileList.="$_ ";
		}
		
		if($finalImport)
		{	
			my $csvlines;
			foreach(@worked)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $title = @both[1];
				$successTitleList.=$bibid." ".$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Success\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
			foreach(@notworked)
			{
				my $title = $_;
				$failedTitleList.=$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Failed Insert\",\"\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
				 
			if($conf{"csvoutput"})
			{
				my $csv = new Loghandler($conf{"csvoutput"});
				$csv->addLine($csvlines);
				undef $csv;
			}
		}
		if($count>0)
		{
			my $totalSuccess=1;
			if($notWorkedCount>0)
			{
				$totalSuccess=0;
			}
			my @tolist = ($conf{"alwaysemail"});		
			my $email = new email($conf{"fromemail"},\@tolist,$valid,$totalSuccess,\%conf);
			$fileList=~s/\s/\r\n/g;
			$email->send("Evergreen Utility - Overdrive Import Report Job # $dateString","I connected to: \r\n ".$conf{"server"}."\r\nand gathered:\r\n$count Record(s) from $fileCount file(s)\r\n$workedCount Successful Imports\r\n$notWorkedCount Not successful Imports\r\n Duration: $duration\r\n\r\n$fileList\r\nSuccessful Imports:\r\n$successTitleList\r\n\r\nUnsuccessful:\r\n$failedTitleList\r\n\r\n-MOBIUS Perl Squad-");
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub deleteFiles
{
	my $log = @_[0];
	my @files = @{@_[1]};
	foreach(@files)
	{
		my $t = new Loghandler($_);
		$log->addLogLine("Deleting $_");
		$t->deleteFile();
	}
}

sub getmarc
{
	my $server = @_[0];
	$server=~ s/http:\/\///gi;
	
	my $loops=0;
	my $login = @_[1];
	my $password = @_[2];	
	my $yearstoscrape = @_[3];
	my $archivefolder = @_[4];
	print "Server: $server\nLogin: $login\npassword: $password\narchivefolder: $archivefolder\n";
	my $log = @_[5];
	my @months = ("01-Jan","02-Feb","03-Mar","04-Apr","05-May","06-Jun","07-Jul","08-Aug","09-Sep","10-Oct","11-Nov","12-Dec");

	my $dt = DateTime->now(time_zone => "local");
	my $curyear = $dt->year();
	my @years = ();
	while($yearstoscrape>0)
	{
		push(@years,$curyear);
		$curyear--;
		$yearstoscrape--;
	}
	my @scrapedFileLinks;
	my @downloadedFiles;
	my @errors;
	my $quit=0;
	for my $yearpos(0..$#years)
	{
		my $thisYear = @years[$yearpos];
		for my $monthpos(0..$#months)
		{
			if(!$quit)
			{
				my $thisMonth = @months[$monthpos];
				my $URL = "http://$login:$password\@$server/Overdrive/$thisYear/$thisMonth/";
				#$log->addLine("Attempting to read $URL");
				if($loops<1)
				{
				
				pQuery($URL)
							->find("a")->each(sub {
										my $link = pQuery($_)->toHtml;
										my $output = "parsing: $link\n";
										my @s= split(/href=\"/,$link);
										@s = split(/\"/,@s[1]);
										$link = @s[0];
										if((index(lc($link),'.dat')>-1) || (index(lc($link),'.mrc')>-1))
										{
											if(!$quit)
											{
												## Check local archive to see if we already downloaded it
												
												my $localFile = $archivefolder."/$thisYear/$thisMonth/$link";
												if(!(-e $localFile))
												{
													if(!(-d $archivefolder."/$thisYear/$thisMonth"))
													{
														make_path($archivefolder."/$thisYear/$thisMonth", {
															verbose => 1,
															mode => 0777,
															});
													}
													sleep 1;
													#$log->addLine("$output Got this: $link");
													$log->addLogLine("New: $URL$link");
													#my $url = 'http://marinetraffic2.aegean.gr/ais/getkml.aspx';
													my $getsuccess = getstore($URL.$link, $localFile);
													if($getsuccess eq "200")
													{
														$loops++;
														#print "success: $getsuccess\n";
														push(@scrapedFileLinks,$URL.$link);
														push(@downloadedFiles,$localFile);
													}
													else
													{
														$log->addLogLine("COULD NOT TRANSFER $URL$link");
														$log->addLogLine("ABORTING SOON");
														push(@errors,"Unable to download: $URL$link");
														$quit=1;
													}
												}
											}
										}
										
									}
									);
				}
			}
		}
	}
	if($#errors > -1)
	{
		foreach(@downloadedFiles)
		{
			my $t = new Loghandler($_);
			$t->deleteFile();
		}
		push(@errors,"-1");
		return \@errors;
	}
	#$log->addLine(Dumper(\@scrapedFileLinks));
	return \@downloadedFiles;

}

sub add9
{
	my $marc = @_[0];
	my @shortnames = @{@_[1]};
	my @recID = $marc->field('856');
	if(defined @recID)
	{
		#$marc->delete_fields( @recID );
		for my $rec(0..$#recID)
		{
			#print Dumper(@recID[$rec]);
			for my $t(0.. $#shortnames)
			{
				my @subfields = @recID[$rec]->subfield( '9' );
				my $shortnameexists=0;
				for my $subs(0..$#subfields)
				{
				#print "Comparing ".@subfields[$subs]. " to ".@shortnames[$t]."\n";
					if(@subfields[$subs] eq @shortnames[$t])
					{
						print "Same!\n";
						$shortnameexists=1;
					}
				}
				#print "shortname exists: $shortnameexists\n";
				if(!$shortnameexists)
				{
					#print "adding ".@shortnames[$t]."\n";
					@recID[$rec]->add_subfields('9'=>@shortnames[$t]);
				}
			}
		}
	}
	return $marc;
}

sub importMARCintoEvergreen
{
	my @ret;
	my @worked;
	my @notworked;
	my $inputFile = @_[0];
	my $log = @_[1];
	my $dbHandler = @_[2];
	my $file = MARC::File::USMARC->in( $inputFile );
	my $r =0;		
	
	my $query;
	print "Working on importMARCintoEvergreen\n";
	while ( my $marc = $file->next() ) 
	{
		if($r<2)
		{
			#my $tcn = getTCN($log,$dbHandler);  #removing this because it has an auto created value in the DB
			my $thisXML = $marc->as_xml();
			$thisXML =~ s/\n//sog;
			$thisXML =~ s/^<\?xml.+\?\s*>//go;
			$thisXML =~ s/>\s+</></go;
			$thisXML =~ s/\p{Cc}//go;
			$thisXML = OpenILS::Application::AppUtils->entityize($thisXML);
			$thisXML =~ s/[\x00-\x1f]//go;
			my $title =  $marc->field('245')->subfield("a");
			my $max = getEvergreenMax($dbHandler);
			print "Importing $title\n";
			$query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(\\N,'IMPORT-1382129068.90847',E'$thisXML',\\N,\\N,E'Overdrive',\\N,\\N)";			
			$log->addLine($query);
			#my $res = $dbHandler->update($query);
			#print "$res";
			my $newmax = getEvergreenMax($dbHandler);
			if($newmax != $max)
			{
				my @temp = ($newmax,$title);
				push @worked, [@temp];
			}
			else
			{
				push (@notworked, $title);
			}
		}
		$r++;
	}
	
	push(@ret, (\@worked,\@notworked));
	print Dumper(@ret);
	return \@ret;
	
}

sub getEvergreenMax
{
	my $dbHandler = @_[0];
	
	my $query = "SELECT MAX(ID) FROM BIBLIO.RECORD_ENTRY";
	my @results = @{$dbHandler->query($query)};
	my $dbmax=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$dbmax = @row[0];
	}
	print "DB Max: $dbmax\n";
	return $dbmax;
}

sub getTCN
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $dbmax=getEvergreenMax($dbHandler);
	$dbmax++;
	my $result = 1;
	my $seed=0;
	my $ap="";
	my $trys = 0;
	while($result==1)
	{
		my $query = "SELECT COUNT(*) FROM BIBLIO.RECORD_ENTRY WHERE TCN_VALUE = 'od$dbmax$ap'";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			if(@row[0]==0)
			{
				$result=0;
			}
		}
		$ap = "_".$seed;
		$seed++;
		$trys++;
	}
	if($trys>1)
	{
		$log->addLogLine("Needed to change tcn $trys times to find: 'od$dbmax$ap'");
	}
	return "od$dbmax$ap";
}
 exit;

 
 