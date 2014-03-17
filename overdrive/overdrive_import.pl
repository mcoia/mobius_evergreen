#!/usr/bin/perl
#1198396
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
use Digest::SHA1;


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
		my @reqs = ("server","login","password","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","yearstoscrape"); 
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
			my $dbHandler;
			$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});
			setupSchema($dbHandler);
			# my @dbMarcs = @{findPBrecordInME($dbHandler)};
			# my @lookingforthese;
			# foreach(@dbMarcs)
			# {
				# my @t  = @{$_};
				# my $marc = @t[1];
				# $marc =~ s/(<leader>.........)./${1}a/;
				# $marc = MARC::Record->new_from_xml($marc);
				# if($marc->field('245'))
				# {
					# if($marc->field('245')->subfield('a'))
					# {
						# push(@lookingforthese,$marc->field('245')->subfield('a'));
					# }
				# }
			# }
			# @files=@{findMatchInArchive(\@lookingforthese,$archivefolder)}; 
			 @files = @{dirtrav(\@files,$archivefolder)};
			#@files = @{getmarc($conf{"server"},$conf{"login"},$conf{"password"},$conf{"yearstoscrape"},$archivefolder,$log)};
			if(@files[$#files]!=-1)
			{
				my $cnt = 0;
				for my $b(0..$#files)
				{
					if($cnt<100004)
					{
						$log->addLogLine("Parsing: ".$files[$b]);
						my $file = MARC::File::USMARC->in($files[$b]);
						while ( my $marc = $file->next() ) 
						{	
							$marc = add9($marc,\@shortnames);
							push(@marcOutputRecords,$marc);
						}
						$file->close();
						undef $file;
						$cnt++;
					}
				}
				my $outputFile = $mobUtil->chooseNewFileName($conf{"tempspace"},"temp","mrc");
				my $marcout = new Loghandler($outputFile);
				$marcout->deleteFile();
				
				my $output;
				#my $output = getListOfMARC($log,$dbHandler,\@marcOutputRecords);
			
				foreach(@marcOutputRecords)
				{
					my $marc = $_;
					$output.=$marc->as_usmarc();
					$count++;
				}
				$log->addLogLine("Outputting $count record(s) into $outputFile");
				$marcout->addLineRaw($output);
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
					my $bib_sourceid = getbibsource($dbHandler);
					#print "Bib source id: $bib_sourceid\n";
					@info = @{importMARCintoEvergreen($outputFile,$log,$dbHandler,$mobUtil,$bib_sourceid)};
					$finalImport = 1;
					$log->addLine(Dumper(\@info));
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
		my @updated = @{@info[2]};
		my $workedCount = $#worked+1;
		my $notWorkedCount = $#notworked+1;
		my $updatedCount = $#updated+1;
		my $fileCount = $#files;
		$fileCount++;
		my $afterProcess = DateTime->now(time_zone => "local");
		my $difference = $afterProcess - $dt;
		my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
		my $duration =  $format->format_duration($difference);
		my $fileList;
		my $successTitleList;
		my $successUpdateTitleList;
		my $failedTitleList;
		foreach(@files)
		{
			my $temp = $_;
			$temp = substr($temp,rindex($temp, '/')+1);
			$fileList.="$temp ";
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
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Insert\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
			foreach(@notworked)
			{
				my $title = $_;
				$failedTitleList.=$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Failed Insert\",\"\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
			foreach(@updated)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $title = @both[1];
				$successUpdateTitleList.=$bibid." ".$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Update\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
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
			if(length($successUpdateTitleList)>5000)
			{
				$successUpdateTitleList = substr($successUpdateTitleList,0,5000)."\r\nTRUNCATED FOR LENGTH";
			}
			if(length($failedTitleList)>5000)
			{
				$failedTitleList = substr($failedTitleList,0,5000)."\r\nTRUNCATED FOR LENGTH";
			}
			my $totalSuccess=1;
			if($notWorkedCount>0)
			{
				$totalSuccess=0;
			}
			my @tolist = ($conf{"alwaysemail"});		
			my $email = new email($conf{"fromemail"},\@tolist,$valid,$totalSuccess,\%conf);
			$fileList=~s/\s/\r\n/g;
			$email->send("Evergreen Utility - Overdrive Import Report Job # $dateString","I connected to: \r\n ".$conf{"server"}."\r\nand gathered:\r\n$count Record(s) from $fileCount file(s)\r\n$workedCount Successful Imports\r\n$notWorkedCount Not successful Imports\r\n Duration: $duration\r\n\r\n$fileList\r\nSuccessful Imports:\r\n$successTitleList\r\n\r\n\r\nSuccessful Updates:\r\n$successUpdateTitleList\r\n\r\nUnsuccessful:\r\n$failedTitleList\r\n\r\n-MOBIUS Perl Squad-");
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub getListOfMARC
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my @marcOutputRecords = @{@_[2]};
	my $ret;
	my $matches=0;
	my $loops=0;
	foreach(@marcOutputRecords)
	{
		my $zero01 = $_->field('001')->data();	
		print "$matches / $loops - $zero01\n";
		my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE deleted='f' AND ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE LABEL!='##URI##') and id in(select distinct lead_bibid from m_dedupe.merge_map) and MARC LIKE '%$zero01%' ";
		my @results = @{$dbHandler->query($query)};
		my $found=0;
		foreach(@results)
		{
			print "Found one!\n";
			$found=1;
		}
		if($found)
		{
			$ret.=$_->as_usmarc();
			$matches++;
		}
		if($matches>5)
		{
			return $ret;
		}
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
				if(1)#$loops<1)
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
													my $getsuccess = getstore($URL.$link, $localFile);
													#print $getsuccess."\n";
													if($getsuccess eq "200")
													{
														$loops++;
														#print "success: $getsuccess\n";
														push(@scrapedFileLinks,$URL.$link);
														push(@downloadedFiles,$localFile);
														#$log->addLine($localFile);
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
	print "errors\n";
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
				my @sub3 = @recID[$rec]->subfield( '3' );
				my $ignore=0;
				foreach(@sub3)
				{
					if(lc($_) eq 'excerpt')
					{
						$ignore=1;
					}
				}
				if(!$ignore)
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
	}
	return $marc;
}

sub removeOldCallNumberURI
{
	my $bibid = @_[0];
	my $dbHandler = @_[1];
	my $query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE record = $bibid AND label = '##URI##'
	)
	";
	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE  record = $bibid AND label = '##URI##'
	)";
	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri WHERE id not in
	(
		SELECT uri FROM asset.uri_call_number_map
	)";
	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = '##URI##'
	";
	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = '##URI##'
	";
	$dbHandler->update($query);

}

sub recordAssetCopyMove
{
	my $oldbib = @_[0];
	my $newbib = @_[1];
	my $dbHandler = @_[2];
	my $overdriveMatchString = @_[3];
	my $log = @_[4];
	my $query = "select id from asset.copy where call_number in(select id from asset.call_number where record in($oldbib) and label!='##URI##')";
	my @cids;
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		push(@cids,@row[0]);
	}
	
	if($#cids>-1)
	{		
		#attempt to put those asset.copies back onto the previously deleted bib from m_dedupe
		moveAssetCopyToPreviouslyDedupedBib($dbHandler,$oldbib,$overdriveMatchString,$log);		
	}	
	
	#Check again after the attempt to undedupe
	@cids = ();
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		push(@cids,@row[0]);
	}
	
	foreach(@cids)
	{
		print "There were asset.copies on $oldbib\n";
		$log->addLine("\t$oldbib\tContained physical Items");
		 $query = "
		INSERT INTO molib2go.item_reassignment(copy,prev_bib,target_bib)
		VALUES ($_,$oldbib,$newbib)";
		$log->addLine("$query");
		$dbHandler->update($query);
	}
}

sub recordBIBMARCChanges
{
	my $bibID = @_[0];
	my $oldMARC = @_[1];
	my $newMARC = @_[2];
	my $dbHandler = @_[3];
	my $log = @_[4];
	
		 my $query = "
		INSERT INTO molib2go.bib_marc_update(record,prev_marc,changed_marc)
		VALUES ($bibID,\$\$$oldMARC\$\$,\$\$$newMARC\$\$)";
		$dbHandler->update($query);
}

sub mergeBIBs
{
	my $oldbib = @_[0];
	my $newbib = @_[1];
	my $dbHandler = @_[2];
	my $overdriveMatchString = @_[3];
	my $log = @_[4];	
	recordAssetCopyMove($oldbib,$newbib,$dbHandler,$overdriveMatchString,$log);
	my $query = "INSERT INTO molib2go.bib_merge(leadbib,subbib) VALUES($newbib,$oldbib)";
	#$log->addLine("MERGE:\t$newbib\t$oldbib");
	$log->addLine($query);
	$dbHandler->update($query);	
	#print "About to merge assets\n";
	$query = "SELECT asset.merge_record_assets($newbib, $oldbib)";
	$log->addLine($query);
	$dbHandler->query($query);
	#print "Merged\n";
}

sub calcSHA1
{
	my $marc = @_[0];
	my $sha1 = Digest::SHA1->new;
	$sha1->add(  length(getsubfield($marc,'007',''))>6 ? substr( getsubfield($marc,'007',''),0,6) : '' );
	$sha1->add(getsubfield($marc,'245','h'));
	$sha1->add(getsubfield($marc,'001',''));
	$sha1->add(getsubfield($marc,'245','a'));
	return $sha1->hexdigest;
}

sub getsubfield
{
	my $marc = @_[0];
	my $tag = @_[1];
	my $subtag = @_[2];
	my $ret;
	#print "Extracting $tag $subtag\n";
	if($marc->field($tag))
	{
		if($tag<10)
		{	
			#print "It was less than 10 so getting data\n";
			$ret = $marc->field($tag)->data();
		}
		elsif($marc->field($tag)->subfield($subtag))
		{
			$ret = $marc->field($tag)->subfield($subtag);
		}
	}
	#print "got $ret\n";
	return $ret;	
}

sub importMARCintoEvergreen
{
	my @ret;
	my @worked;
	my @notworked;
	my @updated;
	my $inputFile = @_[0];
	my $log = @_[1];
	my $dbHandler = @_[2];
	my $mobUtil = @_[3];
	my $bibsourceid = @_[4];	
	my $file = MARC::File::USMARC->in( $inputFile );
	my $r =0;		
	my $overlay = 0;
	my $query;
	print "Working on importMARCintoEvergreen\n";
	
	while ( my $marc = $file->next() ) 
	{
		
		if(1)#$overlay<16)
		{
			#my $tcn = getTCN($log,$dbHandler);  #removing this because it has an auto created value in the DB
			my $title = getsubfield($marc,'245','a');
			#print "Importing $title\n";
			my $sha1 = calcSHA1($marc);
			$marc = readyMARCForInsertIntoME($marc);
			my $bibid=-1;
			my $bibid = findRecord($marc, $dbHandler, $sha1, $bibsourceid, $log);
			
			if($bibid!=-1) #already exists so update the marc
			{
				my @ret = @{chooseWinnerAndDeleteRest($bibid, $dbHandler, $sha1, $marc, $bibsourceid, $title, \@notworked, \@updated, $log)};
				@updated = @{@ret[0]};
				@notworked = @{@ret[1]};
				$overlay+=$#updated+1;
			}
			else  ##need to insert new bib instead of update
			{
				my $starttime = time;
				my $max = getEvergreenMax($dbHandler);
				my $thisXML = convertMARCtoXML($marc,$log);
				
				$query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(null,'IMPORT-$starttime',\$\$$thisXML\$\$,null,$bibsourceid,E'molib2go-script $sha1',null,null)";
				$log->addLine($query);
				my $res = $dbHandler->update($query);
				#print "$res";
				my $newmax = getEvergreenMax($dbHandler);
				if($newmax != $max)
				{
					my @temp = ($newmax,$title);
					push @worked, [@temp];
					$log->addLine("$newmax\thttp://mig.missourievergreen.org/eg/opac/record/$newmax?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
					$query = "INSERT INTO molib2go.bib_marc_update(record,changed_marc,new_record) VALUES($newmax,\$\$$thisXML\$\$,'t')";
					$dbHandler->update($query);
				}
				else
				{
					push (@notworked, $marc);
				}
			}
			
			undef $sha1;
		}
		$r++;
	}
	$file->close();
	undef $file;
	push(@ret, (\@worked, \@notworked, \@updated));
	#print Dumper(@ret);
	return \@ret;
	
}


sub chooseWinnerAndDeleteRest
{
	my @list = @{@_[0]};
	my $dbHandler = @_[1];
	my $sha1 = @_[2];
	my $newMarc = @_[3];
	my $bibsourceid = @_[4];
	my $title = @_[5];
	my @notworked = @{@_[6]};
	my @updated = @{@_[7]};
	my $log = @_[8];
	my $chosenWinner = 0;
	my $bestScore=0;
	my $finalMARC;
	my $i=0;
	my $winnerBibID;
	my $winnerOGMARCxml;
	my $matchnum = $#list+1;
	my $overdriveMatchString = $newMarc->field('001')->data();
	foreach(@list)
	{
		my @attrs = @{$_};	
		my $id = @attrs[0];
		my $score = @attrs[2];
		my $marcxml = @attrs[3];
		if($score>$bestScore)
		{
			$bestScore=$score;
			$chosenWinner=$i;
			$winnerBibID = $id;
			$winnerOGMARCxml = $marcxml;
		}		
		$i++;
	}
	$finalMARC = @{@list[$chosenWinner]}[1];
	$i=0;
	foreach(@list)
	{	
		my @attrs = @{$_};	
		my $id = @attrs[0];
		removeOldCallNumberURI($id, $dbHandler);
		my $marc = @attrs[1];
		my $marcxml = @attrs[3];
		if($i!=$chosenWinner)
		{	
			$finalMARC = mergeMARC856($finalMARC, $marc, $log);
			$finalMARC = fixLeader($finalMARC);			
			mergeBIBs($id, $winnerBibID, $dbHandler, $overdriveMatchString, $log);			
		}
		$i++;
	}
	#attempt to move any items on the winning bib that were deduped 6-28-2013
	moveAssetCopyToPreviouslyDedupedBib($dbHandler,$winnerBibID,$overdriveMatchString,$log);
	# melt the incoming molib2go 856's retaining the rest of the marc from the DB
	# At this point, the 9's have been added to the newMarc (data from molib2go)
	$finalMARC = mergeMARC856($finalMARC, $newMarc, $log);
	$finalMARC = fixLeader($finalMARC);
	my $newmarcforrecord = convertMARCtoXML($finalMARC,$log);
	recordBIBMARCChanges($winnerBibID, $winnerOGMARCxml, $newmarcforrecord, $dbHandler,$log);
	
	my $thisXML = convertMARCtoXML($finalMARC, $log);					
	my $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$\$$thisXML\$\$,tcn_source=E'molib2go-script $sha1',source=$bibsourceid WHERE ID=$winnerBibID";
	$log->addLine($query);
	$log->addLine("$winnerBibID\thttp://missourievergreen.org/eg/opac/record/$winnerBibID?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml\thttp://mig.missourievergreen.org/eg/opac/record/$winnerBibID?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml\t$matchnum");
	my $res = $dbHandler->update($query);
	#print "$res\n";					
	if($res)
	{
		my @temp = ($winnerBibID, $title);
		push @updated, [@temp];
	}
	else
	{
		push (@notworked, $winnerBibID);
	}
	my @ret;
	push @ret, [@updated];
	push @ret, [@notworked];
	
	return \@ret;
	
}

sub moveAssetCopyToPreviouslyDedupedBib
{
	my $dbHandler = @_[0];	
	my $currentBibID = @_[1];
	my $overdriveMatchString = @_[2];
	my $log = @_[3];
	my %possibles;
	#this query will only return previously deleted bibs that do not have the molib2go 001 field ($overdriveMatchString)
	my $query = "select mmm.sub_bibid,bre.marc from m_dedupe.merge_map mmm, biblio.record_entry bre 
	where lead_bibid=$currentBibID and bre.id=mmm.sub_bibid and bre.marc not like '%$overdriveMatchString%'";
	print $query."\n";
	my @results = @{$dbHandler->query($query)};
	my $winner=0;
	my $currentWinnerElectricScore=10000;
	my $currentWinnerMARCScore=0;
	foreach(@results)
	{
		my @row = @{$_};
		my $prevmarc = @row[1];
		$prevmarc =~ s/(<leader>.........)./${1}a/;
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my @temp=($prevmarc,determineElectric($prevmarc),scoreMARC($prevmarc,$log));
		#need to initialize the winner values
		$winner=@row[0];
		$currentWinnerElectricScore = @temp[1];
		$currentWinnerMARCScore = @temp[2];
		$possibles{@row[0]}=\@temp;
	}
	
	#choose the best deleted bib - we want the lowest electronic bib score in this case because we want to attach the 
	#items to the *most physical bib
	while ((my $bib, my $attr) = each(%possibles))
	{
		my @atts = @{$attr};
		if(@atts[1]<$currentWinnerElectricScore)
		{
			$winner=$bib;
			$currentWinnerElectricScore=@atts[1];
			$currentWinnerMARCScore=@atts[2];
		}
		elsif(@atts[1]==$currentWinnerElectricScore && @atts[2]>$currentWinnerMARCScore)
		{
			$winner=$bib;
			$currentWinnerElectricScore=@atts[1];
			$currentWinnerMARCScore=@atts[2];
		}		
	}
	if($winner!=0)
	{
		$query = "select deleted from biblio.record_entry where id=$winner";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my $row = $_;
			my @row = @{$row};
			print "$winner - ".@row[0]."\n";
			#make sure that it is in fact deleted
			if(@row[0] eq 't' ||@row[0] == 1)
			{
				my $tcn_value = $winner;
				my $count=1;			
				#make sure that when we undelete it, it will not collide its tcn_value 
				while($count>0)
				{
					$query = "select count(*) from biblio.record_entry where tcn_value='$tcn_value' and id!=$winner";
					my @results = @{$dbHandler->query($query)};
					foreach(@results)
					{	
						my $row = $_;
						my @row = @{$row};
						$count=@row[0];
					}
					$tcn_value.="_";
				}
				#finally, undelete the bib making it available for the asset.call_number
				$query = "update biblio.record_entry set deleted='f',tcn_source='un-deduped',tcn_value='$tcn_value'  where id=$winner";
				$dbHandler->update($query);
			}
		}
		#find all of the eligible call_numbers
		$query = "SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD=$currentBibID AND LABEL!='##URI##' AND DELETED='f'";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my @row = @{$_};
			my $acnid = @row[0];
			$query = 
"INSERT INTO molib2go.undedupe(oldleadbib,undeletedbib,undeletedbib_electronic_score,undeletedbib_marc_score,moved_call_number)
VALUES($currentBibID,$winner,$currentWinnerElectricScore,$currentWinnerMARCScore,$acnid)";
			$log->addLine($query);
			$dbHandler->update($query);
			$query = "UPDATE ASSET.CALL_NUMBER SET RECORD=$winner WHERE id = $acnid";
			$log->addLine($query);
			$dbHandler->update($query);
		}
	}
}

sub determineElectric
{
	my $marc = @_[0];
	my @e56s = $marc->field('856');	
	my $textmarc = $marc->as_formatted();
	my $score=0;
	my @phrases = ("electronic resource","ebook","eaudiobook","overdrive","download");
	my $has856 = 0;
	my $has245h = getsubfield($marc,'245','h');
	my $found=0;	
	foreach(@e56s)
	{
		my $field = $_;
		my $ind2 = $field->indicator(2);
		if($ind2==0) #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
		{	
			my @subs = $field->subfield('u');
			foreach(@subs)
			{
				#print "checking $_ for http\n";
				if(m/http/g)
				{
					$score++;
				}
			}
		}
	}	
	foreach(@phrases)
	{
		my $phrase = $_;
		my @c = split($phrase,lc$textmarc);
		if($#c>1) # Found more than 1 match on that phrase
		{
			$score++;
		}
	}
	#print "Electric score: $score\n";
	return $score;
}

sub findRecord
{
	my $marcsearch = @_[0];
	my $zero01 = $marcsearch->field('001')->data();
	my $dbHandler = @_[1];
	my $sha1 = @_[2];
	my $bibsourceid = @_[3];
	my $log = @_[4];
	my $query = "SELECT bre.ID,bre.MARC FROM BIBLIO.RECORD_ENTRY bre WHERE bre.tcn_source LIKE '%$sha1%' and bre.source=$bibsourceid and bre.deleted='f'";
	my @results = @{$dbHandler->query($query)};
	my @ret;
	my $none=1;
	my $foundIDs;
	my $count=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		print "found matching sha1: $id\n";		
		my $prevmarc = $marc;
		$prevmarc =~ s/(<leader>.........)./${1}a/;	
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my $score = scoreMARC($prevmarc,$log);
		my @matchedsha = ($id,$prevmarc,$score,$marc);
		$foundIDs.="$id,";
		push (@ret, [@matchedsha]);
		$none=0;
		$count++;
	}
	$foundIDs = substr($foundIDs,0,-1);
	if(length($foundIDs)<1)
	{
		$foundIDs="-1";
	}
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC LIKE '%$zero01%' and ID not in($foundIDs) and deleted='f'";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		print "found matching 001: $id\n";
		my $marc = @row[1];
		my $prevmarc = $marc;
		$prevmarc =~ s/(<leader>.........)./${1}a/;	
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my $score = scoreMARC($prevmarc,$log);
		my @matched001 = ($id,$prevmarc,$score,$marc);
		push (@ret, [@matched001]);	
		$none=0;
		$count++;
	}	
	if($none)
	{
		return -1;
	}
	print "Count matches: $count\n";
	return \@ret;
	
}

sub readyMARCForInsertIntoME
{
	my $marc = @_[0];
	$marc = fixLeader($marc);	
	my $lbyte6 = substr($marc->leader(),6,1);
	
	my $two45 = $marc->field('245');
	my @e856s = $marc->field('856');
	
	if($two45)
	{
		my $value = "item";
		if($lbyte6 eq 'm' || $lbyte6 eq 'i')
		{	
			$value = "eBook";
			if($lbyte6 eq 'i')
			{
				$value = "eAudioBook";
			}
			if($two45->subfield('h'))
			{
				$two45->update( 'h' => "[Overdrive downloadable $value] /" );
			}
			else
			{			
				$two45->add_subfields('h' => "[Overdrive downloadable $value] /");
			}
		}
		if(@e856s)
		{
			foreach(@e856s)
			{
				my $thisfield = $_;
				my @sub3 = $thisfield->subfield( '3' );
				my $ignore=0;
				foreach(@sub3)
				{
					if(lc($_) eq 'excerpt')
					{
						$ignore=1;
					}
				}
				if(!$ignore)
				{
					$thisfield->delete_subfield(code => 'z');					
					$thisfield->add_subfields('z'=> "Click for access to the downloadable $value via Overdrive");
				}
			}
		}			
	}
	return $marc;
}

sub mergeMARC856
{
	my $marc = @_[0];
	my $marc2 = @_[1];
	my $log = @_[2];
	
	my @eight56s = $marc->field("856");
	my @eight56s_2 = $marc2->field("856");
	my @eights;
	my $original856 = $#eight56s + 1;
	@eight56s = (@eight56s,@eight56s_2);

	my %urls;  


	foreach(@eight56s)
	{
		my $thisField = $_;
		
		# Just read the first $u and $z
		my $u = $thisField->subfield("u");
		my $z = $thisField->subfield("z");
		
		if($u) #needs to be defined because its the key
		{
			if(!$urls{$u})
			{
				$urls{$u} = $thisField;
			}
			else
			{
				my @nines = $thisField->subfield("9");
				my $otherField = $urls{$u};
				my @otherNines = $otherField->subfield("9");
				my $otherZ = $otherField->subfield("z");		
				if(!$otherZ)
				{
					if($z)
					{
						$otherField->add_subfields('z'=>$z);
					}
				}
				foreach(@nines)
				{
					my $looking = $_;
					my $found = 0;
					foreach(@otherNines)
					{
						if($looking eq $_)
						{
							$found=1;
						}
					}
					if($found==0)
					{
						$otherField->add_subfields('9' => $looking);
					}
				}
				$urls{$u} = $otherField;
			}
		}
	}
	
	my $finalCount = scalar keys %urls;
	if($original856 != $finalCount)
	{
		$log->addLine("There was $original856 and now there are $finalCount");
	}
	
	my $dump1=Dumper(\%urls);
	my @remove = $marc->field('856');
	#$log->addLine("Removing ".$#remove." 856 records");
	$marc->delete_fields(@remove);


	while ((my $internal, my $mvalue ) = each(%urls))
		{	
			$marc->insert_grouped_field( $mvalue );
		}
	return $marc;
}

sub getEvergreenMax
{
	my $dbHandler = @_[0];
	
	my $query = "SELECT MAX(ID) FROM BIBLIO.RECORD_ENTRY";
	#return 1000;
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

sub convertMARCtoXML
{
	my $marc = @_[0];
	my $log = @_[1];
	my $thisXML =  decode_utf8($marc->as_xml());				
	
	#this code is borrowed from marc2bre.pl
	$thisXML =~ s/\n//sog;	
	$thisXML =~ s/^<\?xml.+\?\s*>//go;	
	$thisXML =~ s/>\s+</></go;	
	$thisXML =~ s/\p{Cc}//go;	
	$thisXML = OpenILS::Application::AppUtils->entityize($thisXML);
	$thisXML =~ s/[\x00-\x1f]//go;
	$thisXML =~ s/^\s+//;
	$thisXML =~ s/\s+$//;
	$thisXML =~ s/<record><leader>/<leader>/;
	$thisXML =~ s/<collection/<record/;	
	$thisXML =~ s/<\/record><\/collection>/<\/record>/;	
	
	#end code
	return $thisXML;
}

sub getbibsource
{
	my $dbHandler = @_[0];
	my $query = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = 'molib2go'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		print "Didnt find molib2go in bib_source, now creating it...\n";
		$query = "INSERT INTO CONFIG.BIB_SOURCE(QUALITY,SOURCE) VALUES(90,'molib2go')";
		my $res = $dbHandler->update($query);
		print "Update results: $res\n";
		$query = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = 'molib2go'";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			return @row[0];
		}
	}
	else
	{
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			return @row[0];
		}
	}
}

sub findPBrecordInME
{
	my $dbHandler = @_[0];	
	#my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC LIKE '%\"9\">PB%' limit 14";
	my $query = "select id,marc from biblio.record_entry where lower(marc) like '%overdrive%' AND lower(marc) like '%ebook%' AND ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE LABEL!='##URI##')";
	my @results = @{$dbHandler->query($query)};
	my @each;
	my @ret;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		@each = ($id,$marc);
		push(@ret,[@each]);	
	}
	return \@ret;
}

sub findMatchInArchive
{
	my @matchList = @{@_[0]};
	my $archiveFolder = @_[1];
	my @files;
	#Get all files in the directory path
	@files = @{dirtrav(\@files,$archiveFolder)};
	my @ret;
	
	for my $b(0..$#files)
	{
	
		my $file = MARC::File::USMARC->in($files[$b]);
		while ( my $marc = $file->next() ) 
		{	
			my $t = $marc->leader();
			my $su=substr($marc->leader(),6,1);
			print "Leader:\n$t\n$su\n";			
			if(1)#$su eq 'a')
			{
				my $all = $marc->as_formatted();
				foreach(@matchList)
				{
					if($all =~ m/$_/g)
					{
						my @booya = ($files[$b]);
						push(@ret,$files[$b]);
						print "This one: ".$files[$b]." matched '$_'\n";
						return \@booya;
					}
				}
			}
		}
		$file->close();
		undef $file;
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

sub fixLeader
{
	my $marc = @_[0];
	my $fullLeader = $marc->leader();
	if(substr($fullLeader,6,1) eq 'a')
	{
		#print "Leader has an a:\n$fullLeader";
		$fullLeader = substr($fullLeader,0,6).'m'.substr($fullLeader,7);
		$marc->leader($fullLeader);
		my $fullLeader = $marc->leader();
		#print "Changed to:\n$fullLeader";
	}
	return $marc;
}


sub scoreMARC
{
	my $marc = shift;
	my $log = shift;
	
	my $score = 0;
	$score+= score($marc,2,100,400,$log,'245');
	$score+= score($marc,1,1,150,$log,'100');
	$score+= score($marc,1,1.1,150,$log,'110');
	$score+= score($marc,0,50,200,$log,'6..');
	$score+= score($marc,0,50,100,$log,'02.');
	
	$score+= score($marc,0,100,200,$log,'246');
	$score+= score($marc,0,100,100,$log,'130');
	$score+= score($marc,0,100,100,$log,'010');
	$score+= score($marc,0,100,200,$log,'490');
	$score+= score($marc,0,10,50,$log,'830');
	
	$score+= score($marc,1,.5,50,$log,'300');
	$score+= score($marc,0,1,100,$log,'7..');
	$score+= score($marc,2,2,100,$log,'50.');
	$score+= score($marc,2,2,100,$log,'52.');
	
	$score+= score($marc,2,.5,200,$log,'51.', '53.', '54.', '55.', '56.', '57.', '58.');

	return $score;
}

sub score
{
	my ($marc) = shift;
	my ($type) = shift;
	my ($weight) = shift;
	my ($cap) = shift;
	my ($log) = shift;
	my @tags = @_;
	my $ou = Dumper(@tags);
	#$log->addLine("Tags: $ou\n\nType: $type\nWeight: $weight\nCap: $cap");
	my $score = 0;			
	if($type == 0) #0 is field count
	{
		#$log->addLine("Calling count_field");
		$score = count_field($marc,$log,\@tags);
	}
	elsif($type == 1) #1 is length of field
	{
		#$log->addLine("Calling field_length");
		$score = field_length($marc,$log,\@tags);
	}
	elsif($type == 2) #2 is subfield count
	{
		#$log->addLine("Calling count_subfield");
		$score = count_subfield($marc,$log,\@tags);
	}
	$score = $score * $weight;
	if($score > $cap)
	{
		$score = $cap;
	}
	$score = int($score);
	#$log->addLine("Weight and cap applied\nScore is: $score");
	return $score;
}

sub count_subfield
{
	my ($marc) = $_[0];
	my $log = $_[1];
	my @tags = @{$_[2]};
	my $total = 0;
	#$log->addLine("Starting count_subfield");
	foreach my $tag (@tags) 
	{
		my @f = $marc->field($tag);
		foreach my $field (@f)
		{
			my @subs = $field->subfields();
			my $ou = Dumper(@subs);
			#$log->addLine($ou);
			if(@subs)
			{
				$total += scalar(@subs);
			}
		}
	}
	#$log->addLine("Total Subfields: $total");
	return $total;
	
}	

sub count_field 
{
	my ($marc) = $_[0];
	my $log = $_[1];
	my @tags = @{$_[2]};
	my $total = 0;
	foreach my $tag (@tags) 
	{
		my @f = $marc->field($tag);
		$total += scalar(@f);
	}
	return $total;
}

sub field_length 
{
	my ($marc) = $_[0];
	my $log = $_[1];
	my @tags = @{$_[2]};

	my @f = $marc->field(@tags[0]);
	return 0 unless @f;
	my $len = length($f[0]->as_string);
	my $ou = Dumper(@f);
	#$log->addLine($ou);
	#$log->addLine("Field Length: $len");
	return $len;
}


sub setupSchema
{
	my $dbHandler = @_[0];
	my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'molib2go'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		$query = "CREATE SCHEMA molib2go";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.item_reassignment(
		id serial,
		copy bigint,
		prev_bib bigint,
		target_bib bigint,
		change_time timestamp default now()
		)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.bib_marc_update(
		id serial,
		record bigint,
		prev_marc text,
		changed_marc text,
		new_record boolean NOT NULL DEFAULT false,
		change_time timestamp default now())";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.bib_merge(
		id serial,
		leadbib bigint,
		subbib bigint,
		change_time timestamp default now())";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.undedupe(
		id serial,
		oldleadbib bigint,
		undeletedbib bigint,
		undeletedbib_electronic_score bigint,
		undeletedbib_marc_score bigint,
		moved_call_number bigint,
		change_time timestamp default now())";
		$dbHandler->update($query);		
	}
}

 exit;

 
 