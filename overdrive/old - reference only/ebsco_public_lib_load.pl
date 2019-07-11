#!/usr/bin/perl

# These Perl modules are required:
# install pQuery
# install Email::MIME
# install Email::Sender::Simple
# install Digest::SHA1

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
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
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
 our %conf;
 our $jobid=-1;
 our $log;
 our $archivefolder;
 our $importSourceName;
 our $importSourceNameDB;
 our $dbHandler;
 our @shortnames;
 
 if($conf)
 {
	%conf = %{$conf};
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		$log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("sourcename","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","incomingmarcfolder"); 
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
		$archivefolder = $conf{"archivefolder"};
		$importSourceName = $conf{"sourcename"};
		$importSourceNameDB = $importSourceName;
		$importSourceNameDB =~ s/\s/\-/g;
		
		if(!(-d $archivefolder))
		{
			$valid = 0;
			print "Sorry, the archive folder does not exist: $archivefolder\n";
			$errorMessage = "Sorry, the archive folder does not exist: $archivefolder";
		}
		#remove trailing slash
		$archivefolder =~ s/\/$//;
		my $finalImport = 0;
		my @info;
		my @infoRemoval;
		my $count=0;
		my $countremoval=0;
		my @files;
		
		if($valid)
		{	
			my @marcOutputRecords;
			my @marcOutputRecordsRemove;
			@shortnames = split(/,/,$conf{"participants"});
			for my $y(0.. $#shortnames)
			{				
				@shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
			}
			$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});
			setupSchema($dbHandler);
			
			# @files = @{dirtrav(\@files,"/mnt/evergreen/tmp/test/marc records/ebrary/")};
			# @files = ("importtest.mrc");
			@files = @{getmarc($conf{"incomingmarcfolder"},$archivefolder,$log)};
			if(@files[$#files]!=-1)
			{
			#print Dumper(@files);
				my $cnt = 0;
				my @removalFiles = ();
				for my $b(0..$#files)
				{
					my $thisfilename = lc($files[$b]);
					
					$log->addLogLine("Parsing: $archivefolder/".$files[$b]);
					my $file = MARC::File::USMARC->in("$archivefolder/".$files[$b]);
					
					if(! ($thisfilename =~ m/remove/))
					{					
						while ( my $marc = $file->next() ) 
						{	
							$marc = add9($marc);
							push(@marcOutputRecords,$marc);
						}
						$cnt++;
					}
					else
					{
						while ( my $marc = $file->next() ) 
						{	
							push(@marcOutputRecordsRemove,$marc);
						}
						push (@removalFiles,$thisfilename);
					}
					$file->close();
					undef $file;
				}
				my $outputFile = $mobUtil->chooseNewFileName($conf{"tempspace"},"temp","mrc");
				my $outputFileRemoval = $mobUtil->chooseNewFileName($conf{"tempspace"},"tempremoval","mrc");
				my $marcout = new Loghandler($outputFile);
				$marcout->deleteFile();
				my $marcoutRemoval = new Loghandler($outputFileRemoval);
				$marcoutRemoval->deleteFile();
				
				my $output;
				#my $output = getListOfMARC($log,$dbHandler,\@marcOutputRecords);
			
				foreach(@marcOutputRecords)
				{
					my $marc = $_;
					$output.=$marc->as_usmarc();
					$count++;
				}
				$log->addLogLine("Outputting $count record(s) into $outputFile");
				$marcout->appendLine($output);
				
				$output='';
				foreach(@marcOutputRecordsRemove)
				{
					my $marc = $_;
					$output.=$marc->as_usmarc();
					$countremoval++;
				}
				$log->addLogLine("Outputting $countremoval record(s) into $outputFileRemoval");
				$marcoutRemoval->appendLine($output);
				
				eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
				if ($@) 
				{
					$log->addLogLine("Could not establish a connection to the database");
					$log->addLogLine("Deleting $outputFile");
					$marcout->deleteFile();
					$marcoutRemoval->deleteFile();
					deleteFiles(\@files);
					$valid = 0;
					$errorMessage = "Could not establish a connection to the database";
				}
				if($valid)
				{
					if(($count+$countremoval) > 0)
					{
						my $bib_sourceid = getbibsource();
						$jobid = createNewJob('processing');
						if($jobid!=-1)
						{
							#print "Bib source id: $bib_sourceid\n";
							@info = @{importMARCintoEvergreen($outputFile,$log,$dbHandler,$mobUtil,$bib_sourceid)};
							$finalImport = 1;
							$log->addLine(Dumper(\@info));
							@infoRemoval = @{removeBibsEvergreen($outputFileRemoval,$log,$dbHandler,$mobUtil,$bib_sourceid)};
						}
						else
						{
							$errorMessage = "Could not create a new job number in the schema - delete the downloaded files and restart.";
							$log->addLogLine("Could not create a new job number in the schema - delete the downloaded files and restart.");
							foreach(@files)
							{
								$log->addLogLine($_);
							}
						}
					}
				}
				$marcout->deleteFile();
				$marcoutRemoval->deleteFile();
				updateJob("Completed","");
			}
			else
			{
				$log->addLogLine("There were some errors during the getmarc function, we are stopping execution. Any partially downloaded files are deleted.");
				$errorMessage = "There were some errors during the getmarc function, we are stopping execution. Any partially downloaded files are deleted.";				
			}
		}
		if($finalImport)
		{
			my @worked = @{@info[0]};
			my @notworked = @{@info[1]};
			my @updated = @{@info[2]};
			
			my @notworkedRemoval = @{@infoRemoval[0]};
			my @workedRemoval = @{@infoRemoval[1]};
			
			my $workedCount = $#worked+1;
			my $notWorkedCount = $#notworked+1;
			my $updatedCount = $#updated+1;
			
			my $workedCountRemoval = $#workedRemoval+1;
			my $notWorkedCountRemoval = $#notworkedRemoval+1;
			
			
			my $fileCount = $#files+1;
			my $afterProcess = DateTime->now(time_zone => "local");
			my $difference = $afterProcess - $dt;
			my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
			my $duration =  $format->format_duration($difference);
			my $fileList;
			my $successTitleList;
			my $successUpdateTitleList;
			my $failedTitleList;
			my $successTitleListRemoval;
			my $failedTitleListRemoval;
			foreach(@files)
			{
				my $temp = $_;
				$temp = substr($temp,rindex($temp, '/')+1);
				$fileList.="$temp ";
			}
			
				
			my $csvlines;
			foreach(@worked)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $title = @both[1];
				$successTitleList.=$bibid." ".$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Insert\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$workedCountRemoval Removed\",\"$notWorkedCountRemoval Failed Removal\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
			foreach(@notworked)
			{
				my $title = $_;
				$failedTitleList.=$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Failed Insert\",\"\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$workedCountRemoval Removed\",\"$notWorkedCountRemoval Failed Removal\",\"$fileList\"";
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
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Update\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$workedCountRemoval Removed\",\"$notWorkedCountRemoval Failed Removal\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
			
			foreach(@workedRemoval)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $title = @both[1];
				$successTitleListRemoval.=$bibid." ".$title."\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Remove\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$workedCountRemoval Removed\",\"$notWorkedCountRemoval Failed Removal\",\"$fileList\"";
				$csvline=~s/\n//g;
				$csvline=~s/\r//g;
				$csvline=~s/\r\n//g;
				$csvlines.="$csvline\n";
			}
			
			foreach(@notworkedRemoval)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $title = @both[1];
				my $cid = @both[2];
				$failedTitleListRemoval.=$bibid." ".$cid." $title\r\n";
				my $csvline = "\"$dateString\",\"$errorMessage\",\"Failed Remove\",\"$bibid $cid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$workedCount success\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$workedCountRemoval Removed\",\"$notWorkedCountRemoval Failed Removal\",\"$fileList\"";
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
			if(length($errorMessage)>0)
			{
				my @tolist = ($conf{"alwaysemail"});
				my $email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
				$fileList=~s/\s/\r\n/g;
				$email->send("Evergreen Utility - $importSourceName Import Report Job # $jobid - ERROR","$errorMessage\r\n\r\n-Evergreen Perl Squad-");
				
			}
			else
			{
				$successUpdateTitleList = truncateOutput($successUpdateTitleList,5000);
				$failedTitleList = truncateOutput($failedTitleList,5000);
				$successTitleListRemoval = truncateOutput($successTitleListRemoval,5000);
				$failedTitleListRemoval = truncateOutput($failedTitleListRemoval,5000);
				
				my $totalSuccess=1;
				if($notWorkedCount>0)
				{
					$totalSuccess=0;
				}
				my @tolist = ($conf{"alwaysemail"});
				my $email = new email($conf{"fromemail"},\@tolist,$valid,$totalSuccess,\%conf);
				my $reports = gatherOutputReport($log,$dbHandler);
				$fileList=~s/\s/\r\n/g;
				$email->send("Evergreen Utility - $importSourceName Import Report Job # $jobid","Connected to: \r\n ".$conf{"server"}."\r\nGathered:\r\n$count adds and $countremoval removals from $fileCount file(s)\r\n Duration: $duration
	\r\n\r\nFiles:\r\n$fileList
Unsuccessful Removals:
$failedTitleListRemoval
	These could have failed because there are copies attached which are listed above.\r\n
Successful Removals:
$successTitleListRemoval\r\n\r\n
$reports\r\nSuccessful Imports:\r\n$successTitleList\r\n\r\n\r\nSuccessful Updates:\r\n$successUpdateTitleList\r\n\r\nUnsuccessful:\r\n$failedTitleList\r\n\r\n-Evergreen Perl Squad-");
			}
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub truncateOutput
{
	my $ret = @_[0];
	my $length = @_[1];
	if(length($ret)>$length)
	{
		$ret = substr($ret,0,$length)."\nTRUNCATED FOR LENGTH\n\n";
	}
	return $ret;
}

sub gatherOutputReport
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $newRecordCount=0;
	my $updatedRecordCount=0;
	my $mergedRecords='';
	my $itemsAssignedRecords='';
	my $undedupeRecords='';
	#bib_marc_update table report new bibs
	my $query = "select count(*) from molib2go.bib_marc_update where job=$jobid and new_record is true";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$newRecordCount=@row[0];
	}
	#bib_marc_update table report non new bibs
	$query = "select count(*) from molib2go.bib_marc_update where job=$jobid and new_record is not true";
	@results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$updatedRecordCount=@row[0];
	}
	
	#bib_merge table report
	$query = "select leadbib,subbib from molib2go.bib_merge where job=$jobid";
	@results = @{$dbHandler->query($query)};	
	my $count=0;	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$mergedRecords.=@row[0]." < ".@row[1]."\r\n";
		$count++;
	}
	if($count>0)
	{
		$mergedRecords = truncateOutput($mergedRecords,5000);
		$mergedRecords="$count records were merged - The left number is the winner\r\n".$mergedRecords;
		$mergedRecords."\r\n\r\n\r\n";
	}
	
	
	#item_reassignment table report
	$query = "select target_bib,prev_bib from molib2go.item_reassignment where job=$jobid";
	@results = @{$dbHandler->query($query)};	
	$count=0;	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$itemsAssignedRecords.=@row[0]." < ".@row[1]."\r\n";
		$count++;
	}
	if($count>0)
	{	
		$itemsAssignedRecords = truncateOutput($itemsAssignedRecords,5000);
		$itemsAssignedRecords="$count Records had physical items assigned - The left number is where the items were moved\r\n".$itemsAssignedRecords;
		$itemsAssignedRecords."\r\n\r\n\r\n";
	}
	
	#undedupe table report
	$query = "select undeletedbib,oldleadbib,(select label from asset.call_number where id=a.moved_call_number) from molib2go.undedupe a where job=$jobid";
	@results = @{$dbHandler->query($query)};	
	$count=0;	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$undedupeRecords.=@row[0]." < ".@row[1]." - '".@row[2]."'\r\n";
		$count++;
	}
	if($count>0)
	{	
		$undedupeRecords = truncateOutput($undedupeRecords,5000);
		$undedupeRecords="$count records had physical items and were moved onto a previously deduped bib - The left number is the undeleted deduped bib\r\n
		The right is the molib2go bib that had it's items moved onto the undeduped bib.\r\n
		We have included the call number as well\r\n".$undedupeRecords;
		$undedupeRecords."\r\n\r\n\r\n";
	}
	my $ret=$newRecordCount." New record(s) were created.\r\n\r\n\r\n".
	$updatedRecordCount." Record(s) were updated\r\n\r\n\r\n".$mergedRecords.$itemsAssignedRecords.$undedupeRecords;
	#print $ret;
	return $ret;
	
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
		my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE deleted is false AND ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE LABEL!=\$\$##URI##\$\$) and id in(select distinct lead_bibid from m_dedupe.merge_map) and MARC ~ '$zero01' ";
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
		my $t = new Loghandler("$archivefolder/$_");
		$log->addLogLine("Deleting $_");
		$t->deleteFile();
	}
}

sub parseFileName
{
    my $fullPath = @_[0];
    my @sp = split('/',$fullPath);
    my $path=substr($fullPath,0,( (length(@sp[$#sp]))*-1) );
    
    my @fsp = split('\.',@sp[$#sp]);
    my $fExtension = pop @fsp;
    my $baseFileName = join('.', @fsp);
    $baseFileName= Encode::encode("CP1252", $baseFileName);
    my @ret = ($path,$baseFileName,$fExtension);
    return \@ret;
}

sub moveFile
{
    my $file = @_[0];
    my $destination = @_[1];
    my $fhandle = new Loghandler($file);

    if( $fhandle->copyFile($destination) )
    {
        if(! (unlink($file)) )
        {
            print "Unable to delete $file";
            return 0;
        }
    }
    undef $fhandle;
    return 1;
}

sub getmarc
{
	my $incomingfolder = @_[0];
    my $archivefolder = @_[1];
    my $log = @_[2];
	
    my @ret;
    my @files;
	#Get all files in the directory path
	@files = @{dirtrav(\@files,$incomingfolder)};
    
	foreach(@files)
	{
		my $filename = $_;
        my @filePathParse = @{parseFileName($filename)};
        $filename = @filePathParse[1].'.'.@filePathParse[2];
		my $download = decideToDownload($filename);
		
		if($download)
		{
			if(-e "$archivefolder/$filename")
			{
				my $size = stat("$archivefolder/$filename")->size; #[7];
				my $rsize = stat("$incomingfolder/$filename")->size;
				# print "Local: $size\n";
				# print "remot: $rsize\n";
				
                if($size ne $rsize)
				{
					$log->addLine("$archivefolder/$filename differes in size remote $filename");
					unlink("$archivefolder/$filename");
				}
				else
				{
					$log->addLine("skipping $filename");
                    unlink(@filePathParse[0].'/'.$filename);
					$download=0;
				}
			}
			else
			{
				$log->addLine("NEW $filename");
			}
			if($download)
			{
                my $fhandle = new Loghandler();
				my $worked = moveFile(@filePathParse[0].'/'.$filename,"$archivefolder/$filename");
				if($worked)
				{
					push (@ret, "$filename");
				}
			}
		}
	}
	$log->addLine(Dumper(\@ret));
	return \@ret;
}

sub decideToDownload
{
	my $filename = @_[0];
	$filename = lc($filename);
	if(! ($filename =~ m/\.pub/g) )
	{
		return 0;
	}
	return 1;
}

sub add9
{
	my $marc = @_[0];
	my @recID = $marc->field('856');
	if(@recID)
	{
		#$marc->delete_fields( @recID );
		for my $rec(0..$#recID)
		{
			#print Dumper(@recID[$rec]);
			for my $t(0.. $#shortnames)
			{
				my @sub3 = @recID[$rec]->subfield( '3' );
				my $ind2 = @recID[$rec]->indicator(2);
				my $ignore=0;
				foreach(@sub3)
				{
					if(lc($_) eq 'excerpt')
					{
						$ignore=1;
					}
				}
				if($ind2 ne '0')
				{
					$ignore=1;
				}
				if(!$ignore)
				{
					my @s7 = @recID[$rec]->subfield( '7' );
					if($#s7==-1)
					{
						@recID[$rec]->add_subfields('7'=>$importSourceName);
					}
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
			my @urls = @recID[$rec]->subfield( 'u' );
			foreach(@urls)
			{
				my $finalURL = $_;
				#$log->addLine("URL is $finalURL");
				$finalURL = '/eg/opac/referringurl?intendedurl='.$finalURL.'&authtype=url,uid' if $finalURL !=~ m/referringurl\?intendedurl/;
				#$log->addLine("Now its $finalURL");
				@recID[$rec]->update( 'u' => $finalURL );
			}
		}
	}
	return $marc;
}

sub removeOldCallNumberURI
{
	my $bibid = @_[0];
	my $dbHandler = @_[1];
    
    my $uriids = '';
    my $query = "select uri from asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE record = $bibid AND label = \$\$##URI##\$\$
	)";
updateJob("Processing","$query");
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $uriids.=@row[0].",";
    }
    $uriids = substr($uriids,0,-1);
    
	my $query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE record = $bibid AND label = \$\$##URI##\$\$
	)
	";
updateJob("Processing","$query");
	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	)";
updateJob("Processing","$query");
	$dbHandler->update($query);
    
    if(length($uriids) > 0)
    {
        $query = "DELETE FROM asset.uri WHERE id in ($uriids)";
    updateJob("Processing","$query");
        $dbHandler->update($query);
    }
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	";
updateJob("Processing","$query");	
	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	";
updateJob("Processing","$query");
	$dbHandler->update($query);

}

sub recordAssetCopyMove
{
	my $oldbib = @_[0];
	my $newbib = @_[1];
	my $dbHandler = @_[2];
	my $overdriveMatchString = @_[3];
	my $log = @_[4];
	my $query = "select id from asset.copy where call_number in(select id from asset.call_number where record in($oldbib) and label!=\$\$##URI##\$\$) and not deleted";
	my @cids;
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		push(@cids,@row[0]);
	}
    
	foreach(@cids)
	{
		print "There were asset.copies on $oldbib even after attempting to put them on a deduped bib\n";
		$log->addLine("\t$oldbib\tContained physical Items");
		 $query = "
		INSERT INTO molib2go.item_reassignment(copy,prev_bib,target_bib,job)
		VALUES ($_,$oldbib,$newbib,$jobid)";
		$log->addLine("$query");
updateJob("Processing","recordAssetCopyMove  $query");
		$dbHandler->update($query);
	}
}

sub recordBIBMARCChanges
{
	my $bibID = @_[0];
	my $oldMARC = @_[1];
	my $newMARC = @_[2];
	
		 my $query = "
		INSERT INTO molib2go.bib_marc_update(record,prev_marc,changed_marc,job)
		VALUES ($bibID,\$1,\$2,$jobid)";
		my @values = ($oldMARC,$newMARC);
		$dbHandler->updateWithParameters($query,\@values);
}

sub mergeBIBs
{

	my $oldbib = @_[0];
	my $newbib = @_[1];
	my $dbHandler = @_[2];
	my $overdriveMatchString = @_[3];
updateJob("Processing","mergeBIBs oldbib: $oldbib newbib=$newbib overdriveMatchString=$overdriveMatchString");
	my $log = @_[4];	
	recordAssetCopyMove($oldbib,$newbib,$dbHandler,$overdriveMatchString,$log);
	my $query = "INSERT INTO molib2go.bib_merge(leadbib,subbib,job) VALUES($newbib,$oldbib,$jobid)";
	#$log->addLine("MERGE:\t$newbib\t$oldbib");
updateJob("Processing","mergeBIBs  $query");	
	$log->addLine($query);
	$dbHandler->update($query);	
	#print "About to merge assets\n";
	$query = "SELECT asset.merge_record_assets($newbib, $oldbib)";
updateJob("Processing","mergeBIBs  $query");
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

sub removeBibsEvergreen
{
	my @ret;
	my @notworked = ();
	my @updated = ();
	my $inputFile = @_[0];
	my $log = @_[1];
	my $dbHandler = @_[2];
	my $mobUtil = @_[3];
	my $bibsourceid = @_[4];
	my $file = MARC::File::USMARC->in( $inputFile );
	my $r =0;		
	my $removed = 0;
	my $loops = 0;
	my $query;	
	#print "Working on removeBibsEvergreen\n";
	updateJob("Processing","removeBibsEvergreen");
	while ( my $marc = $file->next() ) 
	{
		# if($loops < 5)
		# {
		my $title = getsubfield($marc,'245','a');
		my $sha1 = calcSHA1($marc);
		my $bibid = findRecord($marc, $dbHandler, $sha1, $bibsourceid, $log);
		if($bibid!=-1) #already exists
		{	
			@ret = @{attemptRemoveBibs($bibid, $dbHandler, $title, \@notworked, \@updated, $log)};			
			$log->addLine("Got attemptRemoveBibs");
			$log->addLine(Dumper(\@ret));
			@updated = @{@ret[0]};
			@notworked = @{@ret[1]};
			$removed+=$#updated+1;
		}
		else
		{
			my @copy = ($bibid,$title,"No matching bib in ME");
			push( @notworked, [@copy] );
		}
		$loops++;
		#}
	}
	my @ret = ();
	push(@ret, (\@notworked, \@updated));
	return \@ret;
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
	#print "Working on importMARCintoEvergreen\n";
	updateJob("Processing","importMARCintoEvergreen");
	
	while ( my $marc = $file->next() ) 
	{
		
		if(1)#$r>8686)#$overlay<16)
		{
			#my $tcn = getTCN($log,$dbHandler);  #removing this because it has an auto created value in the DB
			my $title = getsubfield($marc,'245','a');
			#print "Importing $title\n";
updateJob("Processing","CalcSHA1");
			my $sha1 = calcSHA1($marc);
updateJob("Processing","updating 245h and 856z");
			$marc = readyMARCForInsertIntoME($marc);
			my $bibid=-1;
			my $bibid = findRecord($marc, $dbHandler, $sha1, $bibsourceid, $log);
			
			if($bibid!=-1) #already exists so update the marc
			{
				@ret = @{chooseWinnerAndDeleteRest($bibid, $dbHandler, $sha1, $marc, $bibsourceid, $title, \@notworked, \@updated, $log)};
				@updated = @{@ret[0]};
				@notworked = @{@ret[1]};
				$overlay+=$#updated+1;
			}
			else  ##need to insert new bib instead of update
			{
				my $starttime = time;
				my $max = getEvergreenMax($dbHandler);
				my $thisXML = convertMARCtoXML($marc);
				my @values = ($thisXML);
				$query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(null,'IMPORT-$starttime',\$1,null,$bibsourceid,\$\$$importSourceNameDB-script $sha1\$\$,null,null)";
				$log->addLine($query);
				my $res = $dbHandler->updateWithParameters($query,\@values);
				#print "$res";
				my $newmax = getEvergreenMax($dbHandler);
				if($newmax != $max)
				{
					my @temp = ($newmax,$title);
					push @worked, [@temp];
					$log->addLine("$newmax\thttp://mig.missourievergreen.org/eg/opac/record/$newmax?locg=157;expand=marchtml#marchtml");
					$query = "INSERT INTO molib2go.bib_marc_update(record,changed_marc,new_record,job) VALUES($newmax,\$1,true,$jobid)";
					my @values = ($thisXML);
					$dbHandler->updateWithParameters($query,\@values);
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
    @ret = ();
	push(@ret, (\@worked, \@notworked, \@updated));
	#print Dumper(@ret);
	return \@ret;

}

sub attemptRemoveBibs
{
#@{attemptRemoveBibs($bibid, $dbHandler, $title, \@notworked, \@updated, $log)};
	my @list = @{@_[0]};
	my $dbHandler = @_[1];
	my $title = @_[2];
	my @notworked = @{@_[3]};
	my @updated = @{@_[4]};
	my $log = @_[5];
	my $matchnum = $#list+1;
	
	foreach(@list)
	{
		my @attrs = @{$_};	
		my $id = @attrs[0];
        $log->addLine("deleting $id");
		my $marcobj = @attrs[1];
		my $score = @attrs[2];
		my $marcxml = @attrs[3];
		my $answer = decideToDeleteOrRemove9($marcobj);
		if($answer==1)
		{
			my $query = "
            SELECT ac.ID,ac.BARCODE FROM ASSET.COPY ac,ASSET.CALL_NUMBER acn
                WHERE 
                ac.call_number = acn.id and
                not ac.deleted and
                not acn.deleted and
                acn.RECORD=$id";
			$log->addLine($query);
			my @results = @{$dbHandler->query($query)};
			foreach(@results)
			{
				my @row = @{$_};
				my $cid = @row[0];
				my $cbarcode = @row[1];
				my @copy = ($id,$cid,$cbarcode);
				push(@notworked, [@copy]);
			}
			if($#results == -1)
			{
				removeOldCallNumberURI($id,$dbHandler);
				my $query = "UPDATE BIBLIO.RECORD_ENTRY SET DELETED=\$\$t\$\$ WHERE ID = \$1";
				$log->addLine($query);
				my @values = ($id);
				my $res = $dbHandler->updateWithParameters($query,\@values);
				if($res)
				{
					my @temp = ($id, $title);
					push (@updated, [@temp]);
				}
				else
				{
					my @copy = ($id,$title,"Error during delete");
					push (@notworked, [@copy]);
				}
			}
		}
		else
		{
			my $finalMARCXML = convertMARCtoXML($answer);
			recordBIBMARCChanges($id, $marcxml, $finalMARCXML);
			my @values = ($finalMARCXML);
			my $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$1 WHERE ID=$id";
		updateJob("Processing","chooseWinnerAndDeleteRest   $query");
			$log->addLine($query);
			$log->addLine("$id\thttp://missourievergreen.org/eg/opac/record/$id?locg=4;expand=marchtml#marchtml\thttp://mig.missourievergreen.org/eg/opac/record/$id?locg=157;expand=marchtml#marchtml\t0");
			my $res = $dbHandler->updateWithParameters($query,\@values);
			if($res)
			{
				my @temp = ($id, $title);
				push (@updated, [@temp]);
			}
			else
			{
				my @copy = ($id,$title,"Error during delete");
				push (@notworked, [@copy]);
			}
			
		}
	}
	my @ret;
	push @ret, [@updated];
	push @ret, [@notworked];
	
	return \@ret;
}

sub decideToDeleteOrRemove9
{
	my $marc = @_[0];
	my @eight56s = $marc->field("856");
	my @eights;
	my $original856 = $#eight56s + 1;

	my %urls;
	my $nonMatch = 0;
	foreach(@eight56s)
	{
		my $thisField = $_;
		my $ind2 = $thisField->indicator(2);
		if($ind2 eq '0')
		{	
			my @ninposes;
			my $poses=0;
			#deleting subfields requires knowledge of what position among all of the subfields they reside.
			#so we have to record at what positions each of the 9's are ahead of time.
			foreach($thisField->subfields())
			{
				
				my @f = @{$_};
				if(@f[0] eq '9')
				{
					push (@ninposes, $poses);
				}
				$poses++;
			}
			my @nines = $thisField->subfield("9");
			my @delete9s = ();			
			my $ninePos = 0;
			my $nonMatchThis856 = 0;
			foreach(@nines)
			{
				my $looking = $_;
				my $found = 0;
				foreach(@shortnames)
				{
					if($looking eq $_)
					{
						$found=1;
					}
				}					
				if($found)
				{
					push(@delete9s, @ninposes[$ninePos]);
				}
				else
				{
					$nonMatch=1;
					$nonMatchThis856 = 1;
				}
				$ninePos++;
			}
			if(!$nonMatchThis856)
			{
				#Just delete the whole field because it only contains these 9's
				$marc->delete_field($thisField);
			}
			else
			{
				#Some of the 9's do not belong to this group, so we just want to delete ours
				#and preserve the record
				$thisField->delete_subfield(code=> '9', 'pos' => \@delete9s);
			}
		}
		
	}
	if(!$nonMatch) #all of the 9s on this record belong to this group and no one else, time to delete the record
	{
		return 1;
	}
	#There were some 9s for other groups, just remove ours and preserve the record
	return $marc;
	
}

sub findMatchingISBN
{
    my $isbn = @_[0];
    my $bibsourceid = @_[1];
    my @ret = ();
    
    my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE
    tcn_source~\$\$$importSourceNameDB-script\$\$ AND
    source=$bibsourceid AND
    NOT DELETED AND
    ID IN(
    select source from metabib.identifier_field_entry
    WHERE
    index_vector  @@ to_tsquery(\$\$$isbn\$\$)
    )
    ";
    $log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
        my $id = @row[0];
        my $marc = @row[1];
        my $marcobj = $marc;
        $marcobj =~ s/(<leader>.........)./${1}a/;
        my $marcobj = MARC::Record->new_from_xml($marcobj);
        my $score = scoreMARC($marcobj,$log);
        my @arr = ($id,$marcobj,$score,$marc);
		push (@ret, [@arr]);
    }
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
	# melt the incoming molib2go 856's retaining the rest of the marc from the DB
	# At this point, the 9's have been added to the newMarc (data from molib2go)
	$finalMARC = mergeMARC856($finalMARC, $newMarc, $log);
	$finalMARC = fixLeader($finalMARC);
	my $newmarcforrecord = convertMARCtoXML($finalMARC);
	recordBIBMARCChanges($winnerBibID, $winnerOGMARCxml, $newmarcforrecord);
	my $thisXML = convertMARCtoXML($finalMARC);
	my @values = ($thisXML);
	#$log->addLine($thisXML);
	my $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$1,tcn_source=\$\$$importSourceNameDB-script $sha1\$\$,source=$bibsourceid WHERE ID=$winnerBibID";
updateJob("Processing","chooseWinnerAndDeleteRest   $query");
	$log->addLine($query);
	$log->addLine($thisXML);
	$log->addLine("$winnerBibID\thttp://missourievergreen.org/eg/opac/record/$winnerBibID?locg=4;expand=marchtml#marchtml\thttp://mig.missourievergreen.org/eg/opac/record/$winnerBibID?locg=157;expand=marchtml#marchtml\t$matchnum");
	my $res = $dbHandler->updateWithParameters($query,\@values);
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

sub moveHolds
{
	my $dbHandler = @_[0];	
	my $oldBib = @_[1];
	my $newBib = @_[2];
	my $log = @_[3];	
	my $query = "UPDATE ACTION.HOLD_REQUEST SET TARGET=$newBib WHERE TARGET=$oldBib AND HOLD_TYPE=\$\$T\$\$ AND current_copy IS NULL AND fulfillment_time IS NULL AND capture_time IS NULL"; 
	$log->addLine($query);
	updateJob("Processing","moveHolds  $query");
	#print $query."\n";
	$dbHandler->update($query);
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
		if($ind2 eq '0') #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
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
	my $query = "SELECT bre.ID,bre.MARC FROM BIBLIO.RECORD_ENTRY bre WHERE bre.tcn_source ~ \$\$$sha1\$\$ and bre.source=$bibsourceid and bre.deleted is false";
updateJob("Processing","$query");
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
    # This is super slow - disabled
	# my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC ~ \$\$$zero01\$\$ and ID not in($foundIDs) and deleted is false ";
# updateJob("Processing","$query");
	# my @results = @{$dbHandler->query($query)};
	# foreach(@results)
	# {
		# my $row = $_;
		# my @row = @{$row};
		# my $id = @row[0];
		# print "found matching 001: $id\n";
		# my $marc = @row[1];
		# my $prevmarc = $marc;
		# $prevmarc =~ s/(<leader>.........)./${1}a/;	
		# $prevmarc = MARC::Record->new_from_xml($prevmarc);
		# my $score = scoreMARC($prevmarc,$log);
		# my @matched001 = ($id,$prevmarc,$score,$marc);
		# push (@ret, [@matched001]);	
		# $none=0;
		# $count++;
	# }
	if($none)
	{
		return -1;
	}
	print "Count matches: $count\n";
updateJob("Processing","Count matches: $count");
	return \@ret;
	
}

sub readyMARCForInsertIntoME
{
	my $marc = @_[0];
	$marc = fixLeader($marc);	
	my $lbyte6 = substr($marc->leader(),6,1);
	
	my $two45 = $marc->field('245');
	my @e856s = $marc->field('856');
    my @e022s = $marc->field('020');
	foreach(@e022s)
    {
        my $thisfield = $_;
        # $log->addLine(Dumper($thisfield->subfields()));
        $thisfield->delete_subfield(code => 'z');
        my $hasMore = 0;
        foreach($thisfield->subfields())
        {
            my @s = @{$_};
            foreach(@s)
            {
                $hasMore = 1;
            }
        }
        # $log->addLine("Deleting the whole field") if !$hasMore;
        $marc->delete_field($thisfield) if !$hasMore;
    }
	if($two45)
	{
		my $value = "item";
		# if($lbyte6 eq 'm' || $lbyte6 eq 'i')
		# {	
			$value = "eBook";
			if($lbyte6 eq 'i')
			{
				$value = "eAudioBook";
			}
			elsif($lbyte6 eq 'g')
			{
				$value = "eVideo";
			}
			if($two45->subfield('h'))
			{
				#$two45->update( 'h' => "[Overdrive downloadable $value] /" );
			}
			else
			{			
				#$two45->add_subfields('h' => "[Overdrive downloadable $value] /");
			}
		# }
		if(@e856s)
		{
			foreach(@e856s)
			{
				my $thisfield = $_;
				my $ind2 = $thisfield->indicator(2);
				if($ind2 eq '0') #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
				{	
					my @sub3 = $thisfield->subfield( '3' );
					my $ignore=0;
					foreach(@sub3)
					{
						if(lc($_) eq 'excerpt')
						{
							$ignore=1;
						}
						if(lc($_) eq 'image')
						{
							$ignore=1;
						}
					}
					if(!$ignore)
					{
						#$thisfield->delete_subfield(code => 'z');					
						#$thisfield->add_subfields('z'=> "Instantly available on ebrary");
					}
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
		my $ind2 = $thisField->indicator(2);
		# Just read the first $u and $z
		my $u = $thisField->subfield("u");
		my $z = $thisField->subfield("z");
		my $s7 = $thisField->subfield("7");
		$log->addLine($u);
		if($u) #needs to be defined because its the key
		{
			if(!$urls{$u})
			{
				if($ind2 ne '0')
				{
					$thisField->delete_subfields('9');
					$thisField->delete_subfields('z');
				}
				$urls{$u} = $thisField;
				#$log->addLine("Not already in array");
			}
			else
			{
				#$log->addLine("Already in array");
				my @nines = $thisField->subfield("9");
				my $otherField = $urls{$u};
				my @otherNines = $otherField->subfield("9");
				my $otherZ = $otherField->subfield("z");		
				my $other7 = $otherField->subfield("7");
				if(!$otherZ)
				{
					if($z)
					{
						$otherField->add_subfields('z'=>$z);
					}
				}
				if(!$other7)
				{
					if($s7)
					{
						$otherField->add_subfields('7'=>$s7);
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
					if($found==0 && $ind2 eq '0')
					{
						$otherField->add_subfields('9' => $looking);
					}
				}
				if($ind2 ne '0')
				{
					$thisField->delete_subfields('9');
					$thisField->delete_subfields('z');
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
	#print "DB Max: $dbmax\n";
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
	my $thisXML =  $marc->as_xml(); #decode_utf8();
	
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

sub createNewJob
{
	my $status = @_[0];
	my $query = "INSERT INTO molib2go.job(status) values('$status')";
	my $results = $dbHandler->update($query);
	if($results)
	{
		$query = "SELECT max( ID ) FROM molib2go.job";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$jobid = @row[0];
			return @row[0];
		}
	}
	return -1;
}

sub updateJob
{
	my $status = @_[0];
	my $action = @_[1];
	my $query = "UPDATE molib2go.job SET last_update_time=now(),status='$status', CURRENT_ACTION_NUM = CURRENT_ACTION_NUM+1,current_action='$action' where id=$jobid";
    $log->addLine($action);
	my $results = $dbHandler->update($query);
	return $results;
}

sub findPBrecordInME
{
	my $dbHandler = @_[0];	
	#my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC ~ '\"9\">PB' limit 14";
	my $query = "select id,marc from biblio.record_entry where marc ~* 'overdrive' AND marc ~* 'ebook' AND ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE LABEL!=\$\$##URI##\$\$ and deleted is false)";
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
	#This is disabled because Shon did not want to change the icon in the catalog
	return $marc;
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
		$query = "CREATE TABLE molib2go.job
		(
		id bigserial NOT NULL,
		start_time timestamp with time zone NOT NULL DEFAULT now(),
		last_update_time timestamp with time zone NOT NULL DEFAULT now(),
		status text default 'processing',	
		current_action text,
		current_action_num bigint default 0,
		CONSTRAINT job_pkey PRIMARY KEY (id)
		  )";		  
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.item_reassignment(
		id serial,
		copy bigint,
		prev_bib bigint,
		target_bib bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT item_reassignment_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE
		)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.bib_marc_update(
		id serial,
		record bigint,
		prev_marc text,
		changed_marc text,
		new_record boolean NOT NULL DEFAULT false,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_marc_update_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.bib_merge(
		id serial,
		leadbib bigint,
		subbib bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.undedupe(
		id serial,
		oldleadbib bigint,
		undeletedbib bigint,
		undeletedbib_electronic_score bigint,
		undeletedbib_marc_score bigint,
		moved_call_number bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT undedupe_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.nine_sync(
		id serial,
		record bigint,
		nines_synced text,
		url text,
		change_time timestamp default now())";
		$dbHandler->update($query);
	}
}

 exit;

 
 